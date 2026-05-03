"""Plan scheduled job concurrency and recovery readiness for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


ScheduledJobSignal = Literal[
    "cron_job",
    "scheduled_worker",
    "recurring_sync",
    "batch_job",
    "nightly_job",
    "interval_polling",
    "maintenance_task",
]
ScheduledJobSafeguard = Literal[
    "distributed_locking",
    "idempotency",
    "missed_run_recovery",
    "backfill_controls",
    "timeout_limits",
    "retry_policy",
    "observability",
    "manual_replay",
]
ScheduledJobRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[ScheduledJobSignal, ...] = (
    "cron_job",
    "scheduled_worker",
    "recurring_sync",
    "batch_job",
    "nightly_job",
    "interval_polling",
    "maintenance_task",
)
_SAFEGUARD_ORDER: tuple[ScheduledJobSafeguard, ...] = (
    "distributed_locking",
    "idempotency",
    "missed_run_recovery",
    "backfill_controls",
    "timeout_limits",
    "retry_policy",
    "observability",
    "manual_replay",
)
_RISK_ORDER: dict[ScheduledJobRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}

_SIGNAL_PATTERNS: dict[ScheduledJobSignal, re.Pattern[str]] = {
    "cron_job": re.compile(r"\b(?:cron|crontab|cronjob|cron job|cron schedule)\b", re.I),
    "scheduled_worker": re.compile(
        r"\b(?:scheduled worker|scheduler|scheduled task|scheduled job|background schedule|"
        r"worker schedule|periodic worker)\b",
        re.I,
    ),
    "recurring_sync": re.compile(
        r"\b(?:recurring sync|periodic sync|scheduled sync|sync job|sync worker|"
        r"recurring import|recurring ingest(?:ion)?)\b",
        re.I,
    ),
    "batch_job": re.compile(r"\b(?:batch job|batch worker|batch process|bulk job|bulk processing)\b", re.I),
    "nightly_job": re.compile(r"\b(?:nightly|overnight|daily job|daily sync|daily run)\b", re.I),
    "interval_polling": re.compile(
        r"\b(?:interval polling|poll every|polls every|polling interval|every \d+\s*(?:seconds?|minutes?|hours?)|"
        r"runs? every \d+\s*(?:seconds?|minutes?|hours?)|periodic polling)\b",
        re.I,
    ),
    "maintenance_task": re.compile(
        r"\b(?:maintenance task|maintenance job|cleanup job|cleanup worker|prune job|"
        r"purge job|compaction job|vacuum job|retention job)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[ScheduledJobSignal, re.Pattern[str]] = {
    "cron_job": re.compile(r"(?:^|/)(?:cron|crontab|cronjobs?)(?:/|\.|_|-|$)", re.I),
    "scheduled_worker": re.compile(r"(?:^|/)(?:schedulers?|scheduled|periodic|workers?)(?:/|\.|_|-|$)", re.I),
    "recurring_sync": re.compile(r"(?:^|/)(?:sync|syncs|ingest|imports?)(?:/|\.|_|-|$)", re.I),
    "batch_job": re.compile(r"(?:^|/)(?:batch|bulk)(?:/|\.|_|-|$)", re.I),
    "nightly_job": re.compile(r"(?:^|/)(?:nightly|daily)(?:/|\.|_|-|$)", re.I),
    "interval_polling": re.compile(r"(?:^|/)(?:poll|polling|interval)(?:/|\.|_|-|$)", re.I),
    "maintenance_task": re.compile(
        r"(?:^|/)(?:maintenance|cleanup|prune|purge|vacuum|retention)(?:/|\.|_|-|$)", re.I
    ),
}
_SAFEGUARD_PATTERNS: dict[ScheduledJobSafeguard, re.Pattern[str]] = {
    "distributed_locking": re.compile(
        r"\b(?:distributed lock|advisory lock|mutex|lease|leader election|single runner|"
        r"prevent overlap|no overlapping runs?|lock timeout|job lock)\b",
        re.I,
    ),
    "idempotency": re.compile(
        r"\b(?:idempotenc\w*|idempotent|dedup(?:e|lication)?|safe to rerun|rerun-safe|replay-safe|"
        r"upsert|unique constraint|duplicate prevention|exactly once)\b",
        re.I,
    ),
    "missed_run_recovery": re.compile(
        r"\b(?:missed[- ]run|missed schedule|catch up|catch-up|resume from checkpoint|checkpoint|"
        r"recover missed|skipped run|late run|last successful run|watermark)\b",
        re.I,
    ),
    "backfill_controls": re.compile(
        r"\b(?:backfill controls?|backfill window|bounded backfill|dry run|rate limit(?:ed)? backfill|"
        r"batch size|chunk size|limit backfill|reconciliation window)\b",
        re.I,
    ),
    "timeout_limits": re.compile(
        r"\b(?:timeout|time limit|deadline|max runtime|maximum runtime|run limit|stale lock|"
        r"kill after|cancel after)\b",
        re.I,
    ),
    "retry_policy": re.compile(
        r"\b(?:retry policy|retries|retry with backoff|exponential backoff|dead[- ]letter|dlq|"
        r"poison message|transient failure)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|metrics?|logs?|traces?|alerts?|monitoring|dashboard|run history|"
        r"job status|success rate|failure rate|duration metric)\b",
        re.I,
    ),
    "manual_replay": re.compile(
        r"\b(?:manual replay|manual rerun|operator replay|admin replay|replay command|rerun command|"
        r"manual trigger|runbook replay|manual backfill)\b",
        re.I,
    ),
}
_MUTATION_RE = re.compile(
    r"\b(?:mutat(?:e|es|ing)|write|writes|persist|update|updates|delete|deletes|purge|prune|"
    r"insert|create|creates|import|ingest|sync|backfill|migrate|reconcile|charge|send email|"
    r"emit notification|modify|archive)\b",
    re.I,
)
_READ_ONLY_RE = re.compile(r"\b(?:read[- ]only|report only|dry run only|no writes|does not mutate)\b", re.I)
_RECOMMENDED_CHECKS: dict[ScheduledJobSafeguard, str] = {
    "distributed_locking": "Add a distributed lock, lease, or single-runner guard to prevent overlapping executions.",
    "idempotency": "Make writes idempotent with dedupe keys, upserts, unique constraints, or safe rerun semantics.",
    "missed_run_recovery": "Define catch-up behavior for skipped, late, or missed schedules using watermarks or checkpoints.",
    "backfill_controls": "Bound backfills with windows, chunk sizes, dry runs, and rate limits.",
    "timeout_limits": "Set max runtime, stale-lock handling, and cancellation behavior for long-running jobs.",
    "retry_policy": "Specify retry limits, backoff, and dead-letter or failure handling for transient errors.",
    "observability": "Emit run metrics, logs, alerts, and run-history status for success, failure, lag, and duration.",
    "manual_replay": "Provide a controlled manual replay or rerun path with operator guidance.",
}


@dataclass(frozen=True, slots=True)
class TaskScheduledJobConcurrencyReadinessRecord:
    """Concurrency and recovery readiness for one scheduled execution task."""

    task_id: str
    title: str
    matched_scheduling_signals: tuple[ScheduledJobSignal, ...]
    required_safeguards: tuple[ScheduledJobSafeguard, ...]
    present_safeguards: tuple[ScheduledJobSafeguard, ...]
    missing_safeguards: tuple[ScheduledJobSafeguard, ...]
    risk_level: ScheduledJobRiskLevel
    recommended_checks: tuple[str, ...]
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "matched_scheduling_signals": list(self.matched_scheduling_signals),
            "required_safeguards": list(self.required_safeguards),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskScheduledJobConcurrencyReadinessPlan:
    """Task-level scheduled job concurrency readiness plan."""

    plan_id: str | None = None
    readiness_records: tuple[TaskScheduledJobConcurrencyReadinessRecord, ...] = field(default_factory=tuple)
    scheduled_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[TaskScheduledJobConcurrencyReadinessRecord, ...]:
        """Compatibility view matching planners that expose rows as records."""
        return self.readiness_records

    @property
    def recommendations(self) -> tuple[TaskScheduledJobConcurrencyReadinessRecord, ...]:
        """Compatibility view for recommendation-oriented consumers."""
        return self.readiness_records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "readiness_records": [record.to_dict() for record in self.readiness_records],
            "records": [record.to_dict() for record in self.records],
            "scheduled_task_ids": list(self.scheduled_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.readiness_records]

    def to_markdown(self) -> str:
        """Render the readiness plan as deterministic Markdown."""
        title = "# Task Scheduled Job Concurrency Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Total tasks: {self.summary.get('task_count', 0)}",
            f"- Scheduled tasks: {self.summary.get('scheduled_task_count', 0)}",
            f"- High risk: {self.summary.get('high_risk_count', 0)}",
            f"- Medium risk: {self.summary.get('medium_risk_count', 0)}",
            f"- Low risk: {self.summary.get('low_risk_count', 0)}",
            f"- Missing safeguards: {self.summary.get('missing_safeguard_count', 0)}",
        ]
        if not self.readiness_records:
            lines.extend(["", "No scheduled job concurrency readiness records were inferred."])
            return "\n".join(lines)
        lines.extend(
            [
                "",
                "## Records",
                "",
                (
                    "| Task | Title | Signals | Risk | Present Safeguards | Missing Safeguards | "
                    "Recommended Checks | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.readiness_records:
            lines.append(
                "| "
                f"{_markdown_cell(record.task_id)} | "
                f"{_markdown_cell(record.title)} | "
                f"{_markdown_cell(', '.join(record.matched_scheduling_signals))} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_scheduled_job_concurrency_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskScheduledJobConcurrencyReadinessPlan:
    """Build scheduled job concurrency readiness records from task-shaped input."""
    plan_id, plan_context, tasks = _source_payload(source)
    records = [
        record
        for index, task in enumerate(tasks, start=1)
        if (record := _record(task, index, plan_context)) is not None
    ]
    records.sort(key=lambda item: (_RISK_ORDER[item.risk_level], item.task_id, item.title.casefold()))
    result = tuple(records)
    signal_counts = {
        signal: sum(1 for item in result if signal in item.matched_scheduling_signals)
        for signal in _SIGNAL_ORDER
    }
    safeguard_counts = {
        safeguard: sum(1 for item in result if safeguard in item.present_safeguards)
        for safeguard in _SAFEGUARD_ORDER
    }
    risk_counts = {risk: sum(1 for item in result if item.risk_level == risk) for risk in _RISK_ORDER}
    return TaskScheduledJobConcurrencyReadinessPlan(
        plan_id=plan_id,
        readiness_records=result,
        scheduled_task_ids=tuple(item.task_id for item in result),
        summary={
            "task_count": len(tasks),
            "scheduled_task_count": len(result),
            "record_count": len(result),
            "high_risk_count": risk_counts["high"],
            "medium_risk_count": risk_counts["medium"],
            "low_risk_count": risk_counts["low"],
            "missing_safeguard_count": sum(len(item.missing_safeguards) for item in result),
            "signal_counts": signal_counts,
            "present_safeguard_counts": safeguard_counts,
            "scheduled_task_ids": [item.task_id for item in result],
        },
    )


def build_task_scheduled_job_concurrency_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskScheduledJobConcurrencyReadinessPlan:
    """Compatibility alias for building scheduled job concurrency readiness plans."""
    return build_task_scheduled_job_concurrency_readiness_plan(source)


def generate_task_scheduled_job_concurrency_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskScheduledJobConcurrencyReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskScheduledJobConcurrencyReadinessPlan:
    """Compatibility alias for generating scheduled job concurrency readiness plans."""
    if isinstance(source, TaskScheduledJobConcurrencyReadinessPlan):
        return source
    return build_task_scheduled_job_concurrency_readiness_plan(source)


def derive_task_scheduled_job_concurrency_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskScheduledJobConcurrencyReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskScheduledJobConcurrencyReadinessPlan:
    """Compatibility alias for deriving scheduled job concurrency readiness plans."""
    return generate_task_scheduled_job_concurrency_readiness_plan(source)


def summarize_task_scheduled_job_concurrency_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskScheduledJobConcurrencyReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic summary counts for scheduled job concurrency readiness."""
    return derive_task_scheduled_job_concurrency_readiness_plan(source).summary


def extract_task_scheduled_job_concurrency_readiness_records(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | TaskScheduledJobConcurrencyReadinessPlan
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[TaskScheduledJobConcurrencyReadinessRecord, ...]:
    """Return scheduled job concurrency readiness records from task-shaped input."""
    return derive_task_scheduled_job_concurrency_readiness_plan(source).readiness_records


def task_scheduled_job_concurrency_readiness_to_dict(
    plan: TaskScheduledJobConcurrencyReadinessPlan,
) -> dict[str, Any]:
    """Serialize a scheduled job concurrency readiness plan to a plain dictionary."""
    return plan.to_dict()


task_scheduled_job_concurrency_readiness_to_dict.__test__ = False


def task_scheduled_job_concurrency_readiness_to_dicts(
    records: (
        tuple[TaskScheduledJobConcurrencyReadinessRecord, ...]
        | list[TaskScheduledJobConcurrencyReadinessRecord]
        | TaskScheduledJobConcurrencyReadinessPlan
    ),
) -> list[dict[str, Any]]:
    """Serialize scheduled job concurrency readiness records to dictionaries."""
    if isinstance(records, TaskScheduledJobConcurrencyReadinessPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


task_scheduled_job_concurrency_readiness_to_dicts.__test__ = False


def task_scheduled_job_concurrency_readiness_to_markdown(
    plan: TaskScheduledJobConcurrencyReadinessPlan,
) -> str:
    """Render a scheduled job concurrency readiness plan as Markdown."""
    return plan.to_markdown()


task_scheduled_job_concurrency_readiness_to_markdown.__test__ = False


def _record(
    task: Mapping[str, Any],
    index: int,
    plan_context: tuple[tuple[str, str], ...],
) -> TaskScheduledJobConcurrencyReadinessRecord | None:
    signals, signal_evidence = _scheduling_signals(task, plan_context)
    if not signals:
        return None
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    present = _present_safeguards(task, plan_context)
    required = _required_safeguards(signals)
    missing = tuple(safeguard for safeguard in required if safeguard not in present)
    mutating = _mutates_data(task, plan_context)
    return TaskScheduledJobConcurrencyReadinessRecord(
        task_id=task_id,
        title=title,
        matched_scheduling_signals=signals,
        required_safeguards=required,
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in present),
        missing_safeguards=missing,
        risk_level=_risk_level(missing, mutating),
        recommended_checks=tuple(_RECOMMENDED_CHECKS[safeguard] for safeguard in missing),
        evidence=tuple(_dedupe(signal_evidence + _safeguard_evidence(task, plan_context))),
    )


def _scheduling_signals(
    task: Mapping[str, Any],
    plan_context: tuple[tuple[str, str], ...],
) -> tuple[tuple[ScheduledJobSignal, ...], list[str]]:
    signals: set[ScheduledJobSignal] = set()
    evidence: list[str] = []
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        path_text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal in _SIGNAL_ORDER:
            if _PATH_SIGNAL_PATTERNS[signal].search(normalized) or _SIGNAL_PATTERNS[signal].search(path_text):
                signals.add(signal)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")
    for source_field, text in (*_task_texts(task), *plan_context):
        matched = False
        for signal in _SIGNAL_ORDER:
            if _SIGNAL_PATTERNS[signal].search(text):
                signals.add(signal)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))
    return tuple(signal for signal in _SIGNAL_ORDER if signal in signals), evidence


def _present_safeguards(
    task: Mapping[str, Any],
    plan_context: tuple[tuple[str, str], ...],
) -> set[ScheduledJobSafeguard]:
    context = " ".join(text for _, text in (*_task_texts(task), *plan_context))
    return {safeguard for safeguard, pattern in _SAFEGUARD_PATTERNS.items() if pattern.search(context)}


def _safeguard_evidence(
    task: Mapping[str, Any],
    plan_context: tuple[tuple[str, str], ...],
) -> list[str]:
    evidence: list[str] = []
    for source_field, text in (*_task_texts(task), *plan_context):
        if any(pattern.search(text) for pattern in _SAFEGUARD_PATTERNS.values()):
            evidence.append(_evidence_snippet(source_field, text))
    return evidence


def _required_safeguards(signals: tuple[ScheduledJobSignal, ...]) -> tuple[ScheduledJobSafeguard, ...]:
    required: list[ScheduledJobSafeguard] = [
        "distributed_locking",
        "idempotency",
        "missed_run_recovery",
        "timeout_limits",
        "retry_policy",
        "observability",
    ]
    if set(signals) & {"recurring_sync", "batch_job", "nightly_job", "maintenance_task"}:
        required.append("backfill_controls")
    if set(signals) & {"cron_job", "scheduled_worker", "recurring_sync", "batch_job", "nightly_job"}:
        required.append("manual_replay")
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in set(required))


def _risk_level(
    missing: tuple[ScheduledJobSafeguard, ...],
    mutating: bool,
) -> ScheduledJobRiskLevel:
    missing_set = set(missing)
    if mutating and {"distributed_locking", "idempotency"} & missing_set:
        return "high"
    if "missed_run_recovery" in missing_set and ("retry_policy" in missing_set or "observability" in missing_set):
        return "medium"
    if len(missing) >= 4:
        return "medium"
    return "low"


def _mutates_data(task: Mapping[str, Any], plan_context: tuple[tuple[str, str], ...]) -> bool:
    context = " ".join(text for _, text in (*_task_texts(task), *plan_context))
    if _READ_ONLY_RE.search(context):
        return False
    return bool(_MUTATION_RE.search(context))


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, tuple[tuple[str, str], ...], list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, (), [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        payload = source.model_dump(mode="python")
        return _optional_text(payload.get("id")), tuple(_plan_context(payload)), [
            task.model_dump(mode="python") for task in source.tasks
        ]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), tuple(_plan_context(payload)), _task_payloads(payload.get("tasks"))
        return None, (), [dict(source)]
    if _looks_like_task(source):
        return None, (), [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), tuple(_plan_context(payload)), _task_payloads(payload.get("tasks"))
    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, (), []
    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return None, (), tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
        return _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _plan_context(plan: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("target_engine", "target_repo", "project_type", "test_strategy", "handoff_prompt", "risk"):
        if text := _optional_text(plan.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("milestones", "risks", "acceptance_criteria", "metadata", "implementation_brief", "brief"):
        texts.extend(_metadata_texts(plan.get(field_name), prefix=field_name))
    return texts


def _task_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "owner_role",
        "suggested_engine",
        "risk",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "risks",
        "depends_on",
        "dependencies",
        "tags",
        "labels",
        "notes",
        "validation_commands",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    texts.extend(_metadata_texts(task.get("metadata")))
    return tuple(texts)


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                if any(pattern.search(key_text) for pattern in (*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())):
                    texts.append((field, str(key)))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in (*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())):
                    texts.append((field, f"{key_text}: {text}"))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "owner_role",
        "suggested_engine",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "risks",
        "risk",
        "risk_level",
        "test_command",
        "validation_commands",
        "status",
        "metadata",
        "blocked_reason",
        "tags",
        "labels",
        "notes",
        "tasks",
        "milestones",
        "implementation_brief",
        "brief",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _normalized_path(value: str) -> str:
    return str(
        PurePosixPath(
            value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
        )
    )


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("\\", "\\\\").replace("|", "\\|").replace("\n", " ").strip()


__all__ = [
    "ScheduledJobRiskLevel",
    "ScheduledJobSafeguard",
    "ScheduledJobSignal",
    "TaskScheduledJobConcurrencyReadinessPlan",
    "TaskScheduledJobConcurrencyReadinessRecord",
    "build_task_scheduled_job_concurrency_readiness",
    "build_task_scheduled_job_concurrency_readiness_plan",
    "derive_task_scheduled_job_concurrency_readiness_plan",
    "extract_task_scheduled_job_concurrency_readiness_records",
    "generate_task_scheduled_job_concurrency_readiness_plan",
    "summarize_task_scheduled_job_concurrency_readiness",
    "task_scheduled_job_concurrency_readiness_to_dict",
    "task_scheduled_job_concurrency_readiness_to_dicts",
    "task_scheduled_job_concurrency_readiness_to_markdown",
]
