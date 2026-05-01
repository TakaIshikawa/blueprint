"""Plan idempotency safeguards for background job execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


BackgroundJobSignal = Literal[
    "queue",
    "worker",
    "cron",
    "webhook",
    "retry",
    "batch",
    "etl",
    "scheduler",
    "background_job",
]
IdempotencyEvidenceSignal = Literal[
    "idempotency",
    "dedupe",
    "replay",
    "checkpoint",
    "retry_safe",
    "dead_letter",
    "lock_ownership",
    "delivery_semantics",
]
IdempotencyReadiness = Literal["strong", "partial", "missing", "not_applicable"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: dict[BackgroundJobSignal, int] = {
    "queue": 0,
    "worker": 1,
    "cron": 2,
    "webhook": 3,
    "retry": 4,
    "batch": 5,
    "etl": 6,
    "scheduler": 7,
    "background_job": 8,
}
_SAFEGUARD_ORDER: dict[IdempotencyEvidenceSignal, int] = {
    "idempotency": 0,
    "dedupe": 1,
    "replay": 2,
    "checkpoint": 3,
    "retry_safe": 4,
    "dead_letter": 5,
    "lock_ownership": 6,
    "delivery_semantics": 7,
}
_READINESS_ORDER: dict[IdempotencyReadiness, int] = {
    "missing": 0,
    "partial": 1,
    "strong": 2,
    "not_applicable": 3,
}
_TEXT_SIGNAL_PATTERNS: dict[BackgroundJobSignal, re.Pattern[str]] = {
    "queue": re.compile(r"\b(?:queue|queued|enqueue|dequeue|sqs|rabbitmq|kafka|pub/sub|pubsub|broker)\b", re.I),
    "worker": re.compile(r"\b(?:worker|consumer|job processor|sidekiq|celery|resque|rq worker)\b", re.I),
    "cron": re.compile(r"\b(?:cron|crontab|scheduled job|nightly job|daily job|hourly job)\b", re.I),
    "webhook": re.compile(r"\b(?:webhook|web hook|callback delivery|event delivery|stripe event|github event)\b", re.I),
    "retry": re.compile(r"\b(?:retry|retries|retrying|backoff|exponential backoff|redelivery|re-delivery)\b", re.I),
    "batch": re.compile(r"\b(?:batch|bulk job|bulk import|bulk export|backfill|reindex|re-index)\b", re.I),
    "etl": re.compile(r"\b(?:etl|extract transform load|pipeline|data sync|data import|data export)\b", re.I),
    "scheduler": re.compile(r"\b(?:scheduler|schedule|scheduled task|agenda|airflow|temporal|durable timer)\b", re.I),
    "background_job": re.compile(r"\b(?:background job|async job|asynchronous job|job runner|job execution)\b", re.I),
}
_SAFEGUARD_PATTERNS: dict[IdempotencyEvidenceSignal, re.Pattern[str]] = {
    "idempotency": re.compile(r"\b(?:idempotent|idempotency|idempotency key|idempotent operation)\b", re.I),
    "dedupe": re.compile(r"\b(?:dedupe|dedup|de-dup|deduplicate|duplicate suppression|unique job|dedupe key)\b", re.I),
    "replay": re.compile(r"\b(?:replay|replayable|reprocess|re-run|rerun|event replay)\b", re.I),
    "checkpoint": re.compile(r"\b(?:checkpoint|cursor|watermark|resume token|offset|last processed)\b", re.I),
    "retry_safe": re.compile(r"\b(?:retry[- ]safe|safe to retry|side effects?|transactional outbox|outbox|upsert)\b", re.I),
    "dead_letter": re.compile(r"\b(?:dead[- ]letter|dlq|poison message|quarantine queue|failure queue)\b", re.I),
    "lock_ownership": re.compile(r"\b(?:lock ownership|lease|advisory lock|distributed lock|leader election|single owner)\b", re.I),
    "delivery_semantics": re.compile(r"\b(?:exactly[- ]once|at[- ]least[- ]once|at[- ]most[- ]once|delivery semantics)\b", re.I),
}


@dataclass(frozen=True, slots=True)
class TaskBackgroundJobIdempotencyRecord:
    """Idempotency readiness guidance for one background execution task."""

    task_id: str
    title: str
    readiness: IdempotencyReadiness
    detected_signals: tuple[BackgroundJobSignal, ...]
    safeguard_evidence: tuple[IdempotencyEvidenceSignal, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    open_questions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "readiness": self.readiness,
            "detected_signals": list(self.detected_signals),
            "safeguard_evidence": list(self.safeguard_evidence),
            "recommended_checks": list(self.recommended_checks),
            "open_questions": list(self.open_questions),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskBackgroundJobIdempotencyPlan:
    """Plan-level background job idempotency review."""

    plan_id: str | None = None
    records: tuple[TaskBackgroundJobIdempotencyRecord, ...] = field(default_factory=tuple)
    background_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "background_task_ids": list(self.background_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return idempotency records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the idempotency plan as deterministic Markdown."""
        title = "# Task Background Job Idempotency Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        counts = self.summary.get("readiness_counts", {})
        summary = (
            f"Summary: {self.summary.get('background_task_count', 0)} background tasks "
            f"(strong: {counts.get('strong', 0)}, partial: {counts.get('partial', 0)}, "
            f"missing: {counts.get('missing', 0)}, not_applicable: {counts.get('not_applicable', 0)})."
        )
        lines = [title, "", summary]
        if not self.records:
            lines.extend(["", "No background job idempotency records were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Readiness | Signals | Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.detected_signals))} | "
                f"{_markdown_cell(', '.join(record.safeguard_evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_task_background_job_idempotency_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskBackgroundJobIdempotencyPlan:
    """Build idempotency guidance for background execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    readiness_counts = {
        readiness: (
            len(not_applicable_task_ids)
            if readiness == "not_applicable"
            else sum(1 for record in records if record.readiness == readiness)
        )
        for readiness in _READINESS_ORDER
    }
    signal_counts = {
        signal: sum(1 for record in records if signal in record.detected_signals)
        for signal in _SIGNAL_ORDER
    }
    safeguard_counts = {
        safeguard: sum(1 for record in records if safeguard in record.safeguard_evidence)
        for safeguard in _SAFEGUARD_ORDER
    }
    return TaskBackgroundJobIdempotencyPlan(
        plan_id=plan_id,
        records=records,
        background_task_ids=tuple(record.task_id for record in records),
        not_applicable_task_ids=not_applicable_task_ids,
        summary={
            "task_count": len(tasks),
            "background_task_count": len(records),
            "not_applicable_task_count": len(not_applicable_task_ids),
            "readiness_counts": readiness_counts,
            "signal_counts": signal_counts,
            "safeguard_counts": safeguard_counts,
        },
    )


def analyze_task_background_job_idempotency(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskBackgroundJobIdempotencyPlan:
    """Compatibility alias for building background job idempotency plans."""
    return build_task_background_job_idempotency_plan(source)


def summarize_task_background_job_idempotency(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskBackgroundJobIdempotencyPlan:
    """Compatibility alias for building background job idempotency plans."""
    return build_task_background_job_idempotency_plan(source)


def summarize_task_background_job_idempotency_plans(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskBackgroundJobIdempotencyPlan:
    """Compatibility alias for building background job idempotency plans."""
    return build_task_background_job_idempotency_plan(source)


def task_background_job_idempotency_plan_to_dict(
    result: TaskBackgroundJobIdempotencyPlan,
) -> dict[str, Any]:
    """Serialize a background job idempotency plan to a plain dictionary."""
    return result.to_dict()


task_background_job_idempotency_plan_to_dict.__test__ = False


def task_background_job_idempotency_plan_to_markdown(
    result: TaskBackgroundJobIdempotencyPlan,
) -> str:
    """Render a background job idempotency plan as Markdown."""
    return result.to_markdown()


task_background_job_idempotency_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    background: tuple[BackgroundJobSignal, ...] = field(default_factory=tuple)
    safeguards: tuple[IdempotencyEvidenceSignal, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    validation_commands: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskBackgroundJobIdempotencyRecord | None:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    if not signals.background:
        return None
    readiness = _readiness(signals)
    return TaskBackgroundJobIdempotencyRecord(
        task_id=task_id,
        title=title,
        readiness=readiness,
        detected_signals=signals.background,
        safeguard_evidence=signals.safeguards,
        recommended_checks=_recommended_checks(signals.background, signals.safeguards, signals.validation_commands),
        open_questions=_open_questions(signals.background, signals.safeguards, readiness),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    background: set[BackgroundJobSignal] = set()
    safeguards: set[IdempotencyEvidenceSignal] = set()
    evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_background, path_safeguards = _path_signals(normalized)
        if path_background or path_safeguards:
            background.update(path_background)
            safeguards.update(path_safeguards)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched = False
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(text):
                background.add(signal)
                matched = True
        for signal, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguards.add(signal)
                matched = True
        if matched:
            evidence.append(snippet)

    validation_commands = tuple(_validation_commands(task))
    for command in validation_commands:
        snippet = _evidence_snippet("validation_commands", command)
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(command) or pattern.search(command_text):
                background.add(signal)
                matched = True
        for signal, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(command) or pattern.search(command_text):
                safeguards.add(signal)
                matched = True
        if matched:
            evidence.append(snippet)

    return _Signals(
        background=tuple(signal for signal in _SIGNAL_ORDER if signal in background),
        safeguards=tuple(signal for signal in _SAFEGUARD_ORDER if signal in safeguards),
        evidence=tuple(_dedupe(evidence)),
        validation_commands=validation_commands,
    )


def _path_signals(path: str) -> tuple[set[BackgroundJobSignal], set[IdempotencyEvidenceSignal]]:
    normalized = path.casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    name = posix.name
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    background: set[BackgroundJobSignal] = set()
    safeguards: set[IdempotencyEvidenceSignal] = set()
    if {"queue", "queues", "jobs", "job", "sidekiq", "celery", "resque"} & parts:
        background.add("queue")
    if {"worker", "workers", "consumer", "consumers"} & parts or "worker" in name:
        background.add("worker")
    if {"cron", "crons", "crontab"} & parts or "cron" in name:
        background.add("cron")
    if {"webhook", "webhooks"} & parts or "webhook" in name:
        background.add("webhook")
    if {"batch", "batches", "backfill", "backfills"} & parts:
        background.add("batch")
    if {"etl", "pipeline", "pipelines"} & parts:
        background.add("etl")
    if {"scheduler", "schedulers", "schedule"} & parts:
        background.add("scheduler")
    if any(token in text for token in ("background job", "async job", "job runner")):
        background.add("background_job")
    if any(token in text for token in ("retry", "backoff", "redelivery")):
        background.add("retry")
    if any(token in text for token in ("idempot", "dedupe", "dedup", "replay", "checkpoint", "dlq", "dead letter", "lease", "lock")):
        for signal, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguards.add(signal)
    return background, safeguards


def _readiness(signals: _Signals) -> IdempotencyReadiness:
    safeguard_set = set(signals.safeguards)
    if not signals.background:
        return "not_applicable"
    if {"idempotency", "dedupe"} & safeguard_set and (
        {"replay", "checkpoint", "retry_safe", "dead_letter", "lock_ownership", "delivery_semantics"} & safeguard_set
        or signals.validation_commands
    ):
        return "strong"
    if safeguard_set or signals.validation_commands:
        return "partial"
    return "missing"


def _recommended_checks(
    background: tuple[BackgroundJobSignal, ...],
    safeguards: tuple[IdempotencyEvidenceSignal, ...],
    validation_commands: tuple[str, ...],
) -> tuple[str, ...]:
    signal_set = set(background)
    safeguard_set = set(safeguards)
    checks: list[str] = []
    if "idempotency" not in safeguard_set and "dedupe" not in safeguard_set:
        checks.append("Define an idempotency or dedupe key for every externally triggered job execution.")
    if "retry" in signal_set and "retry_safe" not in safeguard_set:
        checks.append("Document retry-safe side effects, backoff behavior, and which operations can be repeated.")
    if {"queue", "worker", "webhook"} & signal_set and "dead_letter" not in safeguard_set:
        checks.append("Add dead-letter or poison-message handling with owner-visible recovery steps.")
    if {"batch", "etl"} & signal_set and "checkpoint" not in safeguard_set:
        checks.append("Add batch checkpointing with cursor, watermark, or offset resume behavior.")
    if {"cron", "scheduler"} & signal_set and "lock_ownership" not in safeguard_set:
        checks.append("Add lock ownership, lease expiry, or single-runner guarantees for scheduled execution.")
    if "replay" not in safeguard_set:
        checks.append("Add replay tests that prove duplicate delivery and reprocessing do not double-apply side effects.")
    if "delivery_semantics" not in safeguard_set:
        checks.append("Document exactly-once, at-least-once, or at-most-once delivery expectations and failure behavior.")
    if validation_commands:
        checks.append("Run the detected validation commands against duplicate, retry, and replay scenarios.")
    return tuple(_dedupe(checks))


def _open_questions(
    background: tuple[BackgroundJobSignal, ...],
    safeguards: tuple[IdempotencyEvidenceSignal, ...],
    readiness: IdempotencyReadiness,
) -> tuple[str, ...]:
    questions = [
        "What unique business key prevents duplicate job side effects?",
        "Where are retry attempts, terminal failures, and manual replays observed?",
    ]
    signal_set = set(background)
    safeguard_set = set(safeguards)
    if readiness == "missing":
        questions.append("Which idempotency, dedupe, checkpoint, or replay safeguard will block implementation until specified?")
    if "webhook" in signal_set:
        questions.append("Which provider event id or signature timestamp is persisted for webhook dedupe?")
    if {"batch", "etl"} & signal_set:
        questions.append("Which checkpoint is authoritative when a batch partially succeeds?")
    if {"cron", "scheduler"} & signal_set and "lock_ownership" not in safeguard_set:
        questions.append("What prevents overlapping scheduled runs across deploys or multiple workers?")
    return tuple(_dedupe(questions))


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, []

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
    return None, tasks


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
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "test_commands",
        "validation_command",
        "validation_commands",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("depends_on", "files_or_modules", "files", "acceptance_criteria", "tags", "labels", "notes"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _any_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _any_signal(key_text):
                texts.append((field, str(key)))
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


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in (*_TEXT_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()))


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        if value := task.get(key):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
        if isinstance(metadata, Mapping) and (value := metadata.get(key)):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


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
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "BackgroundJobSignal",
    "IdempotencyEvidenceSignal",
    "IdempotencyReadiness",
    "TaskBackgroundJobIdempotencyPlan",
    "TaskBackgroundJobIdempotencyRecord",
    "analyze_task_background_job_idempotency",
    "build_task_background_job_idempotency_plan",
    "summarize_task_background_job_idempotency",
    "summarize_task_background_job_idempotency_plans",
    "task_background_job_idempotency_plan_to_dict",
    "task_background_job_idempotency_plan_to_markdown",
]
