"""Plan retry-readiness safeguards for background job execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


BackgroundJobRetrySignal = Literal[
    "queue",
    "worker",
    "cron",
    "background_job",
    "retry",
    "dead_letter_queue",
    "duplicate_execution",
    "timeout",
]
BackgroundJobRetryCategory = Literal[
    "async_job",
    "queue_worker",
    "scheduled_job",
    "retry_policy",
    "dead_letter_queue",
    "duplicate_execution",
    "timeout",
]
BackgroundJobRetrySafeguard = Literal[
    "idempotent_job_handler",
    "retry_limit",
    "exponential_backoff",
    "dead_letter_monitoring",
    "timeout_budget",
    "duplicate_suppression",
]
BackgroundJobRetryRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[BackgroundJobRetrySignal, ...] = (
    "queue",
    "worker",
    "cron",
    "background_job",
    "retry",
    "dead_letter_queue",
    "duplicate_execution",
    "timeout",
)
_CATEGORY_ORDER: tuple[BackgroundJobRetryCategory, ...] = (
    "async_job",
    "queue_worker",
    "scheduled_job",
    "retry_policy",
    "dead_letter_queue",
    "duplicate_execution",
    "timeout",
)
_SAFEGUARD_ORDER: tuple[BackgroundJobRetrySafeguard, ...] = (
    "idempotent_job_handler",
    "retry_limit",
    "exponential_backoff",
    "dead_letter_monitoring",
    "timeout_budget",
    "duplicate_suppression",
)
_RISK_ORDER: dict[BackgroundJobRetryRisk, int] = {"high": 0, "medium": 1, "low": 2}

_TEXT_SIGNAL_PATTERNS: dict[BackgroundJobRetrySignal, re.Pattern[str]] = {
    "queue": re.compile(
        r"\b(?:queue(?:s|d)?|enqueue|dequeue|job queue|task queue|message queue|sqs|"
        r"rabbitmq|amqp|pub/sub|pubsub|kafka|redis queue)\b",
        re.I,
    ),
    "worker": re.compile(
        r"\b(?:worker(?:s)?|consumer(?:s)?|job processor(?:s)?|job runner|sidekiq|"
        r"celery|resque|rq worker|task processor)\b",
        re.I,
    ),
    "cron": re.compile(
        r"\b(?:cron|crontab|scheduled job(?:s)?|scheduled task(?:s)?|scheduler|nightly job|"
        r"daily job|hourly job)\b",
        re.I,
    ),
    "background_job": re.compile(
        r"\b(?:background job(?:s)?|async job(?:s)?|asynchronous work|asynchronous task(?:s)?|"
        r"background processing|background worker)\b",
        re.I,
    ),
    "retry": re.compile(
        r"\b(?:retry(?:ing|ied|ies)?|retries|retry policy|retry loop|redeliver(?:y|ed)?|"
        r"re-deliver(?:y|ed)?|attempt(?:s)?|backoff)\b",
        re.I,
    ),
    "dead_letter_queue": re.compile(
        r"\b(?:dead[- ]?letter queue(?:s)?|dead[- ]?letter topic(?:s)?|dlq|poison message(?:s)?|"
        r"failed message queue|failure queue)\b",
        re.I,
    ),
    "duplicate_execution": re.compile(
        r"\b(?:duplicate.{0,40}execution|duplicate job(?:s)?|duplicate delivery|double run|double-run|"
        r"run twice|processed twice|double process(?:ed|ing)?|dedupe|deduplication|exactly[- ]?once|"
        r"at[- ]least[- ]once|idempot(?:ent|ency))\b",
        re.I,
    ),
    "timeout": re.compile(
        r"\b(?:timeout(?:s)?|time[- ]?out|deadline(?:s)?|ttl|time budget|execution budget|"
        r"stuck job(?:s)?|hung worker(?:s)?)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[BackgroundJobRetrySafeguard, re.Pattern[str]] = {
    "idempotent_job_handler": re.compile(
        r"\b(?:idempotent(?: job| handler| operation)?|idempotency key|safe to retry|"
        r"retry[- ]safe|transactional outbox|upsert)\b",
        re.I,
    ),
    "retry_limit": re.compile(
        r"\b(?:max(?:imum)? retries|max(?:imum)? retry attempts?|retry limit|attempt limit|"
        r"bounded retries|capped retries|cap retries|stop retrying|terminal failure|no infinite retries)\b",
        re.I,
    ),
    "exponential_backoff": re.compile(
        r"\b(?:exponential backoff|backoff with jitter|jitter(?:ed)? backoff|retry backoff|"
        r"progressive delay|decorrelated jitter)\b",
        re.I,
    ),
    "dead_letter_monitoring": re.compile(
        r"\b(?:(?:dlq|dead[- ]?letter|failure queue).{0,80}(?:alerts?|monitor(?:ing)?|alarms?|depth|owner|page)|"
        r"(?:alerts?|monitor(?:ing)?|alarms?|page).{0,80}(?:dlq|dead[- ]?letter|failure queue|failed message))\b",
        re.I,
    ),
    "timeout_budget": re.compile(
        r"\b(?:timeout budget|deadline budget|execution deadline|job timeout|worker timeout|"
        r"per[- ]?attempt timeout|overall timeout|time boxed|time-boxed|cancellation deadline)\b",
        re.I,
    ),
    "duplicate_suppression": re.compile(
        r"\b(?:duplicate suppression|dedupe|deduplication|dedupe key|unique job|unique constraint|"
        r"single flight|single-flight|exactly[- ]?once|idempotency key)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[BackgroundJobRetrySafeguard, str] = {
    "idempotent_job_handler": "Verify the job handler is idempotent before retries or manual replays can repeat side effects.",
    "retry_limit": "Set bounded retry attempts with a terminal failure state so failures cannot loop forever.",
    "exponential_backoff": "Use exponential backoff with jitter to avoid retry storms after downstream failures.",
    "dead_letter_monitoring": "Add DLQ or failure-queue monitoring with owner-visible alerts and recovery steps.",
    "timeout_budget": "Define per-attempt and overall timeout budgets, including cancellation behavior for stuck jobs.",
    "duplicate_suppression": "Prove duplicate suppression with a stable idempotency key, unique job key, or dedupe store.",
}


@dataclass(frozen=True, slots=True)
class TaskBackgroundJobRetryReadinessRecord:
    """Retry-readiness guidance for one background execution task."""

    task_id: str
    title: str
    matched_signals: tuple[BackgroundJobRetrySignal, ...]
    categories: tuple[BackgroundJobRetryCategory, ...]
    present_safeguards: tuple[BackgroundJobRetrySafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[BackgroundJobRetrySafeguard, ...] = field(default_factory=tuple)
    risk_level: BackgroundJobRetryRisk = "medium"
    recommendations: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def recommended_checks(self) -> tuple[str, ...]:
        """Compatibility view for planners that name recommendations checks."""
        return self.recommendations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "matched_signals": list(self.matched_signals),
            "categories": list(self.categories),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "recommendations": list(self.recommendations),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskBackgroundJobRetryReadinessPlan:
    """Plan-level retry-readiness review for background execution tasks."""

    plan_id: str | None = None
    records: tuple[TaskBackgroundJobRetryReadinessRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskBackgroundJobRetryReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskBackgroundJobRetryReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return retry-readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render retry-readiness guidance as deterministic Markdown."""
        title = "# Task Background Job Retry Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        category_counts = self.summary.get("category_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Impacted task count: {self.summary.get('impacted_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: "
            + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
            "- Category counts: "
            + ", ".join(
                f"{category} {category_counts.get(category, 0)}"
                for category in _CATEGORY_ORDER
            ),
        ]
        if not self.records:
            lines.extend(["", "No task background job retry-readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(
                    ["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Signals | Categories | Present Safeguards | Missing Safeguards | Recommendations | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.matched_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.categories) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommendations) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_background_job_retry_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskBackgroundJobRetryReadinessPlan:
    """Build retry-readiness records for background execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskBackgroundJobRetryReadinessPlan(
        plan_id=plan_id,
        records=records,
        impacted_task_ids=tuple(record.task_id for record in records),
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_background_job_retry_readiness(source: Any) -> TaskBackgroundJobRetryReadinessPlan:
    """Compatibility alias for building background job retry-readiness plans."""
    return build_task_background_job_retry_readiness_plan(source)


def summarize_task_background_job_retry_readiness(source: Any) -> TaskBackgroundJobRetryReadinessPlan:
    """Compatibility alias for building background job retry-readiness plans."""
    return build_task_background_job_retry_readiness_plan(source)


def extract_task_background_job_retry_readiness(source: Any) -> TaskBackgroundJobRetryReadinessPlan:
    """Compatibility alias for extracting background job retry-readiness plans."""
    return build_task_background_job_retry_readiness_plan(source)


def generate_task_background_job_retry_readiness(source: Any) -> TaskBackgroundJobRetryReadinessPlan:
    """Compatibility alias for generating background job retry-readiness plans."""
    return build_task_background_job_retry_readiness_plan(source)


def derive_task_background_job_retry_readiness(source: Any) -> TaskBackgroundJobRetryReadinessPlan:
    """Compatibility alias for deriving background job retry-readiness plans."""
    return build_task_background_job_retry_readiness_plan(source)


def recommend_task_background_job_retry_readiness(source: Any) -> TaskBackgroundJobRetryReadinessPlan:
    """Compatibility alias for recommending background job retry safeguards."""
    return build_task_background_job_retry_readiness_plan(source)


def task_background_job_retry_readiness_plan_to_dict(
    result: TaskBackgroundJobRetryReadinessPlan,
) -> dict[str, Any]:
    """Serialize a background job retry-readiness plan to a plain dictionary."""
    return result.to_dict()


task_background_job_retry_readiness_plan_to_dict.__test__ = False


def task_background_job_retry_readiness_plan_to_dicts(
    result: TaskBackgroundJobRetryReadinessPlan
    | Iterable[TaskBackgroundJobRetryReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize background job retry-readiness records to plain dictionaries."""
    if isinstance(result, TaskBackgroundJobRetryReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_background_job_retry_readiness_plan_to_dicts.__test__ = False


def task_background_job_retry_readiness_plan_to_markdown(
    result: TaskBackgroundJobRetryReadinessPlan,
) -> str:
    """Render a background job retry-readiness plan as Markdown."""
    return result.to_markdown()


task_background_job_retry_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[BackgroundJobRetrySignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[BackgroundJobRetrySafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskBackgroundJobRetryReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None

    categories = _categories(signals.signals)
    required = _required_safeguards(categories)
    missing = tuple(safeguard for safeguard in required if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskBackgroundJobRetryReadinessRecord(
        task_id=task_id,
        title=title,
        matched_signals=signals.signals,
        categories=categories,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(categories, signals.present_safeguards, missing),
        recommendations=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[BackgroundJobRetrySignal] = set()
    safeguard_hits: set[BackgroundJobRetrySafeguard] = set()
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_signals = _path_signals(normalized)
        if path_signals:
            signal_hits.update(path_signals)
            signal_evidence.append(f"files_or_modules: {path}")
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *(_validation_command_texts(task))]:
        snippet = _evidence_snippet(source_field, text)
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_signal = False
        matched_safeguard = False
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched_signal = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched_safeguard = True
        if matched_signal:
            signal_evidence.append(snippet)
        if matched_safeguard:
            safeguard_evidence.append(snippet)

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_signals(path: str) -> set[BackgroundJobRetrySignal]:
    normalized = path.casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    name = posix.name
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals: set[BackgroundJobRetrySignal] = set()
    if {"queue", "queues", "jobs", "job", "tasks", "task", "sqs", "rabbitmq"} & parts:
        signals.add("queue")
    if {"worker", "workers", "consumer", "consumers", "processor", "processors"} & parts or "worker" in name:
        signals.add("worker")
    if {"cron", "crons", "crontab", "scheduler", "schedulers"} & parts or any(token in name for token in ("cron", "scheduler")):
        signals.add("cron")
    if any(token in text for token in ("background job", "async job", "job runner")):
        signals.add("background_job")
    if any(token in text for token in ("retry", "retries", "backoff", "redelivery")):
        signals.add("retry")
    if any(token in text for token in ("dead letter", "deadletter", "dlq", "failure queue")):
        signals.add("dead_letter_queue")
    if any(token in text for token in ("duplicate", "dedupe", "dedup", "idempot", "exactly once")):
        signals.add("duplicate_execution")
    if any(token in text for token in ("timeout", "deadline", "ttl")):
        signals.add("timeout")
    return signals


def _categories(signals: tuple[BackgroundJobRetrySignal, ...]) -> tuple[BackgroundJobRetryCategory, ...]:
    signal_set = set(signals)
    categories: set[BackgroundJobRetryCategory] = set()
    if {"queue", "worker", "background_job"} & signal_set:
        categories.add("async_job")
    if {"queue", "worker"} & signal_set:
        categories.add("queue_worker")
    if "cron" in signal_set:
        categories.add("scheduled_job")
    if "retry" in signal_set:
        categories.add("retry_policy")
    if "dead_letter_queue" in signal_set:
        categories.add("dead_letter_queue")
    if "duplicate_execution" in signal_set:
        categories.add("duplicate_execution")
    if "timeout" in signal_set:
        categories.add("timeout")
    return tuple(category for category in _CATEGORY_ORDER if category in categories)


def _required_safeguards(
    categories: tuple[BackgroundJobRetryCategory, ...],
) -> tuple[BackgroundJobRetrySafeguard, ...]:
    category_set = set(categories)
    required: set[BackgroundJobRetrySafeguard] = set()
    if {"async_job", "queue_worker", "scheduled_job", "retry_policy", "duplicate_execution"} & category_set:
        required.add("idempotent_job_handler")
    if "retry_policy" in category_set:
        required.update({"retry_limit", "exponential_backoff"})
    if {"queue_worker", "retry_policy", "dead_letter_queue"} & category_set:
        required.add("dead_letter_monitoring")
    if {"async_job", "scheduled_job", "retry_policy", "timeout"} & category_set:
        required.add("timeout_budget")
    if {"queue_worker", "retry_policy", "duplicate_execution"} & category_set:
        required.add("duplicate_suppression")
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required)


def _risk_level(
    categories: tuple[BackgroundJobRetryCategory, ...],
    present: tuple[BackgroundJobRetrySafeguard, ...],
    missing: tuple[BackgroundJobRetrySafeguard, ...],
) -> BackgroundJobRetryRisk:
    if not missing:
        return "low"
    category_set = set(categories)
    present_set = set(present)
    missing_set = set(missing)
    has_retry_or_duplicate = bool({"retry_policy", "duplicate_execution"} & category_set)
    lacks_idempotency = "idempotent_job_handler" not in present_set and "duplicate_suppression" not in present_set
    lacks_retry_limit = "retry_policy" in category_set and "retry_limit" in missing_set
    if has_retry_or_duplicate and (lacks_idempotency or lacks_retry_limit):
        return "high"
    if len(missing) >= 4:
        return "high"
    return "medium"


def _summary(
    records: tuple[TaskBackgroundJobRetryReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "impacted_task_count": len(records),
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.matched_signals)
            for signal in _SIGNAL_ORDER
        },
        "category_counts": {
            category: sum(1 for record in records if category in record.categories)
            for category in _CATEGORY_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


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
        if task := _task_payload(item):
            tasks.append(task)
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
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
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
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_command",
        "test_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "depends_on",
    ):
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
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
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


def _metadata_key_is_signal(value: str) -> bool:
    return any(pattern.search(value) for pattern in [*_TEXT_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


def _validation_command_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value))
            else:
                commands.extend(_strings(metadata_value))
    return tuple(("validation_commands", command) for command in _dedupe(commands))


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
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "BackgroundJobRetryCategory",
    "BackgroundJobRetryRisk",
    "BackgroundJobRetrySafeguard",
    "BackgroundJobRetrySignal",
    "TaskBackgroundJobRetryReadinessPlan",
    "TaskBackgroundJobRetryReadinessRecord",
    "analyze_task_background_job_retry_readiness",
    "build_task_background_job_retry_readiness_plan",
    "derive_task_background_job_retry_readiness",
    "extract_task_background_job_retry_readiness",
    "generate_task_background_job_retry_readiness",
    "recommend_task_background_job_retry_readiness",
    "summarize_task_background_job_retry_readiness",
    "task_background_job_retry_readiness_plan_to_dict",
    "task_background_job_retry_readiness_plan_to_dicts",
    "task_background_job_retry_readiness_plan_to_markdown",
]
