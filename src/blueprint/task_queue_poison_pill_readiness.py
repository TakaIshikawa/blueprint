"""Plan poison-pill readiness safeguards for queue and stream execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


QueuePoisonPillSignal = Literal[
    "queue_consumer",
    "event_stream",
    "retry_loop",
    "dead_letter_queue",
    "idempotent_consumer",
    "malformed_payload_handling",
]
QueuePoisonPillSafeguard = Literal[
    "dlq_routing",
    "max_retry_limits",
    "alerting",
    "replay_tooling",
    "payload_validation",
    "manual_quarantine_ownership",
]
QueuePoisonPillRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[QueuePoisonPillSignal, ...] = (
    "queue_consumer",
    "event_stream",
    "retry_loop",
    "dead_letter_queue",
    "idempotent_consumer",
    "malformed_payload_handling",
)
_SAFEGUARD_ORDER: tuple[QueuePoisonPillSafeguard, ...] = (
    "dlq_routing",
    "max_retry_limits",
    "alerting",
    "replay_tooling",
    "payload_validation",
    "manual_quarantine_ownership",
)
_RISK_ORDER: dict[QueuePoisonPillRisk, int] = {"high": 0, "medium": 1, "low": 2}
_PATH_SIGNAL_PATTERNS: dict[QueuePoisonPillSignal, re.Pattern[str]] = {
    "queue_consumer": re.compile(
        r"(?:queue|worker|consumer|subscriber|handler|sqs|celery|sidekiq|resque|rabbitmq|amqp|pubsub|pubsub)",
        re.I,
    ),
    "event_stream": re.compile(
        r"(?:event[-_]?stream|stream[-_]?consumer|kafka|kinesis|pulsar|nats|redis[-_]?stream|eventhub)",
        re.I,
    ),
    "retry_loop": re.compile(r"(?:retry|retries|backoff|attempts?)", re.I),
    "dead_letter_queue": re.compile(r"(?:dead[-_]?letter|dlq|poison[-_]?pill)", re.I),
    "idempotent_consumer": re.compile(r"(?:idempotent|dedupe|dedup|exactly[-_]?once)", re.I),
    "malformed_payload_handling": re.compile(
        r"(?:malformed|invalid[-_]?payload|schema[-_]?validation|payload[-_]?validation|parse[-_]?error)",
        re.I,
    ),
}
_TEXT_SIGNAL_PATTERNS: dict[QueuePoisonPillSignal, re.Pattern[str]] = {
    "queue_consumer": re.compile(
        r"\b(?:queue(?:s|d)?|message queue|job queue|task queue|worker(?:s)?|consumer(?:s)?|"
        r"subscriber(?:s)?|subscription handler|sqs|celery|sidekiq|resque|rabbitmq|amqp|pub/sub|pubsub)\b",
        re.I,
    ),
    "event_stream": re.compile(
        r"\b(?:event stream(?:s)?|stream consumer(?:s)?|stream processor(?:s)?|kafka|kinesis|"
        r"pulsar|nats|redis stream(?:s)?|event hub(?:s)?|eventhub)\b",
        re.I,
    ),
    "retry_loop": re.compile(
        r"\b(?:retry(?:ing|ied|ies)?|retries|retry loop(?:s)?|retry policy|backoff|attempt(?:s)?|"
        r"poison message loop(?:s)?|infinite retry)\b",
        re.I,
    ),
    "dead_letter_queue": re.compile(
        r"\b(?:dead[- ]?letter queue(?:s)?|dead letter topic(?:s)?|dlq|poison[- ]?pill(?:s)?|"
        r"poison message(?:s)?|failed message queue)\b",
        re.I,
    ),
    "idempotent_consumer": re.compile(
        r"\b(?:idempotent consumer(?:s)?|idempotency|idempotent handling|dedupe|deduplication|"
        r"exactly[- ]?once|duplicate message(?:s)?)\b",
        re.I,
    ),
    "malformed_payload_handling": re.compile(
        r"\b(?:malformed payload(?:s)?|invalid payload(?:s)?|bad message(?:s)?|unparseable message(?:s)?|"
        r"schema validation|payload validation|parse error(?:s)?|deserialization error(?:s)?)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[QueuePoisonPillSafeguard, re.Pattern[str]] = {
    "dlq_routing": re.compile(
        r"\b(?:(?:route|routing|move|send|publish|park).{0,70}(?:dlq|dead[- ]?letter|failed message)|"
        r"(?:dlq|dead[- ]?letter).{0,70}(?:routing|route|receives?|publish|park))\b",
        re.I,
    ),
    "max_retry_limits": re.compile(
        r"\b(?:max(?:imum)? retries|max(?:imum)? retry attempts?|retry limit|attempt limit|"
        r"bounded retries|cap(?:ped)? retries|stop retrying|no infinite retries)\b",
        re.I,
    ),
    "alerting": re.compile(
        r"\b(?:alert(?:s|ing)?|page(?:s|d|r)?|on[- ]?call|alarm(?:s)?|monitor(?:ing)?|"
        r"dlq depth|queue depth|retry spike|failure rate)\b",
        re.I,
    ),
    "replay_tooling": re.compile(
        r"\b(?:replay(?:s|ed|ing)?|reprocess(?:es|ed|ing)?|redrive|backfill|rerun failed|"
        r"replay tooling|replay runbook|safe replay)\b",
        re.I,
    ),
    "payload_validation": re.compile(
        r"\b(?:payload validation|schema validation|validate payload(?:s)?|message schema|contract validation|"
        r"reject invalid payload(?:s)?|deserialization guard(?:s)?|parse error handling)\b",
        re.I,
    ),
    "manual_quarantine_ownership": re.compile(
        r"\b(?:manual quarantine|quarantine owner(?:ship)?|quarantine queue|triage owner(?:ship)?|"
        r"manual triage|ops owner|support owner|runbook owner|ownership for quarantined)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[QueuePoisonPillSafeguard, str] = {
    "dlq_routing": "Verify unrecoverable messages route to a DLQ or failed-message topic without blocking healthy work.",
    "max_retry_limits": "Assert retry loops have bounded max attempts, backoff behavior, and a terminal failure state.",
    "alerting": "Add alerts for DLQ depth, retry spikes, consumer stalls, and sustained message failure rates.",
    "replay_tooling": "Document and test replay, redrive, or reprocessing tooling with idempotency expectations.",
    "payload_validation": "Validate malformed or incompatible payloads fail fast before side effects are applied.",
    "manual_quarantine_ownership": "Assign ownership and runbook steps for manually quarantining, inspecting, and resolving poison messages.",
}


@dataclass(frozen=True, slots=True)
class TaskQueuePoisonPillReadinessRecord:
    """Readiness guidance for one task touching queue poison-pill behavior."""

    task_id: str
    title: str
    matched_signals: tuple[QueuePoisonPillSignal, ...]
    present_safeguards: tuple[QueuePoisonPillSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[QueuePoisonPillSafeguard, ...] = field(default_factory=tuple)
    risk_level: QueuePoisonPillRisk = "medium"
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "matched_signals": list(self.matched_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskQueuePoisonPillReadinessPlan:
    """Plan-level poison-pill readiness review for queue and stream tasks."""

    plan_id: str | None = None
    records: tuple[TaskQueuePoisonPillReadinessRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskQueuePoisonPillReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskQueuePoisonPillReadinessRecord, ...]:
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
        """Return poison-pill readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render poison-pill readiness as deterministic Markdown."""
        title = "# Task Queue Poison Pill Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
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
            "- Signal counts: "
            + ", ".join(
                f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER
            ),
        ]
        if not self.records:
            lines.extend(["", "No task queue poison-pill readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(
                    ["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.matched_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_queue_poison_pill_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskQueuePoisonPillReadinessPlan:
    """Build poison-pill readiness records for execution tasks."""
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
    return TaskQueuePoisonPillReadinessPlan(
        plan_id=plan_id,
        records=records,
        impacted_task_ids=tuple(record.task_id for record in records),
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_queue_poison_pill_readiness(source: Any) -> TaskQueuePoisonPillReadinessPlan:
    """Compatibility alias for building queue poison-pill readiness plans."""
    return build_task_queue_poison_pill_readiness_plan(source)


def summarize_task_queue_poison_pill_readiness(source: Any) -> TaskQueuePoisonPillReadinessPlan:
    """Compatibility alias for building queue poison-pill readiness plans."""
    return build_task_queue_poison_pill_readiness_plan(source)


def extract_task_queue_poison_pill_readiness(source: Any) -> TaskQueuePoisonPillReadinessPlan:
    """Compatibility alias for building queue poison-pill readiness plans."""
    return build_task_queue_poison_pill_readiness_plan(source)


def generate_task_queue_poison_pill_readiness(source: Any) -> TaskQueuePoisonPillReadinessPlan:
    """Compatibility alias for generating queue poison-pill readiness plans."""
    return build_task_queue_poison_pill_readiness_plan(source)


def recommend_task_queue_poison_pill_readiness(source: Any) -> TaskQueuePoisonPillReadinessPlan:
    """Compatibility alias for recommending queue poison-pill safeguards."""
    return build_task_queue_poison_pill_readiness_plan(source)


def task_queue_poison_pill_readiness_plan_to_dict(
    result: TaskQueuePoisonPillReadinessPlan,
) -> dict[str, Any]:
    """Serialize a queue poison-pill readiness plan to a plain dictionary."""
    return result.to_dict()


task_queue_poison_pill_readiness_plan_to_dict.__test__ = False


def task_queue_poison_pill_readiness_plan_to_dicts(
    result: TaskQueuePoisonPillReadinessPlan | Iterable[TaskQueuePoisonPillReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize queue poison-pill readiness records to plain dictionaries."""
    if isinstance(result, TaskQueuePoisonPillReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_queue_poison_pill_readiness_plan_to_dicts.__test__ = False


def task_queue_poison_pill_readiness_plan_to_markdown(
    result: TaskQueuePoisonPillReadinessPlan,
) -> str:
    """Render a queue poison-pill readiness plan as Markdown."""
    return result.to_markdown()


task_queue_poison_pill_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[QueuePoisonPillSignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[QueuePoisonPillSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskQueuePoisonPillReadinessRecord | None:
    signals = _signals(task)
    if not _is_impacted(signals.signals):
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskQueuePoisonPillReadinessRecord(
        task_id=task_id,
        title=title,
        matched_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(signals.signals, missing),
        recommended_checks=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[QueuePoisonPillSignal] = set()
    safeguard_hits: set[QueuePoisonPillSafeguard] = set()
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

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_signal = False
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(text):
                signal_hits.add(signal)
                matched_signal = True
        if matched_signal:
            signal_evidence.append(snippet)
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(snippet)

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_signals(path: str) -> set[QueuePoisonPillSignal]:
    normalized = path.casefold()
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals = {
        signal
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items()
        if pattern.search(normalized) or pattern.search(text)
    }
    name = PurePosixPath(normalized).name
    if name in {"consumer.py", "worker.py", "queue.py", "subscriber.py"}:
        signals.add("queue_consumer")
    if name in {"stream_consumer.py", "event_stream.py"}:
        signals.add("event_stream")
    if {"dead_letter_queue", "retry_loop"} & signals:
        signals.add("queue_consumer")
    return signals


def _is_impacted(signals: tuple[QueuePoisonPillSignal, ...]) -> bool:
    return bool(
        {"queue_consumer", "event_stream", "retry_loop", "dead_letter_queue"} & set(signals)
    )


def _risk_level(
    signals: tuple[QueuePoisonPillSignal, ...],
    missing: tuple[QueuePoisonPillSafeguard, ...],
) -> QueuePoisonPillRisk:
    if not missing:
        return "low"
    signal_set = set(signals)
    missing_set = set(missing)
    if len(missing) >= 4:
        return "high"
    if {"retry_loop", "dead_letter_queue"} & signal_set and {
        "dlq_routing",
        "max_retry_limits",
    } & missing_set:
        return "high"
    if "malformed_payload_handling" in signal_set and "payload_validation" in missing_set:
        return "high"
    if "manual_quarantine_ownership" in missing_set and "dead_letter_queue" in signal_set:
        return "high"
    return "medium"


def _summary(
    records: tuple[TaskQueuePoisonPillReadinessRecord, ...],
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
        "validation_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
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
        "validation_commands",
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
    "QueuePoisonPillRisk",
    "QueuePoisonPillSafeguard",
    "QueuePoisonPillSignal",
    "TaskQueuePoisonPillReadinessPlan",
    "TaskQueuePoisonPillReadinessRecord",
    "analyze_task_queue_poison_pill_readiness",
    "build_task_queue_poison_pill_readiness_plan",
    "extract_task_queue_poison_pill_readiness",
    "generate_task_queue_poison_pill_readiness",
    "recommend_task_queue_poison_pill_readiness",
    "summarize_task_queue_poison_pill_readiness",
    "task_queue_poison_pill_readiness_plan_to_dict",
    "task_queue_poison_pill_readiness_plan_to_dicts",
    "task_queue_poison_pill_readiness_plan_to_markdown",
]
