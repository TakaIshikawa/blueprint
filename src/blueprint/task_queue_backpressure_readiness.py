"""Plan backpressure readiness safeguards for queue-like execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


QueueBackpressureRiskLevel = Literal["low", "medium", "high"]
QueueSurface = Literal["queue", "worker", "consumer", "scheduler", "stream", "batch processor"]
ThroughputSignal = Literal[
    "high_volume",
    "realtime",
    "customer_facing",
    "bursty",
    "long_running",
]
BackpressureControl = Literal[
    "concurrency_limit",
    "retry_backoff",
    "dead_letter_or_quarantine",
    "rate_limit_or_throttle",
    "queue_depth_monitoring",
    "saturation_alerts",
    "load_test_evidence",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[QueueBackpressureRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SURFACE_ORDER: dict[QueueSurface, int] = {
    "queue": 0,
    "worker": 1,
    "consumer": 2,
    "scheduler": 3,
    "stream": 4,
    "batch processor": 5,
}
_SIGNAL_ORDER: dict[ThroughputSignal, int] = {
    "high_volume": 0,
    "realtime": 1,
    "customer_facing": 2,
    "bursty": 3,
    "long_running": 4,
}
_CONTROL_ORDER: dict[BackpressureControl, int] = {
    "concurrency_limit": 0,
    "retry_backoff": 1,
    "dead_letter_or_quarantine": 2,
    "rate_limit_or_throttle": 3,
    "queue_depth_monitoring": 4,
    "saturation_alerts": 5,
    "load_test_evidence": 6,
}
_SURFACE_PATTERNS: dict[QueueSurface, re.Pattern[str]] = {
    "queue": re.compile(r"\b(?:queues?|enqueue|dequeue|queued job|job queue|task queue|message queue)\b", re.I),
    "worker": re.compile(r"\b(?:workers?|worker pool|background job|background processing|job runner)\b", re.I),
    "consumer": re.compile(r"\b(?:consumers?|consume messages?|message handler|subscriber|subscription)\b", re.I),
    "scheduler": re.compile(r"\b(?:schedulers?|scheduled jobs?|cron|periodic jobs?|timer jobs?)\b", re.I),
    "stream": re.compile(r"\b(?:streams?|streaming|kafka|kinesis|pub/sub|pubsub|event bus|event stream)\b", re.I),
    "batch processor": re.compile(r"\b(?:batch processors?|batch jobs?|bulk import|bulk export|etl|data pipeline)\b", re.I),
}
_PATH_PATTERNS: dict[QueueSurface, re.Pattern[str]] = {
    "queue": re.compile(r"(?:^|/)(?:queues?|jobs?)(?:/|$)|queue|enqueue|dequeue", re.I),
    "worker": re.compile(r"(?:^|/)(?:workers?|background_jobs?)(?:/|$)|worker|job_runner", re.I),
    "consumer": re.compile(r"(?:^|/)(?:consumers?|subscribers?)(?:/|$)|consumer|subscriber|subscription", re.I),
    "scheduler": re.compile(r"(?:^|/)(?:schedulers?|cron|periodic)(?:/|$)|scheduler|scheduled_job", re.I),
    "stream": re.compile(r"(?:^|/)(?:streams?|kafka|kinesis|pubsub)(?:/|$)|stream|event_bus", re.I),
    "batch processor": re.compile(r"(?:^|/)(?:batch|etl|pipelines?)(?:/|$)|batch|bulk_|etl", re.I),
}
_SIGNAL_PATTERNS: dict[ThroughputSignal, re.Pattern[str]] = {
    "high_volume": re.compile(
        r"\b(?:high[- ]volume|large volume|millions?|thousands? per|bulk|scale|throughput|qps|rps|tps)\b",
        re.I,
    ),
    "realtime": re.compile(r"\b(?:real[- ]time|realtime|near[- ]real[- ]time|low latency|latency sensitive|live)\b", re.I),
    "customer_facing": re.compile(r"\b(?:customer[- ]facing|user[- ]facing|public api|checkout|signup|production users?)\b", re.I),
    "bursty": re.compile(r"\b(?:burst|spike|surge|peak traffic|flash sale|fan[- ]out)\b", re.I),
    "long_running": re.compile(r"\b(?:long[- ]running|long running|hours?|overnight|backfill|migration job)\b", re.I),
}
_CONTROL_PATTERNS: dict[BackpressureControl, re.Pattern[str]] = {
    "concurrency_limit": re.compile(r"\b(?:concurrency limit|worker limit|max concurrency|parallelism cap|pool size)\b", re.I),
    "retry_backoff": re.compile(r"\b(?:retry backoff|exponential backoff|jitter|retry delay|backoff policy)\b", re.I),
    "dead_letter_or_quarantine": re.compile(r"\b(?:dead[- ]letter|dlq|quarantine|poison message|failed jobs? queue)\b", re.I),
    "rate_limit_or_throttle": re.compile(r"\b(?:rate limit|rate limiting|throttle|throttling|token bucket|leaky bucket)\b", re.I),
    "queue_depth_monitoring": re.compile(r"\b(?:queue depth|queue length|backlog|lag monitoring|consumer lag|depth monitoring)\b", re.I),
    "saturation_alerts": re.compile(r"\b(?:saturation alerts?|saturation alarms?|worker saturation|lag alerts?|backlog alerts?|alert on saturation)\b", re.I),
    "load_test_evidence": re.compile(r"\b(?:load test|load testing|stress test|soak test|capacity test|performance test)\b", re.I),
}


@dataclass(frozen=True, slots=True)
class TaskQueueBackpressureReadinessRecommendation:
    """Backpressure readiness guidance for one affected execution task."""

    task_id: str
    title: str
    queue_surfaces: tuple[QueueSurface, ...] = field(default_factory=tuple)
    missing_backpressure_controls: tuple[BackpressureControl, ...] = field(default_factory=tuple)
    risk_level: QueueBackpressureRiskLevel = "medium"
    throughput_signals: tuple[ThroughputSignal, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "queue_surfaces": list(self.queue_surfaces),
            "missing_backpressure_controls": list(self.missing_backpressure_controls),
            "risk_level": self.risk_level,
            "throughput_signals": list(self.throughput_signals),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskQueueBackpressureReadinessPlan:
    """Plan-level task queue backpressure readiness summary."""

    plan_id: str | None = None
    recommendations: tuple[TaskQueueBackpressureReadinessRecommendation, ...] = field(default_factory=tuple)
    flagged_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [record.to_dict() for record in self.recommendations],
            "flagged_task_ids": list(self.flagged_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return recommendations as plain dictionaries."""
        return [record.to_dict() for record in self.recommendations]


def build_task_queue_backpressure_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskQueueBackpressureReadinessPlan:
    """Build task queue backpressure recommendations for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (
                record
                for index, task in enumerate(tasks, start=1)
                if (record := _record_for_task(task, index)) is not None
            ),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    return TaskQueueBackpressureReadinessPlan(
        plan_id=plan_id,
        recommendations=records,
        flagged_task_ids=tuple(record.task_id for record in records),
        summary=_summary(records, total_task_count=len(tasks)),
    )


def generate_task_queue_backpressure_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[TaskQueueBackpressureReadinessRecommendation, ...]:
    """Return task queue backpressure recommendations for relevant execution tasks."""
    return build_task_queue_backpressure_readiness_plan(source).recommendations


def task_queue_backpressure_readiness_to_dicts(
    records: (
        tuple[TaskQueueBackpressureReadinessRecommendation, ...]
        | list[TaskQueueBackpressureReadinessRecommendation]
        | TaskQueueBackpressureReadinessPlan
    ),
) -> list[dict[str, Any]]:
    """Serialize task queue backpressure recommendations to dictionaries."""
    if isinstance(records, TaskQueueBackpressureReadinessPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


def task_queue_backpressure_readiness_plan_to_dict(result: TaskQueueBackpressureReadinessPlan) -> dict[str, Any]:
    """Serialize a task queue backpressure readiness plan to a plain dictionary."""
    return result.to_dict()


task_queue_backpressure_readiness_plan_to_dict.__test__ = False


def summarize_task_queue_backpressure_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskQueueBackpressureReadinessPlan:
    """Compatibility alias for building task queue backpressure readiness plans."""
    return build_task_queue_backpressure_readiness_plan(source)


def _record_for_task(
    task: Mapping[str, Any],
    index: int,
) -> TaskQueueBackpressureReadinessRecommendation | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    surfaces: dict[QueueSurface, list[str]] = {}
    signals: dict[ThroughputSignal, list[str]] = {}
    controls: set[BackpressureControl] = set()

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _inspect_path(path, surfaces, signals)

    for source_field, text in _candidate_texts(task):
        _inspect_text(source_field, text, surfaces, signals, controls)

    if not surfaces:
        return None

    queue_surfaces = tuple(sorted(surfaces, key=lambda item: _SURFACE_ORDER[item]))
    throughput_signals = tuple(sorted(signals, key=lambda item: _SIGNAL_ORDER[item]))
    missing_controls = tuple(control for control in _CONTROL_ORDER if control not in controls)
    return TaskQueueBackpressureReadinessRecommendation(
        task_id=task_id,
        title=title,
        queue_surfaces=queue_surfaces,
        missing_backpressure_controls=missing_controls,
        risk_level=_risk_level(throughput_signals, missing_controls),
        throughput_signals=throughput_signals,
        evidence=tuple(
            _dedupe(
                evidence
                for key in (*queue_surfaces, *throughput_signals)
                for evidence in (surfaces.get(key, []) if key in surfaces else signals.get(key, []))
            )
        ),
    )


def _inspect_path(
    path: str,
    surfaces: dict[QueueSurface, list[str]],
    signals: dict[ThroughputSignal, list[str]],
) -> None:
    normalized = _normalized_path(path)
    if not normalized:
        return
    evidence = f"files_or_modules: {path}"
    lowered = normalized.casefold()
    for surface, pattern in _PATH_PATTERNS.items():
        if pattern.search(lowered):
            surfaces.setdefault(surface, []).append(evidence)
    if any(token in lowered for token in ("bulk", "backfill", "migration")):
        signals.setdefault("long_running", []).append(evidence)
    if any(token in lowered for token in ("scale", "throughput", "high_volume")):
        signals.setdefault("high_volume", []).append(evidence)


def _inspect_text(
    source_field: str,
    text: str,
    surfaces: dict[QueueSurface, list[str]],
    signals: dict[ThroughputSignal, list[str]],
    controls: set[BackpressureControl],
) -> None:
    evidence = _evidence_snippet(source_field, text)
    for surface, pattern in _SURFACE_PATTERNS.items():
        if pattern.search(text):
            surfaces.setdefault(surface, []).append(evidence)
    for signal, pattern in _SIGNAL_PATTERNS.items():
        if pattern.search(text):
            signals.setdefault(signal, []).append(evidence)
    for control, pattern in _CONTROL_PATTERNS.items():
        if pattern.search(text):
            controls.add(control)


def _risk_level(
    throughput_signals: tuple[ThroughputSignal, ...],
    missing_controls: tuple[BackpressureControl, ...],
) -> QueueBackpressureRiskLevel:
    if not missing_controls:
        return "low"
    if any(signal in throughput_signals for signal in ("high_volume", "realtime", "customer_facing")):
        return "high"
    return "medium"


def _summary(
    records: tuple[TaskQueueBackpressureReadinessRecommendation, ...],
    *,
    total_task_count: int,
) -> dict[str, Any]:
    return {
        "total_task_count": total_task_count,
        "flagged_task_count": len(records),
        "unrelated_task_count": max(total_task_count - len(records), 0),
        "risk_counts": {
            level: sum(1 for record in records if record.risk_level == level)
            for level in ("low", "medium", "high")
        },
        "missing_backpressure_control_counts": {
            control: sum(1 for record in records if control in record.missing_backpressure_controls)
            for control in _CONTROL_ORDER
        },
        "queue_surface_counts": {
            surface: sum(1 for record in records if surface in record.queue_surfaces)
            for surface in _SURFACE_ORDER
        },
        "throughput_signal_counts": {
            signal: sum(1 for record in records if signal in record.throughput_signals)
            for signal in _SIGNAL_ORDER
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
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

    try:
        iterator = iter(source)
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
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


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
    return tasks


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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        patterns = (*_SURFACE_PATTERNS.values(), *_SIGNAL_PATTERNS.values(), *_CONTROL_PATTERNS.values())
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in patterns):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in patterns):
                texts.append((field, key_text))
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
    "BackpressureControl",
    "QueueBackpressureRiskLevel",
    "QueueSurface",
    "TaskQueueBackpressureReadinessPlan",
    "TaskQueueBackpressureReadinessRecommendation",
    "ThroughputSignal",
    "build_task_queue_backpressure_readiness_plan",
    "generate_task_queue_backpressure_readiness",
    "summarize_task_queue_backpressure_readiness",
    "task_queue_backpressure_readiness_plan_to_dict",
    "task_queue_backpressure_readiness_to_dicts",
]
