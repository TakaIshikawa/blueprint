"""Plan reconnect readiness safeguards for realtime execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


RealtimeSignal = Literal[
    "websocket",
    "sse",
    "streaming",
    "subscription",
    "presence",
    "live_updates",
    "reconnect",
    "heartbeat",
    "offline",
]
RealtimeReconnectSafeguard = Literal[
    "reconnect_backoff",
    "heartbeat_timeout",
    "duplicate_event_handling",
    "stale_subscription_cleanup",
    "offline_resume_behavior",
]
RealtimeReconnectReadiness = Literal["strong", "partial", "missing"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[RealtimeReconnectReadiness, int] = {"missing": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: dict[RealtimeSignal, int] = {
    "websocket": 0,
    "sse": 1,
    "streaming": 2,
    "subscription": 3,
    "presence": 4,
    "live_updates": 5,
    "reconnect": 6,
    "heartbeat": 7,
    "offline": 8,
}
_SAFEGUARD_ORDER: dict[RealtimeReconnectSafeguard, int] = {
    "reconnect_backoff": 0,
    "heartbeat_timeout": 1,
    "duplicate_event_handling": 2,
    "stale_subscription_cleanup": 3,
    "offline_resume_behavior": 4,
}
_SIGNAL_PATTERNS: dict[RealtimeSignal, re.Pattern[str]] = {
    "websocket": re.compile(r"\b(?:websocket|web socket|ws connection|socket connection)\b", re.I),
    "sse": re.compile(r"\b(?:sse|server[- ]sent events?|eventsource)\b", re.I),
    "streaming": re.compile(r"\b(?:streaming|streamed response|event stream|stream updates?)\b", re.I),
    "subscription": re.compile(r"\b(?:subscription|subscriptions|subscribe|subscriber|pub/sub|pubsub)\b", re.I),
    "presence": re.compile(r"\b(?:presence|online status|typing indicator|currently online)\b", re.I),
    "live_updates": re.compile(r"\b(?:live updates?|real[- ]time updates?|realtime updates?|live feed|live sync)\b", re.I),
    "reconnect": re.compile(r"\b(?:reconnect|reconnection|disconnect recovery|connection recovery|connection lost)\b", re.I),
    "heartbeat": re.compile(r"\b(?:heartbeat|ping/pong|ping pong|keepalive|keep-alive)\b", re.I),
    "offline": re.compile(r"\b(?:offline|resume|resumption|network loss|network drop|connection resume)\b", re.I),
}
_PATH_PATTERNS: dict[RealtimeSignal, re.Pattern[str]] = {
    "websocket": re.compile(r"web[_-]?socket|websocket|/ws/|socket", re.I),
    "sse": re.compile(r"sse|server[_-]?sent|eventsource", re.I),
    "streaming": re.compile(r"stream|streaming|event[_-]?stream", re.I),
    "subscription": re.compile(r"subscription|subscriber|pubsub|pub[_-]?sub", re.I),
    "presence": re.compile(r"presence|online[_-]?status|typing", re.I),
    "live_updates": re.compile(r"live[_-]?updates?|realtime|real[_-]?time|live[_-]?feed", re.I),
    "reconnect": re.compile(r"reconnect|reconnection|connection[_-]?recovery", re.I),
    "heartbeat": re.compile(r"heartbeat|keepalive|keep[_-]?alive|ping[_-]?pong", re.I),
    "offline": re.compile(r"offline|resume|resumption", re.I),
}
_SAFEGUARD_PATTERNS: dict[RealtimeReconnectSafeguard, re.Pattern[str]] = {
    "reconnect_backoff": re.compile(
        r"\b(?:reconnect backoff|reconnection backoff|exponential backoff|jitter|retry delay|backoff policy)\b",
        re.I,
    ),
    "heartbeat_timeout": re.compile(
        r"\b(?:heartbeat timeout|heartbeat interval|missed heartbeat|ping timeout|pong timeout|keepalive timeout)\b",
        re.I,
    ),
    "duplicate_event_handling": re.compile(
        r"\b(?:duplicate event|duplicate handling|dedupe|dedup|idempotent|idempotency|event id|sequence number|replay safe)\b",
        re.I,
    ),
    "stale_subscription_cleanup": re.compile(
        r"\b(?:stale subscription|subscription cleanup|unsubscribe|cleanup subscriptions?|orphaned subscription|presence cleanup)\b",
        re.I,
    ),
    "offline_resume_behavior": re.compile(
        r"\b(?:offline resume|resume token|resume cursor|last seen event|offline queue|network recovery|resume after reconnect)\b",
        re.I,
    ),
}
_CHECKS: dict[RealtimeReconnectSafeguard, str] = {
    "reconnect_backoff": "Verify reconnect backoff uses capped exponential delay with jitter and retry limits.",
    "heartbeat_timeout": "Define heartbeat timeout behavior for missed ping/pong or keepalive failures.",
    "duplicate_event_handling": "Validate duplicate event handling with event IDs, sequence numbers, or idempotent replay.",
    "stale_subscription_cleanup": "Confirm stale subscription cleanup removes orphaned presence, stream, or channel listeners.",
    "offline_resume_behavior": "Test offline/resume behavior for network loss, resume cursors, and missed event recovery.",
}


@dataclass(frozen=True, slots=True)
class TaskRealtimeReconnectReadinessRecord:
    """Reconnect readiness guidance for one realtime execution task."""

    task_id: str
    title: str
    matched_realtime_signals: tuple[RealtimeSignal, ...]
    readiness: RealtimeReconnectReadiness
    missing_safeguards: tuple[RealtimeReconnectSafeguard, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "matched_realtime_signals": list(self.matched_realtime_signals),
            "readiness": self.readiness,
            "missing_safeguards": list(self.missing_safeguards),
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskRealtimeReconnectReadinessPlan:
    """Plan-level realtime reconnect readiness review."""

    plan_id: str | None = None
    records: tuple[TaskRealtimeReconnectReadinessRecord, ...] = field(default_factory=tuple)
    realtime_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "realtime_task_ids": list(self.realtime_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return realtime reconnect records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    @property
    def recommendations(self) -> tuple[TaskRealtimeReconnectReadinessRecord, ...]:
        """Compatibility view for callers that use recommendation terminology."""
        return self.records

    def to_markdown(self) -> str:
        """Render the reconnect readiness plan as deterministic Markdown."""
        title = "# Task Realtime Reconnect Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('realtime_task_count', 0)} realtime tasks "
                f"across {self.summary.get('task_count', 0)} total tasks "
                f"(strong: {counts.get('strong', 0)}, partial: {counts.get('partial', 0)}, "
                f"missing: {counts.get('missing', 0)})."
            ),
        ]
        if not self.records:
            lines.extend(["", "No realtime reconnect readiness recommendations were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Readiness | Signals | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.matched_realtime_signals))} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_realtime_reconnect_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskRealtimeReconnectReadinessPlan:
    """Build realtime reconnect readiness records for relevant execution tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_record_for_task(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                -len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    realtime_task_ids = tuple(record.task_id for record in records)
    realtime_task_id_set = set(realtime_task_ids)
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in realtime_task_id_set
    )
    return TaskRealtimeReconnectReadinessPlan(
        plan_id=plan_id,
        records=records,
        realtime_task_ids=realtime_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, total_task_count=len(tasks), not_applicable_task_count=len(not_applicable_task_ids)),
    )


def generate_task_realtime_reconnect_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[TaskRealtimeReconnectReadinessRecord, ...]:
    """Return realtime reconnect readiness records for relevant execution tasks."""
    return build_task_realtime_reconnect_readiness_plan(source).records


def recommend_task_realtime_reconnect_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> tuple[TaskRealtimeReconnectReadinessRecord, ...]:
    """Compatibility alias for returning realtime reconnect readiness records."""
    return generate_task_realtime_reconnect_readiness(source)


def summarize_task_realtime_reconnect_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskRealtimeReconnectReadinessPlan:
    """Compatibility alias for building realtime reconnect readiness plans."""
    return build_task_realtime_reconnect_readiness_plan(source)


def task_realtime_reconnect_readiness_plan_to_dict(
    result: TaskRealtimeReconnectReadinessPlan,
) -> dict[str, Any]:
    """Serialize a realtime reconnect readiness plan to a plain dictionary."""
    return result.to_dict()


task_realtime_reconnect_readiness_plan_to_dict.__test__ = False


def task_realtime_reconnect_readiness_to_dicts(
    records: (
        tuple[TaskRealtimeReconnectReadinessRecord, ...]
        | list[TaskRealtimeReconnectReadinessRecord]
        | TaskRealtimeReconnectReadinessPlan
    ),
) -> list[dict[str, Any]]:
    """Serialize realtime reconnect readiness records to dictionaries."""
    if isinstance(records, TaskRealtimeReconnectReadinessPlan):
        return records.to_dicts()
    return [record.to_dict() for record in records]


task_realtime_reconnect_readiness_to_dicts.__test__ = False


def task_realtime_reconnect_readiness_plan_to_markdown(
    result: TaskRealtimeReconnectReadinessPlan,
) -> str:
    """Render a realtime reconnect readiness plan as Markdown."""
    return result.to_markdown()


task_realtime_reconnect_readiness_plan_to_markdown.__test__ = False


def _record_for_task(task: Mapping[str, Any], index: int) -> TaskRealtimeReconnectReadinessRecord | None:
    signals: dict[RealtimeSignal, list[str]] = {}
    safeguards: set[RealtimeReconnectSafeguard] = set()
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        _inspect_path(path, signals)
    for source_field, text in _candidate_texts(task):
        _inspect_text(source_field, text, signals, safeguards)

    if not signals:
        return None

    matched_signals = tuple(signal for signal in _SIGNAL_ORDER if signal in signals)
    missing_safeguards = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in safeguards)
    task_id = _task_id(task, index)
    return TaskRealtimeReconnectReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        matched_realtime_signals=matched_signals,
        readiness=_readiness(safeguards, missing_safeguards),
        missing_safeguards=missing_safeguards,
        recommended_checks=tuple(_CHECKS[safeguard] for safeguard in missing_safeguards),
        evidence=tuple(
            _dedupe(
                evidence
                for signal in matched_signals
                for evidence in signals.get(signal, [])
            )
        ),
    )


def _inspect_path(path: str, signals: dict[RealtimeSignal, list[str]]) -> None:
    normalized = _normalized_path(path)
    if not normalized:
        return
    evidence = f"files_or_modules: {path}"
    searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    for signal, pattern in _PATH_PATTERNS.items():
        if pattern.search(normalized) or pattern.search(searchable):
            signals.setdefault(signal, []).append(evidence)


def _inspect_text(
    source_field: str,
    text: str,
    signals: dict[RealtimeSignal, list[str]],
    safeguards: set[RealtimeReconnectSafeguard],
) -> None:
    evidence = _evidence_snippet(source_field, text)
    for signal, pattern in _SIGNAL_PATTERNS.items():
        if pattern.search(text):
            signals.setdefault(signal, []).append(evidence)
    for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
        if pattern.search(text):
            safeguards.add(safeguard)


def _readiness(
    safeguards: set[RealtimeReconnectSafeguard],
    missing_safeguards: tuple[RealtimeReconnectSafeguard, ...],
) -> RealtimeReconnectReadiness:
    if not missing_safeguards:
        return "strong"
    if safeguards:
        return "partial"
    return "missing"


def _summary(
    records: tuple[TaskRealtimeReconnectReadinessRecord, ...],
    *,
    total_task_count: int,
    not_applicable_task_count: int,
) -> dict[str, Any]:
    return {
        "task_count": total_task_count,
        "realtime_task_count": len(records),
        "not_applicable_task_count": not_applicable_task_count,
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in ("strong", "partial", "missing")
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.matched_realtime_signals)
            for signal in _SIGNAL_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
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
    for field_name in (
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "dependencies",
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
        patterns = (*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values())
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
    "RealtimeReconnectReadiness",
    "RealtimeReconnectSafeguard",
    "RealtimeSignal",
    "TaskRealtimeReconnectReadinessPlan",
    "TaskRealtimeReconnectReadinessRecord",
    "build_task_realtime_reconnect_readiness_plan",
    "generate_task_realtime_reconnect_readiness",
    "recommend_task_realtime_reconnect_readiness",
    "summarize_task_realtime_reconnect_readiness",
    "task_realtime_reconnect_readiness_plan_to_dict",
    "task_realtime_reconnect_readiness_plan_to_markdown",
    "task_realtime_reconnect_readiness_to_dicts",
]
