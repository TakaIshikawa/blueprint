"""Plan graceful degradation readiness safeguards for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DegradationSignal = Literal[
    "fallback",
    "degraded_mode",
    "partial_availability",
    "circuit_breaker",
    "optional_dependency_failure",
    "stale_cache_fallback",
    "read_only_mode",
    "feature_unavailable_message",
]
DegradationSafeguard = Literal[
    "fallback_ux_state",
    "dependency_timeout",
    "degraded_telemetry",
    "recovery_behavior",
    "dependency_failure_tests",
]
DegradationReadinessLevel = Literal["missing", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: dict[DegradationSignal, int] = {
    "fallback": 0,
    "degraded_mode": 1,
    "partial_availability": 2,
    "circuit_breaker": 3,
    "optional_dependency_failure": 4,
    "stale_cache_fallback": 5,
    "read_only_mode": 6,
    "feature_unavailable_message": 7,
}
_SAFEGUARD_ORDER: dict[DegradationSafeguard, int] = {
    "fallback_ux_state": 0,
    "dependency_timeout": 1,
    "degraded_telemetry": 2,
    "recovery_behavior": 3,
    "dependency_failure_tests": 4,
}
_READINESS_ORDER: dict[DegradationReadinessLevel, int] = {"missing": 0, "partial": 1, "strong": 2}
_SIGNAL_PATTERNS: dict[DegradationSignal, re.Pattern[str]] = {
    "fallback": re.compile(
        r"\b(?:fallback|fall back|back(?:up)? path|alternate path|secondary provider|safe default)\b",
        re.I,
    ),
    "degraded_mode": re.compile(
        r"\b(?:degraded mode|degrade gracefully|graceful degradation|reduced functionality|"
        r"limited mode|best effort mode|service degradation)\b",
        re.I,
    ),
    "partial_availability": re.compile(
        r"\b(?:partial availability|partial outage|partial service|partial success|partially available|"
        r"limited availability|some features unavailable)\b",
        re.I,
    ),
    "circuit_breaker": re.compile(
        r"\b(?:circuit breaker|circuit[- ]breaker|open circuit|half[- ]open|trip(?:ped)? circuit)\b",
        re.I,
    ),
    "optional_dependency_failure": re.compile(
        r"\b(?:(?:optional|external|downstream|third[- ]party|partner|expensive) (?:\w+\s+){0,3}"
        r"(?:dependency|service|integration|provider|feature)s? (?:fail|fails|failure|unavailable|outage)|"
        r"(?:optional|external|downstream|third[- ]party|partner|expensive) (?:\w+\s+){0,3}"
        r"(?:dependency|service|integration|provider|feature)s? (?:has|have|during|with) "
        r"(?:a )?(?:partial )?(?:outage|failure)|"
        r"(?:dependency|service|integration|provider) failure|vendor outage|provider unavailable)\b",
        re.I,
    ),
    "stale_cache_fallback": re.compile(
        r"\b(?:stale cache fallback|serve stale|stale data|cached fallback|last known good|"
        r"last-known-good|cache fallback)\b",
        re.I,
    ),
    "read_only_mode": re.compile(
        r"\b(?:read[- ]only mode|read only fallback|disable writes|writes disabled|view[- ]only mode)\b",
        re.I,
    ),
    "feature_unavailable_message": re.compile(
        r"\b(?:feature unavailable|temporarily unavailable|unavailable message|unavailable state|"
        r"disabled feature message|maintenance message)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[DegradationSignal, re.Pattern[str]] = {
    "fallback": re.compile(r"fallback|backup[_-]?path|safe[_-]?default", re.I),
    "degraded_mode": re.compile(r"degrad|limited[_-]?mode|best[_-]?effort", re.I),
    "partial_availability": re.compile(r"partial[_-]?(?:availability|outage|service|success)", re.I),
    "circuit_breaker": re.compile(r"circuit[_-]?breaker|half[_-]?open", re.I),
    "optional_dependency_failure": re.compile(r"dependency[_-]?fail|provider[_-]?fail|service[_-]?fail|optional[_-]?integration", re.I),
    "stale_cache_fallback": re.compile(r"stale[_-]?cache|cache[_-]?fallback|last[_-]?known[_-]?good", re.I),
    "read_only_mode": re.compile(r"read[_-]?only|view[_-]?only|writes[_-]?disabled", re.I),
    "feature_unavailable_message": re.compile(r"unavailable|disabled[_-]?feature|maintenance[_-]?message", re.I),
}
_SAFEGUARD_PATTERNS: dict[DegradationSafeguard, re.Pattern[str]] = {
    "fallback_ux_state": re.compile(
        r"\b(?:fallback ux|fallback state|degraded state|user[- ]visible fallback|banner|toast|"
        r"empty state|unavailable message|disabled state|read[- ]only notice|partial availability message|"
        r"explain(?:s)? (?:degraded|unavailable|fallback))\b",
        re.I,
    ),
    "dependency_timeout": re.compile(
        r"\b(?:timeout|timeouts|time out|deadline|request limit|bounded wait|cancel(?:lation)?|"
        r"after \d+(?:\.\d+)?\s*(?:ms|s|sec|seconds?|minutes?)|circuit breaker threshold)\b",
        re.I,
    ),
    "degraded_telemetry": re.compile(
        r"\b(?:telemetry|metrics?|metric|observability|logging|logs?|trace|tracing|alert|dashboard|"
        r"degraded mode event|fallback event|circuit breaker metric)\b",
        re.I,
    ),
    "recovery_behavior": re.compile(
        r"\b(?:recover(?:y|ing)?|restore|resume|return to normal|exit degraded|health check|"
        r"rejoin|retry after recovery|close the circuit|freshen cache|refresh stale)\b",
        re.I,
    ),
    "dependency_failure_tests": re.compile(
        r"\b(?:test(?:s|ing)?|validation|simulate|mock|stub|fixture|fault injection|chaos|outage test|"
        r"dependency failure|provider failure|service unavailable|timeout test|circuit breaker test)\b",
        re.I,
    ),
}
_RECOMMENDED_CHECKS: dict[DegradationSafeguard, str] = {
    "fallback_ux_state": "Define the user-visible degraded, unavailable, read-only, or fallback state.",
    "dependency_timeout": "Set explicit dependency deadlines, cancellation behavior, and bounded waits.",
    "degraded_telemetry": "Emit logs, metrics, or alerts when fallback or degraded mode starts and ends.",
    "recovery_behavior": "Specify how the task detects recovery and returns to the normal path.",
    "dependency_failure_tests": "Validate dependency failure, timeout, unavailable, and recovery paths in tests.",
}


@dataclass(frozen=True, slots=True)
class TaskGracefulDegradationReadinessRecord:
    """Graceful degradation readiness guidance for one affected execution task."""

    task_id: str
    title: str
    degradation_signals: tuple[DegradationSignal, ...]
    present_safeguards: tuple[DegradationSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[DegradationSafeguard, ...] = field(default_factory=tuple)
    readiness_level: DegradationReadinessLevel = "missing"
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "degradation_signals": list(self.degradation_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness_level": self.readiness_level,
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskGracefulDegradationReadinessPlan:
    """Plan-level graceful degradation readiness review."""

    plan_id: str | None = None
    records: tuple[TaskGracefulDegradationReadinessRecord, ...] = field(default_factory=tuple)
    degradation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "degradation_task_ids": list(self.degradation_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render graceful degradation readiness as deterministic Markdown."""
        title = "# Task Graceful Degradation Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Degradation task count: {self.summary.get('degradation_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            f"- Applicable task ids: {_markdown_cell(', '.join(self.degradation_task_ids) or 'none')}",
            f"- Not-applicable task ids: {_markdown_cell(', '.join(self.not_applicable_task_ids) or 'none')}",
        ]
        if not self.records:
            lines.extend(["", "No graceful degradation readiness records were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Signals | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness_level} | "
                f"{_markdown_cell(', '.join(record.degradation_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        return "\n".join(lines)


def build_task_graceful_degradation_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskGracefulDegradationReadinessPlan:
    """Build graceful degradation readiness records for tasks that need fallback behavior."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness_level],
                len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    degradation_task_ids = tuple(record.task_id for record in records)
    applicable_set = set(degradation_task_ids)
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in applicable_set
    )
    return TaskGracefulDegradationReadinessPlan(
        plan_id=plan_id,
        records=records,
        degradation_task_ids=degradation_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_graceful_degradation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskGracefulDegradationReadinessPlan:
    """Compatibility alias for building graceful degradation readiness plans."""
    return build_task_graceful_degradation_readiness_plan(source)


def summarize_task_graceful_degradation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskGracefulDegradationReadinessPlan:
    """Compatibility alias for building graceful degradation readiness plans."""
    return build_task_graceful_degradation_readiness_plan(source)


def extract_task_graceful_degradation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskGracefulDegradationReadinessPlan:
    """Compatibility alias for building graceful degradation readiness plans."""
    return build_task_graceful_degradation_readiness_plan(source)


def generate_task_graceful_degradation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskGracefulDegradationReadinessPlan:
    """Compatibility alias for generating graceful degradation readiness plans."""
    return build_task_graceful_degradation_readiness_plan(source)


def recommend_task_graceful_degradation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskGracefulDegradationReadinessPlan:
    """Compatibility alias for recommending graceful degradation readiness plans."""
    return build_task_graceful_degradation_readiness_plan(source)


def task_graceful_degradation_readiness_plan_to_dict(
    result: TaskGracefulDegradationReadinessPlan,
) -> dict[str, Any]:
    """Serialize a graceful degradation readiness plan to a plain dictionary."""
    return result.to_dict()


task_graceful_degradation_readiness_plan_to_dict.__test__ = False


def task_graceful_degradation_readiness_plan_to_markdown(
    result: TaskGracefulDegradationReadinessPlan,
) -> str:
    """Render a graceful degradation readiness plan as Markdown."""
    return result.to_markdown()


task_graceful_degradation_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    degradation_signals: tuple[DegradationSignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[DegradationSafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskGracefulDegradationReadinessRecord | None:
    signals = _signals(task)
    if not signals.degradation_signals:
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskGracefulDegradationReadinessRecord(
        task_id=task_id,
        title=title,
        degradation_signals=signals.degradation_signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        readiness_level=_readiness_level(signals.present_safeguards, missing),
        recommended_checks=tuple(_RECOMMENDED_CHECKS[safeguard] for safeguard in missing),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[DegradationSignal] = set()
    safeguard_hits: set[DegradationSafeguard] = set()
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for field_name in ("files_or_modules", "files", "expected_files", "expected_file_paths"):
        for path in _strings(task.get(field_name)):
            normalized = _normalized_path(path)
            if not normalized:
                continue
            searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
            matched = False
            for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
                if pattern.search(normalized) or pattern.search(searchable):
                    signal_hits.add(signal)
                    matched = True
            if matched:
                signal_evidence.append(f"{field_name}: {path}")
            for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
                if pattern.search(normalized) or pattern.search(searchable):
                    safeguard_hits.add(safeguard)
                    safeguard_evidence.append(f"{field_name}: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched_signal = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
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
        degradation_signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _readiness_level(
    present: tuple[DegradationSafeguard, ...],
    missing: tuple[DegradationSafeguard, ...],
) -> DegradationReadinessLevel:
    if not missing:
        return "strong"
    if present:
        return "partial"
    return "missing"


def _summary(
    records: tuple[TaskGracefulDegradationReadinessRecord, ...],
    *,
    task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "degradation_task_count": len(records),
        "degradation_task_ids": [record.task_id for record in records],
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            level: sum(1 for record in records if record.readiness_level == level)
            for level in _READINESS_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.degradation_signals)
            for signal in _SIGNAL_ORDER
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
        "expected_files",
        "expected_file_paths",
        "acceptance_criteria",
        "criteria",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "risks",
        "test_command",
        "validation_commands",
        "validation_plan",
        "validation_plans",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
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
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "criteria",
        "depends_on",
        "tags",
        "labels",
        "notes",
        "risks",
        "validation_commands",
        "validation_plan",
        "validation_plans",
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
            key_text = str(key).replace("_", " ").replace("-", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                if any(pattern.search(key_text) for pattern in patterns):
                    texts.append((field, key_text))
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
    "DegradationReadinessLevel",
    "DegradationSafeguard",
    "DegradationSignal",
    "TaskGracefulDegradationReadinessPlan",
    "TaskGracefulDegradationReadinessRecord",
    "analyze_task_graceful_degradation_readiness",
    "build_task_graceful_degradation_readiness_plan",
    "extract_task_graceful_degradation_readiness",
    "generate_task_graceful_degradation_readiness",
    "recommend_task_graceful_degradation_readiness",
    "summarize_task_graceful_degradation_readiness",
    "task_graceful_degradation_readiness_plan_to_dict",
    "task_graceful_degradation_readiness_plan_to_markdown",
]
