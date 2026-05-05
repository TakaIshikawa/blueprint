"""Plan circuit breaker readiness checks for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar, cast

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


CircuitBreakerReadinessSignal = Literal[
    "failure_threshold",
    "recovery_strategy",
    "fallback_mechanism",
    "health_check",
    "monitoring",
    "timeout_handling",
]
CircuitBreakerReadinessSafeguard = Literal[
    "failure_threshold_config",
    "recovery_strategy",
    "fallback_implementation",
    "health_check_integration",
    "monitoring_integration",
    "timeout_handling",
]
CircuitBreakerReadinessImpactLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[CircuitBreakerReadinessSignal, ...] = (
    "failure_threshold",
    "recovery_strategy",
    "fallback_mechanism",
    "health_check",
    "monitoring",
    "timeout_handling",
)
_SAFEGUARD_ORDER: tuple[CircuitBreakerReadinessSafeguard, ...] = (
    "failure_threshold_config",
    "recovery_strategy",
    "fallback_implementation",
    "health_check_integration",
    "monitoring_integration",
    "timeout_handling",
)
_IMPACT_ORDER: dict[CircuitBreakerReadinessImpactLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_KEYS = (
    "circuit_breaker_signals",
    "reliability_signals",
    "resilience_signals",
    "readiness_signals",
    "signals",
)
_SAFEGUARD_KEYS = (
    "circuit_breaker_safeguards",
    "safeguards",
    "recommended_safeguards",
    "checks",
    "test_coverage",
)
_PATH_SIGNAL_PATTERNS: tuple[tuple[CircuitBreakerReadinessSignal, re.Pattern[str]], ...] = (
    (
        "failure_threshold",
        re.compile(
            r"\b(?:circuit|breaker|threshold|failure)s?\b.*\b(?:config|setting|threshold|limit)\b",
            re.I,
        ),
    ),
    (
        "recovery_strategy",
        re.compile(r"\b(?:recovery|recover|retry|backoff|reset)\b", re.I),
    ),
    (
        "fallback_mechanism",
        re.compile(
            r"\b(?:fallback|degraded|default|cache|graceful)\b.*\b(?:handler|mode|response|behavior)\b",
            re.I,
        ),
    ),
    (
        "health_check",
        re.compile(r"\b(?:health|healthcheck|ping|probe|liveness|readiness)\b", re.I),
    ),
    (
        "monitoring",
        re.compile(r"\b(?:monitor|metric|alert|telemetry|observe|dashboard)\b", re.I),
    ),
    (
        "timeout_handling",
        re.compile(r"\b(?:timeout|deadline|ttl|time[- ]?to[- ]?live)\b", re.I),
    ),
)
_TEXT_SIGNAL_PATTERNS: dict[CircuitBreakerReadinessSignal, re.Pattern[str]] = {
    "failure_threshold": re.compile(
        r"\b(?:(?:failure|error)\s+(?:threshold|limit|count|rate)|"
        r"(?:consecutive|sequential)\s+(?:failures?|errors?)|"
        r"trip\s+(?:the\s+)?circuit|"
        r"open(?:s?)\s+(?:the\s+)?(?:after|when)|"
        r"circuit\s+breaker\s+threshold|"
        r"circuit\s+open)\b",
        re.I,
    ),
    "recovery_strategy": re.compile(
        r"\b(?:recovery\s+(?:strategy|logic|time|window|period)|"
        r"half[- ]open\s+state|"
        r"reset\s+circuit|"
        r"close\s+circuit|"
        r"exponential\s+backoff|"
        r"retry\s+(?:logic|strategy|policy|backoff))\b",
        re.I,
    ),
    "fallback_mechanism": re.compile(
        r"\b(?:fallback(?:\s+(?:mechanism|logic|handler|response|behavior|value|to|implementation|returns?))?|"
        r"graceful\s+degrad(?:ation|e)|"
        r"degraded\s+(?:mode|state|service)|"
        r"default\s+(?:response|behavior|value)|"
        r"cached?\s+(?:fallback|data|response|methods?)|"
        r"stale\s+data)\b",
        re.I,
    ),
    "health_check": re.compile(
        r"\b(?:health\s+(?:check|endpoint|probe|status)|"
        r"liveness\s+probe|"
        r"readiness\s+probe|"
        r"ping\s+(?:endpoint|check|test)|"
        r"service\s+health)\b",
        re.I,
    ),
    "monitoring": re.compile(
        r"\b(?:monitoring?\s+(?:circuit|failures?|errors?|integration|dashboard)|"
        r"circuit\s+(?:metric|telemetry|dashboard|alert|state)|"
        r"alert\s+(?:on\s+)?(?:failure|error|circuit)|"
        r"track\s+(?:failures?|errors?|circuit|error\s+rate)|"
        r"emit(?:s)?\s+(?:metrics?|telemetry)|"
        r"metrics?\s+(?:for|collection)|"
        r"telemetry)\b",
        re.I,
    ),
    "timeout_handling": re.compile(
        r"\b(?:timeout\s+(?:handling|config|setting|value|period|of)|"
        r"request\s+timeout|"
        r"operation\s+timeout|"
        r"deadline\s+(?:exceeded|handling)|"
        r"time[- ]to[- ]live|"
        r"within\s+\d+\s*[sm]|"
        r"\d+\s*(?:ms|sec(?:ond)?s?|min(?:ute)?s?)\s+timeout|"
        r"ttl\s+config)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[CircuitBreakerReadinessSafeguard, re.Pattern[str]] = {
    "failure_threshold_config": re.compile(
        r"\b(?:failure\s+threshold\s+(?:of\s+)?\d+|"
        r"consecutive\s+failures?\s+(?:of\s+)?\d+|"
        r"\d+\s+consecutive\s+(?:failures?|errors?)|"
        r"(?:after|opens?\s+after)\s+\d+|"
        r"\d+\s+(?:failures?|errors?)\s+(?:to\s+)?(?:trip|open)|"
        r"error\s+rate\s+threshold|"
        r"circuit\s+breaker\s+config)\b",
        re.I,
    ),
    "recovery_strategy": re.compile(
        r"\b(?:half[- ]open\s+(?:state|period)|"
        r"recovery\s+(?:window|period|timeout|time)|"
        r"exponential\s+backoff|"
        r"reset\s+(?:after|timeout|period)|"
        r"retry\s+(?:delay|interval|backoff))\b",
        re.I,
    ),
    "fallback_implementation": re.compile(
        r"\b(?:fallback\s+(?:implementation|handler|logic|code)|"
        r"implement\s+fallback|"
        r"degraded\s+mode\s+(?:implementation|behavior)|"
        r"default\s+(?:implementation|handler|response)|"
        r"cache\s+fallback)\b",
        re.I,
    ),
    "health_check_integration": re.compile(
        r"\b(?:health\s+check\s+(?:integration|endpoint|implementation)|"
        r"integrate\s+health\s+(?:check|probe)|"
        r"liveness\s+(?:probe|endpoint)|"
        r"readiness\s+(?:probe|endpoint))\b",
        re.I,
    ),
    "monitoring_integration": re.compile(
        r"\b(?:monitor(?:ing)?\s+(?:integration|dashboard|alert)|"
        r"integrate\s+(?:monitoring|telemetry|metrics)|"
        r"emit\s+(?:metrics?|telemetry)|"
        r"alert\s+(?:integration|config)|"
        r"observability\s+integration)\b",
        re.I,
    ),
    "timeout_handling": re.compile(
        r"\b(?:timeout\s+(?:handling|implementation|config|of\s+\d+)|"
        r"\d+\s*(?:ms|sec(?:ond)?s?|min(?:ute)?s?)\s+timeout|"
        r"handle\s+timeout|"
        r"deadline\s+(?:handling|of\s+\d+))\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[CircuitBreakerReadinessSafeguard, str] = {
    "failure_threshold_config": "Verify the task defines specific failure threshold values (e.g., 5 consecutive failures or 50% error rate).",
    "recovery_strategy": "Verify the task specifies recovery strategy including half-open state duration and retry backoff logic.",
    "fallback_implementation": "Verify the task includes fallback behavior implementation (cached data, default response, or degraded mode).",
    "health_check_integration": "Verify health check endpoints are integrated to monitor service availability and circuit state.",
    "monitoring_integration": "Verify circuit breaker state changes, failures, and recoveries emit metrics and alerts.",
    "timeout_handling": "Verify timeout values are defined for operations to prevent indefinite blocking.",
}
_ALIASES: dict[str, CircuitBreakerReadinessSignal | CircuitBreakerReadinessSafeguard] = {
    "threshold": "failure_threshold",
    "failure": "failure_threshold",
    "failures": "failure_threshold",
    "recovery": "recovery_strategy",
    "retry": "recovery_strategy",
    "backoff": "recovery_strategy",
    "fallback": "fallback_mechanism",
    "degraded": "fallback_mechanism",
    "graceful": "fallback_mechanism",
    "health": "health_check",
    "healthcheck": "health_check",
    "probe": "health_check",
    "monitoring": "monitoring",
    "metrics": "monitoring",
    "alert": "monitoring",
    "timeout": "timeout_handling",
    "deadline": "timeout_handling",
}


@dataclass(frozen=True, slots=True)
class TaskCircuitBreakerReadinessRecord:
    """Circuit breaker readiness guidance for one execution task."""

    task_id: str
    title: str
    matched_signals: tuple[CircuitBreakerReadinessSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[CircuitBreakerReadinessSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[CircuitBreakerReadinessSafeguard, ...] = field(default_factory=tuple)
    impact_level: CircuitBreakerReadinessImpactLevel = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "matched_signals": list(self.matched_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "impact_level": self.impact_level,
            "evidence": list(self.evidence),
            "recommended_checks": list(self.recommended_checks),
        }


@dataclass(frozen=True, slots=True)
class TaskCircuitBreakerReadinessPlan:
    """Plan-level circuit breaker readiness review."""

    plan_id: str | None = None
    records: tuple[TaskCircuitBreakerReadinessRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskCircuitBreakerReadinessRecord, ...]:
        """Compatibility view matching planners that name rows findings."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return circuit breaker readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the circuit breaker readiness plan as deterministic Markdown."""
        title = "# Task Circuit Breaker Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        impact_counts = self.summary.get("impact_level_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Impacted task count: {self.summary.get('impacted_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Impact counts: "
            + ", ".join(f"{level} {impact_counts.get(level, 0)}" for level in _IMPACT_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No circuit breaker readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(
                    ["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Impact | Matched Signals | Present Safeguards | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.impact_level} | "
                f"{_markdown_cell(', '.join(record.matched_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(
                ["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"]
            )
        return "\n".join(lines)


def build_task_circuit_breaker_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskCircuitBreakerReadinessPlan:
    """Build task-level circuit breaker readiness guidance."""
    plan_id, tasks = _source_payload(source)
    candidates = [_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _IMPACT_ORDER[record.impact_level],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    impacted_task_ids = tuple(record.task_id for record in records)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskCircuitBreakerReadinessPlan(
        plan_id=plan_id,
        records=records,
        impacted_task_ids=impacted_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_circuit_breaker_readiness(source: Any) -> TaskCircuitBreakerReadinessPlan:
    """Compatibility alias for building circuit breaker readiness guidance."""
    return build_task_circuit_breaker_readiness_plan(source)


def summarize_task_circuit_breaker_readiness(source: Any) -> TaskCircuitBreakerReadinessPlan:
    """Compatibility alias for building circuit breaker readiness guidance."""
    return build_task_circuit_breaker_readiness_plan(source)


def derive_task_circuit_breaker_readiness_plan(source: Any) -> TaskCircuitBreakerReadinessPlan:
    """Compatibility alias for deriving circuit breaker readiness guidance."""
    return build_task_circuit_breaker_readiness_plan(source)


def task_circuit_breaker_readiness_plan_to_dict(
    result: TaskCircuitBreakerReadinessPlan,
) -> dict[str, Any]:
    """Serialize a circuit breaker readiness plan to a plain dictionary."""
    return result.to_dict()


task_circuit_breaker_readiness_plan_to_dict.__test__ = False


def task_circuit_breaker_readiness_plan_to_dicts(
    result: TaskCircuitBreakerReadinessPlan | Iterable[TaskCircuitBreakerReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize circuit breaker readiness records to plain dictionaries."""
    if isinstance(result, TaskCircuitBreakerReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_circuit_breaker_readiness_plan_to_dicts.__test__ = False


def task_circuit_breaker_readiness_plan_to_markdown(
    result: TaskCircuitBreakerReadinessPlan,
) -> str:
    """Render a circuit breaker readiness plan as Markdown."""
    return result.to_markdown()


task_circuit_breaker_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Matches:
    signals: tuple[CircuitBreakerReadinessSignal, ...]
    safeguards: tuple[CircuitBreakerReadinessSafeguard, ...]
    evidence: tuple[str, ...]


def _record(task: Mapping[str, Any], index: int) -> TaskCircuitBreakerReadinessRecord | None:
    matches = _matches(task)
    if not matches.signals:
        return None
    missing: tuple[CircuitBreakerReadinessSafeguard, ...] = tuple(
        safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in matches.safeguards
    )
    task_id = _task_id(task, index)
    return TaskCircuitBreakerReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        matched_signals=matches.signals,
        present_safeguards=matches.safeguards,
        missing_safeguards=missing,
        impact_level=_impact_level(matches.signals),
        evidence=matches.evidence,
        recommended_checks=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing),
    )


def _matches(task: Mapping[str, Any]) -> _Matches:
    signals: set[CircuitBreakerReadinessSignal] = set()
    safeguards: set[CircuitBreakerReadinessSafeguard] = set()
    evidence: list[str] = []

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for signal in _metadata_signals(metadata):
            signals.add(signal)
            evidence.append(f"metadata.circuit_breaker_signals: {signal}")
        for safeguard in _metadata_safeguards(metadata):
            safeguards.add(safeguard)
            evidence.append(f"metadata.circuit_breaker_safeguards: {safeguard}")

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_signals = _path_signals(text)
        if path_signals:
            for sig in path_signals:
                signals.add(sig)
            evidence.append(f"files_or_modules: {path}")
        path_safeguards = _safeguards_from_text(text)
        if path_safeguards:
            for sg in path_safeguards:
                safeguards.add(sg)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        text_signals = _text_signals(text)
        text_safeguards = _safeguards_from_text(text)
        if text_signals:
            for sig in text_signals:
                signals.add(sig)
            evidence.append(_evidence_snippet(source_field, text))
        if text_safeguards:
            for sg in text_safeguards:
                safeguards.add(sg)
            evidence.append(_evidence_snippet(source_field, text))

    for command in _validation_commands(task):
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        command_signals = {*_text_signals(command), *_text_signals(command_text)}
        command_safeguards = {*_safeguards_from_text(command), *_safeguards_from_text(command_text)}
        if command_signals:
            for sig in command_signals:
                signals.add(sig)
            evidence.append(_evidence_snippet("validation_commands", command))
        if command_safeguards:
            for sg in command_safeguards:
                safeguards.add(sg)
            evidence.append(_evidence_snippet("validation_commands", command))

    return _Matches(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signals),
        safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguards),
        evidence=tuple(_dedupe(evidence)),
    )


def _metadata_signals(metadata: Mapping[str, Any]) -> tuple[CircuitBreakerReadinessSignal, ...]:
    signals: set[CircuitBreakerReadinessSignal] = set()
    for key in _SIGNAL_KEYS:
        for value in _strings(metadata.get(key)):
            normalized = _normalized_key(value)
            alias = _ALIASES.get(normalized, normalized)
            if alias in _SIGNAL_ORDER:
                signals.add(cast(CircuitBreakerReadinessSignal, alias))
    return tuple(signal for signal in _SIGNAL_ORDER if signal in signals)


def _metadata_safeguards(metadata: Mapping[str, Any]) -> tuple[CircuitBreakerReadinessSafeguard, ...]:
    safeguards: set[CircuitBreakerReadinessSafeguard] = set()
    for key in _SAFEGUARD_KEYS:
        for value in _strings(metadata.get(key)):
            normalized = _normalized_key(value)
            alias = _ALIASES.get(normalized, normalized)
            if alias in _SAFEGUARD_ORDER:
                safeguards.add(cast(CircuitBreakerReadinessSafeguard, alias))
            safeguards.update(_safeguards_from_text(value))
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguards)


def _path_signals(text: str) -> tuple[CircuitBreakerReadinessSignal, ...]:
    signals = {signal for signal, pattern in _PATH_SIGNAL_PATTERNS if pattern.search(text)}
    return tuple(signal for signal in _SIGNAL_ORDER if signal in signals)


def _text_signals(text: str) -> tuple[CircuitBreakerReadinessSignal, ...]:
    matched = {signal for signal, pattern in _TEXT_SIGNAL_PATTERNS.items() if pattern.search(text)}
    # Broad match for circuit breaker mentions
    if not matched and re.search(r"\bcircuit[- ]?breaker\b", text, re.I):
        matched.add("failure_threshold")
    return tuple(signal for signal in _SIGNAL_ORDER if signal in matched)


def _safeguards_from_text(text: str) -> set[CircuitBreakerReadinessSafeguard]:
    normalized = _normalized_key(text)
    alias = _ALIASES.get(normalized, normalized)
    safeguards: set[CircuitBreakerReadinessSafeguard] = {
        safeguard
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items()
        if pattern.search(text) or alias == safeguard
    }
    return safeguards


def _impact_level(signals: tuple[CircuitBreakerReadinessSignal, ...]) -> CircuitBreakerReadinessImpactLevel:
    signal_set = set(signals)
    # High impact: failure threshold with fallback or monitoring
    if "failure_threshold" in signal_set and (
        "fallback_mechanism" in signal_set or "monitoring" in signal_set or "recovery_strategy" in signal_set
    ):
        return "high"
    # High impact: recovery with health checks
    if "recovery_strategy" in signal_set and "health_check" in signal_set:
        return "high"
    # High impact: fallback with health check or monitoring
    if "fallback_mechanism" in signal_set and (
        "health_check" in signal_set or "monitoring" in signal_set
    ):
        return "high"
    # Medium impact: any core resilience signals
    if {
        "failure_threshold",
        "recovery_strategy",
        "fallback_mechanism",
        "health_check",
        "timeout_handling",
    } & signal_set:
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskCircuitBreakerReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "record_count": len(records),
        "impacted_task_count": len(records),
        "no_impact_task_count": len(no_impact_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "impact_level_counts": {
            impact: sum(1 for record in records if record.impact_level == impact)
            for impact in _IMPACT_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.matched_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
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
        iterator = iter(cast(Iterable[object], source))
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
        "dependencies",
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
        "tags",
        "labels",
        "notes",
        "risks",
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
    for field_name in (
        "acceptance_criteria",
        "depends_on",
        "dependencies",
        "tags",
        "labels",
        "notes",
        "risks",
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
            if _metadata_key_has_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_has_signal(key_text):
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


def _metadata_key_has_signal(value: str) -> bool:
    return any(
        pattern.search(value)
        for pattern in [*_TEXT_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()]
    )


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
    return str(
        PurePosixPath(
            value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
        )
    )


def _normalized_key(value: str) -> str:
    return _text(value).casefold().replace("-", "_").replace(" ", "_").replace("/", "_")


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
    "CircuitBreakerReadinessImpactLevel",
    "CircuitBreakerReadinessSafeguard",
    "CircuitBreakerReadinessSignal",
    "TaskCircuitBreakerReadinessPlan",
    "TaskCircuitBreakerReadinessRecord",
    "analyze_task_circuit_breaker_readiness",
    "build_task_circuit_breaker_readiness_plan",
    "derive_task_circuit_breaker_readiness_plan",
    "summarize_task_circuit_breaker_readiness",
    "task_circuit_breaker_readiness_plan_to_dict",
    "task_circuit_breaker_readiness_plan_to_dicts",
    "task_circuit_breaker_readiness_plan_to_markdown",
]
