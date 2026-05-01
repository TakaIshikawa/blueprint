"""Identify execution tasks with likely observability planning gaps."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


TelemetryArea = Literal[
    "logs",
    "metrics",
    "traces",
    "dashboards",
    "alerts",
    "audit_events",
    "slos",
    "runbooks",
]
GapSeverity = Literal["none", "low", "medium", "high"]
RiskLevel = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_TELEMETRY_ORDER: dict[TelemetryArea, int] = {
    "logs": 0,
    "metrics": 1,
    "traces": 2,
    "dashboards": 3,
    "alerts": 4,
    "audit_events": 5,
    "slos": 6,
    "runbooks": 7,
}
_SIGNAL_ORDER = (
    "service",
    "api",
    "worker",
    "queue",
    "cron",
    "integration",
    "payment",
    "import_export",
    "data_pipeline",
)
_SEVERITY_RANK: dict[GapSeverity, int] = {
    "none": 0,
    "low": 1,
    "medium": 2,
    "high": 3,
}

_SIGNAL_PATTERNS: dict[str, re.Pattern[str]] = {
    "service": re.compile(
        r"\b(?:service|backend|server|controller|handler|grpc|rest)\b",
        re.IGNORECASE,
    ),
    "api": re.compile(r"\b(?:api|endpoint|route|webhook|graphql|http)\b", re.I),
    "worker": re.compile(r"\b(?:worker|job|async|background|task runner|retry)\b", re.I),
    "queue": re.compile(r"\b(?:queue|queued|dead[- ]?letter|dlq|kafka|sqs|pubsub)\b", re.I),
    "cron": re.compile(r"\b(?:cron|scheduled|scheduler)\b", re.I),
    "integration": re.compile(
        r"\b(?:integration|third[- ]?party|external|vendor|provider|callback|sync|"
        r"api client|stripe|slack|github|salesforce|netsuite)\b",
        re.I,
    ),
    "payment": re.compile(
        r"\b(?:payment|payments|billing|checkout|invoice|subscription|refund|chargeback)\b",
        re.I,
    ),
    "import_export": re.compile(r"\b(?:import|imports|export|exports|csv|etl)\b", re.I),
    "data_pipeline": re.compile(
        r"\b(?:data pipeline|pipeline|backfill|batch|warehouse|stream|transform|"
        r"ingestion|replication)\b",
        re.I,
    ),
}
_FILE_SIGNAL_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("service", re.compile(r"(?:^|/)(?:services?|server|backend|handlers?)(?:/|$)", re.I)),
    ("api", re.compile(r"(?:^|/)(?:api|routes?|controllers?|webhooks?)(?:/|$)", re.I)),
    ("worker", re.compile(r"(?:^|/)(?:workers?|jobs?|tasks?)(?:/|$)", re.I)),
    ("queue", re.compile(r"(?:^|/)(?:queues?|consumers?|producers?|dlq)(?:/|$)", re.I)),
    ("cron", re.compile(r"(?:^|/)(?:cron|schedulers?)(?:/|$)", re.I)),
    ("integration", re.compile(r"(?:^|/)(?:integrations?|clients?|adapters?|providers?)(?:/|$)", re.I)),
    ("payment", re.compile(r"(?:^|/)(?:billing|payments?|checkout|invoices?)(?:/|$)", re.I)),
    ("import_export", re.compile(r"(?:^|/)(?:imports?|exports?|etl)(?:/|$)|(?:import|export)\w*\.", re.I)),
    ("data_pipeline", re.compile(r"(?:^|/)(?:pipelines?|warehouse|backfills?|streams?)(?:/|$)", re.I)),
)
_COVERAGE_PATTERNS: dict[TelemetryArea, re.Pattern[str]] = {
    "logs": re.compile(r"\b(?:log|logs|logging|structured log|logger)\b", re.I),
    "metrics": re.compile(r"\b(?:metric|metrics|counter|histogram|latency|duration|rate)\b", re.I),
    "traces": re.compile(r"\b(?:trace|traces|tracing|span|opentelemetry|otel)\b", re.I),
    "dashboards": re.compile(r"\b(?:dashboard|dashboards|panel|panels|chart|grafana|datadog)\b", re.I),
    "alerts": re.compile(r"\b(?:alert|alerts|paging|pager|on-call|oncall|threshold)\b", re.I),
    "audit_events": re.compile(r"\b(?:audit|audit event|audit events|event log)\b", re.I),
    "slos": re.compile(r"\b(?:slo|slos|sla|service level|error budget)\b", re.I),
    "runbooks": re.compile(r"\b(?:runbook|runbooks|playbook|operational docs|ops docs)\b", re.I),
}


@dataclass(frozen=True, slots=True)
class TaskObservabilityGapRow:
    """One task's inferred observability expectations and missing coverage."""

    task_id: str
    title: str
    risk_level: RiskLevel
    gap_severity: GapSeverity
    operational_signals: tuple[str, ...] = field(default_factory=tuple)
    expected_telemetry: tuple[TelemetryArea, ...] = field(default_factory=tuple)
    covered_telemetry: tuple[TelemetryArea, ...] = field(default_factory=tuple)
    missing_coverage: tuple[TelemetryArea, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_validation_steps: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "risk_level": self.risk_level,
            "gap_severity": self.gap_severity,
            "operational_signals": list(self.operational_signals),
            "expected_telemetry": list(self.expected_telemetry),
            "covered_telemetry": list(self.covered_telemetry),
            "missing_coverage": list(self.missing_coverage),
            "evidence": list(self.evidence),
            "recommended_validation_steps": list(self.recommended_validation_steps),
        }


@dataclass(frozen=True, slots=True)
class TaskObservabilityGapPlan:
    """Plan-level observability gap analysis."""

    plan_id: str | None = None
    rows: tuple[TaskObservabilityGapRow, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return gap rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]


def analyze_task_observability_gaps(
    source: Mapping[str, Any]
    | ExecutionPlan
    | ExecutionTask
    | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskObservabilityGapPlan:
    """Find operational tasks whose observability expectations are missing or incomplete."""
    plan_id, tasks = _source_payload(source)
    rows = tuple(
        row
        for index, task in enumerate(tasks, start=1)
        if (row := _gap_row(task, index)) is not None
    )
    return TaskObservabilityGapPlan(
        plan_id=plan_id,
        rows=rows,
        summary={
            "task_count": len(tasks),
            "operational_task_count": len(rows),
            "gap_count": sum(1 for row in rows if row.missing_coverage),
            "covered_count": sum(1 for row in rows if not row.missing_coverage),
            "severity_counts": _severity_counts(row.gap_severity for row in rows),
        },
    )


def task_observability_gaps_to_dict(result: TaskObservabilityGapPlan) -> dict[str, Any]:
    """Serialize task observability gaps to a plain dictionary."""
    return result.to_dict()


task_observability_gaps_to_dict.__test__ = False


def summarize_task_observability_gaps(
    source: Mapping[str, Any]
    | ExecutionPlan
    | ExecutionTask
    | Iterable[Mapping[str, Any] | ExecutionTask],
) -> TaskObservabilityGapPlan:
    """Compatibility alias for task observability gap analysis."""
    return analyze_task_observability_gaps(source)


def _gap_row(task: Mapping[str, Any], index: int) -> TaskObservabilityGapRow | None:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    risk_level = _risk_level(task.get("risk_level") or task.get("risk"))
    signals = _operational_signals(task)
    if not signals:
        return None

    signal_names = tuple(signal for signal in _SIGNAL_ORDER if signal in signals)
    expected = _expected_telemetry(set(signal_names), risk_level)
    covered = tuple(area for area in expected if area in _coverage_signals(task))
    missing = tuple(area for area in expected if area not in covered)
    severity = _gap_severity(missing, set(signal_names), risk_level)
    return TaskObservabilityGapRow(
        task_id=task_id,
        title=title,
        risk_level=risk_level,
        gap_severity=severity,
        operational_signals=signal_names,
        expected_telemetry=expected,
        covered_telemetry=covered,
        missing_coverage=missing,
        evidence=tuple(_evidence(signals, signal_names)),
        recommended_validation_steps=_validation_steps(missing, set(signal_names), risk_level),
    )


def _operational_signals(task: Mapping[str, Any]) -> dict[str, tuple[str, ...]]:
    signals: dict[str, list[str]] = {}
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        for signal, pattern in _FILE_SIGNAL_PATTERNS:
            if pattern.search(normalized):
                _append(signals, signal, f"files_or_modules: {path}")

    for source_field, text in _task_texts(task):
        _add_operational_text_signals(signals, source_field, text)
    for source_field, text in _metadata_texts(task.get("metadata")):
        _add_operational_text_signals(signals, source_field, text)

    return {signal: tuple(_dedupe(values)) for signal, values in signals.items()}


def _add_operational_text_signals(
    signals: dict[str, list[str]],
    source_field: str,
    text: str,
) -> None:
    evidence = f"{source_field}: {text}"
    for signal, pattern in _SIGNAL_PATTERNS.items():
        if pattern.search(text):
            _append(signals, signal, evidence)


def _coverage_signals(task: Mapping[str, Any]) -> set[TelemetryArea]:
    coverage: set[TelemetryArea] = set()
    for source_field, text in _task_texts(task):
        _add_coverage_signals(coverage, source_field, text)
    for source_field, text in _metadata_texts(task.get("metadata")):
        _add_coverage_signals(coverage, source_field, text)
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path).casefold()
        if any(part in {"observability", "telemetry", "monitoring"} for part in PurePosixPath(normalized).parts):
            coverage.update({"logs", "metrics", "dashboards", "alerts"})
        for area, pattern in _COVERAGE_PATTERNS.items():
            if pattern.search(path):
                coverage.add(area)
    return coverage


def _add_coverage_signals(coverage: set[TelemetryArea], source_field: str, text: str) -> None:
    combined = f"{source_field} {text}"
    for area, pattern in _COVERAGE_PATTERNS.items():
        if pattern.search(combined):
            coverage.add(area)


def _expected_telemetry(signals: set[str], risk_level: RiskLevel) -> tuple[TelemetryArea, ...]:
    expected: set[TelemetryArea] = {"logs", "metrics"}
    if signals & {"service", "api", "integration", "payment"}:
        expected.add("traces")
    if signals & {"worker", "queue", "cron", "integration", "payment", "import_export", "data_pipeline"}:
        expected.update({"dashboards", "alerts", "runbooks"})
    if signals & {"payment", "import_export", "data_pipeline"}:
        expected.add("audit_events")
    if signals & {"api", "service", "payment"}:
        expected.add("slos")
    if risk_level == "high":
        expected.update({"dashboards", "alerts", "runbooks"})
    return tuple(sorted(expected, key=lambda area: _TELEMETRY_ORDER[area]))


def _gap_severity(
    missing: tuple[TelemetryArea, ...],
    signals: set[str],
    risk_level: RiskLevel,
) -> GapSeverity:
    if not missing:
        return "none"
    severity: GapSeverity = "low"
    if len(missing) >= 3 or signals & {"integration", "payment", "queue", "data_pipeline"}:
        severity = "medium"
    if risk_level == "high" or (
        signals & {"payment", "integration", "data_pipeline"} and {"alerts", "runbooks"} <= set(missing)
    ):
        severity = "high"
    return severity


def _validation_steps(
    missing: tuple[TelemetryArea, ...],
    signals: set[str],
    risk_level: RiskLevel,
) -> tuple[str, ...]:
    if not missing:
        return ("Verify the referenced telemetry in the same validation run as the task change.",)

    steps: list[str] = []
    if "logs" in missing:
        steps.append("Add validation that success and failure logs include correlation identifiers.")
    if "metrics" in missing:
        steps.append("Validate counters, failure rates, and latency or duration metrics for the changed path.")
    if "traces" in missing:
        steps.append("Exercise the changed request or dependency boundary and confirm trace propagation.")
    if "dashboards" in missing:
        steps.append("Confirm the task has a dashboard panel or documented dashboard location.")
    if "alerts" in missing:
        steps.append("Define and test an actionable alert for sustained failures or stalled work.")
    if "audit_events" in missing:
        steps.append("Validate audit events include actor, action, target, outcome, and timestamp.")
    if "slos" in missing:
        steps.append("Document the SLO or service-level threshold used to judge production health.")
    if "runbooks" in missing:
        steps.append("Add a runbook step for diagnosis, rollback, or manual recovery.")
    if risk_level == "high" or signals & {"integration", "payment", "data_pipeline"}:
        steps.append("Run a failure-mode validation for provider errors, retries, and operator handoff.")
    return tuple(_dedupe(steps))


def _evidence(
    signals: Mapping[str, tuple[str, ...]],
    signal_names: Iterable[str],
) -> list[str]:
    evidence: list[str] = []
    for signal in signal_names:
        evidence.extend(signals.get(signal, ()))
    return _dedupe(evidence)


def _source_payload(
    source: Mapping[str, Any]
    | ExecutionPlan
    | ExecutionTask
    | Iterable[Mapping[str, Any] | ExecutionTask],
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

    tasks: list[dict[str, Any]] = []
    for item in source:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
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
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "test_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
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


def _risk_level(value: Any) -> RiskLevel:
    text = (_optional_text(value) or "").casefold()
    if text in {"critical", "blocker", "high"}:
        return "high"
    if text in {"medium", "moderate"}:
        return "medium"
    return "low"


def _severity_counts(values: Iterable[GapSeverity]) -> dict[GapSeverity, int]:
    counts: dict[GapSeverity, int] = {
        "none": 0,
        "low": 0,
        "medium": 0,
        "high": 0,
    }
    for value in values:
        counts[value] += 1
    return counts


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


def _append(signals: dict[str, list[str]], signal: str, evidence: str) -> None:
    signals.setdefault(signal, []).append(evidence)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
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


__all__ = [
    "GapSeverity",
    "RiskLevel",
    "TaskObservabilityGapPlan",
    "TaskObservabilityGapRow",
    "TelemetryArea",
    "analyze_task_observability_gaps",
    "summarize_task_observability_gaps",
    "task_observability_gaps_to_dict",
]
