"""Plan maintenance window readiness safeguards."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


MaintenanceWindowSignal = Literal[
    "maintenance_window",
    "downtime",
    "degraded_mode",
    "customer_notice",
    "status_page",
    "freeze_window",
    "change_window",
    "traffic_drain",
    "rollback_window",
    "monitoring",
    "support_coverage",
    "post_window_validation",
    "owner_approval",
]
MaintenanceWindowSafeguard = Literal[
    "customer_notice",
    "status_page",
    "owner_approval",
    "rollback_window",
    "monitoring",
    "support_coverage",
    "post_window_validation",
]
MaintenanceWindowReadinessRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[MaintenanceWindowSignal, ...] = (
    "maintenance_window",
    "downtime",
    "degraded_mode",
    "customer_notice",
    "status_page",
    "freeze_window",
    "change_window",
    "traffic_drain",
    "rollback_window",
    "monitoring",
    "support_coverage",
    "post_window_validation",
    "owner_approval",
)
_SAFEGUARD_ORDER: tuple[MaintenanceWindowSafeguard, ...] = (
    "customer_notice",
    "status_page",
    "owner_approval",
    "rollback_window",
    "monitoring",
    "support_coverage",
    "post_window_validation",
)
_RISK_ORDER: dict[MaintenanceWindowReadinessRisk, int] = {"high": 0, "medium": 1, "low": 2}
_CORE_MAINTENANCE_SIGNALS = {
    "maintenance_window",
    "downtime",
    "degraded_mode",
    "freeze_window",
    "change_window",
    "traffic_drain",
}
_PATH_SIGNAL_PATTERNS: dict[MaintenanceWindowSignal, re.Pattern[str]] = {
    "maintenance_window": re.compile(r"(?:maintenance|scheduled[_-]?maintenance|maintenance[_-]?window)", re.I),
    "downtime": re.compile(r"(?:downtime|outage|service[_-]?interrupt|unavailable)", re.I),
    "degraded_mode": re.compile(r"(?:degraded|read[_-]?only|limited[_-]?mode|brownout)", re.I),
    "customer_notice": re.compile(r"(?:customer[_-]?notice|customer[_-]?comms|notification|announcement)", re.I),
    "status_page": re.compile(r"(?:statuspage|status[_-]?page|incident[_-]?status)", re.I),
    "freeze_window": re.compile(r"(?:freeze|code[_-]?freeze|deploy[_-]?freeze)", re.I),
    "change_window": re.compile(r"(?:change[_-]?window|change[_-]?calendar|change[_-]?request|cab)", re.I),
    "traffic_drain": re.compile(r"(?:traffic[_-]?drain|drain|quiesce|connection[_-]?drain)", re.I),
    "rollback_window": re.compile(r"(?:rollback|backout|revert|restore)", re.I),
    "monitoring": re.compile(r"(?:monitor|alert|probe|synthetic|health[_-]?check|smoke)", re.I),
    "support_coverage": re.compile(r"(?:support|staffing|oncall|on[_-]?call|coverage)", re.I),
    "post_window_validation": re.compile(r"(?:post[_-]?(?:maintenance|window)|validation|smoke|health[_-]?check)", re.I),
    "owner_approval": re.compile(r"(?:approval|signoff|sign[_-]?off|owner|change[_-]?approval)", re.I),
}
_TEXT_SIGNAL_PATTERNS: dict[MaintenanceWindowSignal, re.Pattern[str]] = {
    "maintenance_window": re.compile(
        r"\b(?:scheduled maintenance|planned maintenance|maintenance window|maintenance period|"
        r"service maintenance|maintenance event)\b",
        re.I,
    ),
    "downtime": re.compile(
        r"\b(?:downtime|service outage|planned outage|temporary outage|service interruption|"
        r"service unavailable|offline period)\b",
        re.I,
    ),
    "degraded_mode": re.compile(
        r"\b(?:degraded mode|degraded service|read[- ]only mode|limited mode|brownout|reduced capacity)\b",
        re.I,
    ),
    "customer_notice": re.compile(
        r"\b(?:customer notice|customer notification|notify customers|customer comms|customer communications|"
        r"maintenance notice|advance notice|email customers|announce maintenance)\b",
        re.I,
    ),
    "status_page": re.compile(r"\b(?:status page|statuspage|public status|status banner|incident status)\b", re.I),
    "freeze_window": re.compile(
        r"\b(?:freeze window|code freeze|deploy freeze|release freeze|change freeze|freeze period)\b",
        re.I,
    ),
    "change_window": re.compile(
        r"\b(?:change window|maintenance change|change calendar|change request|cab approval|change advisory)\b",
        re.I,
    ),
    "traffic_drain": re.compile(
        r"\b(?:traffic drain|drain traffic|connection drain|drain connections|quiesce traffic|"
        r"stop accepting traffic|disable writes)\b",
        re.I,
    ),
    "rollback_window": re.compile(
        r"\b(?:rollback window|rollback plan|roll back|rollback|backout|back out|revert|restore service|"
        r"abort criteria|abort plan)\b",
        re.I,
    ),
    "monitoring": re.compile(
        r"\b(?:monitoring|alerts?|dashboards?|synthetic checks?|synthetic probes?|health checks?|"
        r"smoke tests?|slo|error budget|metrics)\b",
        re.I,
    ),
    "support_coverage": re.compile(
        r"\b(?:support coverage|support staffing|support staffed|support team|customer support|"
        r"on[- ]?call|war room|incident commander|escalation channel)\b",
        re.I,
    ),
    "post_window_validation": re.compile(
        r"\b(?:post[- ]maintenance validation|post[- ]window validation|post[- ]change validation|"
        r"after maintenance validation|post maintenance checks?|final smoke tests?|validate after the window|"
        r"post window health checks?)\b",
        re.I,
    ),
    "owner_approval": re.compile(
        r"\b(?:owner approval|service owner|release owner|change owner|approved by|sign[- ]?off|"
        r"signoff|change approval|cab approval)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[MaintenanceWindowSafeguard, re.Pattern[str]] = {
    safeguard: _TEXT_SIGNAL_PATTERNS[safeguard] for safeguard in _SAFEGUARD_ORDER
}
_RECOMMENDED_STEPS: dict[MaintenanceWindowSafeguard, str] = {
    "customer_notice": "Prepare customer notice with timing, expected impact, affected services, and completion updates.",
    "status_page": "Schedule or draft the status page entry before the window and define update cadence.",
    "owner_approval": "Attach service, release, support, or change owner approval before the maintenance window starts.",
    "rollback_window": "Define rollback triggers, responsible owners, and the latest safe rollback time inside the window.",
    "monitoring": "Pin dashboards, alerts, synthetic checks, and health metrics for the window and rollback period.",
    "support_coverage": "Confirm support staffing, on-call escalation, and customer-response coverage during the window.",
    "post_window_validation": "List post-window validation commands, smoke tests, and owner sign-off for returning to normal operations.",
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:downtime|maintenance|maintenance window|customer impact|service interruption)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|expected)\b|"
    r"\b(?:no[- ]?downtime|zero[- ]?downtime|no[- ]?maintenance)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class TaskMaintenanceWindowReadinessRecord:
    """Readiness guidance for one maintenance window task."""

    task_id: str
    title: str
    detected_signals: tuple[MaintenanceWindowSignal, ...]
    present_safeguards: tuple[MaintenanceWindowSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[MaintenanceWindowSafeguard, ...] = field(default_factory=tuple)
    risk_level: MaintenanceWindowReadinessRisk = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_readiness_steps: tuple[str, ...] = field(default_factory=tuple)

    @property
    def readiness_level(self) -> MaintenanceWindowReadinessRisk:
        """Compatibility view for older callers that used readiness_level."""
        return self.risk_level

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "evidence": list(self.evidence),
            "recommended_readiness_steps": list(self.recommended_readiness_steps),
        }


@dataclass(frozen=True, slots=True)
class TaskMaintenanceWindowReadinessPlan:
    """Plan-level maintenance window readiness review."""

    plan_id: str | None = None
    records: tuple[TaskMaintenanceWindowReadinessRecord, ...] = field(default_factory=tuple)
    maintenance_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def affected_task_ids(self) -> tuple[str, ...]:
        """Compatibility view for older callers."""
        return self.maintenance_task_ids

    @property
    def no_signal_task_ids(self) -> tuple[str, ...]:
        """Compatibility view for older callers."""
        return self.not_applicable_task_ids

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "maintenance_task_ids": list(self.maintenance_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render maintenance window readiness as deterministic Markdown."""
        title = "# Task Maintenance Window Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Maintenance task count: {self.summary.get('maintenance_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No maintenance window readiness records were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Signals | Present Safeguards | Missing Safeguards | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not-applicable tasks: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_task_maintenance_window_readiness_plan(source: Any) -> TaskMaintenanceWindowReadinessPlan:
    """Build readiness records for maintenance window tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (_RISK_ORDER[record.risk_level], record.task_id, record.title.casefold()),
        )
    )
    maintenance_task_ids = tuple(record.task_id for record in records)
    not_applicable_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskMaintenanceWindowReadinessPlan(
        plan_id=plan_id,
        records=records,
        maintenance_task_ids=maintenance_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, task_count=len(tasks), not_applicable_task_ids=not_applicable_task_ids),
    )


def analyze_task_maintenance_window_readiness(source: Any) -> TaskMaintenanceWindowReadinessPlan:
    """Compatibility alias for building maintenance window readiness plans."""
    return build_task_maintenance_window_readiness_plan(source)


def summarize_task_maintenance_window_readiness(source: Any) -> TaskMaintenanceWindowReadinessPlan:
    """Compatibility alias for building maintenance window readiness plans."""
    return build_task_maintenance_window_readiness_plan(source)


def extract_task_maintenance_window_readiness(source: Any) -> TaskMaintenanceWindowReadinessPlan:
    """Compatibility alias for extracting maintenance window readiness plans."""
    return build_task_maintenance_window_readiness_plan(source)


def generate_task_maintenance_window_readiness(source: Any) -> TaskMaintenanceWindowReadinessPlan:
    """Compatibility alias for generating maintenance window readiness plans."""
    return build_task_maintenance_window_readiness_plan(source)


def task_maintenance_window_readiness_plan_to_dict(result: TaskMaintenanceWindowReadinessPlan) -> dict[str, Any]:
    """Serialize a maintenance window readiness plan to a plain dictionary."""
    return result.to_dict()


task_maintenance_window_readiness_plan_to_dict.__test__ = False


def task_maintenance_window_readiness_plan_to_dicts(
    result: TaskMaintenanceWindowReadinessPlan | Iterable[TaskMaintenanceWindowReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize maintenance window readiness records to plain dictionaries."""
    if isinstance(result, TaskMaintenanceWindowReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_maintenance_window_readiness_plan_to_dicts.__test__ = False


def task_maintenance_window_readiness_plan_to_markdown(result: TaskMaintenanceWindowReadinessPlan) -> str:
    """Render a maintenance window readiness plan as Markdown."""
    return result.to_markdown()


task_maintenance_window_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[MaintenanceWindowSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[MaintenanceWindowSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    explicitly_no_impact: bool = False


def _task_record(task: Mapping[str, Any], index: int) -> TaskMaintenanceWindowReadinessRecord | None:
    signals = _signals(task)
    if signals.explicitly_no_impact or not (set(signals.signals) & _CORE_MAINTENANCE_SIGNALS):
        return None

    missing = _missing_safeguards(signals.signals, signals.present_safeguards)
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    return TaskMaintenanceWindowReadinessRecord(
        task_id=task_id,
        title=title,
        detected_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(set(signals.signals), set(signals.present_safeguards), missing),
        evidence=signals.evidence,
        recommended_readiness_steps=tuple(_RECOMMENDED_STEPS[safeguard] for safeguard in missing),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[MaintenanceWindowSignal] = set()
    safeguard_hits: set[MaintenanceWindowSafeguard] = set()
    evidence: list[str] = []
    explicitly_no_impact = False

    for path in _strings(task.get("files_or_modules") or task.get("files") or task.get("paths")):
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
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        if _NO_IMPACT_RE.search(text):
            explicitly_no_impact = True
        snippet = _evidence_snippet(source_field, text)
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(snippet)

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        evidence=tuple(_dedupe(evidence)),
        explicitly_no_impact=explicitly_no_impact,
    )


def _missing_safeguards(
    signals: tuple[MaintenanceWindowSignal, ...],
    present_safeguards: tuple[MaintenanceWindowSafeguard, ...],
) -> tuple[MaintenanceWindowSafeguard, ...]:
    signal_set = set(signals)
    required: set[MaintenanceWindowSafeguard] = {
        "customer_notice",
        "status_page",
        "owner_approval",
        "rollback_window",
        "monitoring",
        "support_coverage",
        "post_window_validation",
    }
    if signal_set == {"freeze_window"}:
        required = {"owner_approval", "support_coverage", "post_window_validation"}
    present = set(present_safeguards)
    return tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in required and safeguard not in present)


def _risk_level(
    signals: set[MaintenanceWindowSignal],
    present: set[MaintenanceWindowSafeguard],
    missing: tuple[MaintenanceWindowSafeguard, ...],
) -> MaintenanceWindowReadinessRisk:
    if not missing:
        return "low"
    missing_set = set(missing)
    if {"downtime", "degraded_mode"} & signals and {"customer_notice", "status_page"} & missing_set:
        return "high"
    if "traffic_drain" in signals and {"rollback_window", "monitoring"} & missing_set:
        return "high"
    if {"rollback_window", "monitoring", "post_window_validation"} <= missing_set:
        return "high"
    if len(missing) >= 5:
        return "high"
    if present:
        return "medium"
    return "medium"


def _summary(
    records: tuple[TaskMaintenanceWindowReadinessRecord, ...],
    *,
    task_count: int,
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "maintenance_task_count": len(records),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {risk: sum(1 for record in records if record.risk_level == risk) for risk in _RISK_ORDER},
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
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
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
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
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "risks", "depends_on"):
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
    return str(PurePosixPath(value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")))


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


MaintenanceWindowReadinessLevel = MaintenanceWindowReadinessRisk

__all__ = [
    "MaintenanceWindowReadinessLevel",
    "MaintenanceWindowReadinessRisk",
    "MaintenanceWindowSafeguard",
    "MaintenanceWindowSignal",
    "TaskMaintenanceWindowReadinessPlan",
    "TaskMaintenanceWindowReadinessRecord",
    "analyze_task_maintenance_window_readiness",
    "build_task_maintenance_window_readiness_plan",
    "extract_task_maintenance_window_readiness",
    "generate_task_maintenance_window_readiness",
    "summarize_task_maintenance_window_readiness",
    "task_maintenance_window_readiness_plan_to_dict",
    "task_maintenance_window_readiness_plan_to_dicts",
    "task_maintenance_window_readiness_plan_to_markdown",
]
