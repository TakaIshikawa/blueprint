"""Detect deployment timing conflicts against freeze and release calendars."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


DeploymentFreezeConflictSeverity = Literal["hard_conflict", "warning", "informational"]
DeploymentFreezeConstraintType = Literal[
    "freeze_window",
    "blackout",
    "business_hours",
    "change_management",
    "calendar_hint",
    "inferred_timing",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SEVERITY_ORDER: dict[DeploymentFreezeConflictSeverity, int] = {
    "hard_conflict": 0,
    "warning": 1,
    "informational": 2,
}
_CONSTRAINT_ORDER: dict[DeploymentFreezeConstraintType, int] = {
    "freeze_window": 0,
    "blackout": 1,
    "change_management": 2,
    "business_hours": 3,
    "calendar_hint": 4,
    "inferred_timing": 5,
}
_METADATA_CONSTRAINT_KEYS: tuple[tuple[DeploymentFreezeConstraintType, tuple[str, ...]], ...] = (
    (
        "freeze_window",
        (
            "freeze_window",
            "freeze_windows",
            "deployment_freeze",
            "deployment_freezes",
            "deployment_freeze_windows",
            "production_freeze",
            "production_freeze_windows",
        ),
    ),
    ("blackout", ("blackout", "blackouts", "blackout_date", "blackout_dates", "blackout_windows")),
    (
        "business_hours",
        (
            "business_hours",
            "deployment_hours",
            "change_window",
            "change_windows",
            "allowed_deployment_windows",
        ),
    ),
    (
        "change_management",
        (
            "change_management",
            "change_management_gate",
            "change_management_gates",
            "change_gates",
            "cab",
            "required_approvals",
        ),
    ),
    (
        "calendar_hint",
        (
            "release_calendar",
            "launch_calendar",
            "calendar_hints",
            "release_hints",
            "launch_dates",
        ),
    ),
)
_DEPLOYMENT_RE = re.compile(
    r"\b(?:deploy|deployment|release|rollout|ship|go[- ]?live|production launch|launch)\b",
    re.I,
)
_PRODUCTION_RE = re.compile(r"\b(?:production|prod|live traffic|customer traffic|go[- ]?live)\b", re.I)
_MIGRATION_CUTOVER_RE = re.compile(
    r"\b(?:cutover|migration|migrate|database migration|schema migration|backfill)\b",
    re.I,
)
_FREEZE_BLACKOUT_RE = re.compile(r"\b(?:freeze|frozen|blackout|moratorium|no[- ]?deploy)\b", re.I)
_CALENDAR_RE = re.compile(
    r"\b(?:holiday|weekend|launch date|launch window|release date|quarter[- ]?end|year[- ]?end)\b",
    re.I,
)
_BUSINESS_HOURS_RE = re.compile(
    r"\b(?:after[- ]?hours|outside business hours|business hours|overnight|off[- ]?hours)\b",
    re.I,
)
_CHANGE_GATE_RE = re.compile(
    r"\b(?:change management|change advisory|cab|approval|approver|change ticket|change request)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class PlanDeploymentFreezeConflictRecord:
    """One deployment timing conflict or calendar warning for a task."""

    task_id: str
    title: str
    window_or_constraint: str
    severity: DeploymentFreezeConflictSeverity
    recommended_scheduling_action: str
    required_approvals: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    constraint_type: DeploymentFreezeConstraintType = "inferred_timing"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "window_or_constraint": self.window_or_constraint,
            "severity": self.severity,
            "recommended_scheduling_action": self.recommended_scheduling_action,
            "required_approvals": list(self.required_approvals),
            "evidence": list(self.evidence),
            "constraint_type": self.constraint_type,
        }


@dataclass(frozen=True, slots=True)
class PlanDeploymentFreezeConflictReport:
    """Plan-level deployment freeze and release calendar conflict report."""

    plan_id: str | None = None
    records: tuple[PlanDeploymentFreezeConflictRecord, ...] = field(default_factory=tuple)
    conflicted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_conflict_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "conflicted_task_ids": list(self.conflicted_task_ids),
            "no_conflict_task_ids": list(self.no_conflict_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return conflict records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render the conflict report as deterministic Markdown."""
        title = "# Plan Deployment Freeze Conflict Report"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        severity_counts = self.summary.get("severity_counts", {})
        summary = (
            f"Summary: {self.summary.get('conflict_count', 0)} conflicts across "
            f"{self.summary.get('conflicted_task_count', 0)} tasks "
            f"(hard_conflict: {severity_counts.get('hard_conflict', 0)}, "
            f"warning: {severity_counts.get('warning', 0)}, "
            f"informational: {severity_counts.get('informational', 0)})."
        )
        lines = [title, "", summary]
        if not self.records:
            lines.extend(["", "No deployment freeze conflicts were detected."])
            if self.no_conflict_task_ids:
                lines.extend(["", f"No-conflict tasks: {_markdown_cell(', '.join(self.no_conflict_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Severity | Window / Constraint | Scheduling Action | Required Approvals | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` {_markdown_cell(record.title)} | "
                f"{record.severity} | "
                f"{_markdown_cell(record.window_or_constraint)} | "
                f"{_markdown_cell(record.recommended_scheduling_action)} | "
                f"{_markdown_cell(', '.join(record.required_approvals) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_conflict_task_ids:
            lines.extend(["", f"No-conflict tasks: {_markdown_cell(', '.join(self.no_conflict_task_ids))}"])
        return "\n".join(lines)


def build_plan_deployment_freeze_conflict_report(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanDeploymentFreezeConflictReport:
    """Build a deployment freeze conflict report for an execution plan."""
    plan_id, metadata, tasks = _source_payload(source)
    constraints = _metadata_constraints(metadata)
    task_records: list[tuple[str, tuple[PlanDeploymentFreezeConflictRecord, ...]]] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        records = _task_conflicts(task, index, constraints)
        task_records.append((task_id, records))

    records = tuple(
        sorted(
            (record for _, task_conflicts in task_records for record in task_conflicts),
            key=lambda record: (
                _SEVERITY_ORDER[record.severity],
                _CONSTRAINT_ORDER[record.constraint_type],
                record.task_id,
                record.window_or_constraint.casefold(),
                record.title.casefold(),
            ),
        )
    )
    conflicted_task_ids = tuple(_dedupe(record.task_id for record in records))
    no_conflict_task_ids = tuple(task_id for task_id, task_conflicts in task_records if not task_conflicts)
    severity_counts = {
        severity: sum(1 for record in records if record.severity == severity)
        for severity in _SEVERITY_ORDER
    }
    constraint_counts = {
        constraint_type: sum(1 for record in records if record.constraint_type == constraint_type)
        for constraint_type in _CONSTRAINT_ORDER
    }
    return PlanDeploymentFreezeConflictReport(
        plan_id=plan_id,
        records=records,
        conflicted_task_ids=conflicted_task_ids,
        no_conflict_task_ids=no_conflict_task_ids,
        summary={
            "task_count": len(tasks),
            "conflict_count": len(records),
            "conflicted_task_count": len(conflicted_task_ids),
            "no_conflict_task_count": len(no_conflict_task_ids),
            "severity_counts": severity_counts,
            "constraint_counts": constraint_counts,
        },
    )


def analyze_plan_deployment_freeze_conflicts(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanDeploymentFreezeConflictReport:
    """Compatibility alias for building deployment freeze conflict reports."""
    return build_plan_deployment_freeze_conflict_report(source)


def summarize_plan_deployment_freeze_conflicts(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> PlanDeploymentFreezeConflictReport:
    """Compatibility alias for building deployment freeze conflict reports."""
    return build_plan_deployment_freeze_conflict_report(source)


def plan_deployment_freeze_conflict_report_to_dict(
    result: PlanDeploymentFreezeConflictReport,
) -> dict[str, Any]:
    """Serialize a deployment freeze conflict report to a plain dictionary."""
    return result.to_dict()


plan_deployment_freeze_conflict_report_to_dict.__test__ = False


def plan_deployment_freeze_conflict_report_to_markdown(
    result: PlanDeploymentFreezeConflictReport,
) -> str:
    """Render a deployment freeze conflict report as Markdown."""
    return result.to_markdown()


plan_deployment_freeze_conflict_report_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Constraint:
    constraint_type: DeploymentFreezeConstraintType
    label: str
    approvals: tuple[str, ...]
    evidence: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _TaskSignals:
    deployment: bool = False
    production: bool = False
    migration_cutover: bool = False
    freeze_blackout: bool = False
    calendar_hint: bool = False
    business_hours: bool = False
    change_gate: bool = False
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def deployment_like(self) -> bool:
        return self.deployment or self.production or self.migration_cutover


def _task_conflicts(
    task: Mapping[str, Any],
    index: int,
    constraints: tuple[_Constraint, ...],
) -> tuple[PlanDeploymentFreezeConflictRecord, ...]:
    signals = _task_signals(task)
    if not signals.deployment_like and not (signals.freeze_blackout or signals.calendar_hint):
        return ()

    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    records: list[PlanDeploymentFreezeConflictRecord] = []
    for constraint in constraints:
        severity = _severity(signals, constraint.constraint_type)
        if severity is None:
            continue
        records.append(
            PlanDeploymentFreezeConflictRecord(
                task_id=task_id,
                title=title,
                window_or_constraint=constraint.label,
                severity=severity,
                recommended_scheduling_action=_scheduling_action(severity, constraint.constraint_type),
                required_approvals=_required_approvals(severity, constraint, signals),
                evidence=tuple(_dedupe([*constraint.evidence, *signals.evidence])),
                constraint_type=constraint.constraint_type,
            )
        )

    if not records:
        inferred = _inferred_constraint(signals)
        if inferred is not None:
            severity = _severity(signals, inferred.constraint_type) or "informational"
            records.append(
                PlanDeploymentFreezeConflictRecord(
                    task_id=task_id,
                    title=title,
                    window_or_constraint=inferred.label,
                    severity=severity,
                    recommended_scheduling_action=_scheduling_action(severity, inferred.constraint_type),
                    required_approvals=_required_approvals(severity, inferred, signals),
                    evidence=tuple(_dedupe([*inferred.evidence, *signals.evidence])),
                    constraint_type=inferred.constraint_type,
                )
            )
    return tuple(records)


def _severity(
    signals: _TaskSignals,
    constraint_type: DeploymentFreezeConstraintType,
) -> DeploymentFreezeConflictSeverity | None:
    if constraint_type in {"freeze_window", "blackout"}:
        if signals.deployment_like or signals.freeze_blackout:
            return "hard_conflict" if signals.production or signals.deployment else "warning"
        return None
    if constraint_type == "change_management":
        return "warning" if signals.deployment_like or signals.change_gate else None
    if constraint_type == "business_hours":
        return "warning" if signals.deployment_like or signals.business_hours else None
    if constraint_type == "calendar_hint":
        return "informational" if signals.deployment_like or signals.calendar_hint else None
    if constraint_type == "inferred_timing":
        if signals.freeze_blackout and (signals.deployment_like or signals.production):
            return "hard_conflict"
        if (signals.business_hours or signals.change_gate or signals.migration_cutover) and signals.deployment_like:
            return "warning"
        if signals.calendar_hint or signals.deployment_like:
            return "informational"
    return None


def _scheduling_action(
    severity: DeploymentFreezeConflictSeverity,
    constraint_type: DeploymentFreezeConstraintType,
) -> str:
    if severity == "hard_conflict":
        return "Move the production deployment outside the freeze or blackout window before queueing execution."
    if constraint_type == "change_management":
        return "Schedule only after the change-management gate is approved and attached to the release task."
    if constraint_type == "business_hours":
        return "Confirm the task is scheduled inside the approved deployment window or document an exception."
    if severity == "warning":
        return "Hold scheduling until the timing constraint is reviewed by the release owner."
    return "Treat the calendar signal as a scheduling hint and confirm the target launch date with the release owner."


def _required_approvals(
    severity: DeploymentFreezeConflictSeverity,
    constraint: _Constraint,
    signals: _TaskSignals,
) -> tuple[str, ...]:
    approvals = list(constraint.approvals)
    if severity == "hard_conflict":
        approvals.extend(["Release manager approval", "Change advisory board approval"])
    elif constraint.constraint_type == "change_management" or signals.change_gate:
        approvals.append("Change manager approval")
    elif severity == "warning":
        approvals.append("Release owner approval")
    return tuple(_dedupe(approvals))


def _inferred_constraint(signals: _TaskSignals) -> _Constraint | None:
    if signals.freeze_blackout:
        return _Constraint(
            "inferred_timing",
            "Inferred freeze or blackout timing from task text",
            (),
            ("inferred: task mentions freeze or blackout timing",),
        )
    if signals.business_hours:
        return _Constraint(
            "inferred_timing",
            "Inferred business-hour deployment constraint from task text",
            (),
            ("inferred: task mentions business-hour or after-hours timing",),
        )
    if signals.change_gate:
        return _Constraint(
            "inferred_timing",
            "Inferred change-management gate from task text",
            (),
            ("inferred: task mentions change approval or CAB gate",),
        )
    if signals.calendar_hint and signals.deployment_like:
        return _Constraint(
            "inferred_timing",
            "Inferred release calendar hint from task text",
            (),
            ("inferred: task mentions launch, holiday, weekend, or release date timing",),
        )
    return None


def _task_signals(task: Mapping[str, Any]) -> _TaskSignals:
    deployment = production = migration_cutover = False
    freeze_blackout = calendar_hint = business_hours = change_gate = False
    evidence: list[str] = []
    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        matched = False
        if _DEPLOYMENT_RE.search(text):
            deployment = True
            matched = True
        if _PRODUCTION_RE.search(text):
            production = True
            matched = True
        if _MIGRATION_CUTOVER_RE.search(text):
            migration_cutover = True
            matched = True
        if _FREEZE_BLACKOUT_RE.search(text):
            freeze_blackout = True
            matched = True
        if _CALENDAR_RE.search(text):
            calendar_hint = True
            matched = True
        if _BUSINESS_HOURS_RE.search(text):
            business_hours = True
            matched = True
        if _CHANGE_GATE_RE.search(text):
            change_gate = True
            matched = True
        if matched:
            evidence.append(snippet)
    return _TaskSignals(
        deployment=deployment,
        production=production,
        migration_cutover=migration_cutover,
        freeze_blackout=freeze_blackout,
        calendar_hint=calendar_hint,
        business_hours=business_hours,
        change_gate=change_gate,
        evidence=tuple(_dedupe(evidence)),
    )


def _metadata_constraints(metadata: Any) -> tuple[_Constraint, ...]:
    if not isinstance(metadata, Mapping):
        return ()
    constraints: list[_Constraint] = []
    for constraint_type, keys in _METADATA_CONSTRAINT_KEYS:
        for key in keys:
            if key in metadata:
                constraints.extend(_constraints_from_value(metadata[key], f"metadata.{key}", constraint_type))
    return tuple(_dedupe_constraints(constraints))


def _constraints_from_value(
    value: Any,
    source: str,
    constraint_type: DeploymentFreezeConstraintType,
) -> list[_Constraint]:
    if isinstance(value, Mapping):
        if _looks_like_constraint(value):
            return [_constraint_from_mapping(value, source, constraint_type)]
        constraints: list[_Constraint] = []
        for key in sorted(value, key=lambda item: str(item)):
            child_source = f"{source}.{key}"
            child_type = _constraint_type_from_text(str(key), constraint_type)
            constraints.extend(_constraints_from_value(value[key], child_source, child_type))
        return constraints
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        constraints = []
        for index, item in enumerate(items):
            constraints.extend(_constraints_from_value(item, f"{source}[{index}]", constraint_type))
        return constraints
    text = _optional_text(value)
    if not text:
        return []
    return [
        _Constraint(
            _constraint_type_from_text(text, constraint_type),
            text,
            (),
            (_evidence_snippet(source, text),),
        )
    ]


def _constraint_from_mapping(
    value: Mapping[str, Any],
    source: str,
    fallback_type: DeploymentFreezeConstraintType,
) -> _Constraint:
    name = _first_text(value, "name", "title", "label", "window", "constraint", "date", "summary")
    start = _first_text(value, "start", "starts_at", "start_date", "from")
    end = _first_text(value, "end", "ends_at", "end_date", "until", "to")
    environment = _first_text(value, "environment", "env")
    parts = [name]
    if start and end:
        parts.append(f"{start} to {end}")
    elif start:
        parts.append(start)
    if environment:
        parts.append(environment)
    label = " - ".join(part for part in parts if part) or _text(value)
    approvals = tuple(
        _strings(
            value.get("required_approvals")
            or value.get("approvals")
            or value.get("approvers")
            or value.get("approval")
        )
    )
    evidence_text = label or _text(value)
    constraint_type = _constraint_type_from_text(evidence_text, fallback_type)
    return _Constraint(
        constraint_type,
        evidence_text,
        approvals,
        (_evidence_snippet(source, evidence_text),),
    )


def _constraint_type_from_text(
    text: str,
    fallback: DeploymentFreezeConstraintType,
) -> DeploymentFreezeConstraintType:
    if _FREEZE_BLACKOUT_RE.search(text):
        return "blackout" if re.search(r"\bblackout\b", text, re.I) else "freeze_window"
    if _CHANGE_GATE_RE.search(text):
        return "change_management"
    if _BUSINESS_HOURS_RE.search(text):
        return "business_hours"
    if _CALENDAR_RE.search(text):
        return "calendar_hint"
    return fallback


def _looks_like_constraint(value: Mapping[str, Any]) -> bool:
    return any(
        key in value
        for key in (
            "name",
            "title",
            "label",
            "window",
            "constraint",
            "date",
            "start",
            "start_date",
            "end",
            "end_date",
            "required_approvals",
            "approvals",
        )
    )


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, dict[str, Any], list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, {}, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return (
            _optional_text(source.id),
            dict(source.metadata),
            [task.model_dump(mode="python") for task in source.tasks],
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            metadata = payload.get("metadata")
            return (
                _optional_text(payload.get("id")),
                dict(metadata) if isinstance(metadata, Mapping) else {},
                _task_payloads(payload.get("tasks")),
            )
        return None, {}, [dict(source)]
    if _looks_like_task(source):
        return None, {}, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        metadata = payload.get("metadata")
        return (
            _optional_text(payload.get("id")),
            dict(metadata) if isinstance(metadata, Mapping) else {},
            _task_payloads(payload.get("tasks")),
        )

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, {}, []

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
    return None, {}, tasks


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
    for field_name in ("files_or_modules", "files", "acceptance_criteria", "tags", "labels", "notes"):
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
    return any(
        pattern.search(text)
        for pattern in (
            _DEPLOYMENT_RE,
            _PRODUCTION_RE,
            _MIGRATION_CUTOVER_RE,
            _FREEZE_BLACKOUT_RE,
            _CALENDAR_RE,
            _BUSINESS_HOURS_RE,
            _CHANGE_GATE_RE,
        )
    )


def _first_text(value: Mapping[str, Any], *keys: str) -> str | None:
    for key in keys:
        if text := _optional_text(value.get(key)):
            return text
    return None


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


def _dedupe_constraints(values: Iterable[_Constraint]) -> list[_Constraint]:
    deduped: list[_Constraint] = []
    seen: set[tuple[DeploymentFreezeConstraintType, str]] = set()
    for value in values:
        key = (value.constraint_type, value.label)
        if not value.label or key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "DeploymentFreezeConflictSeverity",
    "DeploymentFreezeConstraintType",
    "PlanDeploymentFreezeConflictRecord",
    "PlanDeploymentFreezeConflictReport",
    "analyze_plan_deployment_freeze_conflicts",
    "build_plan_deployment_freeze_conflict_report",
    "plan_deployment_freeze_conflict_report_to_dict",
    "plan_deployment_freeze_conflict_report_to_markdown",
    "summarize_plan_deployment_freeze_conflicts",
]
