"""Build dependency handoff SLA matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


DependencySlaSeverity = Literal["info", "warning", "error"]
CoordinationRisk = Literal["low", "medium", "high"]
SlaSource = Literal["metadata", "inferred", "missing"]
_T = TypeVar("_T")

_HIGH_RISK_VALUES = {"blocker", "critical", "high"}
_WEAK_SLA_VALUES = {"", "asap", "soon", "tbd", "todo", "unknown", "later", "best effort"}
_COMPLEXITY_HOURS = {
    "trivial": 4,
    "low": 8,
    "small": 8,
    "medium": 24,
    "moderate": 24,
    "high": 48,
    "large": 48,
    "complex": 72,
}


@dataclass(frozen=True, slots=True)
class DependencySlaFinding:
    """One dependency handoff concern."""

    code: str
    severity: DependencySlaSeverity
    reason: str
    suggested_remediation: str
    edge_ids: tuple[str, ...] = field(default_factory=tuple)
    task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "code": self.code,
            "severity": self.severity,
            "reason": self.reason,
            "suggested_remediation": self.suggested_remediation,
            "edge_ids": list(self.edge_ids),
            "task_ids": list(self.task_ids),
        }


@dataclass(frozen=True, slots=True)
class DependencySlaEdge:
    """One dependency edge with handoff expectations."""

    edge_id: str
    prerequisite_task_id: str
    dependent_task_id: str
    prerequisite_title: str | None
    dependent_title: str
    prerequisite_owner_type: str
    dependent_owner_type: str
    prerequisite_engine: str
    dependent_engine: str
    prerequisite_milestone: str | None
    dependent_milestone: str | None
    coordination_risk: CoordinationRisk
    risk_reasons: tuple[str, ...] = field(default_factory=tuple)
    expected_handoff_sla: str | None = None
    sla_source: SlaSource = "missing"
    due_date: str | None = None
    blocked_until: str | None = None
    validation_gates: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "edge_id": self.edge_id,
            "prerequisite_task_id": self.prerequisite_task_id,
            "dependent_task_id": self.dependent_task_id,
            "prerequisite_title": self.prerequisite_title,
            "dependent_title": self.dependent_title,
            "prerequisite_owner_type": self.prerequisite_owner_type,
            "dependent_owner_type": self.dependent_owner_type,
            "prerequisite_engine": self.prerequisite_engine,
            "dependent_engine": self.dependent_engine,
            "prerequisite_milestone": self.prerequisite_milestone,
            "dependent_milestone": self.dependent_milestone,
            "coordination_risk": self.coordination_risk,
            "risk_reasons": list(self.risk_reasons),
            "expected_handoff_sla": self.expected_handoff_sla,
            "sla_source": self.sla_source,
            "due_date": self.due_date,
            "blocked_until": self.blocked_until,
            "validation_gates": list(self.validation_gates),
        }


@dataclass(frozen=True, slots=True)
class PlanDependencySlaMatrix:
    """Dependency handoff SLA matrix and findings for an execution plan."""

    plan_id: str | None = None
    edges: tuple[DependencySlaEdge, ...] = field(default_factory=tuple)
    findings: tuple[DependencySlaFinding, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "edges": [edge.to_dict() for edge in self.edges],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_markdown(self) -> str:
        """Render the dependency SLA matrix as deterministic Markdown."""
        title = "# Plan Dependency SLA Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.edges:
            lines.extend(["", "No dependency edges were found.", "", "## Findings Summary", ""])
            lines.append("No findings.")
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Dependency Matrix",
                "",
                "| Edge | Prerequisite | Dependent | Boundary | Risk | SLA | Validation Gates |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for edge in self.edges:
            boundary = _boundary_label(edge)
            lines.append(
                "| "
                f"{_markdown_cell(edge.edge_id)} | "
                f"{_markdown_cell(edge.prerequisite_task_id)} | "
                f"{_markdown_cell(edge.dependent_task_id)} | "
                f"{_markdown_cell(boundary)} | "
                f"{edge.coordination_risk} | "
                f"{_markdown_cell(edge.expected_handoff_sla or 'missing')} | "
                f"{_markdown_cell('; '.join(edge.validation_gates) or 'none')} |"
            )

        lines.extend(["", "## Findings Summary", ""])
        if not self.findings:
            lines.append("No findings.")
        else:
            for finding in self.findings:
                edges = ", ".join(finding.edge_ids) or "plan"
                lines.append(
                    f"- **{finding.severity}** `{finding.code}`: {finding.reason} Edges: {edges}."
                )
        return "\n".join(lines)


def build_plan_dependency_sla_matrix(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanDependencySlaMatrix:
    """Build dependency handoff SLA expectations and findings for an execution plan."""
    plan_id, tasks = _source_payload(source)
    records = _task_records(tasks)
    by_task_id = {record.task_id: record for record in records}

    edges = tuple(
        _edge_for_dependency(record, dependency_id, by_task_id)
        for record in records
        for dependency_id in record.depends_on
    )
    findings = tuple(finding for edge in edges for finding in _findings_for_edge(edge))
    return PlanDependencySlaMatrix(plan_id=plan_id, edges=edges, findings=findings)


def derive_plan_dependency_sla_matrix(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanDependencySlaMatrix:
    """Compatibility alias for building a dependency SLA matrix."""
    return build_plan_dependency_sla_matrix(source)


def plan_dependency_sla_matrix_to_dict(matrix: PlanDependencySlaMatrix) -> dict[str, Any]:
    """Serialize a dependency SLA matrix to a plain dictionary."""
    return matrix.to_dict()


plan_dependency_sla_matrix_to_dict.__test__ = False


def plan_dependency_sla_matrix_to_markdown(matrix: PlanDependencySlaMatrix) -> str:
    """Render a dependency SLA matrix as Markdown."""
    return matrix.to_markdown()


plan_dependency_sla_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str
    title: str
    milestone: str | None
    owner_type: str
    suggested_engine: str
    depends_on: tuple[str, ...]
    estimated_complexity: str
    risk_level: str
    due_date: str | None
    blocked_until: str | None
    explicit_sla: str | None
    validation_gates: tuple[str, ...]


def _edge_for_dependency(
    dependent: _TaskRecord,
    dependency_id: str,
    by_task_id: Mapping[str, _TaskRecord],
) -> DependencySlaEdge:
    prerequisite = by_task_id.get(dependency_id)
    risk_reasons = _risk_reasons(prerequisite, dependent)
    coordination_risk = _coordination_risk(risk_reasons)
    expected_sla, sla_source = _expected_sla(prerequisite, dependent, risk_reasons)
    validation_gates = tuple(
        _dedupe(
            [
                *(prerequisite.validation_gates if prerequisite else ()),
                *dependent.validation_gates,
            ]
        )
    )
    return DependencySlaEdge(
        edge_id=f"{dependency_id}->{dependent.task_id}",
        prerequisite_task_id=dependency_id,
        dependent_task_id=dependent.task_id,
        prerequisite_title=prerequisite.title if prerequisite else None,
        dependent_title=dependent.title,
        prerequisite_owner_type=prerequisite.owner_type if prerequisite else "unknown",
        dependent_owner_type=dependent.owner_type,
        prerequisite_engine=prerequisite.suggested_engine if prerequisite else "unknown",
        dependent_engine=dependent.suggested_engine,
        prerequisite_milestone=prerequisite.milestone if prerequisite else None,
        dependent_milestone=dependent.milestone,
        coordination_risk=coordination_risk,
        risk_reasons=tuple(risk_reasons),
        expected_handoff_sla=expected_sla,
        sla_source=sla_source,
        due_date=dependent.due_date or (prerequisite.due_date if prerequisite else None),
        blocked_until=dependent.blocked_until
        or (prerequisite.blocked_until if prerequisite else None),
        validation_gates=validation_gates,
    )


def _findings_for_edge(edge: DependencySlaEdge) -> list[DependencySlaFinding]:
    findings: list[DependencySlaFinding] = []
    task_ids = (edge.prerequisite_task_id, edge.dependent_task_id)
    if edge.prerequisite_title is None:
        findings.append(
            DependencySlaFinding(
                code="unknown_dependency",
                severity="error",
                reason=(
                    f"Task {edge.dependent_task_id} depends on unknown task "
                    f"{edge.prerequisite_task_id}."
                ),
                suggested_remediation="Add the missing prerequisite task or remove the dependency edge.",
                edge_ids=(edge.edge_id,),
                task_ids=task_ids,
            )
        )
    if edge.sla_source == "missing":
        findings.append(
            DependencySlaFinding(
                code="missing_handoff_sla",
                severity="warning",
                reason=f"Dependency {edge.edge_id} has no explicit handoff SLA metadata.",
                suggested_remediation=(
                    "Add sla or handoff_sla metadata to the dependent task, including an owner "
                    "and expected response window."
                ),
                edge_ids=(edge.edge_id,),
                task_ids=task_ids,
            )
        )
    elif _is_weak_sla(edge.expected_handoff_sla):
        findings.append(
            DependencySlaFinding(
                code="weak_handoff_sla",
                severity="warning",
                reason=f"Dependency {edge.edge_id} uses weak SLA metadata: {edge.expected_handoff_sla}.",
                suggested_remediation=(
                    "Replace vague SLA text with a concrete duration, due_date, or blocked_until date."
                ),
                edge_ids=(edge.edge_id,),
                task_ids=task_ids,
            )
        )
    if (
        "cross-owner boundary" in edge.risk_reasons
        or "cross-engine boundary" in edge.risk_reasons
        or edge.prerequisite_owner_type != edge.dependent_owner_type
        or edge.prerequisite_engine != edge.dependent_engine
    ):
        findings.append(
            DependencySlaFinding(
                code="cross_boundary_dependency",
                severity="warning",
                reason=f"Dependency {edge.edge_id} crosses owner or engine boundaries.",
                suggested_remediation=(
                    "Name a handoff owner, expected acknowledgement time, and escalation path "
                    "for the boundary."
                ),
                edge_ids=(edge.edge_id,),
                task_ids=task_ids,
            )
        )
    if edge.coordination_risk == "high" and not edge.validation_gates:
        findings.append(
            DependencySlaFinding(
                code="high_risk_dependency_missing_validation_gate",
                severity="error",
                reason=f"High-risk dependency {edge.edge_id} has no validation gate.",
                suggested_remediation=(
                    "Add a test_command, validation_command, or validation_gates metadata before "
                    "the dependent task starts."
                ),
                edge_ids=(edge.edge_id,),
                task_ids=task_ids,
            )
        )
    return findings


def _risk_reasons(
    prerequisite: _TaskRecord | None,
    dependent: _TaskRecord,
) -> list[str]:
    reasons: list[str] = []
    if prerequisite is None:
        reasons.append("unknown prerequisite")
    elif prerequisite.owner_type != dependent.owner_type:
        reasons.append("cross-owner boundary")
    if prerequisite is None:
        pass
    elif prerequisite.suggested_engine != dependent.suggested_engine:
        reasons.append("cross-engine boundary")
    if prerequisite and prerequisite.milestone != dependent.milestone:
        reasons.append("cross-milestone handoff")
    if dependent.risk_level in _HIGH_RISK_VALUES or (
        prerequisite and prerequisite.risk_level in _HIGH_RISK_VALUES
    ):
        reasons.append("high task risk")
    if _complexity_hours(dependent.estimated_complexity) >= 48 or (
        prerequisite and _complexity_hours(prerequisite.estimated_complexity) >= 48
    ):
        reasons.append("high complexity")
    if dependent.blocked_until or (prerequisite and prerequisite.blocked_until):
        reasons.append("blocked schedule")
    return _dedupe(reasons)


def _coordination_risk(reasons: list[str]) -> CoordinationRisk:
    if any(
        reason in reasons
        for reason in (
            "unknown prerequisite",
            "cross-engine boundary",
            "high task risk",
            "high complexity",
        )
    ):
        return "high"
    if "cross-owner boundary" in reasons or "cross-milestone handoff" in reasons:
        return "medium"
    return "low"


def _expected_sla(
    prerequisite: _TaskRecord | None,
    dependent: _TaskRecord,
    risk_reasons: list[str],
) -> tuple[str | None, SlaSource]:
    explicit = dependent.explicit_sla or (prerequisite.explicit_sla if prerequisite else None)
    if explicit:
        return explicit, "metadata"
    due_date = dependent.due_date or (prerequisite.due_date if prerequisite else None)
    if due_date:
        return f"handoff due by {due_date}", "metadata"
    blocked_until = dependent.blocked_until or (
        prerequisite.blocked_until if prerequisite else None
    )
    if blocked_until:
        return f"ready no earlier than {blocked_until}", "metadata"
    if not risk_reasons:
        return "within 1 business day after prerequisite completion", "inferred"
    hours = max(
        8,
        _complexity_hours(dependent.estimated_complexity),
        _complexity_hours(prerequisite.estimated_complexity) if prerequisite else 24,
    )
    if "cross-engine boundary" in risk_reasons or "high task risk" in risk_reasons:
        hours = max(hours, 48)
    elif "cross-owner boundary" in risk_reasons or "cross-milestone handoff" in risk_reasons:
        hours = max(hours, 24)
    return f"within {hours} hours after prerequisite completion", "missing"


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return _optional_text(plan.get("id")), _task_payloads(plan.get("tasks"))
        return None, [dict(source)]
    if hasattr(source, "tasks"):
        plan = _object_payload(source)
        return _optional_text(plan.get("id")), _task_payloads(plan.get("tasks"))

    tasks: list[dict[str, Any]] = []
    for item in source:
        if task := _task_payload(item):
            tasks.append(task)
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


def _task_records(tasks: list[dict[str, Any]]) -> tuple[_TaskRecord, ...]:
    records: list[_TaskRecord] = []
    seen_ids: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        task_id = _optional_text(task.get("id")) or f"task-{index}"
        if task_id in seen_ids:
            continue
        seen_ids.add(task_id)
        metadata = task.get("metadata")
        records.append(
            _TaskRecord(
                task=task,
                task_id=task_id,
                title=_optional_text(task.get("title")) or task_id,
                milestone=_optional_text(task.get("milestone")),
                owner_type=(
                    _optional_text(task.get("owner_type"))
                    or _optional_text(_metadata_value(metadata, "owner_type"))
                    or "unassigned"
                ),
                suggested_engine=(
                    _optional_text(task.get("suggested_engine"))
                    or _optional_text(_metadata_value(metadata, "suggested_engine"))
                    or "unassigned"
                ),
                depends_on=tuple(
                    _dedupe(_strings(task.get("depends_on") or task.get("dependencies")))
                ),
                estimated_complexity=(
                    _optional_text(task.get("estimated_complexity"))
                    or _optional_text(task.get("complexity"))
                    or _optional_text(_metadata_value(metadata, "complexity"))
                    or "medium"
                ).casefold(),
                risk_level=(
                    _optional_text(task.get("risk_level"))
                    or _optional_text(task.get("risk"))
                    or _optional_text(_metadata_value(metadata, "risk_level"))
                    or _optional_text(_metadata_value(metadata, "risk"))
                    or "medium"
                ).casefold(),
                due_date=_metadata_or_field(task, "due_date"),
                blocked_until=_metadata_or_field(task, "blocked_until"),
                explicit_sla=(
                    _metadata_or_field(task, "handoff_sla")
                    or _metadata_or_field(task, "sla")
                    or _metadata_or_field(task, "service_level")
                ),
                validation_gates=tuple(_validation_gates(task)),
            )
        )
    return tuple(records)


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
    return _object_payload(value)


def _object_payload(value: Any) -> dict[str, Any]:
    fields = (
        "id",
        "tasks",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "dependencies",
        "acceptance_criteria",
        "estimated_complexity",
        "complexity",
        "risk_level",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "metadata",
        "blocked_reason",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _validation_gates(task: Mapping[str, Any]) -> list[str]:
    gates: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        if text := _optional_text(task.get(key)):
            gates.append(text)
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "validation_gates",
            "validation_gate",
            "validation_commands",
            "validation_command",
            "test_commands",
            "test_command",
        ):
            value = metadata.get(key)
            if isinstance(value, Mapping):
                gates.extend(flatten_validation_commands(value))
            else:
                gates.extend(_strings(value))
    return _dedupe(gates)


def _metadata_or_field(task: Mapping[str, Any], key: str) -> str | None:
    candidates = [
        key,
        key.replace("_", "-"),
        "".join(
            [part if index == 0 else part.title() for index, part in enumerate(key.split("_"))]
        ),
    ]
    metadata = task.get("metadata")
    for candidate in candidates:
        if text := _optional_text(task.get(candidate)):
            return text
        if text := _optional_text(_metadata_value(metadata, candidate)):
            return text
    return None


def _metadata_value(metadata: Any, key: str) -> Any:
    if not isinstance(metadata, Mapping):
        return None
    return metadata.get(key)


def _complexity_hours(value: str | None) -> int:
    if not value:
        return 24
    return _COMPLEXITY_HOURS.get(value.casefold(), 24)


def _is_weak_sla(value: str | None) -> bool:
    return (value or "").strip().casefold() in _WEAK_SLA_VALUES


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


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = " ".join(str(value).split())
    return text or None


def _dedupe(values: Iterable[_T | None]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


def _boundary_label(edge: DependencySlaEdge) -> str:
    labels: list[str] = []
    if edge.prerequisite_owner_type != edge.dependent_owner_type:
        labels.append(f"owner: {edge.prerequisite_owner_type}->{edge.dependent_owner_type}")
    if edge.prerequisite_engine != edge.dependent_engine:
        labels.append(f"engine: {edge.prerequisite_engine}->{edge.dependent_engine}")
    if edge.prerequisite_milestone != edge.dependent_milestone:
        labels.append(
            "milestone: "
            f"{edge.prerequisite_milestone or 'none'}->{edge.dependent_milestone or 'none'}"
        )
    return "; ".join(labels) or "none"


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "CoordinationRisk",
    "DependencySlaEdge",
    "DependencySlaFinding",
    "DependencySlaSeverity",
    "PlanDependencySlaMatrix",
    "SlaSource",
    "build_plan_dependency_sla_matrix",
    "derive_plan_dependency_sla_matrix",
    "plan_dependency_sla_matrix_to_dict",
    "plan_dependency_sla_matrix_to_markdown",
]
