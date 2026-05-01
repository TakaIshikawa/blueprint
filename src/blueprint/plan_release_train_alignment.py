"""Analyze release train alignment for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


ReleaseTrainSeverity = Literal["info", "warning", "error"]
_T = TypeVar("_T")

_HIGH_RISK_VALUES = {"blocker", "critical", "high"}
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrations|migrate|ddl|alembic|liquibase|flyway|"
    r"backfill|data migration|database migration|db migration)\b",
    re.IGNORECASE,
)
_RELEASE_MILESTONE_RE = re.compile(
    r"\b(?:release|train|window|rollout|launch|deploy|production|prod|canary|wave)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class PlanReleaseTrainFinding:
    """One release-train alignment concern."""

    code: str
    severity: ReleaseTrainSeverity
    reason: str
    suggested_remediation: str
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    train_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "code": self.code,
            "severity": self.severity,
            "reason": self.reason,
            "suggested_remediation": self.suggested_remediation,
            "task_ids": list(self.task_ids),
            "train_ids": list(self.train_ids),
        }


@dataclass(frozen=True, slots=True)
class ReleaseTrainGroup:
    """Tasks assigned to one release train or release window."""

    train_id: str
    label: str
    order: int
    source: Literal["metadata", "milestone", "unassigned"]
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    milestones: tuple[str, ...] = field(default_factory=tuple)
    validation_commands: tuple[str, ...] = field(default_factory=tuple)
    risk_notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "train_id": self.train_id,
            "label": self.label,
            "order": self.order,
            "source": self.source,
            "task_ids": list(self.task_ids),
            "milestones": list(self.milestones),
            "validation_commands": list(self.validation_commands),
            "risk_notes": list(self.risk_notes),
        }


@dataclass(frozen=True, slots=True)
class PlanReleaseTrainAlignment:
    """Release-train grouping and findings for an execution plan."""

    plan_id: str | None = None
    trains: tuple[ReleaseTrainGroup, ...] = field(default_factory=tuple)
    findings: tuple[PlanReleaseTrainFinding, ...] = field(default_factory=tuple)
    task_train_map: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "trains": [train.to_dict() for train in self.trains],
            "findings": [finding.to_dict() for finding in self.findings],
            "task_train_map": dict(self.task_train_map),
        }

    def to_markdown(self) -> str:
        """Render release-train alignment as deterministic Markdown."""
        title = "# Plan Release Train Alignment"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.trains:
            lines.extend(["", "No release trains were derived.", "", "## Issues", "", "No issues found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Release Trains",
                "",
                "| Train | Source | Tasks | Milestones | Validation | Risk Notes |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for train in self.trains:
            lines.append(
                "| "
                f"{_markdown_cell(train.label)} | "
                f"{train.source} | "
                f"{_markdown_cell(', '.join(train.task_ids) or 'none')} | "
                f"{_markdown_cell(', '.join(train.milestones) or 'none')} | "
                f"{_markdown_cell('; '.join(train.validation_commands) or 'none')} | "
                f"{_markdown_cell('; '.join(train.risk_notes) or 'none')} |"
            )

        lines.extend(["", "## Issues", ""])
        if not self.findings:
            lines.append("No issues found.")
        else:
            for finding in self.findings:
                tasks = ", ".join(finding.task_ids) or "plan"
                trains = f" ({', '.join(finding.train_ids)})" if finding.train_ids else ""
                lines.append(
                    f"- **{finding.severity}** `{finding.code}`: {finding.reason} "
                    f"Tasks: {tasks}{trains}."
                )
        return "\n".join(lines)


def build_plan_release_train_alignment(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanReleaseTrainAlignment:
    """Group execution-plan tasks by release train and report sequencing risks."""
    plan_id, milestones, tasks = _source_payload(source)
    records = _task_records(tasks)
    milestone_lookup = _milestone_lookup(milestones)
    train_order = _train_order_from_milestones(milestones)

    assignments = tuple(
        _assignment_for_record(record, milestone_lookup, train_order) for record in records
    )
    for assignment in assignments:
        train_order.setdefault(assignment.train_id, len(train_order))

    train_by_id: dict[str, list[_TaskAssignment]] = {}
    for assignment in assignments:
        train_by_id.setdefault(assignment.train_id, []).append(assignment)

    trains = tuple(
        _release_train_group(train_id, grouped, train_order[train_id])
        for train_id, grouped in sorted(train_by_id.items(), key=lambda item: train_order[item[0]])
    )
    order_lookup = {train.train_id: train.order for train in trains}
    findings = tuple(
        [
            *_missing_window_findings(assignments),
            *_conflicting_assignment_findings(assignments),
            *_dependency_findings(assignments, order_lookup),
            *_late_risk_findings(assignments, order_lookup),
        ]
    )

    return PlanReleaseTrainAlignment(
        plan_id=plan_id,
        trains=trains,
        findings=findings,
        task_train_map={assignment.task_id: assignment.train_id for assignment in assignments},
    )


def derive_plan_release_train_alignment(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanReleaseTrainAlignment:
    """Compatibility alias for building release-train alignment."""
    return build_plan_release_train_alignment(source)


def plan_release_train_alignment_to_dict(
    alignment: PlanReleaseTrainAlignment,
) -> dict[str, Any]:
    """Serialize release-train alignment to a plain dictionary."""
    return alignment.to_dict()


plan_release_train_alignment_to_dict.__test__ = False


def plan_release_train_alignment_to_markdown(alignment: PlanReleaseTrainAlignment) -> str:
    """Render release-train alignment as Markdown."""
    return alignment.to_markdown()


plan_release_train_alignment_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str
    title: str
    milestone: str | None
    depends_on: tuple[str, ...]
    risk_level: str
    validation_commands: tuple[str, ...]
    context: str


@dataclass(frozen=True, slots=True)
class _TaskAssignment:
    record: _TaskRecord
    train_id: str
    label: str
    source: Literal["metadata", "milestone", "unassigned"]
    explicit_release_train: str | None
    explicit_release_window: str | None
    milestone_train: str | None

    @property
    def task_id(self) -> str:
        return self.record.task_id

    @property
    def has_explicit_release_window(self) -> bool:
        return self.explicit_release_window is not None

    @property
    def is_high_risk_or_migration(self) -> bool:
        return self.record.risk_level in _HIGH_RISK_VALUES or _is_migration_context(
            self.record.context
        )


def _release_train_group(
    train_id: str,
    assignments: list[_TaskAssignment],
    order: int,
) -> ReleaseTrainGroup:
    return ReleaseTrainGroup(
        train_id=train_id,
        label=assignments[0].label,
        order=order,
        source=_group_source(assignments),
        task_ids=tuple(assignment.task_id for assignment in assignments),
        milestones=tuple(_dedupe(assignment.record.milestone for assignment in assignments)),
        validation_commands=tuple(
            _dedupe(command for assignment in assignments for command in assignment.record.validation_commands)
        ),
        risk_notes=tuple(_dedupe(_risk_note(assignment) for assignment in assignments)),
    )


def _assignment_for_record(
    record: _TaskRecord,
    milestone_lookup: Mapping[str, dict[str, Any]],
    train_order: dict[str, int],
) -> _TaskAssignment:
    explicit_train = _release_value(record.task, "release_train")
    explicit_window = _release_value(record.task, "release_window")
    milestone_train = _milestone_train(record.milestone, milestone_lookup)
    label = explicit_train or explicit_window or milestone_train or "Unassigned"
    source: Literal["metadata", "milestone", "unassigned"] = "unassigned"
    if explicit_train or explicit_window:
        source = "metadata"
    elif milestone_train:
        source = "milestone"

    return _TaskAssignment(
        record=record,
        train_id=_train_id(label),
        label=label,
        source=source,
        explicit_release_train=explicit_train,
        explicit_release_window=explicit_window,
        milestone_train=milestone_train,
    )


def _missing_window_findings(
    assignments: tuple[_TaskAssignment, ...],
) -> list[PlanReleaseTrainFinding]:
    findings: list[PlanReleaseTrainFinding] = []
    for assignment in assignments:
        if assignment.source == "unassigned":
            findings.append(
                PlanReleaseTrainFinding(
                    code="missing_release_window",
                    severity="warning",
                    reason=f"Task {assignment.task_id} is not assigned to a release train or window.",
                    suggested_remediation=(
                        "Add release_train or release_window metadata, or assign the task "
                        "to a release milestone."
                    ),
                    task_ids=(assignment.task_id,),
                    train_ids=(assignment.train_id,),
                )
            )
        if assignment.is_high_risk_or_migration and not assignment.has_explicit_release_window:
            findings.append(
                PlanReleaseTrainFinding(
                    code="explicit_release_window_required",
                    severity="warning",
                    reason=(
                        f"Task {assignment.task_id} is high-risk or migration-related "
                        "without explicit release_window metadata."
                    ),
                    suggested_remediation=(
                        "Set an explicit release_window for review, migration rehearsal, "
                        "and rollback planning."
                    ),
                    task_ids=(assignment.task_id,),
                    train_ids=(assignment.train_id,),
                )
            )
    return findings


def _conflicting_assignment_findings(
    assignments: tuple[_TaskAssignment, ...],
) -> list[PlanReleaseTrainFinding]:
    findings: list[PlanReleaseTrainFinding] = []
    for assignment in assignments:
        values = [
            value
            for value in (
                assignment.explicit_release_train,
                assignment.explicit_release_window,
                assignment.milestone_train,
            )
            if value
        ]
        train_ids = tuple(_dedupe(_train_id(value) for value in values))
        if len(train_ids) < 2:
            continue
        findings.append(
            PlanReleaseTrainFinding(
                code="conflicting_release_assignment",
                severity="error",
                reason=(
                    f"Task {assignment.task_id} has conflicting release assignments: "
                    + ", ".join(values)
                    + "."
                ),
                suggested_remediation=(
                    "Keep release_train, release_window, and milestone release metadata aligned."
                ),
                task_ids=(assignment.task_id,),
                train_ids=train_ids,
            )
        )
    return findings


def _dependency_findings(
    assignments: tuple[_TaskAssignment, ...],
    order_lookup: Mapping[str, int],
) -> list[PlanReleaseTrainFinding]:
    by_task_id = {assignment.task_id: assignment for assignment in assignments}
    findings: list[PlanReleaseTrainFinding] = []
    for assignment in assignments:
        dependent_order = order_lookup.get(assignment.train_id, 0)
        for dependency_id in assignment.record.depends_on:
            dependency = by_task_id.get(dependency_id)
            if dependency is None:
                continue
            dependency_order = order_lookup.get(dependency.train_id, 0)
            if dependency_order <= dependent_order:
                continue
            findings.append(
                PlanReleaseTrainFinding(
                    code="dependency_release_order_violation",
                    severity="error",
                    reason=(
                        f"Task {assignment.task_id} is scheduled in {assignment.label} "
                        f"before prerequisite {dependency_id} in {dependency.label}."
                    ),
                    suggested_remediation=(
                        "Move the prerequisite to the same or an earlier release train, "
                        "or move the dependent task later."
                    ),
                    task_ids=(assignment.task_id, dependency_id),
                    train_ids=(assignment.train_id, dependency.train_id),
                )
            )
    return findings


def _late_risk_findings(
    assignments: tuple[_TaskAssignment, ...],
    order_lookup: Mapping[str, int],
) -> list[PlanReleaseTrainFinding]:
    release_orders = {
        assignment.train_id: order_lookup.get(assignment.train_id, 0)
        for assignment in assignments
        if assignment.source != "unassigned"
    }
    if len(release_orders) < 2:
        return []
    last_order = max(release_orders.values())
    findings: list[PlanReleaseTrainFinding] = []
    for assignment in assignments:
        if not assignment.is_high_risk_or_migration:
            continue
        if order_lookup.get(assignment.train_id, 0) != last_order:
            continue
        findings.append(
            PlanReleaseTrainFinding(
                code="high_risk_task_scheduled_late",
                severity="warning",
                reason=(
                    f"Task {assignment.task_id} is high-risk or migration-related "
                    f"and is scheduled in the final release train {assignment.label}."
                ),
                suggested_remediation=(
                    "Schedule risky migration or rollout work in an earlier train, "
                    "or add a rehearsal task before the final train."
                ),
                task_ids=(assignment.task_id,),
                train_ids=(assignment.train_id,),
            )
        )
    return findings


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> tuple[str | None, list[dict[str, Any]], list[dict[str, Any]]]:
    if isinstance(source, ExecutionPlan):
        return (
            _optional_text(source.id),
            [dict(milestone) for milestone in source.milestones],
            [task.model_dump(mode="python") for task in source.tasks],
        )
    if isinstance(source, Mapping):
        if "tasks" in source:
            plan = _plan_payload(source)
            return (
                _optional_text(plan.get("id")),
                _milestone_payloads(plan.get("milestones")),
                _task_payloads(plan.get("tasks")),
            )
        return None, [], [dict(source)]
    if hasattr(source, "tasks"):
        plan = _object_payload(source)
        return (
            _optional_text(plan.get("id")),
            _milestone_payloads(plan.get("milestones")),
            _task_payloads(plan.get("tasks")),
        )

    tasks: list[dict[str, Any]] = []
    for item in source:
        if task := _task_payload(item):
            tasks.append(task)
    return None, [], tasks


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
        context = _task_context(task)
        records.append(
            _TaskRecord(
                task=task,
                task_id=task_id,
                title=_optional_text(task.get("title")) or task_id,
                milestone=_optional_text(task.get("milestone")),
                depends_on=tuple(_dedupe(_strings(task.get("depends_on") or task.get("dependencies")))),
                risk_level=(
                    _optional_text(task.get("risk_level"))
                    or _optional_text(task.get("risk"))
                    or _optional_text(_metadata_value(task.get("metadata"), "risk_level"))
                    or _optional_text(_metadata_value(task.get("metadata"), "risk"))
                    or "unspecified"
                ).lower(),
                validation_commands=tuple(_task_validation_commands(task)),
                context=context,
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


def _milestone_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    milestones: list[dict[str, Any]] = []
    for index, item in enumerate(items, start=1):
        if isinstance(item, Mapping):
            milestones.append(dict(item))
        elif text := _optional_text(item):
            milestones.append({"name": text, "order": index})
    return milestones


def _object_payload(value: Any) -> dict[str, Any]:
    fields = (
        "id",
        "milestones",
        "tasks",
        "title",
        "description",
        "milestone",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "acceptance_criteria",
        "risk_level",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "metadata",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _milestone_lookup(milestones: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    lookup: dict[str, dict[str, Any]] = {}
    for index, milestone in enumerate(milestones, start=1):
        name = _milestone_name(milestone, index)
        if name:
            lookup[name] = milestone
    return lookup


def _train_order_from_milestones(milestones: list[dict[str, Any]]) -> dict[str, int]:
    order: dict[str, int] = {}
    for index, milestone in enumerate(milestones):
        name = _milestone_name(milestone, index + 1)
        label = _release_value(milestone, "release_train") or _release_value(
            milestone, "release_window"
        )
        if not label and name and _RELEASE_MILESTONE_RE.search(name):
            label = name
        if label:
            order.setdefault(_train_id(label), len(order))
    return order


def _milestone_train(
    milestone_name: str | None,
    milestone_lookup: Mapping[str, dict[str, Any]],
) -> str | None:
    if not milestone_name:
        return None
    milestone = milestone_lookup.get(milestone_name, {})
    return (
        _release_value(milestone, "release_train")
        or _release_value(milestone, "release_window")
        or (milestone_name if _RELEASE_MILESTONE_RE.search(milestone_name) else None)
    )


def _release_value(item: Mapping[str, Any], key: str) -> str | None:
    candidates = [
        key,
        key.replace("_", "-"),
        "".join([key.split("_")[0], key.split("_")[1].title()]),
    ]
    metadata = item.get("metadata")
    for candidate in candidates:
        if text := _optional_text(item.get(candidate)):
            return text
        if text := _optional_text(_metadata_value(metadata, candidate)):
            return text
    return None


def _group_source(assignments: list[_TaskAssignment]) -> Literal["metadata", "milestone", "unassigned"]:
    sources = _dedupe(assignment.source for assignment in assignments)
    if len(sources) == 1:
        return sources[0]
    if "metadata" in sources:
        return "metadata"
    if "milestone" in sources:
        return "milestone"
    return "unassigned"


def _risk_note(assignment: _TaskAssignment) -> str | None:
    notes: list[str] = []
    if assignment.record.risk_level in _HIGH_RISK_VALUES:
        notes.append(f"{assignment.task_id}: {assignment.record.risk_level} risk")
    if _is_migration_context(assignment.record.context):
        notes.append(f"{assignment.task_id}: migration-related")
    return "; ".join(notes) if notes else None


def _task_context(task: Mapping[str, Any]) -> str:
    values: list[str] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "risk_level",
        "test_command",
        "suggested_test_command",
        "validation_command",
    ):
        if text := _optional_text(task.get(field_name)):
            values.append(text)
    values.extend(_strings(task.get("files_or_modules") or task.get("files")))
    values.extend(_strings(task.get("acceptance_criteria")))
    values.extend(_metadata_texts(task.get("metadata")))
    return " ".join(values)


def _task_validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        if text := _optional_text(task.get(key)):
            commands.append(text)
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
            value = metadata.get(key)
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


def _metadata_texts(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        texts: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            texts.extend(_metadata_texts(value[key]))
        return texts
    return _strings(value)


def _is_migration_context(context: str) -> bool:
    return _MIGRATION_RE.search(context) is not None


def _train_id(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
    return slug or "unassigned"


def _milestone_name(milestone: Mapping[str, Any], index: int) -> str | None:
    return (
        _optional_text(milestone.get("name"))
        or _optional_text(milestone.get("title"))
        or _optional_text(milestone.get("id"))
        or f"Milestone {index}"
    )


def _metadata_value(metadata: Any, key: str) -> Any:
    if not isinstance(metadata, Mapping):
        return None
    return metadata.get(key)


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


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "PlanReleaseTrainAlignment",
    "PlanReleaseTrainFinding",
    "ReleaseTrainGroup",
    "ReleaseTrainSeverity",
    "build_plan_release_train_alignment",
    "derive_plan_release_train_alignment",
    "plan_release_train_alignment_to_dict",
    "plan_release_train_alignment_to_markdown",
]
