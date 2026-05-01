"""Build launch-window timing risk maps for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


LaunchRiskLevel = Literal["low", "medium", "high"]
LaunchPhaseSource = Literal["metadata", "milestone", "dependency", "unassigned"]
LaunchBlockerSeverity = Literal["warning", "error"]
_T = TypeVar("_T")

_HIGH_RISK_VALUES = {"blocker", "critical", "high"}
_WEAK_VALIDATION_VALUES = {"", "none", "n/a", "na", "manual", "todo", "tbd", "unknown", "later"}
_RELEASE_KEYS = (
    "release_window",
    "launch_window",
    "release_train",
    "release_phase",
    "launch_phase",
)
_RELEASE_MILESTONE_RE = re.compile(
    r"\b(?:release|train|window|launch|rollout|deploy|deployment|production|prod|canary|wave)\b",
    re.IGNORECASE,
)
_PRODUCTION_RE = re.compile(
    r"\b(?:production|prod|go-live|go live|release|deploy|deployment|rollout|launch|"
    r"live traffic|customer traffic)\b",
    re.IGNORECASE,
)
_MIGRATION_RE = re.compile(
    r"\b(?:migration|migrations|migrate|schema|ddl|alembic|liquibase|flyway|"
    r"backfill|data migration|database migration|db migration|sql)\b",
    re.IGNORECASE,
)
_EXTERNAL_RE = re.compile(
    r"\b(?:external|third[- ]party|vendor|partner|integration|webhook|oauth|"
    r"api gateway|stripe|slack|pagerduty|opsgenie|salesforce)\b",
    re.IGNORECASE,
)
_ROLLBACK_RE = re.compile(
    r"\b(?:rollback|roll back|revert|restore|recovery|down migration)\b", re.IGNORECASE
)
_VALIDATION_RE = re.compile(
    r"\b(?:test|pytest|smoke|validate|validation|check|verify|ci|lint)\b", re.IGNORECASE
)
_COMPLEXITY_HOURS = {
    "trivial": 2,
    "low": 4,
    "small": 4,
    "medium": 8,
    "moderate": 8,
    "high": 16,
    "large": 16,
    "complex": 24,
}


@dataclass(frozen=True, slots=True)
class LaunchWindowTaskRisk:
    """Timing risk summary for one launch-window task."""

    task_id: str
    title: str
    risk_level: LaunchRiskLevel
    signals: tuple[str, ...] = field(default_factory=tuple)
    validation_gates: tuple[str, ...] = field(default_factory=tuple)
    estimated_hours: float | None = None
    blockers: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "risk_level": self.risk_level,
            "signals": list(self.signals),
            "validation_gates": list(self.validation_gates),
            "estimated_hours": self.estimated_hours,
            "blockers": list(self.blockers),
        }


@dataclass(frozen=True, slots=True)
class LaunchWindowBlocker:
    """A condition that should block or delay launch-window start."""

    code: str
    severity: LaunchBlockerSeverity
    reason: str
    suggested_action: str
    task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "code": self.code,
            "severity": self.severity,
            "reason": self.reason,
            "suggested_action": self.suggested_action,
            "task_ids": list(self.task_ids),
        }


@dataclass(frozen=True, slots=True)
class LaunchWindowPhase:
    """One ordered group of tasks in the launch window."""

    phase_id: str
    label: str
    order: int
    source: LaunchPhaseSource
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    milestones: tuple[str, ...] = field(default_factory=tuple)
    release_windows: tuple[str, ...] = field(default_factory=tuple)
    estimated_hours: float = 0.0
    high_risk_task_ids: tuple[str, ...] = field(default_factory=tuple)
    blockers: tuple[str, ...] = field(default_factory=tuple)
    coordination_notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "phase_id": self.phase_id,
            "label": self.label,
            "order": self.order,
            "source": self.source,
            "task_ids": list(self.task_ids),
            "milestones": list(self.milestones),
            "release_windows": list(self.release_windows),
            "estimated_hours": self.estimated_hours,
            "high_risk_task_ids": list(self.high_risk_task_ids),
            "blockers": list(self.blockers),
            "coordination_notes": list(self.coordination_notes),
        }


@dataclass(frozen=True, slots=True)
class PlanLaunchWindowRiskMap:
    """Launch-window phase map, task risks, and suggested blockers."""

    plan_id: str | None = None
    phases: tuple[LaunchWindowPhase, ...] = field(default_factory=tuple)
    task_risks: tuple[LaunchWindowTaskRisk, ...] = field(default_factory=tuple)
    high_risk_task_ids: tuple[str, ...] = field(default_factory=tuple)
    suggested_launch_blockers: tuple[LaunchWindowBlocker, ...] = field(default_factory=tuple)
    coordination_notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "phases": [phase.to_dict() for phase in self.phases],
            "task_risks": [risk.to_dict() for risk in self.task_risks],
            "high_risk_task_ids": list(self.high_risk_task_ids),
            "suggested_launch_blockers": [
                blocker.to_dict() for blocker in self.suggested_launch_blockers
            ],
            "coordination_notes": list(self.coordination_notes),
        }

    def to_markdown(self) -> str:
        """Render the launch-window risk map as deterministic Markdown."""
        title = "# Plan Launch Window Risk Map"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.phases:
            lines.extend(
                [
                    "",
                    "No launch phases were derived.",
                    "",
                    "## Suggested Launch Blockers",
                    "",
                    "No launch blockers suggested.",
                    "",
                    "## Coordination Notes",
                    "",
                    "No coordination notes.",
                ]
            )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Phase Summary",
                "",
                "| Phase | Source | Tasks | High Risk | Estimated Hours | Blockers | Coordination Notes |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for phase in self.phases:
            lines.append(
                "| "
                f"{_markdown_cell(phase.label)} | "
                f"{phase.source} | "
                f"{_markdown_cell(', '.join(phase.task_ids) or 'none')} | "
                f"{_markdown_cell(', '.join(phase.high_risk_task_ids) or 'none')} | "
                f"{_format_hours(phase.estimated_hours)} | "
                f"{_markdown_cell('; '.join(phase.blockers) or 'none')} | "
                f"{_markdown_cell('; '.join(phase.coordination_notes) or 'none')} |"
            )

        lines.extend(["", "## High-Risk Tasks", ""])
        if not self.high_risk_task_ids:
            lines.append("No high-risk launch tasks detected.")
        else:
            risk_by_id = {risk.task_id: risk for risk in self.task_risks}
            for task_id in self.high_risk_task_ids:
                risk = risk_by_id[task_id]
                lines.append(
                    f"- `{task_id}` ({risk.risk_level}): "
                    f"{', '.join(risk.signals) or 'no signals'}."
                )

        lines.extend(["", "## Suggested Launch Blockers", ""])
        if not self.suggested_launch_blockers:
            lines.append("No launch blockers suggested.")
        else:
            for blocker in self.suggested_launch_blockers:
                tasks = ", ".join(blocker.task_ids) or "plan"
                lines.append(
                    f"- **{blocker.severity}** `{blocker.code}`: {blocker.reason} "
                    f"Tasks: {tasks}."
                )

        lines.extend(["", "## Coordination Notes", ""])
        if not self.coordination_notes:
            lines.append("No coordination notes.")
        else:
            lines.extend(f"- {note}" for note in self.coordination_notes)
        return "\n".join(lines)


def build_plan_launch_window_risk_map(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanLaunchWindowRiskMap:
    """Build launch-window phases and timing hazards for an execution plan."""
    plan_id, milestones, tasks = _source_payload(source)
    records = _task_records(tasks)
    by_task_id = {record.task_id: record for record in records}
    milestone_lookup = _milestone_lookup(milestones)
    milestone_order = _milestone_order(milestones)
    release_order = _release_order(milestones)
    depth_lookup = _dependency_depths(records, by_task_id)
    task_risks = tuple(_task_risk(record, by_task_id) for record in records)
    risk_by_id = {risk.task_id: risk for risk in task_risks}

    phase_assignments = tuple(
        _phase_assignment(record, milestone_lookup, milestone_order, release_order, depth_lookup)
        for record in records
    )
    phase_groups: dict[str, list[_PhaseAssignment]] = {}
    for assignment in phase_assignments:
        phase_groups.setdefault(assignment.phase_id, []).append(assignment)

    phases = tuple(
        _launch_phase(phase_id, assignments, risk_by_id, by_task_id)
        for phase_id, assignments in sorted(
            phase_groups.items(), key=lambda item: (item[1][0].order, item[1][0].first_task_index)
        )
    )
    blockers = tuple(_suggested_blockers(records, by_task_id, risk_by_id))
    coordination_notes = tuple(
        _coordination_notes(records, phase_assignments, risk_by_id, blockers)
    )

    return PlanLaunchWindowRiskMap(
        plan_id=plan_id,
        phases=phases,
        task_risks=task_risks,
        high_risk_task_ids=tuple(risk.task_id for risk in task_risks if risk.risk_level == "high"),
        suggested_launch_blockers=blockers,
        coordination_notes=coordination_notes,
    )


def derive_plan_launch_window_risk_map(
    source: Mapping[str, Any] | ExecutionPlan | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanLaunchWindowRiskMap:
    """Compatibility alias for building a launch-window risk map."""
    return build_plan_launch_window_risk_map(source)


def plan_launch_window_risk_map_to_dict(result: PlanLaunchWindowRiskMap) -> dict[str, Any]:
    """Serialize a launch-window risk map to a plain dictionary."""
    return result.to_dict()


plan_launch_window_risk_map_to_dict.__test__ = False


def plan_launch_window_risk_map_to_markdown(result: PlanLaunchWindowRiskMap) -> str:
    """Render a launch-window risk map as Markdown."""
    return result.to_markdown()


plan_launch_window_risk_map_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str
    title: str
    index: int
    milestone: str | None
    depends_on: tuple[str, ...]
    owner_type: str
    suggested_engine: str
    estimated_complexity: str
    estimated_hours: float | None
    risk_level: str
    status: str
    blocked_reason: str | None
    validation_gates: tuple[str, ...]
    release_window: str | None
    context: str


@dataclass(frozen=True, slots=True)
class _PhaseAssignment:
    record: _TaskRecord
    phase_id: str
    label: str
    order: int
    source: LaunchPhaseSource

    @property
    def first_task_index(self) -> int:
        return self.record.index


def _launch_phase(
    phase_id: str,
    assignments: list[_PhaseAssignment],
    risk_by_id: Mapping[str, LaunchWindowTaskRisk],
    by_task_id: Mapping[str, _TaskRecord],
) -> LaunchWindowPhase:
    records = [assignment.record for assignment in assignments]
    task_ids = tuple(record.task_id for record in records)
    high_risk_task_ids = tuple(
        task_id for task_id in task_ids if risk_by_id[task_id].risk_level == "high"
    )
    blockers = tuple(
        _dedupe(
            blocker
            for record in records
            for blocker in [
                *risk_by_id[record.task_id].blockers,
                *_dependency_blockers(record, by_task_id),
            ]
        )
    )
    notes = tuple(
        _dedupe(note for record in records for note in _task_coordination_notes(record, risk_by_id))
    )
    return LaunchWindowPhase(
        phase_id=phase_id,
        label=assignments[0].label,
        order=assignments[0].order,
        source=_phase_source(assignments),
        task_ids=task_ids,
        milestones=tuple(_dedupe(record.milestone for record in records)),
        release_windows=tuple(_dedupe(record.release_window for record in records)),
        estimated_hours=sum(_estimated_hours(record) for record in records),
        high_risk_task_ids=high_risk_task_ids,
        blockers=blockers,
        coordination_notes=notes,
    )


def _phase_assignment(
    record: _TaskRecord,
    milestone_lookup: Mapping[str, dict[str, Any]],
    milestone_order: Mapping[str, int],
    release_order: Mapping[str, int],
    depth_lookup: Mapping[str, int],
) -> _PhaseAssignment:
    release_window = record.release_window or _milestone_release_window(
        record.milestone, milestone_lookup
    )
    if release_window:
        label = release_window
        source: LaunchPhaseSource = "metadata" if record.release_window else "milestone"
        order = release_order.get(
            _slug(label), len(release_order) + depth_lookup.get(record.task_id, 0)
        )
        return _PhaseAssignment(
            record=record,
            phase_id=f"{order:03d}-{_slug(label)}",
            label=label,
            order=order,
            source=source,
        )
    if record.milestone:
        order = milestone_order.get(record.milestone, len(milestone_order)) + depth_lookup.get(
            record.task_id, 0
        )
        return _PhaseAssignment(
            record=record,
            phase_id=f"{order:03d}-{_slug(record.milestone)}",
            label=record.milestone,
            order=order,
            source="milestone",
        )

    depth = depth_lookup.get(record.task_id, 0)
    return _PhaseAssignment(
        record=record,
        phase_id=f"{depth:03d}-dependency-phase-{depth + 1}",
        label=f"Dependency Phase {depth + 1}",
        order=depth,
        source="dependency",
    )


def _task_risk(
    record: _TaskRecord,
    by_task_id: Mapping[str, _TaskRecord],
) -> LaunchWindowTaskRisk:
    signals = tuple(_risk_signals(record, by_task_id))
    risk_level = _risk_level(record, signals)
    blockers: list[str] = []
    if record.status == "blocked":
        blockers.append(f"{record.task_id}: task status is blocked")
    if record.blocked_reason:
        blockers.append(f"{record.task_id}: {record.blocked_reason}")
    if risk_level == "high" and "weak-validation" in signals:
        blockers.append(f"{record.task_id}: high-risk launch task lacks strong validation")
    if (
        "migration" in signals
        and "production" in signals
        and not _ROLLBACK_RE.search(record.context)
    ):
        blockers.append(f"{record.task_id}: production migration lacks rollback signal")
    return LaunchWindowTaskRisk(
        task_id=record.task_id,
        title=record.title,
        risk_level=risk_level,
        signals=signals,
        validation_gates=record.validation_gates,
        estimated_hours=_estimated_hours(record),
        blockers=tuple(_dedupe(blockers)),
    )


def _risk_signals(
    record: _TaskRecord,
    by_task_id: Mapping[str, _TaskRecord],
) -> list[str]:
    signals: list[str] = []
    if record.risk_level in _HIGH_RISK_VALUES:
        signals.append("explicit-high-risk")
    if _PRODUCTION_RE.search(record.context):
        signals.append("production")
    if _MIGRATION_RE.search(record.context):
        signals.append("migration")
    if _EXTERNAL_RE.search(record.context):
        signals.append("external-dependency")
    if not record.validation_gates or _has_weak_validation(record.validation_gates):
        signals.append("weak-validation")
    if any(dependency_id not in by_task_id for dependency_id in record.depends_on):
        signals.append("unknown-dependency")
    if record.blocked_reason or record.status == "blocked":
        signals.append("blocked")
    return _dedupe(signals)


def _risk_level(record: _TaskRecord, signals: tuple[str, ...]) -> LaunchRiskLevel:
    timing_signals = {
        "production",
        "migration",
        "external-dependency",
        "weak-validation",
        "unknown-dependency",
        "blocked",
    }
    timing_count = len(timing_signals & set(signals))
    if "explicit-high-risk" in signals and timing_count >= 1:
        return "high"
    if timing_count >= 2:
        return "high"
    if record.risk_level in _HIGH_RISK_VALUES:
        return "medium"
    if timing_count == 1:
        return "medium"
    return "low"


def _suggested_blockers(
    records: tuple[_TaskRecord, ...],
    by_task_id: Mapping[str, _TaskRecord],
    risk_by_id: Mapping[str, LaunchWindowTaskRisk],
) -> list[LaunchWindowBlocker]:
    blockers: list[LaunchWindowBlocker] = []
    for record in records:
        missing = tuple(
            dependency_id for dependency_id in record.depends_on if dependency_id not in by_task_id
        )
        if missing:
            blockers.append(
                LaunchWindowBlocker(
                    code="unknown_dependency",
                    severity="error",
                    reason=f"Task {record.task_id} depends on unknown task(s): {', '.join(missing)}.",
                    suggested_action="Add the missing prerequisite tasks or remove the dependency before launch.",
                    task_ids=(record.task_id, *missing),
                )
            )
        risk = risk_by_id[record.task_id]
        if risk.risk_level == "high" and "weak-validation" in risk.signals:
            blockers.append(
                LaunchWindowBlocker(
                    code="high_risk_weak_validation",
                    severity="error",
                    reason=f"High-risk task {record.task_id} has weak or missing validation gates.",
                    suggested_action="Add concrete smoke, migration, integration, or rollback validation evidence.",
                    task_ids=(record.task_id,),
                )
            )
        if record.status == "blocked" or record.blocked_reason:
            blockers.append(
                LaunchWindowBlocker(
                    code="blocked_task_in_launch_window",
                    severity="error",
                    reason=f"Task {record.task_id} is blocked before launch-window start.",
                    suggested_action="Resolve or explicitly defer the blocked task before starting the launch window.",
                    task_ids=(record.task_id,),
                )
            )
        if (
            "migration" in risk.signals
            and "production" in risk.signals
            and not _ROLLBACK_RE.search(record.context)
        ):
            blockers.append(
                LaunchWindowBlocker(
                    code="production_migration_without_rollback",
                    severity="warning",
                    reason=f"Production migration task {record.task_id} has no rollback or recovery signal.",
                    suggested_action="Document rollback, restore, or down-migration steps before launch.",
                    task_ids=(record.task_id,),
                )
            )
    return blockers


def _coordination_notes(
    records: tuple[_TaskRecord, ...],
    assignments: tuple[_PhaseAssignment, ...],
    risk_by_id: Mapping[str, LaunchWindowTaskRisk],
    blockers: tuple[LaunchWindowBlocker, ...],
) -> list[str]:
    notes: list[str] = []
    if any(assignment.source == "dependency" for assignment in assignments):
        notes.append(
            "Some tasks lack milestone or release metadata; dependency depth was used for phase order."
        )
    if blockers:
        notes.append("Resolve suggested launch blockers before opening the launch window.")
    high_risk = [risk.task_id for risk in risk_by_id.values() if risk.risk_level == "high"]
    if high_risk:
        notes.append(
            "Assign a launch captain and rollback decision owner for: " + ", ".join(high_risk) + "."
        )
    for record in records:
        notes.extend(_task_coordination_notes(record, risk_by_id))
    return _dedupe(notes)


def _task_coordination_notes(
    record: _TaskRecord,
    risk_by_id: Mapping[str, LaunchWindowTaskRisk],
) -> list[str]:
    risk = risk_by_id[record.task_id]
    notes: list[str] = []
    if "external-dependency" in risk.signals:
        notes.append(f"{record.task_id}: confirm external owner availability during the window.")
    if record.owner_type != "unassigned" or record.suggested_engine != "unassigned":
        notes.append(
            f"{record.task_id}: owner {record.owner_type}, engine {record.suggested_engine}."
        )
    return notes


def _dependency_blockers(
    record: _TaskRecord,
    by_task_id: Mapping[str, _TaskRecord],
) -> list[str]:
    blockers: list[str] = []
    for dependency_id in record.depends_on:
        dependency = by_task_id.get(dependency_id)
        if dependency is None:
            blockers.append(f"{record.task_id}: missing prerequisite {dependency_id}")
        elif dependency.status not in {"completed", "skipped"}:
            blockers.append(f"{record.task_id}: waits for {dependency_id} ({dependency.status})")
    return blockers


def _dependency_depths(
    records: tuple[_TaskRecord, ...],
    by_task_id: Mapping[str, _TaskRecord],
) -> dict[str, int]:
    depths: dict[str, int] = {}
    visiting: set[str] = set()

    def depth(record: _TaskRecord) -> int:
        if record.task_id in depths:
            return depths[record.task_id]
        if record.task_id in visiting:
            return 0
        visiting.add(record.task_id)
        known_dependencies = [
            by_task_id[dependency_id]
            for dependency_id in record.depends_on
            if dependency_id in by_task_id
        ]
        value = (
            0
            if not known_dependencies
            else 1 + max(depth(dependency) for dependency in known_dependencies)
        )
        visiting.remove(record.task_id)
        depths[record.task_id] = value
        return value

    for record in records:
        depth(record)
    return depths


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
        metadata = task.get("metadata")
        context = _task_context(task)
        records.append(
            _TaskRecord(
                task=task,
                task_id=task_id,
                title=_optional_text(task.get("title")) or task_id,
                index=index,
                milestone=_optional_text(task.get("milestone")),
                depends_on=tuple(
                    _dedupe(_strings(task.get("depends_on") or task.get("dependencies")))
                ),
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
                estimated_complexity=(
                    _optional_text(task.get("estimated_complexity"))
                    or _optional_text(task.get("complexity"))
                    or _optional_text(_metadata_value(metadata, "complexity"))
                    or "medium"
                ).casefold(),
                estimated_hours=_number(
                    task.get("estimated_hours") or _metadata_value(metadata, "estimated_hours")
                ),
                risk_level=(
                    _optional_text(task.get("risk_level"))
                    or _optional_text(task.get("risk"))
                    or _optional_text(_metadata_value(metadata, "risk_level"))
                    or _optional_text(_metadata_value(metadata, "risk"))
                    or "medium"
                ).casefold(),
                status=(_optional_text(task.get("status")) or "pending").casefold(),
                blocked_reason=_optional_text(task.get("blocked_reason"))
                or _optional_text(_metadata_value(metadata, "blocked_reason")),
                validation_gates=tuple(_validation_gates(task)),
                release_window=_release_value(task),
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
        "risk",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "status",
        "blocked_reason",
        "tags",
        "labels",
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
        name = _optional_text(milestone.get("name") or milestone.get("id")) or f"milestone-{index}"
        lookup[name] = milestone
    return lookup


def _milestone_order(milestones: list[dict[str, Any]]) -> dict[str, int]:
    return {
        _optional_text(milestone.get("name") or milestone.get("id"))
        or f"milestone-{index + 1}": index
        for index, milestone in enumerate(milestones)
    }


def _release_order(milestones: list[dict[str, Any]]) -> dict[str, int]:
    order: dict[str, int] = {}
    for milestone in milestones:
        name = _optional_text(milestone.get("name") or milestone.get("id"))
        release_window = _release_value(milestone)
        if not release_window and name and _RELEASE_MILESTONE_RE.search(name):
            release_window = name
        if release_window:
            order.setdefault(_slug(release_window), len(order))
    return order


def _milestone_release_window(
    milestone_name: str | None,
    milestone_lookup: Mapping[str, dict[str, Any]],
) -> str | None:
    if not milestone_name:
        return None
    milestone = milestone_lookup.get(milestone_name, {})
    return _release_value(milestone) or (
        milestone_name if _RELEASE_MILESTONE_RE.search(milestone_name) else None
    )


def _phase_source(assignments: list[_PhaseAssignment]) -> LaunchPhaseSource:
    sources = {assignment.source for assignment in assignments}
    for source in ("metadata", "milestone", "dependency", "unassigned"):
        if source in sources:
            return source  # type: ignore[return-value]
    return "unassigned"


def _release_value(item: Mapping[str, Any]) -> str | None:
    metadata = item.get("metadata")
    for key in _RELEASE_KEYS:
        candidates = (
            key,
            key.replace("_", "-"),
            "".join([key.split("_")[0], key.split("_")[1].title()]),
        )
        for candidate in candidates:
            if text := _optional_text(item.get(candidate)):
                return text
            if text := _optional_text(_metadata_value(metadata, candidate)):
                return text
    return None


def _task_context(task: Mapping[str, Any]) -> str:
    values: list[str] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "estimated_complexity",
        "risk_level",
        "risk",
        "test_command",
        "suggested_test_command",
        "validation_command",
        "status",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            values.append(text)
    values.extend(_strings(task.get("files_or_modules") or task.get("files")))
    values.extend(_strings(task.get("acceptance_criteria")))
    values.extend(_strings(task.get("tags")))
    values.extend(_strings(task.get("labels")))
    values.extend(_metadata_texts(task.get("metadata")))
    return " ".join(values)


def _validation_gates(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        if text := _optional_text(task.get(key)):
            commands.append(text)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "validation_gates",
            "validation_commands",
            "validation_command",
            "test_commands",
            "test_command",
            "smoke_tests",
            "smoke_test",
        ):
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
            texts.append(str(key))
            texts.extend(_metadata_texts(value[key]))
        return texts
    return _strings(value)


def _metadata_value(metadata: Any, key: str) -> Any:
    if not isinstance(metadata, Mapping):
        return None
    return metadata.get(key)


def _has_weak_validation(validation_gates: tuple[str, ...]) -> bool:
    if not validation_gates:
        return True
    return all(
        gate.casefold() in _WEAK_VALIDATION_VALUES or not _VALIDATION_RE.search(gate)
        for gate in validation_gates
    )


def _estimated_hours(record: _TaskRecord) -> float:
    if record.estimated_hours is not None:
        return record.estimated_hours
    return float(_COMPLEXITY_HOURS.get(record.estimated_complexity, 8))


def _number(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def _slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.casefold()).strip("-")
    return slug or "phase"


def _format_hours(value: float) -> str:
    return str(int(value)) if value.is_integer() else str(value)


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "LaunchBlockerSeverity",
    "LaunchPhaseSource",
    "LaunchRiskLevel",
    "LaunchWindowBlocker",
    "LaunchWindowPhase",
    "LaunchWindowTaskRisk",
    "PlanLaunchWindowRiskMap",
    "build_plan_launch_window_risk_map",
    "derive_plan_launch_window_risk_map",
    "plan_launch_window_risk_map_to_dict",
    "plan_launch_window_risk_map_to_markdown",
]
