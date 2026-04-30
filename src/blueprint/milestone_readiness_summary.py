"""Summarize execution readiness by milestone."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan
from blueprint.validation_commands import flatten_validation_commands


ReadinessStatus = Literal["ready", "needs_attention", "blocked"]
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class MilestoneReadinessSummary:
    """Readiness summary for one execution-plan milestone."""

    milestone_name: str
    task_count: int
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    readiness_status: ReadinessStatus = "needs_attention"
    blocked_task_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_owner_task_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_agent_hint_task_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_validation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    unresolved_dependency_ids: tuple[str, ...] = field(default_factory=tuple)
    acceptance_criteria_count: int = 0
    tasks_with_acceptance_criteria_count: int = 0
    tasks_missing_acceptance_criteria_count: int = 0
    missing_acceptance_criteria_task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "milestone_name": self.milestone_name,
            "task_count": self.task_count,
            "task_ids": list(self.task_ids),
            "readiness_status": self.readiness_status,
            "blocked_task_ids": list(self.blocked_task_ids),
            "missing_owner_task_ids": list(self.missing_owner_task_ids),
            "missing_agent_hint_task_ids": list(self.missing_agent_hint_task_ids),
            "missing_validation_task_ids": list(self.missing_validation_task_ids),
            "unresolved_dependency_ids": list(self.unresolved_dependency_ids),
            "acceptance_criteria_count": self.acceptance_criteria_count,
            "tasks_with_acceptance_criteria_count": (
                self.tasks_with_acceptance_criteria_count
            ),
            "tasks_missing_acceptance_criteria_count": (
                self.tasks_missing_acceptance_criteria_count
            ),
            "missing_acceptance_criteria_task_ids": list(
                self.missing_acceptance_criteria_task_ids
            ),
        }


def build_milestone_readiness_summaries(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> tuple[MilestoneReadinessSummary, ...]:
    """Build deterministic milestone readiness summaries without database access."""
    payload = _plan_payload(plan)
    milestone_names = _milestone_names(payload.get("milestones"))
    tasks = _task_records(_task_payloads(payload.get("tasks")))
    known_task_ids = {record.task_id for record in tasks}
    blocked_task_ids = {
        record.task_id
        for record in tasks
        if _text(record.task.get("status")).lower() == "blocked"
    }

    grouped: dict[str, list[_TaskRecord]] = {name: [] for name in milestone_names}
    unassigned: list[_TaskRecord] = []
    declared = set(milestone_names)

    for record in tasks:
        milestone = _optional_text(record.task.get("milestone"))
        if milestone and milestone in declared:
            grouped[milestone].append(record)
        else:
            unassigned.append(record)

    summaries = [
        _summary_for_milestone(
            milestone_name,
            grouped[milestone_name],
            known_task_ids=known_task_ids,
            blocked_task_ids=blocked_task_ids,
        )
        for milestone_name in milestone_names
    ]
    if unassigned:
        summaries.append(
            _summary_for_milestone(
                "Unassigned",
                unassigned,
                known_task_ids=known_task_ids,
                blocked_task_ids=blocked_task_ids,
            )
        )
    return tuple(summaries)


def milestone_readiness_summaries_to_dict(
    summaries: tuple[MilestoneReadinessSummary, ...] | list[MilestoneReadinessSummary],
) -> list[dict[str, Any]]:
    """Serialize milestone readiness summaries to plain dictionaries."""
    return [summary.to_dict() for summary in summaries]


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str


def _summary_for_milestone(
    milestone_name: str,
    records: list[_TaskRecord],
    *,
    known_task_ids: set[str],
    blocked_task_ids: set[str],
) -> MilestoneReadinessSummary:
    task_ids = [record.task_id for record in records]
    blocked = [
        record.task_id
        for record in records
        if _text(record.task.get("status")).lower() == "blocked"
    ]
    missing_owner = [
        record.task_id
        for record in records
        if not _optional_text(record.task.get("owner_type"))
    ]
    missing_agent_hint = [
        record.task_id
        for record in records
        if _requires_agent_hint(record.task)
        and not _optional_text(record.task.get("suggested_engine"))
    ]
    missing_validation = [
        record.task_id for record in records if not _task_validation_commands(record.task)
    ]
    unresolved_dependencies = _unresolved_dependency_ids(
        records,
        known_task_ids=known_task_ids,
        blocked_task_ids=blocked_task_ids,
    )
    acceptance_by_task = {
        record.task_id: _strings(record.task.get("acceptance_criteria"))
        for record in records
    }
    missing_acceptance = [
        task_id for task_id, criteria in acceptance_by_task.items() if not criteria
    ]

    return MilestoneReadinessSummary(
        milestone_name=milestone_name,
        task_count=len(records),
        task_ids=tuple(task_ids),
        readiness_status=_readiness_status(
            task_count=len(records),
            blocked_task_ids=blocked,
            unresolved_dependency_ids=unresolved_dependencies,
            missing_owner_task_ids=missing_owner,
            missing_agent_hint_task_ids=missing_agent_hint,
            missing_validation_task_ids=missing_validation,
            missing_acceptance_criteria_task_ids=missing_acceptance,
        ),
        blocked_task_ids=tuple(blocked),
        missing_owner_task_ids=tuple(missing_owner),
        missing_agent_hint_task_ids=tuple(missing_agent_hint),
        missing_validation_task_ids=tuple(missing_validation),
        unresolved_dependency_ids=tuple(unresolved_dependencies),
        acceptance_criteria_count=sum(len(criteria) for criteria in acceptance_by_task.values()),
        tasks_with_acceptance_criteria_count=sum(
            1 for criteria in acceptance_by_task.values() if criteria
        ),
        tasks_missing_acceptance_criteria_count=len(missing_acceptance),
        missing_acceptance_criteria_task_ids=tuple(missing_acceptance),
    )


def _readiness_status(
    *,
    task_count: int,
    blocked_task_ids: list[str],
    unresolved_dependency_ids: list[str],
    missing_owner_task_ids: list[str],
    missing_agent_hint_task_ids: list[str],
    missing_validation_task_ids: list[str],
    missing_acceptance_criteria_task_ids: list[str],
) -> ReadinessStatus:
    if blocked_task_ids or unresolved_dependency_ids:
        return "blocked"
    if (
        task_count == 0
        or missing_owner_task_ids
        or missing_agent_hint_task_ids
        or missing_validation_task_ids
        or missing_acceptance_criteria_task_ids
    ):
        return "needs_attention"
    return "ready"


def _unresolved_dependency_ids(
    records: list[_TaskRecord],
    *,
    known_task_ids: set[str],
    blocked_task_ids: set[str],
) -> list[str]:
    dependency_ids: list[str] = []
    for record in records:
        for dependency_id in _strings(record.task.get("depends_on")):
            if dependency_id not in known_task_ids or dependency_id in blocked_task_ids:
                dependency_ids.append(dependency_id)
    return _dedupe(dependency_ids)


def _requires_agent_hint(task: Mapping[str, Any]) -> bool:
    owner_type = _text(task.get("owner_type")).lower()
    return owner_type in {"", "agent"}


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
    return {}


def _milestone_names(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []

    names: list[str] = []
    for index, item in enumerate(value, start=1):
        name = ""
        if isinstance(item, Mapping):
            name = _optional_text(item.get("name")) or _optional_text(item.get("title")) or ""
        else:
            name = _optional_text(item) or ""
        names.append(name or f"Milestone {index}")
    return _dedupe(names)


def _task_records(tasks: list[dict[str, Any]]) -> list[_TaskRecord]:
    return [
        _TaskRecord(task=task, task_id=_task_id(task, index))
        for index, task in enumerate(tasks, start=1)
    ]


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        command = _optional_text(task.get(key))
        if command:
            commands.append(command)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        commands.extend(_commands_from_value(metadata.get("validation_commands")))
        commands.extend(_commands_from_value(metadata.get("validation_command")))
        commands.extend(_commands_from_value(metadata.get("test_commands")))
    return _dedupe(commands)


def _commands_from_value(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return flatten_validation_commands(value)
    return _strings(value)


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if isinstance(value, str):
        return " ".join(value.split())
    return ""


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for item in value.values():
            strings.extend(_strings(item))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    return []


def _dedupe(values: list[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "MilestoneReadinessSummary",
    "ReadinessStatus",
    "build_milestone_readiness_summaries",
    "milestone_readiness_summaries_to_dict",
]
