"""Build milestone-level exit criteria for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


_COMPLETED_STATUS = "completed"
_BLOCKING_STATUSES = {"blocked", "in_progress", "pending"}
_HIGH_RISK_LEVELS = {"critical", "high"}
_UNGROUPED_MILESTONE = "Ungrouped"


@dataclass(frozen=True, slots=True)
class MilestoneExitCriteria:
    """Exit readiness criteria for one execution-plan milestone."""

    milestone: str
    criteria: tuple[str, ...] = field(default_factory=tuple)
    blocking_task_ids: tuple[str, ...] = field(default_factory=tuple)
    required_validation_commands: tuple[str, ...] = field(default_factory=tuple)
    ready_to_exit: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "milestone": self.milestone,
            "criteria": list(self.criteria),
            "blocking_task_ids": list(self.blocking_task_ids),
            "required_validation_commands": list(self.required_validation_commands),
            "ready_to_exit": self.ready_to_exit,
        }


def build_milestone_exit_criteria(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> tuple[MilestoneExitCriteria, ...]:
    """Group plan tasks by milestone and derive milestone exit readiness."""
    payload = _plan_payload(plan)
    tasks = _task_payloads(payload.get("tasks"))
    tasks_by_id = {
        task_id: task
        for index, task in enumerate(tasks, start=1)
        if (task_id := _task_id(task, index))
    }
    groups = _group_tasks(payload, tasks)

    exit_criteria: list[MilestoneExitCriteria] = []
    for milestone, milestone_tasks in groups:
        criteria: list[str] = []
        blocking_task_ids: list[str] = []
        validation_commands: list[str] = []

        for index, task in milestone_tasks:
            task_id = _task_id(task, index)
            title = _optional_text(task.get("title")) or task_id
            status = _status(task)

            for criterion in _string_list(task.get("acceptance_criteria")):
                if status == _COMPLETED_STATUS:
                    criteria.append(f"Satisfied: `{task_id}` {criterion}")
                else:
                    criteria.append(f"Complete `{task_id}` {title}: {criterion}")

            if command := _optional_text(task.get("test_command")):
                validation_commands.append(command)
                criteria.append(f"Validate `{task_id}` with `{command}`.")

            for dependency_id in _dependency_ids(task):
                dependency = tasks_by_id.get(dependency_id)
                dependency_status = _status(dependency) if dependency else ""
                if dependency_status == _COMPLETED_STATUS:
                    criteria.append(f"Dependency `{dependency_id}` is completed for `{task_id}`.")
                else:
                    criteria.append(
                        f"Complete dependency `{dependency_id}` before exiting `{task_id}`."
                    )
                    _append_unique(blocking_task_ids, task_id)

            if _is_high_risk(_risk_level(task)):
                criteria.append(f"Review high-risk controls for `{task_id}` before exit.")

            if status in _BLOCKING_STATUSES:
                _append_unique(blocking_task_ids, task_id)

        exit_criteria.append(
            MilestoneExitCriteria(
                milestone=milestone,
                criteria=tuple(criteria),
                blocking_task_ids=tuple(blocking_task_ids),
                required_validation_commands=tuple(_dedupe(validation_commands)),
                ready_to_exit=not blocking_task_ids,
            )
        )

    return tuple(exit_criteria)


def milestone_exit_criteria_to_dict(
    criteria: tuple[MilestoneExitCriteria, ...] | list[MilestoneExitCriteria],
) -> list[dict[str, Any]]:
    """Serialize milestone exit criteria to dictionaries."""
    return [item.to_dict() for item in criteria]


milestone_exit_criteria_to_dict.__test__ = False


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


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


def _group_tasks(
    plan: Mapping[str, Any],
    tasks: list[dict[str, Any]],
) -> list[tuple[str, list[tuple[int, dict[str, Any]]]]]:
    grouped: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    ordered_milestones = _milestone_names(plan.get("milestones"))
    for milestone in ordered_milestones:
        grouped[milestone] = []

    for index, task in enumerate(tasks, start=1):
        milestone = _optional_text(task.get("milestone")) or _UNGROUPED_MILESTONE
        if milestone not in grouped:
            grouped[milestone] = []
        grouped[milestone].append((index, task))

    return [(milestone, grouped[milestone]) for milestone in grouped if grouped[milestone]]


def _milestone_names(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []

    names: list[str] = []
    for index, milestone in enumerate(value, start=1):
        name: str | None = None
        if isinstance(milestone, Mapping):
            name = _optional_text(milestone.get("name")) or _optional_text(
                milestone.get("title")
            )
        if name is None:
            name = f"Milestone {index}"
        _append_unique(names, name)
    return names


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _dependency_ids(task: Mapping[str, Any]) -> list[str]:
    return _string_list(task.get("depends_on"))


def _risk_level(task: Mapping[str, Any]) -> str | None:
    metadata = task.get("metadata")
    metadata_risk = metadata.get("risk_level") if isinstance(metadata, Mapping) else None
    return _optional_text(task.get("risk_level")) or _optional_text(metadata_risk)


def _status(task: Mapping[str, Any] | None) -> str:
    if task is None:
        return ""
    return (_optional_text(task.get("status")) or "pending").lower()


def _is_high_risk(value: str | None) -> bool:
    return value is not None and value.lower() in _HIGH_RISK_LEVELS


def _dedupe(values: list[str]) -> list[str]:
    deduped: list[str] = []
    for value in values:
        _append_unique(deduped, value)
    return deduped


def _append_unique(values: list[str], value: str) -> None:
    if value not in values:
        values.append(value)


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [text for item in value if (text := _optional_text(item))]


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


__all__ = [
    "MilestoneExitCriteria",
    "build_milestone_exit_criteria",
    "milestone_exit_criteria_to_dict",
]
