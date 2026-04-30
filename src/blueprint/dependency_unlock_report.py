"""Report execution-plan tasks unblocked by completed dependencies."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


@dataclass(frozen=True, slots=True)
class UnlockedTask:
    """An incomplete task whose dependencies are all satisfied."""

    task_id: str
    title: str
    dependency_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "dependency_ids": list(self.dependency_ids),
        }


@dataclass(frozen=True, slots=True)
class StillBlockedTask:
    """An incomplete task that cannot start because dependencies remain unresolved."""

    task_id: str
    title: str
    dependency_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_dependency_ids: tuple[str, ...] = field(default_factory=tuple)
    unknown_dependency_ids: tuple[str, ...] = field(default_factory=tuple)
    blocked_reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "dependency_ids": list(self.dependency_ids),
            "missing_dependency_ids": list(self.missing_dependency_ids),
            "unknown_dependency_ids": list(self.unknown_dependency_ids),
            "blocked_reason": self.blocked_reason,
        }


@dataclass(frozen=True, slots=True)
class DependencyUnlockReport:
    """Tasks that are ready or still blocked after dependency completion."""

    plan_id: str | None
    completed_task_ids: tuple[str, ...]
    unlocked_tasks: tuple[UnlockedTask, ...]
    blocked_tasks: tuple[StillBlockedTask, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "completed_task_ids": list(self.completed_task_ids),
            "unlocked_tasks": [task.to_dict() for task in self.unlocked_tasks],
            "blocked_tasks": [task.to_dict() for task in self.blocked_tasks],
        }


def build_dependency_unlock_report(
    plan: Mapping[str, Any] | ExecutionPlan,
    completed_task_ids: list[str] | tuple[str, ...] | set[str],
) -> DependencyUnlockReport:
    """Build a deterministic unlock report for an execution plan.

    Completed tasks are omitted from both result groups. Dependencies are read
    from each task's ``depends_on`` field and interpreted as task IDs.
    """
    plan_payload = _plan_payload(plan)
    tasks = _task_payloads(plan_payload.get("tasks"))
    task_ids = _task_ids(tasks)
    task_id_set = set(task_ids)
    completed_ids = tuple(_strings(completed_task_ids))
    completed_id_set = set(completed_ids)

    unlocked_tasks: list[UnlockedTask] = []
    blocked_tasks: list[StillBlockedTask] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _text(task.get("id")) or f"task-{index}"
        if task_id in completed_id_set:
            continue

        title = _text(task.get("title")) or task_id
        dependency_ids = tuple(_strings(task.get("depends_on")))
        unknown_dependency_ids = tuple(
            dependency_id for dependency_id in dependency_ids if dependency_id not in task_id_set
        )
        missing_dependency_ids = tuple(
            dependency_id
            for dependency_id in dependency_ids
            if dependency_id in task_id_set and dependency_id not in completed_id_set
        )

        if unknown_dependency_ids or missing_dependency_ids:
            blocked_tasks.append(
                StillBlockedTask(
                    task_id=task_id,
                    title=title,
                    dependency_ids=dependency_ids,
                    missing_dependency_ids=missing_dependency_ids,
                    unknown_dependency_ids=unknown_dependency_ids,
                    blocked_reason=_blocked_reason(
                        missing_dependency_ids=missing_dependency_ids,
                        unknown_dependency_ids=unknown_dependency_ids,
                    ),
                )
            )
            continue

        unlocked_tasks.append(
            UnlockedTask(
                task_id=task_id,
                title=title,
                dependency_ids=dependency_ids,
            )
        )

    return DependencyUnlockReport(
        plan_id=_optional_text(plan_payload.get("id")),
        completed_task_ids=completed_ids,
        unlocked_tasks=tuple(unlocked_tasks),
        blocked_tasks=tuple(blocked_tasks),
    )


def dependency_unlock_report_to_dict(report: DependencyUnlockReport) -> dict[str, Any]:
    """Serialize a dependency unlock report to a dictionary."""
    return report.to_dict()


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


def _task_ids(tasks: list[dict[str, Any]]) -> list[str]:
    task_ids: list[str] = []
    seen: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        task_id = _text(task.get("id")) or f"task-{index}"
        if task_id in seen:
            continue
        task_ids.append(task_id)
        seen.add(task_id)
    return task_ids


def _strings(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple, set)):
        return []

    strings: list[str] = []
    seen: set[str] = set()
    for item in value:
        item_text = _text(item)
        if item_text is None or item_text in seen:
            continue
        strings.append(item_text)
        seen.add(item_text)
    return strings


def _blocked_reason(
    *,
    missing_dependency_ids: tuple[str, ...],
    unknown_dependency_ids: tuple[str, ...],
) -> str:
    reasons: list[str] = []
    if missing_dependency_ids:
        reasons.append(
            "waiting for completed dependencies: "
            + ", ".join(missing_dependency_ids)
        )
    if unknown_dependency_ids:
        reasons.append(
            "references unknown dependencies: "
            + ", ".join(unknown_dependency_ids)
        )
    return "; ".join(reasons)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text if text else None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


dependency_unlock_report_to_dict.__test__ = False


__all__ = [
    "DependencyUnlockReport",
    "StillBlockedTask",
    "UnlockedTask",
    "build_dependency_unlock_report",
    "dependency_unlock_report_to_dict",
]
