"""Helpers for grouping execution plan tasks into dispatchable batches."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


_COMPLETED_STATUS = "completed"


@dataclass(frozen=True)
class ExecutionBatch:
    """A deterministic group of tasks that can be dispatched in parallel."""

    batch_index: int
    task_ids: list[str] = field(default_factory=list)
    blocked_task_ids: list[str] = field(default_factory=list)
    unresolved_dependency_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable batch payload."""
        return {
            "batch_index": self.batch_index,
            "task_ids": self.task_ids,
            "blocked_task_ids": self.blocked_task_ids,
            "unresolved_dependency_ids": self.unresolved_dependency_ids,
        }


def build_execution_batches(
    plan: dict[str, Any],
    include_completed: bool = False,
) -> list[ExecutionBatch]:
    """Group plan tasks into earliest-possible parallel execution batches.

    Completed tasks are omitted by default, but still satisfy downstream
    dependencies. Unknown dependencies and dependency cycles are surfaced in a
    final blocked batch instead of raising.
    """
    tasks = _list_of_dicts(plan.get("tasks"))
    task_ids = _task_ids(tasks)
    task_id_set = set(task_ids)
    dependency_ids_by_task_id = {
        task_id: _dependency_ids(task)
        for task in tasks
        if (task_id := _task_id(task)) in task_id_set
    }

    completed_ids = {
        _task_id(task)
        for task in tasks
        if _task_id(task) in task_id_set and _status(task) == _COMPLETED_STATUS
    }
    schedulable_ids = [
        task_id for task_id in task_ids if include_completed or task_id not in completed_ids
    ]
    schedulable_id_set = set(schedulable_ids)
    satisfied_ids = set() if include_completed else set(completed_ids)
    scheduled_ids: set[str] = set()
    blocked_ids: set[str] = set()
    batches: list[ExecutionBatch] = []

    unknown_dependency_ids_by_task_id = {
        task_id: [
            dependency_id
            for dependency_id in dependency_ids_by_task_id.get(task_id, [])
            if dependency_id not in task_id_set
        ]
        for task_id in schedulable_ids
    }
    for task_id, unknown_dependency_ids in unknown_dependency_ids_by_task_id.items():
        if unknown_dependency_ids:
            blocked_ids.add(task_id)

    while True:
        ready_ids = [
            task_id
            for task_id in schedulable_ids
            if task_id not in scheduled_ids
            and task_id not in blocked_ids
            and all(
                dependency_id in satisfied_ids
                for dependency_id in dependency_ids_by_task_id.get(task_id, [])
            )
        ]
        if not ready_ids:
            break

        batches.append(ExecutionBatch(batch_index=len(batches) + 1, task_ids=ready_ids))
        scheduled_ids.update(ready_ids)
        satisfied_ids.update(ready_ids)

    unscheduled_ids = [
        task_id
        for task_id in schedulable_ids
        if task_id not in scheduled_ids and task_id not in blocked_ids
    ]
    if unscheduled_ids:
        blocked_ids.update(unscheduled_ids)

    if blocked_ids:
        blocked_task_ids = [task_id for task_id in schedulable_ids if task_id in blocked_ids]
        unresolved_dependency_ids = _unresolved_dependency_ids(
            blocked_task_ids=blocked_task_ids,
            dependency_ids_by_task_id=dependency_ids_by_task_id,
            schedulable_id_set=schedulable_id_set,
            satisfied_ids=satisfied_ids,
            task_id_set=task_id_set,
        )
        batches.append(
            ExecutionBatch(
                batch_index=len(batches) + 1,
                blocked_task_ids=blocked_task_ids,
                unresolved_dependency_ids=unresolved_dependency_ids,
            )
        )

    return batches


def _unresolved_dependency_ids(
    *,
    blocked_task_ids: list[str],
    dependency_ids_by_task_id: dict[str, list[str]],
    schedulable_id_set: set[str],
    satisfied_ids: set[str],
    task_id_set: set[str],
) -> list[str]:
    unresolved_ids: list[str] = []
    seen_ids: set[str] = set()
    for task_id in blocked_task_ids:
        for dependency_id in dependency_ids_by_task_id.get(task_id, []):
            if dependency_id in satisfied_ids:
                continue
            if dependency_id not in task_id_set or dependency_id in schedulable_id_set:
                if dependency_id not in seen_ids:
                    seen_ids.add(dependency_id)
                    unresolved_ids.append(dependency_id)
    return unresolved_ids


def _task_ids(tasks: list[dict[str, Any]]) -> list[str]:
    task_ids: list[str] = []
    seen_ids: set[str] = set()
    for task in tasks:
        task_id = _task_id(task)
        if task_id and task_id not in seen_ids:
            seen_ids.add(task_id)
            task_ids.append(task_id)
    return task_ids


def _task_id(task: dict[str, Any]) -> str:
    return str(task.get("id") or "")


def _status(task: dict[str, Any]) -> str:
    return str(task.get("status") or "").strip().lower()


def _dependency_ids(task: dict[str, Any]) -> list[str]:
    return _string_list(task.get("depends_on"))


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item]


__all__ = [
    "ExecutionBatch",
    "build_execution_batches",
]
