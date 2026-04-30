"""Score execution task readiness from dependency status."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


TaskReadinessState = Literal[
    "ready",
    "waiting",
    "blocked",
    "completed",
    "in_progress",
    "skipped",
]

_SATISFIED_DEPENDENCY_STATUSES = {"completed", "skipped"}
_WAITING_DEPENDENCY_STATUSES = {"pending", "in_progress"}
_BLOCKED_DEPENDENCY_STATUSES = {"blocked"}
_TERMINAL_TASK_STATUSES = {"completed", "skipped"}


@dataclass(frozen=True)
class TaskReadiness:
    """Readiness classification for one execution task."""

    task_id: str
    title: str
    task_status: str
    readiness: TaskReadinessState
    dependency_ids: list[str] = field(default_factory=list)
    satisfied_dependency_ids: list[str] = field(default_factory=list)
    waiting_dependency_ids: list[str] = field(default_factory=list)
    blocked_dependency_ids: list[str] = field(default_factory=list)
    invalid_dependency_ids: list[str] = field(default_factory=list)

    @property
    def ready(self) -> bool:
        return self.readiness == "ready"

    @property
    def has_invalid_dependencies(self) -> bool:
        return bool(self.invalid_dependency_ids)

    @property
    def blocker_dependency_ids(self) -> list[str]:
        if self.readiness == "waiting":
            return self.waiting_dependency_ids
        if self.readiness == "blocked":
            return [*self.blocked_dependency_ids, *self.invalid_dependency_ids]
        return []

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "task_status": self.task_status,
            "readiness": self.readiness,
            "ready": self.ready,
            "dependency_ids": self.dependency_ids,
            "satisfied_dependency_ids": self.satisfied_dependency_ids,
            "waiting_dependency_ids": self.waiting_dependency_ids,
            "blocked_dependency_ids": self.blocked_dependency_ids,
            "invalid_dependency_ids": self.invalid_dependency_ids,
            "blocker_dependency_ids": self.blocker_dependency_ids,
        }


@dataclass(frozen=True)
class TaskReadinessResult:
    """Aggregate readiness scores for an execution plan."""

    plan_id: str
    tasks: list[TaskReadiness] = field(default_factory=list)

    @property
    def ready_tasks(self) -> list[TaskReadiness]:
        return [task for task in self.tasks if task.readiness == "ready"]

    @property
    def waiting_tasks(self) -> list[TaskReadiness]:
        return [task for task in self.tasks if task.readiness == "waiting"]

    @property
    def blocked_tasks(self) -> list[TaskReadiness]:
        return [task for task in self.tasks if task.readiness == "blocked"]

    @property
    def completed_tasks(self) -> list[TaskReadiness]:
        return [task for task in self.tasks if task.readiness == "completed"]

    @property
    def in_progress_tasks(self) -> list[TaskReadiness]:
        return [task for task in self.tasks if task.readiness == "in_progress"]

    @property
    def skipped_tasks(self) -> list[TaskReadiness]:
        return [task for task in self.tasks if task.readiness == "skipped"]

    @property
    def invalid_tasks(self) -> list[TaskReadiness]:
        return [task for task in self.tasks if task.has_invalid_dependencies]

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "summary": {
                "ready": len(self.ready_tasks),
                "waiting": len(self.waiting_tasks),
                "blocked": len(self.blocked_tasks),
                "completed": len(self.completed_tasks),
                "in_progress": len(self.in_progress_tasks),
                "skipped": len(self.skipped_tasks),
                "invalid": len(self.invalid_tasks),
                "tasks": len(self.tasks),
            },
            "ready_task_ids": [task.task_id for task in self.ready_tasks],
            "waiting_task_ids": [task.task_id for task in self.waiting_tasks],
            "blocked_task_ids": [task.task_id for task in self.blocked_tasks],
            "invalid_task_ids": [task.task_id for task in self.invalid_tasks],
            "tasks": [task.to_dict() for task in self.tasks],
        }


def score_task_readiness(plan: dict[str, Any]) -> TaskReadinessResult:
    """Return deterministic readiness scores for every task in a plan-shaped dict."""
    tasks = _list_of_dicts(plan.get("tasks"))
    tasks_by_id = {
        task_id: task
        for task in tasks
        if (task_id := _task_id(task))
    }

    return TaskReadinessResult(
        plan_id=str(plan.get("id") or ""),
        tasks=[_score_task(task, tasks_by_id) for task in tasks],
    )


def _score_task(
    task: dict[str, Any],
    tasks_by_id: dict[str, dict[str, Any]],
) -> TaskReadiness:
    task_status = _status(task)
    dependency_ids = _dependency_ids(task)
    satisfied_dependency_ids: list[str] = []
    waiting_dependency_ids: list[str] = []
    blocked_dependency_ids: list[str] = []
    invalid_dependency_ids: list[str] = []

    for dependency_id in dependency_ids:
        dependency = tasks_by_id.get(dependency_id)
        if dependency is None:
            invalid_dependency_ids.append(dependency_id)
            continue

        dependency_status = _status(dependency)
        if dependency_status in _SATISFIED_DEPENDENCY_STATUSES:
            satisfied_dependency_ids.append(dependency_id)
        elif dependency_status in _BLOCKED_DEPENDENCY_STATUSES:
            blocked_dependency_ids.append(dependency_id)
        elif dependency_status in _WAITING_DEPENDENCY_STATUSES:
            waiting_dependency_ids.append(dependency_id)
        else:
            waiting_dependency_ids.append(dependency_id)

    if task_status in _TERMINAL_TASK_STATUSES:
        readiness = task_status
    elif task_status == "in_progress":
        readiness = "in_progress"
    elif task_status == "blocked":
        readiness = "blocked"
    elif invalid_dependency_ids or blocked_dependency_ids:
        readiness = "blocked"
    elif waiting_dependency_ids:
        readiness = "waiting"
    else:
        readiness = "ready"

    return TaskReadiness(
        task_id=_task_id(task),
        title=str(task.get("title") or ""),
        task_status=task_status,
        readiness=readiness,
        dependency_ids=dependency_ids,
        satisfied_dependency_ids=satisfied_dependency_ids,
        waiting_dependency_ids=waiting_dependency_ids,
        blocked_dependency_ids=blocked_dependency_ids,
        invalid_dependency_ids=invalid_dependency_ids,
    )


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
    dependency_ids: list[str] = []
    for item in value:
        dependency_id = str(item)
        if dependency_id and dependency_id not in dependency_ids:
            dependency_ids.append(dependency_id)
    return dependency_ids


__all__ = [
    "TaskReadiness",
    "TaskReadinessResult",
    "TaskReadinessState",
    "score_task_readiness",
]
