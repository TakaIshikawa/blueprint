"""Dependency readiness gate for pending execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


DependencyGateStatus = Literal["ready", "waiting", "blocked"]
DependencyGateReasonCode = Literal[
    "all_dependencies_completed",
    "dependency_incomplete",
    "dependency_blocked",
    "dependency_skipped",
    "unknown_dependency",
]

_READY_DEPENDENCY_STATUSES = {"completed"}
_WAITING_DEPENDENCY_STATUSES = {"pending", "in_progress"}
_BLOCKED_DEPENDENCY_STATUSES = {"blocked", "skipped"}


@dataclass(frozen=True)
class DependencyGateReason:
    """A machine-readable reason for one dependency gate decision."""

    code: DependencyGateReasonCode
    dependency_id: str | None
    message: str
    dependency_status: str | None = None

    @property
    def error(self) -> bool:
        return self.code == "unknown_dependency"

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "message": self.message,
            "error": self.error,
        }
        if self.dependency_id is not None:
            payload["dependency_id"] = self.dependency_id
        if self.dependency_status is not None:
            payload["dependency_status"] = self.dependency_status
        return payload


@dataclass(frozen=True)
class DependencyGateTask:
    """Dependency gate classification for one pending task."""

    task_id: str
    title: str
    status: DependencyGateStatus
    dependency_ids: list[str] = field(default_factory=list)
    reasons: list[DependencyGateReason] = field(default_factory=list)

    @property
    def ready(self) -> bool:
        return self.status == "ready"

    @property
    def has_errors(self) -> bool:
        return any(reason.error for reason in self.reasons)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "status": self.status,
            "ready": self.ready,
            "dependency_ids": self.dependency_ids,
            "reasons": [reason.to_dict() for reason in self.reasons],
        }


@dataclass(frozen=True)
class DependencyGateResult:
    """Dependency gate result for pending execution tasks in a plan."""

    plan_id: str
    tasks: list[DependencyGateTask] = field(default_factory=list)

    @property
    def ready_tasks(self) -> list[DependencyGateTask]:
        return [task for task in self.tasks if task.status == "ready"]

    @property
    def waiting_tasks(self) -> list[DependencyGateTask]:
        return [task for task in self.tasks if task.status == "waiting"]

    @property
    def blocked_tasks(self) -> list[DependencyGateTask]:
        return [task for task in self.tasks if task.status == "blocked"]

    @property
    def ready_count(self) -> int:
        return len(self.ready_tasks)

    @property
    def waiting_count(self) -> int:
        return len(self.waiting_tasks)

    @property
    def blocked_count(self) -> int:
        return len(self.blocked_tasks)

    @property
    def error_count(self) -> int:
        return sum(1 for task in self.tasks if task.has_errors)

    @property
    def passed(self) -> bool:
        return self.blocked_count == 0 and self.error_count == 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "passed": self.passed,
            "summary": {
                "ready": self.ready_count,
                "waiting": self.waiting_count,
                "blocked": self.blocked_count,
                "errors": self.error_count,
                "tasks": len(self.tasks),
            },
            "ready_task_ids": [task.task_id for task in self.ready_tasks],
            "waiting_task_ids": [task.task_id for task in self.waiting_tasks],
            "blocked_task_ids": [task.task_id for task in self.blocked_tasks],
            "tasks": [task.to_dict() for task in self.tasks],
        }


def audit_dependency_gate(plan: dict[str, Any]) -> DependencyGateResult:
    """Classify pending tasks by dependency readiness for dispatch."""
    tasks = _list_of_dicts(plan.get("tasks"))
    tasks_by_id = {
        task_id: task
        for task in tasks
        if (task_id := str(task.get("id") or ""))
    }

    gated_tasks = [
        _classify_task(task, tasks_by_id)
        for task in tasks
        if str(task.get("status") or "") == "pending"
    ]
    return DependencyGateResult(plan_id=str(plan.get("id") or ""), tasks=gated_tasks)


def _classify_task(
    task: dict[str, Any],
    tasks_by_id: dict[str, dict[str, Any]],
) -> DependencyGateTask:
    task_id = str(task.get("id") or "")
    dependency_ids = _string_list(task.get("depends_on"))
    reasons = [
        reason
        for dependency_id in dependency_ids
        if (
            reason := _dependency_reason(
                task_id=task_id,
                dependency_id=dependency_id,
                tasks_by_id=tasks_by_id,
            )
        )
    ]

    if any(
        reason.code in {"dependency_blocked", "dependency_skipped", "unknown_dependency"}
        for reason in reasons
    ):
        gate_status: DependencyGateStatus = "blocked"
    elif any(reason.code == "dependency_incomplete" for reason in reasons):
        gate_status = "waiting"
    else:
        gate_status = "ready"
        reasons = [
            DependencyGateReason(
                code="all_dependencies_completed",
                dependency_id=None,
                message=_ready_message(task_id, dependency_ids),
            )
        ]

    return DependencyGateTask(
        task_id=task_id,
        title=str(task.get("title") or ""),
        status=gate_status,
        dependency_ids=dependency_ids,
        reasons=reasons,
    )


def _dependency_reason(
    *,
    task_id: str,
    dependency_id: str,
    tasks_by_id: dict[str, dict[str, Any]],
) -> DependencyGateReason | None:
    dependency = tasks_by_id.get(dependency_id)
    if dependency is None:
        return DependencyGateReason(
            code="unknown_dependency",
            dependency_id=dependency_id,
            dependency_status=None,
            message=f"Task {task_id} depends on unknown task {dependency_id}.",
        )

    dependency_status = str(dependency.get("status") or "")
    if dependency_status in _READY_DEPENDENCY_STATUSES:
        return None
    if dependency_status in _WAITING_DEPENDENCY_STATUSES:
        return DependencyGateReason(
            code="dependency_incomplete",
            dependency_id=dependency_id,
            dependency_status=dependency_status,
            message=(
                f"Task {task_id} is waiting for dependency {dependency_id} "
                f"to complete; current status is {dependency_status}."
            ),
        )
    if dependency_status == "blocked":
        return DependencyGateReason(
            code="dependency_blocked",
            dependency_id=dependency_id,
            dependency_status=dependency_status,
            message=f"Task {task_id} is blocked by blocked dependency {dependency_id}.",
        )
    if dependency_status == "skipped":
        return DependencyGateReason(
            code="dependency_skipped",
            dependency_id=dependency_id,
            dependency_status=dependency_status,
            message=f"Task {task_id} is blocked by skipped dependency {dependency_id}.",
        )

    return DependencyGateReason(
        code="dependency_incomplete",
        dependency_id=dependency_id,
        dependency_status=dependency_status,
        message=(
            f"Task {task_id} is waiting for dependency {dependency_id} "
            f"to complete; current status is {dependency_status or 'unknown'}."
        ),
    )


def _ready_message(task_id: str, dependency_ids: list[str]) -> str:
    if not dependency_ids:
        return f"Task {task_id} has no dependencies."
    return (
        f"Task {task_id} is ready; all dependencies are completed: "
        + ", ".join(dependency_ids)
        + "."
    )


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item]
