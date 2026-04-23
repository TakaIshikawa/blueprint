"""Execution plan size and readiness metrics."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


READY_DEPENDENCY_STATUSES = {"completed", "skipped"}


@dataclass(frozen=True)
class PlanMetrics:
    """Summary metrics for an execution plan."""

    plan_id: str
    task_count: int
    milestone_count: int
    counts_by_status: dict[str, int] = field(default_factory=dict)
    counts_by_suggested_engine: dict[str, int] = field(default_factory=dict)
    counts_by_estimated_complexity: dict[str, int] = field(default_factory=dict)
    ready_task_count: int = 0
    blocked_task_count: int = 0
    completed_percent: float = 0.0
    dependency_edge_count: int = 0
    average_dependencies_per_task: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable metrics payload."""
        return {
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "milestone_count": self.milestone_count,
            "counts_by_status": self.counts_by_status,
            "counts_by_suggested_engine": self.counts_by_suggested_engine,
            "counts_by_estimated_complexity": self.counts_by_estimated_complexity,
            "ready_task_count": self.ready_task_count,
            "blocked_task_count": self.blocked_task_count,
            "completed_percent": self.completed_percent,
            "dependency_edge_count": self.dependency_edge_count,
            "average_dependencies_per_task": self.average_dependencies_per_task,
        }


def calculate_plan_metrics(plan: dict[str, Any]) -> PlanMetrics:
    """Calculate execution plan size and readiness metrics."""
    tasks = _list_of_dicts(plan.get("tasks"))
    task_count = len(tasks)
    tasks_by_id = {
        str(current_task["id"]): current_task
        for current_task in tasks
        if current_task.get("id")
    }
    dependency_edge_count = sum(len(_string_list(task.get("depends_on"))) for task in tasks)

    return PlanMetrics(
        plan_id=str(plan.get("id") or ""),
        task_count=task_count,
        milestone_count=len(_list_of_dicts(plan.get("milestones"))),
        counts_by_status=_count_by(tasks, "status"),
        counts_by_suggested_engine=_count_by(tasks, "suggested_engine"),
        counts_by_estimated_complexity=_count_by(tasks, "estimated_complexity"),
        ready_task_count=sum(1 for task in tasks if _is_ready_task(task, tasks_by_id)),
        blocked_task_count=sum(1 for task in tasks if task.get("status") == "blocked"),
        completed_percent=_percent(
            sum(1 for task in tasks if task.get("status") == "completed"),
            task_count,
        ),
        dependency_edge_count=dependency_edge_count,
        average_dependencies_per_task=_average(dependency_edge_count, task_count),
    )


def _is_ready_task(
    task: dict[str, Any],
    tasks_by_id: dict[str, dict[str, Any]],
) -> bool:
    """Return whether a task is pending and all dependencies are satisfied."""
    if task.get("status") != "pending":
        return False

    for dependency_id in _string_list(task.get("depends_on")):
        dependency = tasks_by_id.get(dependency_id)
        if not dependency or dependency.get("status") not in READY_DEPENDENCY_STATUSES:
            return False
    return True


def _count_by(tasks: list[dict[str, Any]], field_name: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for task in tasks:
        key = _metric_key(task.get(field_name))
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _metric_key(value: Any) -> str:
    if isinstance(value, str) and value.strip():
        return value
    return "unspecified"


def _percent(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def _average(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 2)


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, str) and item.strip()]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
