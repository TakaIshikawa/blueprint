"""Dependency depth and parallelism metrics for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DependencyLayer:
    """A deterministic group of tasks at the same dependency depth."""

    depth: int
    task_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable layer payload."""
        return {
            "depth": self.depth,
            "task_ids": self.task_ids,
        }


@dataclass(frozen=True)
class PlanDependencyMetrics:
    """Dependency depth and parallelism metrics for an execution plan."""

    plan_id: str
    task_count: int
    root_task_count: int
    leaf_task_count: int
    max_dependency_depth: int
    tasks_by_depth: dict[int, list[str]] = field(default_factory=dict)
    blocked_dependency_count: int = 0
    missing_dependencies_by_task_id: dict[str, list[str]] = field(default_factory=dict)
    parallelizable_task_groups: list[DependencyLayer] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable metrics payload."""
        return {
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "root_task_count": self.root_task_count,
            "leaf_task_count": self.leaf_task_count,
            "max_dependency_depth": self.max_dependency_depth,
            "tasks_by_depth": self.tasks_by_depth,
            "blocked_dependency_count": self.blocked_dependency_count,
            "missing_dependencies_by_task_id": self.missing_dependencies_by_task_id,
            "parallelizable_task_groups": [
                group.to_dict() for group in self.parallelizable_task_groups
            ],
        }


def calculate_dependency_metrics(execution_plan: dict[str, Any]) -> PlanDependencyMetrics:
    """Calculate dependency depth and parallelism characteristics for a plan.

    Missing dependency IDs are reported but ignored for internal graph depth so
    malformed plans still produce useful metrics without mutating the input.
    """
    tasks = _list_of_dicts(execution_plan.get("tasks"))
    tasks_by_id = {str(task["id"]): task for task in tasks if task.get("id")}
    task_ids = set(tasks_by_id)

    dependency_ids_by_task_id = {
        task_id: [
            dependency_id
            for dependency_id in _dependency_ids(task)
            if dependency_id in task_ids
        ]
        for task_id, task in tasks_by_id.items()
    }
    missing_dependencies_by_task_id = _missing_dependencies(tasks_by_id, task_ids)
    dependents_by_task_id = _dependents_by_task_id(dependency_ids_by_task_id)
    depths_by_task_id = _depths_by_task_id(dependency_ids_by_task_id)
    tasks_by_depth = _tasks_by_depth(tasks_by_id, depths_by_task_id)

    return PlanDependencyMetrics(
        plan_id=str(execution_plan.get("id") or ""),
        task_count=len(tasks_by_id),
        root_task_count=sum(
            1
            for task_id in tasks_by_id
            if not dependency_ids_by_task_id.get(task_id)
            and not missing_dependencies_by_task_id.get(task_id)
        ),
        leaf_task_count=sum(
            1 for task_id in tasks_by_id if not dependents_by_task_id.get(task_id)
        ),
        max_dependency_depth=max(depths_by_task_id.values(), default=0),
        tasks_by_depth=tasks_by_depth,
        blocked_dependency_count=sum(
            len(dependency_ids)
            for dependency_ids in missing_dependencies_by_task_id.values()
        ),
        missing_dependencies_by_task_id=missing_dependencies_by_task_id,
        parallelizable_task_groups=[
            DependencyLayer(depth=depth, task_ids=task_ids_at_depth)
            for depth, task_ids_at_depth in tasks_by_depth.items()
        ],
    )


def _depths_by_task_id(
    dependency_ids_by_task_id: dict[str, list[str]],
) -> dict[str, int]:
    depths: dict[str, int] = {}
    visiting: set[str] = set()

    def depth_for(task_id: str) -> int:
        if task_id in depths:
            return depths[task_id]
        if task_id in visiting:
            depths[task_id] = 0
            return 0

        visiting.add(task_id)
        dependencies = dependency_ids_by_task_id.get(task_id, [])
        depth = (
            max(depth_for(dependency_id) for dependency_id in dependencies) + 1
            if dependencies
            else 0
        )
        visiting.remove(task_id)
        depths[task_id] = depth
        return depth

    for task_id in dependency_ids_by_task_id:
        depth_for(task_id)
    return depths


def _tasks_by_depth(
    tasks_by_id: dict[str, dict[str, Any]],
    depths_by_task_id: dict[str, int],
) -> dict[int, list[str]]:
    tasks_by_depth: dict[int, list[str]] = {}
    for task_id in tasks_by_id:
        depth = depths_by_task_id.get(task_id, 0)
        tasks_by_depth.setdefault(depth, []).append(task_id)
    return dict(sorted(tasks_by_depth.items()))


def _dependents_by_task_id(
    dependency_ids_by_task_id: dict[str, list[str]],
) -> dict[str, list[str]]:
    dependents: dict[str, list[str]] = {}
    for task_id, dependency_ids in dependency_ids_by_task_id.items():
        for dependency_id in dependency_ids:
            dependents.setdefault(dependency_id, []).append(task_id)
    return dependents


def _missing_dependencies(
    tasks_by_id: dict[str, dict[str, Any]],
    task_ids: set[str],
) -> dict[str, list[str]]:
    missing_dependencies: dict[str, list[str]] = {}
    for task_id, task in tasks_by_id.items():
        missing = [
            dependency_id
            for dependency_id in _dependency_ids(task)
            if dependency_id not in task_ids
        ]
        if missing:
            missing_dependencies[task_id] = sorted(missing)
    return missing_dependencies


def _dependency_ids(task: dict[str, Any]) -> list[str]:
    return _string_list(task.get("depends_on"))


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, str) and item.strip()]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
