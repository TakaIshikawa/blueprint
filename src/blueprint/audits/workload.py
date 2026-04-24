"""Workload distribution audit for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


DEFAULT_OVERLOAD_THRESHOLD = 5
"""Maximum recommended task count for a single owner or engine group."""


@dataclass(frozen=True)
class WorkloadOverload:
    """A workload group whose task count exceeds the documented threshold."""

    dimension: str
    group: str
    task_count: int
    threshold: int
    task_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "dimension": self.dimension,
            "group": self.group,
            "task_count": self.task_count,
            "threshold": self.threshold,
            "task_ids": self.task_ids,
        }


@dataclass(frozen=True)
class CrossMilestoneDependencyCount:
    """Count of dependency edges that cross from one milestone to another."""

    from_milestone: str
    to_milestone: str
    count: int
    dependency_pairs: list[dict[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "from_milestone": self.from_milestone,
            "to_milestone": self.to_milestone,
            "count": self.count,
            "dependency_pairs": self.dependency_pairs,
        }


@dataclass(frozen=True)
class WorkloadResult:
    """Workload distribution summary for an execution plan."""

    plan_id: str
    task_count: int
    overload_threshold: int
    counts_by_owner_type: dict[str, int] = field(default_factory=dict)
    counts_by_suggested_engine: dict[str, int] = field(default_factory=dict)
    counts_by_milestone: dict[str, int] = field(default_factory=dict)
    counts_by_status: dict[str, int] = field(default_factory=dict)
    complexity_buckets: dict[str, int] = field(default_factory=dict)
    overloaded_groups: list[WorkloadOverload] = field(default_factory=list)
    unassigned_task_ids: list[str] = field(default_factory=list)
    cross_milestone_dependencies: list[CrossMilestoneDependencyCount] = field(
        default_factory=list
    )

    @property
    def has_flags(self) -> bool:
        return bool(self.overloaded_groups or self.unassigned_task_ids)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "overload_threshold": self.overload_threshold,
            "counts_by_owner_type": self.counts_by_owner_type,
            "counts_by_suggested_engine": self.counts_by_suggested_engine,
            "counts_by_milestone": self.counts_by_milestone,
            "counts_by_status": self.counts_by_status,
            "complexity_buckets": self.complexity_buckets,
            "overloaded_groups": [
                overloaded_group.to_dict()
                for overloaded_group in self.overloaded_groups
            ],
            "unassigned_task_ids": self.unassigned_task_ids,
            "cross_milestone_dependencies": [
                dependency_count.to_dict()
                for dependency_count in self.cross_milestone_dependencies
            ],
        }


def analyze_workload(
    plan: dict[str, Any],
    *,
    overload_threshold: int = DEFAULT_OVERLOAD_THRESHOLD,
) -> WorkloadResult:
    """Summarize workload distribution and flag impractical assignments.

    The default overload threshold is five tasks. A single `owner_type` or
    `suggested_engine` group above that count is flagged because it can create
    execution bottlenecks for autonomous agents and human teams.
    """
    tasks = _list_of_dicts(plan.get("tasks"))
    tasks_by_id = {
        str(task["id"]): task
        for task in tasks
        if isinstance(task.get("id"), str) and task["id"].strip()
    }

    counts_by_owner_type = _count_by(tasks, "owner_type")
    counts_by_suggested_engine = _count_by(tasks, "suggested_engine")

    return WorkloadResult(
        plan_id=str(plan.get("id") or ""),
        task_count=len(tasks),
        overload_threshold=overload_threshold,
        counts_by_owner_type=counts_by_owner_type,
        counts_by_suggested_engine=counts_by_suggested_engine,
        counts_by_milestone=_count_by(tasks, "milestone"),
        counts_by_status=_count_by(tasks, "status"),
        complexity_buckets=_count_by(tasks, "estimated_complexity"),
        overloaded_groups=_overloaded_groups(
            tasks,
            {
                "owner_type": counts_by_owner_type,
                "suggested_engine": counts_by_suggested_engine,
            },
            overload_threshold,
        ),
        unassigned_task_ids=_unassigned_task_ids(tasks),
        cross_milestone_dependencies=_cross_milestone_dependencies(tasks, tasks_by_id),
    )


def _overloaded_groups(
    tasks: list[dict[str, Any]],
    counts_by_dimension: dict[str, dict[str, int]],
    overload_threshold: int,
) -> list[WorkloadOverload]:
    overloaded_groups: list[WorkloadOverload] = []
    for dimension, counts in counts_by_dimension.items():
        for group, task_count in counts.items():
            if group == "unspecified" or task_count <= overload_threshold:
                continue
            overloaded_groups.append(
                WorkloadOverload(
                    dimension=dimension,
                    group=group,
                    task_count=task_count,
                    threshold=overload_threshold,
                    task_ids=[
                        _task_id(task)
                        for task in tasks
                        if _metric_key(task.get(dimension)) == group
                    ],
                )
            )
    return sorted(
        overloaded_groups,
        key=lambda group: (group.dimension, group.group),
    )


def _unassigned_task_ids(tasks: list[dict[str, Any]]) -> list[str]:
    return [
        _task_id(task)
        for task in tasks
        if _metric_key(task.get("owner_type")) == "unspecified"
    ]


def _cross_milestone_dependencies(
    tasks: list[dict[str, Any]],
    tasks_by_id: dict[str, dict[str, Any]],
) -> list[CrossMilestoneDependencyCount]:
    pairs_by_key: dict[tuple[str, str], list[dict[str, str]]] = {}
    for task in tasks:
        to_milestone = _metric_key(task.get("milestone"))
        task_id = _task_id(task)
        for dependency_id in _string_list(task.get("depends_on")):
            dependency = tasks_by_id.get(dependency_id)
            if dependency is None:
                continue
            from_milestone = _metric_key(dependency.get("milestone"))
            if from_milestone == to_milestone:
                continue
            pairs_by_key.setdefault((from_milestone, to_milestone), []).append(
                {
                    "dependency_task_id": dependency_id,
                    "dependent_task_id": task_id,
                }
            )

    return [
        CrossMilestoneDependencyCount(
            from_milestone=from_milestone,
            to_milestone=to_milestone,
            count=len(dependency_pairs),
            dependency_pairs=dependency_pairs,
        )
        for (from_milestone, to_milestone), dependency_pairs in sorted(
            pairs_by_key.items()
        )
    ]


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


def _task_id(task: dict[str, Any]) -> str:
    return str(task.get("id") or "")


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str) and item.strip()]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
