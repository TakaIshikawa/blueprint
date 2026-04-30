"""Dependency risk scoring helpers for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


HIGH_FAN_OUT_THRESHOLD = 2
HIGH_RISK_LEVEL = "high"


@dataclass(frozen=True)
class TaskDependencyRisk:
    """Dependency risk metrics for a single execution task."""

    task_id: str
    title: str
    risk_level: str | None
    fan_in: int
    fan_out: int
    transitive_blocker_count: int
    downstream_high_risk_task_count: int
    risk_score: float
    is_root_blocker: bool = False
    is_leaf_task: bool = False
    is_high_fan_out: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable task risk payload."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "risk_level": self.risk_level,
            "fan_in": self.fan_in,
            "fan_out": self.fan_out,
            "transitive_blocker_count": self.transitive_blocker_count,
            "downstream_high_risk_task_count": self.downstream_high_risk_task_count,
            "risk_score": self.risk_score,
            "is_root_blocker": self.is_root_blocker,
            "is_leaf_task": self.is_leaf_task,
            "is_high_fan_out": self.is_high_fan_out,
        }


@dataclass(frozen=True)
class PlanDependencyRisk:
    """Dependency risk summary for an execution plan."""

    plan_id: str
    task_count: int
    root_blocker_task_ids: list[str] = field(default_factory=list)
    leaf_task_ids: list[str] = field(default_factory=list)
    high_fan_out_task_ids: list[str] = field(default_factory=list)
    missing_dependencies_by_task_id: dict[str, list[str]] = field(default_factory=dict)
    tasks: list[TaskDependencyRisk] = field(default_factory=list)

    @property
    def has_missing_dependencies(self) -> bool:
        """Return True when one or more tasks reference unknown dependencies."""
        return bool(self.missing_dependencies_by_task_id)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable dependency risk payload."""
        return {
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "summary": {
                "root_blockers": len(self.root_blocker_task_ids),
                "leaf_tasks": len(self.leaf_task_ids),
                "high_fan_out_tasks": len(self.high_fan_out_task_ids),
                "missing_dependency_references": sum(
                    len(dependency_ids)
                    for dependency_ids in self.missing_dependencies_by_task_id.values()
                ),
            },
            "root_blocker_task_ids": self.root_blocker_task_ids,
            "leaf_task_ids": self.leaf_task_ids,
            "high_fan_out_task_ids": self.high_fan_out_task_ids,
            "missing_dependencies_by_task_id": self.missing_dependencies_by_task_id,
            "tasks": [task.to_dict() for task in self.tasks],
        }


def score_plan_dependency_risk(
    execution_plan: dict[str, Any],
    *,
    high_fan_out_threshold: int = HIGH_FAN_OUT_THRESHOLD,
) -> PlanDependencyRisk:
    """Score dependency-structure risk for an ExecutionPlan-shaped dictionary.

    Unknown dependency IDs are reported and ignored for graph scoring so callers
    can still audit partially valid plans without defensive exception handling.
    """
    plan = _validated_plan_payload(execution_plan)
    tasks = _list_of_dicts(plan.get("tasks"))
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
    downstream_by_task_id = {
        task_id: _downstream_task_ids(task_id, dependents_by_task_id)
        for task_id in tasks_by_id
    }
    raw_scores = {
        task_id: _raw_risk_score(
            fan_out=len(dependents_by_task_id.get(task_id, [])),
            transitive_blocker_count=len(downstream_by_task_id[task_id]),
            downstream_high_risk_task_count=_downstream_high_risk_task_count(
                downstream_by_task_id[task_id],
                tasks_by_id,
            ),
        )
        for task_id in tasks_by_id
    }
    task_risks: list[TaskDependencyRisk] = []
    for task_id, task in tasks_by_id.items():
        dependencies = dependency_ids_by_task_id.get(task_id, [])
        dependents = dependents_by_task_id.get(task_id, [])
        downstream = downstream_by_task_id[task_id]
        downstream_high_risk_task_count = _downstream_high_risk_task_count(
            downstream,
            tasks_by_id,
        )
        fan_out = len(dependents)
        is_leaf_task = fan_out == 0
        is_root_blocker = (
            not dependencies
            and not missing_dependencies_by_task_id.get(task_id)
            and not is_leaf_task
        )
        is_high_fan_out = fan_out >= max(1, high_fan_out_threshold)

        task_risks.append(
            TaskDependencyRisk(
                task_id=task_id,
                title=str(task.get("title") or ""),
                risk_level=_risk_level(task),
                fan_in=len(dependencies),
                fan_out=fan_out,
                transitive_blocker_count=len(downstream),
                downstream_high_risk_task_count=downstream_high_risk_task_count,
                risk_score=_normalized_score(raw_scores[task_id], len(tasks_by_id)),
                is_root_blocker=is_root_blocker,
                is_leaf_task=is_leaf_task,
                is_high_fan_out=is_high_fan_out,
            )
        )

    return PlanDependencyRisk(
        plan_id=str(plan.get("id") or ""),
        task_count=len(tasks_by_id),
        root_blocker_task_ids=[
            task.task_id for task in task_risks if task.is_root_blocker
        ],
        leaf_task_ids=[task.task_id for task in task_risks if task.is_leaf_task],
        high_fan_out_task_ids=[
            task.task_id for task in task_risks if task.is_high_fan_out
        ],
        missing_dependencies_by_task_id=missing_dependencies_by_task_id,
        tasks=task_risks,
    )


def _validated_plan_payload(execution_plan: dict[str, Any]) -> dict[str, Any]:
    try:
        return ExecutionPlan.model_validate(execution_plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(execution_plan)


def _dependents_by_task_id(
    dependency_ids_by_task_id: dict[str, list[str]],
) -> dict[str, list[str]]:
    dependents: dict[str, list[str]] = {}
    for task_id, dependency_ids in dependency_ids_by_task_id.items():
        for dependency_id in dependency_ids:
            dependents.setdefault(dependency_id, []).append(task_id)
    return {task_id: sorted(dependents[task_id]) for task_id in sorted(dependents)}


def _downstream_task_ids(
    task_id: str,
    dependents_by_task_id: dict[str, list[str]],
) -> list[str]:
    downstream: set[str] = set()
    visiting: set[str] = set()

    def visit(current_task_id: str) -> None:
        if current_task_id in visiting:
            return
        visiting.add(current_task_id)
        for dependent_id in dependents_by_task_id.get(current_task_id, []):
            if dependent_id == task_id:
                continue
            downstream.add(dependent_id)
            visit(dependent_id)
        visiting.remove(current_task_id)

    visit(task_id)
    return sorted(downstream)


def _downstream_high_risk_task_count(
    downstream_task_ids: list[str],
    tasks_by_id: dict[str, dict[str, Any]],
) -> int:
    return sum(
        1
        for downstream_task_id in downstream_task_ids
        if _risk_level(tasks_by_id[downstream_task_id]) == HIGH_RISK_LEVEL
    )


def _raw_risk_score(
    *,
    fan_out: int,
    transitive_blocker_count: int,
    downstream_high_risk_task_count: int,
) -> int:
    return fan_out + transitive_blocker_count + (2 * downstream_high_risk_task_count)


def _normalized_score(raw_score: int, task_count: int) -> float:
    max_possible_score = 4 * max(task_count - 1, 0)
    if max_possible_score <= 0:
        return 0.0
    return round((raw_score / max_possible_score) * 100, 2)


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


def _risk_level(task: dict[str, Any]) -> str | None:
    risk_level = task.get("risk_level")
    if not isinstance(risk_level, str) or not risk_level.strip():
        return None
    return risk_level.strip().lower()


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


__all__ = [
    "HIGH_FAN_OUT_THRESHOLD",
    "PlanDependencyRisk",
    "TaskDependencyRisk",
    "score_plan_dependency_risk",
]
