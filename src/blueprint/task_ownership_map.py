"""Build deterministic task ownership handoff maps for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from numbers import Real
from typing import Any, Mapping

from blueprint.domain.models import ExecutionPlan


@dataclass(frozen=True, slots=True)
class TaskOwnershipGroup:
    """Tasks assigned to the same owner type and execution engine."""

    owner_type: str | None
    suggested_engine: str | None
    task_ids: tuple[str, ...]
    total_estimated_hours: float | None = None
    high_risk_task_ids: tuple[str, ...] = field(default_factory=tuple)

    @property
    def task_count(self) -> int:
        """Return the number of tasks in this ownership group."""
        return len(self.task_ids)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "owner_type": self.owner_type,
            "suggested_engine": self.suggested_engine,
            "task_count": self.task_count,
            "task_ids": list(self.task_ids),
            "total_estimated_hours": self.total_estimated_hours,
            "high_risk_task_ids": list(self.high_risk_task_ids),
        }


@dataclass(frozen=True, slots=True)
class CrossOwnerDependencyEdge:
    """A dependency edge that crosses owner or engine boundaries."""

    source_task_id: str
    target_task_id: str
    source_owner_type: str | None
    source_suggested_engine: str | None
    target_owner_type: str | None
    target_suggested_engine: str | None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_task_id": self.source_task_id,
            "target_task_id": self.target_task_id,
            "source_owner_type": self.source_owner_type,
            "source_suggested_engine": self.source_suggested_engine,
            "target_owner_type": self.target_owner_type,
            "target_suggested_engine": self.target_suggested_engine,
        }


@dataclass(frozen=True, slots=True)
class TaskOwnershipMap:
    """Ownership handoff map for an execution plan."""

    plan_id: str | None
    owner_groups: tuple[TaskOwnershipGroup, ...]
    unassigned_task_ids: tuple[str, ...]
    cross_owner_dependency_edges: tuple[CrossOwnerDependencyEdge, ...]

    @property
    def task_count(self) -> int:
        """Return the number of tasks included in the map."""
        return sum(group.task_count for group in self.owner_groups)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "owner_groups": [group.to_dict() for group in self.owner_groups],
            "unassigned_task_ids": list(self.unassigned_task_ids),
            "cross_owner_dependency_edges": [
                edge.to_dict() for edge in self.cross_owner_dependency_edges
            ],
        }


def build_task_ownership_map(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> TaskOwnershipMap:
    """Group execution tasks by owner and engine for handoff routing."""
    plan = _plan_payload(execution_plan)
    tasks = _task_payloads(plan.get("tasks"))
    tasks_by_id = {_task_id(task, index): task for index, task in enumerate(tasks, start=1)}

    grouped_task_ids: dict[tuple[str | None, str | None], list[str]] = {}
    grouped_hours: dict[tuple[str | None, str | None], float] = {}
    grouped_has_hours: set[tuple[str | None, str | None]] = set()
    grouped_high_risk_ids: dict[tuple[str | None, str | None], list[str]] = {}
    unassigned_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        owner_type = _optional_text(task.get("owner_type"))
        suggested_engine = _optional_text(task.get("suggested_engine"))
        key = (owner_type, suggested_engine)

        grouped_task_ids.setdefault(key, []).append(task_id)
        if owner_type is None and suggested_engine is None:
            unassigned_ids.append(task_id)

        estimated_hours = _numeric_hours(task.get("estimated_hours"))
        if estimated_hours is not None:
            grouped_has_hours.add(key)
            grouped_hours[key] = grouped_hours.get(key, 0.0) + estimated_hours

        if _is_high_risk(task.get("risk_level") or task.get("risk")):
            grouped_high_risk_ids.setdefault(key, []).append(task_id)

    groups = tuple(
        TaskOwnershipGroup(
            owner_type=owner_type,
            suggested_engine=suggested_engine,
            task_ids=tuple(task_ids),
            total_estimated_hours=(
                grouped_hours.get((owner_type, suggested_engine), 0.0)
                if (owner_type, suggested_engine) in grouped_has_hours
                else None
            ),
            high_risk_task_ids=tuple(grouped_high_risk_ids.get((owner_type, suggested_engine), [])),
        )
        for (owner_type, suggested_engine), task_ids in grouped_task_ids.items()
    )

    return TaskOwnershipMap(
        plan_id=_optional_text(plan.get("id")),
        owner_groups=groups,
        unassigned_task_ids=tuple(unassigned_ids),
        cross_owner_dependency_edges=tuple(_cross_owner_dependency_edges(tasks, tasks_by_id)),
    )


def task_ownership_map_to_dict(ownership_map: TaskOwnershipMap) -> dict[str, Any]:
    """Serialize a task ownership map to a dictionary."""
    return ownership_map.to_dict()


task_ownership_map_to_dict.__test__ = False


def _cross_owner_dependency_edges(
    tasks: list[dict[str, Any]],
    tasks_by_id: dict[str, dict[str, Any]],
) -> list[CrossOwnerDependencyEdge]:
    edges: list[CrossOwnerDependencyEdge] = []
    for index, task in enumerate(tasks, start=1):
        target_task_id = _task_id(task, index)
        target_owner = _optional_text(task.get("owner_type"))
        target_engine = _optional_text(task.get("suggested_engine"))
        target_key = (target_owner, target_engine)

        for dependency_id in _string_list(task.get("depends_on")):
            dependency = tasks_by_id.get(dependency_id)
            if dependency is None:
                continue
            source_owner = _optional_text(dependency.get("owner_type"))
            source_engine = _optional_text(dependency.get("suggested_engine"))
            source_key = (source_owner, source_engine)
            if source_key == target_key:
                continue
            edges.append(
                CrossOwnerDependencyEdge(
                    source_task_id=dependency_id,
                    target_task_id=target_task_id,
                    source_owner_type=source_owner,
                    source_suggested_engine=source_engine,
                    target_owner_type=target_owner,
                    target_suggested_engine=target_engine,
                )
            )
    return edges


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
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


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = value.strip()
        return text or None
    return None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _numeric_hours(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, Real):
        return None
    return float(value)


def _is_high_risk(value: Any) -> bool:
    return isinstance(value, str) and value.strip().lower() in {"high", "critical"}


__all__ = [
    "CrossOwnerDependencyEdge",
    "TaskOwnershipGroup",
    "TaskOwnershipMap",
    "build_task_ownership_map",
    "task_ownership_map_to_dict",
]
