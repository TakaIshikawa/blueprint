"""Capacity-aware execution batch planning."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from numbers import Real
from typing import Any


_COMPLETED_STATUS = "completed"
_DEFAULT_LANE = "unassigned"
_COMPLEXITY_WEIGHTS = {
    "low": 1.0,
    "medium": 2.0,
    "high": 3.0,
}
_DEFAULT_COMPLEXITY_WEIGHT = 2.0


@dataclass(frozen=True)
class CapacityLimitedBatch:
    """A deterministic dispatch batch constrained by per-lane capacity."""

    batch_index: int
    scheduled_task_ids: list[str] = field(default_factory=list)
    deferred_task_ids: list[str] = field(default_factory=list)
    capacity_by_lane: dict[str, float | int | None] = field(default_factory=dict)
    used_capacity_by_lane: dict[str, float | int] = field(default_factory=dict)
    blocked_task_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable batch payload."""
        return {
            "batch_index": self.batch_index,
            "scheduled_task_ids": self.scheduled_task_ids,
            "deferred_task_ids": self.deferred_task_ids,
            "capacity_by_lane": self.capacity_by_lane,
            "used_capacity_by_lane": self.used_capacity_by_lane,
            "blocked_task_ids": self.blocked_task_ids,
        }


def plan_capacity_limited_batches(
    plan: Mapping[str, Any] | Any,
    capacities: Mapping[str, Any] | None = None,
    default_capacity: Any = None,
) -> list[CapacityLimitedBatch]:
    """Plan dependency-valid execution batches with per-lane capacity limits.

    Completed tasks are omitted from scheduled batches, but they still satisfy
    downstream dependencies. Unknown dependencies and dependency cycles are
    surfaced in a final blocked batch instead of raising.
    """
    payload = _plan_payload(plan)
    tasks = _task_payloads(payload.get("tasks"))
    task_ids = _task_ids(tasks)
    task_id_set = set(task_ids)
    task_by_id: dict[str, dict[str, Any]] = {}
    for task in tasks:
        task_id = _task_id(task)
        if task_id in task_id_set and task_id not in task_by_id:
            task_by_id[task_id] = task
    dependency_ids_by_task_id = {
        task_id: _dependency_ids(task_by_id[task_id])
        for task_id in task_ids
    }
    completed_ids = {
        task_id
        for task_id in task_ids
        if _status(task_by_id[task_id]) == _COMPLETED_STATUS
    }
    schedulable_ids = [
        task_id for task_id in task_ids if task_id not in completed_ids
    ]
    satisfied_ids = set(completed_ids)
    scheduled_ids: set[str] = set()
    blocked_ids = {
        task_id
        for task_id in schedulable_ids
        if any(
            dependency_id not in task_id_set
            for dependency_id in dependency_ids_by_task_id.get(task_id, [])
        )
    }
    batches: list[CapacityLimitedBatch] = []
    normalized_capacities = _capacity_map(capacities)
    normalized_default_capacity = _capacity_value(default_capacity)

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

        batch = _build_capacity_batch(
            batch_index=len(batches) + 1,
            ready_ids=ready_ids,
            task_by_id=task_by_id,
            capacities=normalized_capacities,
            default_capacity=normalized_default_capacity,
        )
        if not batch.scheduled_task_ids:
            break

        batches.append(batch)
        scheduled_ids.update(batch.scheduled_task_ids)
        satisfied_ids.update(batch.scheduled_task_ids)

    unscheduled_ids = [
        task_id
        for task_id in schedulable_ids
        if task_id not in scheduled_ids and task_id not in blocked_ids
    ]
    blocked_ids.update(unscheduled_ids)
    if blocked_ids:
        blocked_task_ids = [
            task_id for task_id in schedulable_ids if task_id in blocked_ids
        ]
        batches.append(
            CapacityLimitedBatch(
                batch_index=len(batches) + 1,
                capacity_by_lane=_lane_capacities(
                    blocked_task_ids,
                    task_by_id=task_by_id,
                    capacities=normalized_capacities,
                    default_capacity=normalized_default_capacity,
                ),
                blocked_task_ids=blocked_task_ids,
            )
        )

    return batches


def capacity_limited_batches_to_dicts(
    batches: tuple[CapacityLimitedBatch, ...] | list[CapacityLimitedBatch],
) -> list[dict[str, Any]]:
    """Serialize capacity-limited batches to dictionaries."""
    return [batch.to_dict() for batch in batches]


capacity_limited_batches_to_dicts.__test__ = False


def serialize_capacity_limited_batches(
    batches: tuple[CapacityLimitedBatch, ...] | list[CapacityLimitedBatch],
) -> list[dict[str, Any]]:
    """Serialize capacity-limited batches to dictionaries."""
    return capacity_limited_batches_to_dicts(batches)


serialize_capacity_limited_batches.__test__ = False


def _build_capacity_batch(
    *,
    batch_index: int,
    ready_ids: list[str],
    task_by_id: Mapping[str, Mapping[str, Any]],
    capacities: Mapping[str, float],
    default_capacity: float | None,
) -> CapacityLimitedBatch:
    scheduled_ids: list[str] = []
    deferred_ids: list[str] = []
    used_by_lane: dict[str, float] = {}
    deferred_lanes: set[str] = set()

    for index, task_id in enumerate(ready_ids):
        task = task_by_id[task_id]
        lane = _task_lane(task)
        if lane in deferred_lanes:
            deferred_ids.append(task_id)
            continue

        weight = _task_weight(task)
        capacity = _lane_capacity(lane, capacities=capacities, default_capacity=default_capacity)
        used = used_by_lane.get(lane, 0.0)
        fits = capacity is None or used + weight <= capacity
        oversized = capacity is not None and weight > capacity

        if fits:
            scheduled_ids.append(task_id)
            used_by_lane[lane] = used + weight
            continue

        if oversized and not scheduled_ids and used == 0:
            scheduled_ids.append(task_id)
            used_by_lane[lane] = used + weight
            deferred_ids.extend(ready_ids[index + 1 :])
            break

        deferred_ids.append(task_id)
        deferred_lanes.add(lane)

    considered_ids = scheduled_ids + deferred_ids
    return CapacityLimitedBatch(
        batch_index=batch_index,
        scheduled_task_ids=scheduled_ids,
        deferred_task_ids=deferred_ids,
        capacity_by_lane=_lane_capacities(
            considered_ids,
            task_by_id=task_by_id,
            capacities=capacities,
            default_capacity=default_capacity,
        ),
        used_capacity_by_lane={
            lane: _stable_number(used_by_lane[lane])
            for lane in sorted(used_by_lane)
        },
    )


def _plan_payload(plan: Mapping[str, Any] | Any) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    if isinstance(plan, Mapping):
        return dict(plan)
    return {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_ids(tasks: list[dict[str, Any]]) -> list[str]:
    task_ids: list[str] = []
    seen_ids: set[str] = set()
    for task in tasks:
        task_id = _task_id(task)
        if task_id and task_id not in seen_ids:
            seen_ids.add(task_id)
            task_ids.append(task_id)
    return task_ids


def _task_id(task: Mapping[str, Any]) -> str:
    return _optional_text(task.get("id")) or ""


def _status(task: Mapping[str, Any]) -> str:
    return (_optional_text(task.get("status")) or "").lower()


def _dependency_ids(task: Mapping[str, Any]) -> list[str]:
    return _string_list(task.get("depends_on"))


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item]


def _task_lane(task: Mapping[str, Any]) -> str:
    return (
        _optional_text(task.get("suggested_engine"))
        or _optional_text(task.get("owner_type"))
        or _DEFAULT_LANE
    )


def _task_weight(task: Mapping[str, Any]) -> float:
    estimated_hours = _non_negative_number(task.get("estimated_hours"))
    if estimated_hours is not None:
        return estimated_hours

    complexity = (_optional_text(task.get("estimated_complexity")) or "").lower()
    return _COMPLEXITY_WEIGHTS.get(complexity, _DEFAULT_COMPLEXITY_WEIGHT)


def _capacity_map(capacities: Mapping[str, Any] | None) -> dict[str, float]:
    if capacities is None:
        return {}
    return {
        str(lane): capacity
        for lane, value in capacities.items()
        if lane is not None and (capacity := _capacity_value(value)) is not None
    }


def _capacity_value(value: Any) -> float | None:
    number = _non_negative_number(value)
    if number is None:
        return None
    return number


def _lane_capacity(
    lane: str,
    *,
    capacities: Mapping[str, float],
    default_capacity: float | None,
) -> float | None:
    return capacities.get(lane, default_capacity)


def _lane_capacities(
    task_ids: list[str],
    *,
    task_by_id: Mapping[str, Mapping[str, Any]],
    capacities: Mapping[str, float],
    default_capacity: float | None,
) -> dict[str, float | int | None]:
    lanes = sorted({_task_lane(task_by_id[task_id]) for task_id in task_ids})
    return {
        lane: _stable_optional_number(
            _lane_capacity(lane, capacities=capacities, default_capacity=default_capacity)
        )
        for lane in lanes
    }


def _non_negative_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, Real):
        number = float(value)
    elif isinstance(value, str):
        try:
            number = float(value.strip())
        except ValueError:
            return None
    else:
        return None

    if number < 0:
        return None
    return number


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _stable_optional_number(value: float | None) -> float | int | None:
    if value is None:
        return None
    return _stable_number(value)


def _stable_number(value: float) -> float | int:
    if value.is_integer():
        return int(value)
    return value


__all__ = [
    "CapacityLimitedBatch",
    "capacity_limited_batches_to_dicts",
    "plan_capacity_limited_batches",
    "serialize_capacity_limited_batches",
]
