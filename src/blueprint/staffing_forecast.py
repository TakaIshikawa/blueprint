"""Deterministic staffing forecasts for execution plans."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from math import ceil
from numbers import Real
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


_COMPLETED_STATUSES = {"completed", "skipped"}
_BLOCKED_STATUS = "blocked"
_DEFAULT_MILESTONE = "Ungrouped"
_DEFAULT_BUCKET = "unassigned"


@dataclass(frozen=True, slots=True)
class StaffingForecastBucket:
    """Effort totals for one forecast dimension value."""

    key: str
    task_count: int
    estimated_hours: float
    missing_estimate_count: int
    zero_hour_task_count: int

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "key": self.key,
            "task_count": self.task_count,
            "estimated_hours": self.estimated_hours,
            "missing_estimate_count": self.missing_estimate_count,
            "zero_hour_task_count": self.zero_hour_task_count,
        }


@dataclass(frozen=True, slots=True)
class MilestoneStaffingRecommendation:
    """Recommended minimum parallel slots for one milestone."""

    milestone: str
    task_count: int
    estimated_hours: float
    missing_estimate_count: int
    zero_hour_task_count: int
    dependency_ready_task_count: int
    dependency_blocked_task_count: int
    recommended_min_parallel_slots: int
    dependency_ready_task_ids: tuple[str, ...]
    dependency_blocked_task_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "milestone": self.milestone,
            "task_count": self.task_count,
            "estimated_hours": self.estimated_hours,
            "missing_estimate_count": self.missing_estimate_count,
            "zero_hour_task_count": self.zero_hour_task_count,
            "dependency_ready_task_count": self.dependency_ready_task_count,
            "dependency_blocked_task_count": self.dependency_blocked_task_count,
            "recommended_min_parallel_slots": self.recommended_min_parallel_slots,
            "dependency_ready_task_ids": list(self.dependency_ready_task_ids),
            "dependency_blocked_task_ids": list(self.dependency_blocked_task_ids),
        }


@dataclass(frozen=True, slots=True)
class StaffingForecast:
    """Plan-level staffing forecast."""

    plan_id: str | None
    hours_per_parallel_slot: float
    total: StaffingForecastBucket
    by_milestone: tuple[StaffingForecastBucket, ...]
    by_owner_type: tuple[StaffingForecastBucket, ...]
    by_suggested_engine: tuple[StaffingForecastBucket, ...]
    by_risk_level: tuple[StaffingForecastBucket, ...]
    milestone_recommendations: tuple[MilestoneStaffingRecommendation, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "hours_per_parallel_slot": self.hours_per_parallel_slot,
            "total": self.total.to_dict(),
            "by_milestone": [bucket.to_dict() for bucket in self.by_milestone],
            "by_owner_type": [bucket.to_dict() for bucket in self.by_owner_type],
            "by_suggested_engine": [bucket.to_dict() for bucket in self.by_suggested_engine],
            "by_risk_level": [bucket.to_dict() for bucket in self.by_risk_level],
            "milestone_recommendations": [
                recommendation.to_dict() for recommendation in self.milestone_recommendations
            ],
        }


def build_staffing_forecast(
    plan: Mapping[str, Any] | ExecutionPlan,
    *,
    hours_per_parallel_slot: float | int = 8,
) -> StaffingForecast:
    """Build deterministic effort rollups and staffing recommendations for a plan."""
    payload = _plan_payload(plan)
    tasks = _task_payloads(payload.get("tasks"))
    slot_hours = _positive_number(hours_per_parallel_slot) or 8.0

    total_builder = _BucketBuilder("total")
    milestone_builders: dict[str, _BucketBuilder] = {}
    owner_builders: dict[str, _BucketBuilder] = {}
    engine_builders: dict[str, _BucketBuilder] = {}
    risk_builders: dict[str, _BucketBuilder] = {}

    grouped_tasks: OrderedDict[str, list[dict[str, Any]]] = OrderedDict()
    for task in tasks:
        milestone = _bucket_key(task.get("milestone"), default=_DEFAULT_MILESTONE)
        grouped_tasks.setdefault(milestone, []).append(task)

        hours = _estimated_hours(task.get("estimated_hours"))
        for builder in (
            total_builder,
            _builder(milestone_builders, milestone),
            _builder(owner_builders, _bucket_key(task.get("owner_type"), default=_DEFAULT_BUCKET)),
            _builder(
                engine_builders,
                _bucket_key(task.get("suggested_engine"), default=_DEFAULT_BUCKET),
            ),
            _builder(risk_builders, _bucket_key(task.get("risk_level"), default=_DEFAULT_BUCKET)),
        ):
            builder.add(hours)

    milestone_order = _milestone_order(payload, grouped_tasks)
    milestone_buckets = tuple(
        milestone_builders[milestone].to_bucket()
        for milestone in milestone_order
        if milestone in milestone_builders
    )

    return StaffingForecast(
        plan_id=_optional_text(payload.get("id")),
        hours_per_parallel_slot=_stable_number(slot_hours),
        total=total_builder.to_bucket(),
        by_milestone=milestone_buckets,
        by_owner_type=tuple(builder.to_bucket() for builder in owner_builders.values()),
        by_suggested_engine=tuple(builder.to_bucket() for builder in engine_builders.values()),
        by_risk_level=tuple(builder.to_bucket() for builder in risk_builders.values()),
        milestone_recommendations=tuple(
            _milestone_recommendation(
                milestone=milestone,
                bucket=milestone_builders[milestone],
                tasks=grouped_tasks.get(milestone, []),
                all_tasks=tasks,
                hours_per_parallel_slot=slot_hours,
            )
            for milestone in milestone_order
            if milestone in milestone_builders
        ),
    )


def staffing_forecast_to_dict(forecast: StaffingForecast) -> dict[str, Any]:
    """Serialize a staffing forecast to a dictionary."""
    return forecast.to_dict()


class _BucketBuilder:
    def __init__(self, key: str) -> None:
        self.key = key
        self.task_count = 0
        self.estimated_hours = 0.0
        self.missing_estimate_count = 0
        self.zero_hour_task_count = 0

    def add(self, hours: float | None) -> None:
        self.task_count += 1
        if hours is None:
            self.missing_estimate_count += 1
            return
        if hours == 0:
            self.zero_hour_task_count += 1
        self.estimated_hours += hours

    def to_bucket(self) -> StaffingForecastBucket:
        return StaffingForecastBucket(
            key=self.key,
            task_count=self.task_count,
            estimated_hours=_stable_number(self.estimated_hours),
            missing_estimate_count=self.missing_estimate_count,
            zero_hour_task_count=self.zero_hour_task_count,
        )


def _milestone_recommendation(
    *,
    milestone: str,
    bucket: _BucketBuilder,
    tasks: list[dict[str, Any]],
    all_tasks: list[dict[str, Any]],
    hours_per_parallel_slot: float,
) -> MilestoneStaffingRecommendation:
    completed_ids = {
        task_id
        for task in all_tasks
        if (task_id := _task_id(task)) and _status(task) in _COMPLETED_STATUSES
    }
    known_ids = {_task_id(task) for task in all_tasks if _task_id(task)}

    ready_ids: list[str] = []
    blocked_ids: list[str] = []
    for task in tasks:
        task_id = _task_id(task)
        if not task_id or _status(task) in _COMPLETED_STATUSES:
            continue
        if _is_dependency_ready(task, completed_ids=completed_ids, known_ids=known_ids):
            ready_ids.append(task_id)
        else:
            blocked_ids.append(task_id)

    recommended_slots = _recommended_slots(
        estimated_hours=bucket.estimated_hours,
        ready_task_count=len(ready_ids),
        hours_per_parallel_slot=hours_per_parallel_slot,
    )

    return MilestoneStaffingRecommendation(
        milestone=milestone,
        task_count=bucket.task_count,
        estimated_hours=_stable_number(bucket.estimated_hours),
        missing_estimate_count=bucket.missing_estimate_count,
        zero_hour_task_count=bucket.zero_hour_task_count,
        dependency_ready_task_count=len(ready_ids),
        dependency_blocked_task_count=len(blocked_ids),
        recommended_min_parallel_slots=recommended_slots,
        dependency_ready_task_ids=tuple(ready_ids),
        dependency_blocked_task_ids=tuple(blocked_ids),
    )


def _recommended_slots(
    *,
    estimated_hours: float,
    ready_task_count: int,
    hours_per_parallel_slot: float,
) -> int:
    if ready_task_count <= 0:
        return 0
    effort_slots = max(1, ceil(estimated_hours / hours_per_parallel_slot))
    return min(ready_task_count, effort_slots)


def _is_dependency_ready(
    task: dict[str, Any],
    *,
    completed_ids: set[str],
    known_ids: set[str],
) -> bool:
    if _status(task) == _BLOCKED_STATUS:
        return False
    for dependency_id in _string_list(task.get("depends_on")):
        if dependency_id not in known_ids or dependency_id not in completed_ids:
            return False
    return True


def _milestone_order(
    plan: dict[str, Any],
    grouped_tasks: OrderedDict[str, list[dict[str, Any]]],
) -> list[str]:
    ordered: OrderedDict[str, None] = OrderedDict()
    for index, milestone in enumerate(_list_of_dicts(plan.get("milestones")), 1):
        name = _optional_text(milestone.get("name")) or _optional_text(milestone.get("title"))
        ordered[name or f"Milestone {index}"] = None

    for milestone in grouped_tasks:
        ordered.setdefault(milestone, None)
    return list(ordered)


def _builder(builders: dict[str, _BucketBuilder], key: str) -> _BucketBuilder:
    if key not in builders:
        builders[key] = _BucketBuilder(key)
    return builders[key]


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
    return {}


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


def _estimated_hours(value: Any) -> float | None:
    return _non_negative_number(value)


def _positive_number(value: Any) -> float | None:
    number = _non_negative_number(value)
    if number is None or number <= 0:
        return None
    return number


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


def _bucket_key(value: Any, *, default: str) -> str:
    return _optional_text(value) or default


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _task_id(task: dict[str, Any]) -> str:
    return _optional_text(task.get("id")) or ""


def _status(task: dict[str, Any]) -> str:
    return (_optional_text(task.get("status")) or "").lower()


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [text for item in value if (text := _optional_text(item))]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _stable_number(value: float) -> float:
    rounded = round(value, 6)
    if rounded == 0:
        return 0.0
    return rounded


__all__ = [
    "MilestoneStaffingRecommendation",
    "StaffingForecast",
    "StaffingForecastBucket",
    "build_staffing_forecast",
    "staffing_forecast_to_dict",
]
