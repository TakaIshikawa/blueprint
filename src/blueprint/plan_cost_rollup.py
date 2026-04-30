"""Aggregate execution-plan effort and optional cost estimates."""

from __future__ import annotations

from dataclasses import dataclass
from numbers import Real
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


@dataclass(frozen=True, slots=True)
class PlanCostBucket:
    """Effort and optional cost totals for one rollup group."""

    key: str
    task_count: int
    estimated_hours: float
    missing_estimate_count: int
    estimated_cost: float | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        payload: dict[str, Any] = {
            "key": self.key,
            "task_count": self.task_count,
            "estimated_hours": self.estimated_hours,
            "missing_estimate_count": self.missing_estimate_count,
        }
        if self.estimated_cost is not None:
            payload["estimated_cost"] = self.estimated_cost
        return payload


@dataclass(frozen=True, slots=True)
class PlanCostRollup:
    """Plan-level effort and optional cost rollups."""

    plan_id: str | None
    total: PlanCostBucket
    by_milestone: tuple[PlanCostBucket, ...]
    by_owner_type: tuple[PlanCostBucket, ...]
    by_suggested_engine: tuple[PlanCostBucket, ...]
    by_risk_level: tuple[PlanCostBucket, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "total": self.total.to_dict(),
            "by_milestone": [bucket.to_dict() for bucket in self.by_milestone],
            "by_owner_type": [bucket.to_dict() for bucket in self.by_owner_type],
            "by_suggested_engine": [bucket.to_dict() for bucket in self.by_suggested_engine],
            "by_risk_level": [bucket.to_dict() for bucket in self.by_risk_level],
        }


def build_plan_cost_rollup(
    plan: Mapping[str, Any] | ExecutionPlan,
    hourly_rates: Mapping[str, float | int] | None = None,
) -> PlanCostRollup:
    """Build deterministic effort and optional cost rollups for an execution plan."""
    payload = _plan_payload(plan)
    tasks = _task_payloads(payload.get("tasks"))
    rates = _rate_payload(hourly_rates)

    total_builder = _BucketBuilder("total")
    milestone_builders: dict[str, _BucketBuilder] = {}
    owner_builders: dict[str, _BucketBuilder] = {}
    engine_builders: dict[str, _BucketBuilder] = {}
    risk_builders: dict[str, _BucketBuilder] = {}

    for task in tasks:
        hours = _estimated_hours(task.get("estimated_hours"))
        owner_type = _bucket_key(task.get("owner_type"), default="unassigned")
        suggested_engine = _bucket_key(task.get("suggested_engine"), default="unassigned")
        task_rate = _task_rate(owner_type, suggested_engine, rates)

        builders = (
            total_builder,
            _builder(milestone_builders, _bucket_key(task.get("milestone"), default="Ungrouped")),
            _builder(owner_builders, owner_type),
            _builder(engine_builders, suggested_engine),
            _builder(risk_builders, _bucket_key(task.get("risk_level"), default="unassigned")),
        )
        for builder in builders:
            builder.add(hours, task_rate)

    return PlanCostRollup(
        plan_id=_optional_text(payload.get("id")),
        total=total_builder.to_bucket(),
        by_milestone=tuple(builder.to_bucket() for builder in milestone_builders.values()),
        by_owner_type=tuple(builder.to_bucket() for builder in owner_builders.values()),
        by_suggested_engine=tuple(builder.to_bucket() for builder in engine_builders.values()),
        by_risk_level=tuple(builder.to_bucket() for builder in risk_builders.values()),
    )


def plan_cost_rollup_to_dict(rollup: PlanCostRollup) -> dict[str, Any]:
    """Serialize a plan cost rollup to a dictionary."""
    return rollup.to_dict()


class _BucketBuilder:
    def __init__(self, key: str) -> None:
        self.key = key
        self.task_count = 0
        self.estimated_hours = 0.0
        self.missing_estimate_count = 0
        self.estimated_cost: float | None = None

    def add(self, hours: float | None, rate: float | None) -> None:
        self.task_count += 1
        if hours is None:
            self.missing_estimate_count += 1
            return

        self.estimated_hours += hours
        if rate is None:
            return

        if self.estimated_cost is None:
            self.estimated_cost = 0.0
        self.estimated_cost += hours * rate

    def to_bucket(self) -> PlanCostBucket:
        return PlanCostBucket(
            key=self.key,
            task_count=self.task_count,
            estimated_hours=_stable_number(self.estimated_hours),
            missing_estimate_count=self.missing_estimate_count,
            estimated_cost=(
                _stable_number(self.estimated_cost) if self.estimated_cost is not None else None
            ),
        )


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
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


def _rate_payload(hourly_rates: Mapping[str, float | int] | None) -> dict[str, float]:
    if not hourly_rates:
        return {}

    rates: dict[str, float] = {}
    for key, value in hourly_rates.items():
        rate = _non_negative_number(value)
        if rate is None:
            continue
        text_key = _optional_text(key)
        if text_key:
            rates[text_key] = rate
    return rates


def _task_rate(
    owner_type: str,
    suggested_engine: str,
    hourly_rates: Mapping[str, float],
) -> float | None:
    if owner_type in hourly_rates:
        return hourly_rates[owner_type]
    if suggested_engine in hourly_rates:
        return hourly_rates[suggested_engine]
    return None


def _builder(builders: dict[str, _BucketBuilder], key: str) -> _BucketBuilder:
    if key not in builders:
        builders[key] = _BucketBuilder(key)
    return builders[key]


def _estimated_hours(value: Any) -> float | None:
    return _non_negative_number(value)


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


def _stable_number(value: float) -> float:
    rounded = round(value, 6)
    if rounded == 0:
        return 0.0
    return rounded
