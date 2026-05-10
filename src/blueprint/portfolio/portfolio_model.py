"""Portfolio data models for aggregating multiple execution plans."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class PortfolioStatus(str, Enum):
    """Lifecycle status of a portfolio."""

    PLANNED = "planned"
    ACTIVE = "active"
    ON_TRACK = "on_track"
    AT_RISK = "at_risk"
    COMPLETED = "completed"
    ARCHIVED = "archived"


class HealthRating(str, Enum):
    """Overall health rating for a portfolio."""

    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"


class RiskLevel(str, Enum):
    """Risk severity level."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gen_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True, slots=True)
class PortfolioGoal:
    """A strategic goal for the portfolio."""

    goal_id: str
    title: str
    description: str = ""
    target_date: str | None = None
    status: str = "open"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class PlanReference:
    """Reference to a plan within a portfolio, with optional parent for hierarchy."""

    plan_id: str
    title: str
    parent_plan_id: str | None = None
    status: str = "draft"
    owner: str = ""
    added_at: str = field(default_factory=_now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RollupMetrics:
    """Aggregated metrics across all plans in a portfolio."""

    total_plans: int = 0
    total_tasks: int = 0
    completed_tasks: int = 0
    in_progress_tasks: int = 0
    completion_pct: float = 0.0
    total_budget: float = 0.0
    spent_budget: float = 0.0
    budget_utilization_pct: float = 0.0
    earliest_start: str | None = None
    latest_end: str | None = None
    on_track_plans: int = 0
    at_risk_plans: int = 0
    blocked_plans: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_plans": self.total_plans,
            "total_tasks": self.total_tasks,
            "completed_tasks": self.completed_tasks,
            "in_progress_tasks": self.in_progress_tasks,
            "completion_pct": round(self.completion_pct, 2),
            "total_budget": self.total_budget,
            "spent_budget": self.spent_budget,
            "budget_utilization_pct": round(self.budget_utilization_pct, 2),
            "earliest_start": self.earliest_start,
            "latest_end": self.latest_end,
            "on_track_plans": self.on_track_plans,
            "at_risk_plans": self.at_risk_plans,
            "blocked_plans": self.blocked_plans,
        }


@dataclass(frozen=True, slots=True)
class HealthDashboard:
    """Portfolio health dashboard combining individual plan scores."""

    portfolio_id: str
    overall_health: HealthRating
    completion_health: HealthRating
    budget_health: HealthRating
    schedule_health: HealthRating
    risk_health: HealthRating
    plan_health_scores: dict[str, HealthRating] = field(default_factory=dict)
    generated_at: str = field(default_factory=_now_iso)
    recommendations: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "portfolio_id": self.portfolio_id,
            "overall_health": self.overall_health.value,
            "completion_health": self.completion_health.value,
            "budget_health": self.budget_health.value,
            "schedule_health": self.schedule_health.value,
            "risk_health": self.risk_health.value,
            "plan_health_scores": {k: v.value for k, v in self.plan_health_scores.items()},
            "generated_at": self.generated_at,
            "recommendations": self.recommendations,
        }


@dataclass(frozen=True, slots=True)
class ResourceAllocation:
    """Resource allocation across portfolio plans."""

    resource_id: str
    resource_name: str
    resource_type: str
    total_capacity: float
    allocated: float
    utilization_pct: float
    plan_allocations: dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource_id": self.resource_id,
            "resource_name": self.resource_name,
            "resource_type": self.resource_type,
            "total_capacity": self.total_capacity,
            "allocated": self.allocated,
            "utilization_pct": round(self.utilization_pct, 2),
            "plan_allocations": self.plan_allocations,
        }


@dataclass(frozen=True, slots=True)
class MilestoneEntry:
    """A milestone on the portfolio timeline."""

    milestone_id: str
    plan_id: str
    plan_title: str
    title: str
    target_date: str
    status: str = "pending"
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CrossPlanDependency:
    """Dependency between tasks in different plans."""

    dependency_id: str
    source_plan_id: str
    source_task_id: str
    target_plan_id: str
    target_task_id: str
    dependency_type: str = "finish_to_start"
    status: str = "active"
    description: str = ""


@dataclass(frozen=True, slots=True)
class PortfolioRisk:
    """Aggregated risk at the portfolio level."""

    risk_id: str
    plan_id: str
    title: str
    level: RiskLevel
    impact: str = ""
    mitigation: str = ""
    status: str = "open"


@dataclass(frozen=True, slots=True)
class PerformanceTarget:
    """Target metric for portfolio comparison."""

    metric_name: str
    target_value: float
    actual_value: float
    unit: str = ""
    on_target: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric_name": self.metric_name,
            "target_value": self.target_value,
            "actual_value": self.actual_value,
            "unit": self.unit,
            "on_target": self.on_target,
        }


@dataclass(frozen=True, slots=True)
class Portfolio:
    """Top-level portfolio aggregating multiple plans."""

    portfolio_id: str
    name: str
    description: str = ""
    owner: str = ""
    status: PortfolioStatus = PortfolioStatus.PLANNED
    member_plans: list[PlanReference] = field(default_factory=list)
    goals: list[PortfolioGoal] = field(default_factory=list)
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "portfolio_id": self.portfolio_id,
            "name": self.name,
            "description": self.description,
            "owner": self.owner,
            "status": self.status.value,
            "member_plans": [
                {
                    "plan_id": p.plan_id,
                    "title": p.title,
                    "parent_plan_id": p.parent_plan_id,
                    "status": p.status,
                    "owner": p.owner,
                }
                for p in self.member_plans
            ],
            "goals": [
                {"goal_id": g.goal_id, "title": g.title, "status": g.status}
                for g in self.goals
            ],
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


__all__ = [
    "CrossPlanDependency",
    "HealthDashboard",
    "HealthRating",
    "MilestoneEntry",
    "PerformanceTarget",
    "PlanReference",
    "Portfolio",
    "PortfolioGoal",
    "PortfolioRisk",
    "PortfolioStatus",
    "ResourceAllocation",
    "RiskLevel",
    "RollupMetrics",
    "_gen_id",
    "_now_iso",
]
