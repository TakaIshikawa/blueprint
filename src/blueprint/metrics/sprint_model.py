"""Sprint and velocity data models."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class SprintStatus(str, Enum):
    PLANNED = "planned"
    ACTIVE = "active"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class VelocityTrend(str, Enum):
    ACCELERATING = "accelerating"
    STABLE = "stable"
    DECELERATING = "decelerating"
    INSUFFICIENT_DATA = "insufficient_data"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gen_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True, slots=True)
class Sprint:
    sprint_id: str
    name: str
    start_date: str
    end_date: str
    team_id: str = ""
    capacity: float = 0.0
    committed_points: float = 0.0
    completed_points: float = 0.0
    added_points: float = 0.0
    removed_points: float = 0.0
    status: SprintStatus = SprintStatus.PLANNED
    retrospective: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def predictability(self) -> float:
        if self.committed_points == 0:
            return 0.0
        return min(self.completed_points / self.committed_points, 1.0)


@dataclass(frozen=True, slots=True)
class BurndownPoint:
    date: str
    remaining: float
    ideal_remaining: float


@dataclass(frozen=True, slots=True)
class BurnupPoint:
    date: str
    completed: float
    total_scope: float


@dataclass(frozen=True, slots=True)
class VelocityRecord:
    sprint_id: str
    sprint_name: str
    velocity: float
    committed: float
    completed: float


@dataclass(frozen=True, slots=True)
class TrendAnalysis:
    trend: VelocityTrend
    average_velocity: float
    recent_velocity: float
    data_points: int
    recommendation: str = ""


@dataclass(frozen=True, slots=True)
class CapacityForecast:
    estimated_velocity: float
    sprints_remaining: int
    estimated_points_remaining: float
    confidence: float = 0.0


__all__ = [
    "BurndownPoint",
    "BurnupPoint",
    "CapacityForecast",
    "Sprint",
    "SprintStatus",
    "TrendAnalysis",
    "VelocityRecord",
    "VelocityTrend",
    "_gen_id",
    "_now_iso",
]
