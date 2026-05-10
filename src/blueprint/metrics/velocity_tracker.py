"""Velocity tracker measuring team performance across sprints.

Provides velocity calculation, burndown/burnup chart data,
trend analysis, and capacity planning.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any

from blueprint.metrics.sprint_model import (
    BurndownPoint,
    BurnupPoint,
    CapacityForecast,
    Sprint,
    SprintStatus,
    TrendAnalysis,
    VelocityRecord,
    VelocityTrend,
    _gen_id,
    _now_iso,
)


class VelocityTracker:
    """Tracks team velocity across sprints with analytics."""

    def __init__(self) -> None:
        self._sprints: dict[str, Sprint] = {}

    # ------------------------------------------------------------------
    # Sprint CRUD
    # ------------------------------------------------------------------

    def create_sprint(
        self,
        name: str,
        start_date: str,
        end_date: str,
        *,
        team_id: str = "",
        capacity: float = 0.0,
        committed_points: float = 0.0,
    ) -> Sprint:
        sprint = Sprint(
            sprint_id=_gen_id("spr"),
            name=name,
            start_date=start_date,
            end_date=end_date,
            team_id=team_id,
            capacity=capacity,
            committed_points=committed_points,
        )
        self._sprints[sprint.sprint_id] = sprint
        return sprint

    def get_sprint(self, sprint_id: str) -> Sprint | None:
        return self._sprints.get(sprint_id)

    def complete_sprint(
        self,
        sprint_id: str,
        completed_points: float,
        *,
        added_points: float = 0.0,
        removed_points: float = 0.0,
        retrospective: dict[str, Any] | None = None,
    ) -> Sprint | None:
        sprint = self._sprints.get(sprint_id)
        if sprint is None:
            return None
        updated = replace(
            sprint,
            completed_points=completed_points,
            added_points=added_points,
            removed_points=removed_points,
            status=SprintStatus.COMPLETED,
            retrospective=retrospective or {},
        )
        self._sprints[sprint_id] = updated
        return updated

    def list_sprints(self, *, team_id: str | None = None) -> list[Sprint]:
        sprints = list(self._sprints.values())
        if team_id:
            sprints = [s for s in sprints if s.team_id == team_id]
        return sorted(sprints, key=lambda s: s.start_date)

    # ------------------------------------------------------------------
    # Velocity calculation
    # ------------------------------------------------------------------

    def calculate_velocity(
        self, *, team_id: str | None = None, last_n: int | None = None
    ) -> float:
        completed = [
            s
            for s in self._sprints.values()
            if s.status == SprintStatus.COMPLETED
            and (team_id is None or s.team_id == team_id)
        ]
        completed.sort(key=lambda s: s.start_date)
        if last_n:
            completed = completed[-last_n:]
        if not completed:
            return 0.0
        return sum(s.completed_points for s in completed) / len(completed)

    def velocity_history(self, *, team_id: str | None = None) -> list[VelocityRecord]:
        completed = [
            s
            for s in self._sprints.values()
            if s.status == SprintStatus.COMPLETED
            and (team_id is None or s.team_id == team_id)
        ]
        completed.sort(key=lambda s: s.start_date)
        return [
            VelocityRecord(
                sprint_id=s.sprint_id,
                sprint_name=s.name,
                velocity=s.completed_points,
                committed=s.committed_points,
                completed=s.completed_points,
            )
            for s in completed
        ]

    # ------------------------------------------------------------------
    # Burndown chart data
    # ------------------------------------------------------------------

    def generate_burndown(self, sprint_id: str) -> list[BurndownPoint]:
        sprint = self._sprints.get(sprint_id)
        if sprint is None:
            return []

        start = datetime.fromisoformat(sprint.start_date)
        end = datetime.fromisoformat(sprint.end_date)
        total_days = max((end - start).days, 1)
        total_points = sprint.committed_points

        points: list[BurndownPoint] = []
        remaining = total_points
        daily_burn = total_points / total_days if total_days > 0 else 0

        for day_offset in range(total_days + 1):
            current_date = start + timedelta(days=day_offset)
            ideal = max(total_points - (daily_burn * day_offset), 0)
            if sprint.status == SprintStatus.COMPLETED and day_offset == total_days:
                remaining = total_points - sprint.completed_points
            points.append(
                BurndownPoint(
                    date=current_date.isoformat()[:10],
                    remaining=round(remaining, 2),
                    ideal_remaining=round(ideal, 2),
                )
            )
            if day_offset < total_days:
                remaining = max(remaining - daily_burn, 0)

        return points

    # ------------------------------------------------------------------
    # Burnup chart data
    # ------------------------------------------------------------------

    def generate_burnup(self, sprint_id: str) -> list[BurnupPoint]:
        sprint = self._sprints.get(sprint_id)
        if sprint is None:
            return []

        start = datetime.fromisoformat(sprint.start_date)
        end = datetime.fromisoformat(sprint.end_date)
        total_days = max((end - start).days, 1)
        initial_scope = sprint.committed_points
        final_scope = initial_scope + sprint.added_points - sprint.removed_points

        points: list[BurnupPoint] = []
        daily_complete = sprint.completed_points / total_days if total_days > 0 else 0
        scope_delta = (final_scope - initial_scope) / total_days if total_days > 0 else 0

        for day_offset in range(total_days + 1):
            current_date = start + timedelta(days=day_offset)
            completed = min(daily_complete * day_offset, sprint.completed_points)
            scope = initial_scope + (scope_delta * day_offset)
            points.append(
                BurnupPoint(
                    date=current_date.isoformat()[:10],
                    completed=round(completed, 2),
                    total_scope=round(scope, 2),
                )
            )

        return points

    # ------------------------------------------------------------------
    # Velocity trend analysis
    # ------------------------------------------------------------------

    def analyze_trend(
        self, *, team_id: str | None = None, min_sprints: int = 3
    ) -> TrendAnalysis:
        history = self.velocity_history(team_id=team_id)
        if len(history) < min_sprints:
            return TrendAnalysis(
                trend=VelocityTrend.INSUFFICIENT_DATA,
                average_velocity=0.0,
                recent_velocity=0.0,
                data_points=len(history),
                recommendation="Collect more sprint data for trend analysis.",
            )

        velocities = [h.velocity for h in history]
        avg = sum(velocities) / len(velocities)
        recent_n = min(3, len(velocities))
        recent_avg = sum(velocities[-recent_n:]) / recent_n

        if recent_avg > avg * 1.1:
            trend = VelocityTrend.ACCELERATING
            rec = "Team velocity is increasing. Consider maintaining current practices."
        elif recent_avg < avg * 0.9:
            trend = VelocityTrend.DECELERATING
            rec = "Team velocity is decreasing. Investigate potential blockers."
        else:
            trend = VelocityTrend.STABLE
            rec = "Team velocity is stable. Good pace maintained."

        return TrendAnalysis(
            trend=trend,
            average_velocity=round(avg, 2),
            recent_velocity=round(recent_avg, 2),
            data_points=len(history),
            recommendation=rec,
        )

    # ------------------------------------------------------------------
    # Capacity planning
    # ------------------------------------------------------------------

    def forecast_capacity(
        self,
        remaining_points: float,
        *,
        team_id: str | None = None,
        last_n: int = 3,
    ) -> CapacityForecast:
        velocity = self.calculate_velocity(team_id=team_id, last_n=last_n)
        if velocity <= 0:
            return CapacityForecast(
                estimated_velocity=0.0,
                sprints_remaining=0,
                estimated_points_remaining=remaining_points,
                confidence=0.0,
            )

        sprints_needed = remaining_points / velocity
        history = self.velocity_history(team_id=team_id)
        if len(history) >= 3:
            velocities = [h.velocity for h in history[-last_n:]]
            mean = sum(velocities) / len(velocities)
            variance = sum((v - mean) ** 2 for v in velocities) / len(velocities)
            std = variance**0.5
            confidence = max(0.0, min(1.0, 1.0 - (std / mean if mean else 1.0)))
        else:
            confidence = 0.3

        return CapacityForecast(
            estimated_velocity=round(velocity, 2),
            sprints_remaining=max(1, round(sprints_needed)),
            estimated_points_remaining=remaining_points,
            confidence=round(confidence, 2),
        )

    # ------------------------------------------------------------------
    # Predictability
    # ------------------------------------------------------------------

    def calculate_predictability(self, *, team_id: str | None = None) -> float:
        completed = [
            s
            for s in self._sprints.values()
            if s.status == SprintStatus.COMPLETED
            and (team_id is None or s.team_id == team_id)
        ]
        if not completed:
            return 0.0
        return sum(s.predictability for s in completed) / len(completed)

    # ------------------------------------------------------------------
    # Velocity comparison across teams
    # ------------------------------------------------------------------

    def compare_teams(self, team_ids: list[str]) -> dict[str, float]:
        return {tid: self.calculate_velocity(team_id=tid) for tid in team_ids}


__all__ = ["VelocityTracker"]
