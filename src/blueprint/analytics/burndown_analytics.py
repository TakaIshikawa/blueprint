"""Generate burndown and burnup charts for plan progress visualization.

Tracks remaining and completed work over time to visualize plan progress,
detect scope changes, and predict completion dates. Supports multiple work
metrics and provides Monte Carlo simulations for completion probability.

Methodology
-----------
* **Burndown chart** - shows remaining work decreasing over time
* **Burnup chart** - shows completed work and total scope over time
* **Ideal line** - linear progression from total work to zero
* **Scope change detection** - tracks tasks added/removed mid-sprint
* **Completion forecasting** - Monte Carlo simulation for date predictions
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import numpy as np


class WorkMetric(str, Enum):
    """Work measurement units for burndown/burnup charts."""

    TASK_COUNT = "task_count"
    STORY_POINTS = "story_points"
    HOURS = "hours"


@dataclass(frozen=True, slots=True)
class Point:
    """A point in time with a work value."""

    date: datetime
    value: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date.isoformat(),
            "value": self.value,
        }


@dataclass(frozen=True, slots=True)
class ScopeChange:
    """Record of a scope change event."""

    date: datetime
    change_type: str
    task_id: str
    impact: float
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date.isoformat(),
            "change_type": self.change_type,
            "task_id": self.task_id,
            "impact": self.impact,
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class BurndownData:
    """Burndown chart data showing remaining work over time."""

    plan_id: str
    start_date: datetime
    end_date: datetime
    metric: WorkMetric
    ideal_line: tuple[Point, ...]
    actual_line: tuple[Point, ...]
    total_work: float
    remaining_work: float
    completion_percentage: float
    scope_changes: tuple[ScopeChange, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "metric": self.metric.value,
            "ideal_line": [p.to_dict() for p in self.ideal_line],
            "actual_line": [p.to_dict() for p in self.actual_line],
            "total_work": self.total_work,
            "remaining_work": self.remaining_work,
            "completion_percentage": self.completion_percentage,
            "scope_changes": [sc.to_dict() for sc in self.scope_changes],
        }


@dataclass(frozen=True, slots=True)
class BurnupData:
    """Burnup chart data showing completed work and total scope."""

    plan_id: str
    start_date: datetime
    end_date: datetime
    metric: WorkMetric
    completed_line: tuple[Point, ...]
    total_scope_line: tuple[Point, ...]
    ideal_line: tuple[Point, ...]
    total_work: float
    completed_work: float
    completion_percentage: float
    scope_changes: tuple[ScopeChange, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "metric": self.metric.value,
            "completed_line": [p.to_dict() for p in self.completed_line],
            "total_scope_line": [p.to_dict() for p in self.total_scope_line],
            "ideal_line": [p.to_dict() for p in self.ideal_line],
            "total_work": self.total_work,
            "completed_work": self.completed_work,
            "completion_percentage": self.completion_percentage,
            "scope_changes": [sc.to_dict() for sc in self.scope_changes],
        }


@dataclass(frozen=True, slots=True)
class DateForecast:
    """Predicted completion date with confidence intervals."""

    predicted_date: datetime
    confidence_50_pct: datetime
    confidence_85_pct: datetime
    confidence_95_pct: datetime
    probability_on_time: float
    based_on_samples: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "predicted_date": self.predicted_date.isoformat(),
            "confidence_50_pct": self.confidence_50_pct.isoformat(),
            "confidence_85_pct": self.confidence_85_pct.isoformat(),
            "confidence_95_pct": self.confidence_95_pct.isoformat(),
            "probability_on_time": self.probability_on_time,
            "based_on_samples": self.based_on_samples,
        }


@dataclass(frozen=True, slots=True)
class DailySnapshot:
    """Daily snapshot of plan state."""

    date: datetime
    remaining_tasks: int
    remaining_points: float
    remaining_hours: float
    completed_tasks: int
    completed_points: float
    completed_hours: float
    total_tasks: int
    total_points: float
    total_hours: float


class BurndownAnalytics:
    """Generate burndown and burnup charts for plan progress."""

    def __init__(self) -> None:
        self._snapshots: dict[str, list[DailySnapshot]] = {}
        self._scope_changes: dict[str, list[ScopeChange]] = {}

    def record_snapshot(
        self,
        plan_id: str,
        date: datetime,
        remaining_tasks: int = 0,
        remaining_points: float = 0.0,
        remaining_hours: float = 0.0,
        completed_tasks: int = 0,
        completed_points: float = 0.0,
        completed_hours: float = 0.0,
        total_tasks: int = 0,
        total_points: float = 0.0,
        total_hours: float = 0.0,
    ) -> None:
        """Record a daily snapshot of plan state.

        Args:
            plan_id: Plan identifier
            date: Snapshot date
            remaining_tasks: Tasks remaining
            remaining_points: Story points remaining
            remaining_hours: Hours remaining
            completed_tasks: Tasks completed
            completed_points: Story points completed
            completed_hours: Hours completed
            total_tasks: Total task count
            total_points: Total story points
            total_hours: Total hours
        """
        snapshot = DailySnapshot(
            date=date,
            remaining_tasks=remaining_tasks,
            remaining_points=remaining_points,
            remaining_hours=remaining_hours,
            completed_tasks=completed_tasks,
            completed_points=completed_points,
            completed_hours=completed_hours,
            total_tasks=total_tasks,
            total_points=total_points,
            total_hours=total_hours,
        )

        if plan_id not in self._snapshots:
            self._snapshots[plan_id] = []
        self._snapshots[plan_id].append(snapshot)

    def record_scope_change(
        self,
        plan_id: str,
        date: datetime,
        change_type: str,
        task_id: str,
        impact: float,
        description: str,
    ) -> None:
        """Record a scope change event.

        Args:
            plan_id: Plan identifier
            date: Change date
            change_type: "added" or "removed"
            task_id: Affected task ID
            impact: Impact in work units
            description: Human-readable description
        """
        change = ScopeChange(
            date=date,
            change_type=change_type,
            task_id=task_id,
            impact=impact,
            description=description,
        )

        if plan_id not in self._scope_changes:
            self._scope_changes[plan_id] = []
        self._scope_changes[plan_id].append(change)

    def generate_burndown(
        self,
        plan_id: str,
        start_date: datetime,
        end_date: datetime,
        metric: WorkMetric = WorkMetric.TASK_COUNT,
    ) -> BurndownData:
        """Generate burndown chart data for a plan.

        Args:
            plan_id: Plan identifier
            start_date: Sprint/plan start date
            end_date: Sprint/plan end date
            metric: Work metric to use

        Returns:
            BurndownData with ideal and actual burndown lines
        """
        snapshots = self._snapshots.get(plan_id, [])
        scope_changes = self._scope_changes.get(plan_id, [])

        if not snapshots:
            return BurndownData(
                plan_id=plan_id,
                start_date=start_date,
                end_date=end_date,
                metric=metric,
                ideal_line=(),
                actual_line=(),
                total_work=0.0,
                remaining_work=0.0,
                completion_percentage=0.0,
            )

        filtered_snapshots = [s for s in snapshots if start_date <= s.date <= end_date]
        filtered_snapshots.sort(key=lambda s: s.date)

        if not filtered_snapshots:
            total_work = 0.0
        else:
            total_work = self._get_total_work(filtered_snapshots[0], metric)

        ideal_line = self.calculate_ideal_line(total_work, (end_date - start_date).days + 1, start_date)
        actual_line = self._calculate_actual_burndown(filtered_snapshots, metric, start_date)

        if filtered_snapshots:
            remaining_work = self._get_remaining_work(filtered_snapshots[-1], metric)
        else:
            remaining_work = 0.0

        completion_pct = ((total_work - remaining_work) / total_work * 100) if total_work > 0 else 0.0

        filtered_changes = tuple(sc for sc in scope_changes if start_date <= sc.date <= end_date)

        return BurndownData(
            plan_id=plan_id,
            start_date=start_date,
            end_date=end_date,
            metric=metric,
            ideal_line=ideal_line,
            actual_line=actual_line,
            total_work=total_work,
            remaining_work=remaining_work,
            completion_percentage=completion_pct,
            scope_changes=filtered_changes,
        )

    def generate_burnup(
        self,
        plan_id: str,
        start_date: datetime,
        end_date: datetime,
        metric: WorkMetric = WorkMetric.TASK_COUNT,
    ) -> BurnupData:
        """Generate burnup chart data for a plan.

        Args:
            plan_id: Plan identifier
            start_date: Sprint/plan start date
            end_date: Sprint/plan end date
            metric: Work metric to use

        Returns:
            BurnupData with completed work and total scope lines
        """
        snapshots = self._snapshots.get(plan_id, [])
        scope_changes = self._scope_changes.get(plan_id, [])

        if not snapshots:
            return BurnupData(
                plan_id=plan_id,
                start_date=start_date,
                end_date=end_date,
                metric=metric,
                completed_line=(),
                total_scope_line=(),
                ideal_line=(),
                total_work=0.0,
                completed_work=0.0,
                completion_percentage=0.0,
            )

        filtered_snapshots = [s for s in snapshots if start_date <= s.date <= end_date]
        filtered_snapshots.sort(key=lambda s: s.date)

        if not filtered_snapshots:
            total_work = 0.0
        else:
            total_work = self._get_total_work(filtered_snapshots[-1], metric)

        ideal_line = self.calculate_ideal_line(total_work, (end_date - start_date).days + 1, start_date)
        completed_line = self._calculate_completed_line(filtered_snapshots, metric, start_date)
        total_scope_line = self._calculate_scope_line(filtered_snapshots, metric, start_date)

        if filtered_snapshots:
            completed_work = self._get_completed_work(filtered_snapshots[-1], metric)
        else:
            completed_work = 0.0

        completion_pct = (completed_work / total_work * 100) if total_work > 0 else 0.0

        filtered_changes = tuple(sc for sc in scope_changes if start_date <= sc.date <= end_date)

        return BurnupData(
            plan_id=plan_id,
            start_date=start_date,
            end_date=end_date,
            metric=metric,
            completed_line=completed_line,
            total_scope_line=total_scope_line,
            ideal_line=ideal_line,
            total_work=total_work,
            completed_work=completed_work,
            completion_percentage=completion_pct,
            scope_changes=filtered_changes,
        )

    def calculate_ideal_line(
        self,
        total_work: float,
        duration: int,
        start_date: datetime,
    ) -> tuple[Point, ...]:
        """Calculate ideal linear burndown line.

        Args:
            total_work: Total work at start
            duration: Duration in days
            start_date: Start date

        Returns:
            Tuple of Points forming ideal line
        """
        if duration <= 0:
            return ()

        points = []
        for day in range(duration):
            date = start_date + timedelta(days=day)
            value = total_work * (1 - day / (duration - 1)) if duration > 1 else 0
            points.append(Point(date=date, value=value))

        return tuple(points)

    def detect_scope_changes(self, plan_id: str) -> list[ScopeChange]:
        """Get all scope changes for a plan.

        Args:
            plan_id: Plan identifier

        Returns:
            List of scope changes
        """
        return self._scope_changes.get(plan_id, [])

    def predict_completion_date(
        self,
        burndown: BurndownData,
        target_date: datetime,
        simulations: int = 1000,
    ) -> DateForecast:
        """Predict completion date using Monte Carlo simulation.

        Args:
            burndown: Burndown data
            target_date: Target completion date
            simulations: Number of simulation runs

        Returns:
            DateForecast with predicted date and confidence intervals
        """
        if not burndown.actual_line or burndown.remaining_work == 0:
            return DateForecast(
                predicted_date=target_date,
                confidence_50_pct=target_date,
                confidence_85_pct=target_date,
                confidence_95_pct=target_date,
                probability_on_time=1.0,
                based_on_samples=0,
            )

        daily_velocity = self._calculate_daily_velocity(burndown.actual_line)

        if not daily_velocity or all(v == 0 for v in daily_velocity):
            return DateForecast(
                predicted_date=target_date,
                confidence_50_pct=target_date,
                confidence_85_pct=target_date,
                confidence_95_pct=target_date,
                probability_on_time=0.0,
                based_on_samples=0,
            )

        mean_velocity = float(np.mean([v for v in daily_velocity if v > 0]))
        std_velocity = float(np.std([v for v in daily_velocity if v > 0]))

        if std_velocity == 0:
            std_velocity = mean_velocity * 0.2

        completion_days = []
        for _ in range(simulations):
            remaining = burndown.remaining_work
            days = 0

            while remaining > 0 and days < 365:
                velocity = max(0.1, np.random.normal(mean_velocity, std_velocity))
                remaining -= velocity
                days += 1

            completion_days.append(days)

        completion_days_arr = np.array(completion_days)
        median_days = int(np.median(completion_days_arr))
        p50_days = int(np.percentile(completion_days_arr, 50))
        p85_days = int(np.percentile(completion_days_arr, 85))
        p95_days = int(np.percentile(completion_days_arr, 95))

        current_date = burndown.actual_line[-1].date if burndown.actual_line else burndown.start_date

        predicted = current_date + timedelta(days=median_days)
        conf_50 = current_date + timedelta(days=p50_days)
        conf_85 = current_date + timedelta(days=p85_days)
        conf_95 = current_date + timedelta(days=p95_days)

        on_time_count = sum(1 for d in completion_days if current_date + timedelta(days=d) <= target_date)
        prob_on_time = on_time_count / simulations

        return DateForecast(
            predicted_date=predicted,
            confidence_50_pct=conf_50,
            confidence_85_pct=conf_85,
            confidence_95_pct=conf_95,
            probability_on_time=prob_on_time,
            based_on_samples=simulations,
        )

    def _get_remaining_work(self, snapshot: DailySnapshot, metric: WorkMetric) -> float:
        """Extract remaining work from snapshot based on metric."""
        if metric == WorkMetric.TASK_COUNT:
            return float(snapshot.remaining_tasks)
        elif metric == WorkMetric.STORY_POINTS:
            return snapshot.remaining_points
        else:
            return snapshot.remaining_hours

    def _get_completed_work(self, snapshot: DailySnapshot, metric: WorkMetric) -> float:
        """Extract completed work from snapshot based on metric."""
        if metric == WorkMetric.TASK_COUNT:
            return float(snapshot.completed_tasks)
        elif metric == WorkMetric.STORY_POINTS:
            return snapshot.completed_points
        else:
            return snapshot.completed_hours

    def _get_total_work(self, snapshot: DailySnapshot, metric: WorkMetric) -> float:
        """Extract total work from snapshot based on metric."""
        if metric == WorkMetric.TASK_COUNT:
            return float(snapshot.total_tasks)
        elif metric == WorkMetric.STORY_POINTS:
            return snapshot.total_points
        else:
            return snapshot.total_hours

    def _calculate_actual_burndown(
        self,
        snapshots: list[DailySnapshot],
        metric: WorkMetric,
        start_date: datetime,
    ) -> tuple[Point, ...]:
        """Calculate actual burndown line from snapshots."""
        points = []
        for snapshot in snapshots:
            remaining = self._get_remaining_work(snapshot, metric)
            points.append(Point(date=snapshot.date, value=remaining))
        return tuple(points)

    def _calculate_completed_line(
        self,
        snapshots: list[DailySnapshot],
        metric: WorkMetric,
        start_date: datetime,
    ) -> tuple[Point, ...]:
        """Calculate completed work line from snapshots."""
        points = []
        for snapshot in snapshots:
            completed = self._get_completed_work(snapshot, metric)
            points.append(Point(date=snapshot.date, value=completed))
        return tuple(points)

    def _calculate_scope_line(
        self,
        snapshots: list[DailySnapshot],
        metric: WorkMetric,
        start_date: datetime,
    ) -> tuple[Point, ...]:
        """Calculate total scope line from snapshots."""
        points = []
        for snapshot in snapshots:
            total = self._get_total_work(snapshot, metric)
            points.append(Point(date=snapshot.date, value=total))
        return tuple(points)

    def _calculate_daily_velocity(self, actual_line: tuple[Point, ...]) -> list[float]:
        """Calculate daily velocity from actual burndown line."""
        if len(actual_line) < 2:
            return []

        velocities = []
        for i in range(1, len(actual_line)):
            prev_value = actual_line[i - 1].value
            curr_value = actual_line[i].value
            velocity = prev_value - curr_value
            if velocity > 0:
                velocities.append(velocity)

        return velocities
