"""Time tracking system for tasks with estimation and reporting.

Supports time estimation using multiple techniques (story points, hours, PERT),
actual time logging, variance analysis, and completion forecasting. Integrates
with predictive analytics for timeline predictions.

Methodology
-----------
* **Estimation techniques** - story points, ideal hours, PERT (optimistic/likely/pessimistic)
* **Time logging** - track actual time by user, date, and activity type
* **Variance analysis** - compare estimated vs actual time
* **Burn rate** - calculate work consumption rate
* **Forecasting** - predict completion based on historical patterns
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any


class EstimationTechnique(str, Enum):
    """Supported estimation techniques."""

    STORY_POINTS = "story_points"
    IDEAL_HOURS = "ideal_hours"
    PERT = "pert"


class ActivityType(str, Enum):
    """Types of work activities."""

    DEVELOPMENT = "development"
    REVIEW = "review"
    TESTING = "testing"
    DOCUMENTATION = "documentation"
    DEBUGGING = "debugging"
    MEETING = "meeting"


@dataclass(frozen=True, slots=True)
class Estimate:
    """Time estimate for a task."""

    task_id: str
    technique: EstimationTechnique
    hours: float
    confidence: float
    optimistic_hours: float | None = None
    likely_hours: float | None = None
    pessimistic_hours: float | None = None
    story_points: float | None = None
    created_at: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "technique": self.technique.value,
            "hours": self.hours,
            "confidence": self.confidence,
            "optimistic_hours": self.optimistic_hours,
            "likely_hours": self.likely_hours,
            "pessimistic_hours": self.pessimistic_hours,
            "story_points": self.story_points,
            "created_at": self.created_at.isoformat(),
        }


@dataclass(frozen=True, slots=True)
class TimeEntry:
    """Logged time entry for a task."""

    task_id: str
    user_id: str
    date: datetime
    hours: float
    activity_type: ActivityType
    description: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "user_id": self.user_id,
            "date": self.date.isoformat(),
            "hours": self.hours,
            "activity_type": self.activity_type.value,
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class TimeSummary:
    """Summary of time for a task."""

    task_id: str
    estimated_hours: float
    actual_hours: float
    remaining_hours: float
    completion_percentage: float
    burn_rate: float
    time_entries: tuple[TimeEntry, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "estimated_hours": self.estimated_hours,
            "actual_hours": self.actual_hours,
            "remaining_hours": self.remaining_hours,
            "completion_percentage": self.completion_percentage,
            "burn_rate": self.burn_rate,
            "time_entries": [e.to_dict() for e in self.time_entries],
        }


@dataclass(frozen=True, slots=True)
class VarianceReport:
    """Variance analysis for a task."""

    task_id: str
    estimated_hours: float
    actual_hours: float
    variance_hours: float
    variance_percentage: float
    over_budget: bool
    days_logged: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "estimated_hours": self.estimated_hours,
            "actual_hours": self.actual_hours,
            "variance_hours": self.variance_hours,
            "variance_percentage": self.variance_percentage,
            "over_budget": self.over_budget,
            "days_logged": self.days_logged,
        }


@dataclass(frozen=True, slots=True)
class CompletionForecast:
    """Predicted completion based on burn rate."""

    task_id: str
    estimated_completion_date: datetime | None
    estimated_remaining_hours: float
    confidence: float
    based_on_days: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "estimated_completion_date": self.estimated_completion_date.isoformat() if self.estimated_completion_date else None,
            "estimated_remaining_hours": self.estimated_remaining_hours,
            "confidence": self.confidence,
            "based_on_days": self.based_on_days,
        }


class TimeTrackingManager:
    """Manage time estimates and tracking for tasks."""

    def __init__(self) -> None:
        self._estimates: dict[str, Estimate] = {}
        self._time_entries: dict[str, list[TimeEntry]] = {}

    def set_estimate(
        self,
        task_id: str,
        hours: float,
        confidence: float,
        technique: EstimationTechnique = EstimationTechnique.IDEAL_HOURS,
        optimistic_hours: float | None = None,
        likely_hours: float | None = None,
        pessimistic_hours: float | None = None,
        story_points: float | None = None,
    ) -> Estimate:
        """Set time estimate for a task.

        Args:
            task_id: Task identifier
            hours: Estimated hours (or PERT expected value)
            confidence: Confidence level (0-1)
            technique: Estimation technique used
            optimistic_hours: Best case (for PERT)
            likely_hours: Most likely (for PERT)
            pessimistic_hours: Worst case (for PERT)
            story_points: Story points if using that technique

        Returns:
            Estimate object
        """
        if technique == EstimationTechnique.PERT:
            if optimistic_hours is None or likely_hours is None or pessimistic_hours is None:
                raise ValueError("PERT estimation requires optimistic, likely, and pessimistic hours")
            hours = (optimistic_hours + 4 * likely_hours + pessimistic_hours) / 6

        estimate = Estimate(
            task_id=task_id,
            technique=technique,
            hours=hours,
            confidence=confidence,
            optimistic_hours=optimistic_hours,
            likely_hours=likely_hours,
            pessimistic_hours=pessimistic_hours,
            story_points=story_points,
        )

        self._estimates[task_id] = estimate
        return estimate

    def log_time(
        self,
        task_id: str,
        hours: float,
        date: datetime,
        user_id: str,
        activity_type: ActivityType = ActivityType.DEVELOPMENT,
        description: str = "",
    ) -> TimeEntry:
        """Log time entry for a task.

        Args:
            task_id: Task identifier
            hours: Hours logged
            date: Date of work
            user_id: User who performed work
            activity_type: Type of activity
            description: Optional description

        Returns:
            TimeEntry object
        """
        entry = TimeEntry(
            task_id=task_id,
            user_id=user_id,
            date=date,
            hours=hours,
            activity_type=activity_type,
            description=description,
        )

        if task_id not in self._time_entries:
            self._time_entries[task_id] = []
        self._time_entries[task_id].append(entry)

        return entry

    def get_time_summary(self, task_id: str) -> TimeSummary:
        """Get time summary for a task.

        Args:
            task_id: Task identifier

        Returns:
            TimeSummary with estimated, actual, and remaining hours
        """
        estimate = self._estimates.get(task_id)
        estimated_hours = estimate.hours if estimate else 0.0

        entries = self._time_entries.get(task_id, [])
        actual_hours = sum(e.hours for e in entries)

        remaining_hours = max(0.0, estimated_hours - actual_hours)

        if estimated_hours > 0:
            completion_pct = min(100.0, (actual_hours / estimated_hours) * 100)
        else:
            completion_pct = 0.0

        burn_rate = self._calculate_burn_rate(task_id)

        return TimeSummary(
            task_id=task_id,
            estimated_hours=estimated_hours,
            actual_hours=actual_hours,
            remaining_hours=remaining_hours,
            completion_percentage=completion_pct,
            burn_rate=burn_rate,
            time_entries=tuple(entries),
        )

    def calculate_variance(self, task_id: str) -> VarianceReport:
        """Calculate estimated vs actual variance for a task.

        Args:
            task_id: Task identifier

        Returns:
            VarianceReport with variance metrics
        """
        summary = self.get_time_summary(task_id)

        variance_hours = summary.actual_hours - summary.estimated_hours

        if summary.estimated_hours > 0:
            variance_pct = (variance_hours / summary.estimated_hours) * 100
        else:
            variance_pct = 0.0

        over_budget = variance_hours > 0

        entries = self._time_entries.get(task_id, [])
        unique_dates = len({e.date.date() for e in entries})

        return VarianceReport(
            task_id=task_id,
            estimated_hours=summary.estimated_hours,
            actual_hours=summary.actual_hours,
            variance_hours=variance_hours,
            variance_percentage=variance_pct,
            over_budget=over_budget,
            days_logged=unique_dates,
        )

    def predict_completion(self, plan_id: str, task_ids: list[str]) -> CompletionForecast:
        """Predict completion for a plan based on task burn rates.

        Args:
            plan_id: Plan identifier
            task_ids: List of task IDs in the plan

        Returns:
            CompletionForecast with predicted completion
        """
        total_estimated = 0.0
        total_actual = 0.0
        total_remaining = 0.0
        total_days = 0

        for task_id in task_ids:
            summary = self.get_time_summary(task_id)
            total_estimated += summary.estimated_hours
            total_actual += summary.actual_hours
            total_remaining += summary.remaining_hours

            entries = self._time_entries.get(task_id, [])
            if entries:
                unique_dates = len({e.date.date() for e in entries})
                total_days = max(total_days, unique_dates)

        if total_days == 0 or total_actual == 0:
            return CompletionForecast(
                task_id=plan_id,
                estimated_completion_date=None,
                estimated_remaining_hours=total_remaining,
                confidence=0.0,
                based_on_days=0,
            )

        avg_burn_rate = total_actual / total_days

        if avg_burn_rate > 0:
            days_to_complete = total_remaining / avg_burn_rate
            completion_date = datetime.now() + timedelta(days=days_to_complete)
        else:
            completion_date = None

        confidence = min(1.0, total_days / 10.0)

        return CompletionForecast(
            task_id=plan_id,
            estimated_completion_date=completion_date,
            estimated_remaining_hours=total_remaining,
            confidence=confidence,
            based_on_days=total_days,
        )

    def get_time_report(
        self,
        user_id: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> list[TimeEntry]:
        """Get time entries filtered by criteria.

        Args:
            user_id: Filter by user
            start_date: Filter by start date
            end_date: Filter by end date

        Returns:
            List of matching time entries
        """
        all_entries = []
        for entries in self._time_entries.values():
            all_entries.extend(entries)

        filtered = all_entries

        if user_id:
            filtered = [e for e in filtered if e.user_id == user_id]

        if start_date:
            filtered = [e for e in filtered if e.date >= start_date]

        if end_date:
            filtered = [e for e in filtered if e.date <= end_date]

        return sorted(filtered, key=lambda e: e.date)

    def get_user_summary(
        self,
        user_id: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, Any]:
        """Get time summary for a user.

        Args:
            user_id: User identifier
            start_date: Optional start date
            end_date: Optional end date

        Returns:
            Dictionary with user time statistics
        """
        entries = self.get_time_report(user_id, start_date, end_date)

        total_hours = sum(e.hours for e in entries)
        unique_tasks = len({e.task_id for e in entries})

        by_activity: dict[ActivityType, float] = {}
        for entry in entries:
            by_activity[entry.activity_type] = by_activity.get(entry.activity_type, 0.0) + entry.hours

        return {
            "user_id": user_id,
            "total_hours": total_hours,
            "unique_tasks": unique_tasks,
            "entries_count": len(entries),
            "by_activity": {k.value: v for k, v in by_activity.items()},
        }

    def _calculate_burn_rate(self, task_id: str) -> float:
        """Calculate burn rate (hours per day) for a task."""
        entries = self._time_entries.get(task_id, [])

        if not entries:
            return 0.0

        unique_dates = len({e.date.date() for e in entries})
        total_hours = sum(e.hours for e in entries)

        if unique_dates == 0:
            return 0.0

        return total_hours / unique_dates


from datetime import timedelta
