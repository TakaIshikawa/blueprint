"""Track team velocity and identify performance trends over time.

Analyzes task completion patterns to measure throughput, detect trends, and
forecast sprint capacity. Supports multiple velocity metrics and aggregations
by team, individual, project, and task type.

Methodology
-----------
* **Velocity metrics** - tasks per week, story points per sprint, hours per day
* **Rolling averages** - 7-day, 30-day, 90-day windows to smooth daily noise
* **Trend detection** - acceleration, deceleration, seasonality, anomalies
* **Forecasting** - uses historical velocity for sprint capacity predictions
* **Normalization** - adjusts for team size and working hours
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import numpy as np
from scipy import stats


class VelocityMetricType(str, Enum):
    """Supported velocity measurement units."""

    TASKS_PER_WEEK = "tasks_per_week"
    STORY_POINTS_PER_SPRINT = "story_points_per_sprint"
    HOURS_PER_DAY = "hours_per_day"


class TrendType(str, Enum):
    """Identified trend patterns in velocity data."""

    ACCELERATION = "acceleration"
    DECELERATION = "deceleration"
    STABLE = "stable"
    SEASONALITY = "seasonality"
    ANOMALY = "anomaly"


class ImpactType(str, Enum):
    """Types of events that can impact velocity."""

    TEAM_CHANGE = "team_change"
    HOLIDAY_PERIOD = "holiday_period"
    SCOPE_CHANGE = "scope_change"
    PROCESS_CHANGE = "process_change"


@dataclass(frozen=True, slots=True)
class CompletionRecord:
    """A task completion event."""

    task_id: str
    completed_at: datetime
    story_points: float = 0.0
    hours: float = 0.0
    team_id: str | None = None
    user_id: str | None = None
    project_id: str | None = None
    task_type: str | None = None


@dataclass(frozen=True, slots=True)
class VelocityMetric:
    """Calculated velocity for a time period."""

    metric_type: VelocityMetricType
    value: float
    period_start: datetime
    period_end: datetime
    task_count: int
    rolling_avg_7d: float = 0.0
    rolling_avg_30d: float = 0.0
    rolling_avg_90d: float = 0.0
    team_id: str | None = None
    user_id: str | None = None
    project_id: str | None = None
    task_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "metric_type": self.metric_type.value,
            "value": self.value,
            "period_start": self.period_start.isoformat(),
            "period_end": self.period_end.isoformat(),
            "task_count": self.task_count,
            "rolling_avg_7d": self.rolling_avg_7d,
            "rolling_avg_30d": self.rolling_avg_30d,
            "rolling_avg_90d": self.rolling_avg_90d,
            "team_id": self.team_id,
            "user_id": self.user_id,
            "project_id": self.project_id,
            "task_type": self.task_type,
        }


@dataclass(frozen=True, slots=True)
class TrendAnalysis:
    """Analysis of velocity trends over time."""

    trend_type: TrendType
    confidence: float
    description: str
    start_date: datetime
    end_date: datetime
    velocity_change_pct: float = 0.0
    statistical_significance: float = 0.0
    detected_impacts: tuple[VelocityImpact, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "trend_type": self.trend_type.value,
            "confidence": self.confidence,
            "description": self.description,
            "start_date": self.start_date.isoformat(),
            "end_date": self.end_date.isoformat(),
            "velocity_change_pct": self.velocity_change_pct,
            "statistical_significance": self.statistical_significance,
            "detected_impacts": [imp.to_dict() for imp in self.detected_impacts],
        }


@dataclass(frozen=True, slots=True)
class VelocityImpact:
    """Event that impacted velocity."""

    impact_type: ImpactType
    impact_date: datetime
    velocity_change_pct: float
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "impact_type": self.impact_type.value,
            "impact_date": self.impact_date.isoformat(),
            "velocity_change_pct": self.velocity_change_pct,
            "description": self.description,
        }


@dataclass(frozen=True, slots=True)
class CapacityForecast:
    """Sprint capacity forecast based on historical velocity."""

    sprint_length_days: int
    predicted_tasks: float
    predicted_story_points: float
    predicted_hours: float
    confidence_interval_lower: float
    confidence_interval_upper: float
    based_on_samples: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "sprint_length_days": self.sprint_length_days,
            "predicted_tasks": self.predicted_tasks,
            "predicted_story_points": self.predicted_story_points,
            "predicted_hours": self.predicted_hours,
            "confidence_interval_lower": self.confidence_interval_lower,
            "confidence_interval_upper": self.confidence_interval_upper,
            "based_on_samples": self.based_on_samples,
        }


@dataclass(frozen=True, slots=True)
class ChartData:
    """Data for velocity chart visualization."""

    chart_type: str
    labels: tuple[str, ...]
    datasets: tuple[dict[str, Any], ...]
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "chart_type": self.chart_type,
            "labels": list(self.labels),
            "datasets": list(self.datasets),
            "metadata": self.metadata,
        }


class VelocityTracker:
    """Track and analyze team velocity over time."""

    def __init__(self) -> None:
        self._completions: list[CompletionRecord] = []

    def record_completion(
        self,
        task_id: str,
        completed_at: datetime,
        story_points: float = 0.0,
        hours: float = 0.0,
        team_id: str | None = None,
        user_id: str | None = None,
        project_id: str | None = None,
        task_type: str | None = None,
    ) -> None:
        """Record a task completion event.

        Args:
            task_id: Unique task identifier
            completed_at: Completion timestamp
            story_points: Story points if applicable
            hours: Time spent in hours
            team_id: Team identifier for aggregation
            user_id: User identifier for individual tracking
            project_id: Project identifier for filtering
            task_type: Task type for categorization
        """
        record = CompletionRecord(
            task_id=task_id,
            completed_at=completed_at,
            story_points=story_points,
            hours=hours,
            team_id=team_id,
            user_id=user_id,
            project_id=project_id,
            task_type=task_type,
        )
        self._completions.append(record)

    def calculate_velocity(
        self,
        time_period: tuple[datetime, datetime],
        metric: VelocityMetricType,
        team_id: str | None = None,
        user_id: str | None = None,
        project_id: str | None = None,
        task_type: str | None = None,
    ) -> VelocityMetric:
        """Calculate velocity for a specific time period and metric.

        Args:
            time_period: (start, end) datetime tuple
            metric: Type of velocity metric to calculate
            team_id: Filter by team
            user_id: Filter by user
            project_id: Filter by project
            task_type: Filter by task type

        Returns:
            VelocityMetric with calculated velocity and rolling averages
        """
        start_date, end_date = time_period
        filtered = self._filter_completions(
            start_date, end_date, team_id, user_id, project_id, task_type
        )

        if not filtered:
            return VelocityMetric(
                metric_type=metric,
                value=0.0,
                period_start=start_date,
                period_end=end_date,
                task_count=0,
                team_id=team_id,
                user_id=user_id,
                project_id=project_id,
                task_type=task_type,
            )

        value = self._calculate_metric_value(filtered, start_date, end_date, metric)
        rolling_7d = self._calculate_rolling_average(end_date, 7, metric, team_id, user_id, project_id, task_type)
        rolling_30d = self._calculate_rolling_average(end_date, 30, metric, team_id, user_id, project_id, task_type)
        rolling_90d = self._calculate_rolling_average(end_date, 90, metric, team_id, user_id, project_id, task_type)

        return VelocityMetric(
            metric_type=metric,
            value=value,
            period_start=start_date,
            period_end=end_date,
            task_count=len(filtered),
            rolling_avg_7d=rolling_7d,
            rolling_avg_30d=rolling_30d,
            rolling_avg_90d=rolling_90d,
            team_id=team_id,
            user_id=user_id,
            project_id=project_id,
            task_type=task_type,
        )

    def detect_trends(
        self,
        history_days: int,
        team_id: str | None = None,
        user_id: str | None = None,
        project_id: str | None = None,
    ) -> TrendAnalysis:
        """Detect velocity trends over the specified history period.

        Args:
            history_days: Number of days to analyze
            team_id: Filter by team
            user_id: Filter by user
            project_id: Filter by project

        Returns:
            TrendAnalysis with detected trend type and confidence
        """
        if not self._completions:
            end_date = datetime.now()
        else:
            end_date = max(c.completed_at for c in self._completions)
        start_date = end_date - timedelta(days=history_days)

        filtered = self._filter_completions(start_date, end_date, team_id, user_id, project_id, None)

        if len(filtered) < 7:
            return TrendAnalysis(
                trend_type=TrendType.STABLE,
                confidence=0.0,
                description="Insufficient data for trend analysis",
                start_date=start_date,
                end_date=end_date,
            )

        daily_counts = self._group_by_day(filtered, start_date, end_date)
        trend_type, confidence, change_pct, p_value = self._analyze_trend(daily_counts)

        description = self._describe_trend(trend_type, change_pct)

        return TrendAnalysis(
            trend_type=trend_type,
            confidence=confidence,
            description=description,
            start_date=start_date,
            end_date=end_date,
            velocity_change_pct=change_pct,
            statistical_significance=p_value,
        )

    def forecast_capacity(
        self,
        sprint_length: int,
        team_id: str | None = None,
        user_id: str | None = None,
        project_id: str | None = None,
    ) -> CapacityForecast:
        """Forecast sprint capacity based on historical velocity.

        Args:
            sprint_length: Sprint duration in days
            team_id: Filter by team
            user_id: Filter by user
            project_id: Filter by project

        Returns:
            CapacityForecast with predictions and confidence intervals
        """
        if not self._completions:
            return CapacityForecast(
                sprint_length_days=sprint_length,
                predicted_tasks=0.0,
                predicted_story_points=0.0,
                predicted_hours=0.0,
                confidence_interval_lower=0.0,
                confidence_interval_upper=0.0,
                based_on_samples=0,
            )

        end_date = max(c.completed_at for c in self._completions)
        start_date = end_date - timedelta(days=90)

        filtered = self._filter_completions(start_date, end_date, team_id, user_id, project_id, None)

        if not filtered:
            return CapacityForecast(
                sprint_length_days=sprint_length,
                predicted_tasks=0.0,
                predicted_story_points=0.0,
                predicted_hours=0.0,
                confidence_interval_lower=0.0,
                confidence_interval_upper=0.0,
                based_on_samples=0,
            )

        task_velocities = self._calculate_sprint_velocities(filtered, sprint_length)

        if not task_velocities:
            return CapacityForecast(
                sprint_length_days=sprint_length,
                predicted_tasks=0.0,
                predicted_story_points=0.0,
                predicted_hours=0.0,
                confidence_interval_lower=0.0,
                confidence_interval_upper=0.0,
                based_on_samples=0,
            )

        mean_tasks = np.mean(task_velocities)
        std_tasks = np.std(task_velocities) if len(task_velocities) > 1 else 0.0

        confidence_lower = max(0, mean_tasks - 1.96 * std_tasks)
        confidence_upper = mean_tasks + 1.96 * std_tasks

        total_points = sum(c.story_points for c in filtered)
        total_hours = sum(c.hours for c in filtered)
        total_days = (end_date - start_date).days or 1

        predicted_points = (total_points / total_days) * sprint_length
        predicted_hours = (total_hours / total_days) * sprint_length

        return CapacityForecast(
            sprint_length_days=sprint_length,
            predicted_tasks=float(mean_tasks),
            predicted_story_points=float(predicted_points),
            predicted_hours=float(predicted_hours),
            confidence_interval_lower=float(confidence_lower),
            confidence_interval_upper=float(confidence_upper),
            based_on_samples=len(task_velocities),
        )

    def generate_velocity_chart(
        self,
        time_range: tuple[datetime, datetime],
        chart_type: str = "line",
        metric: VelocityMetricType = VelocityMetricType.TASKS_PER_WEEK,
        team_id: str | None = None,
    ) -> ChartData:
        """Generate chart data for velocity visualization.

        Args:
            time_range: (start, end) datetime tuple
            chart_type: "line", "bar", or "comparison"
            metric: Velocity metric to visualize
            team_id: Filter by team

        Returns:
            ChartData compatible with Chart.js/D3.js
        """
        start_date, end_date = time_range

        if chart_type == "line":
            return self._generate_line_chart(start_date, end_date, metric, team_id)
        elif chart_type == "bar":
            return self._generate_bar_chart(start_date, end_date, metric, team_id)
        elif chart_type == "comparison":
            return self._generate_comparison_chart(start_date, end_date, metric)
        else:
            raise ValueError(f"Unknown chart type: {chart_type}")

    def normalize_velocity(
        self,
        velocity: float,
        team_size: int,
        working_hours_per_week: float = 40.0,
    ) -> float:
        """Normalize velocity for team size and working hours.

        Args:
            velocity: Raw velocity value
            team_size: Number of team members
            working_hours_per_week: Expected working hours per week

        Returns:
            Normalized velocity adjusted for team capacity
        """
        if team_size <= 0:
            return 0.0

        standard_hours = 40.0
        hours_factor = working_hours_per_week / standard_hours

        return velocity / (team_size * hours_factor)

    def _filter_completions(
        self,
        start: datetime,
        end: datetime,
        team_id: str | None,
        user_id: str | None,
        project_id: str | None,
        task_type: str | None,
    ) -> list[CompletionRecord]:
        """Filter completions by time range and optional criteria."""
        filtered = [
            c for c in self._completions
            if start <= c.completed_at <= end
        ]

        if team_id is not None:
            filtered = [c for c in filtered if c.team_id == team_id]
        if user_id is not None:
            filtered = [c for c in filtered if c.user_id == user_id]
        if project_id is not None:
            filtered = [c for c in filtered if c.project_id == project_id]
        if task_type is not None:
            filtered = [c for c in filtered if c.task_type == task_type]

        return filtered

    def _calculate_metric_value(
        self,
        completions: list[CompletionRecord],
        start: datetime,
        end: datetime,
        metric: VelocityMetricType,
    ) -> float:
        """Calculate velocity metric value from completions."""
        period_days = (end - start).days or 1

        if metric == VelocityMetricType.TASKS_PER_WEEK:
            return len(completions) / (period_days / 7.0)
        elif metric == VelocityMetricType.STORY_POINTS_PER_SPRINT:
            total_points = sum(c.story_points for c in completions)
            return total_points / (period_days / 14.0)
        elif metric == VelocityMetricType.HOURS_PER_DAY:
            total_hours = sum(c.hours for c in completions)
            return total_hours / period_days
        else:
            return 0.0

    def _calculate_rolling_average(
        self,
        end_date: datetime,
        window_days: int,
        metric: VelocityMetricType,
        team_id: str | None,
        user_id: str | None,
        project_id: str | None,
        task_type: str | None,
    ) -> float:
        """Calculate rolling average velocity over a window."""
        start_date = end_date - timedelta(days=window_days)
        filtered = self._filter_completions(start_date, end_date, team_id, user_id, project_id, task_type)

        if not filtered:
            return 0.0

        return self._calculate_metric_value(filtered, start_date, end_date, metric)

    def _group_by_day(
        self,
        completions: list[CompletionRecord],
        start: datetime,
        end: datetime,
    ) -> list[int]:
        """Group completions by day and return daily counts."""
        daily_map: dict[str, int] = {}
        for c in completions:
            day_key = c.completed_at.date().isoformat()
            daily_map[day_key] = daily_map.get(day_key, 0) + 1

        days = (end - start).days + 1
        daily_counts = []
        for i in range(days):
            day = start + timedelta(days=i)
            day_key = day.date().isoformat()
            daily_counts.append(daily_map.get(day_key, 0))

        return daily_counts

    def _analyze_trend(self, daily_counts: list[int]) -> tuple[TrendType, float, float, float]:
        """Analyze trend from daily counts using linear regression."""
        if len(daily_counts) < 7:
            return TrendType.STABLE, 0.0, 0.0, 1.0

        x = np.arange(len(daily_counts))
        y = np.array(daily_counts, dtype=float)

        if y.sum() == 0:
            return TrendType.STABLE, 0.0, 0.0, 1.0

        slope, _intercept, r_value, p_value, _std_err = stats.linregress(x, y)

        mean_y = float(y.mean())
        if mean_y > 0:
            change_pct = float((slope * len(daily_counts)) / mean_y * 100)
        else:
            change_pct = 0.0

        confidence = float(abs(r_value))

        if float(p_value) > 0.05:
            trend_type = TrendType.STABLE
        elif abs(change_pct) < 10:
            trend_type = TrendType.STABLE
        elif float(slope) > 0:
            trend_type = TrendType.ACCELERATION
        else:
            trend_type = TrendType.DECELERATION

        outliers = self._detect_outliers(y)
        if outliers and float(p_value) < 0.05:
            trend_type = TrendType.ANOMALY

        return trend_type, confidence, change_pct, float(p_value)

    def _detect_outliers(self, values: np.ndarray) -> list[int]:
        """Detect outliers using IQR method."""
        if len(values) < 4:
            return []

        q1 = np.percentile(values, 25)
        q3 = np.percentile(values, 75)
        iqr = q3 - q1

        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr

        outliers = [i for i, v in enumerate(values) if v < lower_bound or v > upper_bound]
        return outliers

    def _describe_trend(self, trend_type: TrendType, change_pct: float) -> str:
        """Generate human-readable trend description."""
        if trend_type == TrendType.ACCELERATION:
            return f"Velocity increasing by {abs(change_pct):.1f}% over the period"
        elif trend_type == TrendType.DECELERATION:
            return f"Velocity decreasing by {abs(change_pct):.1f}% over the period"
        elif trend_type == TrendType.ANOMALY:
            return "Unusual velocity pattern detected with significant outliers"
        else:
            return f"Stable velocity with {abs(change_pct):.1f}% variation"

    def _calculate_sprint_velocities(
        self,
        completions: list[CompletionRecord],
        sprint_length: int,
    ) -> list[float]:
        """Calculate velocity for each sprint-sized window in the data."""
        if not completions:
            return []

        start = min(c.completed_at for c in completions)
        end = max(c.completed_at for c in completions)

        velocities = []
        current = start

        while current + timedelta(days=sprint_length) <= end:
            sprint_end = current + timedelta(days=sprint_length)
            sprint_completions = [
                c for c in completions
                if current <= c.completed_at < sprint_end
            ]
            velocities.append(float(len(sprint_completions)))
            current = sprint_end

        return velocities

    def _generate_line_chart(
        self,
        start: datetime,
        end: datetime,
        metric: VelocityMetricType,
        team_id: str | None,
    ) -> ChartData:
        """Generate line chart data showing velocity over time."""
        weeks = []
        velocities = []

        current = start
        while current <= end:
            week_end = min(current + timedelta(days=7), end)
            vel = self.calculate_velocity(
                (current, week_end),
                metric,
                team_id=team_id,
            )
            weeks.append(current.strftime("%Y-%m-%d"))
            velocities.append(vel.value)
            current = week_end

        dataset = {
            "label": f"{metric.value}",
            "data": velocities,
            "borderColor": "rgb(75, 192, 192)",
            "tension": 0.1,
        }

        return ChartData(
            chart_type="line",
            labels=tuple(weeks),
            datasets=(dataset,),
            metadata={"metric": metric.value},
        )

    def _generate_bar_chart(
        self,
        start: datetime,
        end: datetime,
        metric: VelocityMetricType,
        team_id: str | None,
    ) -> ChartData:
        """Generate bar chart data for sprint comparisons."""
        sprints = []
        velocities = []

        current = start
        sprint_num = 1

        while current <= end:
            sprint_end = min(current + timedelta(days=14), end)
            vel = self.calculate_velocity(
                (current, sprint_end),
                metric,
                team_id=team_id,
            )
            sprints.append(f"Sprint {sprint_num}")
            velocities.append(vel.value)
            current = sprint_end
            sprint_num += 1

        dataset = {
            "label": f"{metric.value}",
            "data": velocities,
            "backgroundColor": "rgba(75, 192, 192, 0.2)",
            "borderColor": "rgb(75, 192, 192)",
            "borderWidth": 1,
        }

        return ChartData(
            chart_type="bar",
            labels=tuple(sprints),
            datasets=(dataset,),
            metadata={"metric": metric.value},
        )

    def _generate_comparison_chart(
        self,
        start: datetime,
        end: datetime,
        metric: VelocityMetricType,
    ) -> ChartData:
        """Generate comparison chart across multiple teams."""
        team_ids = sorted({c.team_id for c in self._completions if c.team_id})

        if not team_ids:
            return ChartData(
                chart_type="bar",
                labels=(),
                datasets=(),
                metadata={"metric": metric.value},
            )

        team_velocities = []
        for tid in team_ids:
            vel = self.calculate_velocity(
                (start, end),
                metric,
                team_id=tid,
            )
            team_velocities.append(vel.value)

        dataset = {
            "label": f"{metric.value}",
            "data": team_velocities,
            "backgroundColor": "rgba(153, 102, 255, 0.2)",
            "borderColor": "rgb(153, 102, 255)",
            "borderWidth": 1,
        }

        return ChartData(
            chart_type="bar",
            labels=tuple(team_ids),
            datasets=(dataset,),
            metadata={"metric": metric.value, "comparison": "teams"},
        )
