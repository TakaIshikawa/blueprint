"""Test velocity tracking and trend analysis."""

from datetime import datetime, timedelta

import pytest

from blueprint.analytics.velocity_tracking import (
    CapacityForecast,
    ChartData,
    TrendAnalysis,
    TrendType,
    VelocityMetric,
    VelocityMetricType,
    VelocityTracker,
)


@pytest.fixture
def tracker():
    """Create a fresh velocity tracker."""
    return VelocityTracker()


@pytest.fixture
def base_date():
    """Fixed base date for testing."""
    return datetime(2024, 1, 1, 12, 0, 0)


@pytest.fixture
def populated_tracker(tracker, base_date):
    """Tracker with sample completion data."""
    for i in range(30):
        day = base_date + timedelta(days=i)
        tasks_per_day = 3 if i % 7 < 5 else 1

        for j in range(tasks_per_day):
            tracker.record_completion(
                task_id=f"task-{i}-{j}",
                completed_at=day + timedelta(hours=j),
                story_points=2.0,
                hours=4.0,
                team_id="team-alpha",
                user_id=f"user-{j % 3}",
                project_id="project-x",
                task_type="feature",
            )

    return tracker


def test_record_completion(tracker, base_date):
    """Test recording task completions."""
    tracker.record_completion(
        task_id="task-1",
        completed_at=base_date,
        story_points=5.0,
        hours=8.0,
        team_id="team-a",
        user_id="user-1",
        project_id="project-1",
        task_type="bug",
    )

    assert len(tracker._completions) == 1
    record = tracker._completions[0]
    assert record.task_id == "task-1"
    assert record.story_points == 5.0
    assert record.hours == 8.0
    assert record.team_id == "team-a"


def test_calculate_velocity_tasks_per_week(populated_tracker, base_date):
    """Test calculating tasks per week velocity."""
    start = base_date
    end = base_date + timedelta(days=7)

    metric = populated_tracker.calculate_velocity(
        (start, end),
        VelocityMetricType.TASKS_PER_WEEK,
        team_id="team-alpha",
    )

    assert isinstance(metric, VelocityMetric)
    assert metric.metric_type == VelocityMetricType.TASKS_PER_WEEK
    assert metric.value > 0
    assert metric.task_count > 0
    assert metric.team_id == "team-alpha"


def test_calculate_velocity_story_points_per_sprint(populated_tracker, base_date):
    """Test calculating story points per sprint."""
    start = base_date
    end = base_date + timedelta(days=14)

    metric = populated_tracker.calculate_velocity(
        (start, end),
        VelocityMetricType.STORY_POINTS_PER_SPRINT,
        team_id="team-alpha",
    )

    assert isinstance(metric, VelocityMetric)
    assert metric.metric_type == VelocityMetricType.STORY_POINTS_PER_SPRINT
    assert metric.value > 0


def test_calculate_velocity_hours_per_day(populated_tracker, base_date):
    """Test calculating hours per day velocity."""
    start = base_date
    end = base_date + timedelta(days=7)

    metric = populated_tracker.calculate_velocity(
        (start, end),
        VelocityMetricType.HOURS_PER_DAY,
        team_id="team-alpha",
    )

    assert isinstance(metric, VelocityMetric)
    assert metric.metric_type == VelocityMetricType.HOURS_PER_DAY
    assert metric.value > 0


def test_calculate_velocity_with_filters(populated_tracker, base_date):
    """Test velocity calculation with various filters."""
    start = base_date
    end = base_date + timedelta(days=14)

    metric_by_user = populated_tracker.calculate_velocity(
        (start, end),
        VelocityMetricType.TASKS_PER_WEEK,
        user_id="user-0",
    )

    metric_by_project = populated_tracker.calculate_velocity(
        (start, end),
        VelocityMetricType.TASKS_PER_WEEK,
        project_id="project-x",
    )

    metric_by_type = populated_tracker.calculate_velocity(
        (start, end),
        VelocityMetricType.TASKS_PER_WEEK,
        task_type="feature",
    )

    assert metric_by_user.user_id == "user-0"
    assert metric_by_project.project_id == "project-x"
    assert metric_by_type.task_type == "feature"


def test_rolling_averages(populated_tracker, base_date):
    """Test rolling average calculations."""
    end = base_date + timedelta(days=90)

    for i in range(30, 90):
        day = base_date + timedelta(days=i)
        tracker = populated_tracker
        tracker.record_completion(
            task_id=f"task-extra-{i}",
            completed_at=day,
            story_points=2.0,
            hours=4.0,
            team_id="team-alpha",
        )

    metric = populated_tracker.calculate_velocity(
        (end - timedelta(days=7), end),
        VelocityMetricType.TASKS_PER_WEEK,
        team_id="team-alpha",
    )

    assert metric.rolling_avg_7d >= 0
    assert metric.rolling_avg_30d >= 0
    assert metric.rolling_avg_90d >= 0


def test_detect_trends_acceleration(tracker, base_date):
    """Test detecting acceleration trends."""
    for i in range(60):
        day = base_date + timedelta(days=i)
        tasks = 1 + (i // 10)

        for j in range(tasks):
            tracker.record_completion(
                task_id=f"task-{i}-{j}",
                completed_at=day + timedelta(hours=j),
                team_id="team-a",
            )

    analysis = tracker.detect_trends(history_days=60, team_id="team-a")

    assert isinstance(analysis, TrendAnalysis)
    assert analysis.trend_type in [TrendType.ACCELERATION, TrendType.STABLE]
    assert 0.0 <= analysis.confidence <= 1.0


def test_detect_trends_deceleration(tracker, base_date):
    """Test detecting deceleration trends."""
    for i in range(60):
        day = base_date + timedelta(days=i)
        tasks = max(1, 5 - (i // 10))

        for j in range(tasks):
            tracker.record_completion(
                task_id=f"task-{i}-{j}",
                completed_at=day + timedelta(hours=j),
                team_id="team-a",
            )

    analysis = tracker.detect_trends(history_days=60, team_id="team-a")

    assert isinstance(analysis, TrendAnalysis)
    assert analysis.trend_type in [TrendType.DECELERATION, TrendType.STABLE]


def test_detect_trends_stable(tracker, base_date):
    """Test detecting stable trends."""
    for i in range(30):
        day = base_date + timedelta(days=i)

        for j in range(3):
            tracker.record_completion(
                task_id=f"task-{i}-{j}",
                completed_at=day + timedelta(hours=j),
                team_id="team-a",
            )

    analysis = tracker.detect_trends(history_days=30, team_id="team-a")

    assert isinstance(analysis, TrendAnalysis)
    assert analysis.trend_type == TrendType.STABLE


def test_detect_trends_insufficient_data(tracker, base_date):
    """Test trend detection with insufficient data."""
    tracker.record_completion(
        task_id="task-1",
        completed_at=base_date,
        team_id="team-a",
    )

    analysis = tracker.detect_trends(history_days=30, team_id="team-a")

    assert analysis.trend_type == TrendType.STABLE
    assert analysis.confidence == 0.0
    assert "Insufficient data" in analysis.description


def test_forecast_capacity(populated_tracker, base_date):
    """Test sprint capacity forecasting."""
    forecast = populated_tracker.forecast_capacity(
        sprint_length=14,
        team_id="team-alpha",
    )

    assert isinstance(forecast, CapacityForecast)
    assert forecast.sprint_length_days == 14
    assert forecast.predicted_tasks > 0
    assert forecast.predicted_story_points > 0
    assert forecast.predicted_hours > 0
    assert forecast.confidence_interval_lower <= forecast.predicted_tasks
    assert forecast.confidence_interval_upper >= forecast.predicted_tasks
    assert forecast.based_on_samples > 0


def test_forecast_capacity_no_data(tracker):
    """Test forecasting with no data."""
    forecast = tracker.forecast_capacity(sprint_length=14)

    assert forecast.predicted_tasks == 0.0
    assert forecast.predicted_story_points == 0.0
    assert forecast.predicted_hours == 0.0
    assert forecast.based_on_samples == 0


def test_generate_line_chart(populated_tracker, base_date):
    """Test generating line chart data."""
    start = base_date
    end = base_date + timedelta(days=28)

    chart = populated_tracker.generate_velocity_chart(
        (start, end),
        chart_type="line",
        metric=VelocityMetricType.TASKS_PER_WEEK,
        team_id="team-alpha",
    )

    assert isinstance(chart, ChartData)
    assert chart.chart_type == "line"
    assert len(chart.labels) > 0
    assert len(chart.datasets) > 0
    assert len(chart.datasets[0]["data"]) == len(chart.labels)


def test_generate_bar_chart(populated_tracker, base_date):
    """Test generating bar chart data."""
    start = base_date
    end = base_date + timedelta(days=28)

    chart = populated_tracker.generate_velocity_chart(
        (start, end),
        chart_type="bar",
        metric=VelocityMetricType.TASKS_PER_WEEK,
        team_id="team-alpha",
    )

    assert isinstance(chart, ChartData)
    assert chart.chart_type == "bar"
    assert len(chart.labels) > 0
    assert all("Sprint" in label for label in chart.labels)


def test_generate_comparison_chart(tracker, base_date):
    """Test generating team comparison chart."""
    for team in ["team-a", "team-b", "team-c"]:
        for i in range(30):
            day = base_date + timedelta(days=i)
            tracker.record_completion(
                task_id=f"{team}-task-{i}",
                completed_at=day,
                team_id=team,
            )

    start = base_date
    end = base_date + timedelta(days=28)

    chart = tracker.generate_velocity_chart(
        (start, end),
        chart_type="comparison",
        metric=VelocityMetricType.TASKS_PER_WEEK,
    )

    assert isinstance(chart, ChartData)
    assert chart.chart_type == "bar"
    assert len(chart.labels) == 3
    assert "comparison" in chart.metadata


def test_normalize_velocity():
    """Test velocity normalization for team size."""
    tracker = VelocityTracker()

    normalized_5 = tracker.normalize_velocity(100.0, team_size=5)
    normalized_10 = tracker.normalize_velocity(100.0, team_size=10)

    assert normalized_5 > normalized_10
    assert tracker.normalize_velocity(100.0, team_size=0) == 0.0


def test_normalize_velocity_with_hours():
    """Test velocity normalization with custom working hours."""
    tracker = VelocityTracker()

    normal_hours = tracker.normalize_velocity(100.0, team_size=5, working_hours_per_week=40.0)
    reduced_hours = tracker.normalize_velocity(100.0, team_size=5, working_hours_per_week=20.0)

    assert reduced_hours > normal_hours


def test_velocity_metric_to_dict(populated_tracker, base_date):
    """Test serializing velocity metric to dict."""
    metric = populated_tracker.calculate_velocity(
        (base_date, base_date + timedelta(days=7)),
        VelocityMetricType.TASKS_PER_WEEK,
        team_id="team-alpha",
    )

    data = metric.to_dict()

    assert "metric_type" in data
    assert "value" in data
    assert "period_start" in data
    assert "period_end" in data
    assert "task_count" in data
    assert "rolling_avg_7d" in data
    assert data["metric_type"] == "tasks_per_week"


def test_trend_analysis_to_dict(populated_tracker, base_date):
    """Test serializing trend analysis to dict."""
    analysis = populated_tracker.detect_trends(history_days=30, team_id="team-alpha")

    data = analysis.to_dict()

    assert "trend_type" in data
    assert "confidence" in data
    assert "description" in data
    assert "start_date" in data
    assert "end_date" in data
    assert "velocity_change_pct" in data


def test_capacity_forecast_to_dict(populated_tracker):
    """Test serializing capacity forecast to dict."""
    forecast = populated_tracker.forecast_capacity(sprint_length=14, team_id="team-alpha")

    data = forecast.to_dict()

    assert "sprint_length_days" in data
    assert "predicted_tasks" in data
    assert "predicted_story_points" in data
    assert "predicted_hours" in data
    assert "confidence_interval_lower" in data
    assert "confidence_interval_upper" in data
    assert data["sprint_length_days"] == 14


def test_chart_data_to_dict(populated_tracker, base_date):
    """Test serializing chart data to dict."""
    chart = populated_tracker.generate_velocity_chart(
        (base_date, base_date + timedelta(days=14)),
        chart_type="line",
        metric=VelocityMetricType.TASKS_PER_WEEK,
    )

    data = chart.to_dict()

    assert "chart_type" in data
    assert "labels" in data
    assert "datasets" in data
    assert "metadata" in data
    assert data["chart_type"] == "line"


def test_empty_tracker_velocity(tracker, base_date):
    """Test velocity calculation with no data."""
    metric = tracker.calculate_velocity(
        (base_date, base_date + timedelta(days=7)),
        VelocityMetricType.TASKS_PER_WEEK,
    )

    assert metric.value == 0.0
    assert metric.task_count == 0
    assert metric.rolling_avg_7d == 0.0


def test_anomaly_detection(tracker, base_date):
    """Test anomaly detection in velocity trends."""
    for i in range(30):
        day = base_date + timedelta(days=i)
        tasks = 3 if i != 15 else 20

        for j in range(tasks):
            tracker.record_completion(
                task_id=f"task-{i}-{j}",
                completed_at=day + timedelta(hours=j),
            )

    analysis = tracker.detect_trends(history_days=30)

    assert isinstance(analysis, TrendAnalysis)


def test_seasonality_pattern(tracker, base_date):
    """Test tracking seasonality in velocity."""
    for i in range(90):
        day = base_date + timedelta(days=i)
        tasks_per_day = 5 if i % 7 < 5 else 1

        for j in range(tasks_per_day):
            tracker.record_completion(
                task_id=f"task-{i}-{j}",
                completed_at=day + timedelta(hours=j),
            )

    analysis = tracker.detect_trends(history_days=90)

    assert isinstance(analysis, TrendAnalysis)
    assert analysis.trend_type in [TrendType.STABLE, TrendType.SEASONALITY, TrendType.ANOMALY]
