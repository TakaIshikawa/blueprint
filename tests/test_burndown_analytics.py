"""Test burndown and burnup chart analytics."""

from datetime import datetime, timedelta

import pytest

from blueprint.analytics.burndown_analytics import (
    BurndownAnalytics,
    BurndownData,
    BurnupData,
    DateForecast,
    Point,
    ScopeChange,
    WorkMetric,
)


@pytest.fixture
def analytics():
    """Create fresh burndown analytics instance."""
    return BurndownAnalytics()


@pytest.fixture
def base_date():
    """Fixed base date for testing."""
    return datetime(2024, 1, 1, 12, 0, 0)


@pytest.fixture
def populated_analytics(analytics, base_date):
    """Analytics with sample snapshot data for a 14-day sprint."""
    plan_id = "plan-1"
    total_tasks = 20
    total_points = 50.0
    total_hours = 100.0

    for day in range(14):
        date = base_date + timedelta(days=day)
        completed_ratio = day / 14.0

        analytics.record_snapshot(
            plan_id=plan_id,
            date=date,
            remaining_tasks=int(total_tasks * (1 - completed_ratio)),
            remaining_points=total_points * (1 - completed_ratio),
            remaining_hours=total_hours * (1 - completed_ratio),
            completed_tasks=int(total_tasks * completed_ratio),
            completed_points=total_points * completed_ratio,
            completed_hours=total_hours * completed_ratio,
            total_tasks=total_tasks,
            total_points=total_points,
            total_hours=total_hours,
        )

    return analytics


def test_record_snapshot(analytics, base_date):
    """Test recording daily snapshots."""
    analytics.record_snapshot(
        plan_id="plan-1",
        date=base_date,
        remaining_tasks=10,
        remaining_points=25.0,
        remaining_hours=50.0,
        completed_tasks=5,
        completed_points=15.0,
        completed_hours=30.0,
        total_tasks=15,
        total_points=40.0,
        total_hours=80.0,
    )

    assert "plan-1" in analytics._snapshots
    assert len(analytics._snapshots["plan-1"]) == 1
    snapshot = analytics._snapshots["plan-1"][0]
    assert snapshot.remaining_tasks == 10
    assert snapshot.completed_tasks == 5


def test_record_scope_change(analytics, base_date):
    """Test recording scope changes."""
    analytics.record_scope_change(
        plan_id="plan-1",
        date=base_date,
        change_type="added",
        task_id="task-new",
        impact=5.0,
        description="Added 5 story points",
    )

    assert "plan-1" in analytics._scope_changes
    assert len(analytics._scope_changes["plan-1"]) == 1
    change = analytics._scope_changes["plan-1"][0]
    assert change.change_type == "added"
    assert change.impact == 5.0


def test_generate_burndown_tasks(populated_analytics, base_date):
    """Test generating burndown chart with task count metric."""
    start = base_date
    end = base_date + timedelta(days=13)

    burndown = populated_analytics.generate_burndown(
        plan_id="plan-1",
        start_date=start,
        end_date=end,
        metric=WorkMetric.TASK_COUNT,
    )

    assert isinstance(burndown, BurndownData)
    assert burndown.plan_id == "plan-1"
    assert burndown.metric == WorkMetric.TASK_COUNT
    assert burndown.total_work == 20.0
    assert len(burndown.ideal_line) > 0
    assert len(burndown.actual_line) > 0


def test_generate_burndown_story_points(populated_analytics, base_date):
    """Test generating burndown chart with story points metric."""
    start = base_date
    end = base_date + timedelta(days=13)

    burndown = populated_analytics.generate_burndown(
        plan_id="plan-1",
        start_date=start,
        end_date=end,
        metric=WorkMetric.STORY_POINTS,
    )

    assert isinstance(burndown, BurndownData)
    assert burndown.metric == WorkMetric.STORY_POINTS
    assert burndown.total_work == 50.0


def test_generate_burndown_hours(populated_analytics, base_date):
    """Test generating burndown chart with hours metric."""
    start = base_date
    end = base_date + timedelta(days=13)

    burndown = populated_analytics.generate_burndown(
        plan_id="plan-1",
        start_date=start,
        end_date=end,
        metric=WorkMetric.HOURS,
    )

    assert isinstance(burndown, BurndownData)
    assert burndown.metric == WorkMetric.HOURS
    assert burndown.total_work == 100.0


def test_generate_burndown_no_data(analytics, base_date):
    """Test generating burndown with no snapshot data."""
    start = base_date
    end = base_date + timedelta(days=13)

    burndown = analytics.generate_burndown(
        plan_id="plan-unknown",
        start_date=start,
        end_date=end,
        metric=WorkMetric.TASK_COUNT,
    )

    assert burndown.total_work == 0.0
    assert burndown.remaining_work == 0.0
    assert len(burndown.ideal_line) == 0
    assert len(burndown.actual_line) == 0


def test_generate_burnup_tasks(populated_analytics, base_date):
    """Test generating burnup chart with task count metric."""
    start = base_date
    end = base_date + timedelta(days=13)

    burnup = populated_analytics.generate_burnup(
        plan_id="plan-1",
        start_date=start,
        end_date=end,
        metric=WorkMetric.TASK_COUNT,
    )

    assert isinstance(burnup, BurnupData)
    assert burnup.plan_id == "plan-1"
    assert burnup.metric == WorkMetric.TASK_COUNT
    assert burnup.total_work == 20.0
    assert len(burnup.completed_line) > 0
    assert len(burnup.total_scope_line) > 0
    assert len(burnup.ideal_line) > 0


def test_generate_burnup_story_points(populated_analytics, base_date):
    """Test generating burnup chart with story points metric."""
    start = base_date
    end = base_date + timedelta(days=13)

    burnup = populated_analytics.generate_burnup(
        plan_id="plan-1",
        start_date=start,
        end_date=end,
        metric=WorkMetric.STORY_POINTS,
    )

    assert isinstance(burnup, BurnupData)
    assert burnup.metric == WorkMetric.STORY_POINTS
    assert burnup.total_work == 50.0


def test_calculate_ideal_line(analytics, base_date):
    """Test calculating ideal linear burndown line."""
    total_work = 100.0
    duration = 10

    ideal_line = analytics.calculate_ideal_line(total_work, duration, base_date)

    assert len(ideal_line) == duration
    assert ideal_line[0].value == pytest.approx(total_work, rel=0.1)
    assert ideal_line[-1].value == pytest.approx(0.0, abs=0.1)


def test_calculate_ideal_line_single_day(analytics, base_date):
    """Test ideal line calculation for single day."""
    ideal_line = analytics.calculate_ideal_line(100.0, 1, base_date)

    assert len(ideal_line) == 1
    assert ideal_line[0].value == 0.0


def test_detect_scope_changes(analytics, base_date):
    """Test detecting scope changes."""
    analytics.record_scope_change(
        plan_id="plan-1",
        date=base_date,
        change_type="added",
        task_id="task-1",
        impact=3.0,
        description="Added feature",
    )

    analytics.record_scope_change(
        plan_id="plan-1",
        date=base_date + timedelta(days=5),
        change_type="removed",
        task_id="task-2",
        impact=-2.0,
        description="Removed task",
    )

    changes = analytics.detect_scope_changes("plan-1")

    assert len(changes) == 2
    assert changes[0].change_type == "added"
    assert changes[1].change_type == "removed"


def test_predict_completion_date(populated_analytics, base_date):
    """Test completion date prediction with Monte Carlo."""
    start = base_date
    end = base_date + timedelta(days=13)
    target = base_date + timedelta(days=14)

    burndown = populated_analytics.generate_burndown(
        plan_id="plan-1",
        start_date=start,
        end_date=end,
        metric=WorkMetric.TASK_COUNT,
    )

    forecast = populated_analytics.predict_completion_date(
        burndown=burndown,
        target_date=target,
        simulations=100,
    )

    assert isinstance(forecast, DateForecast)
    assert forecast.based_on_samples == 100
    assert 0.0 <= forecast.probability_on_time <= 1.0
    assert forecast.confidence_50_pct <= forecast.confidence_85_pct <= forecast.confidence_95_pct


def test_predict_completion_no_remaining_work(analytics, base_date):
    """Test prediction when no work remains."""
    start = base_date
    end = base_date + timedelta(days=13)
    target = base_date + timedelta(days=14)

    burndown = BurndownData(
        plan_id="plan-1",
        start_date=start,
        end_date=end,
        metric=WorkMetric.TASK_COUNT,
        ideal_line=(),
        actual_line=(),
        total_work=100.0,
        remaining_work=0.0,
        completion_percentage=100.0,
    )

    forecast = analytics.predict_completion_date(burndown, target, simulations=100)

    assert forecast.probability_on_time == 1.0
    assert forecast.based_on_samples == 0


def test_predict_completion_no_velocity(analytics, base_date):
    """Test prediction when velocity is zero."""
    start = base_date
    end = base_date + timedelta(days=5)
    target = base_date + timedelta(days=14)

    actual_line = tuple(
        Point(date=base_date + timedelta(days=i), value=100.0)
        for i in range(5)
    )

    burndown = BurndownData(
        plan_id="plan-1",
        start_date=start,
        end_date=end,
        metric=WorkMetric.TASK_COUNT,
        ideal_line=(),
        actual_line=actual_line,
        total_work=100.0,
        remaining_work=100.0,
        completion_percentage=0.0,
    )

    forecast = analytics.predict_completion_date(burndown, target, simulations=100)

    assert forecast.probability_on_time == 0.0


def test_burndown_with_scope_changes(analytics, base_date):
    """Test burndown generation includes scope changes."""
    plan_id = "plan-1"

    for day in range(10):
        date = base_date + timedelta(days=day)
        analytics.record_snapshot(
            plan_id=plan_id,
            date=date,
            remaining_tasks=10 - day,
            completed_tasks=day,
            total_tasks=10,
        )

    analytics.record_scope_change(
        plan_id=plan_id,
        date=base_date + timedelta(days=5),
        change_type="added",
        task_id="new-task",
        impact=3.0,
        description="Scope increase",
    )

    burndown = analytics.generate_burndown(
        plan_id=plan_id,
        start_date=base_date,
        end_date=base_date + timedelta(days=9),
        metric=WorkMetric.TASK_COUNT,
    )

    assert len(burndown.scope_changes) == 1
    assert burndown.scope_changes[0].change_type == "added"


def test_burnup_shows_scope_growth(analytics, base_date):
    """Test burnup chart shows scope growth over time."""
    plan_id = "plan-1"

    for day in range(10):
        date = base_date + timedelta(days=day)
        total_tasks = 10 + (2 if day >= 5 else 0)

        analytics.record_snapshot(
            plan_id=plan_id,
            date=date,
            completed_tasks=day,
            total_tasks=total_tasks,
        )

    burnup = analytics.generate_burnup(
        plan_id=plan_id,
        start_date=base_date,
        end_date=base_date + timedelta(days=9),
        metric=WorkMetric.TASK_COUNT,
    )

    scope_values = [p.value for p in burnup.total_scope_line]
    assert scope_values[0] < scope_values[-1]


def test_completion_percentage_calculation(populated_analytics, base_date):
    """Test completion percentage is calculated correctly."""
    start = base_date
    end = base_date + timedelta(days=13)

    burndown = populated_analytics.generate_burndown(
        plan_id="plan-1",
        start_date=start,
        end_date=end,
        metric=WorkMetric.TASK_COUNT,
    )

    assert 0 <= burndown.completion_percentage <= 100


def test_burndown_data_to_dict(populated_analytics, base_date):
    """Test serializing burndown data to dict."""
    start = base_date
    end = base_date + timedelta(days=13)

    burndown = populated_analytics.generate_burndown(
        plan_id="plan-1",
        start_date=start,
        end_date=end,
        metric=WorkMetric.TASK_COUNT,
    )

    data = burndown.to_dict()

    assert "plan_id" in data
    assert "metric" in data
    assert "ideal_line" in data
    assert "actual_line" in data
    assert "total_work" in data
    assert "remaining_work" in data
    assert "completion_percentage" in data


def test_burnup_data_to_dict(populated_analytics, base_date):
    """Test serializing burnup data to dict."""
    start = base_date
    end = base_date + timedelta(days=13)

    burnup = populated_analytics.generate_burnup(
        plan_id="plan-1",
        start_date=start,
        end_date=end,
        metric=WorkMetric.TASK_COUNT,
    )

    data = burnup.to_dict()

    assert "plan_id" in data
    assert "completed_line" in data
    assert "total_scope_line" in data
    assert "ideal_line" in data


def test_date_forecast_to_dict(populated_analytics, base_date):
    """Test serializing date forecast to dict."""
    start = base_date
    end = base_date + timedelta(days=13)
    target = base_date + timedelta(days=14)

    burndown = populated_analytics.generate_burndown(
        plan_id="plan-1",
        start_date=start,
        end_date=end,
        metric=WorkMetric.TASK_COUNT,
    )

    forecast = populated_analytics.predict_completion_date(burndown, target, simulations=50)

    data = forecast.to_dict()

    assert "predicted_date" in data
    assert "confidence_50_pct" in data
    assert "confidence_85_pct" in data
    assert "confidence_95_pct" in data
    assert "probability_on_time" in data


def test_point_to_dict():
    """Test serializing point to dict."""
    date = datetime(2024, 1, 1)
    point = Point(date=date, value=42.0)

    data = point.to_dict()

    assert "date" in data
    assert "value" in data
    assert data["value"] == 42.0


def test_scope_change_to_dict():
    """Test serializing scope change to dict."""
    change = ScopeChange(
        date=datetime(2024, 1, 1),
        change_type="added",
        task_id="task-1",
        impact=5.0,
        description="New feature",
    )

    data = change.to_dict()

    assert "date" in data
    assert "change_type" in data
    assert "task_id" in data
    assert "impact" in data
    assert "description" in data
