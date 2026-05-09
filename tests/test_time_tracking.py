"""Test time tracking and estimation system."""

from datetime import datetime, timedelta

import pytest

from blueprint.analytics.time_tracking import (
    ActivityType,
    CompletionForecast,
    Estimate,
    EstimationTechnique,
    TimeEntry,
    TimeSummary,
    TimeTrackingManager,
    VarianceReport,
)


@pytest.fixture
def manager():
    """Create fresh time tracking manager."""
    return TimeTrackingManager()


@pytest.fixture
def base_date():
    """Fixed base date for testing."""
    return datetime(2024, 1, 1, 12, 0, 0)


@pytest.fixture
def populated_manager(manager, base_date):
    """Manager with sample estimates and time entries."""
    manager.set_estimate("task-1", hours=16.0, confidence=0.8)
    manager.set_estimate("task-2", hours=24.0, confidence=0.7)
    manager.set_estimate("task-3", hours=8.0, confidence=0.9)

    for i in range(4):
        date = base_date + timedelta(days=i)
        manager.log_time("task-1", 4.0, date, "user-1", ActivityType.DEVELOPMENT)

    for i in range(3):
        date = base_date + timedelta(days=i)
        manager.log_time("task-2", 6.0, date, "user-2", ActivityType.DEVELOPMENT)

    return manager


def test_set_estimate_ideal_hours(manager):
    """Test setting estimate with ideal hours technique."""
    estimate = manager.set_estimate(
        task_id="task-1",
        hours=16.0,
        confidence=0.8,
        technique=EstimationTechnique.IDEAL_HOURS,
    )

    assert isinstance(estimate, Estimate)
    assert estimate.task_id == "task-1"
    assert estimate.hours == 16.0
    assert estimate.confidence == 0.8
    assert estimate.technique == EstimationTechnique.IDEAL_HOURS


def test_set_estimate_story_points(manager):
    """Test setting estimate with story points technique."""
    estimate = manager.set_estimate(
        task_id="task-1",
        hours=20.0,
        confidence=0.75,
        technique=EstimationTechnique.STORY_POINTS,
        story_points=5.0,
    )

    assert estimate.technique == EstimationTechnique.STORY_POINTS
    assert estimate.story_points == 5.0


def test_set_estimate_pert(manager):
    """Test setting estimate with PERT technique."""
    estimate = manager.set_estimate(
        task_id="task-1",
        hours=0.0,
        confidence=0.85,
        technique=EstimationTechnique.PERT,
        optimistic_hours=8.0,
        likely_hours=12.0,
        pessimistic_hours=20.0,
    )

    assert estimate.technique == EstimationTechnique.PERT
    expected_hours = (8.0 + 4 * 12.0 + 20.0) / 6
    assert estimate.hours == pytest.approx(expected_hours, rel=0.01)
    assert estimate.optimistic_hours == 8.0
    assert estimate.likely_hours == 12.0
    assert estimate.pessimistic_hours == 20.0


def test_set_estimate_pert_missing_values(manager):
    """Test PERT estimation requires all three values."""
    with pytest.raises(ValueError, match="PERT estimation requires"):
        manager.set_estimate(
            task_id="task-1",
            hours=10.0,
            confidence=0.8,
            technique=EstimationTechnique.PERT,
            optimistic_hours=8.0,
        )


def test_log_time(manager, base_date):
    """Test logging time entry."""
    entry = manager.log_time(
        task_id="task-1",
        hours=4.0,
        date=base_date,
        user_id="user-1",
        activity_type=ActivityType.DEVELOPMENT,
        description="Implemented feature X",
    )

    assert isinstance(entry, TimeEntry)
    assert entry.task_id == "task-1"
    assert entry.hours == 4.0
    assert entry.user_id == "user-1"
    assert entry.activity_type == ActivityType.DEVELOPMENT


def test_log_time_multiple_entries(manager, base_date):
    """Test logging multiple time entries for a task."""
    manager.log_time("task-1", 4.0, base_date, "user-1", ActivityType.DEVELOPMENT)
    manager.log_time("task-1", 3.0, base_date + timedelta(days=1), "user-1", ActivityType.TESTING)

    assert len(manager._time_entries["task-1"]) == 2


def test_get_time_summary(populated_manager):
    """Test getting time summary for a task."""
    summary = populated_manager.get_time_summary("task-1")

    assert isinstance(summary, TimeSummary)
    assert summary.task_id == "task-1"
    assert summary.estimated_hours == 16.0
    assert summary.actual_hours == 16.0
    assert summary.remaining_hours == 0.0
    assert summary.completion_percentage == 100.0
    assert summary.burn_rate == 4.0


def test_get_time_summary_no_estimate(manager, base_date):
    """Test time summary for task without estimate."""
    manager.log_time("task-new", 5.0, base_date, "user-1", ActivityType.DEVELOPMENT)

    summary = manager.get_time_summary("task-new")

    assert summary.estimated_hours == 0.0
    assert summary.actual_hours == 5.0
    assert summary.completion_percentage == 0.0


def test_get_time_summary_no_entries(manager):
    """Test time summary for task without time entries."""
    manager.set_estimate("task-1", hours=10.0, confidence=0.8)

    summary = manager.get_time_summary("task-1")

    assert summary.estimated_hours == 10.0
    assert summary.actual_hours == 0.0
    assert summary.remaining_hours == 10.0
    assert summary.completion_percentage == 0.0


def test_calculate_variance_under_budget(manager, base_date):
    """Test variance calculation when under budget."""
    manager.set_estimate("task-1", hours=20.0, confidence=0.8)
    manager.log_time("task-1", 15.0, base_date, "user-1", ActivityType.DEVELOPMENT)

    variance = manager.calculate_variance("task-1")

    assert isinstance(variance, VarianceReport)
    assert variance.task_id == "task-1"
    assert variance.estimated_hours == 20.0
    assert variance.actual_hours == 15.0
    assert variance.variance_hours == -5.0
    assert variance.variance_percentage == -25.0
    assert variance.over_budget is False


def test_calculate_variance_over_budget(manager, base_date):
    """Test variance calculation when over budget."""
    manager.set_estimate("task-1", hours=10.0, confidence=0.8)
    manager.log_time("task-1", 15.0, base_date, "user-1", ActivityType.DEVELOPMENT)

    variance = manager.calculate_variance("task-1")

    assert variance.variance_hours == 5.0
    assert variance.variance_percentage == 50.0
    assert variance.over_budget is True


def test_calculate_variance_days_logged(manager, base_date):
    """Test variance report includes days logged."""
    manager.set_estimate("task-1", hours=20.0, confidence=0.8)

    for i in range(5):
        date = base_date + timedelta(days=i)
        manager.log_time("task-1", 4.0, date, "user-1", ActivityType.DEVELOPMENT)

    variance = manager.calculate_variance("task-1")

    assert variance.days_logged == 5


def test_predict_completion(populated_manager):
    """Test completion prediction for a plan."""
    forecast = populated_manager.predict_completion(
        plan_id="plan-1",
        task_ids=["task-1", "task-2", "task-3"],
    )

    assert isinstance(forecast, CompletionForecast)
    assert forecast.task_id == "plan-1"
    assert forecast.estimated_remaining_hours >= 0
    assert 0.0 <= forecast.confidence <= 1.0


def test_predict_completion_no_data(manager):
    """Test prediction with no time entries."""
    manager.set_estimate("task-1", hours=20.0, confidence=0.8)

    forecast = manager.predict_completion("plan-1", ["task-1"])

    assert forecast.estimated_completion_date is None
    assert forecast.confidence == 0.0
    assert forecast.based_on_days == 0


def test_get_time_report_all(populated_manager):
    """Test getting all time entries."""
    entries = populated_manager.get_time_report()

    assert len(entries) > 0
    assert all(isinstance(e, TimeEntry) for e in entries)


def test_get_time_report_by_user(populated_manager):
    """Test filtering time report by user."""
    entries = populated_manager.get_time_report(user_id="user-1")

    assert all(e.user_id == "user-1" for e in entries)
    assert len(entries) == 4


def test_get_time_report_by_date_range(populated_manager, base_date):
    """Test filtering time report by date range."""
    start = base_date
    end = base_date + timedelta(days=2)

    entries = populated_manager.get_time_report(start_date=start, end_date=end)

    assert all(start <= e.date <= end for e in entries)


def test_get_user_summary(populated_manager, base_date):
    """Test getting user time summary."""
    summary = populated_manager.get_user_summary("user-1")

    assert summary["user_id"] == "user-1"
    assert summary["total_hours"] == 16.0
    assert summary["unique_tasks"] == 1
    assert summary["entries_count"] == 4


def test_get_user_summary_by_activity(manager, base_date):
    """Test user summary breaks down time by activity type."""
    manager.log_time("task-1", 8.0, base_date, "user-1", ActivityType.DEVELOPMENT)
    manager.log_time("task-1", 2.0, base_date, "user-1", ActivityType.TESTING)
    manager.log_time("task-1", 1.0, base_date, "user-1", ActivityType.REVIEW)

    summary = manager.get_user_summary("user-1")

    assert summary["by_activity"]["development"] == 8.0
    assert summary["by_activity"]["testing"] == 2.0
    assert summary["by_activity"]["review"] == 1.0


def test_burn_rate_calculation(manager, base_date):
    """Test burn rate calculation."""
    manager.set_estimate("task-1", hours=20.0, confidence=0.8)

    for i in range(5):
        date = base_date + timedelta(days=i)
        manager.log_time("task-1", 3.0, date, "user-1", ActivityType.DEVELOPMENT)

    summary = manager.get_time_summary("task-1")

    assert summary.burn_rate == 3.0


def test_burn_rate_no_entries(manager):
    """Test burn rate with no time entries."""
    manager.set_estimate("task-1", hours=10.0, confidence=0.8)

    summary = manager.get_time_summary("task-1")

    assert summary.burn_rate == 0.0


def test_estimate_to_dict(manager):
    """Test serializing estimate to dict."""
    estimate = manager.set_estimate(
        task_id="task-1",
        hours=16.0,
        confidence=0.8,
        technique=EstimationTechnique.PERT,
        optimistic_hours=10.0,
        likely_hours=16.0,
        pessimistic_hours=24.0,
    )

    data = estimate.to_dict()

    assert "task_id" in data
    assert "technique" in data
    assert "hours" in data
    assert "confidence" in data
    assert "optimistic_hours" in data
    assert data["technique"] == "pert"


def test_time_entry_to_dict(manager, base_date):
    """Test serializing time entry to dict."""
    entry = manager.log_time(
        task_id="task-1",
        hours=4.0,
        date=base_date,
        user_id="user-1",
        activity_type=ActivityType.DEVELOPMENT,
        description="Work on feature",
    )

    data = entry.to_dict()

    assert "task_id" in data
    assert "user_id" in data
    assert "hours" in data
    assert "activity_type" in data
    assert data["activity_type"] == "development"


def test_time_summary_to_dict(populated_manager):
    """Test serializing time summary to dict."""
    summary = populated_manager.get_time_summary("task-1")

    data = summary.to_dict()

    assert "task_id" in data
    assert "estimated_hours" in data
    assert "actual_hours" in data
    assert "remaining_hours" in data
    assert "completion_percentage" in data
    assert "burn_rate" in data
    assert "time_entries" in data


def test_variance_report_to_dict(populated_manager):
    """Test serializing variance report to dict."""
    variance = populated_manager.calculate_variance("task-1")

    data = variance.to_dict()

    assert "task_id" in data
    assert "estimated_hours" in data
    assert "actual_hours" in data
    assert "variance_hours" in data
    assert "variance_percentage" in data
    assert "over_budget" in data


def test_completion_forecast_to_dict(populated_manager):
    """Test serializing completion forecast to dict."""
    forecast = populated_manager.predict_completion("plan-1", ["task-1", "task-2"])

    data = forecast.to_dict()

    assert "task_id" in data
    assert "estimated_completion_date" in data
    assert "estimated_remaining_hours" in data
    assert "confidence" in data


def test_multiple_users_same_task(manager, base_date):
    """Test multiple users logging time on same task."""
    manager.set_estimate("task-1", hours=20.0, confidence=0.8)

    manager.log_time("task-1", 5.0, base_date, "user-1", ActivityType.DEVELOPMENT)
    manager.log_time("task-1", 3.0, base_date, "user-2", ActivityType.DEVELOPMENT)

    summary = manager.get_time_summary("task-1")

    assert summary.actual_hours == 8.0


def test_completion_percentage_capped_at_100(manager, base_date):
    """Test completion percentage doesn't exceed 100%."""
    manager.set_estimate("task-1", hours=10.0, confidence=0.8)
    manager.log_time("task-1", 15.0, base_date, "user-1", ActivityType.DEVELOPMENT)

    summary = manager.get_time_summary("task-1")

    assert summary.completion_percentage == 100.0
