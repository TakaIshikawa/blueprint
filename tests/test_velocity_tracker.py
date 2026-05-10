"""Tests for velocity tracker covering velocity calculations."""

import pytest

from blueprint.metrics.velocity_tracker import VelocityTracker
from blueprint.metrics.sprint_model import SprintStatus, VelocityTrend


@pytest.fixture
def tracker() -> VelocityTracker:
    return VelocityTracker()


@pytest.fixture
def tracker_with_sprints(tracker: VelocityTracker):
    """Create tracker with completed sprints for velocity analysis."""
    s1 = tracker.create_sprint("Sprint 1", "2025-01-01", "2025-01-14", committed_points=20)
    tracker.complete_sprint(s1.sprint_id, 18)
    s2 = tracker.create_sprint("Sprint 2", "2025-01-15", "2025-01-28", committed_points=22)
    tracker.complete_sprint(s2.sprint_id, 20)
    s3 = tracker.create_sprint("Sprint 3", "2025-01-29", "2025-02-11", committed_points=25)
    tracker.complete_sprint(s3.sprint_id, 24)
    return tracker


class TestSprintCRUD:
    def test_create_sprint(self, tracker: VelocityTracker):
        s = tracker.create_sprint("Sprint 1", "2025-01-01", "2025-01-14", committed_points=20)
        assert s.name == "Sprint 1"
        assert s.status == SprintStatus.PLANNED

    def test_complete_sprint(self, tracker: VelocityTracker):
        s = tracker.create_sprint("S1", "2025-01-01", "2025-01-14", committed_points=20)
        result = tracker.complete_sprint(s.sprint_id, 18, added_points=3, removed_points=1)
        assert result is not None
        assert result.status == SprintStatus.COMPLETED
        assert result.completed_points == 18
        assert result.added_points == 3

    def test_list_sprints_sorted(self, tracker: VelocityTracker):
        tracker.create_sprint("S2", "2025-02-01", "2025-02-14")
        tracker.create_sprint("S1", "2025-01-01", "2025-01-14")
        sprints = tracker.list_sprints()
        assert sprints[0].name == "S1"

    def test_get_sprint(self, tracker: VelocityTracker):
        s = tracker.create_sprint("S1", "2025-01-01", "2025-01-14")
        assert tracker.get_sprint(s.sprint_id) is not None
        assert tracker.get_sprint("missing") is None


class TestVelocityCalculation:
    def test_average_velocity(self, tracker_with_sprints: VelocityTracker):
        vel = tracker_with_sprints.calculate_velocity()
        assert vel == pytest.approx((18 + 20 + 24) / 3, rel=0.01)

    def test_velocity_last_n(self, tracker_with_sprints: VelocityTracker):
        vel = tracker_with_sprints.calculate_velocity(last_n=2)
        assert vel == pytest.approx((20 + 24) / 2, rel=0.01)

    def test_velocity_no_sprints(self, tracker: VelocityTracker):
        assert tracker.calculate_velocity() == 0.0

    def test_velocity_history(self, tracker_with_sprints: VelocityTracker):
        history = tracker_with_sprints.velocity_history()
        assert len(history) == 3
        assert history[0].velocity == 18
        assert history[2].velocity == 24


class TestBurndown:
    def test_burndown_data(self, tracker: VelocityTracker):
        s = tracker.create_sprint("S1", "2025-01-01", "2025-01-14", committed_points=28)
        tracker.complete_sprint(s.sprint_id, 24)
        burndown = tracker.generate_burndown(s.sprint_id)
        assert len(burndown) == 14  # 13 days + start
        assert burndown[0].remaining == 28
        assert burndown[0].ideal_remaining == 28

    def test_burndown_missing_sprint(self, tracker: VelocityTracker):
        assert tracker.generate_burndown("missing") == []


class TestBurnup:
    def test_burnup_data(self, tracker: VelocityTracker):
        s = tracker.create_sprint("S1", "2025-01-01", "2025-01-14", committed_points=20)
        tracker.complete_sprint(s.sprint_id, 18, added_points=5, removed_points=2)
        burnup = tracker.generate_burnup(s.sprint_id)
        assert len(burnup) > 0
        assert burnup[0].completed == 0
        assert burnup[-1].completed == pytest.approx(18, rel=0.1)

    def test_burnup_missing_sprint(self, tracker: VelocityTracker):
        assert tracker.generate_burnup("missing") == []


class TestTrendAnalysis:
    def test_accelerating_trend(self, tracker: VelocityTracker):
        for i, pts in enumerate([10, 12, 15, 20, 25]):
            s = tracker.create_sprint(
                f"S{i}", f"2025-0{i+1}-01", f"2025-0{i+1}-14", committed_points=pts
            )
            tracker.complete_sprint(s.sprint_id, pts)
        trend = tracker.analyze_trend()
        assert trend.trend == VelocityTrend.ACCELERATING

    def test_insufficient_data(self, tracker: VelocityTracker):
        s = tracker.create_sprint("S1", "2025-01-01", "2025-01-14", committed_points=20)
        tracker.complete_sprint(s.sprint_id, 18)
        trend = tracker.analyze_trend()
        assert trend.trend == VelocityTrend.INSUFFICIENT_DATA

    def test_stable_trend(self, tracker: VelocityTracker):
        for i in range(5):
            s = tracker.create_sprint(
                f"S{i}", f"2025-0{i+1}-01", f"2025-0{i+1}-14", committed_points=20
            )
            tracker.complete_sprint(s.sprint_id, 20)
        trend = tracker.analyze_trend()
        assert trend.trend == VelocityTrend.STABLE


class TestCapacityPlanning:
    def test_forecast(self, tracker_with_sprints: VelocityTracker):
        forecast = tracker_with_sprints.forecast_capacity(60.0)
        assert forecast.estimated_velocity > 0
        assert forecast.sprints_remaining >= 1
        assert forecast.confidence > 0

    def test_forecast_no_data(self, tracker: VelocityTracker):
        forecast = tracker.forecast_capacity(100.0)
        assert forecast.estimated_velocity == 0.0
        assert forecast.confidence == 0.0


class TestPredictability:
    def test_predictability(self, tracker_with_sprints: VelocityTracker):
        pred = tracker_with_sprints.calculate_predictability()
        assert 0 < pred <= 1.0

    def test_predictability_no_data(self, tracker: VelocityTracker):
        assert tracker.calculate_predictability() == 0.0


class TestTeamComparison:
    def test_compare_teams(self, tracker: VelocityTracker):
        s1 = tracker.create_sprint("S1", "2025-01-01", "2025-01-14", team_id="alpha", committed_points=20)
        tracker.complete_sprint(s1.sprint_id, 18)
        s2 = tracker.create_sprint("S2", "2025-01-01", "2025-01-14", team_id="beta", committed_points=30)
        tracker.complete_sprint(s2.sprint_id, 28)
        comparison = tracker.compare_teams(["alpha", "beta"])
        assert comparison["alpha"] == 18
        assert comparison["beta"] == 28
