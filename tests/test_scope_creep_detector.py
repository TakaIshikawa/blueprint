"""Tests for scope creep detector with drift measurement."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from blueprint.analytics.scope_creep_detector import (
    AlertSeverity,
    ChangeCategory,
    ChangeType,
    ChangeVelocity,
    DriftResult,
    ScopeBaseline,
    ScopeChange,
    ScopeCreepDetectorConfig,
    SprintScopeTrend,
    TaskSnapshot,
    ThresholdAlert,
    analyze_trends,
    calculate_change_velocity,
    calculate_drift,
    capture_baseline,
    detect_changes,
    generate_scope_change_report,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

NOW = datetime(2025, 6, 1, 12, 0, 0)


def _make_tasks(count: int, effort: float = 2.0) -> list[dict[str, object]]:
    return [
        {"task_id": f"t-{i}", "title": f"Task {i}", "effort": effort, "status": "pending"}
        for i in range(1, count + 1)
    ]


# ---------------------------------------------------------------------------
# Baseline capture
# ---------------------------------------------------------------------------


class TestCaptureBaseline:
    def test_captures_task_count_and_effort(self) -> None:
        tasks = _make_tasks(5, effort=3.0)
        baseline = capture_baseline(tasks, captured_at=NOW)
        assert baseline.task_count == 5
        assert baseline.total_effort == 15.0

    def test_captures_timeline(self) -> None:
        start = datetime(2025, 1, 1)
        end = datetime(2025, 3, 31)
        baseline = capture_baseline([], timeline_start=start, timeline_end=end, captured_at=NOW)
        assert baseline.timeline_start == start
        assert baseline.timeline_end == end

    def test_captures_individual_tasks(self) -> None:
        tasks = [{"task_id": "a", "title": "Alpha", "effort": 1.0, "tags": ["urgent"]}]
        baseline = capture_baseline(tasks, captured_at=NOW)
        assert len(baseline.tasks) == 1
        assert baseline.tasks[0].task_id == "a"
        assert baseline.tasks[0].tags == ("urgent",)

    def test_empty_tasks(self) -> None:
        baseline = capture_baseline([], captured_at=NOW)
        assert baseline.task_count == 0
        assert baseline.total_effort == 0.0

    def test_to_dict_roundtrip(self) -> None:
        baseline = capture_baseline(_make_tasks(2), captured_at=NOW)
        d = baseline.to_dict()
        assert d["task_count"] == 2
        assert len(d["tasks"]) == 2


# ---------------------------------------------------------------------------
# Change detection
# ---------------------------------------------------------------------------


class TestDetectChanges:
    def test_detects_additions(self) -> None:
        baseline = capture_baseline(_make_tasks(2), captured_at=NOW)
        current = _make_tasks(3)
        changes = detect_changes(baseline, current, detected_at=NOW)
        added = [c for c in changes if c.change_type == ChangeType.ADDED]
        assert len(added) == 1
        assert added[0].task_id == "t-3"
        assert added[0].category == ChangeCategory.NEW_REQUIREMENT

    def test_detects_removals(self) -> None:
        baseline = capture_baseline(_make_tasks(3), captured_at=NOW)
        current = _make_tasks(2)
        changes = detect_changes(baseline, current, detected_at=NOW)
        removed = [c for c in changes if c.change_type == ChangeType.REMOVED]
        assert len(removed) == 1
        assert removed[0].task_id == "t-3"
        assert removed[0].category == ChangeCategory.REMOVAL

    def test_detects_modifications(self) -> None:
        baseline = capture_baseline(
            [{"task_id": "x", "title": "Original", "effort": 2.0}], captured_at=NOW
        )
        current = [{"task_id": "x", "title": "Revised", "effort": 2.0}]
        changes = detect_changes(baseline, current, detected_at=NOW)
        modified = [c for c in changes if c.change_type == ChangeType.MODIFIED]
        assert len(modified) == 1
        assert "title changed" in modified[0].description

    def test_effort_increase_categorized_as_rework(self) -> None:
        baseline = capture_baseline(
            [{"task_id": "x", "title": "Task", "effort": 2.0}], captured_at=NOW
        )
        current = [{"task_id": "x", "title": "Task", "effort": 5.0}]
        changes = detect_changes(baseline, current, detected_at=NOW)
        modified = [c for c in changes if c.change_type == ChangeType.MODIFIED]
        assert len(modified) == 1
        assert modified[0].category == ChangeCategory.REWORK

    def test_no_changes_when_identical(self) -> None:
        tasks = _make_tasks(3)
        baseline = capture_baseline(tasks, captured_at=NOW)
        changes = detect_changes(baseline, tasks, detected_at=NOW)
        assert len(changes) == 0

    def test_attribution_tracking(self) -> None:
        baseline = capture_baseline([], captured_at=NOW)
        current = [{"task_id": "n1", "title": "New", "effort": 1.0, "assignee": "alice"}]
        changes = detect_changes(baseline, current, detected_at=NOW)
        assert changes[0].attributed_to == "alice"


# ---------------------------------------------------------------------------
# Drift calculation
# ---------------------------------------------------------------------------


class TestCalculateDrift:
    def test_positive_drift(self) -> None:
        baseline = capture_baseline(_make_tasks(10, effort=1.0), captured_at=NOW)
        current = _make_tasks(13, effort=1.0)
        result = calculate_drift(baseline, current, detected_at=NOW)
        assert result.task_count_drift_pct == pytest.approx(30.0)
        assert result.effort_drift_pct == pytest.approx(30.0)

    def test_negative_drift(self) -> None:
        baseline = capture_baseline(_make_tasks(10, effort=1.0), captured_at=NOW)
        current = _make_tasks(8, effort=1.0)
        result = calculate_drift(baseline, current, detected_at=NOW)
        assert result.task_count_drift_pct == pytest.approx(-20.0)

    def test_zero_drift(self) -> None:
        tasks = _make_tasks(5)
        baseline = capture_baseline(tasks, captured_at=NOW)
        result = calculate_drift(baseline, tasks, detected_at=NOW)
        assert result.task_count_drift_pct == pytest.approx(0.0)
        assert result.effort_drift_pct == pytest.approx(0.0)

    def test_drift_from_empty_baseline(self) -> None:
        baseline = capture_baseline([], captured_at=NOW)
        current = _make_tasks(3)
        result = calculate_drift(baseline, current, detected_at=NOW)
        assert result.task_count_drift_pct == pytest.approx(100.0)

    def test_drift_result_serialization(self) -> None:
        baseline = capture_baseline(_make_tasks(2), captured_at=NOW)
        result = calculate_drift(baseline, _make_tasks(3), detected_at=NOW)
        d = result.to_dict()
        assert "task_count_drift_pct" in d
        assert "changes" in d
        assert isinstance(d["changes"], list)


# ---------------------------------------------------------------------------
# Threshold alerts
# ---------------------------------------------------------------------------


class TestThresholdAlerts:
    def test_warning_alert(self) -> None:
        baseline = capture_baseline(_make_tasks(10, effort=1.0), captured_at=NOW)
        current = _make_tasks(12, effort=1.0)  # 20% drift
        config = ScopeCreepDetectorConfig(
            task_count_warning_pct=10.0, task_count_critical_pct=30.0
        )
        result = calculate_drift(baseline, current, config=config, detected_at=NOW)
        task_alerts = [a for a in result.alerts if a.metric == "task_count_drift_pct"]
        assert len(task_alerts) == 1
        assert task_alerts[0].severity == AlertSeverity.WARNING

    def test_critical_alert(self) -> None:
        baseline = capture_baseline(_make_tasks(10, effort=1.0), captured_at=NOW)
        current = _make_tasks(14, effort=1.0)  # 40% drift
        config = ScopeCreepDetectorConfig(
            task_count_warning_pct=10.0, task_count_critical_pct=25.0
        )
        result = calculate_drift(baseline, current, config=config, detected_at=NOW)
        task_alerts = [a for a in result.alerts if a.metric == "task_count_drift_pct"]
        assert len(task_alerts) == 1
        assert task_alerts[0].severity == AlertSeverity.CRITICAL

    def test_no_alerts_below_threshold(self) -> None:
        baseline = capture_baseline(_make_tasks(10, effort=1.0), captured_at=NOW)
        current = _make_tasks(10, effort=1.0)
        result = calculate_drift(baseline, current, detected_at=NOW)
        assert len(result.alerts) == 0


# ---------------------------------------------------------------------------
# Change velocity
# ---------------------------------------------------------------------------


class TestChangeVelocity:
    def test_velocity_calculation(self) -> None:
        day1 = NOW
        day3 = NOW + timedelta(days=2)
        changes = [
            ScopeChange(
                change_type=ChangeType.ADDED, category=ChangeCategory.NEW_REQUIREMENT,
                task_id="a", title="A", description="added", detected_at=day1,
            ),
            ScopeChange(
                change_type=ChangeType.ADDED, category=ChangeCategory.NEW_REQUIREMENT,
                task_id="b", title="B", description="added", detected_at=day3,
            ),
            ScopeChange(
                change_type=ChangeType.REMOVED, category=ChangeCategory.REMOVAL,
                task_id="c", title="C", description="removed", detected_at=day3,
            ),
        ]
        velocity = calculate_change_velocity(changes)
        assert velocity is not None
        assert velocity.additions_per_day == pytest.approx(1.0)  # 2 adds / 2 days
        assert velocity.removals_per_day == pytest.approx(0.5)   # 1 rem / 2 days

    def test_empty_changes_returns_none(self) -> None:
        assert calculate_change_velocity([]) is None

    def test_no_timestamps_returns_none(self) -> None:
        changes = [
            ScopeChange(
                change_type=ChangeType.ADDED, category=ChangeCategory.NEW_REQUIREMENT,
                task_id="a", title="A", description="added", detected_at=None,
            ),
        ]
        assert calculate_change_velocity(changes) is None


# ---------------------------------------------------------------------------
# Trend analysis
# ---------------------------------------------------------------------------


class TestAnalyzeTrends:
    def test_cumulative_drift(self) -> None:
        baseline = capture_baseline(_make_tasks(10), captured_at=NOW)
        sprints = [
            {
                "sprint_label": "Sprint 1",
                "start": NOW,
                "end": NOW + timedelta(days=14),
                "additions": 2,
                "removals": 0,
                "modifications": 1,
            },
            {
                "sprint_label": "Sprint 2",
                "start": NOW + timedelta(days=14),
                "end": NOW + timedelta(days=28),
                "additions": 3,
                "removals": 1,
                "modifications": 0,
            },
        ]
        trends = analyze_trends(sprints, baseline)
        assert len(trends) == 2
        assert trends[0].net_change == 2
        assert trends[0].drift_pct == pytest.approx(20.0)
        # Sprint 2: cumulative net = 2 + (3-1) = 4
        assert trends[1].drift_pct == pytest.approx(40.0)

    def test_empty_sprints(self) -> None:
        baseline = capture_baseline(_make_tasks(5), captured_at=NOW)
        assert analyze_trends([], baseline) == []


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------


class TestGenerateReport:
    def test_report_structure(self) -> None:
        baseline = capture_baseline(_make_tasks(5, effort=1.0), captured_at=NOW)
        current = _make_tasks(7, effort=1.0)
        result = calculate_drift(baseline, current, detected_at=NOW)
        report = generate_scope_change_report(result)

        assert "summary" in report
        assert report["summary"]["baseline_task_count"] == 5
        assert report["summary"]["current_task_count"] == 7
        assert report["summary"]["task_count_drift_pct"] == pytest.approx(40.0)
        assert "changes_by_category" in report
        assert "changes_by_type" in report
        assert "attributions" in report
        assert "alerts" in report
        assert "changes" in report

    def test_attribution_aggregation(self) -> None:
        baseline = capture_baseline([], captured_at=NOW)
        current = [
            {"task_id": "a", "title": "A", "assignee": "alice"},
            {"task_id": "b", "title": "B", "assignee": "alice"},
            {"task_id": "c", "title": "C", "assignee": "bob"},
        ]
        result = calculate_drift(baseline, current, detected_at=NOW)
        report = generate_scope_change_report(result)
        assert report["attributions"]["alice"] == 2
        assert report["attributions"]["bob"] == 1
