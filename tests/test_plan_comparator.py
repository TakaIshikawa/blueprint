"""Tests for the plan comparison system with side-by-side analysis."""

import json
from datetime import datetime, timedelta, timezone

from blueprint.comparison.plan_comparator import (
    ComparisonDataStore,
    ComparisonDimension,
    ComparisonReport,
    ComparisonResult,
    Difference,
    DifferenceType,
    MetricComparison,
    PlanComparator,
    ResourceComparison,
    TaskOverlap,
    TimelineComparison,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _past(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def _make_store() -> ComparisonDataStore:
    """Build a store with multiple plans for comparison."""
    now = _now()
    store = ComparisonDataStore()

    store.plans["plan-1"] = {
        "id": "plan-1",
        "title": "Auth System",
        "status": "in_progress",
        "tags": ["backend", "security"],
        "user_ids": ["user-1", "user-2"],
        "tasks": [
            {"id": "t-1", "title": "Login endpoint", "depends_on": [], "status": "completed", "assignee": "user-1"},
            {"id": "t-2", "title": "Token refresh", "depends_on": ["t-1"], "status": "in_progress", "assignee": "user-2"},
            {"id": "t-3", "title": "Session management", "depends_on": ["t-2"], "status": "pending", "assignee": "user-1"},
        ],
        "created_at": _past(30),
        "updated_at": now,
    }

    store.plans["plan-2"] = {
        "id": "plan-2",
        "title": "Dashboard UI",
        "status": "completed",
        "tags": ["frontend"],
        "user_ids": ["user-2", "user-3"],
        "tasks": [
            {"id": "t-4", "title": "Login endpoint", "depends_on": [], "status": "completed", "assignee": "user-2"},
            {"id": "t-5", "title": "Chart component", "depends_on": [], "status": "completed", "assignee": "user-3"},
            {"id": "t-6", "title": "Dashboard layout", "depends_on": ["t-4", "t-5"], "status": "completed", "assignee": "user-2"},
        ],
        "created_at": _past(60),
        "updated_at": _past(5),
    }

    store.plans["plan-3"] = {
        "id": "plan-3",
        "title": "Billing System",
        "status": "draft",
        "tags": ["backend", "billing"],
        "user_ids": ["user-1"],
        "tasks": [
            {"id": "t-7", "title": "Stripe setup", "depends_on": [], "status": "pending"},
        ],
        "created_at": _past(10),
        "updated_at": _past(10),
    }

    return store


def _comparator(store: ComparisonDataStore | None = None) -> PlanComparator:
    return PlanComparator(store=store or _make_store())


# ---------------------------------------------------------------------------
# Full comparison tests
# ---------------------------------------------------------------------------


class TestFullComparison:
    def test_compare_two_plans(self):
        comp = _comparator()
        result = comp.compare_plans(["plan-1", "plan-2"])
        assert isinstance(result, ComparisonResult)
        assert len(result.plan_ids) == 2
        assert len(result.dimensions_compared) > 0

    def test_compare_all_dimensions(self):
        comp = _comparator()
        result = comp.compare_plans(["plan-1", "plan-2"])
        assert "overview" in result.dimensions_compared
        assert "tasks" in result.dimensions_compared
        assert "timeline" in result.dimensions_compared
        assert "resources" in result.dimensions_compared

    def test_compare_specific_dimensions(self):
        comp = _comparator()
        result = comp.compare_plans(
            ["plan-1", "plan-2"],
            dimensions=[ComparisonDimension.OVERVIEW],
        )
        assert result.dimensions_compared == ["overview"]

    def test_compare_three_plans(self):
        comp = _comparator()
        result = comp.compare_plans(["plan-1", "plan-2", "plan-3"])
        assert len(result.plan_ids) == 3

    def test_comparison_id_unique(self):
        comp = _comparator()
        r1 = comp.compare_plans(["plan-1", "plan-2"])
        r2 = comp.compare_plans(["plan-1", "plan-2"])
        assert r1.comparison_id != r2.comparison_id


# ---------------------------------------------------------------------------
# Overview comparison tests
# ---------------------------------------------------------------------------


class TestOverviewComparison:
    def test_detects_status_difference(self):
        comp = _comparator()
        result = comp.compare_plans(
            ["plan-1", "plan-2"],
            dimensions=[ComparisonDimension.OVERVIEW],
        )
        status_diffs = [d for d in result.differences if d.field_name == "status"]
        assert len(status_diffs) == 1
        assert status_diffs[0].diff_type == DifferenceType.MODIFIED

    def test_task_count_metric(self):
        comp = _comparator()
        result = comp.compare_plans(
            ["plan-1", "plan-2"],
            dimensions=[ComparisonDimension.OVERVIEW],
        )
        task_metrics = [m for m in result.metrics if m.metric_name == "task_count"]
        assert len(task_metrics) == 1
        assert task_metrics[0].plan_values["plan-1"] == 3.0
        assert task_metrics[0].plan_values["plan-2"] == 3.0

    def test_completion_percentage_metric(self):
        comp = _comparator()
        result = comp.compare_plans(
            ["plan-1", "plan-2"],
            dimensions=[ComparisonDimension.OVERVIEW],
        )
        comp_metrics = [m for m in result.metrics if m.metric_name == "completion_pct"]
        assert len(comp_metrics) == 1
        # plan-2 has all tasks completed
        assert comp_metrics[0].plan_values["plan-2"] == 100.0


# ---------------------------------------------------------------------------
# Task comparison tests
# ---------------------------------------------------------------------------


class TestTaskComparison:
    def test_task_overlap_detected(self):
        comp = _comparator()
        result = comp.compare_plans(
            ["plan-1", "plan-2"],
            dimensions=[ComparisonDimension.TASKS],
        )
        assert result.task_overlap is not None
        # Both plans have "Login endpoint"
        assert "Login endpoint" in result.task_overlap.shared_tasks

    def test_unique_tasks_identified(self):
        comp = _comparator()
        result = comp.compare_plans(
            ["plan-1", "plan-2"],
            dimensions=[ComparisonDimension.TASKS],
        )
        assert result.task_overlap is not None
        unique_plan1 = result.task_overlap.unique_tasks.get("plan-1", [])
        assert "Token refresh" in unique_plan1
        assert "Session management" in unique_plan1

    def test_overlap_ratio(self):
        comp = _comparator()
        result = comp.compare_plans(
            ["plan-1", "plan-2"],
            dimensions=[ComparisonDimension.TASKS],
        )
        assert result.task_overlap is not None
        assert 0 < result.task_overlap.overlap_ratio < 1.0

    def test_unique_task_differences(self):
        comp = _comparator()
        result = comp.compare_plans(
            ["plan-1", "plan-2"],
            dimensions=[ComparisonDimension.TASKS],
        )
        added_diffs = [d for d in result.differences if d.diff_type == DifferenceType.ADDED]
        assert len(added_diffs) > 0


# ---------------------------------------------------------------------------
# Timeline comparison tests
# ---------------------------------------------------------------------------


class TestTimelineComparison:
    def test_timeline_start_dates(self):
        comp = _comparator()
        result = comp.compare_timelines(["plan-1", "plan-2"])
        assert isinstance(result, TimelineComparison)
        assert "plan-1" in result.plan_start_dates
        assert "plan-2" in result.plan_start_dates

    def test_timeline_durations(self):
        comp = _comparator()
        result = comp.compare_timelines(["plan-1", "plan-2"])
        assert result.plan_durations_days["plan-1"] >= 0
        assert result.plan_durations_days["plan-2"] >= 0

    def test_critical_path_lengths(self):
        comp = _comparator()
        result = comp.compare_timelines(["plan-1", "plan-2"])
        # plan-1: t-1 -> t-2 -> t-3, chain of 3
        assert result.critical_path_lengths["plan-1"] == 3
        # plan-2: t-4 -> t-6 (length 2) or t-5 -> t-6 (length 2)
        assert result.critical_path_lengths["plan-2"] == 2


# ---------------------------------------------------------------------------
# Resource comparison tests
# ---------------------------------------------------------------------------


class TestResourceComparison:
    def test_resource_team_sizes(self):
        comp = _comparator()
        result = comp.compare_resources(["plan-1", "plan-2"])
        assert isinstance(result, ResourceComparison)
        assert result.plan_team_sizes["plan-1"] == 2  # user-1, user-2
        assert result.plan_team_sizes["plan-2"] == 2  # user-2, user-3

    def test_shared_assignees(self):
        comp = _comparator()
        result = comp.compare_resources(["plan-1", "plan-2"])
        assert "user-2" in result.shared_assignees

    def test_unique_assignees(self):
        comp = _comparator()
        result = comp.compare_resources(["plan-1", "plan-2"])
        assert "user-1" in result.unique_assignees.get("plan-1", [])
        assert "user-3" in result.unique_assignees.get("plan-2", [])

    def test_utilization_scores(self):
        comp = _comparator()
        result = comp.compare_resources(["plan-1", "plan-2"])
        assert result.utilization_scores["plan-1"] > 0
        assert result.utilization_scores["plan-2"] > 0


# ---------------------------------------------------------------------------
# Dependency comparison tests
# ---------------------------------------------------------------------------


class TestDependencyComparison:
    def test_dependency_count_difference(self):
        comp = _comparator()
        result = comp.compare_plans(
            ["plan-1", "plan-2"],
            dimensions=[ComparisonDimension.DEPENDENCIES],
        )
        dep_metrics = [m for m in result.metrics if m.metric_name == "dependency_count"]
        assert len(dep_metrics) == 1

    def test_dependency_complexity(self):
        comp = _comparator()
        result = comp.compare_plans(
            ["plan-1", "plan-2"],
            dimensions=[ComparisonDimension.DEPENDENCIES],
        )
        complexity = [m for m in result.metrics if m.metric_name == "dependency_complexity"]
        assert len(complexity) == 1


# ---------------------------------------------------------------------------
# Risk and quality comparison tests
# ---------------------------------------------------------------------------


class TestRiskAndQuality:
    def test_risk_score_comparison(self):
        comp = _comparator()
        result = comp.compare_plans(
            ["plan-1", "plan-2"],
            dimensions=[ComparisonDimension.RISKS],
        )
        risk_metrics = [m for m in result.metrics if m.metric_name == "risk_score"]
        assert len(risk_metrics) == 1

    def test_quality_score_comparison(self):
        comp = _comparator()
        result = comp.compare_plans(
            ["plan-1", "plan-2"],
            dimensions=[ComparisonDimension.QUALITY],
        )
        quality_metrics = [m for m in result.metrics if m.metric_name == "quality_score"]
        assert len(quality_metrics) == 1
        # plan-2 has all tasks completed, so higher quality
        assert quality_metrics[0].plan_values["plan-2"] > quality_metrics[0].plan_values["plan-1"]


# ---------------------------------------------------------------------------
# Metric comparison tests
# ---------------------------------------------------------------------------


class TestMetricComparison:
    def test_compare_specific_metrics(self):
        comp = _comparator()
        result = comp.compare_metrics(["plan-1", "plan-2"], metric_names=["task_count"])
        assert len(result) == 1
        assert result[0].metric_name == "task_count"

    def test_metric_best_worst(self):
        comp = _comparator()
        result = comp.compare_metrics(["plan-1", "plan-3"], metric_names=["task_count"])
        assert len(result) == 1
        assert result[0].best_plan_id == "plan-1"  # 3 tasks
        assert result[0].worst_plan_id == "plan-3"  # 1 task

    def test_metric_spread(self):
        comp = _comparator()
        result = comp.compare_metrics(["plan-1", "plan-3"], metric_names=["task_count"])
        assert result[0].spread == 2.0  # 3 - 1

    def test_metric_average(self):
        comp = _comparator()
        result = comp.compare_metrics(["plan-1", "plan-3"], metric_names=["task_count"])
        assert result[0].average == 2.0  # (3 + 1) / 2


# ---------------------------------------------------------------------------
# Report generation tests
# ---------------------------------------------------------------------------


class TestReportGeneration:
    def test_generate_json_report(self):
        comp = _comparator()
        comparison = comp.compare_plans(["plan-1", "plan-2"])
        report = comp.generate_comparison_report(comparison, fmt="json")
        assert isinstance(report, ComparisonReport)
        assert report.format == "json"
        assert len(report.data) > 0

    def test_report_json_parsable(self):
        comp = _comparator()
        comparison = comp.compare_plans(["plan-1", "plan-2"])
        report = comp.generate_comparison_report(comparison)
        data = json.loads(report.data)
        assert data["comparison_id"] == comparison.comparison_id
        assert "differences" in data
        assert "metrics" in data

    def test_export_comparison(self):
        comp = _comparator()
        comparison = comp.compare_plans(["plan-1", "plan-2"])
        data = comp.export_comparison(comparison)
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_report_plan_count(self):
        comp = _comparator()
        comparison = comp.compare_plans(["plan-1", "plan-2"])
        report = comp.generate_comparison_report(comparison)
        assert report.plan_count == 2


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_compare_single_plan(self):
        comp = _comparator()
        result = comp.compare_plans(["plan-1"])
        assert len(result.plan_ids) == 1
        assert len(result.differences) == 0

    def test_compare_empty_plans(self):
        store = ComparisonDataStore()
        store.plans["empty-1"] = {
            "id": "empty-1",
            "title": "Empty 1",
            "status": "draft",
            "tags": [],
            "user_ids": [],
            "tasks": [],
            "created_at": _now(),
            "updated_at": _now(),
        }
        store.plans["empty-2"] = {
            "id": "empty-2",
            "title": "Empty 2",
            "status": "draft",
            "tags": [],
            "user_ids": [],
            "tasks": [],
            "created_at": _now(),
            "updated_at": _now(),
        }
        comp = PlanComparator(store=store)
        result = comp.compare_plans(["empty-1", "empty-2"])
        assert len(result.plan_ids) == 2
        assert result.task_overlap is not None
        assert result.task_overlap.shared_count == 0

    def test_compare_nonexistent_plan(self):
        comp = _comparator()
        result = comp.compare_plans(["plan-1", "nonexistent"])
        # Should still work with the plans found
        assert len(result.plan_ids) == 1

    def test_summary_includes_counts(self):
        comp = _comparator()
        result = comp.compare_plans(["plan-1", "plan-2"])
        assert "total_differences" in result.summary
        assert "plan_count" in result.summary
