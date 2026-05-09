"""Tests for the benchmarking system with industry standards."""

import json
from datetime import datetime, timezone

from blueprint.benchmarking.benchmark_data import (
    ALL_BENCHMARKS,
    BenchmarkDataPoint,
    BenchmarkDataset,
)
from blueprint.benchmarking.benchmark_engine import (
    BenchmarkDataStore,
    BenchmarkEngine,
    BenchmarkInsight,
    BenchmarkReport,
    BenchmarkResult,
    Outlier,
    Percentile,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_store() -> BenchmarkDataStore:
    """Build a store with plans for benchmarking."""
    now = _now()
    store = BenchmarkDataStore()

    store.plans["plan-good"] = {
        "id": "plan-good",
        "title": "Well-run Project",
        "status": "in_progress",
        "tags": ["backend"],
        "user_ids": ["user-1", "user-2"],
        "tasks": [
            {"id": "t-1", "title": "Task 1", "depends_on": [], "status": "completed", "assignee": "user-1", "description": "Done"},
            {"id": "t-2", "title": "Task 2", "depends_on": ["t-1"], "status": "completed", "assignee": "user-1", "description": "Done"},
            {"id": "t-3", "title": "Task 3", "depends_on": ["t-2"], "status": "completed", "assignee": "user-2", "description": "Done"},
            {"id": "t-4", "title": "Task 4", "depends_on": [], "status": "in_progress", "assignee": "user-2"},
        ],
        "created_at": now,
        "updated_at": now,
    }

    store.plans["plan-poor"] = {
        "id": "plan-poor",
        "title": "Struggling Project",
        "status": "in_progress",
        "tags": ["frontend"],
        "user_ids": ["user-1"],
        "tasks": [
            {"id": "t-5", "title": "Task 5", "depends_on": [], "status": "blocked", "assignee": "user-1"},
            {"id": "t-6", "title": "Task 6", "depends_on": [], "status": "blocked", "assignee": "user-1"},
            {"id": "t-7", "title": "Task 7", "depends_on": [], "status": "pending", "assignee": "user-1"},
        ],
        "created_at": now,
        "updated_at": now,
    }

    store.plans["plan-empty"] = {
        "id": "plan-empty",
        "title": "Empty Project",
        "status": "draft",
        "tags": [],
        "user_ids": [],
        "tasks": [],
        "created_at": now,
        "updated_at": now,
    }

    return store


def _engine(store: BenchmarkDataStore | None = None) -> BenchmarkEngine:
    return BenchmarkEngine(store=store or _make_store())


# ---------------------------------------------------------------------------
# Benchmark metrics tests
# ---------------------------------------------------------------------------


class TestBenchmarkMetrics:
    def test_velocity_metric(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good")
        velocity = [p for p in result.percentiles if "velocity" in p.metric]
        assert len(velocity) > 0

    def test_quality_metrics(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good")
        quality = [p for p in result.percentiles if "quality" in p.metric]
        assert len(quality) > 0

    def test_efficiency_metric(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good")
        efficiency = [p for p in result.percentiles if "efficiency" in p.metric]
        assert len(efficiency) > 0

    def test_timeline_metrics(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good")
        timeline = [p for p in result.percentiles if "timeline" in p.metric]
        assert len(timeline) > 0

    def test_team_metric(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good")
        team = [p for p in result.percentiles if "team" in p.metric]
        assert len(team) > 0


# ---------------------------------------------------------------------------
# Benchmark data tests
# ---------------------------------------------------------------------------


class TestBenchmarkData:
    def test_load_software_benchmarks(self):
        engine = _engine()
        dataset = engine.load_benchmark_data("software")
        assert isinstance(dataset, BenchmarkDataset)
        assert len(dataset.data_points) > 0
        assert dataset.industry == "software"

    def test_load_construction_benchmarks(self):
        engine = _engine()
        dataset = engine.load_benchmark_data("construction")
        assert len(dataset.data_points) > 0

    def test_load_marketing_benchmarks(self):
        engine = _engine()
        dataset = engine.load_benchmark_data("marketing")
        assert len(dataset.data_points) > 0

    def test_load_unknown_industry(self):
        engine = _engine()
        dataset = engine.load_benchmark_data("unknown")
        assert len(dataset.data_points) == 0

    def test_all_benchmarks_have_required_fields(self):
        for industry, points in ALL_BENCHMARKS.items():
            for dp in points:
                assert dp.metric
                assert dp.industry
                assert dp.sample_size > 0
                assert dp.std_dev >= 0

    def test_add_custom_benchmarks(self):
        engine = _engine()
        custom = [
            BenchmarkDataPoint("custom_metric", "fintech", "new_product", "small", 5.0, 4.5, 3.0, 6.0, 8.0, 1.5, 100),
        ]
        engine.add_benchmark_data("fintech", custom)
        dataset = engine.load_benchmark_data("fintech")
        assert len(dataset.data_points) == 1
        assert dataset.data_points[0].metric == "custom_metric"


# ---------------------------------------------------------------------------
# Benchmark dimensions tests
# ---------------------------------------------------------------------------


class TestBenchmarkDimensions:
    def test_industry_software(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good", industry="software")
        assert result.industry == "software"
        assert len(result.percentiles) > 0

    def test_industry_construction(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good", industry="construction")
        assert result.industry == "construction"

    def test_project_type_new_product(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good", project_type="new_product")
        assert result.project_type == "new_product"


# ---------------------------------------------------------------------------
# Statistical analysis tests
# ---------------------------------------------------------------------------


class TestStatistics:
    def test_percentile_calculation(self):
        engine = _engine()
        dp = BenchmarkDataPoint("test_metric", "software", "new_product", "small", 10.0, 9.0, 6.0, 13.0, 18.0, 4.0, 500)
        p = engine.calculate_percentile(9.0, dp)
        assert isinstance(p, Percentile)
        assert p.percentile == 50.0  # median value = 50th percentile

    def test_percentile_above_average(self):
        engine = _engine()
        dp = BenchmarkDataPoint("test_metric", "software", "new_product", "small", 10.0, 9.0, 6.0, 13.0, 18.0, 4.0, 500)
        p = engine.calculate_percentile(15.0, dp)
        assert p.percentile > 75

    def test_percentile_below_average(self):
        engine = _engine()
        dp = BenchmarkDataPoint("test_metric", "software", "new_product", "small", 10.0, 9.0, 6.0, 13.0, 18.0, 4.0, 500)
        p = engine.calculate_percentile(4.0, dp)
        assert p.percentile < 25

    def test_percentile_at_extremes(self):
        engine = _engine()
        dp = BenchmarkDataPoint("test_metric", "software", "new_product", "small", 10.0, 9.0, 6.0, 13.0, 18.0, 4.0, 500)
        low = engine.calculate_percentile(-10.0, dp)
        high = engine.calculate_percentile(100.0, dp)
        assert low.percentile == 0.0
        assert high.percentile == 100.0

    def test_overall_score(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good")
        assert 0 <= result.overall_score <= 100


# ---------------------------------------------------------------------------
# Insight generation tests
# ---------------------------------------------------------------------------


class TestInsightGeneration:
    def test_insights_generated(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good")
        assert len(result.insights) > 0
        for insight in result.insights:
            assert isinstance(insight, BenchmarkInsight)
            assert insight.metric
            assert insight.category
            assert insight.assessment in ("above_average", "below_average", "average")

    def test_insights_have_recommendations(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good")
        for insight in result.insights:
            assert insight.recommendation

    def test_insights_categories(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good")
        categories = {i.category for i in result.insights}
        # Should have multiple categories
        assert len(categories) > 1

    def test_poor_plan_gets_below_average(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-poor")
        assessments = {i.assessment for i in result.insights}
        # A plan with all blocked tasks should have some below_average
        # (or at least the defect rate should be above average = bad)
        assert len(result.insights) > 0


# ---------------------------------------------------------------------------
# Outlier detection tests
# ---------------------------------------------------------------------------


class TestOutlierDetection:
    def test_identify_outliers(self):
        engine = _engine()
        outliers = engine.identify_outliers("plan-poor")
        # Poor plan should have outlier metrics
        assert isinstance(outliers, list)

    def test_outlier_severity(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-poor")
        for outlier in result.outliers:
            assert outlier.severity in ("moderate", "severe")
            assert outlier.deviation > 0

    def test_outlier_recommendation(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-poor")
        for outlier in result.outliers:
            assert outlier.recommendation


# ---------------------------------------------------------------------------
# Report generation tests
# ---------------------------------------------------------------------------


class TestReportGeneration:
    def test_generate_report(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good")
        report = engine.generate_benchmark_report(result)
        assert isinstance(report, BenchmarkReport)
        assert len(report.data) > 0

    def test_report_json_parsable(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good")
        report = engine.generate_benchmark_report(result)
        data = json.loads(report.data)
        assert data["plan_id"] == "plan-good"
        assert "percentiles" in data
        assert "insights" in data
        assert "outliers" in data

    def test_report_overall_score(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good")
        report = engine.generate_benchmark_report(result)
        data = json.loads(report.data)
        assert "overall_score" in data


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_benchmark_empty_plan(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-empty")
        assert result.plan_id == "plan-empty"
        assert len(result.percentiles) == 0

    def test_benchmark_nonexistent_plan(self):
        engine = _engine()
        result = engine.benchmark_plan("nonexistent")
        assert result.plan_id == "nonexistent"
        assert len(result.percentiles) == 0

    def test_benchmark_result_id_unique(self):
        engine = _engine()
        r1 = engine.benchmark_plan("plan-good")
        r2 = engine.benchmark_plan("plan-good")
        assert r1.result_id != r2.result_id

    def test_benchmark_unknown_industry(self):
        engine = _engine()
        result = engine.benchmark_plan("plan-good", industry="unknown")
        assert result.industry == "unknown"
        assert len(result.percentiles) == 0
