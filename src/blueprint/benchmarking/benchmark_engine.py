"""Benchmarking system comparing plans against industry standards.

Provides benchmark analysis across velocity, quality, efficiency,
timeline, and team dimensions with percentile calculations,
outlier detection, and insight generation.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from blueprint.benchmarking.benchmark_data import (
    ALL_BENCHMARKS,
    BenchmarkDataPoint,
    BenchmarkDataset,
)


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Percentile:
    """A plan metric's percentile within a benchmark distribution."""

    metric: str
    plan_value: float
    percentile: float
    benchmark_mean: float
    benchmark_median: float
    assessment: str = ""


@dataclass(frozen=True, slots=True)
class Outlier:
    """An outlier metric identified during benchmarking."""

    metric: str
    plan_value: float
    benchmark_mean: float
    deviation: float
    severity: str = "moderate"
    recommendation: str = ""


@dataclass(frozen=True, slots=True)
class BenchmarkInsight:
    """An insight generated from benchmark analysis."""

    metric: str
    category: str
    assessment: str
    recommendation: str
    plan_value: float = 0.0
    benchmark_value: float = 0.0


@dataclass(frozen=True, slots=True)
class BenchmarkResult:
    """Complete benchmark analysis result."""

    result_id: str
    plan_id: str
    industry: str
    project_type: str
    percentiles: list[Percentile] = field(default_factory=list)
    outliers: list[Outlier] = field(default_factory=list)
    insights: list[BenchmarkInsight] = field(default_factory=list)
    overall_score: float = 0.0
    created_at: str = ""


@dataclass(frozen=True, slots=True)
class BenchmarkReport:
    """Formatted benchmark report."""

    report_id: str
    result_id: str
    data: bytes = b""
    created_at: str = ""


# ---------------------------------------------------------------------------
# Data store
# ---------------------------------------------------------------------------


@dataclass
class BenchmarkDataStore:
    """In-memory store providing plan data for benchmarking."""

    plans: dict[str, dict[str, Any]] = field(default_factory=dict)
    tasks: dict[str, dict[str, Any]] = field(default_factory=dict)

    def get_plan(self, plan_id: str) -> dict[str, Any] | None:
        return self.plans.get(plan_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_id(prefix: str = "bmk") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _classify_team_size(count: int) -> str:
    if count <= 5:
        return "small"
    elif count <= 15:
        return "medium"
    else:
        return "large"


def _calculate_percentile(value: float, dp: BenchmarkDataPoint) -> float:
    """Estimate percentile of a value within a benchmark distribution.

    Uses linear interpolation between known percentile points (p25, median, p75, p90).
    """
    points = [
        (0.0, dp.mean - 2 * dp.std_dev),
        (0.25, dp.p25),
        (0.50, dp.median),
        (0.75, dp.p75),
        (0.90, dp.p90),
        (1.0, dp.mean + 2 * dp.std_dev),
    ]

    if value <= points[0][1]:
        return 0.0
    if value >= points[-1][1]:
        return 100.0

    for i in range(len(points) - 1):
        p1_pct, p1_val = points[i]
        p2_pct, p2_val = points[i + 1]
        if p1_val <= value <= p2_val:
            if p2_val == p1_val:
                return p1_pct * 100
            ratio = (value - p1_val) / (p2_val - p1_val)
            return round((p1_pct + ratio * (p2_pct - p1_pct)) * 100, 1)

    return 50.0


def _assess_percentile(percentile: float, metric: str) -> str:
    """Assess a percentile ranking."""
    # For defect rate, lower is better
    if "defect" in metric or "duration_ratio" in metric:
        if percentile < 25:
            return "above_average"
        elif percentile > 75:
            return "below_average"
        else:
            return "average"
    else:
        if percentile > 75:
            return "above_average"
        elif percentile < 25:
            return "below_average"
        else:
            return "average"


def _extract_plan_metrics(plan: dict[str, Any]) -> dict[str, float]:
    """Extract benchmark-relevant metrics from a plan."""
    tasks = plan.get("tasks", [])
    total = len(tasks)
    if total == 0:
        return {}

    completed = sum(1 for t in tasks if t.get("status") == "completed")
    blocked = sum(1 for t in tasks if t.get("status") == "blocked")
    deps = sum(len(t.get("depends_on", [])) for t in tasks)
    assignees = set()
    for t in tasks:
        if t.get("assignee"):
            assignees.add(t["assignee"])
    assignees.update(plan.get("user_ids", []))

    team_size = max(len(assignees), 1)
    completion_rate = completed / total if total > 0 else 0.0
    defect_rate = blocked / total if total > 0 else 0.0

    return {
        "velocity_tasks_per_week": total / max(1, team_size) * 2,  # Estimate
        "quality_defect_rate": round(defect_rate, 3),
        "quality_on_time_delivery": round(completion_rate, 3),
        "efficiency_resource_utilization": round(total / (team_size * 5), 3),
        "timeline_schedule_performance": round(completion_rate * 0.9 + 0.1, 3),
        "timeline_duration_ratio": round(1.0 + (1 - completion_rate) * 0.5, 3),
        "team_collaboration_score": round(min(deps / max(total, 1), 1.0), 3),
    }


# ---------------------------------------------------------------------------
# BenchmarkEngine
# ---------------------------------------------------------------------------


class BenchmarkEngine:
    """Compares plans against industry benchmarks and best practices.

    Provides benchmark analysis across velocity, quality, efficiency,
    timeline, and team dimensions with percentile calculations,
    outlier detection, and insight generation.
    """

    def __init__(self, store: BenchmarkDataStore | None = None) -> None:
        self._store = store or BenchmarkDataStore()
        self._custom_benchmarks: dict[str, list[BenchmarkDataPoint]] = {}

    def benchmark_plan(
        self,
        plan_id: str,
        industry: str = "software",
        project_type: str = "new_product",
    ) -> BenchmarkResult:
        """Benchmark a plan against industry standards.

        Args:
            plan_id: ID of the plan to benchmark.
            industry: Industry to compare against.
            project_type: Type of project.
        """
        plan = self._store.get_plan(plan_id)
        if plan is None:
            return BenchmarkResult(
                result_id=_generate_id(),
                plan_id=plan_id,
                industry=industry,
                project_type=project_type,
                created_at=_now_iso(),
            )

        metrics = _extract_plan_metrics(plan)
        dataset = self.load_benchmark_data(industry)
        team_size = _classify_team_size(len(plan.get("user_ids", [])))

        # Filter to matching data points
        relevant = [
            dp for dp in dataset.data_points
            if dp.project_type == project_type
            and (dp.team_size == team_size or dp.team_size == "small")
        ]

        percentiles: list[Percentile] = []
        outliers: list[Outlier] = []
        insights: list[BenchmarkInsight] = []

        for dp in relevant:
            if dp.metric not in metrics:
                continue
            plan_value = metrics[dp.metric]
            pct = _calculate_percentile(plan_value, dp)
            assessment = _assess_percentile(pct, dp.metric)

            percentiles.append(Percentile(
                metric=dp.metric,
                plan_value=plan_value,
                percentile=pct,
                benchmark_mean=dp.mean,
                benchmark_median=dp.median,
                assessment=assessment,
            ))

            # Detect outliers (beyond 1.5 std deviations)
            deviation = abs(plan_value - dp.mean) / max(dp.std_dev, 0.001)
            if deviation > 1.5:
                severity = "severe" if deviation > 2.5 else "moderate"
                outliers.append(Outlier(
                    metric=dp.metric,
                    plan_value=plan_value,
                    benchmark_mean=dp.mean,
                    deviation=round(deviation, 2),
                    severity=severity,
                    recommendation=self._generate_recommendation(dp.metric, assessment),
                ))

            # Generate insights
            insights.append(BenchmarkInsight(
                metric=dp.metric,
                category=dp.metric.split("_")[0],
                assessment=assessment,
                recommendation=self._generate_recommendation(dp.metric, assessment),
                plan_value=plan_value,
                benchmark_value=dp.mean,
            ))

        # Calculate overall score (average of non-inverted percentiles)
        if percentiles:
            score_values = []
            for p in percentiles:
                if "defect" in p.metric or "duration_ratio" in p.metric:
                    score_values.append(100 - p.percentile)
                else:
                    score_values.append(p.percentile)
            overall = round(sum(score_values) / len(score_values), 1)
        else:
            overall = 0.0

        return BenchmarkResult(
            result_id=_generate_id(),
            plan_id=plan_id,
            industry=industry,
            project_type=project_type,
            percentiles=percentiles,
            outliers=outliers,
            insights=insights,
            overall_score=overall,
            created_at=_now_iso(),
        )

    def load_benchmark_data(self, industry: str) -> BenchmarkDataset:
        """Load benchmark data for an industry."""
        # Check custom benchmarks first
        if industry in self._custom_benchmarks:
            return BenchmarkDataset(
                industry=industry,
                data_points=self._custom_benchmarks[industry],
            )

        data_points = ALL_BENCHMARKS.get(industry, [])
        return BenchmarkDataset(
            industry=industry,
            data_points=data_points,
            metadata={"source": "predefined"},
        )

    def add_benchmark_data(
        self,
        industry: str,
        data_points: list[BenchmarkDataPoint],
    ) -> None:
        """Add custom benchmark data for an industry."""
        existing = self._custom_benchmarks.get(industry, [])
        existing.extend(data_points)
        self._custom_benchmarks[industry] = existing

    def calculate_percentile(
        self,
        plan_metric: float,
        benchmark: BenchmarkDataPoint,
    ) -> Percentile:
        """Calculate the percentile of a plan metric within a benchmark."""
        pct = _calculate_percentile(plan_metric, benchmark)
        assessment = _assess_percentile(pct, benchmark.metric)
        return Percentile(
            metric=benchmark.metric,
            plan_value=plan_metric,
            percentile=pct,
            benchmark_mean=benchmark.mean,
            benchmark_median=benchmark.median,
            assessment=assessment,
        )

    def identify_outliers(
        self,
        plan_id: str,
        industry: str = "software",
    ) -> list[Outlier]:
        """Identify outlier metrics in a plan."""
        result = self.benchmark_plan(plan_id, industry)
        return result.outliers

    def generate_benchmark_report(
        self,
        result: BenchmarkResult,
    ) -> BenchmarkReport:
        """Generate a formatted benchmark report."""
        data = {
            "result_id": result.result_id,
            "plan_id": result.plan_id,
            "industry": result.industry,
            "project_type": result.project_type,
            "overall_score": result.overall_score,
            "percentiles": [
                {
                    "metric": p.metric,
                    "plan_value": p.plan_value,
                    "percentile": p.percentile,
                    "benchmark_mean": p.benchmark_mean,
                    "assessment": p.assessment,
                }
                for p in result.percentiles
            ],
            "outliers": [
                {
                    "metric": o.metric,
                    "plan_value": o.plan_value,
                    "deviation": o.deviation,
                    "severity": o.severity,
                    "recommendation": o.recommendation,
                }
                for o in result.outliers
            ],
            "insights": [
                {
                    "metric": i.metric,
                    "category": i.category,
                    "assessment": i.assessment,
                    "recommendation": i.recommendation,
                }
                for i in result.insights
            ],
        }
        return BenchmarkReport(
            report_id=_generate_id("rpt"),
            result_id=result.result_id,
            data=json.dumps(data, indent=2).encode("utf-8"),
            created_at=_now_iso(),
        )

    def _generate_recommendation(self, metric: str, assessment: str) -> str:
        """Generate a recommendation based on metric and assessment."""
        recommendations = {
            ("velocity_tasks_per_week", "below_average"): "Consider reducing task scope or adding team members to improve throughput",
            ("velocity_tasks_per_week", "above_average"): "Velocity is strong; ensure quality is not being sacrificed",
            ("quality_defect_rate", "below_average"): "High defect rate; invest in code review and testing",
            ("quality_defect_rate", "above_average"): "Low defect rate; quality practices are effective",
            ("quality_on_time_delivery", "below_average"): "Improve estimation accuracy and reduce scope creep",
            ("quality_on_time_delivery", "above_average"): "On-time delivery is strong; maintain current practices",
            ("efficiency_resource_utilization", "below_average"): "Resource utilization is low; review task distribution",
            ("efficiency_resource_utilization", "above_average"): "High utilization; monitor for burnout risk",
            ("timeline_schedule_performance", "below_average"): "Schedule is falling behind; consider re-prioritization",
            ("timeline_duration_ratio", "below_average"): "Projects taking longer than estimated; improve planning",
            ("team_collaboration_score", "below_average"): "Low collaboration; increase cross-functional dependencies",
        }
        return recommendations.get((metric, assessment), "Performance is within normal range")
