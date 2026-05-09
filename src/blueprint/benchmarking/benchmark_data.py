"""Benchmark data definitions and industry standard datasets.

Provides pre-defined benchmark data for various industries,
project types, and team sizes for comparison with plan metrics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class BenchmarkDataPoint:
    """A single benchmark data point."""

    metric: str
    industry: str
    project_type: str
    team_size: str
    mean: float
    median: float
    p25: float
    p75: float
    p90: float
    std_dev: float
    sample_size: int


@dataclass(frozen=True, slots=True)
class BenchmarkDataset:
    """A collection of benchmark data for an industry."""

    industry: str
    data_points: list[BenchmarkDataPoint] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Pre-defined benchmark data (representative industry averages)
# ---------------------------------------------------------------------------

_SOFTWARE_BENCHMARKS = [
    BenchmarkDataPoint("velocity_tasks_per_week", "software", "new_product", "small", 8.0, 7.0, 5.0, 10.0, 14.0, 3.5, 500),
    BenchmarkDataPoint("velocity_tasks_per_week", "software", "new_product", "medium", 15.0, 14.0, 10.0, 18.0, 25.0, 5.0, 300),
    BenchmarkDataPoint("velocity_tasks_per_week", "software", "new_product", "large", 25.0, 23.0, 18.0, 30.0, 40.0, 8.0, 200),
    BenchmarkDataPoint("velocity_tasks_per_week", "software", "maintenance", "small", 12.0, 11.0, 8.0, 15.0, 20.0, 4.0, 400),
    BenchmarkDataPoint("quality_defect_rate", "software", "new_product", "small", 0.15, 0.12, 0.08, 0.20, 0.30, 0.08, 500),
    BenchmarkDataPoint("quality_defect_rate", "software", "new_product", "medium", 0.12, 0.10, 0.06, 0.16, 0.25, 0.07, 300),
    BenchmarkDataPoint("quality_defect_rate", "software", "maintenance", "small", 0.08, 0.06, 0.03, 0.10, 0.15, 0.05, 400),
    BenchmarkDataPoint("quality_on_time_delivery", "software", "new_product", "small", 0.65, 0.68, 0.50, 0.80, 0.90, 0.15, 500),
    BenchmarkDataPoint("quality_on_time_delivery", "software", "new_product", "medium", 0.70, 0.72, 0.55, 0.85, 0.92, 0.14, 300),
    BenchmarkDataPoint("efficiency_resource_utilization", "software", "new_product", "small", 0.72, 0.75, 0.60, 0.85, 0.92, 0.12, 500),
    BenchmarkDataPoint("efficiency_resource_utilization", "software", "new_product", "medium", 0.68, 0.70, 0.55, 0.80, 0.88, 0.13, 300),
    BenchmarkDataPoint("timeline_schedule_performance", "software", "new_product", "small", 0.85, 0.88, 0.70, 0.95, 1.0, 0.12, 500),
    BenchmarkDataPoint("timeline_duration_ratio", "software", "new_product", "small", 1.2, 1.15, 0.95, 1.4, 1.8, 0.3, 500),
    BenchmarkDataPoint("team_collaboration_score", "software", "new_product", "small", 0.70, 0.72, 0.55, 0.82, 0.90, 0.14, 500),
    BenchmarkDataPoint("team_collaboration_score", "software", "new_product", "medium", 0.65, 0.68, 0.50, 0.78, 0.88, 0.15, 300),
]

_CONSTRUCTION_BENCHMARKS = [
    BenchmarkDataPoint("velocity_tasks_per_week", "construction", "new_product", "small", 5.0, 4.5, 3.0, 6.5, 9.0, 2.5, 200),
    BenchmarkDataPoint("velocity_tasks_per_week", "construction", "new_product", "medium", 10.0, 9.0, 6.5, 12.5, 16.0, 4.0, 150),
    BenchmarkDataPoint("quality_defect_rate", "construction", "new_product", "small", 0.10, 0.08, 0.05, 0.14, 0.20, 0.06, 200),
    BenchmarkDataPoint("quality_on_time_delivery", "construction", "new_product", "small", 0.55, 0.58, 0.40, 0.70, 0.82, 0.18, 200),
    BenchmarkDataPoint("timeline_duration_ratio", "construction", "new_product", "small", 1.3, 1.25, 1.0, 1.5, 2.0, 0.35, 200),
]

_MARKETING_BENCHMARKS = [
    BenchmarkDataPoint("velocity_tasks_per_week", "marketing", "new_product", "small", 10.0, 9.0, 6.0, 13.0, 18.0, 4.5, 250),
    BenchmarkDataPoint("quality_defect_rate", "marketing", "new_product", "small", 0.05, 0.04, 0.02, 0.07, 0.10, 0.03, 250),
    BenchmarkDataPoint("quality_on_time_delivery", "marketing", "new_product", "small", 0.75, 0.78, 0.60, 0.88, 0.95, 0.12, 250),
]

ALL_BENCHMARKS: dict[str, list[BenchmarkDataPoint]] = {
    "software": _SOFTWARE_BENCHMARKS,
    "construction": _CONSTRUCTION_BENCHMARKS,
    "marketing": _MARKETING_BENCHMARKS,
}

DEFAULT_DATASET = BenchmarkDataset(
    industry="software",
    data_points=_SOFTWARE_BENCHMARKS,
    metadata={"source": "aggregated_blueprint_plans", "updated": "2025-01"},
)
