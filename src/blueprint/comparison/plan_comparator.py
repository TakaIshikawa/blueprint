"""Plan comparison system with side-by-side analysis.

Compares multiple plans across dimensions including overview, tasks,
timeline, resources, dependencies, risks, and quality.  Generates
comparison reports with highlighting and overlay visualizations.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ComparisonDimension(str, Enum):
    """Dimensions for comparing plans."""

    OVERVIEW = "overview"
    TASKS = "tasks"
    TIMELINE = "timeline"
    RESOURCES = "resources"
    DEPENDENCIES = "dependencies"
    RISKS = "risks"
    QUALITY = "quality"


class DifferenceType(str, Enum):
    """Type of difference between plans."""

    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    UNCHANGED = "unchanged"


class ComparisonUseCase(str, Enum):
    """Pre-defined comparison use cases."""

    BASELINE_VS_ACTUAL = "baseline_vs_actual"
    TEMPLATE_VARIATIONS = "template_variations"
    TEAM_PERFORMANCE = "team_performance"
    VERSION_COMPARISON = "version_comparison"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Difference:
    """A single difference between plans."""

    field_name: str
    dimension: ComparisonDimension
    diff_type: DifferenceType
    plan_values: dict[str, Any] = field(default_factory=dict)
    description: str = ""


@dataclass(frozen=True, slots=True)
class MetricComparison:
    """Comparison of a single metric across plans."""

    metric_name: str
    plan_values: dict[str, float] = field(default_factory=dict)
    best_plan_id: str = ""
    worst_plan_id: str = ""
    spread: float = 0.0
    average: float = 0.0


@dataclass(frozen=True, slots=True)
class TimelineComparison:
    """Comparison of timelines across plans."""

    plan_start_dates: dict[str, str] = field(default_factory=dict)
    plan_end_dates: dict[str, str] = field(default_factory=dict)
    plan_durations_days: dict[str, int] = field(default_factory=dict)
    overlap_periods: list[dict[str, Any]] = field(default_factory=list)
    critical_path_lengths: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ResourceComparison:
    """Comparison of resource usage across plans."""

    plan_team_sizes: dict[str, int] = field(default_factory=dict)
    plan_assignees: dict[str, list[str]] = field(default_factory=dict)
    shared_assignees: list[str] = field(default_factory=list)
    unique_assignees: dict[str, list[str]] = field(default_factory=dict)
    utilization_scores: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class TaskOverlap:
    """Task overlap analysis between plans."""

    shared_tasks: list[str] = field(default_factory=list)
    unique_tasks: dict[str, list[str]] = field(default_factory=dict)
    shared_count: int = 0
    total_unique_count: int = 0
    overlap_ratio: float = 0.0


@dataclass(frozen=True, slots=True)
class ComparisonResult:
    """Full comparison result across all dimensions."""

    comparison_id: str
    plan_ids: list[str] = field(default_factory=list)
    created_at: str = ""
    dimensions_compared: list[str] = field(default_factory=list)
    differences: list[Difference] = field(default_factory=list)
    metrics: list[MetricComparison] = field(default_factory=list)
    timeline: TimelineComparison | None = None
    resources: ResourceComparison | None = None
    task_overlap: TaskOverlap | None = None
    summary: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ComparisonReport:
    """Formatted comparison report."""

    report_id: str
    comparison_id: str
    format: str
    data: bytes
    created_at: str
    plan_count: int = 0


# ---------------------------------------------------------------------------
# Data store
# ---------------------------------------------------------------------------


@dataclass
class ComparisonDataStore:
    """In-memory store providing plan data for comparison operations."""

    plans: dict[str, dict[str, Any]] = field(default_factory=dict)
    tasks: dict[str, dict[str, Any]] = field(default_factory=dict)

    def get_plan(self, plan_id: str) -> dict[str, Any] | None:
        return self.plans.get(plan_id)

    def get_plans(self, plan_ids: list[str]) -> dict[str, dict[str, Any]]:
        return {pid: self.plans[pid] for pid in plan_ids if pid in self.plans}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_id(prefix: str = "cmp") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            pass
    return datetime.min.replace(tzinfo=timezone.utc)


def _get_assignees(plan: dict[str, Any]) -> list[str]:
    """Extract all assignees from a plan's tasks."""
    assignees: set[str] = set()
    assignees.update(plan.get("user_ids", []))
    for task in plan.get("tasks", []):
        if task.get("assignee"):
            assignees.add(task["assignee"])
    return sorted(assignees)


def _get_task_titles(plan: dict[str, Any]) -> set[str]:
    """Extract task titles from a plan."""
    return {t["title"] for t in plan.get("tasks", [])}


def _count_dependencies(plan: dict[str, Any]) -> int:
    """Count total dependencies in a plan."""
    return sum(len(t.get("depends_on", [])) for t in plan.get("tasks", []))


# ---------------------------------------------------------------------------
# PlanComparator
# ---------------------------------------------------------------------------


class PlanComparator:
    """Compares multiple plans across dimensions for side-by-side analysis.

    Supports overview, task, timeline, resource, dependency, risk, and
    quality comparisons with difference highlighting and metric analysis.
    """

    def __init__(self, store: ComparisonDataStore | None = None) -> None:
        self._store = store or ComparisonDataStore()

    def compare_plans(
        self,
        plan_ids: list[str],
        dimensions: list[ComparisonDimension] | None = None,
    ) -> ComparisonResult:
        """Compare plans across all or selected dimensions.

        Args:
            plan_ids: IDs of plans to compare.
            dimensions: Specific dimensions to compare (default: all).
        """
        dims = dimensions or list(ComparisonDimension)
        plans = self._store.get_plans(plan_ids)

        differences: list[Difference] = []
        metrics: list[MetricComparison] = []
        timeline = None
        resources = None
        task_overlap = None
        summary: dict[str, Any] = {"plan_count": len(plans)}

        if ComparisonDimension.OVERVIEW in dims:
            diffs, mets = self._compare_overview(plans)
            differences.extend(diffs)
            metrics.extend(mets)

        if ComparisonDimension.TASKS in dims:
            diffs, overlap = self._compare_tasks(plans)
            differences.extend(diffs)
            task_overlap = overlap

        if ComparisonDimension.TIMELINE in dims:
            timeline = self._compare_timelines(plans)

        if ComparisonDimension.RESOURCES in dims:
            resources = self._compare_resources(plans)

        if ComparisonDimension.DEPENDENCIES in dims:
            diffs, mets = self._compare_dependencies(plans)
            differences.extend(diffs)
            metrics.extend(mets)

        if ComparisonDimension.RISKS in dims:
            mets = self._compare_risks(plans)
            metrics.extend(mets)

        if ComparisonDimension.QUALITY in dims:
            mets = self._compare_quality(plans)
            metrics.extend(mets)

        # Build summary
        summary["total_differences"] = len(differences)
        summary["dimensions_compared"] = [d.value for d in dims]
        summary["common_tasks"] = task_overlap.shared_count if task_overlap else 0

        return ComparisonResult(
            comparison_id=_generate_id(),
            plan_ids=list(plans.keys()),
            created_at=_now_iso(),
            dimensions_compared=[d.value for d in dims],
            differences=differences,
            metrics=metrics,
            timeline=timeline,
            resources=resources,
            task_overlap=task_overlap,
            summary=summary,
        )

    def compare_metrics(
        self,
        plan_ids: list[str],
        metric_names: list[str] | None = None,
    ) -> list[MetricComparison]:
        """Compare specific metrics across plans."""
        plans = self._store.get_plans(plan_ids)
        all_metrics: list[MetricComparison] = []
        _, overview_metrics = self._compare_overview(plans)
        all_metrics.extend(overview_metrics)
        all_metrics.extend(self._compare_risks(plans))
        all_metrics.extend(self._compare_quality(plans))

        if metric_names:
            all_metrics = [m for m in all_metrics if m.metric_name in metric_names]

        return all_metrics

    def compare_timelines(self, plan_ids: list[str]) -> TimelineComparison:
        """Compare timelines specifically."""
        plans = self._store.get_plans(plan_ids)
        return self._compare_timelines(plans)

    def compare_resources(self, plan_ids: list[str]) -> ResourceComparison:
        """Compare resource allocation specifically."""
        plans = self._store.get_plans(plan_ids)
        return self._compare_resources(plans)

    def generate_comparison_report(
        self,
        comparison: ComparisonResult,
        fmt: str = "json",
    ) -> ComparisonReport:
        """Generate a formatted report from comparison results."""
        if fmt == "json":
            data = self._report_json(comparison)
        else:
            data = self._report_json(comparison)

        return ComparisonReport(
            report_id=_generate_id("rpt"),
            comparison_id=comparison.comparison_id,
            format=fmt,
            data=data,
            created_at=_now_iso(),
            plan_count=len(comparison.plan_ids),
        )

    def export_comparison(
        self,
        comparison: ComparisonResult,
        fmt: str = "json",
    ) -> bytes:
        """Export comparison results as bytes."""
        report = self.generate_comparison_report(comparison, fmt)
        return report.data

    # -- private comparison methods ----------------------------------------

    def _compare_overview(
        self,
        plans: dict[str, dict[str, Any]],
    ) -> tuple[list[Difference], list[MetricComparison]]:
        diffs: list[Difference] = []
        metrics: list[MetricComparison] = []

        # Task count
        task_counts = {
            pid: len(p.get("tasks", [])) for pid, p in plans.items()
        }
        if len(set(task_counts.values())) > 1:
            diffs.append(Difference(
                field_name="task_count",
                dimension=ComparisonDimension.OVERVIEW,
                diff_type=DifferenceType.MODIFIED,
                plan_values=task_counts,
                description="Plans have different task counts",
            ))
        metrics.append(_make_metric("task_count", {k: float(v) for k, v in task_counts.items()}))

        # Status
        statuses = {pid: p.get("status", "unknown") for pid, p in plans.items()}
        if len(set(statuses.values())) > 1:
            diffs.append(Difference(
                field_name="status",
                dimension=ComparisonDimension.OVERVIEW,
                diff_type=DifferenceType.MODIFIED,
                plan_values=statuses,
                description="Plans have different statuses",
            ))

        # Completion percentage
        completion: dict[str, float] = {}
        for pid, p in plans.items():
            tasks = p.get("tasks", [])
            if tasks:
                done = sum(1 for t in tasks if t.get("status") == "completed")
                completion[pid] = round(done / len(tasks) * 100, 1)
            else:
                completion[pid] = 0.0
        metrics.append(_make_metric("completion_pct", completion))

        return diffs, metrics

    def _compare_tasks(
        self,
        plans: dict[str, dict[str, Any]],
    ) -> tuple[list[Difference], TaskOverlap]:
        diffs: list[Difference] = []

        # Collect task titles per plan
        plan_tasks: dict[str, set[str]] = {
            pid: _get_task_titles(p) for pid, p in plans.items()
        }

        # Find shared and unique
        all_titles = set()
        for titles in plan_tasks.values():
            all_titles.update(titles)

        shared = all_titles.copy()
        for titles in plan_tasks.values():
            shared &= titles

        unique: dict[str, list[str]] = {}
        for pid, titles in plan_tasks.items():
            unique[pid] = sorted(titles - shared)

        for pid, uniq in unique.items():
            for title in uniq:
                diffs.append(Difference(
                    field_name=f"task:{title}",
                    dimension=ComparisonDimension.TASKS,
                    diff_type=DifferenceType.ADDED,
                    plan_values={pid: title},
                    description=f"Task '{title}' unique to {pid}",
                ))

        total_unique = sum(len(v) for v in unique.values())
        overlap_ratio = len(shared) / len(all_titles) if all_titles else 0.0

        overlap = TaskOverlap(
            shared_tasks=sorted(shared),
            unique_tasks=unique,
            shared_count=len(shared),
            total_unique_count=total_unique,
            overlap_ratio=round(overlap_ratio, 3),
        )
        return diffs, overlap

    def _compare_timelines(
        self,
        plans: dict[str, dict[str, Any]],
    ) -> TimelineComparison:
        start_dates: dict[str, str] = {}
        end_dates: dict[str, str] = {}
        durations: dict[str, int] = {}
        crit_path: dict[str, int] = {}

        for pid, p in plans.items():
            created = p.get("created_at", "")
            start_dates[pid] = created
            # Estimate end date from latest task or plan updated_at
            end = p.get("updated_at", created)
            end_dates[pid] = end

            start_dt = _parse_dt(created)
            end_dt = _parse_dt(end)
            durations[pid] = max((end_dt - start_dt).days, 0)

            # Critical path = longest dependency chain
            tasks = p.get("tasks", [])
            crit_path[pid] = self._longest_chain(tasks)

        return TimelineComparison(
            plan_start_dates=start_dates,
            plan_end_dates=end_dates,
            plan_durations_days=durations,
            critical_path_lengths=crit_path,
        )

    def _compare_resources(
        self,
        plans: dict[str, dict[str, Any]],
    ) -> ResourceComparison:
        plan_assignees: dict[str, list[str]] = {}
        team_sizes: dict[str, int] = {}

        for pid, p in plans.items():
            assignees = _get_assignees(p)
            plan_assignees[pid] = assignees
            team_sizes[pid] = len(assignees)

        # Find shared and unique assignees
        all_assignees: set[str] = set()
        for a in plan_assignees.values():
            all_assignees.update(a)

        shared = all_assignees.copy()
        for a in plan_assignees.values():
            shared &= set(a)

        unique: dict[str, list[str]] = {}
        for pid, a in plan_assignees.items():
            unique[pid] = sorted(set(a) - shared)

        # Utilization = tasks per person
        utilization: dict[str, float] = {}
        for pid, p in plans.items():
            task_count = len(p.get("tasks", []))
            persons = max(team_sizes[pid], 1)
            utilization[pid] = round(task_count / persons, 2)

        return ResourceComparison(
            plan_team_sizes=team_sizes,
            plan_assignees=plan_assignees,
            shared_assignees=sorted(shared),
            unique_assignees=unique,
            utilization_scores=utilization,
        )

    def _compare_dependencies(
        self,
        plans: dict[str, dict[str, Any]],
    ) -> tuple[list[Difference], list[MetricComparison]]:
        diffs: list[Difference] = []
        dep_counts: dict[str, float] = {}

        for pid, p in plans.items():
            dep_counts[pid] = float(_count_dependencies(p))

        if len(set(dep_counts.values())) > 1:
            diffs.append(Difference(
                field_name="dependency_count",
                dimension=ComparisonDimension.DEPENDENCIES,
                diff_type=DifferenceType.MODIFIED,
                plan_values={k: int(v) for k, v in dep_counts.items()},
                description="Plans have different dependency counts",
            ))

        metrics = [_make_metric("dependency_count", dep_counts)]

        # Complexity = deps / tasks ratio
        complexity: dict[str, float] = {}
        for pid, p in plans.items():
            tasks = len(p.get("tasks", []))
            complexity[pid] = round(dep_counts[pid] / max(tasks, 1), 2)
        metrics.append(_make_metric("dependency_complexity", complexity))

        return diffs, metrics

    def _compare_risks(
        self,
        plans: dict[str, dict[str, Any]],
    ) -> list[MetricComparison]:
        metrics: list[MetricComparison] = []

        # Risk score: based on task count, dependencies, and status
        risk_scores: dict[str, float] = {}
        for pid, p in plans.items():
            tasks = p.get("tasks", [])
            deps = _count_dependencies(p)
            blocked = sum(1 for t in tasks if t.get("status") == "blocked")
            risk = round(deps * 0.3 + blocked * 2.0 + len(tasks) * 0.1, 2)
            risk_scores[pid] = risk

        metrics.append(_make_metric("risk_score", risk_scores))
        return metrics

    def _compare_quality(
        self,
        plans: dict[str, dict[str, Any]],
    ) -> list[MetricComparison]:
        metrics: list[MetricComparison] = []

        quality_scores: dict[str, float] = {}
        for pid, p in plans.items():
            tasks = p.get("tasks", [])
            total = len(tasks)
            if total == 0:
                quality_scores[pid] = 0.0
                continue
            completed = sum(1 for t in tasks if t.get("status") == "completed")
            has_desc = sum(1 for t in tasks if t.get("description"))
            quality = round((completed / total * 50) + (has_desc / total * 50), 1)
            quality_scores[pid] = quality

        metrics.append(_make_metric("quality_score", quality_scores))
        return metrics

    def _longest_chain(self, tasks: list[dict[str, Any]]) -> int:
        """Find the longest dependency chain length."""
        if not tasks:
            return 0
        task_map = {t["id"]: t for t in tasks}
        cache: dict[str, int] = {}

        def depth(tid: str) -> int:
            if tid in cache:
                return cache[tid]
            t = task_map.get(tid)
            if not t:
                return 0
            deps = t.get("depends_on", [])
            d = 1 + max((depth(d) for d in deps if d in task_map), default=0)
            cache[tid] = d
            return d

        return max((depth(t["id"]) for t in tasks), default=0)

    def _report_json(self, comparison: ComparisonResult) -> bytes:
        """Serialize comparison result to JSON."""
        data = {
            "comparison_id": comparison.comparison_id,
            "plan_ids": comparison.plan_ids,
            "created_at": comparison.created_at,
            "dimensions": comparison.dimensions_compared,
            "difference_count": len(comparison.differences),
            "differences": [
                {
                    "field": d.field_name,
                    "dimension": d.dimension.value,
                    "type": d.diff_type.value,
                    "values": d.plan_values,
                    "description": d.description,
                }
                for d in comparison.differences
            ],
            "metrics": [
                {
                    "name": m.metric_name,
                    "values": m.plan_values,
                    "best": m.best_plan_id,
                    "worst": m.worst_plan_id,
                    "spread": m.spread,
                    "average": m.average,
                }
                for m in comparison.metrics
            ],
            "summary": comparison.summary,
        }
        return json.dumps(data, indent=2, default=str).encode("utf-8")


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------


def _make_metric(name: str, values: dict[str, float]) -> MetricComparison:
    """Build a MetricComparison from a name and per-plan values."""
    if not values:
        return MetricComparison(metric_name=name)

    float_vals = {k: float(v) for k, v in values.items()}
    best = max(float_vals, key=lambda k: float_vals[k])
    worst = min(float_vals, key=lambda k: float_vals[k])
    vals = list(float_vals.values())
    spread = round(max(vals) - min(vals), 2)
    average = round(sum(vals) / len(vals), 2)

    return MetricComparison(
        metric_name=name,
        plan_values=float_vals,
        best_plan_id=best,
        worst_plan_id=worst,
        spread=spread,
        average=average,
    )
