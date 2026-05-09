"""AI-powered plan quality scoring and recommendations."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Mapping

# Scoring constants
MAX_SCORE = 100
COMPLETENESS_WEIGHT = 0.25
CLARITY_WEIGHT = 0.20
FEASIBILITY_WEIGHT = 0.25
RISK_WEIGHT = 0.15
DEPENDENCY_WEIGHT = 0.15

# Penalties and thresholds
MISSING_ESTIMATE_PENALTY = 5
VAGUE_DESCRIPTION_PENALTY = 3
VAGUE_DESCRIPTION_THRESHOLD = 50
CRITICAL_PATH_THRESHOLD = 0.80
OVERALLOCATION_THRESHOLD = 3  # tasks per person per day


@dataclass(frozen=True, slots=True)
class DimensionScore:
    """Score for a single quality dimension."""

    score: float  # 0-100
    max_score: float  # Maximum possible score
    issues: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "score": self.score,
            "max_score": self.max_score,
            "percentage": round((self.score / self.max_score * 100) if self.max_score > 0 else 0, 2),
            "issues": self.issues,
            "warnings": self.warnings,
        }


@dataclass(frozen=True, slots=True)
class DimensionScores:
    """Scores across all quality dimensions."""

    completeness: DimensionScore
    clarity: DimensionScore
    feasibility: DimensionScore
    risk_coverage: DimensionScore
    dependency_hygiene: DimensionScore

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "completeness": self.completeness.to_dict(),
            "clarity": self.clarity.to_dict(),
            "feasibility": self.feasibility.to_dict(),
            "risk_coverage": self.risk_coverage.to_dict(),
            "dependency_hygiene": self.dependency_hygiene.to_dict(),
        }


@dataclass(frozen=True, slots=True)
class Recommendation:
    """Actionable recommendation for plan improvement."""

    priority: str  # "high", "medium", "low"
    category: str  # dimension name
    message: str
    affected_items: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "priority": self.priority,
            "category": self.category,
            "message": self.message,
            "affected_items": self.affected_items,
        }


@dataclass(frozen=True, slots=True)
class BenchmarkReport:
    """Comparison to benchmarks from similar successful plans."""

    plan_score: float
    benchmark_avg: float
    benchmark_median: float
    percentile: float  # 0-100, where plan ranks among benchmarks
    better_than_percent: float  # percentage of benchmark plans this beats
    comparison: str  # "above", "at", "below"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "plan_score": self.plan_score,
            "benchmark_avg": self.benchmark_avg,
            "benchmark_median": self.benchmark_median,
            "percentile": self.percentile,
            "better_than_percent": self.better_than_percent,
            "comparison": self.comparison,
        }


@dataclass(frozen=True, slots=True)
class TrendPoint:
    """Single point in quality trend history."""

    version: int
    timestamp: datetime
    score: float
    change: float  # change from previous version

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "version": self.version,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "score": self.score,
            "change": self.change,
        }


@dataclass(frozen=True, slots=True)
class TrendAnalysis:
    """Quality score trend analysis over plan versions."""

    plan_id: str
    current_score: float
    trend_direction: str  # "improving", "stable", "declining"
    total_change: float
    history: list[TrendPoint] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "plan_id": self.plan_id,
            "current_score": self.current_score,
            "trend_direction": self.trend_direction,
            "total_change": self.total_change,
            "history": [point.to_dict() for point in self.history],
        }


@dataclass(frozen=True, slots=True)
class QualityScore:
    """Overall quality score for a plan."""

    overall_score: float  # 0-100 composite score
    dimension_scores: DimensionScores
    recommendations: list[Recommendation] = field(default_factory=list)
    summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "overall_score": self.overall_score,
            "dimension_scores": self.dimension_scores.to_dict(),
            "recommendations": [rec.to_dict() for rec in self.recommendations],
            "summary": self.summary,
        }


class PlanQualityScorer:
    """
    Evaluate plan quality using rule-based heuristics.

    Scores plans across multiple dimensions:
    - Completeness: all tasks have estimates, assignees, acceptance criteria
    - Clarity: descriptions clear and detailed
    - Feasibility: realistic estimates, no resource overallocation
    - Risk coverage: high-risk tasks identified and mitigated
    - Dependency hygiene: no cycles, critical path reasonable
    """

    def __init__(self, benchmark_data: list[dict[str, Any]] | None = None):
        """
        Initialize the quality scorer.

        Args:
            benchmark_data: Optional list of historical plan data for benchmarking
        """
        self.benchmark_data = benchmark_data or []
        self._trend_history: dict[str, list[dict[str, Any]]] = {}

    def score_plan(self, plan: Mapping[str, Any]) -> QualityScore:
        """
        Score a plan across all dimensions and generate composite quality score.

        Args:
            plan: Plan data with tasks, milestones, etc.

        Returns:
            QualityScore with overall score, dimension scores, and recommendations
        """
        if not isinstance(plan, Mapping):
            return self._empty_quality_score()

        # Analyze each dimension
        dimension_scores = self.analyze_dimensions(plan)

        # Calculate weighted composite score
        overall_score = self._calculate_composite_score(dimension_scores)

        # Generate recommendations
        recommendations = self.generate_recommendations(plan, dimension_scores)

        # Create summary
        summary = self._generate_summary(overall_score, dimension_scores, recommendations)

        return QualityScore(
            overall_score=overall_score,
            dimension_scores=dimension_scores,
            recommendations=recommendations,
            summary=summary,
        )

    def analyze_dimensions(self, plan: Mapping[str, Any]) -> DimensionScores:
        """
        Analyze plan quality across all dimensions.

        Args:
            plan: Plan data with tasks, milestones, etc.

        Returns:
            DimensionScores with individual dimension assessments
        """
        tasks = plan.get("tasks", [])
        if not isinstance(tasks, list):
            tasks = []

        completeness = self._score_completeness(tasks, plan)
        clarity = self._score_clarity(tasks, plan)
        feasibility = self._score_feasibility(tasks, plan)
        risk_coverage = self._score_risk_coverage(tasks, plan)
        dependency_hygiene = self._score_dependency_hygiene(tasks, plan)

        return DimensionScores(
            completeness=completeness,
            clarity=clarity,
            feasibility=feasibility,
            risk_coverage=risk_coverage,
            dependency_hygiene=dependency_hygiene,
        )

    def generate_recommendations(
        self, plan: Mapping[str, Any], scores: DimensionScores
    ) -> list[Recommendation]:
        """
        Generate specific actionable recommendations based on dimension scores.

        Args:
            plan: Plan data with tasks, milestones, etc.
            scores: Dimension scores from analyze_dimensions

        Returns:
            List of prioritized recommendations
        """
        recommendations: list[Recommendation] = []

        # Extract recommendations from dimension issues
        for dim_name, dim_score in [
            ("completeness", scores.completeness),
            ("clarity", scores.clarity),
            ("feasibility", scores.feasibility),
            ("risk_coverage", scores.risk_coverage),
            ("dependency_hygiene", scores.dependency_hygiene),
        ]:
            # High priority from issues
            for issue in dim_score.issues:
                recommendations.append(
                    Recommendation(
                        priority="high",
                        category=dim_name,
                        message=issue,
                        affected_items=[],
                    )
                )

            # Medium priority from warnings
            for warning in dim_score.warnings:
                recommendations.append(
                    Recommendation(
                        priority="medium",
                        category=dim_name,
                        message=warning,
                        affected_items=[],
                    )
                )

        # Sort by priority
        priority_order = {"high": 0, "medium": 1, "low": 2}
        recommendations.sort(key=lambda r: priority_order.get(r.priority, 3))

        return recommendations

    def compare_to_benchmarks(self, plan: Mapping[str, Any]) -> BenchmarkReport:
        """
        Compare plan quality to benchmarks from similar successful plans.

        Args:
            plan: Plan data with tasks, milestones, etc.

        Returns:
            BenchmarkReport comparing this plan to historical data
        """
        quality_score = self.score_plan(plan)
        plan_score = quality_score.overall_score

        if not self.benchmark_data:
            # No benchmark data available
            return BenchmarkReport(
                plan_score=plan_score,
                benchmark_avg=plan_score,
                benchmark_median=plan_score,
                percentile=50.0,
                better_than_percent=50.0,
                comparison="at",
            )

        # Extract benchmark scores
        benchmark_scores = [
            self._extract_benchmark_score(bench) for bench in self.benchmark_data
        ]
        benchmark_scores = [s for s in benchmark_scores if s is not None]

        if not benchmark_scores:
            return BenchmarkReport(
                plan_score=plan_score,
                benchmark_avg=plan_score,
                benchmark_median=plan_score,
                percentile=50.0,
                better_than_percent=50.0,
                comparison="at",
            )

        # Calculate statistics
        benchmark_avg = sum(benchmark_scores) / len(benchmark_scores)
        sorted_scores = sorted(benchmark_scores)
        n = len(sorted_scores)
        benchmark_median = (
            sorted_scores[n // 2]
            if n % 2 == 1
            else (sorted_scores[n // 2 - 1] + sorted_scores[n // 2]) / 2
        )

        # Calculate percentile
        below_count = sum(1 for s in benchmark_scores if s < plan_score)
        better_than_percent = (below_count / len(benchmark_scores)) * 100
        percentile = better_than_percent

        # Determine comparison
        if plan_score > benchmark_avg:
            comparison = "above"
        elif plan_score < benchmark_avg:
            comparison = "below"
        else:
            comparison = "at"

        return BenchmarkReport(
            plan_score=plan_score,
            benchmark_avg=benchmark_avg,
            benchmark_median=benchmark_median,
            percentile=percentile,
            better_than_percent=better_than_percent,
            comparison=comparison,
        )

    def track_quality_trends(self, plan_id: str) -> TrendAnalysis:
        """
        Track quality score trends over plan versions.

        Args:
            plan_id: Identifier for the plan

        Returns:
            TrendAnalysis with historical trend data
        """
        history = self._trend_history.get(plan_id, [])

        if not history:
            return TrendAnalysis(
                plan_id=plan_id,
                current_score=0.0,
                trend_direction="stable",
                total_change=0.0,
                history=[],
            )

        # Build trend points
        trend_points: list[TrendPoint] = []
        for i, entry in enumerate(history):
            prev_score = history[i - 1]["score"] if i > 0 else entry["score"]
            change = entry["score"] - prev_score

            trend_points.append(
                TrendPoint(
                    version=entry.get("version", i + 1),
                    timestamp=entry.get("timestamp", datetime.now()),
                    score=entry["score"],
                    change=change,
                )
            )

        current_score = history[-1]["score"]
        initial_score = history[0]["score"]
        total_change = current_score - initial_score

        # Determine trend direction
        if total_change > 5:
            trend_direction = "improving"
        elif total_change < -5:
            trend_direction = "declining"
        else:
            trend_direction = "stable"

        return TrendAnalysis(
            plan_id=plan_id,
            current_score=current_score,
            trend_direction=trend_direction,
            total_change=total_change,
            history=trend_points,
        )

    def record_plan_version(
        self, plan_id: str, plan: Mapping[str, Any], version: int | None = None
    ) -> None:
        """
        Record a plan version for trend tracking.

        Args:
            plan_id: Identifier for the plan
            plan: Plan data
            version: Optional version number (auto-increments if not provided)
        """
        quality_score = self.score_plan(plan)

        if plan_id not in self._trend_history:
            self._trend_history[plan_id] = []

        history = self._trend_history[plan_id]
        next_version = version if version is not None else len(history) + 1

        history.append(
            {
                "version": next_version,
                "timestamp": datetime.now(),
                "score": quality_score.overall_score,
            }
        )

    # Internal scoring methods

    def _score_completeness(
        self, tasks: list[dict[str, Any]], plan: Mapping[str, Any]
    ) -> DimensionScore:
        """Score completeness dimension."""
        if not tasks:
            return DimensionScore(score=0, max_score=100, issues=["No tasks in plan"])

        issues: list[str] = []
        warnings: list[str] = []

        # Check for missing estimates
        tasks_without_estimates = [
            t
            for t in tasks
            if not t.get("estimated_hours")
            and not t.get("estimated_complexity")
            and not t.get("estimated_effort")
        ]
        if tasks_without_estimates:
            count = len(tasks_without_estimates)
            issues.append(f"Add estimates to {count} task{'s' if count > 1 else ''}")

        # Check for missing acceptance criteria
        tasks_without_ac = [
            t for t in tasks if not t.get("acceptance_criteria") or len(t.get("acceptance_criteria", [])) == 0
        ]
        if tasks_without_ac:
            count = len(tasks_without_ac)
            issues.append(
                f"Add acceptance criteria to {count} task{'s' if count > 1 else ''}"
            )

        # Check for missing owners/assignees
        tasks_without_owners = [
            t
            for t in tasks
            if not t.get("owner")
            and not t.get("assignee")
            and not t.get("owner_type")
        ]
        if tasks_without_owners:
            count = len(tasks_without_owners)
            warnings.append(
                f"Consider assigning owners to {count} task{'s' if count > 1 else ''}"
            )

        # Calculate score
        max_score = 100.0
        score = max_score
        score -= len(tasks_without_estimates) * MISSING_ESTIMATE_PENALTY
        score -= len(tasks_without_ac) * MISSING_ESTIMATE_PENALTY
        score -= len(tasks_without_owners) * (MISSING_ESTIMATE_PENALTY / 2)
        score = max(0, score)

        return DimensionScore(score=score, max_score=max_score, issues=issues, warnings=warnings)

    def _score_clarity(
        self, tasks: list[dict[str, Any]], plan: Mapping[str, Any]
    ) -> DimensionScore:
        """Score clarity dimension."""
        if not tasks:
            return DimensionScore(score=0, max_score=100, issues=["No tasks in plan"])

        issues: list[str] = []
        warnings: list[str] = []

        # Check for vague descriptions
        vague_tasks = []
        for task in tasks:
            description = task.get("description", "")
            if isinstance(description, str) and len(description) < VAGUE_DESCRIPTION_THRESHOLD:
                vague_tasks.append(task)

        if vague_tasks:
            for task in vague_tasks[:3]:  # List first 3
                task_title = task.get("title", task.get("id", "Unknown"))
                issues.append(f"Clarify description for task '{task_title}'")

            if len(vague_tasks) > 3:
                issues.append(f"And {len(vague_tasks) - 3} more tasks need clearer descriptions")

        # Calculate score
        max_score = 100.0
        score = max_score
        score -= len(vague_tasks) * VAGUE_DESCRIPTION_PENALTY
        score = max(0, score)

        return DimensionScore(score=score, max_score=max_score, issues=issues, warnings=warnings)

    def _score_feasibility(
        self, tasks: list[dict[str, Any]], plan: Mapping[str, Any]
    ) -> DimensionScore:
        """Score feasibility dimension."""
        if not tasks:
            return DimensionScore(score=0, max_score=100, issues=["No tasks in plan"])

        issues: list[str] = []
        warnings: list[str] = []

        # Check for resource overallocation
        # Group tasks by owner and count concurrent tasks
        owner_task_counts: dict[str, int] = {}
        for task in tasks:
            owner = task.get("owner") or task.get("assignee") or task.get("owner_type")
            if owner:
                owner_task_counts[owner] = owner_task_counts.get(owner, 0) + 1

        overallocated = [
            owner for owner, count in owner_task_counts.items() if count > OVERALLOCATION_THRESHOLD * 5
        ]
        if overallocated:
            for owner in overallocated[:2]:
                count = owner_task_counts[owner]
                warnings.append(f"Resource overallocation detected for '{owner}' ({count} tasks)")

        # Check for unrealistic estimates
        overly_large_tasks = [
            t for t in tasks if t.get("estimated_hours", 0) > 40
        ]
        if overly_large_tasks:
            warnings.append(
                f"{len(overly_large_tasks)} task{'s' if len(overly_large_tasks) > 1 else ''} "
                f"may be too large (>40 hours) - consider breaking down"
            )

        # Calculate score
        max_score = 100.0
        score = max_score
        score -= len(overallocated) * 10
        score -= len(overly_large_tasks) * 5
        score = max(0, score)

        return DimensionScore(score=score, max_score=max_score, issues=issues, warnings=warnings)

    def _score_risk_coverage(
        self, tasks: list[dict[str, Any]], plan: Mapping[str, Any]
    ) -> DimensionScore:
        """Score risk coverage dimension."""
        if not tasks:
            return DimensionScore(score=0, max_score=100, issues=["No tasks in plan"])

        issues: list[str] = []
        warnings: list[str] = []

        # Check for high-risk tasks without mitigation
        high_risk_tasks = [
            t
            for t in tasks
            if t.get("risk_level", "").lower() in ("high", "critical")
        ]

        if high_risk_tasks:
            # Check if risks are documented in plan
            plan_risks = plan.get("risks", [])
            if not plan_risks or len(plan_risks) == 0:
                issues.append(
                    f"{len(high_risk_tasks)} high-risk task{'s' if len(high_risk_tasks) > 1 else ''} "
                    f"identified but no mitigation plan documented"
                )

        # Check for tasks with no risk level specified
        tasks_without_risk = [
            t for t in tasks if not t.get("risk_level")
        ]
        if len(tasks_without_risk) > len(tasks) * 0.5:
            warnings.append(
                f"{len(tasks_without_risk)} tasks missing risk assessment"
            )

        # Calculate score
        max_score = 100.0
        score = max_score
        if high_risk_tasks and not plan.get("risks"):
            score -= 20
        score -= min(30, len(tasks_without_risk) * 2)
        score = max(0, score)

        return DimensionScore(score=score, max_score=max_score, issues=issues, warnings=warnings)

    def _score_dependency_hygiene(
        self, tasks: list[dict[str, Any]], plan: Mapping[str, Any]
    ) -> DimensionScore:
        """Score dependency hygiene dimension."""
        if not tasks:
            return DimensionScore(score=0, max_score=100, issues=["No tasks in plan"])

        issues: list[str] = []
        warnings: list[str] = []

        # Build dependency graph
        task_ids = {t.get("id") for t in tasks if t.get("id")}
        dependency_graph: dict[str, list[str]] = {}

        for task in tasks:
            task_id = task.get("id")
            if task_id:
                deps = task.get("depends_on", [])
                if isinstance(deps, list):
                    dependency_graph[task_id] = [d for d in deps if d in task_ids]

        # Check for cycles
        if self._has_cycles(dependency_graph):
            issues.append("Dependency cycles detected - please review task dependencies")

        # Check critical path length
        critical_path_length = self._estimate_critical_path_length(tasks, dependency_graph)
        total_estimated_time = sum(
            t.get("estimated_hours", 0) for t in tasks
        )

        if total_estimated_time > 0:
            critical_path_ratio = critical_path_length / total_estimated_time
            if critical_path_ratio > CRITICAL_PATH_THRESHOLD:
                warnings.append(
                    f"Critical path is {int(critical_path_ratio * 100)}% of timeline - add buffer"
                )

        # Calculate score
        max_score = 100.0
        score = max_score
        if self._has_cycles(dependency_graph):
            score -= 30
        if total_estimated_time > 0 and critical_path_length / total_estimated_time > CRITICAL_PATH_THRESHOLD:
            score -= 15
        score = max(0, score)

        return DimensionScore(score=score, max_score=max_score, issues=issues, warnings=warnings)

    # Helper methods

    def _calculate_composite_score(self, dimension_scores: DimensionScores) -> float:
        """Calculate weighted composite score from dimension scores."""
        weighted_sum = (
            (dimension_scores.completeness.score / dimension_scores.completeness.max_score)
            * COMPLETENESS_WEIGHT
            + (dimension_scores.clarity.score / dimension_scores.clarity.max_score)
            * CLARITY_WEIGHT
            + (dimension_scores.feasibility.score / dimension_scores.feasibility.max_score)
            * FEASIBILITY_WEIGHT
            + (dimension_scores.risk_coverage.score / dimension_scores.risk_coverage.max_score)
            * RISK_WEIGHT
            + (dimension_scores.dependency_hygiene.score / dimension_scores.dependency_hygiene.max_score)
            * DEPENDENCY_WEIGHT
        )

        return round(weighted_sum * MAX_SCORE, 2)

    def _generate_summary(
        self,
        overall_score: float,
        dimension_scores: DimensionScores,
        recommendations: list[Recommendation],
    ) -> str:
        """Generate a human-readable summary of the quality assessment."""
        high_priority_count = sum(1 for r in recommendations if r.priority == "high")

        # Adjust quality rating based on high priority issues
        if overall_score >= 90 and high_priority_count == 0:
            quality = "excellent"
        elif overall_score >= 75 and high_priority_count <= 1:
            quality = "good"
        elif overall_score >= 60:
            quality = "fair"
        else:
            quality = "needs improvement"

        summary = f"Plan quality is {quality} (score: {overall_score}/100). "

        if high_priority_count > 0:
            summary += f"Found {high_priority_count} high-priority issue{'s' if high_priority_count > 1 else ''} to address. "
        else:
            summary += "No critical issues found. "

        return summary

    def _empty_quality_score(self) -> QualityScore:
        """Return an empty quality score for invalid input."""
        empty_dim = DimensionScore(score=0, max_score=100)
        return QualityScore(
            overall_score=0.0,
            dimension_scores=DimensionScores(
                completeness=empty_dim,
                clarity=empty_dim,
                feasibility=empty_dim,
                risk_coverage=empty_dim,
                dependency_hygiene=empty_dim,
            ),
            recommendations=[],
            summary="Invalid plan data",
        )

    def _extract_benchmark_score(self, benchmark: dict[str, Any]) -> float | None:
        """Extract quality score from benchmark data."""
        # Try different field names
        for field in ("quality_score", "score", "overall_score"):
            if field in benchmark:
                score = benchmark[field]
                if isinstance(score, (int, float)):
                    return float(score)
        return None

    def _has_cycles(self, graph: dict[str, list[str]]) -> bool:
        """Detect cycles in dependency graph using DFS with depth limit."""
        visited = set()
        rec_stack = set()
        max_depth = len(graph) + 1  # Prevent infinite recursion

        def visit(node: str, depth: int = 0) -> bool:
            if depth > max_depth:
                return True  # Too deep, likely a cycle

            if node in rec_stack:
                return True  # Found a cycle

            if node in visited:
                return False  # Already processed this node

            visited.add(node)
            rec_stack.add(node)

            for neighbor in graph.get(node, []):
                if visit(neighbor, depth + 1):
                    return True

            rec_stack.remove(node)
            return False

        for node in graph:
            if node not in visited:
                if visit(node):
                    return True

        return False

    def _estimate_critical_path_length(
        self, tasks: list[dict[str, Any]], graph: dict[str, list[str]]
    ) -> float:
        """Estimate critical path length in hours."""
        task_map = {t.get("id"): t for t in tasks if t.get("id")}

        # Calculate longest path for each task
        memo: dict[str, float] = {}
        visiting: set[str] = set()  # Track currently visiting nodes to detect cycles

        def longest_path(task_id: str) -> float:
            if task_id in memo:
                return memo[task_id]

            if task_id in visiting:
                # Cycle detected, return 0 to break recursion
                return 0.0

            task = task_map.get(task_id)
            if not task:
                return 0.0

            task_hours = task.get("estimated_hours", 0)
            deps = graph.get(task_id, [])

            if not deps:
                memo[task_id] = task_hours
                return task_hours

            visiting.add(task_id)
            max_dep_path = max((longest_path(dep) for dep in deps), default=0.0)
            visiting.remove(task_id)

            total = task_hours + max_dep_path
            memo[task_id] = total
            return total

        # Find maximum path across all tasks
        return max((longest_path(tid) for tid in graph), default=0.0)


__all__ = [
    "PlanQualityScorer",
    "QualityScore",
    "DimensionScores",
    "DimensionScore",
    "Recommendation",
    "BenchmarkReport",
    "TrendAnalysis",
    "TrendPoint",
]
