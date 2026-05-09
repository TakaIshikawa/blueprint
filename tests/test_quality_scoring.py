"""Tests for AI-powered plan quality scoring and recommendations."""

import pytest
from datetime import datetime

from blueprint.ai.quality_scoring import (
    PlanQualityScorer,
    QualityScore,
    DimensionScores,
    DimensionScore,
    Recommendation,
    BenchmarkReport,
    TrendAnalysis,
    TrendPoint,
)


def test_empty_plan_returns_zero_score():
    """Empty plan should return zero score."""
    scorer = PlanQualityScorer()
    result = scorer.score_plan({})

    assert isinstance(result, QualityScore)
    assert result.overall_score == 0.0


def test_invalid_plan_returns_zero_score():
    """Invalid plan (non-mapping) should return zero score."""
    scorer = PlanQualityScorer()
    result = scorer.score_plan("not a plan")

    assert isinstance(result, QualityScore)
    assert result.overall_score == 0.0
    assert result.summary == "Invalid plan data"


def test_perfect_plan_high_score():
    """Perfect plan with all requirements should score highly."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Implement authentication",
                "description": "Add JWT-based authentication with refresh tokens and proper error handling",
                "estimated_hours": 8,
                "owner": "alice",
                "acceptance_criteria": [
                    "JWT tokens working",
                    "Refresh token flow implemented",
                    "Tests passing",
                ],
                "risk_level": "medium",
                "depends_on": [],
            },
            {
                "id": "task-2",
                "title": "Add user profile",
                "description": "Create user profile page with avatar upload and bio editing capabilities",
                "estimated_hours": 5,
                "owner": "bob",
                "acceptance_criteria": [
                    "Profile page rendered",
                    "Avatar upload working",
                ],
                "risk_level": "low",
                "depends_on": ["task-1"],
            },
        ],
        "risks": [
            "Authentication complexity may require additional time",
        ],
    }

    scorer = PlanQualityScorer()
    result = scorer.score_plan(plan)

    assert result.overall_score > 90


def test_completeness_dimension_missing_estimates():
    """Test completeness dimension detects missing estimates."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "Description",
                "acceptance_criteria": ["AC1"],
            },
            {
                "id": "task-2",
                "title": "Task 2",
                "description": "Description",
                "estimated_hours": 5,
                "acceptance_criteria": ["AC1"],
            },
        ],
    }

    scorer = PlanQualityScorer()
    dimensions = scorer.analyze_dimensions(plan)

    assert dimensions.completeness.score < 100
    assert any("estimate" in issue.lower() for issue in dimensions.completeness.issues)


def test_completeness_dimension_missing_acceptance_criteria():
    """Test completeness dimension detects missing acceptance criteria."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "Description",
                "estimated_hours": 5,
                "acceptance_criteria": [],
            },
            {
                "id": "task-2",
                "title": "Task 2",
                "description": "Description",
                "estimated_hours": 5,
            },
        ],
    }

    scorer = PlanQualityScorer()
    dimensions = scorer.analyze_dimensions(plan)

    assert dimensions.completeness.score < 100
    assert any("acceptance criteria" in issue.lower() for issue in dimensions.completeness.issues)


def test_completeness_dimension_missing_owners():
    """Test completeness dimension warns about missing owners."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "Description",
                "estimated_hours": 5,
                "acceptance_criteria": ["AC1"],
            },
        ],
    }

    scorer = PlanQualityScorer()
    dimensions = scorer.analyze_dimensions(plan)

    assert any("owner" in warning.lower() for warning in dimensions.completeness.warnings)


def test_clarity_dimension_vague_descriptions():
    """Test clarity dimension detects vague descriptions."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "Do it",  # Too short
                "estimated_hours": 5,
                "acceptance_criteria": ["AC1"],
            },
            {
                "id": "task-2",
                "title": "Task 2",
                "description": "This is a detailed description with proper explanation of what needs to be done",
                "estimated_hours": 5,
                "acceptance_criteria": ["AC1"],
            },
        ],
    }

    scorer = PlanQualityScorer()
    dimensions = scorer.analyze_dimensions(plan)

    assert dimensions.clarity.score < 100
    assert any("task-1" in issue or "clarify" in issue.lower() for issue in dimensions.clarity.issues)


def test_feasibility_dimension_resource_overallocation():
    """Test feasibility dimension detects resource overallocation."""
    # Create 20 tasks for same person
    tasks = []
    for i in range(20):
        tasks.append(
            {
                "id": f"task-{i}",
                "title": f"Task {i}",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 5,
                "owner": "alice",
                "acceptance_criteria": ["AC1"],
            }
        )

    plan = {"tasks": tasks}

    scorer = PlanQualityScorer()
    dimensions = scorer.analyze_dimensions(plan)

    assert any(
        "overallocation" in warning.lower() or "alice" in warning
        for warning in dimensions.feasibility.warnings
    )


def test_feasibility_dimension_large_tasks():
    """Test feasibility dimension warns about overly large tasks."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Huge task",
                "description": "A very detailed description of what needs to be done for this task",
                "estimated_hours": 50,  # Too large
                "acceptance_criteria": ["AC1"],
            },
        ],
    }

    scorer = PlanQualityScorer()
    dimensions = scorer.analyze_dimensions(plan)

    assert any("large" in warning.lower() or "40" in warning for warning in dimensions.feasibility.warnings)


def test_risk_coverage_dimension_high_risk_no_mitigation():
    """Test risk coverage dimension detects high-risk tasks without mitigation."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Risky task",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 5,
                "risk_level": "high",
                "acceptance_criteria": ["AC1"],
            },
        ],
        "risks": [],  # No mitigation plan
    }

    scorer = PlanQualityScorer()
    dimensions = scorer.analyze_dimensions(plan)

    assert dimensions.risk_coverage.score < 100
    assert any("high-risk" in issue.lower() for issue in dimensions.risk_coverage.issues)


def test_risk_coverage_dimension_many_tasks_without_risk_level():
    """Test risk coverage warns when many tasks lack risk assessment."""
    plan = {
        "tasks": [
            {
                "id": f"task-{i}",
                "title": f"Task {i}",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 5,
                "acceptance_criteria": ["AC1"],
                # No risk_level
            }
            for i in range(10)
        ],
    }

    scorer = PlanQualityScorer()
    dimensions = scorer.analyze_dimensions(plan)

    assert any("risk assessment" in warning.lower() for warning in dimensions.risk_coverage.warnings)


def test_dependency_hygiene_detects_cycles():
    """Test dependency hygiene dimension detects dependency cycles."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 5,
                "acceptance_criteria": ["AC1"],
                "depends_on": ["task-2"],
            },
            {
                "id": "task-2",
                "title": "Task 2",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 5,
                "acceptance_criteria": ["AC1"],
                "depends_on": ["task-1"],  # Cycle
            },
        ],
    }

    scorer = PlanQualityScorer()
    dimensions = scorer.analyze_dimensions(plan)

    assert dimensions.dependency_hygiene.score < 100
    assert any("cycle" in issue.lower() for issue in dimensions.dependency_hygiene.issues)


def test_dependency_hygiene_critical_path_warning():
    """Test dependency hygiene warns about long critical paths."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 10,
                "acceptance_criteria": ["AC1"],
                "depends_on": [],
            },
            {
                "id": "task-2",
                "title": "Task 2",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 10,
                "acceptance_criteria": ["AC1"],
                "depends_on": ["task-1"],
            },
            {
                "id": "task-3",
                "title": "Task 3",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 2,
                "acceptance_criteria": ["AC1"],
                "depends_on": [],
            },
        ],
    }

    scorer = PlanQualityScorer()
    dimensions = scorer.analyze_dimensions(plan)

    # Critical path is 20 hours out of 22 total (>80%)
    assert any("critical path" in warning.lower() for warning in dimensions.dependency_hygiene.warnings)


def test_generate_recommendations_from_issues():
    """Test recommendation generation from dimension issues."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "Short",  # Vague
                "acceptance_criteria": [],  # Missing
            },
        ],
    }

    scorer = PlanQualityScorer()
    quality_score = scorer.score_plan(plan)

    assert len(quality_score.recommendations) > 0
    assert any(rec.priority == "high" for rec in quality_score.recommendations)


def test_recommendations_sorted_by_priority():
    """Test recommendations are sorted by priority."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "Short",
                "acceptance_criteria": [],
                "risk_level": "high",
            },
        ],
        "risks": [],
    }

    scorer = PlanQualityScorer()
    quality_score = scorer.score_plan(plan)

    # Verify high priority comes first
    priorities = [rec.priority for rec in quality_score.recommendations]
    high_indices = [i for i, p in enumerate(priorities) if p == "high"]
    medium_indices = [i for i, p in enumerate(priorities) if p == "medium"]

    if high_indices and medium_indices:
        assert max(high_indices) < min(medium_indices)


def test_compare_to_benchmarks_no_data():
    """Test benchmark comparison with no benchmark data."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 5,
                "owner": "alice",
                "acceptance_criteria": ["AC1"],
            },
        ],
    }

    scorer = PlanQualityScorer()
    benchmark = scorer.compare_to_benchmarks(plan)

    assert isinstance(benchmark, BenchmarkReport)
    assert benchmark.comparison == "at"
    assert benchmark.percentile == 50.0


def test_compare_to_benchmarks_with_data():
    """Test benchmark comparison with historical data."""
    benchmark_data = [
        {"quality_score": 75.0},
        {"quality_score": 80.0},
        {"quality_score": 85.0},
        {"quality_score": 90.0},
    ]

    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 5,
                "owner": "alice",
                "acceptance_criteria": ["AC1"],
                "risk_level": "low",
            },
        ],
        "risks": ["Some risk"],
    }

    scorer = PlanQualityScorer(benchmark_data=benchmark_data)
    benchmark = scorer.compare_to_benchmarks(plan)

    assert isinstance(benchmark, BenchmarkReport)
    assert benchmark.benchmark_avg == 82.5
    assert benchmark.benchmark_median == 82.5
    assert benchmark.plan_score > 0
    assert 0 <= benchmark.percentile <= 100


def test_compare_to_benchmarks_above_average():
    """Test benchmark comparison when plan is above average."""
    benchmark_data = [
        {"quality_score": 50.0},
        {"quality_score": 60.0},
        {"quality_score": 70.0},
    ]

    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 5,
                "owner": "alice",
                "acceptance_criteria": ["AC1", "AC2"],
                "risk_level": "low",
            },
        ],
        "risks": ["Some risk"],
    }

    scorer = PlanQualityScorer(benchmark_data=benchmark_data)
    benchmark = scorer.compare_to_benchmarks(plan)

    assert benchmark.comparison == "above"
    assert benchmark.better_than_percent > 50


def test_track_quality_trends_no_history():
    """Test trend tracking with no history."""
    scorer = PlanQualityScorer()
    trends = scorer.track_quality_trends("plan-123")

    assert isinstance(trends, TrendAnalysis)
    assert trends.plan_id == "plan-123"
    assert trends.current_score == 0.0
    assert trends.trend_direction == "stable"
    assert len(trends.history) == 0


def test_track_quality_trends_with_history():
    """Test trend tracking with version history."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 5,
                "owner": "alice",
                "acceptance_criteria": ["AC1"],
            },
        ],
    }

    scorer = PlanQualityScorer()

    # Record multiple versions
    scorer.record_plan_version("plan-123", plan, version=1)

    # Improve plan
    plan["tasks"][0]["risk_level"] = "low"
    plan["risks"] = ["Some risk"]
    scorer.record_plan_version("plan-123", plan, version=2)

    # Track trends
    trends = scorer.track_quality_trends("plan-123")

    assert trends.plan_id == "plan-123"
    assert len(trends.history) == 2
    assert trends.history[0].version == 1
    assert trends.history[1].version == 2
    assert trends.current_score > 0


def test_track_quality_trends_improving():
    """Test trend detection for improving plan."""
    scorer = PlanQualityScorer()

    # Manually set trend history to simulate improvement
    scorer._trend_history["plan-123"] = [
        {"version": 1, "timestamp": datetime.now(), "score": 60.0},
        {"version": 2, "timestamp": datetime.now(), "score": 75.0},
    ]

    trends = scorer.track_quality_trends("plan-123")

    assert trends.trend_direction == "improving"
    assert trends.total_change > 0


def test_track_quality_trends_declining():
    """Test trend detection for declining plan."""
    scorer = PlanQualityScorer()

    scorer._trend_history["plan-123"] = [
        {"version": 1, "timestamp": datetime.now(), "score": 80.0},
        {"version": 2, "timestamp": datetime.now(), "score": 65.0},
    ]

    trends = scorer.track_quality_trends("plan-123")

    assert trends.trend_direction == "declining"
    assert trends.total_change < 0


def test_track_quality_trends_stable():
    """Test trend detection for stable plan."""
    scorer = PlanQualityScorer()

    scorer._trend_history["plan-123"] = [
        {"version": 1, "timestamp": datetime.now(), "score": 75.0},
        {"version": 2, "timestamp": datetime.now(), "score": 77.0},
    ]

    trends = scorer.track_quality_trends("plan-123")

    assert trends.trend_direction == "stable"


def test_record_plan_version_auto_increment():
    """Test automatic version incrementing."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "A detailed description",
                "estimated_hours": 5,
                "acceptance_criteria": ["AC1"],
            },
        ],
    }

    scorer = PlanQualityScorer()

    # Record without version number
    scorer.record_plan_version("plan-123", plan)
    scorer.record_plan_version("plan-123", plan)

    trends = scorer.track_quality_trends("plan-123")

    assert len(trends.history) == 2
    assert trends.history[0].version == 1
    assert trends.history[1].version == 2


def test_quality_score_to_dict():
    """Test QualityScore serialization to dict."""
    dim_score = DimensionScore(
        score=80.0,
        max_score=100.0,
        issues=["Issue 1"],
        warnings=["Warning 1"],
    )

    dimensions = DimensionScores(
        completeness=dim_score,
        clarity=dim_score,
        feasibility=dim_score,
        risk_coverage=dim_score,
        dependency_hygiene=dim_score,
    )

    recommendations = [
        Recommendation(
            priority="high",
            category="completeness",
            message="Fix this",
            affected_items=["task-1"],
        )
    ]

    quality_score = QualityScore(
        overall_score=85.5,
        dimension_scores=dimensions,
        recommendations=recommendations,
        summary="Good plan",
    )

    result = quality_score.to_dict()

    assert isinstance(result, dict)
    assert result["overall_score"] == 85.5
    assert result["summary"] == "Good plan"
    assert "dimension_scores" in result
    assert len(result["recommendations"]) == 1


def test_dimension_score_to_dict():
    """Test DimensionScore serialization to dict."""
    dim_score = DimensionScore(
        score=75.0,
        max_score=100.0,
        issues=["Issue 1", "Issue 2"],
        warnings=["Warning 1"],
    )

    result = dim_score.to_dict()

    assert isinstance(result, dict)
    assert result["score"] == 75.0
    assert result["max_score"] == 100.0
    assert result["percentage"] == 75.0
    assert len(result["issues"]) == 2
    assert len(result["warnings"]) == 1


def test_recommendation_to_dict():
    """Test Recommendation serialization to dict."""
    rec = Recommendation(
        priority="high",
        category="completeness",
        message="Add estimates to 5 tasks",
        affected_items=["task-1", "task-2"],
    )

    result = rec.to_dict()

    assert isinstance(result, dict)
    assert result["priority"] == "high"
    assert result["category"] == "completeness"
    assert result["message"] == "Add estimates to 5 tasks"
    assert len(result["affected_items"]) == 2


def test_benchmark_report_to_dict():
    """Test BenchmarkReport serialization to dict."""
    report = BenchmarkReport(
        plan_score=85.0,
        benchmark_avg=80.0,
        benchmark_median=82.0,
        percentile=75.0,
        better_than_percent=75.0,
        comparison="above",
    )

    result = report.to_dict()

    assert isinstance(result, dict)
    assert result["plan_score"] == 85.0
    assert result["benchmark_avg"] == 80.0
    assert result["comparison"] == "above"


def test_trend_analysis_to_dict():
    """Test TrendAnalysis serialization to dict."""
    trend_points = [
        TrendPoint(version=1, timestamp=datetime.now(), score=70.0, change=0.0),
        TrendPoint(version=2, timestamp=datetime.now(), score=80.0, change=10.0),
    ]

    trends = TrendAnalysis(
        plan_id="plan-123",
        current_score=80.0,
        trend_direction="improving",
        total_change=10.0,
        history=trend_points,
    )

    result = trends.to_dict()

    assert isinstance(result, dict)
    assert result["plan_id"] == "plan-123"
    assert result["current_score"] == 80.0
    assert result["trend_direction"] == "improving"
    assert len(result["history"]) == 2


def test_trend_point_to_dict():
    """Test TrendPoint serialization to dict."""
    now = datetime.now()
    point = TrendPoint(version=1, timestamp=now, score=75.0, change=5.0)

    result = point.to_dict()

    assert isinstance(result, dict)
    assert result["version"] == 1
    assert result["score"] == 75.0
    assert result["change"] == 5.0
    assert result["timestamp"] == now.isoformat()


def test_summary_excellent_quality():
    """Test summary generation for excellent quality plan."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 5,
                "owner": "alice",
                "acceptance_criteria": ["AC1", "AC2"],
                "risk_level": "low",
            },
        ],
        "risks": ["Some risk"],
    }

    scorer = PlanQualityScorer()
    result = scorer.score_plan(plan)

    assert "excellent" in result.summary.lower() or "good" in result.summary.lower()


def test_summary_poor_quality():
    """Test summary generation for poor quality plan."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "Do it",
                "acceptance_criteria": [],
            },
        ],
    }

    scorer = PlanQualityScorer()
    result = scorer.score_plan(plan)

    assert "needs improvement" in result.summary.lower() or "fair" in result.summary.lower()


def test_dataclass_immutability():
    """Test that dataclasses are immutable."""
    dim_score = DimensionScore(score=80.0, max_score=100.0)

    with pytest.raises(AttributeError):
        dim_score.score = 90.0


def test_no_tasks_in_plan():
    """Test plan with no tasks."""
    plan = {"milestones": ["Milestone 1"], "risks": ["Risk 1"]}

    scorer = PlanQualityScorer()
    result = scorer.score_plan(plan)

    assert result.overall_score == 0.0


def test_tasks_not_list():
    """Test plan with tasks field that is not a list."""
    plan = {"tasks": "not a list"}

    scorer = PlanQualityScorer()
    result = scorer.score_plan(plan)

    assert result.overall_score == 0.0


def test_weighted_composite_score():
    """Test that composite score is properly weighted."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 5,
                "owner": "alice",
                "acceptance_criteria": ["AC1"],
                "risk_level": "low",
            },
        ],
        "risks": ["Risk mitigation"],
    }

    scorer = PlanQualityScorer()
    result = scorer.score_plan(plan)

    # Score should be between 0 and 100
    assert 0 <= result.overall_score <= 100

    # All dimensions should contribute
    dims = result.dimension_scores
    assert dims.completeness.score > 0
    assert dims.clarity.score > 0
    assert dims.feasibility.score > 0
    assert dims.risk_coverage.score > 0
    assert dims.dependency_hygiene.score > 0


def test_multiple_high_risk_tasks_with_mitigation():
    """Test that high-risk tasks with proper mitigation score better."""
    plan_without_mitigation = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 5,
                "risk_level": "high",
                "acceptance_criteria": ["AC1"],
            },
        ],
    }

    plan_with_mitigation = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "A detailed description of what needs to be done for this task",
                "estimated_hours": 5,
                "risk_level": "high",
                "acceptance_criteria": ["AC1"],
            },
        ],
        "risks": ["High risk task requires extra review and testing"],
    }

    scorer = PlanQualityScorer()
    score_without = scorer.score_plan(plan_without_mitigation)
    score_with = scorer.score_plan(plan_with_mitigation)

    assert score_with.overall_score > score_without.overall_score


def test_benchmark_extraction_various_field_names():
    """Test benchmark score extraction from various field names."""
    benchmark_data = [
        {"quality_score": 80.0},
        {"score": 85.0},
        {"overall_score": 90.0},
    ]

    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "A detailed description",
                "estimated_hours": 5,
                "acceptance_criteria": ["AC1"],
            },
        ],
    }

    scorer = PlanQualityScorer(benchmark_data=benchmark_data)
    benchmark = scorer.compare_to_benchmarks(plan)

    # Should successfully extract and average all three scores
    assert benchmark.benchmark_avg == 85.0


def test_critical_path_calculation_linear_chain():
    """Test critical path calculation for linear dependency chain."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "A detailed description",
                "estimated_hours": 5,
                "acceptance_criteria": ["AC1"],
                "depends_on": [],
            },
            {
                "id": "task-2",
                "title": "Task 2",
                "description": "A detailed description",
                "estimated_hours": 10,
                "acceptance_criteria": ["AC1"],
                "depends_on": ["task-1"],
            },
            {
                "id": "task-3",
                "title": "Task 3",
                "description": "A detailed description",
                "estimated_hours": 5,
                "acceptance_criteria": ["AC1"],
                "depends_on": ["task-2"],
            },
        ],
    }

    scorer = PlanQualityScorer()
    dimensions = scorer.analyze_dimensions(plan)

    # Critical path should be 20 hours (5 + 10 + 5)
    # Total is also 20 hours, so ratio is 100%
    assert any("critical path" in warning.lower() for warning in dimensions.dependency_hygiene.warnings)


def test_no_false_positives_good_plan():
    """Test that a truly good plan doesn't generate false positive warnings."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Implement authentication service",
                "description": "Create JWT-based authentication service with refresh tokens, rate limiting, and proper error handling",
                "estimated_hours": 8,
                "owner": "alice",
                "acceptance_criteria": [
                    "JWT tokens working",
                    "Refresh tokens implemented",
                    "Rate limiting active",
                    "Tests passing",
                ],
                "risk_level": "medium",
                "depends_on": [],
            },
            {
                "id": "task-2",
                "title": "Build user profile UI",
                "description": "Create responsive user profile page with avatar upload, bio editing, and settings management",
                "estimated_hours": 5,
                "owner": "bob",
                "acceptance_criteria": [
                    "Profile page renders correctly",
                    "Avatar upload works",
                    "Bio editing functional",
                ],
                "risk_level": "low",
                "depends_on": ["task-1"],
            },
        ],
        "risks": [
            "Authentication complexity may require additional time for security review",
        ],
    }

    scorer = PlanQualityScorer()
    result = scorer.score_plan(plan)

    # Should have high score
    assert result.overall_score >= 85

    # Should have few or no high-priority recommendations
    high_priority_recs = [r for r in result.recommendations if r.priority == "high"]
    assert len(high_priority_recs) == 0
