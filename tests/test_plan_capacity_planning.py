"""Tests for capacity planning matrix generator."""

from blueprint.plan_capacity_planning import (
    CapacityPlanningMatrix,
    CapacityPlanningMatrixRow,
    generate_capacity_planning_matrix,
)


def test_empty_plan_returns_empty_matrix():
    """Empty plan should return matrix with no rows."""
    result = generate_capacity_planning_matrix({"tasks": []})

    assert isinstance(result, CapacityPlanningMatrix)
    assert len(result.rows) == 0
    assert result.summary["task_count"] == 0


def test_resource_forecasts_detected():
    """Detect resource forecasts in task."""
    plan = {
        "id": "test-plan-1",
        "tasks": [
            {
                "id": "task-1",
                "title": "Capacity Planning",
                "description": "Create resource forecast for CPU and memory",
            }
        ],
    }

    result = generate_capacity_planning_matrix(plan)

    assert len(result.rows) == 1
    assert result.rows[0].resource_forecasts == "present"


def test_growth_projections_detected():
    """Detect growth projections in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Growth Planning",
                "description": "Analyze traffic growth projections",
            }
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert result.rows[0].growth_projections == "present"


def test_scaling_triggers_detected():
    """Detect scaling triggers in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Auto-scaling Setup",
                "description": "Configure scaling triggers at 80% CPU threshold",
            }
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert result.rows[0].scaling_triggers == "present"


def test_performance_targets_detected():
    """Detect performance targets in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "SLA Definition",
                "description": "Set latency targets and SLO targets",
            }
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert result.rows[0].performance_targets == "present"


def test_budget_constraints_detected():
    """Detect budget constraints in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Cost Planning",
                "description": "Define budget constraints and cost ceiling",
            }
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert result.rows[0].budget_constraints == "present"


def test_bottleneck_analysis_detected():
    """Detect bottleneck analysis in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Performance Analysis",
                "description": "Identify resource bottlenecks and constraints",
            }
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert result.rows[0].bottleneck_analysis == "present"


def test_scaling_flexibility_detected():
    """Detect scaling flexibility in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Elastic Scaling",
                "description": "Enable auto-scaling for scaling flexibility",
            }
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert result.rows[0].scaling_flexibility == "present"


def test_cost_efficiency_detected():
    """Detect cost efficiency in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Cost Optimization",
                "description": "Implement cost optimization and right-sizing",
            }
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert result.rows[0].cost_efficiency == "present"


def test_adequate_planning_score():
    """Task with 6+ signals should get adequate score."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Complete Capacity Planning",
                "description": (
                    "Create resource forecast with growth projections. "
                    "Configure scaling triggers and performance targets. "
                    "Define budget constraints and analyze bottlenecks. "
                    "Enable auto-scaling for scaling flexibility. "
                    "Implement cost optimization for efficiency."
                ),
            }
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert result.rows[0].planning_score == "adequate"


def test_partial_planning_score():
    """Task with 3-5 signals should get partial score."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Partial Planning",
                "description": (
                    "Create resource forecast with scaling triggers. "
                    "Set performance targets and budget constraints."
                ),
            }
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert result.rows[0].planning_score == "partial"


def test_insufficient_planning_score():
    """Task with <3 signals should get insufficient score."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Minimal Planning",
                "description": "Basic capacity planning",
            }
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert result.rows[0].planning_score == "insufficient"


def test_multiple_tasks_analyzed():
    """Multiple tasks should be analyzed independently."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "Resource forecast and growth projections",
            },
            {
                "id": "task-2",
                "title": "Task 2",
                "description": "Scaling triggers and performance targets",
            },
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert len(result.rows) == 2
    assert result.rows[0].resource_forecasts == "present"
    assert result.rows[1].scaling_triggers == "present"


def test_summary_statistics():
    """Summary should contain accurate statistics."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Adequate Task",
                "description": (
                    "Resource forecast, growth projections, scaling triggers, "
                    "performance targets, budget constraints, bottlenecks, "
                    "scaling flexibility, cost optimization"
                ),
            },
            {
                "id": "task-2",
                "title": "Partial Task",
                "description": "Resource forecast, scaling triggers, performance targets",
            },
            {
                "id": "task-3",
                "title": "Insufficient Task",
                "description": "Basic planning",
            },
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert result.summary["task_count"] == 3
    assert result.summary["adequate_count"] == 1
    assert result.summary["partial_count"] == 1
    assert result.summary["insufficient_count"] == 1
    assert result.summary["overall_coverage"] > 0


def test_to_dict_method():
    """Test to_dict() serialization."""
    plan = {
        "id": "test-plan",
        "tasks": [
            {
                "id": "task-1",
                "title": "Test Task",
                "description": "Resource forecast",
            }
        ],
    }

    result = generate_capacity_planning_matrix(plan)
    result_dict = result.to_dict()

    assert isinstance(result_dict, dict)
    assert "plan_id" in result_dict
    assert "rows" in result_dict
    assert "summary" in result_dict


def test_to_markdown_method():
    """Test to_markdown() rendering."""
    plan = {
        "id": "test-plan",
        "tasks": [
            {
                "id": "task-1",
                "title": "Test Task",
                "description": "Resource forecast",
            }
        ],
    }

    result = generate_capacity_planning_matrix(plan)
    markdown = result.to_markdown()

    assert isinstance(markdown, str)
    assert "# Capacity Planning Matrix" in markdown
    assert "## Summary" in markdown
    assert "## Matrix" in markdown


def test_row_to_dict_method():
    """Test MatrixRow to_dict() serialization."""
    row = CapacityPlanningMatrixRow(
        task_id="task-1",
        title="Test Task",
        resource_forecasts="present",
        growth_projections="missing",
        planning_score="partial",
        evidence=("forecast", "projection"),
    )

    result = row.to_dict()

    assert isinstance(result, dict)
    assert result["task_id"] == "task-1"
    assert result["resource_forecasts"] == "present"
    assert isinstance(result["evidence"], list)


def test_empty_plan_markdown():
    """Empty plan should render informative markdown."""
    result = generate_capacity_planning_matrix({"tasks": []})
    markdown = result.to_markdown()

    assert "No capacity planning signals detected" in markdown


def test_case_insensitive_matching():
    """Pattern matching should be case-insensitive."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "description": "Create resource forecast with growth projections",
            }
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert result.rows[0].resource_forecasts == "present"
    assert result.rows[0].growth_projections == "present"


def test_acceptance_criteria_scanned():
    """Acceptance criteria should be scanned for signals."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "acceptance_criteria": ["Resource forecast completed", "Scaling triggers configured"],
            }
        ]
    }

    result = generate_capacity_planning_matrix(plan)

    assert result.rows[0].resource_forecasts == "present"
    assert result.rows[0].scaling_triggers == "present"
