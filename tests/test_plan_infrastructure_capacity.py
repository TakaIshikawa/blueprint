"""Tests for infrastructure capacity matrix generator."""

from blueprint.plan_infrastructure_capacity import (
    InfrastructureCapacityMatrix,
    InfrastructureCapacityMatrixRow,
    generate_infrastructure_capacity_matrix,
)


def test_empty_plan_returns_empty_matrix():
    """Empty plan should return matrix with no rows."""
    result = generate_infrastructure_capacity_matrix({"tasks": []})

    assert isinstance(result, InfrastructureCapacityMatrix)
    assert len(result.rows) == 0
    assert result.summary["task_count"] == 0


def test_compute_requirements_detected():
    """Detect compute requirements in task."""
    plan = {
        "id": "test-plan-1",
        "tasks": [
            {
                "id": "task-1",
                "title": "Compute Planning",
                "description": "Define compute requirements with 8 vCPU and instance types",
            }
        ],
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert len(result.rows) == 1
    assert result.rows[0].compute_requirements == "present"


def test_storage_needs_detected():
    """Detect storage needs in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Storage Planning",
                "description": "Configure storage needs with 500GB EBS capacity",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].storage_needs == "present"


def test_network_bandwidth_detected():
    """Detect network bandwidth in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Network Planning",
                "description": "Plan network bandwidth for 10Gbps throughput and data transfer",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].network_bandwidth == "present"


def test_scaling_triggers_detected():
    """Detect scaling triggers in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Auto-scaling Setup",
                "description": "Configure scaling triggers at 80% CPU threshold with scaling policy",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].scaling_triggers == "present"


def test_resource_constraints_detected():
    """Detect resource constraints in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Quota Planning",
                "description": "Review resource constraints and API limits quota",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].resource_constraints == "present"


def test_bottlenecks_detected():
    """Detect bottlenecks in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Performance Analysis",
                "description": "Identify performance bottlenecks and saturation points",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].bottlenecks == "present"


def test_cost_spikes_detected():
    """Detect cost spikes in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Cost Monitoring",
                "description": "Set up alerts for cost spikes and unexpected costs",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].cost_spikes == "present"


def test_availability_zones_detected():
    """Detect availability zones in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "HA Setup",
                "description": "Deploy multi-AZ configuration for high availability",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].availability_zones == "present"


def test_sizing_accuracy_detected():
    """Detect sizing accuracy in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Right-sizing",
                "description": "Perform right-sizing analysis for sizing accuracy",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].sizing_accuracy == "present"


def test_headroom_buffers_detected():
    """Detect headroom buffers in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Capacity Buffer",
                "description": "Define capacity buffer and headroom for unexpected load",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].headroom_buffers == "present"


def test_autoscaling_config_detected():
    """Detect autoscaling configuration in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "ASG Configuration",
                "description": "Configure autoscaling group with min and max instances",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].autoscaling_config == "present"


def test_cost_projections_detected():
    """Detect cost projections in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Budget Planning",
                "description": "Create cost projections and monthly cost estimates",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].cost_projections == "present"


def test_optimal_capacity_score():
    """Task with core + scaling + optimization signals should get optimal score."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Complete Infrastructure Planning",
                "description": (
                    "Define compute requirements with 16 vCPU instances. "
                    "Configure storage needs with 1TB EBS. "
                    "Plan network bandwidth for 25Gbps throughput. "
                    "Set up autoscaling group with scaling triggers at 70% CPU. "
                    "Deploy multi-AZ for high availability. "
                    "Perform right-sizing analysis with capacity headroom. "
                    "Create cost projections and budget estimates."
                ),
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].capacity_score == "optimal"


def test_adequate_capacity_score():
    """Task with core + some scaling signals should get adequate score."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Basic Infrastructure Planning",
                "description": (
                    "Define compute requirements with 8 vCPU. "
                    "Configure storage needs with 500GB EBS. "
                    "Set up scaling triggers at 80% CPU."
                ),
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].capacity_score == "adequate"


def test_at_risk_capacity_score():
    """Task with missing core signals should get at_risk score."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Minimal Planning",
                "description": "Basic infrastructure setup with autoscaling",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].capacity_score == "at_risk"


def test_multiple_tasks_analyzed():
    """Multiple tasks should be analyzed independently."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "Compute requirements and storage needs",
            },
            {
                "id": "task-2",
                "title": "Task 2",
                "description": "Network bandwidth and scaling triggers",
            },
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert len(result.rows) == 2
    assert result.rows[0].compute_requirements == "present"
    assert result.rows[1].network_bandwidth == "present"


def test_summary_statistics():
    """Summary should contain accurate statistics."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Optimal Task",
                "description": (
                    "Compute requirements, storage needs, network bandwidth, "
                    "scaling triggers, autoscaling group, multi-AZ, "
                    "right-sizing, headroom, cost projections"
                ),
            },
            {
                "id": "task-2",
                "title": "Adequate Task",
                "description": "Compute requirements, storage needs, scaling triggers",
            },
            {
                "id": "task-3",
                "title": "At Risk Task",
                "description": "Basic setup",
            },
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.summary["task_count"] == 3
    assert result.summary["optimal_count"] == 1
    assert result.summary["adequate_count"] == 1
    assert result.summary["at_risk_count"] == 1
    assert result.summary["overall_capacity_score"] > 0


def test_to_dict_method():
    """Test to_dict() serialization."""
    plan = {
        "id": "test-plan",
        "tasks": [
            {
                "id": "task-1",
                "title": "Test Task",
                "description": "Compute requirements",
            }
        ],
    }

    result = generate_infrastructure_capacity_matrix(plan)
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
                "description": "Compute requirements",
            }
        ],
    }

    result = generate_infrastructure_capacity_matrix(plan)
    markdown = result.to_markdown()

    assert isinstance(markdown, str)
    assert "# Infrastructure Capacity Matrix" in markdown
    assert "## Summary" in markdown
    assert "## Matrix" in markdown


def test_row_to_dict_method():
    """Test MatrixRow to_dict() serialization."""
    row = InfrastructureCapacityMatrixRow(
        task_id="task-1",
        title="Test Task",
        compute_requirements="present",
        storage_needs="missing",
        capacity_score="adequate",
        evidence=("compute", "requirements"),
        recommendations=("Add storage needs",),
    )

    result = row.to_dict()

    assert isinstance(result, dict)
    assert result["task_id"] == "task-1"
    assert result["compute_requirements"] == "present"
    assert isinstance(result["evidence"], list)
    assert isinstance(result["recommendations"], list)


def test_empty_plan_markdown():
    """Empty plan should render informative markdown."""
    result = generate_infrastructure_capacity_matrix({"tasks": []})
    markdown = result.to_markdown()

    assert "No infrastructure capacity signals detected" in markdown


def test_case_insensitive_matching():
    """Pattern matching should be case-insensitive."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "description": "COMPUTE REQUIREMENTS and STORAGE NEEDS",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].compute_requirements == "present"
    assert result.rows[0].storage_needs == "present"


def test_acceptance_criteria_scanned():
    """Acceptance criteria should be scanned for signals."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "acceptance_criteria": [
                    "Compute requirements documented",
                    "Scaling triggers configured",
                ],
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].compute_requirements == "present"
    assert result.rows[0].scaling_triggers == "present"


def test_recommendations_generated_for_missing_signals():
    """Recommendations should be generated for missing signals."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Incomplete Task",
                "description": "Basic infrastructure",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert len(result.rows[0].recommendations) > 0
    assert any("compute" in rec.lower() for rec in result.rows[0].recommendations)


def test_recommendations_include_compute_when_missing():
    """Should recommend compute requirements when missing."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "description": "Storage needs only",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)
    recommendations = result.rows[0].recommendations

    assert any("compute" in rec.lower() for rec in recommendations)


def test_recommendations_include_storage_when_missing():
    """Should recommend storage needs when missing."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "description": "Compute requirements only",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)
    recommendations = result.rows[0].recommendations

    assert any("storage" in rec.lower() for rec in recommendations)


def test_recommendations_include_network_when_missing():
    """Should recommend network bandwidth when missing."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "description": "Compute and storage defined",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)
    recommendations = result.rows[0].recommendations

    assert any("network" in rec.lower() for rec in recommendations)


def test_markdown_includes_recommendations_section():
    """Markdown should include recommendations when present."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Incomplete Task",
                "description": "Basic setup",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)
    markdown = result.to_markdown()

    assert "## Recommendations" in markdown


def test_burst_capacity_edge_case():
    """Test burst capacity scenario with temporary load spikes."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Burst Capacity Planning",
                "description": (
                    "Handle burst capacity with autoscaling policy "
                    "and capacity headroom buffer for temporary spikes. "
                    "Define compute requirements with 8 vCPU, "
                    "storage needs with 200GB, and scaling triggers."
                ),
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].autoscaling_config == "present"
    assert result.rows[0].headroom_buffers == "present"
    assert result.rows[0].compute_requirements == "present"
    assert result.rows[0].storage_needs == "present"
    assert result.rows[0].capacity_score in ["optimal", "adequate"]


def test_multi_region_deployment_edge_case():
    """Test multi-region deployment scenario."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Multi-Region Deployment",
                "description": (
                    "Deploy across multiple availability zones with cross-region redundancy. "
                    "Configure compute requirements and network bandwidth for regional traffic. "
                    "Set up autoscaling group in each region."
                ),
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].availability_zones == "present"
    assert result.rows[0].compute_requirements == "present"
    assert result.rows[0].network_bandwidth == "present"
    assert result.rows[0].autoscaling_config == "present"


def test_reserved_instances_edge_case():
    """Test reserved instances scenario with cost optimization."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Reserved Instance Planning",
                "description": (
                    "Plan reserved instances for compute requirements with right-sizing. "
                    "Create cost projections for annual cost savings. "
                    "Configure storage needs and network bandwidth."
                ),
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert result.rows[0].compute_requirements == "present"
    assert result.rows[0].cost_projections == "present"
    assert result.rows[0].sizing_accuracy == "present"


def test_bottleneck_with_constraints_recommendation():
    """Should recommend documenting constraints when bottlenecks present."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Performance Issues",
                "description": (
                    "Address performance bottlenecks. "
                    "Define compute requirements and storage needs."
                ),
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)
    recommendations = result.rows[0].recommendations

    assert result.rows[0].bottlenecks == "present"
    assert result.rows[0].resource_constraints == "missing"
    assert any("constraint" in rec.lower() for rec in recommendations)


def test_cost_spikes_with_projections_recommendation():
    """Should recommend cost monitoring when spikes detected but no projections."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Cost Issues",
                "description": (
                    "Handle unexpected cost spikes. "
                    "Define compute requirements and storage needs."
                ),
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)
    recommendations = result.rows[0].recommendations

    assert result.rows[0].cost_spikes == "present"
    assert result.rows[0].cost_projections == "missing"
    assert any("cost monitoring" in rec.lower() or "alert" in rec.lower() for rec in recommendations)


def test_overall_capacity_score_calculation():
    """Test overall capacity score calculation logic."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Optimal",
                "description": (
                    "Compute requirements, storage needs, network bandwidth, "
                    "scaling triggers, autoscaling, multi-AZ, right-sizing, headroom, cost projections"
                ),
            },
            {
                "id": "task-2",
                "title": "Adequate",
                "description": "Compute requirements, storage needs, scaling triggers",
            },
            {
                "id": "task-3",
                "title": "At Risk",
                "description": "Basic setup",
            },
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    # optimal=100, adequate=60, at_risk=0 -> (100 + 60 + 0) / 3 = 53.33 ~= 53
    assert result.summary["overall_capacity_score"] == 53


def test_evidence_collection():
    """Test that evidence is collected from pattern matches."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "description": "Compute requirements with 8 vCPU and storage needs",
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    assert len(result.rows[0].evidence) > 0


def test_no_recommendations_for_complete_task():
    """Complete task with all signals should have minimal recommendations."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Complete Task",
                "description": (
                    "Compute requirements, storage needs, network bandwidth, "
                    "scaling triggers, autoscaling group, multi-AZ deployment, "
                    "right-sizing analysis, capacity headroom, cost projections, "
                    "resource constraints, bottleneck analysis"
                ),
            }
        ]
    }

    result = generate_infrastructure_capacity_matrix(plan)

    # Should have very few or no recommendations when all signals present
    assert len(result.rows[0].recommendations) == 0
