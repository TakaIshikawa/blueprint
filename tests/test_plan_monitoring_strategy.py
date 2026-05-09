"""Tests for monitoring strategy matrix generator."""

from blueprint.plan_monitoring_strategy import (
    MonitoringStrategyMatrix,
    MonitoringStrategyMatrixRow,
    generate_monitoring_strategy_matrix,
)


def test_empty_plan_returns_empty_matrix():
    """Empty plan should return matrix with no rows."""
    result = generate_monitoring_strategy_matrix({"tasks": []})

    assert isinstance(result, MonitoringStrategyMatrix)
    assert len(result.rows) == 0
    assert result.summary["task_count"] == 0


def test_metrics_collection_detected():
    """Detect metrics collection in task."""
    plan = {
        "id": "test-plan-1",
        "tasks": [
            {
                "id": "task-1",
                "title": "Metrics Setup",
                "description": "Configure metrics collection for CPU and memory",
            }
        ],
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert len(result.rows) == 1
    assert result.rows[0].metrics_collection == "present"


def test_alerting_rules_detected():
    """Detect alerting rules in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Alerting Setup",
                "description": "Configure alerting rules and thresholds",
            }
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.rows[0].alerting_rules == "present"


def test_dashboards_detected():
    """Detect dashboard requirements in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Dashboard Setup",
                "description": "Create Grafana dashboards for monitoring",
            }
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.rows[0].dashboards == "present"


def test_log_aggregation_detected():
    """Detect log aggregation in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Logging Setup",
                "description": "Configure log aggregation with centralized logging",
            }
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.rows[0].log_aggregation == "present"


def test_tracing_strategy_detected():
    """Detect tracing strategy in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Tracing Setup",
                "description": "Implement distributed tracing with OpenTelemetry",
            }
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.rows[0].tracing_strategy == "present"


def test_blind_spots_detected():
    """Detect blind spots identification in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Coverage Analysis",
                "description": "Identify monitoring blind spots and gaps",
            }
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.rows[0].blind_spots_identified == "present"


def test_noisy_alerts_detected():
    """Detect noisy alerts handling in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Alert Optimization",
                "description": "Reduce noisy alerts and alert fatigue",
            }
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.rows[0].noisy_alerts_addressed == "present"


def test_sli_defined_detected():
    """Detect SLI definition in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "SLI Definition",
                "description": "Define service level indicators and SLI metrics",
            }
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.rows[0].sli_defined == "present"


def test_retention_policy_detected():
    """Detect retention policy in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Data Retention",
                "description": "Configure metrics retention policy",
            }
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.rows[0].retention_policy == "present"


def test_incident_detection_detected():
    """Detect incident detection in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Incident Management",
                "description": "Implement incident detection and anomaly detection",
            }
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.rows[0].incident_detection == "present"


def test_comprehensive_coverage_score():
    """Task with 7+ signals should get comprehensive score."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Complete Monitoring",
                "description": (
                    "Configure metrics collection with alerting rules. "
                    "Create dashboards and log aggregation. "
                    "Implement distributed tracing and define SLI. "
                    "Set retention policy and incident detection. "
                    "Address noisy alerts and blind spots."
                ),
            }
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.rows[0].coverage_score == "comprehensive"


def test_partial_coverage_score():
    """Task with 4-6 signals should get partial score."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Partial Monitoring",
                "description": (
                    "Configure metrics collection with alerting rules. "
                    "Create dashboards and implement log aggregation. "
                    "Define SLI metrics."
                ),
            }
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.rows[0].coverage_score == "partial"


def test_minimal_coverage_score():
    """Task with <4 signals should get minimal score."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Minimal Monitoring",
                "description": "Basic monitoring setup",
            }
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.rows[0].coverage_score == "minimal"


def test_multiple_tasks_analyzed():
    """Multiple tasks should be analyzed independently."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "Metrics collection and alerting rules",
            },
            {
                "id": "task-2",
                "title": "Task 2",
                "description": "Dashboards and log aggregation",
            },
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert len(result.rows) == 2
    assert result.rows[0].metrics_collection == "present"
    assert result.rows[1].dashboards == "present"


def test_summary_statistics():
    """Summary should contain accurate statistics."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Comprehensive Task",
                "description": (
                    "Metrics collection, alerting rules, dashboards, "
                    "log aggregation, distributed tracing, SLI, "
                    "retention policy, incident detection"
                ),
            },
            {
                "id": "task-2",
                "title": "Partial Task",
                "description": "Metrics collection, alerting rules, dashboards, SLI",
            },
            {
                "id": "task-3",
                "title": "Minimal Task",
                "description": "Basic monitoring",
            },
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.summary["task_count"] == 3
    assert result.summary["comprehensive_count"] == 1
    assert result.summary["partial_count"] == 1
    assert result.summary["minimal_count"] == 1
    assert result.summary["overall_coverage"] > 0


def test_to_dict_method():
    """Test to_dict() serialization."""
    plan = {
        "id": "test-plan",
        "tasks": [
            {
                "id": "task-1",
                "title": "Test Task",
                "description": "Metrics collection",
            }
        ],
    }

    result = generate_monitoring_strategy_matrix(plan)
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
                "description": "Metrics collection",
            }
        ],
    }

    result = generate_monitoring_strategy_matrix(plan)
    markdown = result.to_markdown()

    assert isinstance(markdown, str)
    assert "# Monitoring Strategy Matrix" in markdown
    assert "## Summary" in markdown
    assert "## Matrix" in markdown


def test_row_to_dict_method():
    """Test MatrixRow to_dict() serialization."""
    row = MonitoringStrategyMatrixRow(
        task_id="task-1",
        title="Test Task",
        metrics_collection="present",
        alerting_rules="missing",
        coverage_score="partial",
        evidence=("metrics", "collection"),
    )

    result = row.to_dict()

    assert isinstance(result, dict)
    assert result["task_id"] == "task-1"
    assert result["metrics_collection"] == "present"
    assert isinstance(result["evidence"], list)


def test_empty_plan_markdown():
    """Empty plan should render informative markdown."""
    result = generate_monitoring_strategy_matrix({"tasks": []})
    markdown = result.to_markdown()

    assert "No monitoring strategy signals detected" in markdown


def test_acceptance_criteria_scanned():
    """Acceptance criteria should be scanned for signals."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "acceptance_criteria": [
                    "Metrics collection configured",
                    "Alerting rules defined",
                ],
            }
        ]
    }

    result = generate_monitoring_strategy_matrix(plan)

    assert result.rows[0].metrics_collection == "present"
    assert result.rows[0].alerting_rules == "present"
