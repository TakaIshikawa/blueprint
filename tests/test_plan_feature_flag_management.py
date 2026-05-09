"""Tests for feature flag management matrix generator."""

from blueprint.plan_feature_flag_management import (
    FeatureFlagManagementMatrix,
    FeatureFlagManagementMatrixRow,
    generate_feature_flag_management_matrix,
)


def test_empty_plan_returns_empty_matrix():
    """Empty plan should return matrix with no rows."""
    result = generate_feature_flag_management_matrix({"tasks": []})

    assert isinstance(result, FeatureFlagManagementMatrix)
    assert len(result.rows) == 0
    assert result.summary["task_count"] == 0


def test_flag_inventory_detected():
    """Detect flag inventory in task."""
    plan = {
        "id": "test-plan-1",
        "tasks": [
            {
                "id": "task-1",
                "title": "Feature Flag Setup",
                "description": "Create flag inventory and catalog",
            }
        ],
    }

    result = generate_feature_flag_management_matrix(plan)

    assert len(result.rows) == 1
    assert result.rows[0].flag_inventory == "present"


def test_ownership_assignments_detected():
    """Detect ownership assignments in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Flag Ownership",
                "description": "Assign flag owner and team responsibility",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].ownership_assignments == "present"


def test_cleanup_schedules_detected():
    """Detect cleanup schedules in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Flag Cleanup",
                "description": "Schedule flag cleanup and removal timeline",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].cleanup_schedules == "present"


def test_rollout_strategies_detected():
    """Detect rollout strategies in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Rollout Planning",
                "description": "Define gradual rollout strategy for feature",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].rollout_strategies == "present"


def test_targeting_rules_detected():
    """Detect targeting rules in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "User Targeting",
                "description": "Configure targeting rules and user segments",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].targeting_rules == "present"


def test_flag_tracking_detected():
    """Detect flag tracking in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Flag Monitoring",
                "description": "Set up flag tracking and usage metrics",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].flag_tracking == "present"


def test_owner_assignment_detected():
    """Detect owner assignment strategy in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Assignment Strategy",
                "description": "Define owner assignment process and DRI assignment",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].owner_assignment == "present"


def test_cleanup_scheduling_detected():
    """Detect cleanup scheduling strategy in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Cleanup Timeline",
                "description": "Create cleanup scheduling and retirement schedule",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].cleanup_scheduling == "present"


def test_gradual_rollout_detected():
    """Detect gradual rollout strategy in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Progressive Rollout",
                "description": "Implement gradual rollout with traffic ramping",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].gradual_rollout == "present"


def test_targeting_configuration_detected():
    """Detect targeting configuration in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Targeting Setup",
                "description": "Configure targeting rules and define cohorts",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].targeting_configuration == "present"


def test_dependency_mapping_detected():
    """Detect dependency mapping in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Flag Dependencies",
                "description": "Map flag dependencies and interactions",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].dependency_mapping == "present"


def test_kill_switch_setup_detected():
    """Detect kill switch setup in task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Emergency Controls",
                "description": "Set up kill-switch for emergency disable",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].kill_switch_setup == "present"


def test_comprehensive_management_score():
    """Task with 8+ signals should get comprehensive score."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Complete Flag Management",
                "description": (
                    "Create flag inventory with ownership assignments. "
                    "Schedule cleanup and define rollout strategy. "
                    "Configure targeting rules with flag tracking. "
                    "Implement owner assignment and cleanup scheduling. "
                    "Enable gradual rollout with targeting configuration. "
                    "Map flag dependencies and set up kill-switch."
                ),
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].management_score == "comprehensive"


def test_partial_management_score():
    """Task with 4-7 signals should get partial score."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Partial Flag Management",
                "description": (
                    "Create flag inventory with ownership assignments. "
                    "Define rollout strategy and configure targeting rules. "
                    "Set up flag tracking."
                ),
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].management_score == "partial"


def test_minimal_management_score():
    """Task with <4 signals should get minimal score."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Basic Flag Management",
                "description": "Basic feature flag setup",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].management_score == "minimal"


def test_multiple_tasks_analyzed():
    """Multiple tasks should be analyzed independently."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Task 1",
                "description": "Flag inventory and ownership assignments",
            },
            {
                "id": "task-2",
                "title": "Task 2",
                "description": "Rollout strategy and targeting rules",
            },
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert len(result.rows) == 2
    assert result.rows[0].flag_inventory == "present"
    assert result.rows[1].rollout_strategies == "present"


def test_summary_statistics():
    """Summary should contain accurate statistics."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Comprehensive Task",
                "description": (
                    "Flag inventory, ownership assignments, cleanup schedules, "
                    "rollout strategies, targeting rules, flag tracking, "
                    "owner assignment, cleanup scheduling, gradual rollout, "
                    "targeting configuration, dependency mapping, kill-switch"
                ),
            },
            {
                "id": "task-2",
                "title": "Partial Task",
                "description": "Flag inventory, rollout strategy, targeting rules, kill-switch setup",
            },
            {
                "id": "task-3",
                "title": "Minimal Task",
                "description": "Basic flag setup",
            },
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

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
                "description": "Flag inventory",
            }
        ],
    }

    result = generate_feature_flag_management_matrix(plan)
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
                "description": "Flag inventory",
            }
        ],
    }

    result = generate_feature_flag_management_matrix(plan)
    markdown = result.to_markdown()

    assert isinstance(markdown, str)
    assert "# Feature Flag Management Matrix" in markdown
    assert "## Summary" in markdown
    assert "## Matrix" in markdown


def test_row_to_dict_method():
    """Test MatrixRow to_dict() serialization."""
    row = FeatureFlagManagementMatrixRow(
        task_id="task-1",
        title="Test Task",
        flag_inventory="present",
        ownership_assignments="missing",
        management_score="partial",
        evidence=("inventory", "tracking"),
    )

    result = row.to_dict()

    assert isinstance(result, dict)
    assert result["task_id"] == "task-1"
    assert result["flag_inventory"] == "present"
    assert isinstance(result["evidence"], list)


def test_empty_plan_markdown():
    """Empty plan should render informative markdown."""
    result = generate_feature_flag_management_matrix({"tasks": []})
    markdown = result.to_markdown()

    assert "No feature flag management signals detected" in markdown


def test_case_insensitive_matching():
    """Pattern matching should be case-insensitive."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "description": "Create FLAG INVENTORY with OWNERSHIP ASSIGNMENTS",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].flag_inventory == "present"
    assert result.rows[0].ownership_assignments == "present"


def test_acceptance_criteria_scanned():
    """Acceptance criteria should be scanned for signals."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "acceptance_criteria": [
                    "Flag inventory created",
                    "Cleanup schedule defined",
                ],
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].flag_inventory == "present"
    assert result.rows[0].cleanup_schedules == "present"


def test_all_signals_missing():
    """Task with no management signals should have all missing."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Unrelated Task",
                "description": "This is about something else entirely",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    row = result.rows[0]
    assert row.flag_inventory == "missing"
    assert row.ownership_assignments == "missing"
    assert row.cleanup_schedules == "missing"
    assert row.rollout_strategies == "missing"
    assert row.targeting_rules == "missing"
    assert row.management_score == "minimal"


def test_evidence_collection():
    """Evidence should be collected from matched patterns."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Flag Management",
                "description": "Create flag inventory and ownership assignments",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert len(result.rows[0].evidence) > 0


def test_evidence_limited_to_five():
    """Evidence should be limited to 5 items."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Complete Management",
                "description": (
                    "Flag inventory, ownership assignments, cleanup schedules, "
                    "rollout strategies, targeting rules, flag tracking, "
                    "owner assignment, cleanup scheduling, gradual rollout, "
                    "targeting configuration, dependency mapping, kill-switch"
                ),
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert len(result.rows[0].evidence) <= 5


def test_plan_id_preserved():
    """Plan ID should be preserved in result."""
    plan = {
        "id": "my-special-plan",
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "description": "Flag inventory",
            }
        ],
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.plan_id == "my-special-plan"


def test_markdown_escapes_pipe_characters():
    """Markdown should escape pipe characters in cells."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test | with | pipes",
                "description": "Flag inventory",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)
    markdown = result.to_markdown()

    assert "Test \\| with \\| pipes" in markdown


def test_overall_coverage_calculation():
    """Overall coverage should be calculated correctly."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Comprehensive",
                "description": (
                    "Flag inventory, ownership assignments, cleanup schedule, rollout strategy, targeting rules, "
                    "flag tracking, owner assignment, cleanup scheduling, gradual rollout, targeting configuration, "
                    "flag dependencies, kill-switch"
                ),
            },
            {
                "id": "task-2",
                "title": "Partial",
                "description": "Flag inventory, rollout strategy, targeting rules, kill-switch",
            },
            {
                "id": "task-3",
                "title": "Minimal",
                "description": "Nothing relevant",
            },
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    # (1 comprehensive + 0.5 * 1 partial) / 3 tasks * 100 = 50%
    assert result.summary["overall_coverage"] == 50


def test_requirements_field_scanned():
    """Requirements field should be scanned for signals."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "requirements": ["Flag inventory must be created", "Kill-switch required"],
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].flag_inventory == "present"
    assert result.rows[0].kill_switch_setup == "present"


def test_notes_field_scanned():
    """Notes field should be scanned for signals."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Test",
                "notes": "Remember to set up gradual rollout",
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].gradual_rollout == "present"


def test_mixed_signal_detection():
    """Different signals should be detected correctly in same task."""
    plan = {
        "tasks": [
            {
                "id": "task-1",
                "title": "Flag Setup",
                "description": "Create flag inventory",
                "acceptance_criteria": ["Cleanup schedule defined"],
                "requirements": ["Gradual rollout enabled"],
            }
        ]
    }

    result = generate_feature_flag_management_matrix(plan)

    assert result.rows[0].flag_inventory == "present"
    assert result.rows[0].cleanup_schedules == "present"
    assert result.rows[0].gradual_rollout == "present"
