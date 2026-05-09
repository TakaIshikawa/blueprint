"""Tests for API deprecation matrix generator."""

import json

from blueprint.plan_api_deprecation import (
    ApiDeprecationMatrix,
    ApiDeprecationMatrixGenerator,
    ApiDeprecationMatrixRow,
    analyze_api_deprecation_matrix,
    api_deprecation_matrix_to_dict,
    api_deprecation_matrix_to_dicts,
    api_deprecation_matrix_to_markdown,
    build_api_deprecation_matrix,
    generate_api_deprecation_matrix,
)


def test_extract_deprecated_endpoints_and_sunset_timelines():
    """Test extraction of deprecated endpoints and sunset timelines."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-deprecate-v1",
                title="Deprecate v1 REST API endpoints",
                description="Sunset /api/v1/users and /api/v1/orders endpoints by Q2 2026",
                acceptance_criteria=[
                    "Endpoints clearly identified: /api/v1/users, /api/v1/orders",
                    "Sunset date set to 2026-06-30",
                ],
            ),
        ])
    )

    assert isinstance(result, ApiDeprecationMatrix)
    assert len(result.rows) == 1
    row = result.rows[0]
    assert isinstance(row, ApiDeprecationMatrixRow)
    assert row.task_id == "task-deprecate-v1"
    assert row.deprecated_endpoints == "present"
    assert row.sunset_timeline == "present"


def test_identify_client_impact_and_migration_paths():
    """Test identification of client impact and migration paths."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-migrate-partners",
                title="Migrate partner integrations from v1 to v2 API",
                description="Identify affected clients and provide migration guide to v2 endpoints",
                acceptance_criteria=[
                    "Client impact assessment completed for 50+ partner integrations",
                    "Migration path documented with step-by-step upgrade instructions",
                    "Alternative v2 endpoints specified as replacements",
                ],
            ),
        ])
    )

    row = result.rows[0]
    assert row.client_impact == "present"
    assert row.migration_path == "present"
    assert row.replacement_api == "present"


def test_readiness_scoring_various_scenarios():
    """Test readiness scoring for various deprecation scenarios."""
    # Scenario 1: Fully ready deprecation
    result_ready = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-ready",
                title="Sunset legacy webhook API",
                description=(
                    "Deprecate /webhooks/v1 endpoint with sunset date 2026-12-31. "
                    "Replacement available at /webhooks/v2. "
                    "All 25 downstream consumers identified. "
                    "Migration guide published with communication plan. "
                    "Grace period of 6 months with dual-write support. "
                    "Metrics tracking dashboard and rollback procedure in place."
                ),
                acceptance_criteria=[
                    "Endpoint deprecated: /webhooks/v1",
                    "Timeline: sunset 2026-12-31",
                    "Client impact analyzed",
                    "Communication sent to customers",
                ],
            ),
        ])
    )

    ready_row = result_ready.rows[0]
    assert ready_row.readiness == "ready"
    assert ready_row.readiness_score > 0.9
    assert len(ready_row.gaps) == 0

    # Scenario 2: Partial readiness
    result_partial = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-partial",
                title="Deprecate GraphQL schema field",
                description=(
                    "Deprecate GraphQL field with sunset timeline 2026-06-30. "
                    "Replacement field available. "
                    "Client impact needs assessment."
                ),
                acceptance_criteria=[
                    "Field identified for deprecation",
                    "Alternative field exists",
                    "Timeline set",
                ],
            ),
        ])
    )

    partial_row = result_partial.rows[0]
    assert partial_row.readiness == "partial"
    assert 0.3 < partial_row.readiness_score < 0.7
    assert len(partial_row.gaps) > 0

    # Scenario 3: Blocked deprecation (missing multiple critical fields)
    result_blocked = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-blocked",
                title="Remove old API",
                description="Deprecate API",  # Minimal info - missing endpoint, timeline, clients, communication
                acceptance_criteria=["Remove it"],
            ),
        ])
    )

    blocked_row = result_blocked.rows[0]
    assert blocked_row.readiness == "blocked"
    assert blocked_row.readiness_score < 0.5


def test_matrix_generation_with_multiple_deprecated_apis():
    """Test matrix generation with multiple deprecated APIs."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-1",
                title="Sunset REST v1 API",
                description=(
                    "Deprecate /api/v1 endpoints by Q3 2026. "
                    "Clients migrating to v2. Communication plan ready."
                ),
                acceptance_criteria=[
                    "Sunset timeline: 2026-09-30",
                    "Client impact: 100+ integrations",
                    "Migration guide published",
                ],
            ),
            _task(
                "task-2",
                title="Remove deprecated GraphQL fields",
                description=(
                    "Remove legacy fields from schema with replacement fields available. "
                    "Metrics tracking active usage."
                ),
                acceptance_criteria=[
                    "Deprecated fields identified",
                    "Alternatives documented",
                    "Monitoring in place",
                ],
            ),
            _task(
                "task-3",
                title="Retire webhook v1",
                description=(
                    "End-of-life for /webhooks/v1. "
                    "All consumers notified. "
                    "Rollback plan available."
                ),
                acceptance_criteria=[
                    "Webhook endpoint deprecated",
                    "Communication sent",
                    "Rollback procedure documented",
                ],
            ),
        ])
    )

    assert len(result.rows) == 3
    assert result.summary["deprecation_task_count"] == 3
    assert result.deprecation_task_ids == ("task-1", "task-2", "task-3")


def test_edge_case_partial_deprecation():
    """Test edge case: partial deprecation where some endpoints remain."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-partial-deprecation",
                title="Deprecate subset of v2 API methods",
                description=(
                    "Sunset POST /api/v2/orders but keep GET /api/v2/orders. "
                    "Migration path to POST /api/v3/orders. "
                    "Communication to affected clients."
                ),
                acceptance_criteria=[
                    "Endpoint: POST /api/v2/orders deprecated",
                    "Replacement: POST /api/v3/orders",
                    "GET /api/v2/orders remains active",
                    "Client notifications sent",
                ],
            ),
        ])
    )

    row = result.rows[0]
    assert row.deprecated_endpoints == "present"
    assert row.replacement_api == "present"
    assert row.communication_plan == "present"


def test_edge_case_version_based_deprecation():
    """Test edge case: version-based deprecation with version numbers."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-version-deprecation",
                title="Upgrade API from v1 to v3, deprecate v2",
                description=(
                    "Deprecate v2 API in favor of v3. "
                    "v1 already sunset. "
                    "Timeline: v2 sunset by Q4 2026. "
                    "Migration guide from v2 to v3 available."
                ),
                acceptance_criteria=[
                    "API v2 endpoints identified",
                    "Sunset date: 2026-12-31",
                    "Migration path to v3 documented",
                    "Client impact assessed",
                ],
            ),
        ])
    )

    row = result.rows[0]
    assert row.deprecated_endpoints == "present"
    assert row.sunset_timeline == "present"
    assert row.migration_path == "present"


def test_edge_case_emergency_deprecation():
    """Test edge case: emergency deprecation due to security issues."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-emergency",
                title="Emergency deprecation of vulnerable endpoint",
                description=(
                    "Immediately deprecate /api/auth/legacy due to security vulnerability. "
                    "Breaking change required. "
                    "Replacement endpoint /api/auth/v2 available. "
                    "Urgent communication to all clients. "
                    "Rollback not recommended due to security risk."
                ),
                acceptance_criteria=[
                    "Endpoint /api/auth/legacy deprecated immediately",
                    "Security issue documented",
                    "Replacement /api/auth/v2 ready",
                    "Emergency notifications sent",
                    "Breaking change accepted",
                ],
            ),
        ])
    )

    row = result.rows[0]
    assert row.deprecated_endpoints == "present"
    assert row.replacement_api == "present"
    assert row.communication_plan == "present"
    assert row.breaking_changes == "present"


def test_unrelated_api_tasks_excluded():
    """Test that non-deprecation API tasks are excluded."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-new-api",
                title="Add new REST API endpoint",
                description="Create new endpoint /api/v2/products for product catalog",
                acceptance_criteria=["Endpoint created", "Tests passing"],
            ),
            _task(
                "task-update",
                title="Update API documentation",
                description="Refresh API docs with latest endpoint changes",
                acceptance_criteria=["Docs updated"],
            ),
        ])
    )

    assert len(result.rows) == 0
    assert result.summary["deprecation_task_count"] == 0
    assert len(result.no_deprecation_task_ids) == 2


def test_generator_class_extract_signals():
    """Test ApiDeprecationMatrixGenerator signal extraction."""
    generator = ApiDeprecationMatrixGenerator()

    task = _task(
        "test-task",
        title="Deprecate endpoint with sunset timeline",
        description=(
            "Sunset /api/v1/orders by 2026-06-30. "
            "Migration guide available. "
            "Client impact assessment completed. "
            "Communication plan ready."
        ),
    )

    signals = generator.extract_deprecation_signals(task)

    assert signals["deprecated_endpoints"] == "present"
    assert signals["sunset_timeline"] == "present"
    assert signals["migration_path"] == "present"
    assert signals["client_impact"] == "present"
    assert signals["communication_plan"] == "present"


def test_generator_class_calculate_readiness_score():
    """Test ApiDeprecationMatrixGenerator readiness score calculation."""
    generator = ApiDeprecationMatrixGenerator()

    # All critical signals present
    signals_complete = {
        "deprecated_endpoints": "present",
        "sunset_timeline": "present",
        "client_impact": "present",
        "communication_plan": "present",
        "migration_path": "present",
        "replacement_api": "present",
        "grace_period": "present",
        "metrics_tracking": "present",
        "rollback_scenario": "present",
        "breaking_changes": "present",
    }

    score_complete = generator.calculate_readiness_score(signals_complete)
    assert score_complete == 1.0

    # Only critical signals present
    signals_critical_only = {
        "deprecated_endpoints": "present",
        "sunset_timeline": "present",
        "client_impact": "present",
        "communication_plan": "present",
        "migration_path": "missing",
        "replacement_api": "missing",
        "grace_period": "missing",
        "metrics_tracking": "missing",
        "rollback_scenario": "missing",
        "breaking_changes": "missing",
    }

    score_critical = generator.calculate_readiness_score(signals_critical_only)
    assert 0.4 < score_critical < 0.7  # Critical signals are weighted higher

    # No signals present
    signals_empty = {key: "missing" for key in signals_complete}
    score_empty = generator.calculate_readiness_score(signals_empty)
    assert score_empty == 0.0


def test_generator_class_generate_method():
    """Test ApiDeprecationMatrixGenerator.generate() method."""
    generator = ApiDeprecationMatrixGenerator()

    plan = _plan([
        _task(
            "task-test",
            title="Deprecate API endpoint",
            description="Sunset /api/v1/test by Q2 2026 with migration to v2",
        ),
    ])

    result = generator.generate(plan)

    assert isinstance(result, ApiDeprecationMatrix)
    assert len(result.rows) == 1


def test_to_dict_serialization():
    """Test serialization to dictionary."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-1",
                title="Deprecate endpoint",
                description="Sunset API endpoint with timeline and communication",
            ),
        ])
    )

    result_dict = api_deprecation_matrix_to_dict(result)

    assert isinstance(result_dict, dict)
    assert "plan_id" in result_dict
    assert "rows" in result_dict
    assert "records" in result_dict
    assert "deprecation_task_ids" in result_dict
    assert "summary" in result_dict
    assert "recommendations" in result_dict

    # Ensure JSON serializable
    json_str = json.dumps(result_dict)
    assert json_str


def test_to_dicts_serialization():
    """Test serialization to list of dictionaries."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-1",
                title="Deprecate v1",
                description="Deprecate API v1",
            ),
            _task(
                "task-2",
                title="Deprecate v2",
                description="Deprecate API v2",
            ),
        ])
    )

    dicts = api_deprecation_matrix_to_dicts(result)

    assert isinstance(dicts, list)
    assert len(dicts) == 2
    assert all(isinstance(d, dict) for d in dicts)


def test_to_markdown_rendering():
    """Test Markdown rendering."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-deprecate",
                title="Sunset API endpoint",
                description=(
                    "Deprecate /api/v1/orders by 2026-06-30. "
                    "Migration to v2. Client impact assessed. Communication ready."
                ),
            ),
        ])
    )

    markdown = api_deprecation_matrix_to_markdown(result)

    assert "# API Deprecation Matrix" in markdown
    assert "## Deprecation Readiness" in markdown
    assert "task-deprecate" in markdown
    assert "## Gaps and Recommendations" in markdown
    assert "## General Recommendations" in markdown


def test_markdown_empty_matrix():
    """Test Markdown rendering for empty matrix."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-unrelated",
                title="Add feature",
                description="Add new feature to application",
            ),
        ])
    )

    markdown = result.to_markdown()

    assert "# API Deprecation Matrix" in markdown
    assert "No API deprecation tasks were identified." in markdown


def test_recommendations_generated():
    """Test that recommendations are generated based on gaps."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-1",
                title="Deprecate endpoint without timeline",
                description="Deprecate API endpoint",
            ),
            _task(
                "task-2",
                title="Deprecate endpoint without communication",
                description="Deprecate API endpoint with sunset date 2026-12-31",
            ),
        ])
    )

    assert len(result.recommendations) > 0
    assert any("grace period" in rec.lower() for rec in result.recommendations)
    assert any("metrics" in rec.lower() for rec in result.recommendations)


def test_summary_statistics():
    """Test summary statistics generation."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-ready",
                title="Ready deprecation",
                description=(
                    "Deprecate endpoint /api/v1 by sunset date 2026-12-31. "
                    "Migration path to v2 documented with replacement API. "
                    "Client impact assessed for all consumers. "
                    "Communication plan ready for customer notification. "
                    "Grace period with metrics tracking. "
                    "Rollback available."
                ),
            ),
            _task(
                "task-partial",
                title="Partial deprecation",
                description=(
                    "Deprecate API endpoint with sunset timeline 2026-06-30. "
                    "Client impact assessment in progress. "
                    # Missing: communication plan, migration path, etc.
                ),
            ),
            _task(
                "task-blocked",
                title="Blocked API deprecation",
                description="Deprecate API",  # Minimal - missing critical fields
            ),
        ])
    )

    summary = result.summary

    assert summary["task_count"] == 3
    assert summary["deprecation_task_count"] == 3
    assert summary["readiness_counts"]["ready"] == 1
    assert summary["readiness_counts"]["partial"] == 1
    assert summary["readiness_counts"]["blocked"] == 1
    assert "average_readiness_score" in summary
    assert summary["blocked_count"] == 1
    assert summary["partial_count"] == 1
    assert summary["ready_count"] == 1


def test_analyze_api_deprecation_matrix_passthrough():
    """Test analyze function with existing matrix."""
    original = build_api_deprecation_matrix(_plan([]))

    result = analyze_api_deprecation_matrix(original)

    assert result is original


def test_generate_api_deprecation_matrix_alias():
    """Test generate function alias."""
    plan = _plan([
        _task("task-1", title="Deprecate API", description="Deprecate endpoint"),
    ])

    result = generate_api_deprecation_matrix(plan)

    assert isinstance(result, ApiDeprecationMatrix)


def test_execution_task_single_task():
    """Test with a single ExecutionTask-like object."""
    from blueprint.domain.models import ExecutionTask

    task = ExecutionTask(
        id="task-single",
        title="Deprecate webhook endpoint",
        description="Sunset /webhooks/v1 by 2026-12-31 with migration to v2",
        depends_on=[],
        files_or_modules=[],
        acceptance_criteria=["Endpoint deprecated", "Timeline set"],
        status="pending",
    )

    result = build_api_deprecation_matrix(task)

    assert len(result.rows) == 1
    assert result.rows[0].task_id == "task-single"


def test_breaking_changes_detected():
    """Test detection of breaking changes."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-breaking",
                title="API breaking change",
                description=(
                    "Deprecate endpoint with backwards incompatible changes. "
                    "Non-backward-compatible update required."
                ),
            ),
        ])
    )

    row = result.rows[0]
    assert row.breaking_changes == "present"


def test_grace_period_and_metrics_detected():
    """Test detection of grace period and metrics tracking."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-grace-metrics",
                title="Deprecate with grace period",
                description=(
                    "6-month grace period with dual-write support. "
                    "Monitor usage metrics with dashboard and alerts."
                ),
            ),
        ])
    )

    row = result.rows[0]
    assert row.grace_period == "present"
    assert row.metrics_tracking == "present"


def test_rollback_scenario_detected():
    """Test detection of rollback scenarios."""
    result = build_api_deprecation_matrix(
        _plan([
            _task(
                "task-rollback",
                title="Deprecate with rollback plan",
                description=(
                    "Deprecate API endpoint with documented rollback procedure. "
                    "Can revert or extend deadline if needed."
                ),
            ),
        ])
    )

    row = result.rows[0]
    assert row.rollback_scenario == "present"


# Helper functions


def _plan(tasks):
    """Create a plan dictionary for testing."""
    return {
        "id": "plan-api-deprecation",
        "implementation_brief_id": "brief-api-deprecation",
        "milestones": [],
        "tasks": tasks,
    }


def _task(task_id, *, title=None, description=None, acceptance_criteria=None):
    """Create a task dictionary for testing."""
    return {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [],
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
