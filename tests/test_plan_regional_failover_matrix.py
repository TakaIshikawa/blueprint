import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_regional_failover_matrix import (
    PlanRegionalFailoverMatrix,
    PlanRegionalFailoverMatrixRow,
    build_plan_regional_failover_matrix,
    derive_plan_regional_failover_matrix,
    extract_plan_regional_failover_matrix,
    generate_plan_regional_failover_matrix,
    plan_regional_failover_matrix_to_dict,
    plan_regional_failover_matrix_to_dicts,
    plan_regional_failover_matrix_to_markdown,
    summarize_plan_regional_failover_matrix,
)


def test_multi_region_tasks_and_milestones_create_readiness_rows():
    result = build_plan_regional_failover_matrix(
        _plan(
            [
                _task(
                    "task-api",
                    title="Deploy public API to multiple regions",
                    description=(
                        "Multi-region deployment uses active-active traffic steering through the "
                        "global load balancer for customer-facing service availability."
                    ),
                    acceptance_criteria=[
                        "Regional health checks and synthetic monitoring validate availability by region."
                    ],
                    files_or_modules=["infra/multi-region/api.tf"],
                ),
                _task(
                    "task-copy",
                    title="Update empty-state copy",
                    description="Tighten dashboard wording.",
                ),
            ],
            milestones=[
                {
                    "name": "DR launch gate",
                    "description": "Disaster recovery readiness for regional outage failover.",
                    "acceptance_criteria": ["RTO and RPO are approved after DR rehearsal."],
                }
            ],
        )
    )

    assert isinstance(result, PlanRegionalFailoverMatrix)
    assert all(isinstance(row, PlanRegionalFailoverMatrixRow) for row in result.rows)
    assert result.plan_id == "plan-regional-failover"
    assert result.failover_source_ids == ("task-api", "DR launch gate")
    assert result.no_signal_source_ids == ("task-copy",)
    assert [row.region_failover_concern for row in result.rows] == [
        "multi_region_deployment",
        "dns_routing",
        "regional_availability",
        "regional_failover",
        "disaster_recovery",
    ]
    assert _row(result, "task-api", "dns_routing").component == "networking"
    assert _row(result, "DR launch gate", "disaster_recovery").source_type == "milestone"
    assert _row(result, "DR launch gate", "disaster_recovery").recommended_owner == "operations_owner"
    assert "RTO/RPO" in _row(result, "DR launch gate", "disaster_recovery").recommended_action
    assert result.summary["source_count"] == 3
    assert result.summary["failover_source_count"] == 2
    assert result.summary["no_signal_source_count"] == 1


def test_database_replica_task_infers_data_concern_and_validation_gap():
    result = build_plan_regional_failover_matrix(
        _plan(
            [
                _task(
                    "task-db",
                    title="Add database read replica failover",
                    description=(
                        "Create cross-region read replicas for the primary database and promote "
                        "secondary database during failover."
                    ),
                    metadata={"team": "data-platform"},
                )
            ]
        )
    )

    replica = _row(result, "task-db", "regional_replica")
    failover = _row(result, "task-db", "regional_failover")

    assert replica.component == "database"
    assert replica.recommended_owner == "data_platform_owner"
    assert replica.trigger_or_dependency == "replica promotion dependency"
    assert replica.data_replication_concern == "Replicated data needs consistency, lag, and promotion validation."
    assert replica.validation_gap == "Replica lag, consistency checks, and RPO validation are missing."
    assert failover.recommended_action == (
        "Name the failover trigger, decision owner, traffic shift path, and regional rollback validation."
    )
    assert result.summary["concern_counts"]["regional_replica"] == 1
    assert result.summary["component_counts"]["database"] == 2


def test_unrelated_empty_and_invalid_inputs_are_deterministic_empty_matrices():
    no_signal = build_plan_regional_failover_matrix(
        _plan(
            [
                _task("task-api", title="Optimize API pagination", description="Tune query limits."),
                _task("task-ui", title="Polish UI labels", description="Update static copy."),
            ],
            plan_id="no-failover",
        )
    )
    empty = build_plan_regional_failover_matrix({"id": "empty-plan", "tasks": [], "milestones": []})
    invalid = build_plan_regional_failover_matrix(17)

    assert no_signal.rows == ()
    assert no_signal.failover_source_ids == ()
    assert no_signal.no_signal_source_ids == ("task-api", "task-ui")
    assert no_signal.to_markdown() == "\n".join(
        [
            "# Plan Regional Failover Matrix: no-failover",
            "",
            "Summary: 0 of 2 sources require regional failover planning (0 rows).",
            "",
            "No regional failover matrix rows were inferred.",
            "",
            "No failover signals: task-api, task-ui",
        ]
    )
    assert empty.to_dict() == {
        "plan_id": "empty-plan",
        "rows": [],
        "records": [],
        "failover_source_ids": [],
        "no_signal_source_ids": [],
        "summary": {
            "source_count": 0,
            "row_count": 0,
            "failover_source_count": 0,
            "no_signal_source_count": 0,
            "concern_counts": {
                "multi_region_deployment": 0,
                "regional_failover": 0,
                "dns_routing": 0,
                "regional_replica": 0,
                "disaster_recovery": 0,
                "regional_availability": 0,
            },
            "component_counts": {
                "application": 0,
                "database": 0,
                "infrastructure": 0,
                "networking": 0,
                "operations": 0,
                "data_platform": 0,
            },
            "failover_source_ids": [],
            "no_signal_source_ids": [],
        },
    }
    assert empty.to_markdown() == "\n".join(
        [
            "# Plan Regional Failover Matrix: empty-plan",
            "",
            "Summary: 0 of 0 sources require regional failover planning (0 rows).",
            "",
            "No regional failover matrix rows were inferred.",
        ]
    )
    assert invalid.plan_id is None
    assert invalid.rows == ()
    assert invalid.summary["row_count"] == 0


def test_serialization_aliases_model_list_object_input_and_markdown_are_stable():
    plan = _plan(
        [
            _task(
                "task-route | dns",
                title="DNS failover | Route53",
                description="Route53 DNS failover shifts traffic weights after health checks fail.",
                metadata={"owner": "network"},
            ),
            _task("task-copy", title="Copy refresh", description="Update labels."),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_regional_failover_matrix(ExecutionPlan.model_validate(plan))
    payload = plan_regional_failover_matrix_to_dict(result)
    markdown = plan_regional_failover_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_regional_failover_matrix(plan).to_dict() == result.to_dict()
    assert extract_plan_regional_failover_matrix(plan).to_dict() == result.to_dict()
    assert derive_plan_regional_failover_matrix(result) is result
    assert summarize_plan_regional_failover_matrix(result) == result.summary
    assert isinstance(summarize_plan_regional_failover_matrix(plan), PlanRegionalFailoverMatrix)
    assert plan_regional_failover_matrix_to_dicts(result) == payload["rows"]
    assert plan_regional_failover_matrix_to_dicts(result.records) == payload["records"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "failover_source_ids",
        "no_signal_source_ids",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "source_id",
        "source_type",
        "component",
        "region_failover_concern",
        "trigger_or_dependency",
        "data_replication_concern",
        "validation_gap",
        "recommended_owner",
        "recommended_action",
        "evidence",
    ]
    assert "`task-route \\| dns`" in markdown
    assert "| Source | Component | Concern | Trigger or Dependency | Data Replication | Validation Gap | Owner | Recommended Action | Evidence |" in markdown

    object_task = SimpleNamespace(
        id="task-object",
        title="Regional availability object task",
        description="Regional availability depends on regional health dashboard validation.",
        acceptance_criteria=["Done"],
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Replica lag validation",
            description="Validate replica lag and checksum reconciliation for cross-region replicas.",
        )
    )
    object_result = build_plan_regional_failover_matrix([object_task, model_task])

    assert object_result.plan_id is None
    assert object_result.failover_source_ids == ("task-object", "task-model")
    assert _row(object_result, "task-object", "regional_availability").validation_gap == (
        "Validation evidence is present; confirm it is exercised before launch."
    )
    assert _row(object_result, "task-model", "regional_replica").data_replication_concern == (
        "Replication consistency validation is specified."
    )


def _row(result, source_id, concern):
    return next(
        row
        for row in result.rows
        if row.source_id == source_id and row.region_failover_concern == concern
    )


def _plan(tasks, *, milestones=None, plan_id="plan-regional-failover"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-regional-failover",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [] if milestones is None else milestones,
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
):
    return {
        "id": task_id,
        "execution_plan_id": "plan-regional-failover",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "medium",
        "estimated_hours": 2.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
