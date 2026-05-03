import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_migration_cutover_readiness_matrix import (
    PlanMigrationCutoverReadinessMatrix,
    PlanMigrationCutoverReadinessRow,
    analyze_plan_migration_cutover_readiness_matrix,
    build_plan_migration_cutover_readiness_matrix,
    derive_plan_migration_cutover_readiness_matrix,
    extract_plan_migration_cutover_readiness_matrix,
    generate_plan_migration_cutover_readiness_matrix,
    plan_migration_cutover_readiness_matrix_to_dict,
    plan_migration_cutover_readiness_matrix_to_dicts,
    plan_migration_cutover_readiness_matrix_to_markdown,
    summarize_plan_migration_cutover_readiness_matrix,
)


def test_cutover_tasks_group_by_surface_with_required_readiness_signals():
    result = build_plan_migration_cutover_readiness_matrix(
        _plan(
            [
                _task(
                    "task-orders-window",
                    title="Cutover orders_v2 writes",
                    description="Cutover orders_v2 with dual-write and shadow read validation.",
                    acceptance_criteria=[
                        "Cutover window is Sunday 02:00 UTC.",
                        "Dependency freeze blocks schema and writer changes.",
                        "Validation gates include parity checks and smoke tests.",
                    ],
                ),
                _task(
                    "task-orders-watch",
                    title="orders_v2 switchover monitoring",
                    description="Communication owner SRE lead watches post-cutover monitoring.",
                    acceptance_criteria=[
                        "Rollback trigger restores old writes on parity failure.",
                        "Monitoring dashboard and alerts cover latency and error rate.",
                        "Post-cutover cleanup drains and decommissions old paths.",
                    ],
                ),
                _task(
                    "task-billing",
                    title="Traffic shift billing_v3",
                    description="Traffic shifting billing_v3 during a scheduled switchover window.",
                    acceptance_criteria=[
                        "Dependency freeze, validation gates, rollback trigger, communication owner, monitoring, and cleanup are documented.",
                    ],
                ),
                _task("task-copy", title="Update copy", description="Refresh plain content."),
            ]
        )
    )

    assert isinstance(result, PlanMigrationCutoverReadinessMatrix)
    assert all(isinstance(row, PlanMigrationCutoverReadinessRow) for row in result.rows)
    assert result.plan_id == "plan-cutover"
    assert result.cutover_task_ids == ("task-billing", "task-orders-window", "task-orders-watch")
    assert result.no_cutover_task_ids == ("task-copy",)
    assert [row.cutover_surface for row in result.rows] == ["billing_v3", "orders_v2"]

    orders = _row(result, "orders_v2")
    assert orders.task_ids == ("task-orders-window", "task-orders-watch")
    assert orders.readiness == "ready"
    assert orders.severity == "low"
    assert orders.gaps == ()
    assert any("orders_v2" in item for item in orders.evidence)


def test_missing_validation_rollback_monitoring_or_communication_owner_blocks_readiness():
    result = build_plan_migration_cutover_readiness_matrix(
        _plan(
            [
                _task(
                    "task-risky",
                    title="Cutover ledger_v2",
                    description="Cutover ledger_v2 during a maintenance window after dependency freeze.",
                    acceptance_criteria=["Post-cutover cleanup removes old jobs."],
                ),
                _task(
                    "task-partial",
                    title="Switchover profile_store",
                    description="Communication owner runs profile_store switchover with validation gates, rollback trigger, and monitoring.",
                    acceptance_criteria=["Cutover window is scheduled."],
                ),
            ]
        )
    )

    blocked = _row(result, "ledger_v2")
    assert blocked.readiness == "blocked"
    assert blocked.severity == "high"
    assert "Missing validation gates." in blocked.gaps
    assert "Missing rollback trigger." in blocked.gaps
    assert "Missing communication owner." in blocked.gaps
    assert "Missing monitoring criteria." in blocked.gaps

    partial = _row(result, "profile_store")
    assert partial.readiness == "partial"
    assert partial.severity == "medium"
    assert partial.gaps == ("Missing dependency freeze.", "Missing post-cutover cleanup.")
    assert result.summary["readiness_counts"] == {"blocked": 1, "partial": 1, "ready": 0}


def test_no_cutover_plans_return_empty_matrix_with_not_applicable_ids():
    result = build_plan_migration_cutover_readiness_matrix(
        _plan(
            [
                _task("task-api", title="Build API endpoint", description="Implement normal CRUD behavior."),
                _task("task-docs", title="Document endpoint", description="Update docs."),
            ]
        )
    )

    assert result.rows == ()
    assert result.cutover_task_ids == ()
    assert result.no_cutover_task_ids == ("task-api", "task-docs")
    assert result.summary == {
        "task_count": 2,
        "row_count": 0,
        "cutover_task_count": 0,
        "no_cutover_task_count": 2,
        "readiness_counts": {"blocked": 0, "partial": 0, "ready": 0},
        "severity_counts": {"high": 0, "medium": 0, "low": 0},
        "gap_counts": {},
        "surface_counts": {},
    }
    assert "No migration cutover readiness rows were inferred." in result.to_markdown()
    assert "No cutover signals: task-api, task-docs" in result.to_markdown()


def test_serialization_aliases_markdown_model_object_input_and_file_path_hints():
    plan = _plan(
        [
            _task(
                "task-cutover | plan",
                title="Cutover account | store",
                description="Cutover account_store with dual-write and shadow read.",
                files_or_modules=["runbooks/cutovers/account_store.md"],
                acceptance_criteria=[
                    "Window, dependency freeze, validation gates, rollback trigger, communication owner, monitoring, and cleanup are ready.",
                ],
            )
        ]
    )
    original = copy.deepcopy(plan)
    model_plan = ExecutionPlan.model_validate(plan)

    result = build_plan_migration_cutover_readiness_matrix(model_plan)
    payload = plan_migration_cutover_readiness_matrix_to_dict(result)
    markdown = plan_migration_cutover_readiness_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_migration_cutover_readiness_matrix(plan).to_dict() == result.to_dict()
    assert analyze_plan_migration_cutover_readiness_matrix(plan).to_dict() == result.to_dict()
    assert derive_plan_migration_cutover_readiness_matrix(plan).to_dict() == result.to_dict()
    assert extract_plan_migration_cutover_readiness_matrix(plan).to_dict() == result.to_dict()
    assert summarize_plan_migration_cutover_readiness_matrix(result) == result.summary
    assert plan_migration_cutover_readiness_matrix_to_dicts(result) == payload["rows"]
    assert plan_migration_cutover_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "cutover_task_ids",
        "no_cutover_task_ids",
        "summary",
    ]
    assert "account \\| store" in markdown
    assert "task-cutover \\| plan" in markdown

    object_result = build_plan_migration_cutover_readiness_matrix(
        SimpleNamespace(
            id="object-cutover",
            title="Cutover object_store",
            description="Communication owner handles object_store cutover window, validation gates, rollback trigger, monitoring, dependency freeze, and cleanup.",
            acceptance_criteria=["Ready"],
        )
    )
    invalid = build_plan_migration_cutover_readiness_matrix(23)

    assert object_result.rows[0].task_ids == ("object-cutover",)
    assert invalid.rows == ()
    assert invalid.summary["task_count"] == 0


def _row(result, surface):
    return next(row for row in result.rows if row.cutover_surface == surface)


def _plan(tasks, *, plan_id="plan-cutover"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-cutover",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    depends_on=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "depends_on": [] if depends_on is None else depends_on,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
