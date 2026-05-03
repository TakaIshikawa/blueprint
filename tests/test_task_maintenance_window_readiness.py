import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_maintenance_window_readiness import (
    TaskMaintenanceWindowReadinessPlan,
    TaskMaintenanceWindowReadinessRecord,
    analyze_task_maintenance_window_readiness,
    build_task_maintenance_window_readiness_plan,
    extract_task_maintenance_window_readiness,
    generate_task_maintenance_window_readiness,
    summarize_task_maintenance_window_readiness,
    task_maintenance_window_readiness_plan_to_dict,
    task_maintenance_window_readiness_plan_to_dicts,
    task_maintenance_window_readiness_plan_to_markdown,
)


def test_high_risk_downtime_window_without_customer_notice_or_rollback():
    result = build_task_maintenance_window_readiness_plan(
        _plan(
            [
                _task(
                    "task-maintenance",
                    title="Run scheduled database maintenance",
                    description=(
                        "Scheduled maintenance window requires downtime and degraded mode while "
                        "we drain traffic from the primary database."
                    ),
                    files_or_modules=[
                        "ops/maintenance/database_window.md",
                        "infra/runbooks/traffic_drain.yaml",
                    ],
                    acceptance_criteria=["The service returns after the planned outage."],
                )
            ]
        )
    )

    assert isinstance(result, TaskMaintenanceWindowReadinessPlan)
    assert result.maintenance_task_ids == ("task-maintenance",)
    record = result.records[0]
    assert isinstance(record, TaskMaintenanceWindowReadinessRecord)
    assert record.detected_signals == (
        "maintenance_window",
        "downtime",
        "degraded_mode",
        "traffic_drain",
    )
    assert record.present_safeguards == ()
    assert record.missing_safeguards == (
        "customer_notice",
        "status_page",
        "owner_approval",
        "rollback_window",
        "monitoring",
        "support_coverage",
        "post_window_validation",
    )
    assert record.risk_level == "high"
    assert record.evidence[:2] == (
        "files_or_modules: ops/maintenance/database_window.md",
        "files_or_modules: infra/runbooks/traffic_drain.yaml",
    )
    assert result.summary["maintenance_task_count"] == 1
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}
    assert result.summary["signal_counts"]["traffic_drain"] == 1
    assert result.summary["missing_safeguard_counts"]["customer_notice"] == 1


def test_low_risk_fully_covered_maintenance_window():
    result = analyze_task_maintenance_window_readiness(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Planned maintenance window for search cluster",
                    description=(
                        "Planned maintenance window runs in a change window after customer notice, "
                        "status page scheduling, CAB approval, and support coverage are confirmed. "
                        "Drain traffic, watch monitoring dashboards and alerts, keep a rollback "
                        "window with abort criteria, and finish with post-maintenance validation."
                    ),
                    acceptance_criteria=[
                        "Post-window validation runs smoke tests and health checks.",
                        "Customer support and on-call remain staffed until completion update is sent.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_signals == (
        "maintenance_window",
        "customer_notice",
        "status_page",
        "change_window",
        "traffic_drain",
        "rollback_window",
        "monitoring",
        "support_coverage",
        "post_window_validation",
        "owner_approval",
    )
    assert record.present_safeguards == (
        "customer_notice",
        "status_page",
        "owner_approval",
        "rollback_window",
        "monitoring",
        "support_coverage",
        "post_window_validation",
    )
    assert record.missing_safeguards == ()
    assert record.risk_level == "low"
    assert record.recommended_readiness_steps == ()
    assert result.summary["missing_safeguard_count"] == 0
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_metadata_tags_and_validation_commands_detect_window_safeguards():
    result = build_task_maintenance_window_readiness_plan(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Schedule degraded mode maintenance",
                    description="Use a freeze window while checkout runs in degraded mode.",
                    metadata={
                        "status_page": "Draft public status page update before the window.",
                        "support": {"coverage": "Support staffing and on-call escalation channel are ready."},
                        "validation_commands": {
                            "post": ["poetry run pytest tests/checkout/test_post_window_validation.py"]
                        },
                    },
                    tags=["Customer notification sent", "service owner approval"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_signals == (
        "degraded_mode",
        "customer_notice",
        "status_page",
        "freeze_window",
        "support_coverage",
        "post_window_validation",
        "owner_approval",
    )
    assert record.present_safeguards == (
        "customer_notice",
        "status_page",
        "owner_approval",
        "support_coverage",
        "post_window_validation",
    )
    assert record.missing_safeguards == ("rollback_window", "monitoring")
    assert record.risk_level == "medium"
    assert any("metadata.status_page" in item for item in record.evidence)
    assert any("metadata.validation_commands.post[0]: poetry run pytest" in item for item in record.evidence)
    assert result.summary["present_safeguard_counts"]["support_coverage"] == 1


def test_no_downtime_and_unrelated_tasks_are_not_applicable_with_stable_summary():
    result = build_task_maintenance_window_readiness_plan(
        _plan(
            [
                _task(
                    "task-api",
                    title="Deploy account endpoint",
                    description="Zero-downtime rollout with no maintenance window required.",
                    files_or_modules=["src/blueprint/api/accounts.py"],
                ),
                _task(
                    "task-copy",
                    title="Update dashboard copy",
                    description="Adjust labels and helper text.",
                ),
            ]
        )
    )

    assert result.records == ()
    assert result.maintenance_task_ids == ()
    assert result.not_applicable_task_ids == ("task-api", "task-copy")
    assert result.to_dicts() == []
    assert result.summary["task_count"] == 2
    assert result.summary["maintenance_task_count"] == 0
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 0}
    assert result.summary["signal_counts"]["maintenance_window"] == 0
    assert result.summary["present_safeguard_counts"]["status_page"] == 0
    assert result.summary["missing_safeguard_counts"]["rollback_window"] == 0
    markdown = result.to_markdown()
    assert "No maintenance window readiness records were inferred." in markdown
    assert "Not-applicable tasks: task-api, task-copy" in markdown


def test_serialization_aliases_markdown_ordering_and_no_source_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Maintenance window | rollback",
                description=(
                    "Maintenance window with customer notice, status page, owner approval, rollback plan, "
                    "monitoring dashboards, support coverage, and post-window validation."
                ),
            ),
            _task(
                "task-a",
                title="Database downtime window",
                description="Planned downtime requires a rollback window only.",
            ),
            _task("task-copy", title="Profile UI copy", description="Adjust labels."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_maintenance_window_readiness(plan)
    payload = task_maintenance_window_readiness_plan_to_dict(result)
    markdown = task_maintenance_window_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_maintenance_window_readiness_plan_to_dicts(result) == payload["records"]
    assert task_maintenance_window_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_maintenance_window_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_maintenance_window_readiness(plan).to_dict() == result.to_dict()
    assert result.affected_task_ids == result.maintenance_task_ids
    assert result.no_signal_task_ids == result.not_applicable_task_ids
    assert result.maintenance_task_ids == ("task-a", "task-z")
    assert list(payload) == [
        "plan_id",
        "records",
        "maintenance_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "evidence",
        "recommended_readiness_steps",
    ]
    assert [record.risk_level for record in result.records] == ["high", "low"]
    assert markdown.startswith("# Task Maintenance Window Readiness: plan-maintenance")
    assert "Maintenance window \\| rollback" in markdown


def test_execution_plan_task_and_object_like_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Scheduled maintenance with support staffing",
        description=(
            "Scheduled maintenance window has customer notice, public status page, owner approval, "
            "rollback window, monitoring alerts, support coverage, and post-maintenance validation."
        ),
        files_or_modules=["ops/maintenance/search_window.md"],
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Traffic drain change window",
            description="Drain traffic during the change window with rollback plan and monitoring.",
        )
    )
    plan_model = ExecutionPlan.model_validate(
        _plan([task_model.model_dump(mode="python")], plan_id="plan-model")
    )

    iterable_result = build_task_maintenance_window_readiness_plan([object_task])
    task_result = build_task_maintenance_window_readiness_plan(task_model)
    plan_result = build_task_maintenance_window_readiness_plan(plan_model)

    assert iterable_result.records[0].task_id == "task-object"
    assert iterable_result.records[0].risk_level == "low"
    assert task_result.records[0].task_id == "task-model"
    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].task_id == "task-model"


def _plan(tasks, plan_id="plan-maintenance"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-maintenance",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
