import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_stakeholder_notification_matrix import (
    PlanStakeholderNotificationMatrix,
    PlanStakeholderNotificationRecord,
    build_plan_stakeholder_notification_matrix,
    plan_stakeholder_notification_matrix_to_dict,
    plan_stakeholder_notification_matrix_to_markdown,
    summarize_plan_stakeholder_notification_matrix,
)


def test_detects_notification_worthy_task_from_text_path_criteria_and_metadata():
    result = build_plan_stakeholder_notification_matrix(
        _plan(
            [
                _task(
                    "task-dashboard",
                    title="Launch customer-facing account dashboard",
                    description="Customers see new admin settings and UI behavior.",
                    files_or_modules=["src/web/pages/account_dashboard.py"],
                    acceptance_criteria=["Release notes explain the customer impact."],
                    metadata={"notification": "Send in-app message during rollout."},
                )
            ]
        )
    )

    assert isinstance(result, PlanStakeholderNotificationMatrix)
    assert result.plan_id == "plan-notifications"
    assert result.notification_task_ids == ("task-dashboard",)
    record = result.records[0]
    assert isinstance(record, PlanStakeholderNotificationRecord)
    assert record.notification_audiences == (
        "admins",
        "end_users",
        "customer_success",
        "support_agents",
        "operations",
        "product",
    )
    assert record.notification_triggers == (
        "admin_change",
        "customer_impact",
        "ui_behavior_change",
    )
    assert record.risk_level == "medium"
    assert "Identify the audience owner" in record.missing_inputs[0]
    assert "title: Launch customer-facing account dashboard" in record.evidence
    assert "files_or_modules: src/web/pages/account_dashboard.py" in record.evidence
    assert "acceptance_criteria[0]: Release notes explain the customer impact." in record.evidence
    assert "metadata.notification: Send in-app message during rollout." in record.evidence


def test_downtime_billing_permission_and_data_changes_escalate_to_high_risk():
    result = build_plan_stakeholder_notification_matrix(
        _plan(
            [
                _task(
                    "task-critical",
                    title="Billing permission migration with downtime",
                    description=(
                        "Migrate invoice data, change RBAC permissions, and schedule "
                        "a maintenance window with customer downtime."
                    ),
                    files_or_modules=["src/billing/migrations/permissions.py"],
                    acceptance_criteria=["Migration job completes."],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.risk_level == "high"
    assert record.notification_triggers == (
        "downtime",
        "billing_change",
        "permission_change",
        "data_change",
        "migration",
        "admin_change",
        "customer_impact",
    )
    assert record.notification_audiences == (
        "billing_contacts",
        "admins",
        "end_users",
        "customer_success",
        "support_agents",
        "operations",
        "data_governance",
        "security",
        "product",
    )
    assert len(record.missing_inputs) == 6


def test_all_required_notification_inputs_clear_missing_and_lower_risk():
    result = build_plan_stakeholder_notification_matrix(
        _task(
            "task-billing",
            title="Change billing invoice notification behavior",
            description="Customer-visible billing invoice email behavior changes.",
            acceptance_criteria=[
                "Audience owner is Billing Success.",
                "Message channel is email and status page.",
                "Timing is before rollout and after rollout.",
                "Support context and support macro are linked.",
                "Rollback message is prepared for reverted invoices.",
                "Success confirmation is sent after validation confirmation.",
            ],
        )
    )

    record = result.records[0]
    assert record.notification_triggers == (
        "billing_change",
        "customer_impact",
        "ui_behavior_change",
    )
    assert record.missing_inputs == ()
    assert record.risk_level == "medium"


def test_markdown_output_and_helpers_are_stable():
    result = build_plan_stakeholder_notification_matrix(
        _plan(
            [
                _task(
                    "task-pipe",
                    title="Customer | notification rollout",
                    description="Customer-facing UI notification behavior changes.",
                    acceptance_criteria=["Audience owner is Product."],
                )
            ],
            plan_id="plan-pipes",
        )
    )
    markdown = plan_stakeholder_notification_matrix_to_markdown(result)

    assert markdown == result.to_markdown()
    assert markdown.startswith("# Plan Stakeholder Notification Matrix: plan-pipes")
    assert "## Summary" in markdown
    assert "| Task | Title | Risk | Audiences | Triggers | Missing Inputs | Evidence |" in markdown
    assert "Customer \\| notification rollout" in markdown


def test_dict_serialization_model_inputs_and_deterministic_ordering():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-low",
                    title="Update UI copy for customers",
                    description="Customer-facing UI copy changes.",
                    acceptance_criteria=[
                        "Audience owner is Product.",
                        "Message channel is in-app.",
                        "Timing is during rollout.",
                        "Support context is linked.",
                        "Rollback message is ready.",
                        "Success confirmation is posted.",
                    ],
                ),
                _task(
                    "task-high",
                    title="Permission data migration",
                    description="Migrate customer data and update admin RBAC permissions.",
                    acceptance_criteria=["Migration succeeds."],
                ),
            ],
            plan_id="plan-model",
        )
    )
    task = ExecutionTask.model_validate(
        _task(
            "task-object",
            title="Admin permission workflow",
            description="Admins can change RBAC roles in settings.",
        )
    )

    plan_result = build_plan_stakeholder_notification_matrix(plan)
    task_result = summarize_plan_stakeholder_notification_matrix(task)
    payload = plan_stakeholder_notification_matrix_to_dict(plan_result)

    assert [record.task_id for record in plan_result.records] == ["task-high", "task-low"]
    assert task_result.notification_task_ids == ("task-object",)
    assert payload == plan_result.to_dict()
    assert plan_result.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "notification_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "notification_audiences",
        "notification_triggers",
        "missing_inputs",
        "risk_level",
        "evidence",
    ]


def test_empty_plan_returns_empty_matrix_summary_and_does_not_mutate_input():
    plan = _plan(
        [
            _task(
                "task-internal",
                title="Refactor parser internals",
                description="Simplify internal token handling.",
                files_or_modules=["src/blueprint/parser.py"],
                metadata={"component": "parser"},
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_stakeholder_notification_matrix(plan)

    assert plan == original
    assert result.records == ()
    assert result.notification_task_ids == ()
    assert result.summary == {
        "task_count": 1,
        "notification_task_count": 0,
        "missing_input_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "audience_counts": {
            "billing_contacts": 0,
            "admins": 0,
            "end_users": 0,
            "customer_success": 0,
            "support_agents": 0,
            "operations": 0,
            "data_governance": 0,
            "security": 0,
            "product": 0,
            "engineering": 0,
        },
        "trigger_counts": {
            "downtime": 0,
            "billing_change": 0,
            "permission_change": 0,
            "data_change": 0,
            "migration": 0,
            "admin_change": 0,
            "customer_impact": 0,
            "ui_behavior_change": 0,
        },
    }
    assert result.to_markdown() == (
        "# Plan Stakeholder Notification Matrix: plan-notifications\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Notification-worthy task count: 0\n"
        "- Missing input count: 0\n"
        "- Risk counts: high 0, medium 0, low 0\n"
        "\n"
        "No stakeholder notification needs were detected."
    )


def _plan(tasks, *, plan_id="plan-notifications", metadata=None):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-notifications",
        "milestones": [],
        "metadata": {} if metadata is None else metadata,
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
    risks=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    if risks is not None:
        task["risks"] = risks
    return task
