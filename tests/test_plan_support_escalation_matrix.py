import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_support_escalation_matrix import (
    PlanSupportEscalationMatrix,
    PlanSupportEscalationRecord,
    build_plan_support_escalation_matrix,
    plan_support_escalation_matrix_to_dict,
    plan_support_escalation_matrix_to_markdown,
    summarize_plan_support_escalation_matrix,
)


def test_detects_user_facing_task_and_missing_support_artifacts():
    result = build_plan_support_escalation_matrix(
        _plan(
            [
                _task(
                    "task-profile",
                    title="Launch customer-facing profile dashboard",
                    description="Users can update profile settings from a new dashboard page.",
                    files_or_modules=["src/web/pages/profile_settings.py"],
                    acceptance_criteria=["Dashboard saves profile settings successfully."],
                )
            ]
        )
    )

    assert isinstance(result, PlanSupportEscalationMatrix)
    assert result.plan_id == "plan-support"
    assert result.support_relevant_task_ids == ("task-profile",)
    record = result.records[0]
    assert isinstance(record, PlanSupportEscalationRecord)
    assert record.affected_audience == (
        "admins",
        "end_users",
        "customer_success",
        "support_agents",
        "operations",
    )
    assert record.likely_support_triggers == (
        "admin_workflow_confusion",
        "user_visible_behavior_change",
        "release_rollout_question",
    )
    assert record.required_enablement_artifacts == (
        "support_docs",
        "support_macros",
        "escalation_path",
        "customer_communication",
        "admin_guide",
    )
    assert record.risk_level == "medium"
    assert "Support-facing documentation" in record.missing_acceptance_criteria[0]
    assert "title: Launch customer-facing profile dashboard" in record.evidence
    assert "files_or_modules: src/web/pages/profile_settings.py" in record.evidence


def test_billing_account_escalation_is_high_risk_with_owner_hints():
    result = build_plan_support_escalation_matrix(
        _plan(
            [
                _task(
                    "task-billing",
                    title="Change subscription seat billing",
                    description="Update account invoices when admins change paid seats.",
                    acceptance_criteria=[
                        "Seat changes update subscriptions.",
                        "Support macro covers invoice questions.",
                        "Escalation path routes failed charges to Billing Ops.",
                    ],
                    metadata={"owner": "Billing Platform"},
                )
            ],
            metadata={"support_owner": "Support Billing Queue"},
        )
    )

    record = result.records[0]
    assert record.risk_level == "high"
    assert record.affected_audience == (
        "billing_contacts",
        "admins",
        "customer_success",
        "support_agents",
    )
    assert record.likely_support_triggers == ("billing_or_account_ticket",)
    assert record.required_enablement_artifacts == (
        "support_docs",
        "support_macros",
        "escalation_path",
        "billing_faq",
        "admin_guide",
    )
    assert record.escalation_owner_hints == ("Billing Platform", "Support Billing Queue")
    assert not any("Support macros" in item for item in record.missing_acceptance_criteria)
    assert not any("escalation paths" in item for item in record.missing_acceptance_criteria)
    assert any("Billing/account FAQ" in item for item in record.missing_acceptance_criteria)


def test_support_artifact_acceptance_criteria_reduce_missing_findings():
    result = build_plan_support_escalation_matrix(
        _task(
            "task-migration",
            title="Migrate existing customer account exports",
            description="Backfill export records for existing customers during rollout.",
            acceptance_criteria=[
                "Support docs and troubleshooting notes are linked.",
                "Support macros cover missing export tickets.",
                "Escalation path identifies Data Ops and on-call.",
                "Operational runbook covers rollback and launch watch.",
                "Customer communication and release notes are ready.",
                "Admin guide covers customer account export questions.",
                "Migration guide explains reconciliation steps.",
            ],
        )
    )

    record = result.records[0]
    assert record.likely_support_triggers == (
        "migration_or_data_discrepancy",
        "user_visible_behavior_change",
        "release_rollout_question",
    )
    assert record.required_enablement_artifacts == (
        "support_docs",
        "support_macros",
        "escalation_path",
        "operational_runbook",
        "customer_communication",
        "admin_guide",
        "migration_guide",
    )
    assert record.missing_acceptance_criteria == ()
    assert record.risk_level == "medium"


def test_markdown_output_and_helpers_are_stable():
    result = build_plan_support_escalation_matrix(
        _plan(
            [
                _task(
                    "task-pipe",
                    title="Customer | notification rollout",
                    description="Release customer-facing email notification workflow.",
                    acceptance_criteria=["Support docs are ready."],
                )
            ],
            plan_id="plan-pipes",
        )
    )
    markdown = plan_support_escalation_matrix_to_markdown(result)

    assert markdown == result.to_markdown()
    assert markdown.startswith("# Plan Support Escalation Matrix: plan-pipes")
    assert "## Summary" in markdown
    assert "| Task | Title | Risk | Audience | Triggers | Required Artifacts | Owner Hints | Missing Acceptance Criteria | Evidence |" in markdown
    assert "Customer \\| notification rollout" in markdown


def test_dict_serialization_model_inputs_and_deterministic_ordering():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-low",
                    title="Release customer help copy",
                    description="Customer-facing help copy update.",
                    acceptance_criteria=[
                        "Support docs are ready.",
                        "Support macros are ready.",
                        "Escalation path is documented.",
                        "Customer communication is ready.",
                    ],
                ),
                _task(
                    "task-high",
                    title="Billing invoice migration",
                    description="Migrate existing account invoices.",
                    acceptance_criteria=["Billing job succeeds."],
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

    plan_result = build_plan_support_escalation_matrix(plan)
    task_result = summarize_plan_support_escalation_matrix(task)
    payload = plan_support_escalation_matrix_to_dict(plan_result)

    assert [record.task_id for record in plan_result.records] == ["task-high", "task-low"]
    assert task_result.support_relevant_task_ids == ("task-object",)
    assert payload == plan_result.to_dict()
    assert plan_result.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "support_relevant_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "affected_audience",
        "likely_support_triggers",
        "required_enablement_artifacts",
        "escalation_owner_hints",
        "missing_acceptance_criteria",
        "evidence",
        "risk_level",
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

    result = build_plan_support_escalation_matrix(plan)

    assert plan == original
    assert result.records == ()
    assert result.support_relevant_task_ids == ()
    assert result.summary == {
        "task_count": 1,
        "support_relevant_task_count": 0,
        "missing_acceptance_criterion_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "audience_counts": {
            "billing_contacts": 0,
            "admins": 0,
            "end_users": 0,
            "customer_success": 0,
            "support_agents": 0,
            "operations": 0,
            "developers": 0,
        },
        "artifact_counts": {
            "support_docs": 0,
            "support_macros": 0,
            "escalation_path": 0,
            "operational_runbook": 0,
            "customer_communication": 0,
            "billing_faq": 0,
            "admin_guide": 0,
            "migration_guide": 0,
        },
    }
    assert result.to_markdown() == (
        "# Plan Support Escalation Matrix: plan-support\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Support-relevant task count: 0\n"
        "- Missing acceptance criterion count: 0\n"
        "- Risk counts: high 0, medium 0, low 0\n"
        "\n"
        "No support escalation needs were detected."
    )


def _plan(tasks, *, plan_id="plan-support", metadata=None):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-support",
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
