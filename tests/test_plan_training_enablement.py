import json

from blueprint.domain.models import ExecutionPlan, ImplementationBrief
from blueprint.plan_training_enablement import (
    PlanTrainingEnablementChecklist,
    PlanTrainingEnablementChecklistItem,
    build_plan_training_enablement_checklist,
    plan_training_enablement_checklist_to_dict,
    plan_training_enablement_checklist_to_markdown,
    summarize_plan_training_enablement_checklist,
)


def test_brief_only_signals_build_customer_enablement_rows():
    brief = _brief(
        workflow_context=(
            "Admins configure onboarding workflows and customer-facing reporting dashboards."
        ),
        scope=[
            "Publish end user release notes.",
            "Prepare customer success onboarding guide.",
        ],
    )

    result = build_plan_training_enablement_checklist(brief)

    assert result.plan_id is None
    assert result.brief_id == "brief-enable"
    assert [item.audience for item in result.items] == [
        "support",
        "customer_success",
        "operations",
        "admins",
        "end_users",
    ]
    admins = _item(result, "admins")
    assert admins.required_materials == (
        "admin guide",
        "reporting walkthrough",
        "workflow walkthrough",
        "onboarding guide",
    )
    assert admins.affected_task_ids == ()
    assert admins.readiness_status == "needs_materials"
    assert admins.owner_hints == ("product owner",)
    assert any(
        evidence.startswith("workflow_context: Admins configure onboarding workflows")
        for evidence in admins.evidence
    )


def test_plan_task_signals_include_task_ids_and_materials():
    result = build_plan_training_enablement_checklist(
        _plan(
            [
                _task(
                    "task-perms",
                    title="Launch admin permissions",
                    description=(
                        "Add RBAC permissions and settings for admins. Support needs "
                        "ticket macros and troubleshooting notes."
                    ),
                    files_or_modules=["docs/support/admin_permissions.md"],
                    acceptance_criteria=["Runbook ready and documented for support."],
                )
            ]
        )
    )

    assert result.items == (
        PlanTrainingEnablementChecklistItem(
            audience="support",
            required_materials=("support runbook", "support macros or FAQ"),
            affected_task_ids=("task-perms",),
            readiness_status="ready",
            owner_hints=("support lead",),
            evidence=(
                "files_or_modules: docs/support/admin_permissions.md",
                (
                    "description: Add RBAC permissions and settings for admins. Support "
                    "needs ticket macros and troubleshooting notes."
                ),
                "acceptance_criteria[0]: Runbook ready and documented for support.",
                "title: Launch admin permissions",
            ),
        ),
        PlanTrainingEnablementChecklistItem(
            audience="admins",
            required_materials=("admin guide", "permissions matrix"),
            affected_task_ids=("task-perms",),
            readiness_status="ready",
            owner_hints=("product owner",),
            evidence=(
                "files_or_modules: docs/support/admin_permissions.md",
                (
                    "description: Add RBAC permissions and settings for admins. Support "
                    "needs ticket macros and troubleshooting notes."
                ),
                "acceptance_criteria[0]: Runbook ready and documented for support.",
                "title: Launch admin permissions",
            ),
        ),
    )
    assert result.summary["affected_task_count"] == 1
    assert result.summary["ready_count"] == 2


def test_multiple_audiences_are_returned_in_stable_order():
    result = build_plan_training_enablement_checklist(
        _plan(
            [
                _task(
                    "task-billing",
                    title="Update billing invoices",
                    description=(
                        "Billing changes require sales talk track, customer success "
                        "rollout guide, admin invoice settings, and support FAQ."
                    ),
                ),
                _task(
                    "task-reporting",
                    title="Add user reporting dashboard",
                    description="End users get reporting dashboards and workflow help text.",
                ),
            ]
        )
    )

    assert [item.audience for item in result.items] == [
        "support",
        "sales",
        "customer_success",
        "operations",
        "admins",
        "end_users",
    ]
    assert _item(result, "sales").required_materials == ("sales talk track", "billing FAQ")
    assert _item(result, "customer_success").affected_task_ids == (
        "task-billing",
        "task-reporting",
    )
    assert _item(result, "end_users").required_materials == (
        "reporting walkthrough",
        "workflow walkthrough",
        "end-user release note",
    )


def test_combined_inputs_and_explicit_owner_extraction():
    brief = ImplementationBrief.model_validate(
        _brief(
            scope=["Customer success owns onboarding workflow enablement."],
            validation_plan="Confirm enablement materials ready with CS.",
        )
    )
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-owner",
                    title="Prepare onboarding rollout",
                    description="Onboarding workflow for customer success and end users.",
                    metadata={
                        "customer_success_owner": "CS Enablement",
                        "end_users_owner": "Product Marketing",
                    },
                )
            ]
        )
    )

    result = summarize_plan_training_enablement_checklist(brief, plan)

    assert result.plan_id == "plan-enable"
    assert result.brief_id == "brief-enable"
    assert _item(result, "customer_success").owner_hints == ("CS Enablement",)
    assert _item(result, "end_users").owner_hints == ("Product Marketing",)
    assert _item(result, "customer_success").readiness_status == "ready"
    payload = plan_training_enablement_checklist_to_dict(result)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["items"]
    assert list(payload) == ["plan_id", "brief_id", "items", "summary"]
    assert list(payload["items"][0]) == [
        "audience",
        "required_materials",
        "affected_task_ids",
        "readiness_status",
        "owner_hints",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_no_training_needed_output_is_empty_and_stable():
    result = build_plan_training_enablement_checklist(
        {"id": "plan-empty", "implementation_brief_id": "brief-empty", "tasks": []}
    )

    assert isinstance(result, PlanTrainingEnablementChecklist)
    assert result.items == ()
    assert result.summary == {
        "item_count": 0,
        "audience_counts": {
            "support": 0,
            "sales": 0,
            "customer_success": 0,
            "operations": 0,
            "admins": 0,
            "end_users": 0,
        },
        "affected_task_count": 0,
        "ready_count": 0,
        "needs_materials_count": 0,
    }
    assert plan_training_enablement_checklist_to_markdown(result) == "\n".join(
        [
            "# Plan Training Enablement Checklist: plan-empty",
            "",
            "No training or enablement needs were inferred.",
        ]
    )


def _item(result, audience):
    return next(item for item in result.items if item.audience == audience)


def _brief(**overrides):
    brief = {
        "id": "brief-enable",
        "source_brief_id": "source-enable",
        "title": "Enablement Launch",
        "domain": "growth",
        "target_user": "workspace admins",
        "buyer": "operations leaders",
        "workflow_context": "Update user workflows.",
        "problem_statement": "Teams need a clear rollout.",
        "mvp_goal": "Ship the workflow update.",
        "product_surface": "settings",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Validate rollout.",
        "definition_of_done": [],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
    brief.update(overrides)
    return brief


def _plan(tasks):
    return {
        "id": "plan-enable",
        "implementation_brief_id": "brief-enable",
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
):
    return {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": (
            acceptance_criteria if acceptance_criteria is not None else ["Done"]
        ),
        "status": "pending",
        "metadata": metadata or {},
    }
