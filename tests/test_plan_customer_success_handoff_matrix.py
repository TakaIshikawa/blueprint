import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_customer_success_handoff_matrix import (
    PlanCustomerSuccessHandoffMatrix,
    PlanCustomerSuccessHandoffRow,
    analyze_plan_customer_success_handoff_matrix,
    build_plan_customer_success_handoff_matrix,
    plan_customer_success_handoff_matrix_to_dict,
    plan_customer_success_handoff_matrix_to_dicts,
    plan_customer_success_handoff_matrix_to_markdown,
    summarize_plan_customer_success_handoff_matrix,
)


def test_enterprise_rollout_plan_identifies_handoff_needs():
    result = build_plan_customer_success_handoff_matrix(
        _plan(
            [
                _task(
                    "task-enterprise-rollout",
                    title="Enterprise customer rollout for new API",
                    description=(
                        "Phased rollout of new API features to enterprise accounts. "
                        "CSM team needs to coordinate with technical account managers. "
                        "Migration guide is in draft status."
                    ),
                    acceptance_criteria=[
                        "Enterprise customers notified.",
                        "Migration guide completed.",
                        "CSM team trained on new features.",
                    ],
                ),
                _task("task-internal-cache", title="Optimize cache", description="Internal cache tuning."),
            ]
        )
    )

    assert isinstance(result, PlanCustomerSuccessHandoffMatrix)
    assert isinstance(result.rows[0], PlanCustomerSuccessHandoffRow)
    assert result.handoff_task_ids == ("task-enterprise-rollout",)
    assert result.no_handoff_task_ids == ("task-internal-cache",)
    assert result.rows[0].segment == "enterprise"
    assert result.rows[0].trigger == "rollout"
    assert result.rows[0].owner == "csm"
    assert "guide" in result.rows[0].customer_artifact.lower()
    assert "draft" in result.rows[0].gap.lower() or "missing" in result.rows[0].gap.lower()
    assert len(result.rows[0].recommended_action) > 0
    assert result.summary["handoff_task_count"] == 1
    assert result.summary["segment_counts"]["enterprise"] == 1
    assert result.summary["trigger_counts"]["rollout"] == 1


def test_migration_and_onboarding_plans_detected():
    result = build_plan_customer_success_handoff_matrix(
        _plan(
            [
                _task(
                    "task-migration",
                    title="Migrate SMB customers to new pricing model",
                    description=(
                        "Migrate small business customers from legacy pricing to new tiers. "
                        "Support team will handle customer communications. "
                        "Email templates and FAQ documentation ready."
                    ),
                    acceptance_criteria=[
                        "All SMB customers migrated.",
                        "Support team trained.",
                        "Customer emails sent.",
                    ],
                ),
                _task(
                    "task-onboarding",
                    title="Individual user onboarding improvements",
                    description=(
                        "Improve self-serve onboarding for individual users. "
                        "Documentation team creating getting started guide."
                    ),
                    acceptance_criteria=[
                        "Onboarding flow updated.",
                        "Getting started guide published.",
                    ],
                ),
            ]
        )
    )

    assert len(result.rows) == 2

    # Check migration task
    migration_row = next(row for row in result.rows if "migration" in row.task_id)
    assert migration_row.segment == "smb"
    assert migration_row.trigger == "migration"
    assert migration_row.owner == "support"
    assert "email" in migration_row.customer_artifact.lower() or "faq" in migration_row.customer_artifact.lower() or "documentation" in migration_row.customer_artifact.lower()

    # Check onboarding task
    onboarding_row = next(row for row in result.rows if "onboarding" in row.task_id)
    assert onboarding_row.segment == "individual"
    assert onboarding_row.trigger == "onboarding"
    assert onboarding_row.owner == "documentation"
    assert "guide" in onboarding_row.customer_artifact.lower()


def test_unrelated_plans_produce_empty_output():
    result = build_plan_customer_success_handoff_matrix(
        _plan(
            [
                _task(
                    "task-backend-refactor",
                    title="Refactor internal database schema",
                    description="Internal backend optimization, no customer impact.",
                    acceptance_criteria=["Schema updated.", "Tests pass."],
                ),
                _task(
                    "task-monitoring",
                    title="Add internal monitoring dashboards",
                    description="Engineering-only monitoring improvements.",
                    acceptance_criteria=["Dashboards deployed."],
                ),
            ]
        )
    )

    assert result.rows == ()
    assert result.handoff_task_ids == ()
    assert len(result.no_handoff_task_ids) == 2
    assert result.summary["handoff_task_count"] == 0
    assert result.summary["no_handoff_task_count"] == 2
    assert "No customer success handoff rows were inferred." in result.to_markdown()


def test_object_input_serializes_and_renders_markdown():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-launch",
                    title="Launch new feature for all customers",
                    description=(
                        "General availability launch of new analytics feature. "
                        "Product team coordinating with customer success. "
                        "Release notes, blog post, and tutorial video are ready."
                    ),
                    acceptance_criteria=[
                        "Feature flag enabled.",
                        "Release notes published.",
                        "Customer success team notified.",
                    ],
                )
            ]
        )
    )

    result = analyze_plan_customer_success_handoff_matrix(plan)
    payload = plan_customer_success_handoff_matrix_to_dict(result)

    assert isinstance(result, PlanCustomerSuccessHandoffMatrix)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "handoff_task_ids",
        "no_handoff_task_ids",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "segment",
        "trigger",
        "owner",
        "customer_artifact",
        "gap",
        "recommended_action",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    markdown = plan_customer_success_handoff_matrix_to_markdown(result)
    assert "Plan Customer Success Handoff Matrix" in markdown
    assert "task-launch" in markdown
    assert markdown == result.to_markdown()


def test_dict_helpers_and_aliases_work():
    result = build_plan_customer_success_handoff_matrix(
        _plan(
            [
                _task(
                    "task-trial",
                    title="Trial user onboarding automation",
                    description=(
                        "Automate onboarding for trial users with product-led growth approach. "
                        "Sales team involved for enterprise trial conversions."
                    ),
                    acceptance_criteria=[
                        "Automated email sequence active.",
                        "Sales handoff criteria defined.",
                    ],
                )
            ]
        )
    )

    assert summarize_plan_customer_success_handoff_matrix(result) == result.summary
    assert analyze_plan_customer_success_handoff_matrix(result) is result
    dicts = plan_customer_success_handoff_matrix_to_dicts(result)
    assert dicts == result.to_dicts()
    assert len(dicts) == 1
    assert dicts[0]["task_id"] == "task-trial"
    assert dicts[0]["segment"] == "trial"
    assert dicts[0]["trigger"] in ("onboarding", "launch")


def test_multiple_segments_and_triggers_with_gaps():
    result = build_plan_customer_success_handoff_matrix(
        _plan(
            [
                _task(
                    "task-sunset",
                    title="Sunset legacy API for free tier users",
                    description=(
                        "Deprecate old API endpoints for free tier customers. "
                        "Communication plan missing. Need to define migration deadline."
                    ),
                    acceptance_criteria=[
                        "Deprecation notice sent.",
                        "Migration guide needed.",
                    ],
                ),
                _task(
                    "task-enterprise-onboarding",
                    title="White glove onboarding for strategic accounts",
                    description=(
                        "CSM-led onboarding for Fortune 500 enterprise accounts. "
                        "Runbook and training materials complete."
                    ),
                    acceptance_criteria=[
                        "Onboarding runbook ready.",
                        "CSM training complete.",
                    ],
                ),
            ]
        )
    )

    assert len(result.rows) == 2

    # Check sunset task
    sunset_row = next(row for row in result.rows if "sunset" in row.task_id)
    assert sunset_row.segment == "free_tier"
    assert sunset_row.trigger == "sunset"
    assert sunset_row.gap != "none"

    # Check enterprise onboarding task
    enterprise_row = next(row for row in result.rows if "enterprise" in row.task_id)
    assert enterprise_row.segment == "enterprise"
    assert enterprise_row.trigger == "onboarding"
    assert enterprise_row.owner == "csm"

    # Check summary counts
    assert result.summary["segment_counts"]["free_tier"] == 1
    assert result.summary["segment_counts"]["enterprise"] == 1
    assert result.summary["trigger_counts"]["sunset"] == 1
    assert result.summary["trigger_counts"]["onboarding"] == 1
    assert result.summary["tasks_with_gaps"] >= 1


def _plan(tasks):
    return {
        "id": "plan-customer-success",
        "implementation_brief_id": "brief-cs-handoff",
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
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
        "metadata": metadata or {},
    }
