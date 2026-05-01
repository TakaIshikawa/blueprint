import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_stakeholder_approvals import (
    PlanStakeholderApprovalMatrix,
    PlanStakeholderApprovalRow,
    build_plan_stakeholder_approval_matrix,
    plan_stakeholder_approval_matrix_to_dict,
    plan_stakeholder_approval_matrix_to_markdown,
    summarize_plan_stakeholder_approvals,
)


def test_security_data_finance_operations_and_product_needs_are_inferred():
    result = build_plan_stakeholder_approval_matrix(
        _plan(
            [
                _task(
                    "task-security",
                    title="Protect invoice export",
                    description=(
                        "Add RBAC and audit logs before admins export customer data."
                    ),
                    files_or_modules=[
                        "src/auth/invoice_permissions.py",
                        "src/exports/customer_invoices.py",
                    ],
                    metadata={
                        "security_reviewer": "AppSec",
                        "data_owner": "Data Governance",
                    },
                ),
                _task(
                    "task-billing",
                    title="Billing retry workflow",
                    description="Retry failed subscription payments and ledger posting.",
                    files_or_modules=["src/billing/payment_retries.py"],
                    metadata={"finance_approver": "Revenue Ops"},
                ),
                _task(
                    "task-release",
                    title="Customer-facing launch rollout",
                    description="Release the feature flag with rollback alerts.",
                    files_or_modules=["infra/deploy/customer_rollout.tf"],
                    tags=["product launch", "operations"],
                ),
            ]
        )
    )

    assert [row.stakeholder_group for row in result.rows] == [
        "product",
        "engineering",
        "security",
        "data",
        "operations",
        "finance",
    ]
    assert {
        row.stakeholder_group: row.approval_status for row in result.rows
    } == {
        "product": "required",
        "engineering": "advisory",
        "security": "required",
        "data": "required",
        "operations": "required",
        "finance": "required",
    }
    assert _row(result, "security").reviewers == ("AppSec",)
    assert _row(result, "data").owners == ("Data Governance",)
    assert _row(result, "finance").reviewers == ("Revenue Ops",)
    assert _row(result, "security").affected_task_ids == ("task-security",)
    assert _row(result, "data").affected_task_ids == ("task-security",)
    assert _row(result, "operations").affected_task_ids == ("task-release",)
    assert result.summary["required_count"] == 5
    assert result.summary["advisory_count"] == 1
    assert result.summary["affected_task_count"] == 3


def test_brief_fields_risks_constraints_tags_and_metadata_infer_legal_support():
    brief = {
        "id": "brief-approvals",
        "title": "Privacy consent rollout",
        "problem_statement": "Update consent terms for customer-facing onboarding.",
        "data_requirements": "Personal data analytics must respect GDPR deletion.",
        "risks": ["Support ticket volume may spike after launch."],
        "constraints": ["Legal sign-off is required before dispatch."],
        "metadata": {
            "legal_owner": "Privacy Counsel",
            "support_reviewer": "Support Lead",
        },
    }
    plan = _plan(
        [
            _task(
                "task-consent",
                title="Consent preference center",
                description="Store opt-out choices and publish support FAQ updates.",
                files_or_modules=["src/privacy/consent_preferences.py"],
                tags=["support readiness"],
            )
        ]
    )

    result = build_plan_stakeholder_approval_matrix(brief, plan)

    assert _row(result, "legal").approval_status == "required"
    assert _row(result, "legal").owners == ("Privacy Counsel",)
    assert _row(result, "support").approval_status == "advisory"
    assert _row(result, "support").reviewers == ("Support Lead",)
    assert _row(result, "support").affected_task_ids == ("task-consent",)
    assert any(
        "constraints[0]: Legal sign-off is required before dispatch." == evidence
        for evidence in _row(result, "legal").evidence
    )


def test_no_approval_signals_has_empty_summary_and_stable_markdown():
    result = build_plan_stakeholder_approval_matrix(
        {"id": "plan-empty", "implementation_brief_id": "brief-empty", "tasks": []}
    )

    assert result.rows == ()
    assert result.summary == {
        "row_count": 0,
        "required_count": 0,
        "advisory_count": 0,
        "affected_task_count": 0,
        "group_counts": {
            "product": 0,
            "engineering": 0,
            "security": 0,
            "data": 0,
            "legal": 0,
            "support": 0,
            "operations": 0,
            "finance": 0,
        },
    }
    assert result.to_markdown() == "\n".join(
        [
            "# Plan Stakeholder Approval Matrix: plan-empty",
            "",
            "No stakeholder approvals were inferred.",
        ]
    )


def test_model_input_serializes_stably_and_renders_markdown():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-auth",
                    title="Admin authentication",
                    description="Require SSO before admin users can change pricing.",
                    files_or_modules=["src/auth/admin_sso.py"],
                    metadata={"owner": "security platform", "reviewer": "security lead"},
                )
            ]
        )
    )

    result = summarize_plan_stakeholder_approvals(plan)
    payload = plan_stakeholder_approval_matrix_to_dict(result)

    assert isinstance(result, PlanStakeholderApprovalMatrix)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert list(payload) == ["plan_id", "brief_id", "rows", "summary"]
    assert list(payload["rows"][0]) == [
        "stakeholder_group",
        "approval_status",
        "owners",
        "reviewers",
        "affected_task_ids",
        "rationale",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert PlanStakeholderApprovalRow(
        stakeholder_group="finance",
        approval_status="required",
        owners=(),
        reviewers=(),
        affected_task_ids=("task-auth",),
        rationale=(
            "Finance approval is required because the plan touches billing, "
            "payments, invoices, pricing, tax, revenue, or ledger workflows."
        ),
        evidence=("description: Require SSO before admin users can change pricing.",),
    ) in result.rows
    assert plan_stakeholder_approval_matrix_to_markdown(result) == "\n".join(
        [
            "# Plan Stakeholder Approval Matrix: plan-approvals",
            "",
            "| Stakeholder | Status | Owners | Reviewers | Affected Tasks | Rationale | Evidence |",
            "| --- | --- | --- | --- | --- | --- | --- |",
            "| engineering | advisory | - | - | task-auth | Engineering review is advisory "
            "for confirming implementation ownership, test coverage, and code impact. | "
            "files_or_modules: src/auth/admin_sso.py |",
            "| security | required | security platform | security lead | task-auth | Security "
            "approval is required because the plan touches authentication, authorization, "
            "secrets, auditability, or access controls. | files_or_modules: "
            "src/auth/admin_sso.py; title: Admin authentication; description: "
            "Require SSO before admin users can change pricing. |",
            "| finance | required | - | - | task-auth | Finance approval is required because "
            "the plan touches billing, payments, invoices, pricing, tax, revenue, or "
            "ledger workflows. | description: Require SSO before admin users can change pricing. |",
        ]
    )


def _row(result, group):
    return next(row for row in result.rows if row.stakeholder_group == group)


def _plan(tasks):
    return {
        "id": "plan-approvals",
        "implementation_brief_id": "brief-approvals",
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
    tags=None,
    metadata=None,
):
    task = {
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
    if tags is not None:
        task["tags"] = tags
    return task
