import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_customer_support_readiness import (
    TaskCustomerSupportReadinessPlan,
    TaskCustomerSupportReadinessRecord,
    build_task_customer_support_readiness,
    build_task_customer_support_readiness_plan,
    derive_task_customer_support_readiness_plan,
    task_customer_support_readiness_plan_to_dict,
    task_customer_support_readiness_plan_to_markdown,
)


def test_customer_facing_billing_migration_rollout_and_docs_signals_are_detected():
    result = build_task_customer_support_readiness_plan(
        _plan(
            [
                _task(
                    "task-billing-migration",
                    title="Roll out billing account migration",
                    description=(
                        "Change customer-facing checkout billing flow, migrate existing "
                        "account subscription data, and launch with a feature flag rollback."
                    ),
                    files_or_modules=[
                        "src/frontend/Checkout.tsx",
                        "src/billing/subscriptions.py",
                        "migrations/20260502_accounts.sql",
                        "docs/release_notes/billing.md",
                    ],
                    acceptance_criteria=[
                        "Support notes, rollback wording, and launch watch items are ready.",
                    ],
                )
            ]
        )
    )

    records = {record.readiness_category: record for record in result.records}

    assert list(records) == [
        "billing_account_flow",
        "data_migration",
        "rollout_risk",
        "user_visible_behavior",
        "documentation_change",
    ]
    assert records["billing_account_flow"].severity == "critical"
    assert records["data_migration"].severity == "critical"
    assert records["rollout_risk"].severity == "critical"
    assert "Customer messaging for billing or account-impacting changes." in (
        records["billing_account_flow"].suggested_artifacts
    )
    assert "Troubleshooting steps for missing, delayed, or unexpected migrated data." in (
        records["data_migration"].suggested_artifacts
    )
    assert "Launch watch items and owner handoff." in (records["rollout_risk"].suggested_artifacts)
    assert "Support notes linking the updated documentation." in (
        records["documentation_change"].suggested_artifacts
    )
    assert all(record.rationale for record in result.records)
    assert all(isinstance(record, TaskCustomerSupportReadinessRecord) for record in result.records)
    assert result.support_ready_task_ids == ("task-billing-migration",)
    assert result.no_support_task_ids == ()


def test_support_artifact_signals_from_metadata_select_specific_recommendations():
    result = build_task_customer_support_readiness_plan(
        _plan(
            [
                _task(
                    "task-support-content",
                    title="Prepare support content for early access launch",
                    description="Notify known customers about the dashboard behavior change.",
                    files_or_modules=[
                        "support/macros/dashboard_launch.md",
                        "help_center/faq/dashboard_launch.md",
                        "support/escalations/dashboard_launch.md",
                    ],
                    metadata={
                        "support": {
                            "macro": "Zendesk macro for customer questions.",
                            "faq": "Help center FAQ covers expected behavior.",
                            "escalation_path": "Escalate tier 2 cases to the launch on-call.",
                            "customer_comms": "Account managers notify known customers.",
                        }
                    },
                )
            ]
        )
    )

    records = {record.readiness_category: record for record in result.records}

    assert list(records) == [
        "rollout_risk",
        "escalation_path",
        "known_customer_communication",
        "user_visible_behavior",
        "support_macro",
        "faq_help_center",
    ]
    assert records["support_macro"].suggested_artifacts[:2] == (
        "Support macro or canned response update.",
        "Agent-facing support notes with usage guidance.",
    )
    assert "Rollback wording for support replies." in records["support_macro"].suggested_artifacts
    assert "Escalation owner and severity routing." in records["support_macro"].suggested_artifacts
    assert records["faq_help_center"].suggested_artifacts[:2] == (
        "FAQ or help center article update.",
        "Troubleshooting steps for common customer questions.",
    )
    assert "Launch watch items and owner handoff." in records["faq_help_center"].suggested_artifacts
    assert records["escalation_path"].severity == "high"
    assert (
        "Escalation owner and severity routing." in records["escalation_path"].suggested_artifacts
    )
    assert records["known_customer_communication"].severity == "high"
    assert "Known-customer communication plan." in (
        records["known_customer_communication"].suggested_artifacts
    )
    assert any(
        "metadata.support.customer_comms" in item
        for item in records["known_customer_communication"].evidence
    )


def test_no_support_tasks_are_excluded_but_summary_counts_remain_accurate():
    result = build_task_customer_support_readiness_plan(
        _plan(
            [
                _task(
                    "task-internal",
                    title="Refactor internal helper",
                    description="Simplify pure helper naming for maintainability.",
                    files_or_modules=["src/blueprint/internal_helpers.py"],
                    acceptance_criteria=["Unit tests still pass."],
                )
            ]
        )
    )

    assert result.records == ()
    assert result.support_ready_task_ids == ()
    assert result.no_support_task_ids == ("task-internal",)
    assert result.summary == {
        "task_count": 1,
        "support_ready_task_count": 0,
        "no_support_task_count": 1,
        "record_count": 0,
        "category_counts": {
            "billing_account_flow": 0,
            "data_migration": 0,
            "rollout_risk": 0,
            "user_visible_behavior": 0,
            "documentation_change": 0,
            "support_macro": 0,
            "faq_help_center": 0,
            "escalation_path": 0,
            "known_customer_communication": 0,
        },
        "severity_counts": {"critical": 0, "high": 0, "medium": 0},
    }
    assert result.to_markdown() == "\n".join(
        [
            "# Task Customer Support Readiness Plan: plan-support-readiness",
            "",
            "No customer support readiness signals detected.",
        ]
    )


def test_model_dict_alias_serialization_and_markdown_are_stable():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Update help center FAQ",
                description="Add customer help article for new settings page.",
                files_or_modules=["help/faq/settings.md"],
            ),
            _task(
                "task-a",
                title="Add account billing notification",
                description="Customer-facing account email announces a subscription renewal change.",
                files_or_modules=["src/accounts/renewal_email.py"],
            ),
        ],
        plan_id="plan-model",
    )

    result = build_task_customer_support_readiness_plan(ExecutionPlan.model_validate(plan))
    alias_result = derive_task_customer_support_readiness_plan(plan)
    short_alias_result = build_task_customer_support_readiness(plan)
    single = build_task_customer_support_readiness_plan(
        ExecutionTask.model_validate(plan["tasks"][0])
    )
    payload = task_customer_support_readiness_plan_to_dict(result)

    assert isinstance(result, TaskCustomerSupportReadinessPlan)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert alias_result.to_dict() == result.to_dict()
    assert short_alias_result.to_dict() == result.to_dict()
    assert single.plan_id is None
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "records",
        "support_ready_task_ids",
        "no_support_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "readiness_category",
        "severity",
        "suggested_artifacts",
        "rationale",
        "evidence",
    ]
    assert [
        (record.task_id, record.readiness_category, record.severity) for record in result.records
    ] == [
        ("task-a", "billing_account_flow", "critical"),
        ("task-a", "user_visible_behavior", "medium"),
        ("task-z", "user_visible_behavior", "medium"),
        ("task-z", "faq_help_center", "medium"),
    ]
    assert task_customer_support_readiness_plan_to_markdown(result) == "\n".join(
        [
            "# Task Customer Support Readiness Plan: plan-model",
            "",
            "| Task | Category | Severity | Suggested Artifacts | Rationale | Evidence |",
            "| --- | --- | --- | --- | --- | --- |",
            (
                "| `task-a` | billing_account_flow | critical | Customer messaging for billing or account-impacting "
                "changes.; Troubleshooting steps for payment, subscription, or account failures.; Escalation owner "
                "and severity routing. | Billing or account changes can create urgent customer-impacting support "
                "cases. | files_or_modules: src/accounts/renewal_email.py; title: Add account billing notification; "
                "description: Customer-facing account email announces a subscription renewal change. |"
            ),
            (
                "| `task-a` | user_visible_behavior | medium | Support notes explaining the changed customer behavior.; "
                "Troubleshooting steps for the affected workflow. | Customer-visible behavior changes require "
                "support context before launch. | title: Add account billing notification; description: "
                "Customer-facing account email announces a subscription renewal change. |"
            ),
            (
                "| `task-z` | user_visible_behavior | medium | Support notes explaining the changed customer behavior.; "
                "Troubleshooting steps for the affected workflow. | Customer-visible behavior changes require "
                "support context before launch. | description: Add customer help article for new settings page. |"
            ),
            (
                "| `task-z` | faq_help_center | medium | FAQ or help center article update.; Troubleshooting steps "
                "for common customer questions. | FAQ or help center updates reduce avoidable support contacts "
                "after launch. | files_or_modules: help/faq/settings.md; title: Update help center FAQ; "
                "description: Add customer help article for new settings page. |"
            ),
        ]
    )


def _plan(tasks, *, plan_id="plan-support-readiness"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-support-readiness",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "Run support readiness checks.",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
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
        "milestone": "Launch",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "test_command": None,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
