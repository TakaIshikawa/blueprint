import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_customer_migration_window import (
    PlanCustomerMigrationWindowMatrix,
    PlanCustomerMigrationWindowRow,
    build_plan_customer_migration_window_matrix,
    plan_customer_migration_window_matrix_to_dict,
    plan_customer_migration_window_matrix_to_markdown,
    summarize_plan_customer_migration_window,
)


def test_detects_customer_migration_window_signals_and_readiness_gaps():
    result = build_plan_customer_migration_window_matrix(
        _plan(
            [
                _task(
                    "task-accounts",
                    title="Migrate customer accounts to new workspace model",
                    description=(
                        "Run a staged account transition for customer accounts with a migration window."
                    ),
                    files_or_modules=["src/accounts/account_migration.py"],
                    acceptance_criteria=[
                        "Customer success notifies customers before each wave.",
                        "Rollback checkpoint restores the account to the old workspace mapping.",
                    ],
                )
            ]
        )
    )

    assert isinstance(result, PlanCustomerMigrationWindowMatrix)
    assert result.plan_id == "plan-customer-migration"
    assert result.impacted_task_ids == ("task-accounts",)
    row = result.rows[0]
    assert isinstance(row, PlanCustomerMigrationWindowRow)
    assert row.migration_surface == "account_transition"
    assert row.recommended_window_type == "staged_account_transition"
    assert "success_metrics_or_monitoring" in row.readiness_gaps
    assert "scheduled_window_or_cohort" not in row.readiness_gaps
    assert "customer_communication_plan" not in row.readiness_gaps
    assert "title: Migrate customer accounts to new workspace model" in row.evidence
    assert result.summary["impacted_task_count"] == 1
    assert result.summary["window_type_counts"]["staged_account_transition"] == 1


def test_milestone_risk_and_metadata_signals_are_used():
    result = summarize_plan_customer_migration_window(
        _plan(
            [
                _task(
                    "task-billing",
                    title="Move enterprise customers to billing plans",
                    description="Update entitlement mapping.",
                    metadata={
                        "migration_window": "Beta cohort for billing customers",
                        "surface": "billing subscription plan migration",
                    },
                    acceptance_criteria=[
                        "Monitor invoice metrics and alert support escalation owner.",
                        "Rollback checkpoint returns beta customers to legacy entitlements.",
                    ],
                )
            ],
            milestones=[
                {
                    "id": "m1",
                    "title": "Billing beta cohort",
                    "task_ids": ["task-billing"],
                    "description": "Invite beta customers before the billing migration window.",
                }
            ],
            risks=[
                {
                    "task_id": "task-billing",
                    "description": "Customer-visible migration could confuse subscription admins.",
                }
            ],
        )
    )

    row = result.rows[0]
    assert row.migration_surface == "billing_or_subscription"
    assert row.recommended_window_type == "beta_cohort"
    assert row.readiness_gaps == ()
    assert any(item.startswith("metadata.migration_window:") for item in row.evidence)
    assert any(item.startswith("milestones[0]:") for item in row.evidence)
    assert any(item.startswith("risks[0]:") for item in row.evidence)
    assert any("Billing support brief" in need for need in row.communication_needs)


def test_deterministic_ordering_dict_serialization_and_non_mutating_behavior():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Partner API migration",
                description="Customer-visible migration for partner API clients.",
                acceptance_criteria=["Notify partners and monitor API success metrics."],
            ),
            _task(
                "task-a",
                title="Maintenance window for SSO migration",
                description="Customer downtime during SSO authentication migration.",
                acceptance_criteria=["Status page is updated and support owner is on call."],
            ),
            _task(
                "task-m",
                title="Beta cohort tenant cutover",
                description="Beta cohort moves tenant workspaces in waves.",
                acceptance_criteria=[
                    "Customer success owns communication.",
                    "Rollback checkpoint exists.",
                    "Monitor health checks.",
                    "Support escalation owner is assigned.",
                ],
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_customer_migration_window_matrix(plan)
    payload = plan_customer_migration_window_matrix_to_dict(result)

    assert plan == original
    assert result.impacted_task_ids == ("task-m", "task-a", "task-z")
    assert [row.recommended_window_type for row in result.rows] == [
        "beta_cohort",
        "maintenance_window",
        "customer_visible_migration_window",
    ]
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "impacted_task_ids", "summary"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "migration_surface",
        "recommended_window_type",
        "communication_needs",
        "rollback_checkpoint",
        "readiness_gaps",
        "evidence",
    ]
    assert payload["summary"]["task_count"] == 3
    assert payload["summary"]["impacted_task_count"] == 3
    assert payload["summary"]["window_type_counts"] == {
        "maintenance_window": 1,
        "customer_visible_migration_window": 1,
        "beta_cohort": 1,
        "staged_account_transition": 0,
    }


def test_execution_plan_model_input_and_markdown_rendering_are_supported():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-pipe",
                    title="Customer | data migration",
                    description="Customer-visible migration window for customer data.",
                    acceptance_criteria=[
                        "Notify customers through email.",
                        "Scheduled window is published.",
                        "Rollback checkpoint uses backups.",
                        "Monitoring dashboard tracks success metrics.",
                        "Support escalation owner is assigned.",
                    ],
                )
            ],
            plan_id="plan-model",
        )
    )

    result = build_plan_customer_migration_window_matrix(plan)
    markdown = plan_customer_migration_window_matrix_to_markdown(result)

    assert result.plan_id == "plan-model"
    assert result.rows[0].readiness_gaps == ()
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Plan Customer Migration Window Matrix: plan-model")
    assert "Customer \\| data migration" in markdown
    assert "| `task-pipe` |" in markdown
    assert "No customer migration-window signals were detected." not in markdown


def test_non_migration_tasks_are_suppressed_and_empty_markdown_is_stable():
    result = build_plan_customer_migration_window_matrix(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Clarify onboarding labels.",
                    files_or_modules=["src/ui/settings_copy.py"],
                    tags=["frontend"],
                    metadata={"surface": "settings page"},
                )
            ]
        )
    )

    assert result.rows == ()
    assert result.impacted_task_ids == ()
    assert result.summary == {
        "task_count": 1,
        "impacted_task_count": 0,
        "window_type_counts": {
            "maintenance_window": 0,
            "customer_visible_migration_window": 0,
            "beta_cohort": 0,
            "staged_account_transition": 0,
        },
        "surface_counts": {
            "account_transition": 0,
            "billing_or_subscription": 0,
            "tenant_or_workspace": 0,
            "identity_or_access": 0,
            "api_or_integration": 0,
            "data_or_content": 0,
            "customer_experience": 0,
        },
    }
    assert result.to_markdown() == (
        "# Plan Customer Migration Window Matrix: plan-customer-migration\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Impacted task count: 0\n"
        "- Window type counts: maintenance_window 0, customer_visible_migration_window 0, beta_cohort 0, staged_account_transition 0\n"
        "- Surface counts: account_transition 0, billing_or_subscription 0, tenant_or_workspace 0, identity_or_access 0, api_or_integration 0, data_or_content 0, customer_experience 0\n"
        "\n"
        "No customer migration-window signals were detected."
    )


def _plan(tasks, plan_id="plan-customer-migration", milestones=None, risks=None):
    plan = {
        "id": plan_id,
        "implementation_brief_id": "brief-customer-migration",
        "milestones": [] if milestones is None else milestones,
        "tasks": tasks,
    }
    if risks is not None:
        plan["risks"] = risks
    return plan


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
        "depends_on": [],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
