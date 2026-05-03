import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_release_rollback_decision_matrix import (
    PlanReleaseRollbackDecisionMatrix,
    PlanReleaseRollbackDecisionRow,
    analyze_plan_release_rollback_decisions,
    build_plan_release_rollback_decision_matrix,
    generate_plan_release_rollback_decision_matrix,
    plan_release_rollback_decision_matrix_to_dict,
    plan_release_rollback_decision_matrix_to_dicts,
    plan_release_rollback_decision_matrix_to_markdown,
)


def test_detects_rollback_sensitive_risks_from_task_surfaces():
    result = build_plan_release_rollback_decision_matrix(
        _plan(
            [
                _task(
                    "task-schema",
                    title="Add account schema migration",
                    files_or_modules=["migrations/20260503_account.sql"],
                ),
                _task(
                    "task-integration",
                    title="Roll out Stripe webhook integration",
                    description="Customer-visible checkout release with provider fallback.",
                    files_or_modules=["src/integrations/stripe/webhook.py"],
                    acceptance_criteria=["Rollback criteria: abort if provider errors exceed 2%."],
                    test_command="poetry run pytest tests/test_stripe_webhook.py",
                    metadata={"owner": "payments team"},
                ),
                _task(
                    "task-purge",
                    title="Purge legacy billing records",
                    description="Irreversible hard delete after export.",
                    depends_on=["task-schema", "task-data-migration"],
                    validation_commands=["dry-run purge validation"],
                ),
            ]
        )
    )

    by_id = {row.task_id: row for row in result.rows}

    assert isinstance(result, PlanReleaseRollbackDecisionMatrix)
    assert all(isinstance(row, PlanReleaseRollbackDecisionRow) for row in result.rows)
    assert by_id["task-schema"].risk_categories == ("schema_change",)
    assert by_id["task-integration"].risk_categories == (
        "external_integration",
        "customer_visible_change",
        "billing_change",
    )
    assert by_id["task-purge"].risk_categories == (
        "schema_change",
        "data_migration",
        "billing_change",
        "irreversible_operation",
    )
    assert "explicit rollback criteria" in by_id["task-integration"].rollback_decision_signals
    assert "validation command" in by_id["task-purge"].rollback_decision_signals
    assert any("depends_on" in item for item in by_id["task-purge"].evidence)
    assert result.rollback_task_ids == ("task-integration", "task-purge", "task-schema")


def test_required_decision_points_owners_missing_inputs_and_priority_are_deterministic():
    result = build_plan_release_rollback_decision_matrix(
        _plan(
            [
                _task(
                    "task-flag",
                    title="Feature flag rollout for new dashboard",
                    description="Production rollout to customers.",
                    files_or_modules=["src/flags/dashboard_rollout.py"],
                    metadata={"owner": "growth team"},
                    acceptance_criteria=[
                        "Go/no-go rollback threshold: disable flag if dashboard errors exceed 1%.",
                        "Support communication covers customer impact.",
                    ],
                    test_command="poetry run pytest tests/test_dashboard_rollout.py",
                ),
                _task(
                    "task-billing",
                    title="Launch billing metering change",
                    description="Customer-visible billing release.",
                    files_or_modules=["src/billing/metering.py"],
                ),
            ]
        )
    )

    billing, flag = result.rows

    assert billing.task_id == "task-billing"
    assert billing.priority == "high"
    assert billing.required_decision_points == (
        "Named go/no-go owner and backup approver",
        "Objective rollback trigger thresholds before release",
        "Post-rollback validation command or evidence",
        "Customer-impact threshold and communication handoff",
        "Charge, invoice, tax, or metering correction threshold",
    )
    assert billing.owner_suggestions == (
        "release owner",
        "engineering owner",
        "support lead",
        "customer success owner",
        "billing owner",
    )
    assert billing.missing_inputs == (
        "Assign rollback decision owner.",
        "Define objective go/no-go and rollback trigger criteria.",
        "Add post-rollback validation command or evidence.",
        "Document customer-impact and communication input.",
        "Document billing correction or reversal input.",
    )

    assert flag.task_id == "task-flag"
    assert flag.priority == "medium"
    assert flag.owner_suggestions[:3] == ("growth team", "release owner", "engineering owner")
    assert flag.missing_inputs == ()


def test_explicit_rollback_criteria_reduce_missing_inputs():
    explicit = build_plan_release_rollback_decision_matrix(
        _plan(
            [
                _task(
                    "task-explicit",
                    title="Deploy customer-visible API rollout",
                    acceptance_criteria=[
                        "Rollback criteria: roll back if API error rate exceeds 3% for five minutes.",
                    ],
                    test_command="poetry run pytest tests/test_api_smoke.py",
                    metadata={"owner": "api owner"},
                )
            ]
        )
    ).rows[0]
    implicit = build_plan_release_rollback_decision_matrix(
        _plan([_task("task-implicit", title="Deploy customer-visible API rollout")])
    ).rows[0]

    assert len(explicit.missing_inputs) < len(implicit.missing_inputs)
    assert "Define objective go/no-go and rollback trigger criteria." in implicit.missing_inputs
    assert "Define objective go/no-go and rollback trigger criteria." not in explicit.missing_inputs


def test_no_risk_plans_return_empty_matrix_and_markdown_message():
    result = build_plan_release_rollback_decision_matrix(
        _plan(
            [_task("task-copy", title="Update local copy", description="Edit static copy.")],
            plan_id="plan-empty",
        )
    )

    assert result.plan_id == "plan-empty"
    assert result.rows == ()
    assert result.records == ()
    assert result.summary == {
        "task_count": 1,
        "rollback_task_count": 0,
        "decision_row_count": 0,
        "priority_counts": {"high": 0, "medium": 0, "low": 0},
        "risk_counts": {
            "schema_change": 0,
            "data_migration": 0,
            "external_integration": 0,
            "feature_flag_rollout": 0,
            "customer_visible_change": 0,
            "billing_change": 0,
            "irreversible_operation": 0,
        },
        "missing_input_count": 0,
    }
    assert result.to_markdown() == "\n".join(
        [
            "# Plan Release Rollback Decision Matrix: plan-empty",
            "",
            "## Summary",
            "",
            "- Task count: 1",
            "- Rollback task count: 0",
            "- Decision row count: 0",
            "- Priority counts: high 0, medium 0, low 0",
            "",
            "No release rollback decision rows were detected.",
        ]
    )


def test_dict_and_markdown_serialization_are_stable_for_model_input():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-api",
                    title="Deploy billing API | endpoint",
                    acceptance_criteria=[
                        "Rollback criteria: revert if billing API smoke test fails.",
                    ],
                    test_command="poetry run pytest tests/test_billing_api.py",
                ),
            ],
            plan_id="plan-model",
        )
    )

    result = generate_plan_release_rollback_decision_matrix(plan)
    payload = plan_release_rollback_decision_matrix_to_dict(result)
    markdown = plan_release_rollback_decision_matrix_to_markdown(result)

    assert analyze_plan_release_rollback_decisions(result) is result
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert plan_release_rollback_decision_matrix_to_dicts(result) == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "rollback_task_ids", "summary"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "risk_categories",
        "rollback_decision_signals",
        "required_decision_points",
        "owner_suggestions",
        "missing_inputs",
        "priority",
        "evidence",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Plan Release Rollback Decision Matrix: plan-model")
    assert "Deploy billing API \\| endpoint" in markdown


def _plan(tasks, *, plan_id="plan-release-rollback-decisions"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-release-rollback-decisions",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "web_app",
        "milestones": [{"name": "Build"}, {"name": "Launch"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement the plan",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    depends_on=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    test_command=None,
    validation_commands=None,
):
    task = {
        "id": task_id,
        "title": title or f"Task {task_id}",
        "description": description or f"Implement {task_id}.",
        "milestone": "Build",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": files_or_modules or ["src/app.py"],
        "acceptance_criteria": acceptance_criteria or [f"{task_id} works"],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "test_command": test_command,
        "status": "pending",
        "metadata": metadata or {},
        "blocked_reason": None,
    }
    if validation_commands is not None:
        task["validation_commands"] = validation_commands
    return task
