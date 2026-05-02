import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_environment_promotion_matrix import (
    PlanEnvironmentPromotionMatrix,
    PlanEnvironmentPromotionRow,
    build_plan_environment_promotion_matrix,
    plan_environment_promotion_matrix_to_dict,
    plan_environment_promotion_matrix_to_markdown,
)


def test_deploy_sensitive_tasks_create_deterministic_promotion_rows():
    result = build_plan_environment_promotion_matrix(
        _plan(
            [
                _task(
                    "task-deploy",
                    title="Deploy search API with canary",
                    description=(
                        "Promote through dev, staging, and production with canary rollout, "
                        "smoke tests, release approval, and rollback steps."
                    ),
                    acceptance_criteria=[
                        "Staging validation passes before production approval.",
                        "Post-deploy smoke test evidence is captured.",
                    ],
                    owner_type="release_engineering",
                ),
                _task(
                    "task-migration",
                    title="Run account schema migration",
                    description="Apply database migration and validate row counts in staging.",
                    files_or_modules=["migrations/versions/add_account_status.sql"],
                    acceptance_criteria=["Production promotion includes restore plan."],
                    risk_level="high",
                ),
                _task(
                    "task-config",
                    title="Update provider config",
                    description=(
                        "Change integration endpoint configuration and env vars for the external "
                        "billing provider."
                    ),
                    metadata={"service_owner": "billing-platform"},
                ),
                _task(
                    "task-docs",
                    title="Refresh docs",
                    description="Update internal documentation for the new API behavior.",
                ),
            ]
        )
    )

    assert isinstance(result, PlanEnvironmentPromotionMatrix)
    assert result.plan_id == "plan-promotion"
    assert result.no_signal_task_ids == ("task-docs",)
    assert [row.task_id for row in result.rows] == [
        "task-deploy",
        "task-migration",
        "task-config",
    ]

    deploy = _row(result, "task-deploy")
    assert isinstance(deploy, PlanEnvironmentPromotionRow)
    assert deploy.affected_environments == (
        "development",
        "staging",
        "production",
        "rollback",
    )
    assert deploy.required_gates == (
        "development_validation",
        "staging_validation",
        "production_approval",
        "smoke_test",
        "canary",
        "rollback_plan",
    )
    assert deploy.rollback_requirement == "Define canary abort triggers and traffic rollback steps."
    assert deploy.priority == "high"
    assert deploy.owner_hint == "release_engineering"
    assert "title: Deploy search API with canary" in deploy.validation_evidence

    migration = _row(result, "task-migration")
    assert migration.required_gates == (
        "development_validation",
        "staging_validation",
        "production_approval",
        "migration_check",
        "backfill_reconciliation",
        "rollback_plan",
    )
    assert migration.owner_hint == "data_owner"
    assert migration.priority == "high"

    config = _row(result, "task-config")
    assert config.affected_environments == ("development", "staging")
    assert config.required_gates == (
        "development_validation",
        "staging_validation",
        "integration_check",
        "config_review",
    )
    assert config.rollback_requirement == "Confirm rollback is not required for this promotion."
    assert config.priority == "medium"
    assert config.owner_hint == "billing-platform"

    assert result.summary == {
        "task_count": 4,
        "promoted_task_count": 3,
        "gate_counts": {
            "development_validation": 3,
            "staging_validation": 3,
            "production_approval": 2,
            "smoke_test": 1,
            "canary": 1,
            "feature_flag": 0,
            "migration_check": 1,
            "backfill_reconciliation": 1,
            "integration_check": 1,
            "config_review": 1,
            "rollback_plan": 2,
        },
        "priority_counts": {"high": 2, "medium": 1, "low": 0},
    }


def test_model_input_serialization_ordering_and_markdown_are_stable():
    plan = _plan(
        [
            _task(
                "task-flag | beta",
                title="Feature flag beta checkout | release",
                description="Rollout behind a feature flag after staging validation.",
                metadata={"owner": "growth"},
            ),
            _task(
                "task-webhook",
                title="Validate webhook integration",
                description="Run environment-specific validation for external provider callbacks.",
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_environment_promotion_matrix(ExecutionPlan.model_validate(plan))
    payload = plan_environment_promotion_matrix_to_dict(result)
    markdown = plan_environment_promotion_matrix_to_markdown(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "no_signal_task_ids", "summary"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "task_title",
        "affected_environments",
        "required_gates",
        "validation_evidence",
        "rollback_requirement",
        "priority",
        "owner_hint",
    ]
    assert [(row.task_id, row.priority) for row in result.rows] == [
        ("task-flag | beta", "medium"),
        ("task-webhook", "medium"),
    ]
    assert markdown.startswith("# Plan Environment Promotion Matrix: plan-promotion")
    assert (
        "| Task | Title | Environments | Gates | Rollback | Priority | Owner | Evidence |"
        in markdown
    )
    assert "`task-flag \\| beta`" in markdown
    assert "Feature flag beta checkout \\| release" in markdown
    assert plan_environment_promotion_matrix_to_markdown(result) == result.to_markdown()


def test_empty_invalid_and_no_signal_inputs_render_deterministic_empty_outputs():
    empty = build_plan_environment_promotion_matrix({"id": "empty-plan", "tasks": []})
    invalid = build_plan_environment_promotion_matrix(17)
    no_signal = build_plan_environment_promotion_matrix(
        _plan(
            [
                _task(
                    "task-api",
                    title="Optimize API pagination",
                    description="Tune backend query limits for account search.",
                    files_or_modules=["src/api/search.py"],
                )
            ]
        )
    )

    assert empty.to_dict() == {
        "plan_id": "empty-plan",
        "rows": [],
        "no_signal_task_ids": [],
        "summary": {
            "task_count": 0,
            "promoted_task_count": 0,
            "gate_counts": {
                "development_validation": 0,
                "staging_validation": 0,
                "production_approval": 0,
                "smoke_test": 0,
                "canary": 0,
                "feature_flag": 0,
                "migration_check": 0,
                "backfill_reconciliation": 0,
                "integration_check": 0,
                "config_review": 0,
                "rollback_plan": 0,
            },
            "priority_counts": {"high": 0, "medium": 0, "low": 0},
        },
    }
    assert empty.to_markdown() == "\n".join(
        [
            "# Plan Environment Promotion Matrix: empty-plan",
            "",
            "Summary: 0 of 0 tasks require promotion gates (high: 0, medium: 0, low: 0).",
            "",
            "No environment promotion gates were inferred.",
        ]
    )
    assert invalid.plan_id is None
    assert invalid.rows == ()
    assert invalid.summary["task_count"] == 0
    assert no_signal.rows == ()
    assert no_signal.no_signal_task_ids == ("task-api",)
    assert "No promotion signals: task-api" in no_signal.to_markdown()


def _row(result, task_id):
    return next(row for row in result.rows if row.task_id == task_id)


def _plan(tasks, *, plan_id="plan-promotion"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-promotion",
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
    owner_type=None,
    risk_level=None,
    metadata=None,
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
    if owner_type is not None:
        task["owner_type"] = owner_type
    if risk_level is not None:
        task["risk_level"] = risk_level
    if metadata is not None:
        task["metadata"] = metadata
    return task
