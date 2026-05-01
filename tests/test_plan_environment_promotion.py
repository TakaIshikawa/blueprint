import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_environment_promotion import (
    PlanEnvironmentPromotionMap,
    PromotionBlocker,
    build_plan_environment_promotion_map,
    derive_plan_environment_promotion_map,
    plan_environment_promotion_map_to_dict,
    plan_environment_promotion_map_to_markdown,
)


def test_tasks_are_assigned_to_stable_stages_from_text_paths_commands_and_metadata():
    result = build_plan_environment_promotion_map(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update local help copy",
                    files_or_modules=["docs/help.md"],
                ),
                _task(
                    "task-ci",
                    title="Add CI validation",
                    files_or_modules=[".github/workflows/plan.yml"],
                    test_command="poetry run pytest tests/test_plan.py",
                ),
                _task(
                    "task-preview",
                    title="Add preview deployment config",
                    description="Create a review app for the dashboard.",
                    files_or_modules=["config/vercel/preview.json"],
                ),
                _task(
                    "task-prod",
                    title="Roll out checkout release",
                    description="Deploy through staging and production.",
                    files_or_modules=["deploy/production/checkout.yaml"],
                    metadata={"validation_commands": {"test": ["make smoke"]}},
                    risk_level="medium",
                ),
            ]
        )
    )

    assert [stage.name for stage in result.stages] == [
        "local",
        "ci",
        "preview",
        "staging",
        "production",
    ]
    assert result.task_stage_map == {
        "task-copy": ["local"],
        "task-ci": ["local", "ci"],
        "task-preview": ["local", "preview"],
        "task-prod": ["local", "ci", "staging", "production"],
    }
    assert _stage(result, "local").task_ids == (
        "task-copy",
        "task-ci",
        "task-preview",
        "task-prod",
    )
    assert _stage(result, "ci").task_ids == ("task-ci", "task-prod")
    assert _stage(result, "production").risk_notes == (
        "task-prod: risk level is medium.",
        "task-prod: promotion context contains elevated risk signals.",
    )


def test_later_stage_dependencies_on_incomplete_earlier_tasks_become_blockers():
    result = build_plan_environment_promotion_map(
        _plan(
            [
                _task(
                    "task-migration",
                    title="Add customer schema migration",
                    files_or_modules=["migrations/20260501_customer.sql"],
                    status="in_progress",
                ),
                _task(
                    "task-release",
                    title="Production rollout",
                    description="Deploy to production after migration.",
                    files_or_modules=["deploy/prod/app.yaml"],
                    depends_on=["task-migration"],
                ),
                _task(
                    "task-complete-ci",
                    title="CI pipeline",
                    files_or_modules=[".github/workflows/ci.yml"],
                    status="completed",
                ),
                _task(
                    "task-staging",
                    title="Staging smoke test",
                    description="Run staging validation.",
                    depends_on=["task-complete-ci"],
                ),
            ]
        )
    )

    production = _stage(result, "production")

    assert production.blockers == (
        PromotionBlocker(
            task_id="task-release",
            blocked_by="task-migration",
            blocker_stage="data-migration",
            status="in_progress",
            reason=(
                "task-release cannot enter production until task-migration clears data-migration."
            ),
        ),
    )
    assert _stage(result, "staging").blockers == ()


def test_migration_and_rollback_tasks_create_evidence_before_production_promotion():
    result = build_plan_environment_promotion_map(
        _plan(
            [
                _task(
                    "task-backfill",
                    title="Backfill invoices",
                    description="Run data migration for invoice search.",
                    files_or_modules=["db/migrations/20260501_backfill_invoices.py"],
                ),
                _task(
                    "task-rollback",
                    title="Document production rollback",
                    description="Add rollback checklist and restore procedure.",
                    files_or_modules=["runbooks/rollback/invoice-search.md"],
                ),
            ]
        )
    )

    assert _stage(result, "data-migration").required_evidence == (
        "task-backfill: migration dry-run or non-production apply result is recorded.",
        "task-backfill: backup, restore, or down-migration path is documented.",
    )
    assert _stage(result, "rollback").required_evidence == (
        "task-rollback: rollback procedure is rehearsed or reviewed.",
        "task-rollback: post-rollback verification command or checklist is recorded.",
    )
    assert _stage(result, "production").required_evidence == (
        "task-backfill: migration evidence must be approved before production promotion.",
        "task-rollback: rollback evidence must be approved before production promotion.",
    )


def test_serialization_markdown_and_input_plan_are_deterministic_without_mutation():
    plan = _plan(
        [
            _task(
                "task-migration",
                title="Add production migration with rollback",
                description=(
                    "Deploy database migration to production with a down migration "
                    "and rollback restore path."
                ),
                files_or_modules=["migrations/20260501_add_flag.sql"],
                risk_level="high",
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_environment_promotion_map(plan)
    payload = plan_environment_promotion_map_to_dict(result)

    assert plan == original
    assert isinstance(result, PlanEnvironmentPromotionMap)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["stages"]
    assert list(payload) == ["plan_id", "stages", "task_stage_map"]
    assert list(payload["stages"][0]) == [
        "name",
        "task_ids",
        "blockers",
        "required_evidence",
        "risk_notes",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert plan_environment_promotion_map_to_markdown(result) == "\n".join(
        [
            "# Plan Environment Promotion Map: plan-promotion",
            "",
            "| Stage | Tasks | Blockers | Required Evidence | Risk Notes |",
            "| --- | --- | --- | --- | --- |",
            "| local | task-migration | none | none | task-migration: risk level is high. |",
            "| data-migration | task-migration | none | "
            "task-migration: migration dry-run or non-production apply result is recorded.; "
            "task-migration: backup, restore, or down-migration path is documented. | "
            "task-migration: risk level is high.; "
            "task-migration: promotion context contains elevated risk signals. |",
            "| rollback | task-migration | none | "
            "task-migration: rollback procedure is rehearsed or reviewed.; "
            "task-migration: post-rollback verification command or checklist is recorded. | "
            "task-migration: risk level is high.; "
            "task-migration: promotion context contains elevated risk signals. |",
            "| production | task-migration | none | "
            "task-migration: migration evidence must be approved before production promotion.; "
            "task-migration: rollback evidence must be approved before production promotion. | "
            "task-migration: risk level is high.; "
            "task-migration: promotion context contains elevated risk signals. |",
        ]
    )


def test_execution_plan_model_iterable_alias_and_empty_plan_are_supported():
    model = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-ci",
                    title="Run CI",
                    files_or_modules=[".github/workflows/ci.yml"],
                    test_command="poetry run pytest",
                ),
                _task(
                    "task-docs",
                    title="Update docs",
                    files_or_modules=["README.md"],
                ),
            ]
        )
    )

    result = derive_plan_environment_promotion_map(model)
    iterable = build_plan_environment_promotion_map([model.tasks[1], model.tasks[0]])
    empty = build_plan_environment_promotion_map({"id": "plan-empty", "tasks": []})

    assert result.plan_id == "plan-promotion"
    assert result.task_stage_map["task-ci"] == ["local", "ci"]
    assert iterable.plan_id is None
    assert iterable.task_stage_map == {
        "task-docs": ["local"],
        "task-ci": ["local", "ci"],
    }
    assert empty.to_markdown() == "\n".join(
        [
            "# Plan Environment Promotion Map: plan-empty",
            "",
            "No environment promotion stages were derived.",
        ]
    )


def _stage(result, name):
    return next(stage for stage in result.stages if stage.name == name)


def _plan(tasks):
    return {
        "id": "plan-promotion",
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
    depends_on=None,
    risk_level=None,
    test_command=None,
    status="pending",
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": depends_on or [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "risk_level": risk_level,
        "test_command": test_command,
        "status": status,
        "metadata": metadata or {},
    }
    return task
