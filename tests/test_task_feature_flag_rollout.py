import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_feature_flag_rollout import (
    build_task_feature_flag_rollout_plan,
    task_feature_flag_rollout_plan_to_dict,
    task_feature_flag_rollout_plan_to_markdown,
)


def test_detects_feature_flag_tasks_from_multiple_fields_with_guidance():
    result = build_task_feature_flag_rollout_plan(
        _plan(
            [
                _task(
                    "task-title",
                    title="Add beta checkout feature flag",
                    description="Gate the new checkout workflow behind a release toggle.",
                    acceptance_criteria=[
                        "Flag defaults off for existing merchants.",
                        "Rollback disables the checkout flag without a deploy.",
                    ],
                ),
                _task(
                    "task-path",
                    title="Wire checkout config",
                    description="Add config entry for checkout rollout.",
                    files_or_modules=["config/feature_flags.yml"],
                    acceptance_criteria=["Config loads in all environments."],
                ),
                _task(
                    "task-metadata",
                    title="Prepare checkout cohort",
                    description="Add cohort targeting.",
                    metadata={"rollout": {"type": "experiment", "feature_flag": "Checkout Beta"}},
                ),
                _task(
                    "task-tags",
                    title="Enable guarded checkout",
                    description="Enable for internal users.",
                    tags=["feature flag", "beta"],
                ),
            ]
        )
    )

    assert result.flagged_task_ids == (
        "task-title",
        "task-path",
        "task-metadata",
        "task-tags",
    )
    assert _recommendation(result, "task-title").flag_name == "checkout"
    assert _recommendation(result, "task-title").rollout_phase == "guard"
    assert _recommendation(result, "task-title").missing_evidence == (
        "no monitoring criterion",
        "no cleanup task",
    )
    assert _recommendation(result, "task-path").evidence == (
        "files_or_modules: config/feature_flags.yml",
        "description: Add config entry for checkout rollout.",
    )
    assert _recommendation(result, "task-path").required_checks == (
        "Flag has a documented default and owner.",
        "Rollout starts disabled or scoped to a safe cohort.",
        "Rollback path is captured in acceptance criteria.",
        "Monitoring signal is captured in acceptance criteria.",
        "Cleanup or retirement task exists for the flag.",
    )


def test_ordinary_flag_language_does_not_create_rollout_recommendations():
    result = build_task_feature_flag_rollout_plan(
        _plan(
            [
                _task(
                    "task-cli",
                    title="Add verbose CLI flag",
                    description="Add a command line option for local diagnostics.",
                    acceptance_criteria=["The --verbose flag prints request ids."],
                ),
                _task(
                    "task-quality",
                    title="Flag suspicious addresses",
                    description="Add a warning flag for invalid shipping addresses.",
                    acceptance_criteria=["Orders with invalid addresses are flagged for review."],
                ),
            ]
        )
    )

    assert result.recommendations == ()
    assert result.flagged_task_ids == ()


def test_slug_is_stable_from_explicit_flag_name_and_title_fallback():
    result = build_task_feature_flag_rollout_plan(
        _plan(
            [
                _task(
                    "task-explicit",
                    title="Add account upgrade feature flag",
                    description="Rollout account upgrade.",
                    metadata={"flag_name": "Account Upgrade V2!"},
                ),
                _task(
                    "task-title",
                    title="Introduce New Billing Summary Toggle",
                    description="Feature toggle controls billing summary.",
                ),
            ]
        )
    )

    assert _recommendation(result, "task-explicit").flag_name == "account_upgrade_v2"
    assert _recommendation(result, "task-title").flag_name == "new_billing_summary"


def test_cleanup_task_detection_satisfies_related_rollout_evidence():
    result = build_task_feature_flag_rollout_plan(
        _plan(
            [
                _task(
                    "task-introduce",
                    title="Introduce subscription export feature flag",
                    description="Rollout subscription export behind a feature flag.",
                    metadata={"flag_name": "subscription_export"},
                    acceptance_criteria=[
                        "Rollback disables subscription export immediately.",
                        "Dashboard monitors export error rate during rollout.",
                    ],
                ),
                _task(
                    "task-cleanup",
                    title="Remove subscription export feature flag",
                    description="Cleanup old toggle after subscription export rollout.",
                    metadata={"flag_name": "subscription_export"},
                    acceptance_criteria=["Regression validation passes after flag removal."],
                ),
            ]
        )
    )

    introduce = _recommendation(result, "task-introduce")
    cleanup = _recommendation(result, "task-cleanup")

    assert introduce.missing_evidence == ()
    assert cleanup.rollout_phase == "cleanup"
    assert cleanup.required_checks == (
        "Flag removal covers enabled and disabled code paths.",
        "Configuration and documentation references are removed.",
        "Post-cleanup regression validation is defined.",
    )
    assert cleanup.missing_evidence == ()


def test_model_and_dict_inputs_are_supported_without_mutation_and_serialize_stably():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Monitor search beta rollout",
                description="Observe feature flag metrics and alert on error rate.",
                files_or_modules=["src/config/search_flags.py"],
                acceptance_criteria=["Dashboard tracks beta rollout conversion and error rate."],
                metadata={"feature_flag": "search-beta"},
            )
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = build_task_feature_flag_rollout_plan(model)
    payload = task_feature_flag_rollout_plan_to_dict(result)
    markdown = task_feature_flag_rollout_plan_to_markdown(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["recommendations"]
    assert list(payload) == ["plan_id", "recommendations", "flagged_task_ids"]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "flag_name",
        "rollout_phase",
        "required_checks",
        "missing_evidence",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert payload["recommendations"][0]["rollout_phase"] == "monitor"
    assert markdown.startswith("# Task Feature Flag Rollout Plan: plan-flags")
    assert "| `task-model` | `search_beta` | monitor |" in markdown


def _recommendation(result, task_id):
    return next(
        recommendation
        for recommendation in result.recommendations
        if recommendation.task_id == task_id
    )


def _plan(tasks):
    return {
        "id": "plan-flags",
        "implementation_brief_id": "brief-flags",
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
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
