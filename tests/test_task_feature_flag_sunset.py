import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_feature_flag_sunset import (
    TaskFeatureFlagSunsetRecommendation,
    build_task_feature_flag_sunset_plan,
    task_feature_flag_sunset_plan_to_dict,
    task_feature_flag_sunset_plan_to_markdown,
)


def test_rollout_flag_gets_rollout_complete_sunset_recommendation():
    result = build_task_feature_flag_sunset_plan(
        _plan(
            [
                _task(
                    "task-rollout",
                    title="Add checkout beta feature flag",
                    description="Rollout checkout behind a release toggle for internal users.",
                    acceptance_criteria=[
                        "Flag defaults off.",
                        "Rollout reaches 100% before cleanup is started.",
                    ],
                    test_command="pytest tests/test_checkout.py",
                    metadata={"flag_name": "checkout_beta"},
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert isinstance(recommendation, TaskFeatureFlagSunsetRecommendation)
    assert result.flagged_task_ids == ("task-rollout",)
    assert result.no_impact_task_ids == ()
    assert recommendation.flag_name == "checkout_beta"
    assert recommendation.category == "rollout"
    assert recommendation.cleanup_trigger == "rollout_complete"
    assert recommendation.owner_role == "release owner"
    assert recommendation.validation_command == "pytest tests/test_checkout.py"
    assert recommendation.suggested_acceptance_criteria == (
        "Sunset work for checkout_beta is scheduled when rollout complete.",
        "checkout_beta configuration and guarded code paths are removed after sunset.",
        "Validation command passes after cleanup: pytest tests/test_checkout.py",
    )


def test_experiment_flag_gets_experiment_concluded_trigger_and_metadata_owner():
    result = build_task_feature_flag_sunset_plan(
        _plan(
            [
                _task(
                    "task-experiment",
                    title="Run signup copy experiment",
                    description="Add A/B test variants behind an experiment flag.",
                    acceptance_criteria=[
                        "Winner selected after conversion results are reviewed.",
                    ],
                    metadata={
                        "experiment": "Signup Copy V2",
                        "sunset_owner_role": "growth product owner",
                        "sunset_validation_command": "pytest tests/test_signup_experiment.py",
                    },
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.flag_name == "signup_copy_v2"
    assert recommendation.category == "experiment"
    assert recommendation.cleanup_trigger == "experiment_concluded"
    assert recommendation.owner_role == "growth product owner"
    assert recommendation.validation_command == "pytest tests/test_signup_experiment.py"
    assert "metadata.experiment: Signup Copy V2" in recommendation.evidence


def test_configuration_flag_is_detected_from_paths_and_metadata():
    result = build_task_feature_flag_sunset_plan(
        _plan(
            [
                _task(
                    "task-config",
                    title="Wire invoice settings",
                    description="Add config entry for invoice rendering.",
                    files_or_modules=["config/feature_flags.yml", "src/settings.py"],
                    acceptance_criteria=["Configuration validation covers enabled and disabled modes."],
                    metadata={"feature_flag": "invoice-render-v2"},
                )
            ]
        )
    )

    recommendation = result.recommendations[0]

    assert recommendation.flag_name == "invoice_render_v2"
    assert recommendation.category == "configuration"
    assert recommendation.cleanup_trigger == "validation_complete"
    assert recommendation.owner_role == "platform owner"
    assert recommendation.evidence == (
        "files_or_modules: config/feature_flags.yml",
        "files_or_modules[0]: config/feature_flags.yml",
        "metadata.feature_flag: invoice-render-v2",
    )


def test_no_impact_tasks_are_reported_without_recommendations():
    result = build_task_feature_flag_sunset_plan(
        _plan(
            [
                _task(
                    "task-cli",
                    title="Add verbose CLI flag",
                    description="Add a command line option for local diagnostics.",
                    acceptance_criteria=["The --verbose flag prints request ids."],
                ),
                _task(
                    "task-copy",
                    title="Update dashboard empty-state copy",
                    description="No rollout or experiment behavior changes.",
                    acceptance_criteria=["Snapshot test passes."],
                ),
            ]
        )
    )

    assert result.recommendations == ()
    assert result.flagged_task_ids == ()
    assert result.no_impact_task_ids == ("task-cli", "task-copy")
    assert result.to_markdown() == "\n".join(
        [
            "# Task Feature Flag Sunset Plan: plan-sunset",
            "",
            "No feature-flag sunset recommendations were derived.",
        ]
    )


def test_model_dict_and_markdown_representations_are_deterministic():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Introduce search rollout toggle",
                description="Gradual rollout for search ranking.",
                files_or_modules=["src/search/ranking.py"],
                acceptance_criteria=["Enabled for all users before deleting the toggle."],
                test_command="pytest tests/test_search.py",
                metadata={"feature_flag": "search-ranking"},
            ),
            _task("task-docs", title="Update docs", description="Document search ranking."),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = build_task_feature_flag_sunset_plan(model)
    payload = task_feature_flag_sunset_plan_to_dict(result)
    markdown = task_feature_flag_sunset_plan_to_markdown(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["recommendations"]
    assert list(payload) == [
        "plan_id",
        "recommendations",
        "flagged_task_ids",
        "no_impact_task_ids",
    ]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "flag_name",
        "category",
        "cleanup_trigger",
        "owner_role",
        "validation_command",
        "suggested_acceptance_criteria",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert payload["recommendations"][0]["cleanup_trigger"] == "rollout_complete"
    assert markdown.startswith("# Task Feature Flag Sunset Plan: plan-sunset")
    assert "| `task-model` | `search_ranking` | rollout | rollout_complete |" in markdown


def _plan(tasks):
    return {
        "id": "plan-sunset",
        "implementation_brief_id": "brief-sunset",
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
    test_command=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "test_command": test_command,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
