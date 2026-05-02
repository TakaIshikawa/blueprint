import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_feature_flag_cleanup_readiness import (
    TaskFeatureFlagCleanupReadinessPlan,
    TaskFeatureFlagCleanupReadinessRecord,
    build_task_feature_flag_cleanup_readiness_plan,
    extract_task_feature_flag_cleanup_readiness,
    summarize_task_feature_flag_cleanup_readiness,
    task_feature_flag_cleanup_readiness_plan_to_dict,
    task_feature_flag_cleanup_readiness_plan_to_markdown,
)


def test_detects_stale_flag_cleanup_with_missing_readiness_criteria():
    result = build_task_feature_flag_cleanup_readiness_plan(
        _plan(
            [
                _task(
                    "task-stale",
                    title="Remove stale checkout feature flag",
                    description="Clean up the long-lived checkout_v2 toggle.",
                    files_or_modules=[
                        "config/feature_flags.yml",
                        "src/checkout/checkout_flags.py",
                    ],
                    metadata={"flag_name": "checkout_v2", "cleanup_owner": "payments"},
                )
            ]
        )
    )

    record = result.records[0]

    assert isinstance(result, TaskFeatureFlagCleanupReadinessPlan)
    assert isinstance(record, TaskFeatureFlagCleanupReadinessRecord)
    assert result.flagged_task_ids == ("task-stale",)
    assert result.no_impact_task_ids == ()
    assert record.flag_name == "checkout_v2"
    assert record.cleanup_signals == ("stale_flag", "toggle_removal")
    assert record.cleanup_surfaces == ("flag_configuration", "guarded_code_paths")
    assert record.removal_risk == "high"
    assert record.missing_acceptance_criteria == (
        "rollout_confirmation",
        "config_deletion",
        "code_path_removal",
        "observability_cleanup",
        "rollback_alternative",
    )
    assert record.owner_assumptions == ("payments owns cleanup sign-off.",)
    assert "files_or_modules: config/feature_flags.yml" in record.evidence
    assert "title: Remove stale checkout feature flag" in record.evidence
    assert "Confirm the flag is at 100% rollout or the experiment winner is final." in (
        record.required_verification_steps
    )


def test_experiment_cleanup_toggle_removal_and_dead_branch_cleanup_are_detected():
    result = build_task_feature_flag_cleanup_readiness_plan(
        _plan(
            [
                _task(
                    "task-experiment",
                    title="Retire signup copy experiment",
                    description="Cleanup the A/B test after winner selected.",
                    files_or_modules=["src/experiments/signup_copy.py"],
                    acceptance_criteria=[
                        "Winner selected and experiment concluded before removal.",
                        "Remove experiment config from LaunchDarkly.",
                        "Remove old variant code path.",
                        "Clean up dashboard and alert references.",
                        "Rollback alternative is restoring the winning copy directly.",
                    ],
                    metadata={"experiment": "Signup Copy V2"},
                ),
                _task(
                    "task-toggle",
                    title="Delete account overview rollout toggle",
                    description="Remove feature flag configuration after all users are on the new page.",
                    acceptance_criteria=[
                        "Enabled for all users is confirmed.",
                        "Delete flag entry from config.",
                        "Remove guarded code path.",
                        "Clean up metric dashboard references.",
                        "Fallback is a normal deploy revert.",
                    ],
                    metadata={"feature_flag": "account-overview-v2"},
                ),
                _task(
                    "task-branch",
                    title="Clean up dead branch behind search toggle",
                    description="Remove old code path and disabled branch after rollout complete.",
                    files_or_modules=["src/search/results.py"],
                    acceptance_criteria=[
                        "Rollout complete for 100% of traffic.",
                        "Delete flag configuration.",
                        "Remove dead branch and old code path.",
                        "Clean up observability dashboard and alerts.",
                        "Rollback alternative is restoring the prior implementation.",
                    ],
                    metadata={"flag_name": "search_results_v2"},
                ),
            ]
        )
    )

    assert [record.task_id for record in result.records] == [
        "task-branch",
        "task-experiment",
        "task-toggle",
    ]
    assert _record(result, "task-experiment").cleanup_signals == (
        "experiment_cleanup",
        "toggle_removal",
    )
    assert _record(result, "task-toggle").cleanup_signals == ("toggle_removal",)
    assert _record(result, "task-branch").cleanup_signals == (
        "toggle_removal",
        "dead_branch_cleanup",
    )
    assert _record(result, "task-experiment").removal_risk == "low"
    assert _record(result, "task-toggle").removal_risk == "low"
    assert _record(result, "task-branch").removal_risk == "low"
    assert _record(result, "task-experiment").missing_acceptance_criteria == ()
    assert "experiment_artifacts" in _record(result, "task-experiment").cleanup_surfaces
    assert "observability_assets" in _record(result, "task-branch").cleanup_surfaces


def test_initial_rollout_and_ordinary_flag_tasks_are_not_cleanup_readiness():
    result = build_task_feature_flag_cleanup_readiness_plan(
        _plan(
            [
                _task(
                    "task-rollout",
                    title="Add checkout beta feature flag",
                    description="Rollout checkout behind a release toggle for internal users.",
                    acceptance_criteria=[
                        "Flag defaults off.",
                        "Rollback disables the flag.",
                    ],
                ),
                _task(
                    "task-cli",
                    title="Remove verbose CLI flag",
                    description="Delete a command line option for local diagnostics.",
                    acceptance_criteria=["The --verbose option is no longer documented."],
                ),
            ]
        )
    )

    assert result.records == ()
    assert result.flagged_task_ids == ()
    assert result.no_impact_task_ids == ("task-rollout", "task-cli")
    assert result.summary == {
        "flagged_task_count": 0,
        "risk_counts": {"low": 0, "medium": 0, "high": 0},
        "signal_counts": {
            "stale_flag": 0,
            "experiment_cleanup": 0,
            "toggle_removal": 0,
            "dead_branch_cleanup": 0,
        },
    }


def test_existing_acceptance_safeguards_lower_reported_risk():
    guarded = build_task_feature_flag_cleanup_readiness_plan(
        _plan(
            [
                _task(
                    "task-guarded",
                    title="Remove stale invoice feature flag",
                    description="Cleanup stale invoice flag.",
                    acceptance_criteria=[
                        "Rollout complete for 100% of accounts.",
                        "Delete flag config in all environments.",
                        "Remove guarded code path.",
                        "Clean up dashboards and alerts.",
                        "Rollback alternative is a normal code revert.",
                    ],
                    metadata={"flag_name": "invoice_v2"},
                )
            ]
        )
    )
    unguarded = build_task_feature_flag_cleanup_readiness_plan(
        _plan(
            [
                _task(
                    "task-unguarded",
                    title="Remove stale invoice feature flag",
                    description="Cleanup stale invoice flag.",
                    metadata={"flag_name": "invoice_v2"},
                )
            ]
        )
    )

    assert guarded.records[0].missing_acceptance_criteria == ()
    assert guarded.records[0].removal_risk == "low"
    assert unguarded.records[0].missing_acceptance_criteria == (
        "rollout_confirmation",
        "config_deletion",
        "code_path_removal",
        "observability_cleanup",
        "rollback_alternative",
    )
    assert unguarded.records[0].removal_risk == "high"


def test_model_dict_and_markdown_representations_are_deterministic():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Consolidate billing release toggle",
                description="Remove feature flag after all traffic uses billing v2.",
                files_or_modules=["config/flags.yaml", "src/billing/view.py"],
                acceptance_criteria=[
                    "All users are enabled before cleanup.",
                    "Delete flag configuration.",
                    "Remove old code path.",
                    "Clean up alert dashboard references.",
                    "Rollback alternative is the billing v1 revert plan.",
                ],
                metadata={"feature_flag": "billing-v2"},
            ),
            _task("task-docs", title="Update docs", description="Document billing v2."),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = summarize_task_feature_flag_cleanup_readiness(model)
    payload = task_feature_flag_cleanup_readiness_plan_to_dict(result)
    markdown = task_feature_flag_cleanup_readiness_plan_to_markdown(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert extract_task_feature_flag_cleanup_readiness(model).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "records",
        "flagged_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "flag_name",
        "cleanup_signals",
        "cleanup_surfaces",
        "removal_risk",
        "required_verification_steps",
        "missing_acceptance_criteria",
        "owner_assumptions",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert payload["records"][0]["removal_risk"] == "low"
    assert markdown.startswith("# Task Feature Flag Cleanup Readiness: plan-cleanup")
    assert "| `task-model` | `billing_v2` | toggle_removal, dead_branch_cleanup |" in markdown


def test_single_task_model_input_is_supported_without_mutation():
    task = _task(
        "task-single",
        title="Remove stale profile toggle",
        description="Delete stale feature flag configuration after rollout complete.",
        acceptance_criteria=[
            "Rollout complete for all users.",
            "Delete flag configuration.",
            "Remove old code path.",
            "Clean up dashboard references.",
            "Rollback alternative is restoring the old branch.",
        ],
        metadata={"flag_name": "profile_v2"},
    )
    model = ExecutionTask.model_validate(task)
    before = copy.deepcopy(model.model_dump(mode="python"))

    result = build_task_feature_flag_cleanup_readiness_plan(model)

    assert model.model_dump(mode="python") == before
    assert result.plan_id is None
    assert result.flagged_task_ids == ("task-single",)
    assert result.records[0].flag_name == "profile_v2"


def _record(result, task_id):
    return next(record for record in result.records if record.task_id == task_id)


def _plan(tasks):
    return {
        "id": "plan-cleanup",
        "implementation_brief_id": "brief-cleanup",
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
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "test_command": None,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
