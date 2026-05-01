import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_feature_flag_readiness import (
    TaskFeatureFlagReadinessPlan,
    TaskFeatureFlagReadinessRecord,
    build_task_feature_flag_readiness_plan,
    summarize_task_feature_flag_readiness,
    task_feature_flag_readiness_plan_to_dict,
    task_feature_flag_readiness_plan_to_markdown,
)


def test_single_task_input_builds_deterministic_flag_safeguards():
    task = ExecutionTask.model_validate(
        _task(
            "task-checkout-flag",
            title="Gate checkout rewrite behind feature flag",
            description=(
                "Use a kill switch, default off, and cohort: internal users before "
                "a 10% rollout."
            ),
            acceptance_criteria=[
                "Rollback: disable the flag and confirm old checkout is restored.",
            ],
            files_or_modules=["src/flags/checkout_feature_flag.py"],
            owner_type="payments",
            test_command="poetry run pytest tests/test_checkout_flags.py",
        )
    )

    result = build_task_feature_flag_readiness_plan(task)

    assert result.plan_id is None
    assert result.flagged_task_ids == ("task-checkout-flag",)
    assert result.summary["flagged_task_count"] == 1
    assert result.records == (
        TaskFeatureFlagReadinessRecord(
            task_id="task-checkout-flag",
            title="Gate checkout rewrite behind feature flag",
            readiness_level="ready",
            detected_flag_signals=(
                "feature_flag",
                "kill_switch",
                "cohort",
                "percentage_rollout",
            ),
            required_steps=(
                "Confirm flag owner before implementation starts.",
                "Keep the default state disabled until validation passes.",
                "Verify a kill switch or immediate disable path exists.",
                "Define the first cohort, expansion criteria, and exclusion rules.",
                "Document rollback behavior for flag disablement and code revert fallback.",
                "Record the staged rollout percentages and promotion gates.",
                "Run the suggested validation commands before each exposure increase.",
            ),
            rollout_safeguards={
                "owner": "payments",
                "default_state": "off by default",
                "kill_switch": "kill switch documented in task context",
                "cohort_definition": "internal users before a 10% rollout",
                "rollback_behavior": "disable the flag and confirm old checkout is restored",
            },
            suggested_validation_commands=(
                "poetry run pytest tests/test_checkout_flags.py",
            ),
            evidence=(
                "files_or_modules: src/flags/checkout_feature_flag.py",
                "title: Gate checkout rewrite behind feature flag",
                "description: Use a kill switch, default off, and cohort: internal users before a 10% rollout.",
            ),
        ),
    )


def test_plan_input_uses_plan_commands_and_detects_text_fields():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-beta",
                    title="Enable beta access toggle for search",
                    description="Private beta users get staged enablement through a feature toggle.",
                    acceptance_criteria=[
                        "Control group remains excluded during the experiment.",
                    ],
                    metadata={
                        "flag_owner": "growth",
                        "flag_default": "disabled",
                        "kill_switch": "Disable search_beta_access in LaunchDarkly.",
                        "rollback_behavior": "Turn off search_beta_access for all cohorts.",
                    },
                ),
                _task(
                    "task-docs",
                    title="Update API docs",
                    description="Document the search endpoint.",
                ),
            ],
            metadata={
                "validation_commands": {
                    "test": ["poetry run pytest tests/test_search_beta.py"],
                    "lint": ["poetry run ruff check src/blueprint"],
                }
            },
        )
    )

    result = summarize_task_feature_flag_readiness(plan)

    assert isinstance(result, TaskFeatureFlagReadinessPlan)
    assert result.plan_id == "plan-feature-flags"
    assert result.flagged_task_ids == ("task-beta",)
    record = result.records[0]
    assert record.readiness_level == "ready"
    assert record.detected_flag_signals == (
        "feature_flag",
        "toggle",
        "kill_switch",
        "cohort",
        "experiment",
        "beta_access",
        "staged_enablement",
    )
    assert record.rollout_safeguards == {
        "owner": "growth",
        "default_state": "disabled",
        "kill_switch": "Disable search_beta_access in LaunchDarkly",
        "cohort_definition": "beta users",
        "rollback_behavior": "Turn off search_beta_access for all cohorts",
    }
    assert record.suggested_validation_commands == (
        "poetry run pytest tests/test_search_beta.py",
        "poetry run ruff check src/blueprint",
    )


def test_metadata_notes_and_file_paths_are_evidence():
    result = build_task_feature_flag_readiness_plan(
        [
            _task(
                "task-metadata",
                title="Prepare account settings rollout",
                description="No public launch until safeguards are reviewed.",
                files_or_modules=[
                    "config/feature-flags/account_settings.yaml",
                    "src/experiments/account_settings.py",
                ],
                notes=[
                    "Canary ramp starts at 5% of accounts.",
                    "Kill switch is the account_settings_v2 flag.",
                ],
                metadata={
                    "release": {
                        "audience": "enterprise admins",
                        "owner_team": "accounts",
                        "default_state": "off by default",
                    },
                    "validation_command": "poetry run pytest tests/test_account_settings.py",
                },
            )
        ]
    )

    record = result.records[0]
    assert record.detected_flag_signals == (
        "feature_flag",
        "kill_switch",
        "cohort",
        "percentage_rollout",
        "experiment",
    )
    assert record.rollout_safeguards["owner"] == "accounts"
    assert record.rollout_safeguards["cohort_definition"] == "enterprise admins"
    assert record.suggested_validation_commands == (
        "poetry run pytest tests/test_account_settings.py",
    )
    assert "files_or_modules: config/feature-flags/account_settings.yaml" in record.evidence
    assert "files_or_modules: src/experiments/account_settings.py" in record.evidence
    assert "notes[0]: Canary ramp starts at 5% of accounts." in record.evidence
    assert "notes[1]: Kill switch is the account_settings_v2 flag." in record.evidence


def test_records_and_serialization_are_stably_ordered():
    result = build_task_feature_flag_readiness_plan(
        [
            _task(
                "task-z",
                title="Roll out notifications feature flag",
                metadata={"owner": "notifications"},
            ),
            _task(
                "task-a",
                title="Run pricing A/B test",
                description="Experiment with a holdout cohort and feature flag.",
                metadata={
                    "owner": "pricing",
                    "default_state": "off",
                    "kill_switch": "Disable pricing_test.",
                    "cohort": "trial accounts",
                    "rollback": "Disable pricing_test.",
                },
            ),
        ]
    )
    payload = task_feature_flag_readiness_plan_to_dict(result)

    assert result.flagged_task_ids == ("task-a", "task-z")
    assert result.to_dicts() == payload["records"]
    assert list(payload) == ["plan_id", "records", "flagged_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "readiness_level",
        "detected_flag_signals",
        "required_steps",
        "rollout_safeguards",
        "suggested_validation_commands",
        "evidence",
    ]
    assert list(payload["records"][0]["rollout_safeguards"]) == [
        "owner",
        "default_state",
        "kill_switch",
        "cohort_definition",
        "rollback_behavior",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert task_feature_flag_readiness_plan_to_markdown(result) == result.to_markdown()
    assert task_feature_flag_readiness_plan_to_markdown(result).startswith(
        "# Task Feature Flag Readiness"
    )


def test_non_flag_tasks_return_no_checklist_entries():
    result = build_task_feature_flag_readiness_plan(
        _plan(
            [
                _task(
                    "task-api",
                    title="Add profile endpoint",
                    description="Create backend route for profile reads.",
                    acceptance_criteria=["Endpoint returns active profile fields."],
                    files_or_modules=["src/api/profile.py"],
                )
            ]
        )
    )

    assert result.plan_id == "plan-feature-flags"
    assert result.records == ()
    assert result.flagged_task_ids == ()
    assert result.to_dict() == {
        "plan_id": "plan-feature-flags",
        "records": [],
        "flagged_task_ids": [],
        "summary": {
            "flagged_task_count": 0,
            "readiness_counts": {
                "ready": 0,
                "needs_safeguards": 0,
                "needs_owner": 0,
            },
            "signal_counts": {
                "feature_flag": 0,
                "toggle": 0,
                "kill_switch": 0,
                "cohort": 0,
                "percentage_rollout": 0,
                "experiment": 0,
                "beta_access": 0,
                "staged_enablement": 0,
            },
        },
    }
    assert result.to_markdown() == (
        "# Task Feature Flag Readiness: plan-feature-flags\n\n"
        "No feature-flag readiness checklist entries were inferred."
    )


def _plan(tasks, *, metadata=None):
    return {
        "id": "plan-feature-flags",
        "implementation_brief_id": "brief-feature-flags",
        "milestones": [],
        "tasks": tasks,
        "metadata": metadata or {},
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    notes=None,
    owner_type=None,
    test_command=None,
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
    if notes is not None:
        task["notes"] = notes
    if owner_type is not None:
        task["owner_type"] = owner_type
    if test_command is not None:
        task["test_command"] = test_command
    return task
