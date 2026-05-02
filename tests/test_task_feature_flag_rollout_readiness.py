import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_feature_flag_rollout_readiness import (
    TaskFeatureFlagRolloutReadinessPlan,
    TaskFeatureFlagRolloutReadinessRecord,
    analyze_task_feature_flag_rollout_readiness,
    build_task_feature_flag_rollout_readiness_plan,
    extract_task_feature_flag_rollout_readiness,
    generate_task_feature_flag_rollout_readiness,
    summarize_task_feature_flag_rollout_readiness,
    task_feature_flag_rollout_readiness_plan_to_dict,
    task_feature_flag_rollout_readiness_plan_to_dicts,
    task_feature_flag_rollout_readiness_plan_to_markdown,
)


def test_percentage_rollout_generates_progressive_gates_and_enabled_disabled_validation():
    result = build_task_feature_flag_rollout_readiness_plan(
        _plan(
            [
                _task(
                    "task-ramp",
                    title="Roll out checkout rewrite behind feature flag",
                    description=(
                        "Create feature flag checkout_v2, default off, target eligible merchants, "
                        "then ramp 5%, 25%, and 100% with monitoring gates."
                    ),
                    files_or_modules=["config/feature_flags/checkout_v2.yml"],
                    acceptance_criteria=[
                        "Rollback disables the flag and confirms old checkout is restored.",
                        "QA covers flag on and flag off states before each ramp.",
                    ],
                ),
                _task("task-copy", title="Update settings copy", description="Change labels only."),
            ]
        )
    )

    assert isinstance(result, TaskFeatureFlagRolloutReadinessPlan)
    assert result.rollout_task_ids == ("task-ramp",)
    assert result.no_signal_task_ids == ("task-copy",)
    record = result.records[0]
    assert isinstance(record, TaskFeatureFlagRolloutReadinessRecord)
    assert record.detected_signals == (
        "feature_flag",
        "default_state",
        "targeting",
        "percentage_rollout",
        "monitoring_gate",
        "rollback",
        "qa_coverage",
    )
    assert record.readiness_level == "ready"
    assert any("progressive rollout percentages" in task for task in record.rollout_tasks)
    assert any("Disabled state validation" in criterion for criterion in record.validation_criteria)
    assert any("Enabled state validation" in criterion for criterion in record.validation_criteria)
    assert any("Percentage rollout validation" in criterion for criterion in record.validation_criteria)
    assert "files_or_modules: config/feature_flags/checkout_v2.yml" in record.evidence
    assert result.summary["status"] == "ready"
    assert result.summary["signal_counts"]["percentage_rollout"] == 1


def test_beta_only_release_uses_beta_cohort_tasks_without_percentage_ramp():
    result = analyze_task_feature_flag_rollout_readiness(
        _plan(
            [
                _task(
                    "task-beta",
                    title="Enable reporting private beta feature flag",
                    description=(
                        "Private beta users receive access through an allowlist. "
                        "Default disabled for everyone else."
                    ),
                    metadata={
                        "targeting": {
                            "audience": "beta accounts",
                            "exclusion_rules": "exclude suspended tenants",
                        }
                    },
                    acceptance_criteria=[
                        "Support can identify beta users and confirm ineligible tenants remain unexposed.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_signals == (
        "feature_flag",
        "default_state",
        "targeting",
        "beta_cohort",
    )
    assert record.readiness_level == "partial"
    assert any("beta cohort targeting" in task for task in record.rollout_tasks)
    assert not any("progressive rollout percentages" in task for task in record.rollout_tasks)
    assert any("Beta cohort validation" in criterion for criterion in record.validation_criteria)
    assert not any("Percentage rollout validation" in criterion for criterion in record.validation_criteria)


def test_kill_switch_requirements_add_immediate_disable_task_and_validation():
    result = generate_task_feature_flag_rollout_readiness(
        _plan(
            [
                _task(
                    "task-kill",
                    title="Add payments kill switch",
                    description=(
                        "Implement a killswitch for payment retries with an operator off switch, "
                        "rollback, and dashboard alert on retry error rate."
                    ),
                    files_or_modules=["src/flags/payment_retry_kill_switch.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.detected_signals == (
        "feature_flag",
        "monitoring_gate",
        "rollback",
        "kill_switch",
    )
    assert any("kill switch behavior" in task for task in record.rollout_tasks)
    assert any("without a code deploy" in task for task in record.rollout_tasks)
    assert any("Kill switch validation" in criterion for criterion in record.validation_criteria)
    assert any("Rollback validation" in criterion for criterion in record.validation_criteria)


def test_cleanup_expectations_include_removal_criteria_and_cleanup_validation():
    result = build_task_feature_flag_rollout_readiness_plan(
        _plan(
            [
                _task(
                    "task-cleanup",
                    title="Remove stale search feature flag",
                    description=(
                        "Cleanup search_v2 after 100% rollout and delete flag config, "
                        "dead branches, and documentation references."
                    ),
                    files_or_modules=["config/feature_flags/search_v2.yml"],
                    acceptance_criteria=["Regression tests cover enabled and disabled state before removal."],
                )
            ]
        )
    )

    record = result.records[0]
    assert "cleanup" in record.detected_signals
    assert any("cleanup criteria" in task for task in record.rollout_tasks)
    assert any("100% rollout" in task for task in record.rollout_tasks)
    assert any("Cleanup validation" in criterion for criterion in record.validation_criteria)
    assert result.summary["signal_counts"]["cleanup"] == 1


def test_unrelated_briefs_return_no_rollout_tasks_and_stable_summary():
    result = build_task_feature_flag_rollout_readiness_plan(
        _plan(
            [
                _task(
                    "task-api",
                    title="Add profile endpoint",
                    description="Create backend route for profile reads.",
                    files_or_modules=["src/api/profile.py"],
                    acceptance_criteria=["Endpoint returns active profile fields."],
                )
            ],
            plan_id="plan-no-rollout",
        )
    )
    malformed = build_task_feature_flag_rollout_readiness_plan(42)

    assert result.records == ()
    assert result.rollout_task_ids == ()
    assert result.flagged_task_ids == ()
    assert result.no_signal_task_ids == ("task-api",)
    assert result.summary == {
        "task_count": 1,
        "rollout_task_count": 0,
        "rollout_task_ids": [],
        "flagged_task_ids": [],
        "no_signal_task_count": 1,
        "no_signal_task_ids": ["task-api"],
        "generated_rollout_task_count": 0,
        "validation_criteria_count": 0,
        "readiness_counts": {"needs_rollout_tasks": 0, "partial": 0, "ready": 0},
        "signal_counts": {
            "feature_flag": 0,
            "default_state": 0,
            "targeting": 0,
            "beta_cohort": 0,
            "percentage_rollout": 0,
            "monitoring_gate": 0,
            "rollback": 0,
            "kill_switch": 0,
            "cleanup": 0,
            "qa_coverage": 0,
        },
        "status": "no_rollout_control_signals",
    }
    assert result.to_dicts() == []
    assert "No feature flag rollout readiness tasks were inferred." in result.to_markdown()
    assert malformed.records == ()
    assert malformed.summary["task_count"] == 0


def test_serialization_markdown_aliases_models_objects_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Introduce account overview feature flag",
                description="Feature flag defaults off for internal cohort first.",
            ),
            _task(
                "task-a",
                title="Prepare billing rollout | staged",
                description="Feature toggle ramps to 10% after monitoring gates pass.",
                acceptance_criteria=["Rollback turns the flag off; QA covers both states."],
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Launch docs beta toggle",
            description="Private beta cohort uses an allowlist.",
        )
    )
    object_task = SimpleNamespace(
        id="task-object",
        title="Add reports rollout flag",
        description="Feature flag defaults disabled and targets enterprise segment.",
        acceptance_criteria=["Disabled state and enabled state validation pass."],
        files_or_modules=["src/flags/reports.py"],
        status="pending",
    )

    result = summarize_task_feature_flag_rollout_readiness(plan)
    generated = generate_task_feature_flag_rollout_readiness(model)
    extracted = extract_task_feature_flag_rollout_readiness([object_task, task_model])
    payload = task_feature_flag_rollout_readiness_plan_to_dict(result)
    markdown = task_feature_flag_rollout_readiness_plan_to_markdown(result)

    assert plan == original
    assert generated.to_dict() == result.to_dict()
    assert summarize_task_feature_flag_rollout_readiness(result) is result
    assert json.loads(json.dumps(payload)) == payload
    assert task_feature_flag_rollout_readiness_plan_to_dicts(result) == payload["records"]
    assert task_feature_flag_rollout_readiness_plan_to_dicts(result.records) == payload["records"]
    assert list(payload) == [
        "plan_id",
        "records",
        "rollout_task_ids",
        "flagged_task_ids",
        "no_signal_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "readiness_level",
        "rollout_tasks",
        "validation_criteria",
        "evidence",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Feature Flag Rollout Readiness: plan-rollout-readiness")
    assert "Prepare billing rollout \\| staged" in markdown
    assert extracted.rollout_task_ids == ("task-model", "task-object")


def _plan(tasks, *, plan_id="plan-rollout-readiness"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-rollout-readiness",
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
    risks=None,
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
    if risks is not None:
        task["risks"] = risks
    if metadata is not None:
        task["metadata"] = metadata
    return task
