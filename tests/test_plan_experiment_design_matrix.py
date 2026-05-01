import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_experiment_design_matrix import (
    PlanExperimentDesignMatrixRow,
    build_plan_experiment_design_matrix,
    plan_experiment_design_matrix_to_dict,
    plan_experiment_design_matrix_to_markdown,
)


def test_explicit_metadata_builds_complete_experiment_row():
    result = build_plan_experiment_design_matrix(
        _plan(
            [
                _task(
                    "task-exp",
                    title="Launch checkout experiment",
                    description="Gate checkout redesign behind a feature flag.",
                    metadata={
                        "experiment": {
                            "objective": "Checkout redesign",
                            "hypothesis": (
                                "shorter checkout increases paid conversion for new merchants"
                            ),
                            "cohort": "new merchants in US self-serve signup",
                            "success_metrics": ["paid conversion rate", "checkout completion"],
                            "guardrail_metrics": ["payment error rate", "support tickets"],
                            "exposure_strategy": "10% feature-flag ramp with 50/50 holdout",
                            "analysis_risks": ["holiday seasonality may bias conversion"],
                        }
                    },
                )
            ]
        )
    )

    assert result.rows == (
        PlanExperimentDesignMatrixRow(
            objective="Checkout redesign",
            covered_task_ids=("task-exp",),
            hypothesis="shorter checkout increases paid conversion for new merchants",
            cohort="new merchants in US self-serve signup",
            success_metrics=("paid conversion rate", "checkout completion"),
            guardrail_metrics=("payment error rate", "support tickets"),
            exposure_strategy="10% feature-flag ramp with 50/50 holdout",
            risks=("holiday seasonality may bias conversion",),
        ),
    )
    assert result.summary == {
        "experiment_count": 1,
        "covered_task_count": 1,
        "missing_hypothesis_count": 0,
        "missing_metric_count": 0,
    }


def test_text_only_inference_adds_placeholders_and_follow_ups():
    result = build_plan_experiment_design_matrix(
        _plan(
            [
                _task(
                    "task-text",
                    title="Run A/B test on onboarding checklist",
                    description=(
                        "Experiment for beta admins with gradual exposure. "
                        "Track adoption and retention. Guardrail support ticket rate."
                    ),
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.objective == "Onboarding checklist"
    assert row.covered_task_ids == ("task-text",)
    assert row.hypothesis == (
        "Changing Onboarding checklist for beta admins will improve adoption."
    )
    assert row.cohort == "beta admins"
    assert row.success_metrics == ("adoption", "retention")
    assert row.guardrail_metrics == ("support ticket",)
    assert row.exposure_strategy == "gradual exposure"
    assert row.follow_up_questions == ()


def test_deduplicates_related_tasks_by_objective_and_task_ids():
    result = build_plan_experiment_design_matrix(
        _plan(
            [
                _task(
                    "task-flag",
                    title="Add search ranking feature flag",
                    metadata={
                        "feature_flag": "search-ranking",
                        "success_metrics": ["conversion", "conversion"],
                    },
                ),
                _task(
                    "task-flag",
                    title="Monitor search ranking experiment",
                    description="Guardrail latency and error rate during canary rollout.",
                    metadata={"experiment_name": "search ranking"},
                ),
                _task(
                    "task-control",
                    title="Document search ranking holdout",
                    description="Use a holdout cohort and analyze sample size risk.",
                    metadata={"experiment_name": "search ranking"},
                ),
            ]
        )
    )

    assert len(result.rows) == 1
    row = result.rows[0]
    assert row.objective == "Search ranking"
    assert row.covered_task_ids == ("task-flag", "task-control")
    assert row.success_metrics == ("conversion",)
    assert row.guardrail_metrics == ("latency", "error rate")
    assert row.risks == ("sample size or power may be insufficient",)
    assert row.follow_up_questions == (
        "What is the falsifiable hypothesis for this experiment?",
        "Which cohort or segment should be exposed?",
    )


def test_no_experiment_plan_returns_empty_deterministic_matrix():
    result = build_plan_experiment_design_matrix(
        _plan(
            [
                _task(
                    "task-api",
                    title="Add profile endpoint",
                    description="Create backend route for profile reads.",
                    acceptance_criteria=["Endpoint returns active profile fields."],
                )
            ]
        )
    )

    assert result.plan_id == "plan-experiments"
    assert result.rows == ()
    assert result.to_dict() == {
        "plan_id": "plan-experiments",
        "summary": {
            "experiment_count": 0,
            "covered_task_count": 0,
            "missing_hypothesis_count": 0,
            "missing_metric_count": 0,
        },
        "rows": [],
    }
    assert result.to_markdown() == (
        "# Plan Experiment Design Matrix: plan-experiments\n\n"
        "No experiment design rows were inferred."
    )


def test_model_serialization_and_markdown_are_stable():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Enable pricing page A/B test",
                description=(
                    "Hypothesis: pricing proof points improve conversion. "
                    "Cohort: trial accounts. Guardrail refund rate. Rollout 25% of users."
                ),
                metadata={"success_metrics": ["trial conversion"]},
            )
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = build_plan_experiment_design_matrix(model)
    payload = plan_experiment_design_matrix_to_dict(result)
    markdown = plan_experiment_design_matrix_to_markdown(result)

    assert plan == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert list(payload) == ["plan_id", "summary", "rows"]
    assert list(payload["rows"][0]) == [
        "objective",
        "covered_task_ids",
        "hypothesis",
        "cohort",
        "success_metrics",
        "guardrail_metrics",
        "exposure_strategy",
        "risks",
        "follow_up_questions",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert markdown.startswith("# Plan Experiment Design Matrix: plan-experiments")
    assert (
        "| Pricing page A/B test | task-model | pricing proof points improve conversion | "
        "trial accounts | trial conversion, conversion | refund"
    ) in markdown


def _plan(tasks):
    return {
        "id": "plan-experiments",
        "implementation_brief_id": "brief-experiments",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    metadata=None,
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
    return task
