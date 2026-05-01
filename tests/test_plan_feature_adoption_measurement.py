import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_feature_adoption_measurement import (
    PlanFeatureAdoptionMeasurementMatrix,
    PlanFeatureAdoptionMeasurementRow,
    build_plan_feature_adoption_measurement_matrix,
    plan_feature_adoption_measurement_matrix_to_dict,
    plan_feature_adoption_measurement_matrix_to_markdown,
    summarize_plan_feature_adoption_measurement,
)


def test_adoption_activation_engagement_retention_funnel_and_analytics_generate_rows():
    result = build_plan_feature_adoption_measurement_matrix(
        _plan(
            [
                _task(
                    "task-adoption-b",
                    title="Measure feature adoption",
                    description="Track feature usage rate and rollout uptake after launch.",
                    files_or_modules=["analytics/adoption/feature_usage.yml"],
                ),
                _task(
                    "task-adoption-a",
                    title="Adoption dashboard panels",
                    description="Add adoption rate dashboard and current baseline comparison.",
                    files_or_modules=["analytics/dashboards/adoption.yml"],
                ),
                _task(
                    "task-activation",
                    title="Activation analytics",
                    description="Measure activation when onboarding completion reaches first value.",
                ),
                _task(
                    "task-engagement",
                    title="Engagement telemetry",
                    description="Emit engagement session events and usage frequency metrics.",
                ),
                _task(
                    "task-retention",
                    title="Retention cohort report",
                    description="Create cohort retention and churn dashboard by plan tier.",
                ),
                _task(
                    "task-funnel",
                    title="Checkout funnel conversion",
                    description="Instrument funnel drop-off and step conversion reporting.",
                ),
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Clarify labels.",
                ),
            ]
        )
    )

    assert isinstance(result, PlanFeatureAdoptionMeasurementMatrix)
    assert result.plan_id == "plan-adoption"
    assert [row.measurement_objective for row in result.rows] == [
        "adoption",
        "activation",
        "engagement",
        "retention",
        "funnel",
        "analytics",
    ]
    by_objective = {row.measurement_objective: row for row in result.rows}
    assert by_objective["adoption"].covered_task_ids == ("task-adoption-a", "task-adoption-b")
    assert by_objective["analytics"].covered_task_ids == (
        "task-activation",
        "task-adoption-a",
        "task-adoption-b",
        "task-engagement",
        "task-funnel",
        "task-retention",
    )
    assert by_objective["funnel"].success_metrics == ("Funnel conversion rate",)
    assert by_objective["retention"].dimensions == ("cohort_week", "account_id", "plan_tier")
    assert result.summary["covered_task_count"] == 6
    assert result.summary["objective_counts"] == {
        "adoption": 1,
        "activation": 1,
        "engagement": 1,
        "retention": 1,
        "funnel": 1,
        "analytics": 1,
    }


def test_explicit_metadata_for_metrics_events_dimensions_dashboards_and_owners_is_preserved():
    plan = _plan(
        [
            _task(
                "task-explicit",
                title="Launch activation tracking",
                description="Measure activation for the new onboarding flow.",
                metadata={
                    "measurement_objectives": ["activation", "funnel"],
                    "success_metrics": ["Activation rate | qualified accounts", "Trial-to-paid conversion"],
                    "event_names": ["onboarding_started", "onboarding_completed"],
                    "dimensions": ["plan_tier", "workspace_id"],
                    "baseline_requirement": "Use the prior 28 days of onboarding completion.",
                    "dashboard": "Growth dashboard | activation tab",
                    "owner": "growth analytics DRI",
                    "risks": ["Low traffic may delay cohort readout."],
                },
            )
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = summarize_plan_feature_adoption_measurement(model)
    payload = plan_feature_adoption_measurement_matrix_to_dict(result)
    activation = _row(result, "activation")

    assert plan == original
    assert isinstance(activation, PlanFeatureAdoptionMeasurementRow)
    assert activation.success_metrics == ("Activation rate | qualified accounts", "Trial-to-paid conversion")
    assert activation.required_events == ("onboarding_started", "onboarding_completed")
    assert activation.dimensions == ("plan_tier", "workspace_id")
    assert activation.baseline_requirement == "Use the prior 28 days of onboarding completion."
    assert activation.reporting_surface == "Growth dashboard | activation tab"
    assert activation.owner_hint == "growth analytics DRI"
    assert activation.risks == ("Low traffic may delay cohort readout.",)
    assert activation.follow_up_questions == ()
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "rows", "summary"]
    assert list(payload["rows"][0]) == [
        "measurement_objective",
        "covered_task_ids",
        "success_metrics",
        "required_events",
        "dimensions",
        "baseline_requirement",
        "reporting_surface",
        "owner_hint",
        "risks",
        "follow_up_questions",
        "evidence",
    ]


def test_text_only_inference_creates_follow_up_questions_for_missing_measurement_details():
    result = build_plan_feature_adoption_measurement_matrix(
        _plan(
            [
                _task(
                    "task-usage",
                    title="Adoption readout",
                    description="Measure feature adoption and engagement for the new workspace overview.",
                )
            ]
        )
    )

    adoption = _row(result, "adoption")
    engagement = _row(result, "engagement")

    assert adoption.required_events == ("feature_viewed", "feature_used")
    assert adoption.baseline_requirement == "Capture a pre-launch baseline or comparison window for adoption."
    assert adoption.reporting_surface == "Product analytics dashboard for adoption."
    assert adoption.follow_up_questions == (
        "Which canonical event names prove adoption measurement?",
        "What pre-launch baseline or comparison window applies to adoption?",
        "Which dashboard or reporting surface will track adoption?",
    )
    assert engagement.follow_up_questions == (
        "Which canonical event names prove engagement measurement?",
        "What pre-launch baseline or comparison window applies to engagement?",
        "Which dashboard or reporting surface will track engagement?",
    )
    assert any("ambiguous" in risk for risk in adoption.risks)
    assert result.summary["follow_up_question_count"] == 6


def test_empty_plan_serialization_and_markdown_are_deterministic():
    result = build_plan_feature_adoption_measurement_matrix(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Adjust labels and helper text.",
                    files_or_modules=["docs/settings-copy.md"],
                )
            ]
        )
    )

    assert result.rows == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "measurement_objective_count": 0,
        "covered_task_count": 0,
        "follow_up_question_count": 0,
        "risk_count": 0,
        "objective_counts": {
            "adoption": 0,
            "activation": 0,
            "engagement": 0,
            "retention": 0,
            "funnel": 0,
            "analytics": 0,
        },
    }
    assert result.to_markdown() == (
        "# Plan Feature Adoption Measurement Matrix: plan-adoption\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Measurement objective count: 0\n"
        "- Covered task count: 0\n"
        "- Follow-up question count: 0\n"
        "- Objective counts: adoption 0, activation 0, engagement 0, retention 0, funnel 0, analytics 0\n"
        "\n"
        "No feature adoption measurement signals were detected."
    )


def test_markdown_renderer_escapes_pipes_stably():
    result = build_plan_feature_adoption_measurement_matrix(
        _plan(
            [
                _task(
                    "task-pipe",
                    title="Activation | funnel",
                    description="Measure activation dashboard baseline and event names.",
                    metadata={
                        "event_names": ["activation_started | mobile"],
                        "dashboard": "Growth | activation",
                    },
                )
            ]
        )
    )

    markdown = plan_feature_adoption_measurement_matrix_to_markdown(result)

    assert markdown == result.to_markdown()
    assert "activation_started \\| mobile" in markdown
    assert "Growth \\| activation" in markdown


def _row(result, objective):
    return next(row for row in result.rows if row.measurement_objective == objective)


def _plan(tasks, *, metadata=None):
    return {
        "id": "plan-adoption",
        "implementation_brief_id": "brief-adoption",
        "milestones": [],
        "tasks": tasks,
        "metadata": metadata or {},
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
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
