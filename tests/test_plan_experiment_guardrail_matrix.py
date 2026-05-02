import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_experiment_guardrail_matrix import (
    PlanExperimentGuardrailMatrix,
    PlanExperimentGuardrailMatrixRow,
    build_plan_experiment_guardrail_matrix,
    derive_plan_experiment_guardrail_matrix,
    generate_plan_experiment_guardrail_matrix,
    plan_experiment_guardrail_matrix_to_dict,
    plan_experiment_guardrail_matrix_to_dicts,
    plan_experiment_guardrail_matrix_to_markdown,
    summarize_plan_experiment_guardrail_matrix,
)


def test_detects_experiment_tasks_and_extracts_guardrail_signals():
    result = build_plan_experiment_guardrail_matrix(
        _plan(
            [
                _task(
                    "task-ab",
                    title="Run checkout A/B test",
                    description=(
                        "A/B test checkout copy. Primary metric: paid conversion. "
                        "Guardrail metrics: payment error rate and support tickets. "
                        "Cohort: 20% of eligible US users. Stop if payment errors exceed 1.5%. "
                        "Owner: Growth Analytics."
                    ),
                ),
                _task(
                    "task-rollout",
                    title="Rollout profile feature to beta cohort",
                    description="Rollout the profile feature to beta users.",
                    metadata={"experiment": {"owner": "Profiles PM"}},
                ),
                _task(
                    "task-docs",
                    title="Update API docs",
                    description="Refresh endpoint examples.",
                ),
            ]
        )
    )

    by_id = {row.task_id: row for row in result.rows}
    assert set(by_id) == {"task-ab", "task-rollout"}
    assert isinstance(by_id["task-ab"], PlanExperimentGuardrailMatrixRow)
    assert by_id["task-ab"].experiment_surface == "a/b test"
    assert by_id["task-ab"].primary_metric == "paid conversion"
    assert by_id["task-ab"].guardrail_metrics == ("payment error rate and support tickets",)
    assert by_id["task-ab"].sample_or_cohort_signal == "20% of eligible US users"
    assert by_id["task-ab"].stop_condition == "payment errors exceed 1.5%"
    assert by_id["task-ab"].owner == "Growth Analytics"
    assert by_id["task-ab"].missing_fields == ()
    assert by_id["task-rollout"].experiment_surface == "rollout"
    assert by_id["task-rollout"].owner == "Profiles PM"
    assert by_id["task-rollout"].sample_or_cohort_signal == "beta cohort"
    assert by_id["task-rollout"].missing_fields == (
        "primary_metric",
        "guardrail_metrics",
        "stop_condition",
    )
    assert result.summary["total_task_count"] == 3
    assert result.summary["experiment_task_count"] == 2
    assert result.summary["unrelated_task_count"] == 1


def test_complete_metadata_builds_ready_row_and_stable_evidence():
    result = build_plan_experiment_guardrail_matrix(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Launch recommendation experiment",
                    metadata={
                        "experiment": {
                            "primary_metric": "recommendation click-through",
                            "guardrail_metrics": ["p95 latency", "complaint rate"],
                            "cohort": "holdout plus treatment cohorts",
                            "stop_condition": "pause if latency exceeds 400ms",
                            "owner": "Search Data",
                        }
                    },
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.experiment_surface == "experiment"
    assert row.primary_metric == "recommendation click-through"
    assert row.guardrail_metrics == ("p95 latency", "complaint rate")
    assert row.sample_or_cohort_signal == "holdout plus treatment cohorts"
    assert row.stop_condition == "pause if latency exceeds 400ms"
    assert row.owner == "Search Data"
    assert row.recommendation.startswith("Ready:")
    assert "metadata.experiment.guardrail_metrics: complaint rate" in row.evidence
    assert result.summary["missing_field_count"] == 0
    assert result.summary["tasks_missing_guardrails"] == 0


def test_missing_guardrail_stop_condition_and_owner_counts_are_actionable():
    result = build_plan_experiment_guardrail_matrix(
        _plan(
            [
                _task(
                    "task-gaps",
                    title="Feature test onboarding conversion",
                    description=(
                        "Feature test onboarding. Primary metric: activation. "
                        "Sample size: 5,000 new accounts."
                    ),
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.experiment_surface == "feature test"
    assert row.missing_fields == ("guardrail_metrics", "stop_condition", "owner")
    assert "add guardrail metrics" in row.recommendation
    assert "set an explicit stop" in row.recommendation
    assert "assign a directly responsible owner" in row.recommendation
    assert result.summary["tasks_missing_guardrails"] == 1
    assert result.summary["tasks_missing_stop_conditions"] == 1
    assert result.summary["tasks_missing_owners"] == 1
    assert result.summary["missing_field_counts"]["primary_metric"] == 0
    assert result.summary["missing_field_counts"]["guardrail_metrics"] == 1


def test_markdown_escapes_table_cells_and_serializes_deterministically():
    result = build_plan_experiment_guardrail_matrix(
        _plan(
            [
                _task(
                    "task-md",
                    title="Checkout | pricing experiment",
                    description=(
                        "Split test pricing. Primary metric: upgrade | paid conversion. "
                        "Guardrail metrics: refund rate | support tickets. "
                        "Cohort: 10% of users. Stop if refunds exceed baseline by 5%. "
                        "Owner: Monetization | Analytics."
                    ),
                )
            ]
        )
    )

    markdown = plan_experiment_guardrail_matrix_to_markdown(result)
    payload = plan_experiment_guardrail_matrix_to_dict(result)

    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "summary", "rows"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "experiment_surface",
        "primary_metric",
        "guardrail_metrics",
        "sample_or_cohort_signal",
        "stop_condition",
        "owner",
        "missing_fields",
        "recommendation",
        "evidence",
    ]
    assert "Checkout \\| pricing experiment" in markdown
    assert "upgrade \\| paid conversion" in markdown
    assert "refund rate \\| support tickets" in markdown
    assert markdown == result.to_markdown()


def test_non_experiment_plan_returns_stable_empty_matrix_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-api",
                title="Add profile endpoint",
                description="Create backend route for profile reads.",
            )
        ],
        plan_id="plan-non-experiment",
    )
    original = copy.deepcopy(plan)

    result = build_plan_experiment_guardrail_matrix(plan)

    assert plan == original
    assert isinstance(result, PlanExperimentGuardrailMatrix)
    assert result.plan_id == "plan-non-experiment"
    assert result.rows == ()
    assert result.records == ()
    assert result.to_dict() == {
        "plan_id": "plan-non-experiment",
        "summary": {
            "total_task_count": 1,
            "experiment_task_count": 0,
            "unrelated_task_count": 1,
            "tasks_missing_guardrails": 0,
            "tasks_missing_stop_conditions": 0,
            "tasks_missing_owners": 0,
            "missing_field_count": 0,
            "missing_field_counts": {
                "primary_metric": 0,
                "guardrail_metrics": 0,
                "sample_or_cohort_signal": 0,
                "stop_condition": 0,
                "owner": 0,
            },
        },
        "rows": [],
    }
    assert result.to_markdown() == (
        "# Plan Experiment Guardrail Matrix: plan-non-experiment\n\n"
        "## Summary\n\n"
        "- Total tasks: 1\n"
        "- Experiment tasks: 0\n"
        "- Tasks missing guardrails: 0\n"
        "- Tasks missing stop conditions: 0\n"
        "- Tasks missing owners: 0\n\n"
        "No experiment guardrail rows were inferred."
    )
    assert generate_plan_experiment_guardrail_matrix({"tasks": "not a list"}) == ()
    assert build_plan_experiment_guardrail_matrix(None).summary["total_task_count"] == 0


def test_model_input_aliases_and_iterable_rows_match():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Signup conversion experiment",
                description=(
                    "Run signup experiment. Success metric: completed registration. "
                    "Guardrails: p99 latency. Audience: new visitor cohort. "
                    "Rollback if latency exceeds 700ms. Analysis owner: Growth Data."
                ),
            )
        ],
        plan_id="plan-model",
    )
    model = ExecutionPlan.model_validate(plan)

    result = build_plan_experiment_guardrail_matrix(model)
    derived = derive_plan_experiment_guardrail_matrix(result)
    summarized = summarize_plan_experiment_guardrail_matrix(plan)
    rows = generate_plan_experiment_guardrail_matrix(model)
    payload = plan_experiment_guardrail_matrix_to_dict(result)

    assert derived is result
    assert rows == result.rows
    assert summarized.to_dict() == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert plan_experiment_guardrail_matrix_to_dicts(result) == payload["rows"]
    assert plan_experiment_guardrail_matrix_to_dicts(rows) == payload["rows"]
    assert payload["rows"][0]["experiment_surface"] == "experiment"
    assert payload["rows"][0]["primary_metric"] == "completed registration"


def _plan(tasks, *, plan_id="plan-experiment"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-experiment",
        "milestones": [{"name": "Launch"}],
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
