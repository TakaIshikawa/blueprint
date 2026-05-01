import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_canary_analysis_matrix import (
    PlanCanaryAnalysisMatrix,
    PlanCanaryAnalysisMatrixRow,
    build_plan_canary_analysis_matrix,
    generate_plan_canary_analysis_matrix,
    plan_canary_analysis_matrix_to_dict,
    plan_canary_analysis_matrix_to_dicts,
    plan_canary_analysis_matrix_to_markdown,
    summarize_plan_canary_analysis,
)


def test_detects_canary_progressive_exposure_and_related_surfaces():
    result = build_plan_canary_analysis_matrix(
        _plan(
            [
                _task("task-canary", title="Run checkout canary"),
                _task("task-progressive", title="Progressive exposure for invoices"),
                _task("task-percent", title="Rollout to 10% of users"),
                _task("task-beta", title="Open beta cohort for search"),
                _task("task-dark", title="Dark launch recommendation service"),
                _task("task-shadow", title="Replay shadow traffic for ranking"),
                _task("task-ramp", title="Prepare experiment ramp schedule"),
                _task("task-docs", title="Update docs only"),
            ]
        )
    )

    by_id = {row.task_id: row for row in result.rows}
    assert set(by_id) == {
        "task-canary",
        "task-progressive",
        "task-percent",
        "task-beta",
        "task-dark",
        "task-shadow",
        "task-ramp",
    }
    assert by_id["task-canary"].rollout_surface == "canary"
    assert by_id["task-progressive"].rollout_surface == "progressive exposure"
    assert by_id["task-percent"].rollout_surface == "percentage rollout"
    assert by_id["task-beta"].rollout_surface == "beta cohort"
    assert by_id["task-dark"].rollout_surface == "dark launch"
    assert by_id["task-shadow"].rollout_surface == "shadow traffic"
    assert by_id["task-ramp"].rollout_surface == "experiment ramp"
    assert result.summary["total_task_count"] == 8
    assert result.summary["canary_task_count"] == 7
    assert result.summary["unrelated_task_count"] == 1


def test_complete_metadata_builds_low_risk_row():
    result = build_plan_canary_analysis_matrix(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Canary checkout pricing",
                    metadata={
                        "canary": {
                            "baseline_metric": "checkout completion baseline",
                            "success_metric": "paid conversion",
                            "guardrail_metric": "payment error rate",
                            "sample_window": "72 hours",
                            "owner": "Growth Analytics",
                            "rollback_threshold": "payment errors exceed 1.5%",
                            "customer_impact_check": "support and CX review complete",
                        }
                    },
                )
            ]
        )
    )

    row = result.rows[0]
    assert isinstance(row, PlanCanaryAnalysisMatrixRow)
    assert row.task_id == "task-complete"
    assert row.title == "Canary checkout pricing"
    assert row.rollout_surface == "canary"
    assert row.required_metrics == (
        "baseline_metric: checkout completion baseline",
        "success_metric: paid conversion",
        "guardrail_metric: payment error rate",
    )
    assert row.missing_analysis_inputs == ()
    assert row.rollback_thresholds == ("payment errors exceed 1.5%",)
    assert row.risk_level == "low"
    assert "metadata.canary.rollback_threshold: payment errors exceed 1.5%" in row.evidence
    assert "metadata.canary.customer_impact_check: support and CX review complete" in row.evidence
    assert result.summary["risk_counts"] == {"low": 1, "medium": 0, "high": 0}
    assert result.summary["at_risk_task_count"] == 0


def test_missing_inputs_and_rollback_gap_escalate_risk_and_summary_counts():
    result = build_plan_canary_analysis_matrix(
        _plan(
            [
                _task(
                    "task-high",
                    title="Canary profile rollout",
                    description="Canary the new profile API. Success metric: activation.",
                ),
                _task(
                    "task-medium",
                    title="Progressive exposure for dashboard",
                    description=(
                        "Progressive exposure. Baseline metric: dashboard load success. "
                        "Success metric: adoption. Guardrail: p95 latency. "
                        "Sample window: 7 days. Owner: Data PM. "
                        "Rollback if p95 latency exceeds 800ms."
                    ),
                ),
            ]
        )
    )

    by_id = {row.task_id: row for row in result.rows}
    assert result.rows[0].task_id == "task-high"
    assert by_id["task-high"].risk_level == "high"
    assert "rollback_threshold" in by_id["task-high"].missing_analysis_inputs
    assert by_id["task-high"].rollback_thresholds == ()
    assert by_id["task-medium"].risk_level == "medium"
    assert by_id["task-medium"].missing_analysis_inputs == ("customer_impact_check",)
    assert by_id["task-medium"].rollback_thresholds == ("p95 latency exceeds 800ms",)
    assert result.summary["at_risk_task_count"] == 2
    assert result.summary["rollback_threshold_gap_count"] == 1
    assert result.summary["missing_analysis_input_counts"]["customer_impact_check"] == 2
    assert result.summary["missing_analysis_input_counts"]["rollback_threshold"] == 1


def test_markdown_rendering_includes_summary_and_stable_rows():
    result = build_plan_canary_analysis_matrix(
        _plan(
            [
                _task(
                    "task-md",
                    title="Rollout to 25% of accounts",
                    description=(
                        "Baseline metric: trial activation. Success metric: upgrade rate. "
                        "Guardrail metric: support tickets. Sample window: 48 hours. "
                        "Owner: Monetization. Rollback threshold: tickets exceed baseline by 10%. "
                        "Customer impact check: no enterprise accounts included."
                    ),
                )
            ]
        )
    )

    markdown = plan_canary_analysis_matrix_to_markdown(result)

    assert markdown.startswith("# Plan Canary Analysis Matrix: plan-canary")
    assert "- Canary tasks: 1" in markdown
    assert "- Rollback threshold gaps: 0" in markdown
    assert (
        "| task-md | Rollout to 25% of accounts | percentage rollout | "
        "baseline_metric: trial activation, success_metric: upgrade rate, "
        "guardrail_metric: support tickets | none | tickets exceed baseline by 10% | low |"
    ) in markdown


def test_empty_and_non_canary_sources_return_deterministic_noop():
    result = build_plan_canary_analysis_matrix(
        _plan(
            [
                _task(
                    "task-api",
                    title="Add profile endpoint",
                    description="Create backend route for profile reads.",
                )
            ]
        )
    )

    assert isinstance(result, PlanCanaryAnalysisMatrix)
    assert result.plan_id == "plan-canary"
    assert result.rows == ()
    assert result.to_dict() == {
        "plan_id": "plan-canary",
        "summary": {
            "total_task_count": 1,
            "canary_task_count": 0,
            "unrelated_task_count": 1,
            "at_risk_task_count": 0,
            "missing_input_count": 0,
            "rollback_threshold_gap_count": 0,
            "risk_counts": {"low": 0, "medium": 0, "high": 0},
            "missing_analysis_input_counts": {
                "baseline_metric": 0,
                "success_metric": 0,
                "guardrail_metric": 0,
                "sample_window": 0,
                "owner": 0,
                "rollback_threshold": 0,
                "customer_impact_check": 0,
            },
        },
        "rows": [],
    }
    assert result.to_markdown() == (
        "# Plan Canary Analysis Matrix: plan-canary\n\n"
        "## Summary\n\n"
        "- Total tasks: 1\n"
        "- Canary tasks: 0\n"
        "- At-risk tasks: 0\n"
        "- Missing analysis inputs: 0\n"
        "- Rollback threshold gaps: 0\n\n"
        "No canary or progressive-exposure tasks were inferred."
    )
    assert generate_plan_canary_analysis_matrix({"tasks": "not a list"}) == ()
    assert build_plan_canary_analysis_matrix(None).summary["total_task_count"] == 0


def test_model_input_serializes_stably_without_mutation_and_aliases_match():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Dark launch search suggestions",
                description=(
                    "Dark launch for search suggestions. Baseline: search engagement. "
                    "Primary metric: suggestion click-through. Guardrails: p99 latency. "
                    "Sample period: 1 week. Analysis owner: Search Data. "
                    "Revert if p99 latency exceeds 500ms. Customer impact review: internal only."
                ),
            )
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = build_plan_canary_analysis_matrix(model)
    alias_result = summarize_plan_canary_analysis(plan)
    rows = generate_plan_canary_analysis_matrix(model)
    payload = plan_canary_analysis_matrix_to_dict(result)

    assert plan == original
    assert rows == result.rows
    assert alias_result.to_dict() == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert plan_canary_analysis_matrix_to_dicts(result) == payload["rows"]
    assert plan_canary_analysis_matrix_to_dicts(rows) == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "summary", "rows"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "rollout_surface",
        "required_metrics",
        "missing_analysis_inputs",
        "rollback_thresholds",
        "risk_level",
        "evidence",
    ]
    assert payload["rows"][0]["risk_level"] == "low"
    assert payload["rows"][0]["rollout_surface"] == "dark launch"


def _plan(tasks):
    return {
        "id": "plan-canary",
        "implementation_brief_id": "brief-canary",
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
