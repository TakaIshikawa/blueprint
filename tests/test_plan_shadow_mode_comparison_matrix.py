import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_shadow_mode_comparison_matrix import (
    PlanShadowModeComparisonMatrix,
    PlanShadowModeComparisonMatrixRow,
    build_plan_shadow_mode_comparison_matrix,
    derive_plan_shadow_mode_comparison_matrix,
    generate_plan_shadow_mode_comparison_matrix,
    plan_shadow_mode_comparison_matrix_to_dict,
    plan_shadow_mode_comparison_matrix_to_dicts,
    plan_shadow_mode_comparison_matrix_to_markdown,
    summarize_plan_shadow_mode_comparison_matrix,
)


def test_detects_shadow_parallel_dual_and_mirrored_comparison_tasks():
    result = build_plan_shadow_mode_comparison_matrix(
        _plan(
            [
                _task(
                    "task-shadow",
                    title="Run search ranking shadow mode",
                    description=(
                        "Shadow mode compare old ranker against new ranker for 48 hours. "
                        "Baseline system: legacy search ranker. Candidate system: neural ranker. "
                        "Traffic sample: 25% of search traffic. Mismatch tolerance: top-3 "
                        "result delta under 0.5%. Escalation owner: Search Relevance."
                    ),
                ),
                _task(
                    "task-parallel",
                    title="Parallel-run billing calculator",
                    description="Run billing calculator in parallel with legacy invoices.",
                ),
                _task(
                    "task-dual-write",
                    title="Dual-write comparison for account events",
                    description=(
                        "Dual-write comparison between current event store and candidate stream."
                    ),
                ),
                _task(
                    "task-dark",
                    title="Dark reads for customer profile service",
                    description="Dark reads compare old-vs-new profile result comparison.",
                ),
                _task(
                    "task-mirror",
                    title="Mirrored traffic for recommendation service",
                    description="Mirror traffic to the candidate recommendation API.",
                ),
                _task(
                    "task-docs",
                    title="Refresh docs",
                    description="Update endpoint examples.",
                ),
            ]
        )
    )

    by_id = {row.task_id: row for row in result.rows}
    assert set(by_id) == {
        "task-shadow",
        "task-parallel",
        "task-dual-write",
        "task-dark",
        "task-mirror",
    }
    assert isinstance(by_id["task-shadow"], PlanShadowModeComparisonMatrixRow)
    assert by_id["task-shadow"].comparison_surface == "shadow mode"
    assert by_id["task-shadow"].baseline_system == "legacy search ranker"
    assert by_id["task-shadow"].candidate_system == "neural ranker"
    assert by_id["task-shadow"].traffic_sample_signal == "25% of search traffic"
    assert by_id["task-shadow"].mismatch_tolerance == "top-3 result delta under 0.5%"
    assert by_id["task-shadow"].escalation_owner == "Search Relevance"
    assert by_id["task-shadow"].missing_fields == ()
    assert by_id["task-parallel"].comparison_surface == "parallel run"
    assert by_id["task-dual-write"].comparison_surface == "dual-write comparison"
    assert by_id["task-dark"].comparison_surface == "dark reads"
    assert by_id["task-mirror"].comparison_surface == "mirrored traffic"
    assert result.summary["total_task_count"] == 6
    assert result.summary["shadow_task_count"] == 5
    assert result.summary["unrelated_task_count"] == 1


def test_metadata_extracts_complete_comparison_fields():
    result = build_plan_shadow_mode_comparison_matrix(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Shadow compare permissions service",
                    metadata={
                        "shadow_mode": {
                            "baseline_system": "current ACL service",
                            "candidate_system": "policy engine v2",
                            "replay_window": "7 days of production authorization checks",
                            "mismatch_tolerance": "zero allow/deny mismatches",
                            "escalation_owner": "Identity Platform",
                        }
                    },
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.comparison_surface == "shadow mode"
    assert row.baseline_system == "current ACL service"
    assert row.candidate_system == "policy engine v2"
    assert row.traffic_sample_signal == "7 days of production authorization checks"
    assert row.mismatch_tolerance == "zero allow/deny mismatches"
    assert row.escalation_owner == "Identity Platform"
    assert row.recommendation.startswith("Ready:")
    assert "metadata.shadow_mode.mismatch_tolerance: zero allow/deny mismatches" in row.evidence
    assert result.summary["missing_field_count"] == 0
    assert result.summary["tasks_missing_tolerances"] == 0


def test_incomplete_comparison_criteria_list_missing_fields_and_actions():
    result = build_plan_shadow_mode_comparison_matrix(
        _plan(
            [
                _task(
                    "task-gaps",
                    title="Mirror traffic for checkout pricing",
                    description=(
                        "Mirrored traffic compares the legacy checkout price API to the new "
                        "pricing service over 24 hours."
                    ),
                )
            ]
        )
    )

    row = result.rows[0]
    assert row.missing_fields == (
        "baseline_system",
        "mismatch_tolerance",
        "escalation_owner",
    )
    assert row.candidate_system == "the new pricing service"
    assert row.traffic_sample_signal == "over 24 hours"
    assert "define mismatch tolerances" in row.recommendation
    assert "assign an escalation owner" in row.recommendation
    assert "confirm the replay window" in row.recommendation
    assert "rollback or disable actions" in row.recommendation
    assert result.summary["tasks_missing_tolerances"] == 1
    assert result.summary["tasks_missing_owners"] == 1
    assert result.summary["missing_field_counts"]["mismatch_tolerance"] == 1


def test_non_shadow_plan_returns_stable_empty_matrix_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-api",
                title="Add profile endpoint",
                description="Create backend route for profile reads.",
            )
        ],
        plan_id="plan-non-shadow",
    )
    original = copy.deepcopy(plan)

    result = build_plan_shadow_mode_comparison_matrix(plan)

    assert plan == original
    assert isinstance(result, PlanShadowModeComparisonMatrix)
    assert result.plan_id == "plan-non-shadow"
    assert result.rows == ()
    assert result.records == ()
    assert result.to_dict() == {
        "plan_id": "plan-non-shadow",
        "summary": {
            "total_task_count": 1,
            "shadow_task_count": 0,
            "unrelated_task_count": 1,
            "tasks_missing_tolerances": 0,
            "tasks_missing_owners": 0,
            "missing_field_count": 0,
            "missing_field_counts": {
                "baseline_system": 0,
                "candidate_system": 0,
                "traffic_sample_signal": 0,
                "mismatch_tolerance": 0,
                "escalation_owner": 0,
            },
            "comparison_surface_counts": {
                "shadow mode": 0,
                "parallel run": 0,
                "dual-run": 0,
                "dual-write comparison": 0,
                "dark reads": 0,
                "mirrored traffic": 0,
                "old-vs-new comparison": 0,
            },
        },
        "rows": [],
    }
    assert result.to_markdown() == (
        "# Plan Shadow Mode Comparison Matrix: plan-non-shadow\n\n"
        "## Summary\n\n"
        "- Total tasks: 1\n"
        "- Shadow comparison tasks: 0\n"
        "- Tasks missing tolerances: 0\n"
        "- Tasks missing owners: 0\n"
        "- Missing comparison fields: 0\n\n"
        "No shadow-mode comparison rows were inferred."
    )
    assert generate_plan_shadow_mode_comparison_matrix({"tasks": "not a list"}) == ()
    assert build_plan_shadow_mode_comparison_matrix(None).summary["total_task_count"] == 0


def test_markdown_escapes_table_cells_and_serializes_deterministically():
    result = build_plan_shadow_mode_comparison_matrix(
        _plan(
            [
                _task(
                    "task-md",
                    title="Shadow | compare checkout",
                    description=(
                        "Shadow mode. Baseline system: checkout | v1. "
                        "Candidate system: checkout | v2. Traffic sample: 10% of traffic. "
                        "Mismatch tolerance: totals differ by < 0.1% | no tax mismatches. "
                        "Escalation owner: Payments | Data."
                    ),
                )
            ]
        )
    )

    markdown = plan_shadow_mode_comparison_matrix_to_markdown(result)
    payload = plan_shadow_mode_comparison_matrix_to_dict(result)

    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "summary", "rows"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "comparison_surface",
        "baseline_system",
        "candidate_system",
        "traffic_sample_signal",
        "mismatch_tolerance",
        "escalation_owner",
        "missing_fields",
        "recommendation",
        "evidence",
    ]
    assert "Shadow \\| compare checkout" in markdown
    assert "checkout \\| v1" in markdown
    assert "checkout \\| v2" in markdown
    assert "Payments \\| Data" in markdown
    assert markdown == result.to_markdown()


def test_model_input_aliases_and_iterable_rows_match():
    plan = _plan(
        [
            _task(
                "task-model",
                title="Old-vs-new result comparison for fraud scoring",
                description=(
                    "Compare old scoring service against candidate model for 3 days. "
                    "Tolerance: score bucket mismatch under 0.2%. Owner: Risk Data."
                ),
                metadata={
                    "comparison": {
                        "baseline_system": "rules engine",
                        "candidate_system": "ml scorer",
                        "traffic_split": "5% of auth attempts",
                    }
                },
            )
        ],
        plan_id="plan-model",
    )
    model = ExecutionPlan.model_validate(plan)

    result = build_plan_shadow_mode_comparison_matrix(model)
    derived = derive_plan_shadow_mode_comparison_matrix(result)
    summarized = summarize_plan_shadow_mode_comparison_matrix(plan)
    rows = generate_plan_shadow_mode_comparison_matrix(model)
    payload = plan_shadow_mode_comparison_matrix_to_dict(result)

    assert derived is result
    assert rows == result.rows
    assert summarized.to_dict() == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert plan_shadow_mode_comparison_matrix_to_dicts(result) == payload["rows"]
    assert plan_shadow_mode_comparison_matrix_to_dicts(rows) == payload["rows"]
    assert payload["rows"][0]["comparison_surface"] == "old-vs-new comparison"
    assert payload["rows"][0]["baseline_system"] == "rules engine"
    assert payload["rows"][0]["candidate_system"] == "ml scorer"


def _plan(tasks, *, plan_id="plan-shadow"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-shadow",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    owner_type=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
    }
    if owner_type is not None:
        task["owner_type"] = owner_type
    if metadata is not None:
        task["metadata"] = metadata
    return task
