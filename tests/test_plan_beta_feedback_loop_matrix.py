import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_beta_feedback_loop_matrix import (
    PlanBetaFeedbackLoopMatrix,
    PlanBetaFeedbackLoopMatrixRow,
    build_plan_beta_feedback_loop_matrix,
    generate_plan_beta_feedback_loop_matrix,
    plan_beta_feedback_loop_matrix_to_dict,
    plan_beta_feedback_loop_matrix_to_dicts,
    plan_beta_feedback_loop_matrix_to_markdown,
    summarize_plan_beta_feedback_loop_matrix,
)


def test_detects_beta_pilot_preview_and_feedback_channels_from_tasks_and_plan_metadata():
    result = build_plan_beta_feedback_loop_matrix(
        _plan(
            [
                _task("task-beta", title="Invite beta users to onboarding"),
                _task(
                    "task-pilot",
                    title="Pilot cohort for billing",
                    description="Collect survey results and support tagging during pilot.",
                ),
                _task(
                    "task-preview",
                    title="Preview release for reports",
                    acceptance_criteria=["Add feedback form and product analytics dashboard."],
                ),
                _task("task-docs", title="Update internal docs"),
            ],
            metadata={"rollout": "Public preview release uses usability notes for launch review."},
        )
    )

    by_id = {row.task_id: row for row in result.rows}
    assert set(by_id) == {"task-beta", "task-pilot", "task-preview", "task-docs"}
    assert by_id["task-pilot"].feedback_channels == ("survey", "support_tagging", "usability_notes")
    assert by_id["task-preview"].feedback_channels == (
        "feedback_form",
        "product_analytics",
        "usability_notes",
    )
    assert by_id["task-docs"].evidence == (
        "plan.metadata.rollout: Public preview release uses usability notes for launch review.",
    )
    assert result.summary["task_count"] == 4
    assert result.summary["feedback_loop_task_count"] == 4
    assert result.summary["feedback_channel_counts"]["usability_notes"] == 4


def test_complete_feedback_loop_builds_low_risk_row():
    result = build_plan_beta_feedback_loop_matrix(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Launch beta workspace switcher",
                    description=(
                        "Beta users get the preview release. Feedback owner: Growth PM. "
                        "Review cadence: weekly readout. Decision criteria: launch if CSAT is above 4.2. "
                        "Iteration criteria: prioritize feedback backlog before GA. "
                        "Broad launch gate: go/no-go review after pilot."
                    ),
                    metadata={
                        "feedback_loop": {
                            "feedback_channels": [
                                "user feedback sessions",
                                "survey",
                                "product analytics",
                            ]
                        }
                    },
                )
            ]
        )
    )

    row = result.rows[0]
    assert isinstance(row, PlanBetaFeedbackLoopMatrixRow)
    assert row.feedback_channels == ("user_feedback", "survey", "product_analytics")
    assert row.missing_loop_steps == ()
    assert row.risk_level == "low"
    assert any(item.startswith("description: Beta users get the preview release.") for item in row.evidence)
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}
    assert result.summary["missing_loop_step_count"] == 0


def test_missing_channels_decision_and_iteration_escalate_risk_and_sort_before_medium():
    result = build_plan_beta_feedback_loop_matrix(
        _plan(
            [
                _task(
                    "task-medium",
                    title="Collect launch feedback",
                    description=(
                        "Collect user feedback. Feedback owner: Product. "
                        "Weekly feedback review. Launch gate after go/no-go."
                    ),
                ),
                _task(
                    "task-high",
                    title="Open beta cohort",
                    description="Beta cohort for admins before broad launch.",
                ),
            ]
        )
    )

    by_id = {row.task_id: row for row in result.rows}
    assert [row.task_id for row in result.rows] == ["task-high", "task-medium"]
    assert by_id["task-high"].risk_level == "high"
    assert by_id["task-high"].missing_loop_steps == (
        "feedback_channel",
        "triage_owner",
        "review_cadence",
        "decision_criteria",
        "iteration_criteria",
    )
    assert by_id["task-medium"].risk_level == "medium"
    assert by_id["task-medium"].missing_loop_steps == ("iteration_criteria",)
    assert result.summary["missing_loop_step_counts"]["iteration_criteria"] == 2


def test_markdown_and_serialization_helpers_are_stable_and_escape_cells():
    result = build_plan_beta_feedback_loop_matrix(
        _plan(
            [
                _task(
                    "task-md",
                    title="Pilot cohort | analytics",
                    description=(
                        "Pilot cohort. User feedback and feedback form. Owner: PM. "
                        "Daily review cadence. Decision criteria: promote if activation improves. "
                        "Iteration criteria: revise onboarding copy. Broad launch gate: GA review."
                    ),
                )
            ]
        )
    )

    markdown = plan_beta_feedback_loop_matrix_to_markdown(result)
    payload = plan_beta_feedback_loop_matrix_to_dict(result)

    assert markdown == result.to_markdown()
    assert markdown.startswith("# Plan Beta Feedback Loop Matrix: plan-beta")
    assert "- Feedback-loop task count: 1" in markdown
    assert "| `task-md` | Pilot cohort \\| analytics | low | user_feedback; feedback_form | none |" in markdown
    assert list(payload) == ["plan_id", "summary", "rows"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "feedback_channels",
        "missing_loop_steps",
        "risk_level",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert plan_beta_feedback_loop_matrix_to_dicts(result) == payload["rows"]
    assert plan_beta_feedback_loop_matrix_to_dicts(result.rows) == payload["rows"]


def test_empty_invalid_and_model_inputs_are_deterministic_and_do_not_mutate():
    plan = _plan(
        [
            _task(
                "task-api",
                title="Add account endpoint",
                description="Create backend route for account reads.",
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_beta_feedback_loop_matrix(plan)
    model_result = build_plan_beta_feedback_loop_matrix(ExecutionPlan.model_validate(plan))
    alias_result = summarize_plan_beta_feedback_loop_matrix(model_result)

    assert plan == original
    assert isinstance(result, PlanBetaFeedbackLoopMatrix)
    assert result.rows == ()
    assert result.to_dict() == {
        "plan_id": "plan-beta",
        "summary": {
            "task_count": 1,
            "feedback_loop_task_count": 0,
            "missing_loop_step_count": 0,
            "at_risk_task_count": 0,
            "risk_counts": {"high": 0, "medium": 0, "low": 0},
            "feedback_channel_counts": {
                "user_feedback": 0,
                "survey": 0,
                "feedback_form": 0,
                "product_analytics": 0,
                "support_tagging": 0,
                "usability_notes": 0,
            },
            "missing_loop_step_counts": {
                "feedback_channel": 0,
                "triage_owner": 0,
                "review_cadence": 0,
                "decision_criteria": 0,
                "iteration_criteria": 0,
                "broad_launch_gate": 0,
            },
        },
        "rows": [],
    }
    assert result.to_markdown() == (
        "# Plan Beta Feedback Loop Matrix: plan-beta\n\n"
        "## Summary\n\n"
        "- Task count: 1\n"
        "- Feedback-loop task count: 0\n"
        "- Missing loop step count: 0\n"
        "- Risk counts: high 0, medium 0, low 0\n\n"
        "No beta or feedback-loop signals were detected."
    )
    assert model_result.to_dict() == result.to_dict()
    assert alias_result is model_result
    assert generate_plan_beta_feedback_loop_matrix({"tasks": "not a list"}) == ()


def _plan(tasks, *, metadata=None):
    return {
        "id": "plan-beta",
        "implementation_brief_id": "brief-beta",
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
