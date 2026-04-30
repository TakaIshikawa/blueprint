import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_status_recommendations import (
    PlanStatusRecommendation,
    plan_status_recommendation_to_dict,
    recommend_plan_status,
)


def test_all_completed_tasks_recommend_completed_plan_status():
    result = recommend_plan_status(
        _plan(
            status="in_progress",
            tasks=[
                _task("task-api", "API", status="completed"),
                _task("task-ui", "UI", status="completed"),
            ],
        )
    )

    assert result.to_dict() == {
        "plan_id": "plan-status",
        "current_status": "in_progress",
        "recommended_status": "completed",
        "explanation_codes": ["all_tasks_completed"],
        "relevant_task_ids": ["task-api", "task-ui"],
        "blocking_task_ids": [],
    }


def test_blocked_draft_plan_recommends_draft_with_blocking_task_ids():
    result = recommend_plan_status(
        _plan(
            status="draft",
            tasks=[
                _task("task-api", "API", status="blocked", blocked_reason="Needs token"),
                _task("task-ui", "UI", depends_on=["task-api"]),
            ],
        )
    )

    assert result.recommended_status == "draft"
    assert result.explanation_codes == ("blocked_tasks_present", "draft_blocked")
    assert result.relevant_task_ids == ("task-api",)
    assert result.blocking_task_ids == ("task-api",)


def test_blocked_started_plan_recommends_failed():
    result = recommend_plan_status(
        _plan(
            status="in_progress",
            tasks=[
                _task("task-api", "API", status="blocked", blocked_reason="CI failed"),
                _task("task-ui", "UI", status="completed"),
            ],
        )
    )

    assert result.current_status == "in_progress"
    assert result.recommended_status == "failed"
    assert result.explanation_codes == ("blocked_tasks_present",)
    assert result.blocking_task_ids == ("task-api",)


def test_invalid_dependencies_block_draft_plan_with_explanation_code():
    result = recommend_plan_status(
        _plan(
            status="ready",
            tasks=[
                _task("task-ui", "UI", depends_on=["task-missing"]),
            ],
        )
    )

    assert result.recommended_status == "draft"
    assert result.explanation_codes == ("invalid_dependencies", "draft_blocked")
    assert result.relevant_task_ids == ("task-ui",)
    assert result.blocking_task_ids == ("task-ui",)


def test_invalid_dependencies_fail_queued_plan():
    result = recommend_plan_status(
        _plan(
            status="queued",
            tasks=[
                _task("task-ui", "UI", depends_on=["task-missing"]),
            ],
        )
    )

    assert result.recommended_status == "failed"
    assert result.explanation_codes == ("invalid_dependencies",)
    assert result.blocking_task_ids == ("task-ui",)


def test_pending_tasks_with_satisfied_dependencies_recommend_ready_from_draft():
    result = recommend_plan_status(
        _plan(
            status="draft",
            tasks=[
                _task("task-setup", "Setup", status="completed"),
                _task("task-api", "API", depends_on=["task-setup"]),
            ],
        )
    )

    assert result.current_status == "draft"
    assert result.recommended_status == "ready"
    assert result.explanation_codes == ("ready_tasks_available",)
    assert result.relevant_task_ids == ("task-api",)


def test_pending_tasks_with_satisfied_dependencies_recommend_queued_from_ready():
    result = recommend_plan_status(
        _plan(
            status="ready",
            tasks=[
                _task("task-api", "API"),
                _task("task-ui", "UI", depends_on=["task-api"]),
            ],
        )
    )

    assert result.recommended_status == "queued"
    assert result.explanation_codes == ("queued_tasks_available",)
    assert result.relevant_task_ids == ("task-api",)


def test_missing_validation_coverage_keeps_plan_in_draft():
    result = recommend_plan_status(
        _plan(
            status="draft",
            test_strategy=None,
            tasks=[
                _task("task-api", "API", test_command=None),
                _task("task-ui", "UI", test_command=" "),
            ],
        )
    )

    assert result.recommended_status == "draft"
    assert result.explanation_codes == (
        "missing_validation_coverage",
        "draft_blocked",
    )
    assert result.blocking_task_ids == ("task-api", "task-ui")


def test_plan_level_validation_coverage_can_cover_pending_tasks():
    result = recommend_plan_status(
        _plan(
            status="draft",
            metadata={"validation_commands": {"test": ["poetry run pytest"]}},
            tasks=[
                _task("task-api", "API", test_command=None),
            ],
        )
    )

    assert result.recommended_status == "ready"
    assert result.explanation_codes == ("ready_tasks_available",)
    assert result.relevant_task_ids == ("task-api",)


def test_in_progress_tasks_recommend_in_progress_before_queueing_more_work():
    result = recommend_plan_status(
        _plan(
            status="queued",
            tasks=[
                _task("task-api", "API", status="in_progress"),
                _task("task-ui", "UI"),
            ],
        )
    )

    assert result.recommended_status == "in_progress"
    assert result.explanation_codes == ("tasks_in_progress",)
    assert result.relevant_task_ids == ("task-api",)


def test_accepts_execution_plan_models_and_serializes_stably():
    model = ExecutionPlan.model_validate(
        _plan(
            status="draft",
            tasks=[
                _task("task-api", "API"),
            ],
        )
    )

    result = recommend_plan_status(model)
    payload = plan_status_recommendation_to_dict(result)

    assert isinstance(result, PlanStatusRecommendation)
    assert payload == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "current_status",
        "recommended_status",
        "explanation_codes",
        "relevant_task_ids",
        "blocking_task_ids",
    ]
    assert json.loads(json.dumps(payload, sort_keys=True)) == payload


def test_partial_mapping_falls_back_without_model_validation():
    result = recommend_plan_status(
        {
            "id": "plan-partial",
            "status": "draft",
            "tasks": [{"id": "task-partial", "status": "pending", "test_command": "pytest"}],
            "unexpected": "forces fallback because plan models forbid extras",
        }
    )

    assert result.to_dict() == {
        "plan_id": "plan-partial",
        "current_status": "draft",
        "recommended_status": "ready",
        "explanation_codes": ["ready_tasks_available"],
        "relevant_task_ids": ["task-partial"],
        "blocking_task_ids": [],
    }


def _plan(
    *,
    status="draft",
    tasks,
    test_strategy="Run focused task tests",
    metadata=None,
):
    return {
        "id": "plan-status",
        "implementation_brief_id": "brief-status",
        "target_engine": "codex",
        "target_repo": "blueprint",
        "project_type": "python",
        "milestones": [{"id": "m1", "title": "Build"}],
        "test_strategy": test_strategy,
        "handoff_prompt": "Implement the plan",
        "status": status,
        "metadata": metadata or {},
        "tasks": tasks,
    }


def _task(
    task_id,
    title,
    *,
    status="pending",
    depends_on=None,
    test_command="poetry run pytest tests/test_plan_status_recommendations.py -o addopts=''",
    blocked_reason=None,
):
    task = {
        "id": task_id,
        "execution_plan_id": "plan-status",
        "title": title,
        "description": f"Implement {title}",
        "milestone": "m1",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": ["src/blueprint/plan_status_recommendations.py"],
        "acceptance_criteria": [f"Verify {title} works."],
        "estimated_complexity": "small",
        "estimated_hours": 1,
        "risk_level": "low",
        "test_command": test_command,
        "status": status,
        "metadata": {},
    }
    if blocked_reason is not None:
        task["blocked_reason"] = blocked_reason
    return task
