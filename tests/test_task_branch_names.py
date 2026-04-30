import re

import pytest

from blueprint.domain.models import ExecutionPlan
from blueprint.task_branch_names import generate_task_branch_names


def test_task_branch_names_are_stable_for_every_task():
    plan = _plan_with_tasks(
        [
            _task("task-api", "Build API"),
            _task("task-ui", "Build UI"),
        ]
    )

    assert generate_task_branch_names(plan) == {
        "task-api": "task/task-api-build-api",
        "task-ui": "task/task-ui-build-ui",
    }


def test_task_branch_names_accept_execution_plan_models():
    plan = ExecutionPlan.model_validate(
        _plan_with_tasks([_task("task-model", "Model Task")])
    )

    assert generate_task_branch_names(plan) == {
        "task-model": "task/task-model-model-task",
    }


def test_task_branch_names_remove_unsafe_whitespace_and_control_characters():
    result = generate_task_branch_names(
        _plan_with_tasks(
            [
                _task(
                    "task/bad@{id}.lock",
                    "Fix: unsafe ~ branch^ name?\nwith\tspaces\x00",
                ),
            ]
        )
    )

    branch_name = result["task/bad@{id}.lock"]
    assert branch_name == "task/task-bad-id-lock-fix-unsafe-branch-name-with-spaces"
    assert not re.search(r"[\s\x00-\x1f\x7f~^:?*[\]\\]", branch_name)
    assert "@{" not in branch_name
    assert not any(component.endswith(".lock") for component in branch_name.split("/"))


def test_task_branch_names_truncate_long_names_to_max_length():
    result = generate_task_branch_names(
        _plan_with_tasks(
            [
                _task(
                    "task-very-long-identifier",
                    "Implement a very long title that needs deterministic truncation",
                ),
            ]
        ),
        max_length=48,
    )

    branch_name = result["task-very-long-identifier"]
    assert branch_name.startswith("task/task-very-long-identifier-implement-a-very")
    assert len(branch_name) <= 48


def test_task_branch_names_deduplicate_duplicate_or_similar_titles():
    result = generate_task_branch_names(
        _plan_with_tasks(
            [
                _task("task/a", "Build API!"),
                _task("task?a", "Build API?"),
                _task("task:a", "Build API."),
            ]
        )
    )

    assert result == {
        "task/a": "task/task-a-build-api",
        "task?a": "task/task-a-build-api-2",
        "task:a": "task/task-a-build-api-3",
    }


def test_task_branch_names_can_error_on_collision():
    with pytest.raises(ValueError, match="duplicate branch name"):
        generate_task_branch_names(
            _plan_with_tasks(
                [
                    _task("task/a", "Build API"),
                    _task("task?a", "Build API"),
                ]
            ),
            collision_strategy="error",
        )


def test_task_branch_names_handle_missing_titles_without_model_validation():
    plan = _plan_with_tasks(
        [
            {
                **_task("task-missing-title", "placeholder"),
                "title": "",
            },
        ]
    )

    assert generate_task_branch_names(plan) == {
        "task-missing-title": "task/task-missing-title-task",
    }


def test_task_branch_names_honor_custom_prefix_and_max_length():
    result = generate_task_branch_names(
        _plan_with_tasks([_task("task-branch", "Ship Feature")]),
        prefix="agents/codex branches",
        max_length=40,
    )

    assert result == {"task-branch": "agents/codex-branches/task-branch-ship-f"}
    assert len(result["task-branch"]) <= 40


def test_task_branch_names_reject_unknown_collision_strategy():
    with pytest.raises(ValueError, match="collision_strategy"):
        generate_task_branch_names(
            _plan_with_tasks([_task("task-a", "A")]),
            collision_strategy="random",
        )


def _plan_with_tasks(tasks):
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Build", "description": "Build the implementation"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": tasks,
    }


def _task(task_id, title):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": [f"{title} works"],
        "estimated_complexity": "medium",
        "risk_level": "medium",
        "status": "pending",
    }
