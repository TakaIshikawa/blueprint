from copy import deepcopy

from blueprint.plan_dependency_metrics import calculate_dependency_metrics


def test_dependency_metrics_handles_linear_chains():
    plan = _plan_with_tasks(
        [
            _task("task-a", "Task A"),
            _task("task-b", "Task B", depends_on=["task-a"]),
            _task("task-c", "Task C", depends_on=["task-b"]),
        ]
    )

    result = calculate_dependency_metrics(plan)

    assert result.root_task_count == 1
    assert result.leaf_task_count == 1
    assert result.max_dependency_depth == 2
    assert result.tasks_by_depth == {
        0: ["task-a"],
        1: ["task-b"],
        2: ["task-c"],
    }
    assert [group.task_ids for group in result.parallelizable_task_groups] == [
        ["task-a"],
        ["task-b"],
        ["task-c"],
    ]


def test_dependency_metrics_handles_branching_dags():
    plan = _plan_with_tasks(
        [
            _task("task-root", "Root"),
            _task("task-left", "Left", depends_on=["task-root"]),
            _task("task-right", "Right", depends_on=["task-root"]),
            _task("task-docs", "Docs"),
            _task("task-join", "Join", depends_on=["task-left", "task-right"]),
        ]
    )

    result = calculate_dependency_metrics(plan)

    assert result.root_task_count == 2
    assert result.leaf_task_count == 2
    assert result.max_dependency_depth == 2
    assert result.tasks_by_depth == {
        0: ["task-root", "task-docs"],
        1: ["task-left", "task-right"],
        2: ["task-join"],
    }
    assert [group.to_dict() for group in result.parallelizable_task_groups] == [
        {"depth": 0, "task_ids": ["task-root", "task-docs"]},
        {"depth": 1, "task_ids": ["task-left", "task-right"]},
        {"depth": 2, "task_ids": ["task-join"]},
    ]


def test_dependency_metrics_reports_missing_dependencies_without_crashing():
    plan = _plan_with_tasks(
        [
            _task("task-a", "Task A"),
            _task("task-b", "Task B", depends_on=["task-a", "task-missing"]),
            _task("task-c", "Task C", depends_on=["task-unknown"]),
        ]
    )

    result = calculate_dependency_metrics(plan)

    assert result.blocked_dependency_count == 2
    assert result.missing_dependencies_by_task_id == {
        "task-b": ["task-missing"],
        "task-c": ["task-unknown"],
    }
    assert result.root_task_count == 1
    assert result.max_dependency_depth == 1
    assert result.tasks_by_depth == {
        0: ["task-a", "task-c"],
        1: ["task-b"],
    }


def test_dependency_metrics_handles_empty_task_lists():
    result = calculate_dependency_metrics(_plan_with_tasks([]))

    assert result.to_dict() == {
        "plan_id": "plan-test",
        "task_count": 0,
        "root_task_count": 0,
        "leaf_task_count": 0,
        "max_dependency_depth": 0,
        "tasks_by_depth": {},
        "blocked_dependency_count": 0,
        "missing_dependencies_by_task_id": {},
        "parallelizable_task_groups": [],
    }


def test_dependency_metrics_includes_completed_and_blocked_tasks_by_dependency_layer():
    plan = _plan_with_tasks(
        [
            _task("task-completed", "Completed", status="completed"),
            _task("task-blocked", "Blocked", status="blocked"),
            _task(
                "task-next",
                "Next",
                depends_on=["task-completed", "task-blocked"],
            ),
        ]
    )

    result = calculate_dependency_metrics(plan)

    assert result.root_task_count == 2
    assert result.leaf_task_count == 1
    assert result.tasks_by_depth == {
        0: ["task-completed", "task-blocked"],
        1: ["task-next"],
    }
    assert result.blocked_dependency_count == 0


def test_dependency_metrics_does_not_mutate_execution_plan():
    plan = _plan_with_tasks(
        [
            _task("task-b", "Task B", depends_on=["task-a"]),
            _task("task-a", "Task A"),
        ]
    )
    original = deepcopy(plan)

    calculate_dependency_metrics(plan)

    assert plan == original


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


def _task(task_id, title, *, depends_on=None, status="pending"):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": [f"{title} works"],
        "estimated_complexity": "medium",
        "status": status,
    }
