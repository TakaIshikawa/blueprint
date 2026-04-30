from copy import deepcopy
import json

from blueprint.dependency_unlock_report import (
    DependencyUnlockReport,
    StillBlockedTask,
    UnlockedTask,
    build_dependency_unlock_report,
    dependency_unlock_report_to_dict,
)
from blueprint.domain.models import ExecutionPlan


def test_completed_tasks_are_excluded_and_unlocked_tasks_preserve_plan_order():
    plan = _plan(
        [
            _task("task-done", "Done"),
            _task("task-later", "Later", depends_on=["task-mid"]),
            _task("task-api", "API", depends_on=["task-done"]),
            _task("task-ui", "UI", depends_on=["task-done"]),
            _task("task-mid", "Middle", depends_on=["task-done"]),
        ]
    )

    report = build_dependency_unlock_report(plan, ["task-done"])

    assert isinstance(report, DependencyUnlockReport)
    assert [task.task_id for task in report.unlocked_tasks] == [
        "task-api",
        "task-ui",
        "task-mid",
    ]
    assert "task-done" not in [task.task_id for task in report.unlocked_tasks]
    assert report.unlocked_tasks[0] == UnlockedTask(
        task_id="task-api",
        title="API",
        dependency_ids=("task-done",),
    )


def test_partial_and_unknown_dependencies_are_reported_as_blocked_details():
    plan = _plan(
        [
            _task("task-root", "Root"),
            _task(
                "task-api",
                "API",
                depends_on=["task-root", "task-db", "task-missing"],
            ),
            _task("task-db", "DB"),
        ]
    )

    report = build_dependency_unlock_report(plan, ["task-root"])

    assert report.blocked_tasks == (
        StillBlockedTask(
            task_id="task-api",
            title="API",
            dependency_ids=("task-root", "task-db", "task-missing"),
            missing_dependency_ids=("task-db",),
            unknown_dependency_ids=("task-missing",),
            blocked_reason=(
                "waiting for completed dependencies: task-db; "
                "references unknown dependencies: task-missing"
            ),
        ),
    )
    assert "task-db" in report.blocked_tasks[0].blocked_reason
    assert "task-missing" in report.blocked_tasks[0].blocked_reason


def test_roots_and_tasks_with_all_dependencies_completed_are_unlocked():
    report = build_dependency_unlock_report(
        _plan(
            [
                _task("task-root", "Root"),
                _task("task-a", "A", depends_on=["task-root"]),
                _task("task-b", "B", depends_on=["task-root", "task-a"]),
            ]
        ),
        ["task-root"],
    )

    assert [task.task_id for task in report.unlocked_tasks] == ["task-a"]
    assert [task.task_id for task in report.blocked_tasks] == ["task-b"]
    assert report.blocked_tasks[0].missing_dependency_ids == ("task-a",)


def test_helper_accepts_execution_plan_instances():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task("task-done", "Done"),
                _task("task-next", "Next", depends_on=["task-done"]),
            ]
        )
    )

    report = build_dependency_unlock_report(plan, ("task-done",))

    assert report.plan_id == "plan-unlock"
    assert report.completed_task_ids == ("task-done",)
    assert [task.to_dict() for task in report.unlocked_tasks] == [
        {
            "task_id": "task-next",
            "title": "Next",
            "dependency_ids": ["task-done"],
        }
    ]


def test_build_dependency_unlock_report_does_not_mutate_input_plan():
    plan = _plan(
        [
            _task("task-root", "Root"),
            _task("task-next", "Next", depends_on=["task-root", "task-missing"]),
        ]
    )
    original = deepcopy(plan)

    build_dependency_unlock_report(plan, ["task-root"])

    assert plan == original


def test_serialization_is_json_compatible_and_stable():
    report = build_dependency_unlock_report(
        _plan(
            [
                _task("task-done", "Done"),
                _task("task-next", "Next", depends_on=["task-done"]),
            ]
        ),
        ["task-done"],
    )

    payload = dependency_unlock_report_to_dict(report)

    assert payload == report.to_dict()
    assert list(payload) == [
        "plan_id",
        "completed_task_ids",
        "unlocked_tasks",
        "blocked_tasks",
    ]
    assert list(payload["unlocked_tasks"][0]) == [
        "task_id",
        "title",
        "dependency_ids",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _plan(tasks):
    return {
        "id": "plan-unlock",
        "implementation_brief_id": "brief-unlock",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Build", "description": "Build the feature"}],
        "test_strategy": "Run focused pytest",
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
        "description": f"Implement {title}.",
        "milestone": "Build",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": ["src/blueprint/example.py"],
        "acceptance_criteria": [f"{title} works"],
        "estimated_complexity": "low",
        "risk_level": "low",
        "test_command": "poetry run pytest",
        "status": status,
    }
