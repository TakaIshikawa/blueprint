from copy import deepcopy

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_dependency_cycles import explain_plan_dependency_cycles


def test_dependency_cycles_reports_no_cycles_for_dag():
    result = explain_plan_dependency_cycles(
        _plan_with_tasks(
            [
                _task("task-a", "Task A"),
                _task("task-b", "Task B", depends_on=["task-a"]),
                _task("task-c", "Task C", depends_on=["task-b"]),
            ]
        )
    )

    assert result.has_cycles is False
    assert result.severity == "none"
    assert result.affected_task_ids == []
    assert result.cycles == []
    assert result.missing_dependencies == []


def test_dependency_cycles_detects_self_cycle():
    result = explain_plan_dependency_cycles(
        _plan_with_tasks([_task("task-a", "Task A", depends_on=["task-a"])])
    )

    assert result.has_cycles is True
    assert result.severity == "blocking"
    assert result.affected_task_ids == ["task-a"]
    assert len(result.cycles) == 1
    assert result.cycles[0].path == ["task-a", "task-a"]
    assert result.cycles[0].affected_task_ids == ["task-a"]
    assert [suggestion.to_dict() for suggestion in result.cycles[0].suggested_removals] == [
        {"task_id": "task-a", "dependency_id": "task-a"}
    ]


def test_dependency_cycles_detects_multi_task_cycle_with_stable_path():
    result = explain_plan_dependency_cycles(
        _plan_with_tasks(
            [
                _task("task-c", "Task C", depends_on=["task-b"]),
                _task("task-a", "Task A", depends_on=["task-c"]),
                _task("task-b", "Task B", depends_on=["task-a"]),
                _task("task-d", "Task D", depends_on=["task-a"]),
            ]
        )
    )

    assert result.affected_task_ids == ["task-a", "task-b", "task-c"]
    assert len(result.cycles) == 1
    cycle = result.cycles[0]
    assert cycle.path == ["task-a", "task-c", "task-b", "task-a"]
    assert cycle.affected_task_ids == ["task-a", "task-b", "task-c"]
    assert [suggestion.to_dict() for suggestion in cycle.suggested_removals] == [
        {"task_id": "task-a", "dependency_id": "task-c"},
        {"task_id": "task-c", "dependency_id": "task-b"},
        {"task_id": "task-b", "dependency_id": "task-a"},
    ]


def test_dependency_cycles_collapses_duplicate_cycle_representations():
    result = explain_plan_dependency_cycles(
        _plan_with_tasks(
            [
                _task("task-a", "Task A", depends_on=["task-b", "task-b"]),
                _task("task-b", "Task B", depends_on=["task-c"]),
                _task("task-c", "Task C", depends_on=["task-a"]),
            ]
        )
    )

    assert [cycle.path for cycle in result.cycles] == [["task-a", "task-b", "task-c", "task-a"]]


def test_dependency_cycles_reports_missing_dependencies_without_cycle_edges():
    result = explain_plan_dependency_cycles(
        _plan_with_tasks(
            [
                _task("task-a", "Task A", depends_on=["task-missing"]),
                _task("task-b", "Task B", depends_on=["task-a", "task-unknown"]),
            ]
        )
    )

    assert result.has_cycles is False
    assert result.has_missing_dependencies is True
    assert [dependency.to_dict() for dependency in result.missing_dependencies] == [
        {"task_id": "task-a", "dependency_id": "task-missing"},
        {"task_id": "task-b", "dependency_id": "task-unknown"},
    ]


def test_dependency_cycles_accepts_execution_plan_model_and_serializes():
    plan = ExecutionPlan.model_validate(
        _plan_with_tasks(
            [
                _task("task-a", "Task A", depends_on=["task-b"]),
                _task("task-b", "Task B", depends_on=["task-a"]),
            ]
        )
    )

    result = explain_plan_dependency_cycles(plan)

    assert result.to_dict() == {
        "plan_id": "plan-test",
        "task_count": 2,
        "severity": "blocking",
        "affected_task_ids": ["task-a", "task-b"],
        "cycles": [
            {
                "path": ["task-a", "task-b", "task-a"],
                "affected_task_ids": ["task-a", "task-b"],
                "severity": "blocking",
                "suggested_removals": [
                    {"task_id": "task-a", "dependency_id": "task-b"},
                    {"task_id": "task-b", "dependency_id": "task-a"},
                ],
            }
        ],
        "missing_dependencies": [],
    }


def test_dependency_cycles_does_not_mutate_execution_plan():
    plan = _plan_with_tasks(
        [
            _task("task-a", "Task A", depends_on=["task-b"]),
            _task("task-b", "Task B", depends_on=["task-a"]),
        ]
    )
    original = deepcopy(plan)

    explain_plan_dependency_cycles(plan)

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


def _task(task_id, title, *, depends_on=None):
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
        "risk_level": "medium",
        "status": "pending",
    }
