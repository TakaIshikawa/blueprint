from copy import deepcopy

from blueprint.plan_dependency_risk import score_plan_dependency_risk


def test_dependency_risk_identifies_root_leaf_and_high_fan_out_tasks():
    result = score_plan_dependency_risk(
        _plan_with_tasks(
            [
                _task("task-root", "Root"),
                _task("task-api", "API", depends_on=["task-root"]),
                _task("task-ui", "UI", depends_on=["task-root"]),
                _task("task-docs", "Docs", depends_on=["task-root"]),
                _task("task-release", "Release", depends_on=["task-api", "task-ui"]),
            ]
        )
    )

    assert result.root_blocker_task_ids == ["task-root"]
    assert result.leaf_task_ids == ["task-docs", "task-release"]
    assert result.high_fan_out_task_ids == ["task-root"]

    risks = {task.task_id: task for task in result.tasks}
    assert risks["task-root"].fan_in == 0
    assert risks["task-root"].fan_out == 3
    assert risks["task-root"].transitive_blocker_count == 4
    assert risks["task-release"].fan_in == 2
    assert risks["task-release"].fan_out == 0
    assert risks["task-release"].transitive_blocker_count == 0


def test_dependency_risk_scores_increase_for_more_downstream_work():
    result = score_plan_dependency_risk(
        _plan_with_tasks(
            [
                _task("task-a", "A"),
                _task("task-b", "B", depends_on=["task-a"]),
                _task("task-c", "C", depends_on=["task-b"]),
                _task("task-d", "D", depends_on=["task-a"]),
            ]
        )
    )

    risks = {task.task_id: task for task in result.tasks}
    assert risks["task-a"].risk_score > 0
    assert risks["task-b"].risk_score > risks["task-c"].risk_score
    assert risks["task-a"].risk_score > risks["task-b"].risk_score


def test_dependency_risk_scores_increase_for_downstream_high_risk_tasks():
    low_downstream = score_plan_dependency_risk(
        _plan_with_tasks(
            [
                _task("task-root", "Root"),
                _task("task-child", "Child", depends_on=["task-root"], risk_level="low"),
            ]
        )
    )
    high_downstream = score_plan_dependency_risk(
        _plan_with_tasks(
            [
                _task("task-root", "Root"),
                _task(
                    "task-child",
                    "Child",
                    depends_on=["task-root"],
                    risk_level="high",
                ),
            ]
        )
    )

    low_root = low_downstream.tasks[0]
    high_root = high_downstream.tasks[0]
    assert low_root.downstream_high_risk_task_count == 0
    assert high_root.downstream_high_risk_task_count == 1
    assert high_root.risk_score > low_root.risk_score


def test_dependency_risk_reports_missing_dependencies_without_crashing():
    result = score_plan_dependency_risk(
        _plan_with_tasks(
            [
                _task("task-a", "A"),
                _task("task-b", "B", depends_on=["task-a", "task-missing"]),
                _task("task-c", "C", depends_on=["task-unknown"]),
            ]
        )
    )

    assert result.has_missing_dependencies is True
    assert result.missing_dependencies_by_task_id == {
        "task-b": ["task-missing"],
        "task-c": ["task-unknown"],
    }
    assert result.root_blocker_task_ids == ["task-a"]
    assert result.leaf_task_ids == ["task-b", "task-c"]

    risks = {task.task_id: task for task in result.tasks}
    assert risks["task-b"].fan_in == 1
    assert risks["task-c"].fan_in == 0


def test_dependency_risk_to_dict_is_deterministic():
    result = score_plan_dependency_risk(
        _plan_with_tasks(
            [
                _task("task-root", "Root"),
                _task(
                    "task-risk",
                    "Risky",
                    depends_on=["task-root"],
                    risk_level="high",
                ),
            ]
        )
    )

    assert result.to_dict() == {
        "plan_id": "plan-test",
        "task_count": 2,
        "summary": {
            "root_blockers": 1,
            "leaf_tasks": 1,
            "high_fan_out_tasks": 0,
            "missing_dependency_references": 0,
        },
        "root_blocker_task_ids": ["task-root"],
        "leaf_task_ids": ["task-risk"],
        "high_fan_out_task_ids": [],
        "missing_dependencies_by_task_id": {},
        "tasks": [
            {
                "task_id": "task-root",
                "title": "Root",
                "risk_level": "medium",
                "fan_in": 0,
                "fan_out": 1,
                "transitive_blocker_count": 1,
                "downstream_high_risk_task_count": 1,
                "risk_score": 100.0,
                "is_root_blocker": True,
                "is_leaf_task": False,
                "is_high_fan_out": False,
            },
            {
                "task_id": "task-risk",
                "title": "Risky",
                "risk_level": "high",
                "fan_in": 1,
                "fan_out": 0,
                "transitive_blocker_count": 0,
                "downstream_high_risk_task_count": 0,
                "risk_score": 0.0,
                "is_root_blocker": False,
                "is_leaf_task": True,
                "is_high_fan_out": False,
            },
        ],
    }


def test_dependency_risk_does_not_mutate_execution_plan():
    plan = _plan_with_tasks(
        [
            _task("task-b", "Task B", depends_on=["task-a"]),
            _task("task-a", "Task A"),
        ]
    )
    original = deepcopy(plan)

    score_plan_dependency_risk(plan)

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


def _task(task_id, title, *, depends_on=None, risk_level="medium"):
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
        "risk_level": risk_level,
        "status": "pending",
    }
