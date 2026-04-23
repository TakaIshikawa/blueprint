import json

from click.testing import CliRunner
import pytest

from blueprint import config as blueprint_config
from blueprint.audits.critical_path import (
    DependencyCycleError,
    analyze_critical_path,
)
from blueprint.cli import cli
from blueprint.store import init_db


def test_critical_path_finds_linear_chain():
    plan = _plan_with_tasks(
        [
            _task("task-setup", "Setup project", "Foundation", "low"),
            _task(
                "task-api",
                "Build API",
                "Foundation",
                "medium",
                depends_on=["task-setup"],
            ),
            _task(
                "task-ui",
                "Build UI",
                "Interface",
                "high",
                depends_on=["task-api"],
            ),
        ]
    )

    result = analyze_critical_path(plan)

    assert result.task_ids == ["task-setup", "task-api", "task-ui"]
    assert result.total_weight == 6
    assert [task.cumulative_weight for task in result.tasks] == [1, 3, 6]
    assert result.tasks[2].blocking_dependencies == ["task-api"]


def test_critical_path_chooses_heaviest_branch():
    plan = _plan_with_tasks(
        [
            _task("task-foundation", "Foundation", "Foundation", "low"),
            _task(
                "task-fast-path",
                "Fast path",
                "Build",
                "medium",
                depends_on=["task-foundation"],
            ),
            _task(
                "task-heavy-path",
                "Heavy path",
                "Build",
                "high",
                depends_on=["task-foundation"],
            ),
            _task(
                "task-release",
                "Release",
                "Launch",
                "medium",
                depends_on=["task-fast-path", "task-heavy-path"],
            ),
        ]
    )

    result = analyze_critical_path(plan)

    assert result.task_ids == ["task-foundation", "task-heavy-path", "task-release"]
    assert result.total_weight == 6
    assert result.tasks[-1].blocking_dependencies == [
        "task-fast-path",
        "task-heavy-path",
    ]


def test_critical_path_handles_independent_tasks():
    plan = _plan_with_tasks(
        [
            _task("task-small", "Small task", "Build", "low"),
            _task("task-unknown", "Unknown task", "Build", None),
            _task("task-large", "Large task", "Build", "high"),
        ]
    )

    result = analyze_critical_path(plan)

    assert result.task_ids == ["task-large"]
    assert result.total_weight == 3
    assert result.tasks[0].blocking_dependencies == []


def test_critical_path_rejects_dependency_cycles():
    plan = _plan_with_tasks(
        [
            _task("task-a", "Task A", "Build", "low", depends_on=["task-c"]),
            _task("task-b", "Task B", "Build", "medium", depends_on=["task-a"]),
            _task("task-c", "Task C", "Build", "high", depends_on=["task-b"]),
        ]
    )

    with pytest.raises(DependencyCycleError, match="Dependency cycle detected"):
        analyze_critical_path(plan)


def test_critical_path_cli_outputs_json(tmp_path, monkeypatch):
    _seed_plan(
        tmp_path,
        monkeypatch,
        [
            _task("task-a", "Task A", "Foundation", "low"),
            _task("task-b", "Task B", "Foundation", "high", depends_on=["task-a"]),
            _task("task-c", "Task C", "Launch", "medium", depends_on=["task-b"]),
        ],
    )

    result = CliRunner().invoke(
        cli,
        ["task", "critical-path", "plan-test", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == {
        "plan_id": "plan-test",
        "total_weight": 6,
        "task_ids": ["task-a", "task-b", "task-c"],
        "tasks": [
            {
                "id": "task-a",
                "title": "Task A",
                "milestone": "Foundation",
                "estimated_complexity": "low",
                "weight": 1,
                "cumulative_weight": 1,
                "blocking_dependencies": [],
            },
            {
                "id": "task-b",
                "title": "Task B",
                "milestone": "Foundation",
                "estimated_complexity": "high",
                "weight": 3,
                "cumulative_weight": 4,
                "blocking_dependencies": ["task-a"],
            },
            {
                "id": "task-c",
                "title": "Task C",
                "milestone": "Launch",
                "estimated_complexity": "medium",
                "weight": 2,
                "cumulative_weight": 6,
                "blocking_dependencies": ["task-b"],
            },
        ],
    }


def _seed_plan(tmp_path, monkeypatch, tasks):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), tasks)


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
exports:
  output_dir: {tmp_path}
"""
    )
    blueprint_config.reload_config()


def _plan_with_tasks(tasks):
    plan = _execution_plan()
    plan["tasks"] = tasks
    return plan


def _execution_plan():
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Build", "description": "Build the implementation"},
            {"name": "Interface", "description": "Build the user-facing flow"},
            {"name": "Launch", "description": "Release the work"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _task(
    task_id,
    title,
    milestone,
    estimated_complexity,
    *,
    depends_on=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}",
        "milestone": milestone,
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": [f"{title} works"],
        "estimated_complexity": estimated_complexity,
        "status": "pending",
    }


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need task management",
        "mvp_goal": "Expose tasks in the CLI",
        "product_surface": "CLI",
        "scope": ["Task commands"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Incorrect critical path"],
        "validation_plan": "Run task CLI tests",
        "definition_of_done": ["Critical path works"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
