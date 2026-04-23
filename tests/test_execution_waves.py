import json

from click.testing import CliRunner
import pytest

from blueprint import config as blueprint_config
from blueprint.audits.execution_waves import (
    DependencyCycleError,
    UnknownDependencyError,
    analyze_execution_waves,
)
from blueprint.cli import cli
from blueprint.store import init_db


def test_execution_waves_groups_independent_tasks_in_same_wave():
    plan = _plan_with_tasks(
        [
            _task("task-a", "Task A", "Foundation"),
            _task("task-b", "Task B", "Foundation"),
            _task("task-c", "Task C", "Build", depends_on=["task-a"]),
        ]
    )

    result = analyze_execution_waves(plan)

    assert [wave.task_ids for wave in result.waves] == [
        ["task-a", "task-b"],
        ["task-c"],
    ]
    assert result.waves[0].tasks[0].title == "Task A"
    assert result.waves[0].tasks[0].milestone == "Foundation"
    assert result.waves[0].tasks[0].suggested_engine == "codex"
    assert result.waves[0].tasks[0].files_or_modules == ["src/app.py"]


def test_execution_waves_handles_chained_dependencies():
    plan = _plan_with_tasks(
        [
            _task("task-a", "Task A", "Foundation"),
            _task("task-b", "Task B", "Build", depends_on=["task-a"]),
            _task("task-c", "Task C", "Launch", depends_on=["task-b"]),
        ]
    )

    result = analyze_execution_waves(plan)

    assert [wave.task_ids for wave in result.waves] == [
        ["task-a"],
        ["task-b"],
        ["task-c"],
    ]


def test_execution_waves_handles_diamond_dependencies():
    plan = _plan_with_tasks(
        [
            _task("task-root", "Root", "Foundation"),
            _task("task-left", "Left", "Build", depends_on=["task-root"]),
            _task("task-right", "Right", "Build", depends_on=["task-root"]),
            _task(
                "task-join",
                "Join",
                "Launch",
                depends_on=["task-left", "task-right"],
            ),
        ]
    )

    result = analyze_execution_waves(plan)

    assert [wave.task_ids for wave in result.waves] == [
        ["task-root"],
        ["task-left", "task-right"],
        ["task-join"],
    ]


def test_execution_waves_includes_completed_and_skipped_tasks():
    plan = _plan_with_tasks(
        [
            _task("task-complete", "Complete", "Foundation", status="completed"),
            _task("task-skip", "Skip", "Foundation", status="skipped"),
            _task(
                "task-next",
                "Next",
                "Build",
                depends_on=["task-complete", "task-skip"],
            ),
        ]
    )

    result = analyze_execution_waves(plan)

    assert [wave.task_ids for wave in result.waves] == [
        ["task-complete", "task-skip"],
        ["task-next"],
    ]


def test_execution_waves_rejects_unknown_dependencies():
    plan = _plan_with_tasks(
        [
            _task("task-a", "Task A", "Foundation"),
            _task("task-b", "Task B", "Build", depends_on=["task-missing"]),
        ]
    )

    with pytest.raises(UnknownDependencyError, match="task-b: task-missing"):
        analyze_execution_waves(plan)


def test_execution_waves_rejects_cycles():
    plan = _plan_with_tasks(
        [
            _task("task-a", "Task A", "Foundation", depends_on=["task-c"]),
            _task("task-b", "Task B", "Build", depends_on=["task-a"]),
            _task("task-c", "Task C", "Launch", depends_on=["task-b"]),
        ]
    )

    with pytest.raises(DependencyCycleError, match="Dependency cycle detected"):
        analyze_execution_waves(plan)


def test_task_waves_cli_outputs_json(tmp_path, monkeypatch):
    _seed_plan(
        tmp_path,
        monkeypatch,
        [
            _task("task-a", "Task A", "Foundation"),
            _task("task-b", "Task B", "Foundation"),
            _task("task-c", "Task C", "Launch", depends_on=["task-a", "task-b"]),
        ],
    )

    result = CliRunner().invoke(cli, ["task", "waves", "plan-test", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["plan_id"] == "plan-test"
    assert payload["task_count"] == 3
    assert [wave["task_ids"] for wave in payload["waves"]] == [
        ["task-a", "task-b"],
        ["task-c"],
    ]
    assert payload["waves"][0]["tasks"][0] == {
        "id": "task-a",
        "title": "Task A",
        "milestone": "Foundation",
        "suggested_engine": "codex",
        "files_or_modules": ["src/app.py"],
    }


def test_task_waves_cli_outputs_table_grouped_by_wave(tmp_path, monkeypatch):
    _seed_plan(
        tmp_path,
        monkeypatch,
        [
            _task("task-a", "Task A", "Foundation"),
            _task("task-b", "Task B", "Build", depends_on=["task-a"]),
        ],
    )

    result = CliRunner().invoke(cli, ["task", "waves", "plan-test"])

    assert result.exit_code == 0, result.output
    assert "Execution waves for plan plan-test" in result.output
    assert "\nWave 1\n" in result.output
    assert "\nWave 2\n" in result.output
    assert "task-a" in result.output
    assert "task-b" in result.output
    assert "Total: 2 tasks in 2 waves" in result.output


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
    *,
    depends_on=None,
    status="pending",
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
        "estimated_complexity": "medium",
        "status": status,
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
        "problem_statement": "Need task scheduling",
        "mvp_goal": "Expose waves in the CLI",
        "product_surface": "CLI",
        "scope": ["Task waves"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Incorrect waves"],
        "validation_plan": "Run task wave tests",
        "definition_of_done": ["Waves work"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
