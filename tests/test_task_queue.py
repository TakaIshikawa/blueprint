import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.store import init_db


def test_task_queue_shows_independent_ready_tasks(tmp_path, monkeypatch):
    _seed_queue_plan(tmp_path, monkeypatch)

    result = CliRunner().invoke(cli, ["task", "queue", "--plan-id", "plan-queue"])

    assert result.exit_code == 0, result.output
    assert "task-independent" in result.output
    assert "Foundation" in result.output
    assert "codex" in result.output
    assert "low" in result.output
    assert "Independent task" in result.output
    assert "pyproject.toml" in result.output


def test_task_queue_includes_tasks_unlocked_by_completed_dependencies(
    tmp_path, monkeypatch
):
    _seed_queue_plan(tmp_path, monkeypatch)

    result = CliRunner().invoke(
        cli,
        ["task", "queue", "--plan-id", "plan-queue", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    unlocked_task = next(task for task in payload if task["id"] == "task-unlocked")
    assert unlocked_task["description"] == "Run after setup"
    assert unlocked_task["depends_on"] == ["task-setup"]
    assert unlocked_task["ready_reason"] == (
        "All dependencies are completed or skipped: task-setup"
    )


def test_task_queue_filters_by_engine(tmp_path, monkeypatch):
    _seed_queue_plan(tmp_path, monkeypatch)

    result = CliRunner().invoke(
        cli,
        ["task", "queue", "--plan-id", "plan-queue", "--engine", "manual"],
    )

    assert result.exit_code == 0, result.output
    assert "task-manual" in result.output
    assert "task-independent" not in result.output
    assert "task-unlocked" not in result.output
    assert "Total: 1 ready tasks" in result.output


def test_task_queue_excludes_tasks_with_blocked_dependencies(tmp_path, monkeypatch):
    _seed_queue_plan(tmp_path, monkeypatch)

    result = CliRunner().invoke(
        cli,
        ["task", "queue", "--plan-id", "plan-queue", "--json"],
    )

    assert result.exit_code == 0, result.output
    task_ids = {task["id"] for task in json.loads(result.output)}
    assert "task-blocked-dependent" not in task_ids
    assert "task-progress-dependent" not in task_ids


def _seed_queue_plan(tmp_path, monkeypatch):
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

    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())


def _execution_plan():
    return {
        "id": "plan-queue",
        "implementation_brief_id": "ib-queue",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Delivery", "description": "Ship the workflow"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _tasks():
    return [
        _task(
            "task-independent",
            "Independent task",
            "Can start immediately",
            status="pending",
            files_or_modules=["pyproject.toml"],
            estimated_complexity="low",
        ),
        _task(
            "task-setup",
            "Setup dependency",
            "Already complete",
            status="completed",
            files_or_modules=["src/setup.py"],
        ),
        _task(
            "task-unlocked",
            "Unlocked task",
            "Run after setup",
            depends_on=["task-setup"],
            status="pending",
            files_or_modules=["src/app.py"],
            estimated_complexity="medium",
        ),
        _task(
            "task-manual",
            "Manual review",
            "Ready for a manual agent",
            status="pending",
            suggested_engine="manual",
            files_or_modules=["docs/review.md"],
        ),
        _task(
            "task-blocked",
            "Blocked dependency",
            "Cannot proceed",
            status="blocked",
            files_or_modules=["src/blocker.py"],
        ),
        _task(
            "task-blocked-dependent",
            "Blocked dependent",
            "Depends on blocked work",
            depends_on=["task-blocked"],
            status="pending",
            files_or_modules=["src/blocked_dependent.py"],
        ),
        _task(
            "task-progress",
            "Progress dependency",
            "Still running",
            status="in_progress",
            files_or_modules=["src/progress.py"],
        ),
        _task(
            "task-progress-dependent",
            "Progress dependent",
            "Depends on in-progress work",
            depends_on=["task-progress"],
            status="pending",
            files_or_modules=["src/progress_dependent.py"],
        ),
    ]


def _task(
    task_id,
    title,
    description,
    *,
    depends_on=None,
    status="pending",
    suggested_engine="codex",
    files_or_modules=None,
    estimated_complexity="medium",
):
    return {
        "id": task_id,
        "title": title,
        "description": description,
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": suggested_engine,
        "depends_on": depends_on or [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": ["Task is complete"],
        "estimated_complexity": estimated_complexity,
        "status": status,
    }


def _implementation_brief():
    return {
        "id": "ib-queue",
        "source_brief_id": "sb-queue",
        "title": "Queue Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need a ready queue",
        "mvp_goal": "Expose ready tasks in the CLI",
        "product_surface": "CLI",
        "scope": ["Task queue"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Incorrect readiness"],
        "validation_plan": "Run task queue tests",
        "definition_of_done": ["CLI lists ready tasks"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
