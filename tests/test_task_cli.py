from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.store import Store, init_db


def test_store_get_list_and_update_execution_tasks(tmp_path):
    db_path = tmp_path / "blueprint.db"
    store = init_db(str(db_path))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    task = store.get_execution_task("task-api")
    assert task["id"] == "task-api"
    assert task["execution_plan_id"] == "plan-test"
    assert task["milestone"] == "Foundation"
    assert task["suggested_engine"] == "codex"
    assert task["depends_on"] == ["task-setup"]
    assert task["files_or_modules"] == ["src/app.py"]
    assert task["acceptance_criteria"] == ["API returns data"]

    blocked_tasks = store.list_execution_tasks(plan_id="plan-test", status="blocked")
    assert [task["id"] for task in blocked_tasks] == ["task-ui"]

    foundation_tasks = store.list_execution_tasks(
        plan_id="plan-test",
        milestone="Foundation",
    )
    assert [task["id"] for task in foundation_tasks] == ["task-setup", "task-api"]

    assert store.update_execution_task_status("task-api", "completed") is True
    assert store.get_execution_task("task-api")["status"] == "completed"
    assert (
        store.update_execution_task_status(
            "task-api",
            "blocked",
            blocked_reason="Waiting for schema approval",
        )
        is True
    )
    blocked_task = store.get_execution_task("task-api")
    assert blocked_task["status"] == "blocked"
    assert blocked_task["blocked_reason"] == "Waiting for schema approval"
    assert blocked_task["metadata"] == {
        "blocked_reason": "Waiting for schema approval",
    }
    assert store.update_execution_task_status("task-missing", "completed") is False
    assert store.get_execution_task("task-missing") is None


def test_task_list_cli_shows_task_metadata(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(cli, ["task", "list", "--plan-id", "plan-test"])

    assert result.exit_code == 0, result.output
    assert "task-api" in result.output
    assert "Foundation" in result.output
    assert "codex" in result.output
    assert "Dependencies: task-setup" in result.output
    assert "Files:        src/app.py" in result.output
    assert "API returns data" in result.output
    assert "Total: 3 tasks" in result.output


def test_task_list_cli_filters_by_status_and_milestone(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(
        cli,
        [
            "task",
            "list",
            "--plan-id",
            "plan-test",
            "--status",
            "blocked",
            "--milestone",
            "Interface",
        ],
    )

    assert result.exit_code == 0, result.output
    assert "task-ui" in result.output
    assert "Build API" not in result.output
    assert "Total: 1 tasks" in result.output


def test_task_inspect_cli_shows_task_details(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(cli, ["task", "inspect", "task-api"])

    assert result.exit_code == 0, result.output
    assert "Execution Task: task-api" in result.output
    assert "Plan:            plan-test" in result.output
    assert "Milestone:       Foundation" in result.output
    assert "Engine:          codex" in result.output
    assert "Dependencies: task-setup" in result.output
    assert "Files:        src/app.py" in result.output
    assert "API returns data" in result.output


def test_task_update_cli_changes_status(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(
        cli,
        ["task", "update", "task-api", "--status", "completed"],
    )

    assert result.exit_code == 0, result.output
    assert "Updated task task-api status to completed" in result.output
    assert Store(str(tmp_path / "blueprint.db")).get_execution_task("task-api")[
        "status"
    ] == "completed"


def test_task_update_cli_sets_blocked_reason(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(
        cli,
        [
            "task",
            "update",
            "task-api",
            "--status",
            "blocked",
            "--blocked-reason",
            "Waiting for credentials",
        ],
    )

    task = Store(str(tmp_path / "blueprint.db")).get_execution_task("task-api")
    assert result.exit_code == 0, result.output
    assert "Blocked reason: Waiting for credentials" in result.output
    assert task["status"] == "blocked"
    assert task["blocked_reason"] == "Waiting for credentials"


def test_task_update_cli_rejects_invalid_status(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)

    result = CliRunner().invoke(
        cli,
        ["task", "update", "task-api", "--status", "done"],
    )

    assert result.exit_code != 0
    assert "Invalid value for '--status'" in result.output


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


def _execution_plan():
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Interface", "description": "Build the user-facing flow"},
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
        {
            "id": "task-setup",
            "title": "Setup project",
            "description": "Create the baseline project structure",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["pyproject.toml"],
            "acceptance_criteria": ["Project installs"],
            "estimated_complexity": "low",
            "status": "completed",
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup"],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
            "estimated_complexity": "medium",
            "status": "in_progress",
        },
        {
            "id": "task-ui",
            "title": "Build UI",
            "description": "Render the interface",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI displays API data"],
            "estimated_complexity": "medium",
            "status": "blocked",
        },
    ]


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
        "risks": ["Incorrect status updates"],
        "validation_plan": "Run task CLI tests",
        "definition_of_done": ["CLI lists, inspects, and updates tasks"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
