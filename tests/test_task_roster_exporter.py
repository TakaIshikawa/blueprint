import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli

from blueprint.store import init_db


def test_task_roster_json_output_is_grouped_and_sorted(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(
        cli,
        ["task", "roster", "plan-test", "--format", "json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["plan_id"] == "plan-test"
    assert [(group["owner_type"], group["suggested_engine"]) for group in payload["groups"]] == [
        ("agent", "codex"),
        ("human", "manual"),
        ("unassigned", "codex"),
        ("unassigned", "unassigned"),
    ]
    assert [task["id"] for task in payload["groups"][0]["tasks"]] == [
        "task-api",
        "task-setup",
    ]
    assert payload["groups"][0]["tasks"][0] == {
        "id": "task-api",
        "title": "Build API",
        "status": "in_progress",
        "dependencies": ["task-setup"],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": ["API returns data"],
    }


def test_task_roster_markdown_groups_by_owner_and_engine(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(cli, ["task", "roster", "plan-test"])

    assert result.exit_code == 0, result.output
    assert "# Task Assignment Roster: plan-test" in result.output
    assert "## Owner: agent / Engine: codex" in result.output
    assert "### task-api - Build API" in result.output
    assert "- Status: in_progress" in result.output
    assert "- Dependencies: task-setup" in result.output
    assert "- Files or Modules: src/app.py" in result.output
    assert "  - API returns data" in result.output
    assert "## Owner: unassigned / Engine: codex" in result.output
    assert "## Owner: unassigned / Engine: unassigned" in result.output


def test_task_roster_output_writes_parent_directories(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())
    output_path = tmp_path / "exports" / "handoff" / "roster.json"

    result = CliRunner().invoke(
        cli,
        [
            "task",
            "roster",
            "plan-test",
            "--format",
            "json",
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert "Wrote task roster to:" in result.output
    payload = json.loads(output_path.read_text())
    assert payload["groups"][0]["owner_type"] == "agent"


def test_task_roster_missing_plan(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(cli, ["task", "roster", "plan-missing"])

    assert result.exit_code != 0
    assert "Execution plan not found: plan-missing" in result.output


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
        "test_strategy": "Run task roster tests",
        "handoff_prompt": "Build the plan",
        "status": "ready",
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
            "id": "task-unowned",
            "title": "Review copy",
            "description": "Review user-facing copy",
            "milestone": "Interface",
            "owner_type": None,
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["Copy is clear"],
            "estimated_complexity": "low",
            "status": "pending",
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
            "id": "task-ops",
            "title": "Schedule rollout",
            "description": "Coordinate release timing",
            "milestone": "Interface",
            "owner_type": "human",
            "suggested_engine": "manual",
            "depends_on": ["task-api"],
            "files_or_modules": None,
            "acceptance_criteria": ["Rollout owner is assigned"],
            "estimated_complexity": "low",
            "status": "pending",
        },
        {
            "id": "task-unassigned",
            "title": "Resolve ownership",
            "description": "Identify who should own the task",
            "milestone": "Interface",
            "owner_type": None,
            "suggested_engine": None,
            "depends_on": [],
            "files_or_modules": [],
            "acceptance_criteria": ["Owner and engine are selected"],
            "estimated_complexity": "low",
            "status": "pending",
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
        "risks": ["Incorrect rosters"],
        "validation_plan": "Run task roster tests",
        "definition_of_done": ["CLI renders a focused roster"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
