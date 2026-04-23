import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.task_handoff import TaskHandoffExporter
from blueprint.store import init_db
from blueprint.store.models import ExecutionTaskModel


def test_task_handoff_markdown_output(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(cli, ["task", "handoff", "task-api"])

    assert result.exit_code == 0, result.output
    assert "# Build API" in result.output
    assert "- Task ID: `task-api`" in result.output
    assert "- Status: in_progress" in result.output
    assert "| `task-setup` | Setup project | completed |" in result.output
    assert "- src/app.py" in result.output
    assert "- API returns data" in result.output
    assert "## Implementation Brief Context" in result.output
    assert "- MVP Goal: Expose tasks in the CLI" in result.output
    assert "## Plan Test Strategy\nRun task handoff tests" in result.output
    assert "## Handoff Prompt\nBuild the plan" in result.output


def test_task_handoff_json_output(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(cli, ["task", "handoff", "task-api", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["task"]["id"] == "task-api"
    assert payload["dependency_tasks"][0]["id"] == "task-setup"
    assert payload["dependency_tasks"][0]["status"] == "completed"
    assert payload["plan"] == {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "status": "ready",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "test_strategy": "Run task handoff tests",
        "handoff_prompt": "Build the plan",
    }
    assert payload["brief_summary"]["id"] == "ib-test"
    assert payload["brief_summary"]["mvp_goal"] == "Expose tasks in the CLI"


def test_task_handoff_missing_task(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(cli, ["task", "handoff", "task-missing"])

    assert result.exit_code != 0
    assert "Execution task not found: task-missing" in result.output


def test_task_handoff_handles_missing_linked_plan_and_brief(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    with store.get_session() as session:
        session.add(
            ExecutionTaskModel(
                id="task-orphan",
                execution_plan_id="plan-missing",
                title="Orphan task",
                description="Render without linked context",
                milestone="Loose",
                owner_type="agent",
                suggested_engine="codex",
                depends_on=["task-absent"],
                files_or_modules=["src/orphan.py"],
                acceptance_criteria=["Handoff still renders"],
                estimated_complexity="low",
                status="pending",
            )
        )
        session.commit()

    result = CliRunner().invoke(cli, ["task", "handoff", "task-orphan"])

    assert result.exit_code == 0, result.output
    assert "# Orphan task" in result.output
    assert "- Plan ID: `plan-missing`" in result.output
    assert "| `task-absent` | N/A | missing |" in result.output
    assert "- Brief ID: `N/A`" in result.output
    assert "## Plan Test Strategy\nN/A" in result.output
    assert "## Handoff Prompt" not in result.output

    payload = TaskHandoffExporter().render_json(
        {
            "id": "task-orphan",
            "execution_plan_id": "plan-missing",
            "title": "Orphan task",
            "description": "Render without linked context",
            "depends_on": ["task-absent"],
            "acceptance_criteria": ["Handoff still renders"],
        },
        [],
        None,
        None,
    )
    assert payload["plan"] is None
    assert payload["brief_summary"] is None


def test_task_handoff_writes_output_file(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())
    output_path = tmp_path / "handoffs" / "task-api.md"

    result = CliRunner().invoke(
        cli,
        ["task", "handoff", "task-api", "--output", str(output_path)],
    )

    assert result.exit_code == 0, result.output
    assert "Wrote task handoff to:" in result.output
    assert output_path.exists()
    assert "# Build API" in output_path.read_text()


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
        "test_strategy": "Run task handoff tests",
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
        "risks": ["Incorrect handoffs"],
        "validation_plan": "Run task handoff tests",
        "definition_of_done": ["CLI renders a focused handoff"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
