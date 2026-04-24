import json
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_rendered_export
from blueprint.exporters.vscode_tasks import VSCodeTasksExporter
from blueprint.store import Store, init_db


def test_vscode_tasks_exporter_writes_valid_tasks_json(tmp_path):
    output_path = tmp_path / "tasks.json"

    VSCodeTasksExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    payload = json.loads(output_path.read_text())
    assert payload["version"] == "2.0.0"
    assert len(payload["tasks"]) == 3
    assert [task["type"] for task in payload["tasks"]] == ["shell", "shell", "shell"]


def test_vscode_tasks_exporter_maps_commands_and_dependencies(tmp_path):
    output_path = tmp_path / "tasks.json"

    VSCodeTasksExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    tasks = {
        task["label"]: task
        for task in json.loads(output_path.read_text())["tasks"]
    }
    assert tasks["task-setup: Setup project"]["command"] == "poetry install"
    assert tasks["task-api: Build API"]["command"] == "echo 'Build API'"
    assert tasks["task-api: Build API"]["dependsOn"] == [
        "task-setup: Setup project",
        "task-schema: Build schema",
    ]
    assert "dependsOn" not in tasks["task-schema: Build schema"]


def test_vscode_tasks_export_validation_passes_for_rendered_export(tmp_path):
    output_path = tmp_path / "tasks.json"
    plan = _execution_plan()

    VSCodeTasksExporter().export(plan, _implementation_brief(), str(output_path))

    findings = validate_rendered_export(
        target="vscode-tasks",
        artifact_path=output_path,
        execution_plan=plan,
        implementation_brief=_implementation_brief(),
    )

    assert findings == []


def test_export_preview_run_and_validate_support_vscode_tasks(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "blueprint.db"
    export_dir = tmp_path / "exports"
    Path(".blueprint.yaml").write_text(
        f"""
database:
  path: {db_path}
exports:
  output_dir: {export_dir}
"""
    )
    blueprint_config.reload_config()

    store = init_db(str(db_path))
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    preview = CliRunner().invoke(cli, ["export", "preview", plan_id, "--target", "vscode-tasks"])
    assert preview.exit_code == 0, preview.output
    preview_payload = json.loads(preview.output)
    assert preview_payload["tasks"][0]["label"] == "task-setup: Setup project"

    run = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "vscode-tasks"])
    assert run.exit_code == 0, run.output
    output_path = export_dir / f"{plan_id}-vscode-tasks.json"
    assert output_path.exists()
    assert "Exported to:" in run.output

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "vscode-tasks"
    assert records[0]["export_format"] == "json"
    assert records[0]["output_path"] == str(output_path)

    validate = CliRunner().invoke(cli, ["export", "validate", plan_id, "--target", "vscode-tasks"])
    assert validate.exit_code == 0, validate.output
    assert "Validation passed for vscode-tasks" in validate.output


def _execution_plan(include_tasks=True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation", "description": "Set up the project"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
    if include_tasks:
        plan["tasks"] = _tasks()
    return plan


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
            "status": "pending",
            "metadata": {"command": "poetry install"},
        },
        {
            "id": "task-schema",
            "title": "Build schema",
            "description": "Create persistence schema",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/schema.py"],
            "acceptance_criteria": ["Schema validates payloads"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup", "task-schema", "task-missing"],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
            "estimated_complexity": "medium",
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
        "problem_statement": "Need VS Code tasks for execution plans",
        "mvp_goal": "Open execution plans in implementation workspaces",
        "product_surface": "CLI",
        "scope": ["VS Code tasks exporter"],
        "non_goals": ["VS Code extension integration"],
        "assumptions": ["Developers use tasks.json"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Invalid task JSON"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Tasks export as VS Code shell tasks"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
