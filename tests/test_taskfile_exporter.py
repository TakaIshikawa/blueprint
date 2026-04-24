from pathlib import Path

import yaml
from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_rendered_export
from blueprint.exporters.taskfile import TaskfileExporter
from blueprint.store import Store, init_db


def test_taskfile_exporter_writes_parseable_taskfile_yaml(tmp_path):
    output_path = tmp_path / "Taskfile.yml"

    TaskfileExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    payload = yaml.safe_load(output_path.read_text())
    assert payload["version"] == "3"
    assert set(payload["tasks"]) == {
        "default",
        "foundation:task-setup",
        "foundation:task-schema",
        "delivery:task-api",
    }


def test_taskfile_exporter_maps_dependencies_to_generated_task_names(tmp_path):
    output_path = tmp_path / "Taskfile.yml"

    TaskfileExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    tasks = yaml.safe_load(output_path.read_text())["tasks"]
    assert tasks["delivery:task-api"]["deps"] == [
        "foundation:task-setup",
        "foundation:task-schema",
    ]
    assert "task-setup" not in tasks["delivery:task-api"]["deps"]
    assert "task-missing" not in tasks["delivery:task-api"]["deps"]


def test_taskfile_exporter_uses_milestone_namespaces_and_placeholder_commands(tmp_path):
    output_path = tmp_path / "Taskfile.yml"

    TaskfileExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    tasks = yaml.safe_load(output_path.read_text())["tasks"]
    assert "foundation:task-setup" in tasks
    assert "delivery:task-api" in tasks
    assert tasks["foundation:task-setup"]["desc"] == "task-setup: Setup project"
    assert "Description: Create the baseline project structure" in tasks[
        "foundation:task-setup"
    ]["cmds"][1]
    assert "Acceptance criteria:" in tasks["foundation:task-setup"]["cmds"][2]
    assert "task-api -> task delivery:task-api" in tasks["default"]["cmds"][3]


def test_taskfile_export_validation_passes_for_rendered_export(tmp_path):
    output_path = tmp_path / "Taskfile.yml"
    plan = _execution_plan()

    TaskfileExporter().export(plan, _implementation_brief(), str(output_path))

    findings = validate_rendered_export(
        target="taskfile",
        artifact_path=output_path,
        execution_plan=plan,
        implementation_brief=_implementation_brief(),
    )

    assert findings == []


def test_taskfile_export_validation_rejects_unknown_dependency(tmp_path):
    output_path = tmp_path / "Taskfile.yml"
    TaskfileExporter().export(_execution_plan(), _implementation_brief(), str(output_path))
    payload = yaml.safe_load(output_path.read_text())
    payload["tasks"]["delivery:task-api"]["deps"].append("missing:task")
    output_path.write_text(yaml.safe_dump(payload, sort_keys=False))

    findings = validate_rendered_export(
        target="taskfile",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert "taskfile.unknown_dependency" in [finding.code for finding in findings]


def test_export_preview_run_and_validate_support_taskfile(tmp_path, monkeypatch):
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

    preview = CliRunner().invoke(cli, ["export", "preview", plan_id, "--target", "taskfile"])
    assert preview.exit_code == 0, preview.output
    preview_payload = yaml.safe_load(preview.output)
    assert "foundation:task-setup" in preview_payload["tasks"]

    run = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "taskfile"])
    assert run.exit_code == 0, run.output
    output_path = export_dir / f"{plan_id}-taskfile.yml"
    assert output_path.exists()
    assert "Exported to:" in run.output

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "taskfile"
    assert records[0]["export_format"] == "yaml"
    assert records[0]["output_path"] == str(output_path)

    validate = CliRunner().invoke(cli, ["export", "validate", plan_id, "--target", "taskfile"])
    assert validate.exit_code == 0, validate.output
    assert "Validation passed for taskfile" in validate.output


def _execution_plan(include_tasks=True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Delivery", "description": "Ship the API"},
        ],
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
            "milestone": "Delivery",
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
        "problem_statement": "Need Taskfile tasks for execution plans",
        "mvp_goal": "Open execution plans in implementation workspaces",
        "product_surface": "CLI",
        "scope": ["Taskfile exporter"],
        "non_goals": ["go-task runtime integration"],
        "assumptions": ["Developers use Taskfile.yml"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Invalid Taskfile YAML"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Tasks export as Taskfile tasks"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
