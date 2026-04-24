import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters import RelayYamlExporter
from blueprint.exporters.export_validation import validate_rendered_export
from blueprint.exporters.relay import RelayExporter
from blueprint.store import Store, init_db


def test_relay_yaml_exporter_writes_same_payload_shape_as_relay_json(tmp_path):
    yaml_path = tmp_path / "relay.yaml"
    json_path = tmp_path / "relay.json"

    RelayYamlExporter().export(_execution_plan(), _implementation_brief(), str(yaml_path))
    RelayExporter().export(_execution_plan(), _implementation_brief(), str(json_path))

    yaml_payload = yaml.safe_load(yaml_path.read_text())
    json_payload = json.loads(json_path.read_text())

    assert yaml_path.suffix == ".yaml"
    assert yaml_payload == json_payload
    assert yaml_payload["schema_version"] == "blueprint.relay.v1"
    assert [milestone["name"] for milestone in yaml_payload["milestones"]] == [
        "Foundation",
        "Delivery",
    ]
    assert [task["id"] for task in yaml_payload["tasks"]] == [
        "task-setup",
        "task-api",
        "task-ui",
    ]


def test_export_render_relay_yaml_writes_yaml_file_and_records_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(cli, ["export", "render", plan_id, "--target", "relay-yaml"])

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-relay-yaml.yaml"
    assert output_path.exists()
    assert "Exported to:" in result.output

    payload = yaml.safe_load(output_path.read_text())
    assert payload["schema_version"] == "blueprint.relay.v1"
    assert len(payload["tasks"]) == 3

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "relay-yaml"
    assert records[0]["export_format"] == "yaml"
    assert records[0]["output_path"] == str(output_path)


def test_relay_yaml_validation_passes_for_rendered_export(tmp_path):
    output_path = tmp_path / "relay.yaml"
    plan = _execution_plan()
    brief = _implementation_brief()
    RelayYamlExporter().export(plan, brief, str(output_path))

    findings = validate_rendered_export(
        target="relay-yaml",
        artifact_path=output_path,
        execution_plan=plan,
        implementation_brief=brief,
    )

    assert findings == []


def test_relay_yaml_validation_fails_when_tasks_are_missing(tmp_path):
    output_path = tmp_path / "relay.yaml"
    plan = _execution_plan()
    brief = _implementation_brief()
    payload = RelayExporter().render_payload(plan, brief)
    payload["tasks"] = payload["tasks"][:-1]
    output_path.write_text(yaml.safe_dump(payload, sort_keys=False))

    findings = validate_rendered_export(
        target="relay-yaml",
        artifact_path=output_path,
        execution_plan=plan,
        implementation_brief=brief,
    )

    codes = [finding.code for finding in findings]
    assert "relay_yaml.task_count_mismatch" in codes
    assert "relay_yaml.missing_task" in codes
    assert any("task-ui" in finding.message for finding in findings)


def _execution_plan(include_tasks=True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "relay",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Delivery", "description": "Ship the feature"},
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
            "suggested_engine": "relay",
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
            "milestone": "Delivery",
            "owner_type": "agent",
            "suggested_engine": "relay",
            "depends_on": ["task-setup"],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
        {
            "id": "task-ui",
            "title": "Wire UI",
            "description": "Expose the command output",
            "milestone": "Delivery",
            "owner_type": "agent",
            "suggested_engine": "relay",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI renders API data"],
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
        "problem_statement": "Need a human-editable Relay task queue",
        "mvp_goal": "Expose Relay payloads as YAML",
        "product_surface": "CLI",
        "scope": ["Relay YAML export"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Reuse normalized Relay payloads",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Malformed YAML"],
        "validation_plan": "Run Relay YAML tests",
        "definition_of_done": ["Each task exports once"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
