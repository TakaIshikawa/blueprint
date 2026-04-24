import json
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export
from blueprint.exporters.linear import LinearExporter
from blueprint.store import Store, init_db


def test_linear_exporter_writes_one_issue_per_task(tmp_path):
    output_path = tmp_path / "linear.json"

    result_path = LinearExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    assert result_path == str(output_path)
    payload = json.loads(output_path.read_text())
    assert payload["schema_version"] == "blueprint.linear.v1"
    assert payload["exporter"] == "linear"
    assert payload["plan"]["id"] == "plan-test"
    assert [issue["metadata"]["taskId"] for issue in payload["issues"]] == [
        "task-setup",
        "task-api",
    ]


def test_linear_exporter_renders_issue_fields_and_dependency_relations(tmp_path):
    output_path = tmp_path / "linear.json"

    LinearExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    payload = json.loads(output_path.read_text())
    setup, api = payload["issues"]

    assert setup["externalId"] == "plan-test:task:task-setup"
    assert setup["teamKey"] == "ENG"
    assert setup["labels"] == ["codex", "agent", "Foundation", "setup", "backend"]
    assert setup["priority"] == 4
    assert setup["estimate"] == 1

    assert api["externalId"] == "plan-test:task:task-api"
    assert api["teamKey"] == "PLAT"
    assert api["priority"] == 1
    assert api["estimate"] == 8
    assert api["relations"] == [
        {
            "type": "blocked_by",
            "externalId": "plan-test:task:task-setup",
            "taskId": "task-setup",
        }
    ]
    assert api["metadata"]["dependsOn"] == ["task-setup"]
    assert "## Acceptance Criteria" in api["description"]
    assert "- API returns data" in api["description"]
    assert "## Dependencies\n- `task-setup`" in api["description"]
    assert "## Files/Modules\n- src/app.py\n- src/schema.py" in api["description"]


def test_linear_exporter_output_is_deterministic(tmp_path):
    first_path = tmp_path / "first.json"
    second_path = tmp_path / "second.json"

    LinearExporter().export(_execution_plan(), _implementation_brief(), str(first_path))
    LinearExporter().export(_execution_plan(), _implementation_brief(), str(second_path))

    assert first_path.read_text() == second_path.read_text()


def test_linear_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "linear")

    assert result.passed
    assert result.findings == []


def test_linear_validation_fails_when_task_is_missing(monkeypatch):
    class BadLinearExporter:
        def get_extension(self) -> str:
            return ".json"

        def export(self, execution_plan, implementation_brief, output_path):
            Path(output_path).write_text(
                json.dumps(
                    {
                        "schema_version": "blueprint.linear.v1",
                        "exporter": "linear",
                        "plan": {"id": execution_plan["id"]},
                        "issues": [],
                    }
                )
            )
            return output_path

    monkeypatch.setattr(
        "blueprint.exporters.export_validation.create_exporter",
        lambda target: BadLinearExporter(),
    )

    result = validate_export(_execution_plan(), _implementation_brief(), "linear")

    assert not result.passed
    assert any(finding.code == "linear.task_count_mismatch" for finding in result.findings)
    assert any(
        finding.code == "linear.task_occurrence_mismatch"
        and "task-setup appears 0 times" in finding.message
        for finding in result.findings
    )


def test_export_run_linear_writes_file_and_records_export(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "linear"])

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-linear.json"
    assert output_path.exists()

    payload = json.loads(output_path.read_text())
    assert payload["issues"][0]["title"] == "Setup project"

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "linear"
    assert records[0]["export_format"] == "json"
    assert records[0]["output_path"] == str(output_path)


def test_export_validate_linear_supports_positional_target_json(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "validate", plan_id, "linear", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == {"target": "linear", "passed": True, "findings": []}


def _setup_store(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "blueprint.db"
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {db_path}
exports:
  output_dir: {tmp_path / "exports"}
"""
    )
    blueprint_config.reload_config()
    return init_db(str(db_path))


def _execution_plan(include_tasks: bool = True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "acme/widgets",
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
        "metadata": {"linear_team_key": "eng"},
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
            "metadata": {"labels": ["setup"], "components": ["backend"]},
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Delivery",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup"],
            "files_or_modules": ["src/app.py", "src/schema.py"],
            "acceptance_criteria": ["API returns data", "Schema validates payloads"],
            "estimated_complexity": "medium",
            "status": "pending",
            "metadata": {
                "linear_team_key": "plat",
                "linear_priority": "urgent",
                "linear_estimate": 8,
                "labels": ["api"],
                "tags": ["planning"],
            },
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
        "problem_statement": "Need a Linear issue export",
        "mvp_goal": "Export execution tasks for Linear import",
        "product_surface": "CLI",
        "scope": ["Linear issue exporter"],
        "non_goals": ["Linear API integration"],
        "assumptions": ["Import tooling can map external IDs"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": ["CLI export command"],
        "risks": ["Dependency relations must stay reconstructable"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Tasks export as Linear issue JSON"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
