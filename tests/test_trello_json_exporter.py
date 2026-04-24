import json
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.exporters.trello_json import TrelloJsonExporter
from blueprint.store import Store, init_db


def test_trello_json_exporter_renders_board_lists_and_cards(tmp_path):
    output_path = tmp_path / "trello.json"

    result_path = TrelloJsonExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    assert result_path == str(output_path)
    payload = json.loads(output_path.read_text())
    assert payload["schema_version"] == "blueprint.trello.v1"
    assert payload["exporter"] == "trello-json"
    assert payload["board"]["metadata"]["planId"] == "plan-test"
    assert [list_item["name"] for list_item in payload["lists"]] == [
        "Foundation",
        "Delivery",
    ]
    assert [card["metadata"]["taskId"] for card in payload["cards"]] == [
        "task-api",
        "task-setup",
    ]


def test_trello_json_exporter_writes_every_task_exactly_once(tmp_path):
    output_path = tmp_path / "trello.json"

    TrelloJsonExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    payload = json.loads(output_path.read_text())
    task_ids = [card["metadata"]["taskId"] for card in payload["cards"]]
    assert sorted(task_ids) == sorted(task["id"] for task in _tasks())
    assert len(task_ids) == len(set(task_ids))


def test_trello_json_exporter_renders_labels_checklists_and_dependencies(tmp_path):
    output_path = tmp_path / "trello.json"

    TrelloJsonExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    payload = json.loads(output_path.read_text())
    api_card = next(card for card in payload["cards"] if card["metadata"]["taskId"] == "task-api")
    label_names = {label["name"] for label in payload["labels"]}
    assert {
        "suggested_engine: codex",
        "owner_type: agent",
        "complexity: medium",
    }.issubset(label_names)
    assert api_card["labels"] == [
        "suggested_engine: codex",
        "owner_type: agent",
        "complexity: medium",
    ]
    assert "`task-setup` - Setup project" in api_card["desc"]
    assert api_card["checklists"] == [
        {
            "id": "checklist-plan-test-task-api-acceptance",
            "name": "Acceptance Criteria",
            "items": [
                {"name": "API returns data", "checked": False},
                {"name": "Schema validates payloads", "checked": False},
            ],
        }
    ]


def test_trello_json_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "trello-json")

    assert result.passed
    assert result.findings == []


def test_trello_json_validation_catches_invalid_json(tmp_path):
    artifact_path = tmp_path / "trello.json"
    artifact_path.write_text("{not json")

    findings = validate_rendered_export(
        target="trello-json",
        artifact_path=artifact_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert any(finding.code == "trello_json.invalid_json" for finding in findings)


def test_trello_json_validation_catches_missing_lists_and_cards(tmp_path):
    artifact_path = tmp_path / "trello.json"
    artifact_path.write_text(
        json.dumps(
            {
                "schema_version": "blueprint.trello.v1",
                "exporter": "trello-json",
                "board": {"id": "board-plan-test", "name": "Test Brief"},
                "lists": [
                    {
                        "id": "list-foundation",
                        "name": "Foundation",
                        "source": "milestone",
                        "cards": [],
                    }
                ],
                "labels": [],
                "cards": [],
            }
        )
    )

    findings = validate_rendered_export(
        target="trello-json",
        artifact_path=artifact_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert any(finding.code == "trello_json.card_count_mismatch" for finding in findings)
    assert any(finding.code == "trello_json.missing_list" for finding in findings)
    assert any(
        finding.code == "trello_json.task_occurrence_mismatch"
        and "task-api appears 0 times" in finding.message
        for finding in findings
    )


def test_trello_json_validation_catches_checklist_mismatch(tmp_path):
    output_path = tmp_path / "trello.json"
    TrelloJsonExporter().export(_execution_plan(), _implementation_brief(), str(output_path))
    payload = json.loads(output_path.read_text())
    payload["cards"][0]["checklists"][0]["items"] = []
    output_path.write_text(json.dumps(payload))

    findings = validate_rendered_export(
        target="trello-json",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert any(
        finding.code == "trello_json.card.acceptance_criteria_mismatch"
        for finding in findings
    )


def test_export_render_trello_json_writes_file_and_records_export(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "render", plan_id, "trello-json"])

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-trello-json.json"
    assert output_path.exists()

    payload = json.loads(output_path.read_text())
    assert payload["cards"][0]["metadata"]["taskId"] == "task-api"

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "trello-json"
    assert records[0]["export_format"] == "json"
    assert records[0]["output_path"] == str(output_path)


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
            "metadata": {},
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
            "metadata": {},
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
        "problem_statement": "Need a Trello board export",
        "mvp_goal": "Export execution tasks for Trello import",
        "product_surface": "CLI",
        "scope": ["Trello JSON exporter"],
        "non_goals": ["Trello API integration"],
        "assumptions": ["Import tooling can map JSON fields"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": ["CLI export command"],
        "risks": ["Dependencies must stay visible"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Tasks export as Trello cards"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
