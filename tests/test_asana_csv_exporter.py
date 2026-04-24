import csv
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.asana_csv import AsanaCsvExporter
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.store import Store, init_db


def test_asana_csv_exporter_writes_one_row_per_task_with_required_headers(tmp_path):
    output_path = tmp_path / "asana.csv"

    AsanaCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert _read_headers(output_path) == AsanaCsvExporter.FIELDNAMES
    assert len(rows) == 3
    assert [row["Name"] for row in rows] == [
        "Setup project",
        "Build API",
        "Wire UI",
    ]
    assert [row["Section/Column"] for row in rows] == ["Foundation", "Delivery", "Delivery"]


def test_asana_csv_exporter_maps_notes_assignee_tags_and_dependencies(tmp_path):
    output_path = tmp_path / "asana.csv"

    AsanaCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert rows[0]["Assignee"] == ""
    assert rows[1]["Assignee"] == "api-owner@example.com"
    assert rows[1]["Due Date"] == "2026-05-01"
    assert rows[1]["Dependencies"] == "task-setup"
    assert rows[2]["Assignee"] == "Design Team"
    assert rows[2]["Dependencies"] == "task-setup, task-api"
    assert rows[1]["Tags"] == (
        "blueprint-plan:plan-test, blueprint-brief:ib-test, codex, agent, "
        "Delivery, api, planning"
    )

    notes = rows[1]["Notes"]
    assert "Implement the command API" in notes
    assert "Plan: plan-test" in notes
    assert "Implementation brief: ib-test - Test Brief" in notes
    assert "Task ID:\n- task-api" in notes
    assert "Acceptance Criteria:\n- API returns data\n- Schema validates payloads" in notes
    assert "Files/Modules:\n- src/app.py\n- src/schema.py" in notes


def test_asana_csv_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "asana-csv")

    assert result.passed
    assert result.findings == []


def test_asana_csv_rendered_validation_fails_for_missing_required_header(tmp_path):
    output_path = tmp_path / "asana.csv"
    output_path.write_text(
        "Name,Notes,Section/Column,Assignee,Due Date,Tags\n"
        "Setup project,Description,Foundation,,,setup\n"
        "Build API,Description,Delivery,api-owner@example.com,,api\n"
        "Wire UI,Description,Delivery,Design Team,,ui\n"
    )

    findings = validate_rendered_export(
        target="asana-csv",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == ["asana_csv.missing_column"]
    assert "Dependencies" in findings[0].message


def test_asana_csv_rendered_validation_fails_for_missing_task_rows(tmp_path):
    output_path = tmp_path / "asana.csv"
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=AsanaCsvExporter.FIELDNAMES)
        writer.writeheader()
        writer.writerow(
            {
                "Name": "Only task",
                "Notes": "Description",
                "Section/Column": "Foundation",
                "Assignee": "",
                "Due Date": "",
                "Tags": "blueprint-plan:plan-test",
                "Dependencies": "",
            }
        )

    findings = validate_rendered_export(
        target="asana-csv",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == ["asana_csv.row_count_mismatch"]


def test_export_render_asana_csv_writes_file_and_records_csv_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(cli, ["export", "render", plan_id, "asana-csv"])

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-asana-csv.csv"
    assert output_path.exists()
    assert "Exported to:" in result.output

    rows = _read_csv(output_path)
    assert len(rows) == 3
    assert rows[0]["Name"] == "Setup project"

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "asana-csv"
    assert records[0]["export_format"] == "csv"
    assert records[0]["output_path"] == str(output_path)


def test_export_preview_and_validate_support_asana_csv(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "blueprint.db"
    Path(".blueprint.yaml").write_text(
        f"""
database:
  path: {db_path}
"""
    )
    blueprint_config.reload_config()

    store = init_db(str(db_path))
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    preview = CliRunner().invoke(cli, ["export", "preview", plan_id, "asana-csv"])
    assert preview.exit_code == 0, preview.output
    assert "Name,Notes,Section/Column,Assignee,Due Date,Tags,Dependencies" in preview.output
    assert "Build API" in preview.output

    validation = CliRunner().invoke(cli, ["export", "validate", plan_id, "--target", "asana-csv"])
    assert validation.exit_code == 0, validation.output
    assert "Validation passed for asana-csv" in validation.output


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _read_headers(path: Path) -> list[str]:
    with path.open(newline="") as f:
        return list(csv.reader(f))[0]


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
                "asana_assignee": "api-owner@example.com",
                "asana_due_date": "2026-05-01",
                "labels": ["api"],
                "tags": ["planning"],
            },
        },
        {
            "id": "task-ui",
            "title": "Wire UI",
            "description": "Connect the web UI",
            "milestone": "Delivery",
            "owner_type": "Design Team",
            "suggested_engine": "codex",
            "depends_on": ["task-setup", "task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI renders API data"],
            "estimated_complexity": "high",
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
        "problem_statement": "Need an Asana CSV task export",
        "mvp_goal": "Export tasks for Asana import",
        "product_surface": "CLI",
        "scope": ["Asana CSV exporter"],
        "non_goals": ["Asana API integration"],
        "assumptions": ["Asana import parses standard CSV quoting"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Invalid CSV escaping"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Tasks export as Asana rows"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
