import csv
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.clickup_csv import ClickUpCsvExporter
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.store import Store, init_db


def test_clickup_csv_exporter_writes_stable_headers_and_task_rows(tmp_path):
    output_path = tmp_path / "clickup.csv"

    ClickUpCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert _read_headers(output_path) == ClickUpCsvExporter.FIELDNAMES
    assert len(rows) == 5
    assert [row["Task ID"] for row in rows] == [
        "task-setup",
        "task-api",
        "task-ui",
        "task-docs",
        "task-skip",
    ]
    assert [row["Task Name"] for row in rows] == [
        "Setup project",
        'Build API, "v2"',
        "Wire UI",
        "Write docs",
        "Skip legacy path",
    ]


def test_clickup_csv_exporter_maps_status_priority_dependencies_and_context(tmp_path):
    output_path = tmp_path / "clickup.csv"

    ClickUpCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert [row["Status"] for row in rows] == [
        "To Do",
        "In Progress",
        "Blocked",
        "Complete",
        "Canceled",
    ]
    assert [row["Priority"] for row in rows] == ["Urgent", "High", "High", "Low", "Normal"]
    assert rows[1]["Assignee"] == "api-owner@example.com"
    assert rows[1]["Due Date"] == "2026-05-01"
    assert rows[1]["List Name"] == "Delivery"
    assert rows[1]["Dependencies"] == "Setup project (task-setup)"
    assert rows[2]["Dependencies"] == 'Setup project (task-setup)\nBuild API, "v2" (task-api)'
    assert rows[1]["Acceptance Criteria"] == 'API returns "quoted", comma-safe data\nSchema validates pipes | and payloads'
    assert rows[1]["Files"] == "src/app.py\nsrc/schema.py"
    assert rows[1]["Tags"] == (
        "blueprint-plan:plan-test | blueprint-brief:ib-test | codex | agent | "
        "Delivery | api,import | planning | csv|pipe"
    )

    description = rows[1]["Task Description"]
    assert 'Implement the command API with comma, quote " and pipe | handling' in description
    assert "Plan: plan-test" in description
    assert "Implementation brief: ib-test - Test Brief" in description
    assert "Task ID:\n- task-api" in description
    assert 'Acceptance Criteria:\n- API returns "quoted", comma-safe data' in description


def test_clickup_csv_exporter_round_trips_csv_escaped_text(tmp_path):
    output_path = tmp_path / "clickup.csv"

    ClickUpCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    raw = output_path.read_text()
    rows = _read_csv(output_path)
    assert 'Build API, ""v2""' in raw
    assert '"Implement the command API with comma, quote "" and pipe | handling' in raw
    assert rows[1]["Task Name"] == 'Build API, "v2"'
    assert rows[1]["Task Description"].startswith(
        'Implement the command API with comma, quote " and pipe | handling'
    )
    assert "Schema validates pipes | and payloads" in rows[1]["Acceptance Criteria"]


def test_clickup_csv_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "clickup-csv")

    assert result.passed
    assert result.findings == []


def test_clickup_csv_validation_reports_useful_findings_for_malformed_artifact(tmp_path):
    output_path = tmp_path / "clickup.csv"
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ClickUpCsvExporter.FIELDNAMES)
        writer.writeheader()
        row = _row(task_id="task-setup")
        row["Status"] = "Mystery"
        row["Priority"] = "Maybe"
        writer.writerow(row)
        writer.writerow(_row(task_id="task-setup"))

    findings = validate_rendered_export(
        target="clickup-csv",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == [
        "clickup_csv.row_count_mismatch",
        "clickup_csv.missing_tasks",
        "clickup_csv.duplicate_task_id",
        "clickup_csv.unknown_status",
        "clickup_csv.unknown_priority",
    ]
    assert "task-api" in findings[1].message


def test_clickup_csv_validation_reports_missing_columns(tmp_path):
    output_path = tmp_path / "clickup.csv"
    output_path.write_text(
        "Task Name,Task Description,Status,Priority,Task ID\n"
        "Setup project,Description,To Do,Normal,task-setup\n"
    )

    findings = validate_rendered_export(
        target="clickup-csv",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert findings[0].code == "clickup_csv.missing_column"
    assert "Dependencies" in findings[0].message
    assert "Files" in findings[0].message


def test_export_run_clickup_csv_writes_file_and_records_csv_export(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "clickup-csv"])

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-clickup-csv.csv"
    assert output_path.exists()
    assert "Exported to:" in result.output

    rows = _read_csv(output_path)
    assert len(rows) == 5
    assert rows[1]["Task ID"] == "task-api"

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "clickup-csv"
    assert records[0]["export_format"] == "csv"
    assert records[0]["output_path"] == str(output_path)


def test_export_validate_supports_clickup_csv(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    validation = CliRunner().invoke(cli, ["export", "validate", plan_id, "clickup-csv"])

    assert validation.exit_code == 0, validation.output
    assert "Validation passed for clickup-csv" in validation.output


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _read_headers(path: Path) -> list[str]:
    with path.open(newline="") as f:
        return list(csv.reader(f))[0]


def _row(task_id: str) -> dict[str, str]:
    return {
        "Task Name": f"Title {task_id}",
        "Task Description": f"Body {task_id}",
        "Status": "To Do",
        "Priority": "Normal",
        "Assignee": "",
        "Due Date": "",
        "Tags": "blueprint-plan:plan-test",
        "Dependencies": "",
        "List Name": "Delivery",
        "Task ID": task_id,
        "Acceptance Criteria": "Done",
        "Files": "src/app.py",
    }


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
            "metadata": {"priority": "P0", "labels": ["setup"], "components": ["backend"]},
        },
        {
            "id": "task-api",
            "title": 'Build API, "v2"',
            "description": 'Implement the command API with comma, quote " and pipe | handling',
            "milestone": "Delivery",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup"],
            "files_or_modules": ["src/app.py", "src/schema.py"],
            "acceptance_criteria": [
                'API returns "quoted", comma-safe data',
                "Schema validates pipes | and payloads",
            ],
            "estimated_complexity": "medium",
            "status": "in_progress",
            "metadata": {
                "clickup_assignee": "api-owner@example.com",
                "clickup_due_date": "2026-05-01",
                "labels": ["api,import"],
                "tags": ["planning"],
                "components": ["csv|pipe"],
                "risk": "high",
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
            "status": "blocked",
            "metadata": {},
        },
        {
            "id": "task-docs",
            "title": "Write docs",
            "description": "Document the import path",
            "milestone": "Delivery",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["docs/clickup.md"],
            "acceptance_criteria": ["Docs explain CSV import"],
            "estimated_complexity": "low",
            "status": "completed",
            "metadata": {},
        },
        {
            "id": "task-skip",
            "title": "Skip legacy path",
            "description": "Skip a superseded path",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": [],
            "acceptance_criteria": ["No legacy export is generated"],
            "estimated_complexity": "medium",
            "status": "skipped",
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
        "problem_statement": "Need a ClickUp CSV export",
        "mvp_goal": "Export tasks for ClickUp import",
        "product_surface": "CLI",
        "scope": ["ClickUp CSV exporter"],
        "non_goals": ["ClickUp API automation"],
        "assumptions": ["CSV consumers can import task rows"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Plan and task dictionaries",
        "integration_points": ["CLI export command"],
        "risks": ["Task IDs must remain stable"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Tasks export as CSV rows"],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
