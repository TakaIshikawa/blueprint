import csv
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.exporters.openproject_csv import OpenProjectCsvExporter
from blueprint.store import Store, init_db


def test_openproject_csv_exporter_writes_stable_headers_and_work_package_rows(tmp_path):
    output_path = tmp_path / "openproject.csv"

    OpenProjectCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert _read_headers(output_path) == OpenProjectCsvExporter.FIELDNAMES
    assert len(rows) == 5
    assert [row["Blueprint task ID"] for row in rows] == [
        "task-setup",
        "task-api",
        "task-ui",
        "task-docs",
        "task-skip",
    ]
    assert [row["Subject"] for row in rows] == [
        "Setup project",
        'Build API, "v2"',
        "Wire UI",
        "Write docs",
        "Skip legacy path",
    ]
    assert {row["Type"] for row in rows} == {"Task"}


def test_openproject_csv_exporter_formats_dependencies_and_milestones(tmp_path):
    output_path = tmp_path / "openproject.csv"

    OpenProjectCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert [row["Milestone/Version"] for row in rows] == [
        "Foundation",
        "Delivery",
        "Delivery",
        "Delivery",
        "Foundation",
    ]
    assert rows[0]["Predecessors"] == ""
    assert rows[1]["Predecessors"] == "task-setup"
    assert rows[2]["Predecessors"] == "task-setup, task-api"
    assert rows[3]["Predecessors"] == "task-api"


def test_openproject_csv_exporter_maps_fields_and_description_context(tmp_path):
    output_path = tmp_path / "openproject.csv"

    OpenProjectCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert [row["Status"] for row in rows] == [
        "New",
        "In progress",
        "On hold",
        "Closed",
        "Rejected",
    ]
    assert [row["Priority"] for row in rows] == ["Immediate", "High", "High", "Low", "Normal"]
    assert rows[1]["Assignee"] == "api-owner@example.com"
    assert rows[1]["Start date"] == "2026-04-30"
    assert rows[1]["Due date"] == "2026-05-01"
    assert rows[1]["Estimated time"] == "4h"
    assert rows[1]["Parent"] == "WP-100"
    assert rows[1]["Tags"] == (
        "blueprint-plan:plan-test, blueprint-brief:ib-test, codex, agent, "
        "Delivery, api,import, planning, csv|pipe"
    )

    description = rows[1]["Description"]
    assert 'Implement the command API with comma, quote " and pipe | handling' in description
    assert "Plan: plan-test" in description
    assert "Implementation brief: ib-test - Test Brief" in description
    assert "Task ID:\n- task-api" in description
    assert 'Acceptance Criteria:\n- API returns "quoted", comma-safe data' in description
    assert "Implementation Notes:\n- Preserve external Blueprint task IDs" in description
    assert "Files/Modules:\n- src/app.py\n- src/schema.py" in description


def test_openproject_csv_exporter_writes_empty_optional_fields(tmp_path):
    output_path = tmp_path / "openproject.csv"
    plan = _execution_plan()
    task = plan["tasks"][0]
    task["milestone"] = None
    task["owner_type"] = "agent"
    task["metadata"] = {}
    task["depends_on"] = []

    OpenProjectCsvExporter().export(plan, _implementation_brief(), str(output_path))

    row = _read_csv(output_path)[0]
    assert row["Assignee"] == ""
    assert row["Start date"] == ""
    assert row["Due date"] == ""
    assert row["Estimated time"] == ""
    assert row["Parent"] == ""
    assert row["Predecessors"] == ""
    assert row["Milestone/Version"] == ""


def test_openproject_csv_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "openproject-csv")

    assert result.passed
    assert result.findings == []


def test_openproject_csv_validation_reports_missing_duplicate_and_bad_values(tmp_path):
    output_path = tmp_path / "openproject.csv"
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=OpenProjectCsvExporter.FIELDNAMES)
        writer.writeheader()
        row = _row(task_id="task-setup")
        row["Status"] = "Mystery"
        row["Priority"] = "Maybe"
        writer.writerow(row)
        writer.writerow(_row(task_id="task-setup"))

    findings = validate_rendered_export(
        target="openproject-csv",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == [
        "openproject_csv.row_count_mismatch",
        "openproject_csv.missing_tasks",
        "openproject_csv.duplicate_task_id",
        "openproject_csv.unknown_status",
        "openproject_csv.unknown_priority",
    ]
    assert "task-api" in findings[1].message


def test_openproject_csv_validation_reports_exact_header_mismatch(tmp_path):
    output_path = tmp_path / "openproject.csv"
    reordered = [
        "Description",
        "Subject",
        *OpenProjectCsvExporter.FIELDNAMES[2:],
    ]
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=reordered)
        writer.writeheader()
        writer.writerow(_row(task_id="task-setup"))

    findings = validate_rendered_export(
        target="openproject-csv",
        artifact_path=output_path,
        execution_plan={**_execution_plan(include_tasks=False), "tasks": [_tasks()[0]]},
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == ["openproject_csv.header_mismatch"]


def test_export_run_openproject_csv_writes_file_and_records_csv_export(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "openproject-csv"])

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-openproject-csv.csv"
    assert output_path.exists()
    assert "Exported to:" in result.output

    rows = _read_csv(output_path)
    assert len(rows) == 5
    assert rows[1]["Blueprint task ID"] == "task-api"

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "openproject-csv"
    assert records[0]["export_format"] == "csv"
    assert records[0]["output_path"] == str(output_path)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _read_headers(path: Path) -> list[str]:
    with path.open(newline="") as f:
        return list(csv.reader(f))[0]


def _row(task_id: str) -> dict[str, str]:
    return {
        "Subject": f"Title {task_id}",
        "Description": f"Body {task_id}",
        "Type": "Task",
        "Status": "New",
        "Priority": "Normal",
        "Assignee": "",
        "Start date": "",
        "Due date": "",
        "Estimated time": "",
        "Parent": "",
        "Predecessors": "",
        "Tags": "blueprint-plan:plan-test",
        "Milestone/Version": "Delivery",
        "Blueprint task ID": task_id,
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
                "openproject_assignee": "api-owner@example.com",
                "openproject_start_date": "2026-04-30",
                "openproject_due_date": "2026-05-01",
                "openproject_estimated_time": "4h",
                "openproject_parent": "WP-100",
                "implementation_notes": ["Preserve external Blueprint task IDs"],
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
            "files_or_modules": ["docs/openproject.md"],
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
        "problem_statement": "Need an OpenProject CSV export",
        "mvp_goal": "Export tasks for OpenProject import",
        "product_surface": "CLI",
        "scope": ["OpenProject CSV exporter"],
        "non_goals": ["OpenProject API automation"],
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
