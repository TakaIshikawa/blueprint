import csv
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.exporters.registry import create_exporter
from blueprint.exporters.youtrack_csv import YouTrackCsvExporter
from blueprint.store import Store, init_db


def test_create_exporter_returns_youtrack_csv_exporter():
    exporter = create_exporter("youtrack-csv")

    assert isinstance(exporter, YouTrackCsvExporter)
    assert exporter.get_format() == "csv"
    assert exporter.get_extension() == ".csv"


def test_youtrack_csv_exporter_writes_stable_headers_and_task_rows(tmp_path):
    output_path = tmp_path / "youtrack.csv"

    YouTrackCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert _read_headers(output_path) == YouTrackCsvExporter.FIELDNAMES
    assert len(rows) == 5
    assert [row["External ID"] for row in rows] == [
        "task-setup",
        "task-api",
        "task-ui",
        "task-docs",
        "task-skip",
    ]
    assert [row["Summary"] for row in rows] == [
        "Setup project",
        'Build API, "v2"',
        "Wire UI",
        "Write docs",
        "Skip legacy path",
    ]


def test_youtrack_csv_exporter_maps_metadata_status_tags_and_context(tmp_path):
    output_path = tmp_path / "youtrack.csv"

    YouTrackCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert [row["State"] for row in rows] == [
        "Submitted",
        "In Progress",
        "Open",
        "Fixed",
        "Won't fix",
    ]
    assert [row["Priority"] for row in rows] == [
        "Critical",
        "Major",
        "Major",
        "Minor",
        "Normal",
    ]
    assert rows[1]["Project"] == "YT"
    assert rows[1]["Issue Id"] == "YT-42"
    assert rows[1]["Type"] == "Feature"
    assert rows[1]["Assignee"] == "alex"
    assert rows[1]["Subsystem"] == "API"
    assert rows[1]["Estimation"] == "4h"
    assert rows[1]["Depends On"] == "task-setup"
    assert rows[2]["Depends On"] == "task-setup, task-api"
    assert rows[1]["Tags"] == (
        "blueprint-plan:plan-test, blueprint-brief:ib-test, codex, agent, "
        "Delivery, api,import, planning, csv|pipe"
    )

    description = rows[1]["Description"]
    assert 'Implement the command API with comma, quote " and pipe | handling' in description
    assert "Plan: plan-test" in description
    assert "Implementation brief: ib-test - Test Brief" in description
    assert "Task ID:\n- task-api" in description
    assert "Dependencies:\n- task-setup" in description
    assert 'Acceptance Criteria:\n- API returns "quoted", comma-safe data' in description
    assert "Files/Modules:\n- src/app.py\n- src/schema.py" in description


def test_youtrack_csv_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "youtrack-csv")

    assert result.passed
    assert result.findings == []


def test_youtrack_csv_validation_reports_missing_required_headers(tmp_path):
    output_path = tmp_path / "youtrack.csv"
    output_path.write_text(
        "Summary,Description,Project,Issue Id,Type,Priority,State,Assignee,Tags,"
        "Subsystem,Depends On,External ID\n"
        "Setup project,Description,YT,,Task,Normal,Submitted,,setup,,,task-setup\n"
    )

    findings = validate_rendered_export(
        target="youtrack-csv",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert findings[0].code == "youtrack_csv.missing_column"
    assert "Estimation" in findings[0].message
    assert any(finding.code == "youtrack_csv.row_count_mismatch" for finding in findings)


def test_youtrack_csv_validation_reports_row_count_missing_and_duplicate_tasks(tmp_path):
    output_path = tmp_path / "youtrack.csv"
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=YouTrackCsvExporter.FIELDNAMES)
        writer.writeheader()
        writer.writerow(_row(task_id="task-setup"))
        writer.writerow(_row(task_id="task-setup"))

    findings = validate_rendered_export(
        target="youtrack-csv",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == [
        "youtrack_csv.row_count_mismatch",
        "youtrack_csv.missing_tasks",
        "youtrack_csv.duplicate_task_id",
    ]
    assert "task-api" in findings[1].message


def test_export_run_youtrack_csv_writes_file_and_records_csv_export(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "youtrack-csv"])

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-youtrack-csv.csv"
    assert output_path.exists()
    assert "Exported to:" in result.output

    rows = _read_csv(output_path)
    assert len(rows) == 5
    assert rows[1]["External ID"] == "task-api"

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "youtrack-csv"
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
        "Summary": f"Title {task_id}",
        "Description": f"Body {task_id}",
        "Project": "YT",
        "Issue Id": "",
        "Type": "Task",
        "Priority": "Normal",
        "State": "Submitted",
        "Assignee": "",
        "Tags": "blueprint-plan:plan-test",
        "Subsystem": "Delivery",
        "Estimation": "",
        "Depends On": "",
        "External ID": task_id,
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
                "youtrack_project": "YT",
                "youtrack_issue_id": "YT-42",
                "youtrack_type": "Feature",
                "youtrack_assignee": "alex",
                "youtrack_estimation": "4h",
                "youtrack_subsystem": "API",
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
            "files_or_modules": ["docs/youtrack.md"],
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
        "problem_statement": "Need a YouTrack CSV export",
        "mvp_goal": "Export tasks for YouTrack import",
        "product_surface": "CLI",
        "scope": ["YouTrack CSV exporter"],
        "non_goals": ["YouTrack API automation"],
        "assumptions": ["CSV consumers can import issue rows"],
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
