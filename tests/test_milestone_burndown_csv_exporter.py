import csv
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.exporters.milestone_burndown_csv import MilestoneBurndownCsvExporter
from blueprint.store import Store, init_db


def test_milestone_burndown_csv_exporter_aggregates_by_plan_milestone_order(tmp_path):
    output_path = tmp_path / "burndown.csv"

    MilestoneBurndownCsvExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    rows = _read_csv(output_path)
    assert [row["Milestone"] for row in rows] == [
        "Foundation",
        "Interface",
        "Release",
        "Follow-up",
        "Unassigned",
    ]
    assert rows[0] == {
        "Milestone": "Foundation",
        "Total Tasks": "2",
        "Pending": "0",
        "In Progress": "1",
        "Blocked": "0",
        "Skipped": "0",
        "Completed": "1",
        "Completion Percent": "50.00%",
        "Blocked Percent": "0.00%",
    }
    assert rows[1]["Blocked"] == "1"
    assert rows[1]["Skipped"] == "1"
    assert rows[1]["Blocked Percent"] == "50.00%"


def test_milestone_burndown_csv_exporter_includes_unassigned_tasks(tmp_path):
    output_path = tmp_path / "burndown.csv"

    MilestoneBurndownCsvExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    unassigned = _rows_by_milestone(output_path)["Unassigned"]
    assert unassigned["Total Tasks"] == "1"
    assert unassigned["Pending"] == "1"
    assert unassigned["Completion Percent"] == "0.00%"


def test_milestone_burndown_csv_exporter_formats_zero_total_percentages(tmp_path):
    output_path = tmp_path / "burndown.csv"

    MilestoneBurndownCsvExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    release = _rows_by_milestone(output_path)["Release"]
    assert release["Total Tasks"] == "0"
    assert release["Completion Percent"] == "0.00%"
    assert release["Blocked Percent"] == "0.00%"


def test_milestone_burndown_csv_validation_passes_and_catches_bad_counts(tmp_path):
    assert validate_export(
        _execution_plan(),
        _implementation_brief(),
        "milestone-burndown-csv",
    ).passed

    output_path = tmp_path / "burndown.csv"
    MilestoneBurndownCsvExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )
    rows = _read_csv(output_path)
    rows[0]["Completed"] = "99"
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=MilestoneBurndownCsvExporter.FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)

    findings = validate_rendered_export(
        target="milestone-burndown-csv",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )
    assert [finding.code for finding in findings] == [
        "milestone_burndown_csv.row_mismatch"
    ]


def test_export_run_milestone_burndown_csv_writes_file_and_records_csv_format(
    tmp_path,
    monkeypatch,
):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(
        cli,
        ["export", "run", plan_id, "--target", "milestone-burndown-csv"],
    )

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-milestone-burndown-csv.csv"
    assert output_path.exists()
    assert _read_csv(output_path)[0]["Milestone"] == "Foundation"

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "milestone-burndown-csv"
    assert records[0]["export_format"] == "csv"
    assert records[0]["output_path"] == str(output_path)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _rows_by_milestone(path: Path) -> dict[str, dict[str, str]]:
    return {row["Milestone"]: row for row in _read_csv(path)}


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
            {"name": "Foundation", "description": "Build the base service"},
            {"name": "Interface", "description": "Expose the user flow"},
            {"name": "Release", "description": "Ship the change"},
        ],
        "test_strategy": "Run pytest and inspect the CSV",
        "handoff_prompt": "Build the plan",
        "status": "in_progress",
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
        {
            "id": "task-ui",
            "title": "Build UI",
            "description": "Render the summary interface",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI shows data"],
            "estimated_complexity": "high",
            "status": "blocked",
            "blocked_reason": "Waiting for API contract",
        },
        {
            "id": "task-copy",
            "title": "Write copy",
            "description": "Draft lead-facing summary text",
            "milestone": "Interface",
            "owner_type": "human",
            "suggested_engine": "human",
            "depends_on": [],
            "files_or_modules": ["README.md"],
            "acceptance_criteria": ["Copy is approved"],
            "estimated_complexity": "low",
            "status": "skipped",
        },
        {
            "id": "task-follow-up",
            "title": "Follow-up task",
            "description": "Track task-only milestone names",
            "milestone": "Follow-up",
            "owner_type": "human",
            "suggested_engine": "human",
            "depends_on": [],
            "files_or_modules": ["docs/follow-up.md"],
            "acceptance_criteria": ["Follow-up is tracked"],
            "estimated_complexity": "low",
            "status": "completed",
        },
        {
            "id": "task-unassigned",
            "title": "Unassigned task",
            "description": "Track work without a milestone",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/unassigned.py"],
            "acceptance_criteria": ["Unassigned work is visible"],
            "estimated_complexity": "low",
            "status": "pending",
        },
    ]


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Leads",
        "buyer": "Engineering",
        "workflow_context": "Planning workflow",
        "problem_statement": "Need a plan-level milestone burndown",
        "mvp_goal": "Export milestone burndown summaries for execution plans",
        "product_surface": "CLI",
        "scope": ["Milestone burndown CSV exporter"],
        "non_goals": ["HTML dashboard"],
        "assumptions": ["Tasks have statuses"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Missing task metadata"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Burndown CSV includes milestone counts"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
