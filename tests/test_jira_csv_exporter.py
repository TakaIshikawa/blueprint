import csv
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export
from blueprint.exporters.jira_csv import JiraCsvExporter
from blueprint.store import Store, init_db


def test_jira_csv_exporter_writes_epics_then_child_issues(tmp_path):
    output_path = tmp_path / "jira.csv"

    JiraCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert [row["Issue Type"] for row in rows] == ["Epic", "Epic", "Story", "Task"]
    assert [row["Summary"] for row in rows] == [
        "Foundation",
        "Delivery",
        "Setup project",
        "Build API",
    ]
    assert rows[0]["Epic Name"] == "Foundation"
    assert rows[0]["Parent"] == ""
    assert rows[2]["Parent"] == rows[0]["External ID"]
    assert rows[3]["Parent"] == rows[1]["External ID"]


def test_jira_csv_task_description_includes_planning_context(tmp_path):
    output_path = tmp_path / "jira.csv"

    JiraCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    description = rows[3]["Description"]
    assert "Implement the command API" in description
    assert "Dependencies:\n- task-setup" in description
    assert "Files/Modules:\n- src/app.py\n- src/schema.py" in description
    assert "Acceptance Criteria:\n- API returns data\n- Schema validates payloads" in description


def test_jira_csv_exporter_derives_labels_priority_and_issue_type(tmp_path):
    output_path = tmp_path / "jira.csv"

    JiraCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert rows[2]["Labels"] == "codex, agent, setup, backend"
    assert rows[3]["Labels"] == "codex, agent, api, planning"
    assert rows[2]["Priority"] == "Low"
    assert rows[3]["Priority"] == "Highest"
    assert rows[3]["Issue Type"] == "Task"


def test_jira_csv_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "jira-csv")

    assert result.passed
    assert result.findings == []


def test_export_run_jira_csv_writes_file_and_records_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "jira-csv"])

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-jira-csv.csv"
    assert output_path.exists()
    assert "Exported to:" in result.output

    rows = _read_csv(output_path)
    assert len(rows) == 4
    assert rows[0]["Issue Type"] == "Epic"
    assert rows[2]["Summary"] == "Setup project"

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "jira-csv"
    assert records[0]["export_format"] == "csv"
    assert records[0]["output_path"] == str(output_path)


def test_export_preview_and_validate_support_jira_csv(tmp_path, monkeypatch):
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

    preview = CliRunner().invoke(cli, ["export", "preview", plan_id, "jira-csv"])
    assert preview.exit_code == 0, preview.output
    assert "Summary,Description,Issue Type,Labels,Epic Name,Parent,Priority,External ID" in (
        preview.output
    )
    assert "Foundation" in preview.output

    validation = CliRunner().invoke(cli, ["export", "validate", plan_id, "--target", "jira-csv"])
    assert validation.exit_code == 0, validation.output
    assert "Validation passed for jira-csv" in validation.output


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


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
                "jira_issue_type": "Task",
                "jira_priority": "Highest",
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
        "problem_statement": "Need a Jira CSV issue export",
        "mvp_goal": "Export milestones and tasks for Jira import",
        "product_surface": "CLI",
        "scope": ["Jira CSV exporter"],
        "non_goals": ["Jira API integration"],
        "assumptions": ["Jira import parses standard CSV quoting"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Invalid CSV escaping"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Milestones and tasks export as Jira rows"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
