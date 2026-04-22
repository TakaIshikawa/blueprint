from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.status_report import StatusReportExporter
from blueprint.store import Store, init_db


def test_status_report_exporter_renders_report_content(tmp_path):
    output_path = tmp_path / "status.md"

    StatusReportExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    report = output_path.read_text()
    assert report.startswith("# Execution Plan Status Report: plan-test\n")
    assert "## Plan Metadata" in report
    assert "- Plan ID: `plan-test`" in report
    assert "- Target Engine: codex" in report
    assert "## Implementation Brief Summary" in report
    assert "- Title: Test Brief" in report
    assert "- MVP Goal: Export status reports for execution plans" in report
    assert "## Task Counts By Status" in report
    assert "- Total: 5" in report
    assert "- pending: 2" in report
    assert "- completed: 1" in report
    assert "- skipped: 1" in report
    assert "- blocked: 1" in report
    assert "## Milestone Progress" in report
    assert "- Foundation: 1/3 completed (33%)" in report
    assert "- Interface: 0/2 completed (0%)" in report
    assert "## Recent Export Metadata" in report
    assert "- csv-tasks: export_format: csv; output_path: exports/plan-test-csv-tasks.csv" in report


def test_status_report_exporter_renders_blocked_reasons(tmp_path):
    output_path = tmp_path / "status.md"

    StatusReportExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    report = output_path.read_text()
    assert "## Blocked Tasks" in report
    assert "- `task-copy` Write copy: Waiting for product direction" in report


def test_status_report_exporter_renders_ready_tasks(tmp_path):
    output_path = tmp_path / "status.md"

    StatusReportExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    report = output_path.read_text()
    assert "## Ready Tasks" in report
    assert "- `task-api` Build API (dependencies satisfied: task-setup, task-schema)" in report
    assert "- `task-docs` Write docs (no dependencies)" in report
    assert "`task-ui`" not in report


def test_export_run_status_report_writes_file_and_records_export(tmp_path, monkeypatch):
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
    plan_id = store.insert_execution_plan(
        _execution_plan(include_tasks=False, include_recent_exports=False),
        _tasks(),
    )

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "status-report"])

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-status-report.md"
    assert output_path.exists()
    assert "Exported to:" in result.output
    assert "# Execution Plan Status Report:" in output_path.read_text()

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "status-report"
    assert records[0]["export_format"] == "markdown"
    assert records[0]["output_path"] == str(output_path)
    assert records[0]["export_metadata"] == {
        "brief_id": "ib-test",
        "brief_title": "Test Brief",
    }


def _execution_plan(include_tasks=True, include_recent_exports=True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Interface", "description": "Build the user-facing flow"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "in_progress",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
    if include_recent_exports:
        plan["recent_exports"] = [
            {
                "target_engine": "csv-tasks",
                "export_format": "csv",
                "output_path": "exports/plan-test-csv-tasks.csv",
            }
        ]
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
            "status": "skipped",
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup", "task-schema"],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
        {
            "id": "task-copy",
            "title": "Write copy",
            "description": "Draft interface copy",
            "milestone": "Interface",
            "owner_type": "human",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["Copy is approved"],
            "estimated_complexity": "low",
            "status": "blocked",
            "blocked_reason": "Waiting for product direction",
        },
        {
            "id": "task-docs",
            "title": "Write docs",
            "description": "Document the report output",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["README.md"],
            "acceptance_criteria": ["Docs describe usage"],
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
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need a Markdown status export",
        "mvp_goal": "Export status reports for execution plans",
        "product_surface": "CLI",
        "scope": ["Status report exporter"],
        "non_goals": ["HTML dashboard"],
        "assumptions": ["Tasks already have statuses"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Missing task status data"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Report contains progress and blockers"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
