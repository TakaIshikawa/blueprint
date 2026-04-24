from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export
from blueprint.exporters.milestone_summary import MilestoneSummaryExporter
from blueprint.store import Store, init_db


def test_milestone_summary_exporter_renders_lead_overview(tmp_path):
    output_path = tmp_path / "milestone-summary.md"

    MilestoneSummaryExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    summary = output_path.read_text()
    assert summary.startswith("# Milestone Summary: Test Brief\n")
    assert "## Plan Overview" in summary
    assert "- Plan ID: `plan-test`" in summary
    assert "- Total Tasks: 4" in summary
    assert "- Status Breakdown: pending: 1, blocked: 1, completed: 1, skipped: 1" in summary
    assert "## Cross-Milestone Dependencies" in summary
    assert "- `task-ui` (Interface) depends on `task-api` (Foundation)" in summary
    assert "### Foundation" in summary
    assert "- Total Tasks: 2" in summary
    assert "- Status Breakdown: pending: 1, completed: 1" in summary
    assert "- Suggested Engines: codex: 2" in summary
    assert "### Interface" in summary
    assert "- Status Breakdown: blocked: 1, skipped: 1" in summary
    assert "- Suggested Engines: codex: 1, human: 1" in summary
    assert "- Risk Notes:" in summary
    assert (
        "  - `task-ui` Build UI: blocked: Waiting for API contract; "
        "complexity: high; risk_level: high"
    ) in summary
    assert "  - `task-copy` Write copy: stakeholder review required" in summary
    assert "- Exit Criteria:" in summary
    assert "  - API returns data" in summary
    assert "  - UI shows data" in summary
    assert "  - `task-ui` Build UI (blocked, engine: codex)" in summary


def test_milestone_summary_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "milestone-summary")

    assert result.passed
    assert result.findings == []


def test_export_preview_milestone_summary_prints_markdown(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(
        cli,
        ["export", "preview", plan_id, "--target", "milestone-summary"],
    )

    assert result.exit_code == 0, result.output
    assert "# Milestone Summary: Test Brief" in result.output
    assert "`task-ui` (Interface) depends on `task-api` (Foundation)" in result.output
    assert Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id) == []


def test_export_run_milestone_summary_writes_file_and_records_export(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(
        cli,
        ["export", "run", plan_id, "--target", "milestone-summary"],
    )

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-milestone-summary.md"
    assert output_path.exists()
    assert "# Milestone Summary: Test Brief" in output_path.read_text()

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "milestone-summary"
    assert records[0]["export_format"] == "markdown"
    assert records[0]["output_path"] == str(output_path)


def test_export_validate_milestone_summary_cli_passes(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(
        cli,
        ["export", "validate", plan_id, "--target", "milestone-summary"],
    )

    assert result.exit_code == 0, result.output
    assert "Validation passed for milestone-summary" in result.output


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
        ],
        "test_strategy": "Run pytest and inspect the summary",
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
            "status": "pending",
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
            "metadata": {"risk_level": "high"},
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
            "metadata": {"risk_notes": ["stakeholder review required"]},
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
        "problem_statement": "Need a plan-level milestone summary",
        "mvp_goal": "Export milestone summaries for execution plans",
        "product_surface": "CLI",
        "scope": ["Milestone summary exporter"],
        "non_goals": ["HTML dashboard"],
        "assumptions": ["Tasks have milestone names"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Missing task metadata"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Summary includes milestone counts"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
