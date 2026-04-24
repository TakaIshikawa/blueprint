from pathlib import Path

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.release_notes import ReleaseNotesExporter
from blueprint.store import Store, init_db


def test_release_notes_exporter_renders_milestone_grouped_markdown(tmp_path):
    output_path = tmp_path / "release-notes.md"

    ReleaseNotesExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    notes = output_path.read_text()
    assert notes.startswith("# Release Notes: Test Brief\n")
    assert "## Summary" in notes
    assert "- Plan ID: `plan-test`" in notes
    assert "- Validation Strategy: Run pytest and inspect release notes" in notes
    assert "## Milestones" in notes
    assert "### Foundation" in notes
    assert "- Task Statuses: completed: 1, pending: 1" in notes
    assert "#### task-setup - Setup project" in notes
    assert "- Status: completed" in notes
    assert "  - Project installs" in notes
    assert "#### task-api - Build API" in notes
    assert "- Status: pending" in notes
    assert "### Interface" in notes
    assert "#### task-copy - Write copy" in notes
    assert "- Status: blocked" in notes
    assert "## Completed Tasks" in notes
    assert "- `task-setup` Setup project (completed, Foundation)" in notes
    assert "## Pending Tasks" in notes
    assert "- `task-api` Build API (pending, Foundation)" in notes
    assert "- `task-copy` Write copy (blocked, Interface)" in notes
    assert "## Validation Notes" in notes
    assert "- Brief Validation Plan: Run exporter tests" in notes
    assert "## Known Risks" in notes
    assert "- Missing task status data" in notes
    assert "- task-copy blocked: Waiting for product direction" in notes


def test_release_notes_exporter_validates_payload_before_rendering(tmp_path):
    output_path = tmp_path / "release-notes.md"
    invalid_plan = _execution_plan()
    invalid_plan.pop("implementation_brief_id")

    with pytest.raises(ValidationError):
        ReleaseNotesExporter().export(
            invalid_plan,
            _implementation_brief(),
            str(output_path),
        )

    assert not output_path.exists()


def test_export_run_release_notes_writes_file_and_records_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(
        cli,
        ["export", "run", plan_id, "--target", "release-notes"],
    )

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-release-notes.md"
    assert output_path.exists()
    assert "Exported to:" in result.output
    assert "# Release Notes: Test Brief" in output_path.read_text()

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "release-notes"
    assert records[0]["export_format"] == "markdown"
    assert records[0]["output_path"] == str(output_path)


def _execution_plan(include_tasks=True):
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
        "test_strategy": "Run pytest and inspect release notes",
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
        "problem_statement": "Need stakeholder-facing release notes",
        "mvp_goal": "Export release notes for execution plans",
        "product_surface": "CLI",
        "scope": ["Release notes exporter"],
        "non_goals": ["HTML dashboard"],
        "assumptions": ["Tasks already have statuses"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Missing task status data"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Notes include statuses and acceptance criteria"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
