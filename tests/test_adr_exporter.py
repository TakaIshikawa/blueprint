from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.adr import ADRExporter
from blueprint.exporters.export_validation import (
    validate_export,
    validate_rendered_export,
)
from blueprint.store import Store, init_db


def test_adr_exporter_writes_readme_and_numbered_adr_files(tmp_path):
    output_dir = tmp_path / "plan-test-adr"

    result_path = ADRExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_dir),
    )

    assert result_path == str(output_dir)
    assert output_dir.is_dir()
    assert (output_dir / "README.md").exists()
    assert (output_dir / "001-milestone-foundation.md").exists()
    assert (output_dir / "002-milestone-api.md").exists()
    assert (output_dir / "003-integration-point-cli-export-command.md").exists()


def test_adr_exporter_renders_required_sections_and_blueprint_ids(tmp_path):
    output_dir = tmp_path / "plan-test-adr"

    ADRExporter().export(_execution_plan(), _implementation_brief(), str(output_dir))

    readme = (output_dir / "README.md").read_text()
    assert "# Architecture Decision Records: plan-test" in readme
    assert "- Plan ID: `plan-test`" in readme
    assert "- Implementation Brief ID: `ib-test`" in readme
    assert "1. [Implement Foundation](001-milestone-foundation.md)" in readme

    adr = (output_dir / "001-milestone-foundation.md").read_text()
    assert "# ADR-001: Implement Foundation" in adr
    assert "## Context" in adr
    assert "Architecture notes: Use the exporter interface" in adr
    assert "## Decision" in adr
    assert "Deliver the `Foundation` milestone" in adr
    assert "## Consequences" in adr
    assert "Risk to manage: ADR filenames must stay stable" in adr
    assert "## Related Tasks" in adr
    assert "- `task-setup` Setup project (pending)" in adr
    assert "## Source Blueprint IDs" in adr
    assert "- Source Brief ID: `sb-test`" in adr


def test_adr_exporter_derives_context_when_architecture_notes_missing(tmp_path):
    output_dir = tmp_path / "plan-test-adr"
    brief = _implementation_brief()
    brief["architecture_notes"] = None

    ADRExporter().export(_execution_plan(), brief, str(output_dir))

    adr = (output_dir / "001-milestone-foundation.md").read_text()
    assert "Problem: Need architecture decision records" in adr
    assert "MVP goal: Export durable implementation decisions" in adr
    assert "Assumption: Markdown ADRs are durable enough for teams" in adr
    assert "Integration point: CLI export command" in adr
    assert "Risk: ADR filenames must stay stable" in adr


def test_adr_export_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "adr")

    assert result.passed
    assert result.findings == []


def test_adr_export_validation_fails_when_required_section_missing(tmp_path):
    output_dir = tmp_path / "bad-adr"
    output_dir.mkdir()
    (output_dir / "README.md").write_text(
        "# Architecture Decision Records: plan-test\n"
        "## Source Blueprint\n"
        "- Plan ID: `plan-test`\n"
        "- Implementation Brief ID: `ib-test`\n"
        "## ADR Index\n"
    )
    (output_dir / "001-milestone-foundation.md").write_text(
        "# ADR-001: Implement Foundation\n"
        "## Context\n"
        "- Context\n"
        "## Decision\n"
        "Decision\n"
        "## Related Tasks\n"
        "- None\n"
        "## Source Blueprint IDs\n"
        "- Plan ID: `plan-test`\n"
        "- Implementation Brief ID: `ib-test`\n"
        "- Source Brief ID: `sb-test`\n"
    )

    findings = validate_rendered_export(
        target="adr",
        artifact_path=output_dir,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert any(finding.code == "adr.missing_heading" for finding in findings)
    assert any("## Consequences" in finding.message for finding in findings)


def test_export_run_adr_writes_directory_and_records_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "adr"])

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-adr"
    assert output_path.is_dir()
    assert (output_path / "README.md").exists()
    assert (output_path / "001-milestone-foundation.md").exists()
    assert "Exported to:" in result.output

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "adr"
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
            {"name": "Foundation", "description": "Set up the exporter surface"},
            {"name": "API", "description": "Wire CLI and validation hooks"},
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
            "description": "Create the baseline ADR exporter",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/blueprint/exporters/adr.py"],
            "acceptance_criteria": ["ADRs export as Markdown files"],
            "estimated_complexity": "low",
            "status": "pending",
        },
        {
            "id": "task-cli",
            "title": "Wire CLI",
            "description": "Register the ADR target for the CLI export command",
            "milestone": "API",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup"],
            "files_or_modules": ["src/blueprint/cli.py"],
            "acceptance_criteria": ["CLI export command accepts adr"],
            "estimated_complexity": "medium",
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
        "problem_statement": "Need architecture decision records",
        "mvp_goal": "Export durable implementation decisions",
        "product_surface": "CLI",
        "scope": ["ADR exporter"],
        "non_goals": ["Decision approval workflow"],
        "assumptions": ["Markdown ADRs are durable enough for teams"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Plan and brief dictionaries",
        "integration_points": ["CLI export command"],
        "risks": ["ADR filenames must stay stable"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["ADRs export as Markdown files"],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
