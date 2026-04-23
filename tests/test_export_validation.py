import json
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export
from blueprint.store import init_db


def test_relay_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "relay")

    assert result.passed
    assert result.findings == []


def test_relay_validation_fails_for_invalid_json(monkeypatch):
    class BadRelayExporter:
        def get_extension(self) -> str:
            return ".json"

        def export(self, execution_plan, implementation_brief, output_path):
            Path(output_path).write_text('{"schema_version": "blueprint.relay.v1"')
            return output_path

    monkeypatch.setattr(
        "blueprint.exporters.export_validation.create_exporter",
        lambda target: BadRelayExporter(),
    )

    result = validate_export(_execution_plan(), _implementation_brief(), "relay")

    assert not result.passed
    assert [finding.code for finding in result.findings] == ["relay.invalid_json"]


def test_codex_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "codex")

    assert result.passed
    assert result.findings == []


def test_codex_validation_fails_when_required_heading_missing(monkeypatch):
    class BadCodexExporter:
        def get_extension(self) -> str:
            return ".md"

        def export(self, execution_plan, implementation_brief, output_path):
            Path(output_path).write_text(
                "# BUILD: Test Brief\n"
                "## Overview\n"
                "Problem statement\n"
                "## Technical Specification\n"
                "## Feature Scope\n"
                "## Quality Requirements\n"
                "## Implementation Notes\n"
                "Blueprint Plan ID: `plan-test`\n"
            )
            return output_path

    monkeypatch.setattr(
        "blueprint.exporters.export_validation.create_exporter",
        lambda target: BadCodexExporter(),
    )

    result = validate_export(_execution_plan(), _implementation_brief(), "codex")

    assert not result.passed
    assert any(
        finding.code == "markdown.missing_heading" and "## Build Plan" in finding.message
        for finding in result.findings
    )


def test_export_validate_json_mode_reports_findings_and_fails(tmp_path, monkeypatch):
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

    class BadRelayExporter:
        def get_extension(self) -> str:
            return ".json"

        def export(self, execution_plan, implementation_brief, output_path):
            Path(output_path).write_text('{"schema_version": "blueprint.relay.v1"')
            return output_path

    monkeypatch.setattr(
        "blueprint.exporters.export_validation.create_exporter",
        lambda target: BadRelayExporter(),
    )

    result = CliRunner().invoke(
        cli,
        ["export", "validate", plan_id, "--target", "relay", "--json"],
    )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.output)
    assert payload["target"] == "relay"
    assert payload["passed"] is False
    assert payload["findings"][0]["code"] == "relay.invalid_json"


def test_export_validate_exits_non_zero_on_failure(tmp_path, monkeypatch):
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

    class BadCodexExporter:
        def get_extension(self) -> str:
            return ".md"

        def export(self, execution_plan, implementation_brief, output_path):
            Path(output_path).write_text("# BUILD: Test Brief\n## Overview\n")
            return output_path

    monkeypatch.setattr(
        "blueprint.exporters.export_validation.create_exporter",
        lambda target: BadCodexExporter(),
    )

    result = CliRunner().invoke(
        cli,
        ["export", "validate", plan_id, "--target", "codex"],
    )

    assert result.exit_code == 1, result.output
    assert "Validation failed for codex" in result.output
    assert "markdown.missing_heading" in result.output


def _execution_plan(include_tasks: bool = True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "relay",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation", "description": "Set up the project"}],
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
            "suggested_engine": "relay",
            "depends_on": [],
            "files_or_modules": ["pyproject.toml"],
            "acceptance_criteria": ["Project installs"],
            "estimated_complexity": "low",
            "status": "pending",
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "relay",
            "depends_on": ["task-setup"],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
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
        "problem_statement": "Need a schema validation command",
        "mvp_goal": "Validate rendered export artifacts",
        "product_surface": "CLI",
        "scope": ["Validation command"],
        "non_goals": ["Export generation"],
        "assumptions": ["Targets render to temp files"],
        "architecture_notes": "Use shared exporter validation helpers",
        "data_requirements": "Plans and briefs",
        "integration_points": ["CLI export command"],
        "risks": ["Malformed output should fail validation"],
        "validation_plan": "Run export validation tests",
        "definition_of_done": ["Validation command exits non-zero on failure"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
