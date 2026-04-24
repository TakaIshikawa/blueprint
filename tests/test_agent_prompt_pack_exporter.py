import json
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.agent_prompt_pack import AgentPromptPackExporter
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.store import Store, init_db


def test_agent_prompt_pack_writes_manifest_and_prompt_files(tmp_path):
    output_dir = tmp_path / "agent-prompts"

    result_path = AgentPromptPackExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_dir),
    )

    assert result_path == str(output_dir)
    assert output_dir.is_dir()
    assert (output_dir / "manifest.json").exists()
    assert (output_dir / "task-setup.md").exists()
    assert (output_dir / "task-api.md").exists()


def test_agent_prompt_pack_manifest_maps_tasks_to_prompts_and_dependencies(tmp_path):
    output_dir = tmp_path / "agent-prompts"

    AgentPromptPackExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_dir),
    )

    manifest = json.loads((output_dir / "manifest.json").read_text())
    assert manifest["plan_id"] == "plan-test"
    assert manifest["implementation_brief_id"] == "ib-test"
    assert manifest["prompt_format"] == "markdown"
    assert manifest["manifest_format"] == "json"
    assert manifest["tasks"]["task-setup"] == {
        "title": "Setup project",
        "prompt_path": "task-setup.md",
        "dependencies": [],
    }
    assert manifest["tasks"]["task-api"] == {
        "title": "Build API",
        "prompt_path": "task-api.md",
        "dependencies": ["task-setup", "task-schema"],
    }


def test_agent_prompt_pack_prompt_contains_context_and_task_details(tmp_path):
    output_dir = tmp_path / "agent-prompts"

    AgentPromptPackExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_dir),
    )

    prompt = (output_dir / "task-api.md").read_text()
    assert "# Agent Task: Build API" in prompt
    assert "- Work on an isolated branch for this task before making changes." in prompt
    assert "- Brief ID: `ib-test`" in prompt
    assert "- Plan ID: `plan-test`" in prompt
    assert "- Project: Test Brief" in prompt
    assert "Need autonomous agent prompt packs" in prompt
    assert "Export per-task prompts for execution workflows" in prompt
    assert "Use the exporter interface" in prompt
    assert "- Task ID: `task-api`" in prompt
    assert "- Dependencies: task-setup, task-schema" in prompt
    assert "- Expected Files/Modules: src/app.py, src/schema.py" in prompt
    assert "Implement the command API" in prompt
    assert "- API returns data" in prompt
    assert "- Schema validates payloads" in prompt
    assert "- Suggested Test Command: `poetry run pytest tests/test_agent_prompt_pack_exporter.py`" in prompt
    assert "- Test Strategy: Run pytest" in prompt
    assert "  - Prompts are ready for autonomous coding agents" in prompt


def test_agent_prompt_pack_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "agent-prompt-pack")

    assert result.passed
    assert result.findings == []


def test_agent_prompt_pack_validation_reports_missing_manifest(tmp_path):
    output_dir = tmp_path / "agent-prompts"
    output_dir.mkdir()

    findings = validate_rendered_export(
        target="agent-prompt-pack",
        artifact_path=output_dir,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == [
        "agent_prompt_pack.missing_manifest"
    ]


def test_export_run_agent_prompt_pack_writes_directory_and_records_metadata(
    tmp_path, monkeypatch
):
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
        ["export", "run", plan_id, "--target", "agent-prompt-pack"],
    )

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-agent-prompt-pack"
    assert output_path.is_dir()
    assert (output_path / "manifest.json").exists()
    assert (output_path / "task-api.md").exists()
    assert "Exported to:" in result.output

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "agent-prompt-pack"
    assert records[0]["export_format"] == "markdown"
    assert records[0]["output_path"] == str(output_path)
    assert records[0]["export_metadata"] == {
        "brief_id": "ib-test",
        "brief_title": "Test Brief",
        "artifact_type": "directory",
        "manifest_format": "json",
        "prompt_format": "markdown",
    }


def _execution_plan(include_tasks=True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation", "description": "Set up the project"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "metadata": {
            "test_command": "poetry run pytest tests/test_agent_prompt_pack_exporter.py"
        },
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
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup", "task-schema"],
            "files_or_modules": ["src/app.py", "src/schema.py"],
            "acceptance_criteria": ["API returns data", "Schema validates payloads"],
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
        "problem_statement": "Need autonomous agent prompt packs",
        "mvp_goal": "Export per-task prompts for execution workflows",
        "product_surface": "CLI",
        "scope": ["Agent prompt pack exporter"],
        "non_goals": ["Task execution"],
        "assumptions": ["Agents can consume Markdown prompts"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Plan and task dictionaries",
        "integration_points": ["CLI export command"],
        "risks": ["Prompt files must be stable"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Prompts are ready for autonomous coding agents"],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
