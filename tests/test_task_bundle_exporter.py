from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.task_bundle import TaskBundleExporter
from blueprint.store import Store, init_db


def test_task_bundle_exporter_writes_readme_and_task_files(tmp_path):
    output_dir = tmp_path / "plan-test-task-bundle"

    result_path = TaskBundleExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_dir),
    )

    assert result_path == str(output_dir)
    assert output_dir.is_dir()
    assert (output_dir / "README.md").exists()
    assert (output_dir / "001-task-setup.md").exists()
    assert (output_dir / "002-task-api.md").exists()


def test_task_bundle_exporter_renders_index_and_task_content(tmp_path):
    output_dir = tmp_path / "plan-test-task-bundle"

    TaskBundleExporter().export(_execution_plan(), _implementation_brief(), str(output_dir))

    readme = (output_dir / "README.md").read_text()
    assert "# Task Bundle: plan-test" in readme
    assert "- Title: Test Brief" in readme
    assert "1. [task-setup - Setup project](001-task-setup.md)" in readme
    assert "2. [task-api - Build API](002-task-api.md)" in readme

    task = (output_dir / "002-task-api.md").read_text()
    assert "# Build API" in task
    assert "- Task ID: `task-api`" in task
    assert "- Status: pending" in task
    assert "- Milestone: Foundation" in task
    assert "- Dependencies: task-setup, task-schema" in task
    assert "- Files or Modules: src/app.py, src/schema.py" in task
    assert "Implement the command API" in task
    assert "- API returns data" in task
    assert "- Suggested Engine: codex" in task
    assert "- Test Strategy: Run pytest" in task
    assert "- Brief Validation Plan: Run exporter tests" in task
    assert "  - Tasks export as Markdown files" in task


def test_task_bundle_exporter_handles_empty_task_lists(tmp_path):
    output_dir = tmp_path / "plan-test-task-bundle"
    plan = _execution_plan()
    plan["tasks"] = []

    TaskBundleExporter().export(plan, _implementation_brief(), str(output_dir))

    readme = (output_dir / "README.md").read_text()
    assert "No tasks defined." in readme
    assert [path.name for path in output_dir.iterdir()] == ["README.md"]


def test_export_run_task_bundle_writes_directory_and_records_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "task-bundle"])

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-task-bundle"
    assert output_path.is_dir()
    assert (output_path / "README.md").exists()
    assert (output_path / "001-task-setup.md").exists()
    assert "Exported to:" in result.output

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "task-bundle"
    assert records[0]["export_format"] == "markdown"
    assert records[0]["output_path"] == str(output_path)
    assert records[0]["export_metadata"] == {
        "brief_id": "ib-test",
        "brief_title": "Test Brief",
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
        "problem_statement": "Need a Markdown task bundle export",
        "mvp_goal": "Export tasks for execution workflows",
        "product_surface": "CLI",
        "scope": ["Task bundle exporter"],
        "non_goals": ["Task execution"],
        "assumptions": ["Markdown consumers can follow links"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Plan and task dictionaries",
        "integration_points": ["CLI export command"],
        "risks": ["Task filenames must stay stable"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Tasks export as Markdown files"],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
