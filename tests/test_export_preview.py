from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.codex import CodexExporter
from blueprint.exporters.csv_tasks import CsvTasksExporter
from blueprint.store import Store, init_db


def test_export_preview_prints_stdout_without_recording_export(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    expected_path = tmp_path / "expected.csv"
    CsvTasksExporter().export(_execution_plan(), _implementation_brief(), str(expected_path))

    result = CliRunner().invoke(cli, ["export", "preview", plan_id, "--target", "csv-tasks"])

    assert result.exit_code == 0, result.output
    assert result.output == expected_path.read_text()
    assert not (tmp_path / "exports").exists()
    assert Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id) == []


def test_export_preview_writes_optional_output_file_without_recording_export(
    tmp_path,
    monkeypatch,
):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())
    output_path = tmp_path / "previews" / "codex.md"

    expected_path = tmp_path / "expected.md"
    CodexExporter().export(_execution_plan(), _implementation_brief(), str(expected_path))

    result = CliRunner().invoke(
        cli,
        ["export", "preview", plan_id, "--target", "codex", "--output", str(output_path)],
    )

    assert result.exit_code == 0, result.output
    assert "Wrote preview to:" in result.output
    assert output_path.read_text() == expected_path.read_text()
    assert not (tmp_path / "exports").exists()
    assert Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id) == []


def test_export_preview_supports_directory_targets(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(
        cli,
        ["export", "preview", plan_id, "--target", "task-bundle"],
    )

    assert result.exit_code == 0, result.output
    assert "## README.md" in result.output
    assert "Task Bundle: plan-test" in result.output
    assert "001-task-setup.md" in result.output
    assert Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id) == []


def test_export_run_requires_coherence_blocks_incoherent_plan(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_incoherent_brief())
    plan_id = store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(
        cli,
        ["export", "run", plan_id, "--target", "csv-tasks", "--require-coherence"],
    )

    assert result.exit_code == 1, result.output
    assert "Brief-plan coherence audit: plan-test" in result.output
    assert "[scope_item_uncovered]" in result.output
    assert "Export blocked by brief-plan coherence errors" in result.output
    assert not (tmp_path / "exports").exists()
    assert Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id) == []


def test_export_preview_requires_coherence_blocks_incoherent_plan(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_incoherent_brief())
    plan_id = store.insert_execution_plan(_execution_plan(), _tasks())
    output_path = tmp_path / "previews" / "codex.md"

    result = CliRunner().invoke(
        cli,
        [
            "export",
            "preview",
            plan_id,
            "--target",
            "codex",
            "--output",
            str(output_path),
            "--require-coherence",
        ],
    )

    assert result.exit_code == 1, result.output
    assert "Brief-plan coherence audit: plan-test" in result.output
    assert "[scope_item_uncovered]" in result.output
    assert "Export blocked by brief-plan coherence errors" in result.output
    assert not output_path.exists()
    assert Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id) == []


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
        "problem_statement": "Need configurable exports",
        "mvp_goal": "Render configured markdown templates",
        "product_surface": "CLI",
        "scope": ["Template rendering"],
        "non_goals": ["Template conditionals"],
        "assumptions": ["Simple placeholders are enough"],
        "architecture_notes": "Use shared exporter template renderer",
        "data_requirements": "Briefs, plans, and tasks",
        "integration_points": [],
        "risks": ["Missing placeholders"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Templates render"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _incoherent_brief():
    brief = _implementation_brief()
    brief["scope"] = ["Unmatched scope item"]
    brief["validation_plan"] = "Run pytest"
    brief["definition_of_done"] = ["Build the plan"]
    return brief
