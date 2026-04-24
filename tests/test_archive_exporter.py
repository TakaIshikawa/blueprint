import csv
import io
import json
from pathlib import Path
from zipfile import ZipFile

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.store import init_db


def test_export_archive_writes_complete_portable_zip(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    output_path = tmp_path / "exports" / "plan-test.zip"
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_source_brief(_source_brief())
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(
        cli,
        ["export", "archive", "plan-test", "--output", str(output_path)],
    )

    assert result.exit_code == 0, result.output
    assert "Exported archive to:" in result.output
    assert output_path.exists()

    with ZipFile(output_path) as archive:
        names = sorted(archive.namelist())
        assert names == [
            "graph.dot",
            "graph.json",
            "implementation_brief.json",
            "manifest.json",
            "plan.json",
            "source_brief.json",
            "status_report.md",
            "tasks.csv",
        ]

        manifest = json.loads(archive.read("manifest.json"))
        assert manifest["schema_version"] == "1"
        assert manifest["plan_id"] == "plan-test"
        assert manifest["brief_id"] == "ib-test"
        assert manifest["task_count"] == 2
        assert manifest["included_files"] == names
        assert manifest["generated_at"].endswith("Z")

        assert json.loads(archive.read("plan.json"))["id"] == "plan-test"
        assert json.loads(archive.read("implementation_brief.json"))["id"] == "ib-test"
        assert json.loads(archive.read("source_brief.json"))["id"] == "sb-test"
        assert archive.read("status_report.md").decode().startswith(
            "# Execution Plan Status Report: plan-test\n"
        )
        assert archive.read("graph.dot").decode().startswith('digraph "plan-test" {\n')

        graph = json.loads(archive.read("graph.json"))
        assert graph["edges"] == [{"from": "task-setup", "to": "task-api"}]

        task_rows = list(
            csv.DictReader(io.StringIO(archive.read("tasks.csv").decode()))
        )
        assert [row["task_id"] for row in task_rows] == ["task-setup", "task-api"]


def test_export_archive_omits_source_brief_when_unavailable(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    output_path = tmp_path / "plan-test.zip"
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), _tasks())

    result = CliRunner().invoke(
        cli,
        ["export", "archive", "plan-test", "--output", str(output_path)],
    )

    assert result.exit_code == 0, result.output
    with ZipFile(output_path) as archive:
        assert "source_brief.json" not in archive.namelist()
        manifest = json.loads(archive.read("manifest.json"))
        assert "source_brief.json" not in manifest["included_files"]


def test_export_archive_missing_plan_fails_clearly(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(
        cli,
        ["export", "archive", "plan-missing", "--output", str(tmp_path / "missing.zip")],
    )

    assert result.exit_code != 0
    assert "Execution plan not found: plan-missing" in result.output


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    Path(".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
exports:
  output_dir: {tmp_path / "exports"}
"""
    )
    blueprint_config.reload_config()


def _source_brief():
    return {
        "id": "sb-test",
        "title": "Source Brief",
        "domain": "testing",
        "summary": "A source brief used for archive export tests.",
        "source_project": "manual",
        "source_entity_type": "markdown_brief",
        "source_id": "briefs/source.md",
        "source_payload": {"raw_markdown": "# Source Brief"},
        "source_links": {"file_path": "briefs/source.md"},
    }


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need a portable archive export",
        "mvp_goal": "Export all plan artifacts into a zip",
        "product_surface": "CLI",
        "scope": ["Archive exporter"],
        "non_goals": ["Remote storage"],
        "assumptions": ["Zip files are portable"],
        "architecture_notes": "Reuse existing exporters",
        "data_requirements": "Execution plan, briefs, tasks, graph",
        "integration_points": [],
        "risks": ["Missing related records"],
        "validation_plan": "Inspect archive contents",
        "definition_of_done": ["Archive contains manifest and rendered files"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _execution_plan():
    return {
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
    ]
