import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_diff import (
    _normalize_json_text,
    _normalize_markdown_text,
    _normalize_xml_text,
    compare_rendered_exports,
)
from blueprint.store import init_db


def test_export_diff_normalizes_json_xml_and_markdown_text():
    left_json = '{"b": 2, "created_at": "2024-01-01T00:00:00Z", "a": 1}'
    right_json = '{"a": 1, "updated_at": "2024-01-02T00:00:00Z", "b": 2}'
    assert _normalize_json_text(left_json) == _normalize_json_text(right_json)

    left_xml = '<testsuite b="2" a="1"><testcase> value </testcase></testsuite>'
    right_xml = '<testsuite a="1" b="2"><testcase>value</testcase></testsuite>'
    assert _normalize_xml_text(left_xml) == _normalize_xml_text(right_xml)

    left_markdown = "# Report\n\n- Created: 2024-01-01\n\nBody\n"
    right_markdown = "# Report\r\n\r\nBody\r\n"
    assert _normalize_markdown_text(left_markdown) == _normalize_markdown_text(right_markdown)


def test_export_diff_normalizes_status_report_timestamps():
    left_plan = _plan(created_at="2024-01-01T00:00:00Z", updated_at="2024-01-01T00:00:00Z")
    right_plan = _plan(created_at="2024-01-02T00:00:00Z", updated_at="2024-01-02T00:00:00Z")

    result = compare_rendered_exports(
        left_plan,
        _implementation_brief(),
        right_plan,
        _implementation_brief(),
        "status-report",
    )

    assert result.artifact_type == "file"
    assert not result.has_changes
    assert result.to_dict()["summary"] == {
        "left_files": 1,
        "right_files": 1,
        "added_files": 0,
        "removed_files": 0,
        "changed_files": 0,
        "unchanged_files": 1,
    }


def test_export_diff_cli_reports_human_readable_no_changes(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_plan(), _tasks(["task-setup-left"]))

    result = CliRunner().invoke(cli, ["export", "diff", "plan-test", "plan-test", "--target", "codex"])

    assert result.exit_code == 0, result.output
    assert "Export diff: plan-test -> plan-test" in result.output
    assert "No differences found after normalization." in result.output


def test_export_diff_cli_outputs_json_for_directory_targets(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_plan("plan-left"), _tasks(["task-setup-left"]))
    store.insert_execution_plan(
        _plan("plan-right"),
        _tasks(["task-setup-right", "task-api-right"]),
    )

    result = CliRunner().invoke(
        cli,
        ["export", "diff", "plan-left", "plan-right", "--target", "task-bundle", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["target"] == "task-bundle"
    assert payload["artifact_type"] == "directory"
    assert payload["left_plan_id"] == "plan-left"
    assert payload["right_plan_id"] == "plan-right"
    assert payload["summary"]["added_files"] == 2
    assert payload["summary"]["removed_files"] == 1
    assert payload["summary"]["changed_files"] == 1
    assert any(change["path"] == "README.md" for change in payload["files"]["changed"])
    assert any(change["path"] == "001-task-setup-right.md" for change in payload["files"]["added"])
    assert any(change["path"] == "002-task-api-right.md" for change in payload["files"]["added"])


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


def _plan(
    plan_id: str = "plan-test",
    *,
    created_at: str | None = None,
    updated_at: str | None = None,
):
    plan = {
        "id": plan_id,
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
        "created_at": created_at,
        "updated_at": updated_at,
    }
    return plan


def _tasks(task_ids: list[str]):
    task_map = {
        "task-setup-left": {
            "id": "task-setup-left",
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
        "task-setup-right": {
            "id": "task-setup-right",
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
        "task-api-right": {
            "id": "task-api-right",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup-right"],
            "files_or_modules": ["src/app.py", "src/schema.py"],
            "acceptance_criteria": ["API returns data", "Schema validates payloads"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
    }
    return [task_map[task_id] for task_id in task_ids]


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
