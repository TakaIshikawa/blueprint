import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.status_timeline import StatusTimelineExporter
from blueprint.store import init_db


def test_status_timeline_markdown_orders_events_by_creation_time(tmp_path):
    exporter = StatusTimelineExporter()
    output_path = tmp_path / "timeline.md"
    events = [
        {
            "id": "se-late",
            "entity_type": "task",
            "entity_id": "task-test",
            "old_status": "in_progress",
            "new_status": "completed",
            "reason": "Done",
            "created_at": "2026-04-25T10:30:00",
            "metadata": {},
        },
        {
            "id": "se-early",
            "entity_type": "task",
            "entity_id": "task-test",
            "old_status": "pending",
            "new_status": "in_progress",
            "reason": "Started",
            "created_at": "2026-04-25T09:00:00",
            "metadata": {},
        },
    ]

    result = exporter.export("task-test", events, str(output_path))

    assert result == str(output_path)
    content = output_path.read_text()
    assert "# Status Timeline: `task-test`" in content
    assert "- Event Count: 2" in content
    assert content.index("pending` -> `in_progress") < content.index(
        "in_progress` -> `completed"
    )
    assert "- Entity Type: task" in content
    assert "- Reason: Started" in content
    assert "`2026-04-25T09:00:00`" in content


def test_history_export_json_uses_status_update_events_without_markdown_file(
    tmp_path, monkeypatch
):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), [_execution_task()])
    assert store.update_execution_task_status(
        "task-test",
        "in_progress",
        reason="Started implementation",
    )
    assert store.update_execution_task_status(
        "task-test",
        "completed",
        reason="Validated locally",
    )

    result = CliRunner().invoke(cli, ["history", "export", "task-test", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["entity_id"] == "task-test"
    assert payload["event_count"] == 2
    assert [event["new_status"] for event in payload["events"]] == [
        "in_progress",
        "completed",
    ]
    assert payload["events"][0]["reason"] == "Started implementation"
    assert not (tmp_path / "task-test-status-timeline.md").exists()


def test_history_export_writes_markdown_when_output_is_provided(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    assert store.update_implementation_brief_status(
        "ib-test",
        "ready_for_planning",
        reason="Ready for planning",
    )
    output_path = tmp_path / "history" / "brief.md"

    result = CliRunner().invoke(
        cli,
        ["history", "export", "ib-test", "--output", str(output_path)],
    )

    assert result.exit_code == 0, result.output
    assert "Exported status timeline to:" in result.output
    content = output_path.read_text()
    assert "- Entity Type: brief" in content
    assert "- Status: `draft` -> `ready_for_planning`" in content
    assert "- Reason: Ready for planning" in content


def test_history_export_reports_unwritable_output_path(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    blocking_file = tmp_path / "not-a-directory"
    blocking_file.write_text("blocks parent creation")

    result = CliRunner().invoke(
        cli,
        [
            "history",
            "export",
            "task-test",
            "--output",
            str(blocking_file / "timeline.md"),
        ],
    )

    assert result.exit_code != 0
    assert "Could not write history export" in result.output


def test_empty_history_produces_explicit_artifact_and_zero_count(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))
    output_path = tmp_path / "empty.md"

    result = CliRunner().invoke(
        cli,
        [
            "history",
            "export",
            "missing-entity",
            "--output",
            str(output_path),
            "--json",
        ],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["entity_id"] == "missing-entity"
    assert payload["event_count"] == 0
    assert payload["events"] == []
    assert (
        "No status history events found for `missing-entity`."
        in output_path.read_text()
    )


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
exports:
  output_dir: {tmp_path}
"""
    )
    blueprint_config.reload_config()


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need status history",
        "mvp_goal": "Record status transitions",
        "product_surface": "CLI",
        "scope": ["Status history"],
        "non_goals": ["External audit sinks"],
        "assumptions": ["SQLite is the local store"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Status event rows",
        "integration_points": [],
        "risks": ["Missing status changes"],
        "validation_plan": "Run pytest",
        "definition_of_done": ["History is visible in the CLI"],
        "status": "draft",
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
        "milestones": [{"name": "Foundation", "description": "Set up history"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build history",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _execution_task():
    return {
        "id": "task-test",
        "execution_plan_id": "plan-test",
        "title": "Record history",
        "description": "Persist status updates as events",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": ["src/blueprint/store/db.py"],
        "acceptance_criteria": ["Status changes create events"],
        "estimated_complexity": "low",
        "status": "pending",
    }
