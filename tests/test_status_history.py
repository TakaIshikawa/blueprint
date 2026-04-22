import sqlite3

from click.testing import CliRunner
from sqlalchemy import inspect

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.domain import StatusEvent
from blueprint.store import Store, init_db


def test_status_updates_record_history_and_skip_noop_changes(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), [_execution_task()])

    assert store.update_implementation_brief_status(
        "ib-test",
        "ready_for_planning",
        reason="Ready for plan",
    )
    assert store.update_execution_plan_status(
        "plan-test",
        "ready",
        reason="Plan reviewed",
    )
    assert store.update_execution_task_status(
        "task-test",
        "blocked",
        blocked_reason="Waiting for access",
        reason="Cannot validate",
    )
    assert store.update_execution_task_status(
        "task-test",
        "blocked",
        reason="Duplicate no-op",
    )

    brief_events = store.list_status_events("ib-test")
    plan_events = store.list_status_events("plan-test")
    task_events = store.list_status_events("task-test")

    assert len(brief_events) == 1
    assert brief_events[0]["entity_type"] == "brief"
    assert brief_events[0]["old_status"] == "draft"
    assert brief_events[0]["new_status"] == "ready_for_planning"
    assert brief_events[0]["reason"] == "Ready for plan"
    assert plan_events[0]["entity_type"] == "plan"
    assert plan_events[0]["old_status"] == "draft"
    assert plan_events[0]["new_status"] == "ready"
    assert len(task_events) == 1
    assert task_events[0]["old_status"] == "pending"
    assert task_events[0]["new_status"] == "blocked"
    assert task_events[0]["metadata"] == {"blocked_reason": "Waiting for access"}


def test_history_cli_shows_status_events_for_entity(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), [_execution_task()])

    update_result = CliRunner().invoke(
        cli,
        [
            "task",
            "update",
            "task-test",
            "--status",
            "completed",
            "--reason",
            "Verified locally",
        ],
    )
    history_result = CliRunner().invoke(cli, ["history", "task-test"])

    assert update_result.exit_code == 0, update_result.output
    assert history_result.exit_code == 0, history_result.output
    assert "task" in history_result.output
    assert "pending -> completed" in history_result.output
    assert "Verified locally" in history_result.output
    assert "Total: 1 events" in history_result.output


def test_brief_and_plan_update_cli_accept_reason(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), [_execution_task()])

    brief_result = CliRunner().invoke(
        cli,
        [
            "brief",
            "update",
            "ib-test",
            "--status",
            "ready_for_planning",
            "--reason",
            "Ready",
        ],
    )
    plan_result = CliRunner().invoke(
        cli,
        [
            "plan",
            "update",
            "plan-test",
            "--status",
            "ready",
            "--reason",
            "Reviewed",
        ],
    )

    db = Store(str(tmp_path / "blueprint.db"))
    assert brief_result.exit_code == 0, brief_result.output
    assert plan_result.exit_code == 0, plan_result.output
    assert db.get_implementation_brief("ib-test")["status"] == "ready_for_planning"
    assert db.get_execution_plan("plan-test")["status"] == "ready"
    assert db.list_status_events("ib-test")[0]["reason"] == "Ready"
    assert db.list_status_events("plan-test")[0]["reason"] == "Reviewed"


def test_existing_sqlite_database_gets_status_events_table(tmp_path):
    db_path = tmp_path / "blueprint.db"
    with sqlite3.connect(db_path) as connection:
        connection.execute("CREATE TABLE execution_tasks (id TEXT PRIMARY KEY)")

    store = Store(str(db_path))
    inspector = inspect(store.engine)

    assert "status_events" in inspector.get_table_names()


def test_status_event_domain_model_validates():
    StatusEvent.model_validate(
        {
            "id": "se-test",
            "entity_type": "task",
            "entity_id": "task-test",
            "old_status": "pending",
            "new_status": "completed",
            "reason": "Done",
            "metadata": {"source": "cli"},
        }
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
