import json
import sqlite3

from blueprint.exporters.codex import CodexExporter
from blueprint.exporters.relay import RelayExporter
from blueprint.generators.heuristic_plan_generator import HeuristicPlanGenerator
from blueprint.store import Store, init_db


def test_execution_task_planning_metadata_round_trips(tmp_path):
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), [_task()])

    task = store.get_execution_task("task-api")
    plan = store.get_execution_plan("plan-test")

    assert task["estimated_hours"] == 2.5
    assert task["risk_level"] == "high"
    assert task["test_command"] == "poetry run pytest tests/test_api.py"
    assert plan["tasks"][0]["estimated_hours"] == 2.5
    assert plan["tasks"][0]["risk_level"] == "high"
    assert plan["tasks"][0]["test_command"] == "poetry run pytest tests/test_api.py"


def test_existing_execution_task_table_is_migrated_for_planning_metadata(tmp_path):
    db_path = tmp_path / "legacy.db"
    connection = sqlite3.connect(db_path)
    connection.execute("CREATE TABLE execution_tasks (id TEXT PRIMARY KEY)")
    connection.commit()
    connection.close()

    Store(str(db_path))

    migrated_connection = sqlite3.connect(db_path)
    columns = {
        row[1] for row in migrated_connection.execute("PRAGMA table_info(execution_tasks)")
    }
    migrated_connection.close()

    assert {"estimated_hours", "risk_level", "test_command", "metadata"} <= columns


def test_heuristic_generator_populates_planning_metadata_defaults():
    plan, tasks = HeuristicPlanGenerator().generate(_implementation_brief())

    assert plan["generation_model"] == "heuristic"
    assert tasks
    for task in tasks:
        assert task["estimated_hours"] > 0
        assert task["risk_level"] in {"low", "medium", "high"}
        assert "test_command" in task


def test_codex_and_relay_exports_include_task_planning_metadata(tmp_path):
    plan = {**_execution_plan(), "tasks": [_task()]}
    brief = _implementation_brief()
    codex_path = tmp_path / "codex.md"
    relay_path = tmp_path / "relay.json"

    CodexExporter().export(plan, brief, str(codex_path))
    RelayExporter().export(plan, brief, str(relay_path))

    codex_content = codex_path.read_text()
    relay_payload = json.loads(relay_path.read_text())

    assert "*Planning:* estimated hours: 2.5; risk: high; test: `poetry run pytest tests/test_api.py`" in codex_content
    assert relay_payload["tasks"][0]["estimated_hours"] == 2.5
    assert relay_payload["tasks"][0]["risk_level"] == "high"
    assert relay_payload["tasks"][0]["test_command"] == "poetry run pytest tests/test_api.py"
    assert "poetry run pytest tests/test_api.py" in relay_payload["validation"]["commands"]


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Task Planning Metadata",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Execution tasks need practical planning metadata.",
        "mvp_goal": "Persist and export task effort, risk, and validation command data.",
        "product_surface": "Python CLI",
        "scope": ["Task planning metadata"],
        "non_goals": ["Scheduling UI"],
        "assumptions": ["SQLite migrations are lightweight"],
        "architecture_notes": "Use existing task model and exporter patterns.",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Older databases may lack the new columns"],
        "validation_plan": "Run focused exporter and persistence tests",
        "definition_of_done": ["Task metadata persists and exports"],
        "status": "draft",
    }


def _execution_plan():
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation", "description": "Set up metadata"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement task planning metadata.",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _task():
    return {
        "id": "task-api",
        "title": "Persist planning metadata",
        "description": "Store effort, risk, and validation command fields.",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": ["src/blueprint/store/models.py", "tests/test_api.py"],
        "acceptance_criteria": ["Task metadata round trips through the store"],
        "estimated_complexity": "medium",
        "estimated_hours": 2.5,
        "risk_level": "high",
        "test_command": "poetry run pytest tests/test_api.py",
        "status": "pending",
    }
