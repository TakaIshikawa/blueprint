import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.store import init_db


def test_dependency_repair_without_apply_preserves_read_only_json(tmp_path, monkeypatch):
    tasks = _tasks()
    tasks[2]["depends_on"] = ["task-ap"]
    store = _seed_plan(tmp_path, monkeypatch, tasks)

    result = CliRunner().invoke(cli, ["plan", "dependency-repair", "plan-test", "--json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert set(payload) == {"ok", "plan_id", "summary", "suggestions"}
    assert payload["summary"] == {"suggestions": 1}
    assert payload["suggestions"][0]["action"] == "replace_dependency"
    assert store.get_execution_task("task-ui")["depends_on"] == ["task-ap"]


def test_dependency_repair_apply_replaces_dependency(tmp_path, monkeypatch):
    tasks = _tasks()
    tasks[2]["depends_on"] = ["task-ap"]
    store = _seed_plan(tmp_path, monkeypatch, tasks)

    result = CliRunner().invoke(
        cli,
        [
            "plan",
            "dependency-repair",
            "plan-test",
            "--apply",
            "--min-confidence",
            "0.8",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["summary"] == {"applied": 1, "audit_errors": 0, "audit_warnings": 0}
    assert payload["applied_edits"] == [
        {
            "action": "replace_dependency",
            "task_id": "task-ui",
            "dependency_id": "task-ap",
            "replacement_dependency_id": "task-api",
            "confidence": 0.86,
            "before_depends_on": ["task-ap"],
            "after_depends_on": ["task-api"],
        }
    ]
    assert payload["audit"]["ok"] is True
    assert store.get_execution_task("task-ui")["depends_on"] == ["task-api"]


def test_dependency_repair_apply_removes_dependency(tmp_path, monkeypatch):
    tasks = _tasks()
    tasks[2]["depends_on"] = ["external-launch-gate"]
    store = _seed_plan(tmp_path, monkeypatch, tasks)

    result = CliRunner().invoke(
        cli,
        [
            "plan",
            "dependency-repair",
            "plan-test",
            "--apply",
            "--min-confidence",
            "0.7",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["summary"]["applied"] == 1
    assert payload["applied_edits"][0]["action"] == "remove_dependency"
    assert payload["applied_edits"][0]["before_depends_on"] == ["external-launch-gate"]
    assert payload["applied_edits"][0]["after_depends_on"] == []
    assert store.get_execution_task("task-ui")["depends_on"] == []


def test_dependency_repair_apply_respects_min_confidence(tmp_path, monkeypatch):
    tasks = _tasks()
    tasks[2]["depends_on"] = ["external-launch-gate"]
    store = _seed_plan(tmp_path, monkeypatch, tasks)

    result = CliRunner().invoke(
        cli,
        [
            "plan",
            "dependency-repair",
            "plan-test",
            "--apply",
            "--min-confidence",
            "0.8",
            "--json",
        ],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["summary"] == {"applied": 0, "audit_errors": 1, "audit_warnings": 0}
    assert payload["applied_edits"] == []
    assert payload["audit"]["issues"][0]["code"] == "unknown_dependency"
    assert store.get_execution_task("task-ui")["depends_on"] == ["external-launch-gate"]


def test_dependency_repair_apply_reports_post_apply_audit(tmp_path, monkeypatch):
    tasks = _tasks()
    tasks[1]["depends_on"] = ["task-api"]
    _seed_plan(tmp_path, monkeypatch, tasks)

    result = CliRunner().invoke(
        cli,
        ["plan", "dependency-repair", "plan-test", "--apply", "--json"],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["ok"] is True
    assert payload["audit"] == {
        "plan_id": "plan-test",
        "ok": True,
        "summary": {"errors": 0, "warnings": 0},
        "issues": [],
    }


def test_dependency_repair_apply_preserves_status_history_and_metadata(
    tmp_path,
    monkeypatch,
):
    tasks = _tasks()
    tasks[2]["depends_on"] = ["task-ap"]
    store = _seed_plan(tmp_path, monkeypatch, tasks)
    store.update_execution_task_status(
        "task-ui",
        "blocked",
        blocked_reason="Waiting on API",
        reason="Dependency not ready",
    )
    before_history = store.list_status_events("task-ui")

    result = CliRunner().invoke(
        cli,
        ["plan", "dependency-repair", "plan-test", "--apply", "--json"],
    )

    assert result.exit_code == 0
    task = store.get_execution_task("task-ui")
    assert task["status"] == "blocked"
    assert task["blocked_reason"] == "Waiting on API"
    assert store.list_status_events("task-ui") == before_history


def _seed_plan(tmp_path, monkeypatch, tasks):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), tasks)
    return store


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


def _execution_plan():
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Interface", "description": "Build the user-facing flow"},
        ],
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
            "status": "pending",
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
            "status": "in_progress",
        },
        {
            "id": "task-ui",
            "title": "Build UI",
            "description": "Render the interface",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI displays API data"],
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
        "problem_statement": "Need task management",
        "mvp_goal": "Expose tasks in the CLI",
        "product_surface": "CLI",
        "scope": ["Task commands"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Incorrect status updates"],
        "validation_plan": "Run CLI tests",
        "definition_of_done": ["Tests pass"],
        "status": "ready_for_planning",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
