import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.store import init_db


def test_task_blockers_reports_direct_impact(tmp_path, monkeypatch):
    _seed_blocker_plan(
        tmp_path,
        monkeypatch,
        [
            _task(
                "task-blocked",
                "Blocked task",
                status="blocked",
                metadata={"blocked_reason": "Waiting for credentials"},
            ),
            _task("task-dependent", "Dependent task", depends_on=["task-blocked"]),
        ],
    )

    result = CliRunner().invoke(
        cli,
        ["task", "blockers", "plan-blockers", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == [
        {
            "blocked_task_id": "task-blocked",
            "blocked_reason": "Waiting for credentials",
            "direct_dependents": ["task-dependent"],
            "transitive_dependents": [],
            "impacted_count": 1,
        }
    ]


def test_task_blockers_reports_transitive_impact(tmp_path, monkeypatch):
    _seed_blocker_plan(
        tmp_path,
        monkeypatch,
        [
            _task("task-blocked", "Blocked task", status="blocked"),
            _task("task-direct", "Direct dependent", depends_on=["task-blocked"]),
            _task("task-transitive", "Transitive dependent", depends_on=["task-direct"]),
        ],
    )

    result = CliRunner().invoke(
        cli,
        ["task", "blockers", "plan-blockers", "--json"],
    )

    assert result.exit_code == 0, result.output
    [blocker] = json.loads(result.output)
    assert blocker["direct_dependents"] == ["task-direct"]
    assert blocker["transitive_dependents"] == ["task-transitive"]
    assert blocker["impacted_count"] == 2


def test_task_blockers_reports_multiple_blockers(tmp_path, monkeypatch):
    _seed_blocker_plan(
        tmp_path,
        monkeypatch,
        [
            _task("task-auth", "Auth", status="blocked"),
            _task("task-api", "API", depends_on=["task-auth"]),
            _task("task-ui", "UI", depends_on=["task-api"]),
            _task(
                "task-schema",
                "Schema",
                status="blocked",
                metadata={"blocked_reason": "Awaiting product decision"},
            ),
            _task("task-import", "Import", depends_on=["task-schema"]),
        ],
    )

    result = CliRunner().invoke(
        cli,
        ["task", "blockers", "plan-blockers", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = {
        blocker["blocked_task_id"]: blocker for blocker in json.loads(result.output)
    }
    assert set(payload) == {"task-auth", "task-schema"}
    assert payload["task-auth"]["direct_dependents"] == ["task-api"]
    assert payload["task-auth"]["transitive_dependents"] == ["task-ui"]
    assert payload["task-auth"]["impacted_count"] == 2
    assert payload["task-schema"]["blocked_reason"] == "Awaiting product decision"
    assert payload["task-schema"]["direct_dependents"] == ["task-import"]
    assert payload["task-schema"]["transitive_dependents"] == []
    assert payload["task-schema"]["impacted_count"] == 1


def test_task_blockers_reports_no_blocked_tasks(tmp_path, monkeypatch):
    _seed_blocker_plan(
        tmp_path,
        monkeypatch,
        [
            _task("task-setup", "Setup", status="completed"),
            _task("task-api", "API", depends_on=["task-setup"]),
        ],
    )

    result = CliRunner().invoke(cli, ["task", "blockers", "plan-blockers"])

    assert result.exit_code == 0, result.output
    assert "No blocked execution tasks found in plan plan-blockers" in result.output


def test_task_blockers_rejects_missing_plan(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(cli, ["task", "blockers", "plan-missing"])

    assert result.exit_code != 0
    assert "Error: Execution plan not found: plan-missing" in result.output


def _seed_blocker_plan(tmp_path, monkeypatch, tasks):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
    store.insert_execution_plan(_execution_plan(), tasks)


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
        "id": "plan-blockers",
        "implementation_brief_id": "ib-blockers",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Delivery", "description": "Ship the workflow"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _task(
    task_id,
    title,
    *,
    depends_on=None,
    status="pending",
    metadata=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": f"{title} description",
        "milestone": "Foundation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": [],
        "acceptance_criteria": ["Task is complete"],
        "estimated_complexity": "medium",
        "status": status,
        "metadata": metadata or {},
    }


def _implementation_brief():
    return {
        "id": "ib-blockers",
        "source_brief_id": "sb-blockers",
        "title": "Blocker Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need blocker analysis",
        "mvp_goal": "Expose blockers in the CLI",
        "product_surface": "CLI",
        "scope": ["Task blockers"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Incorrect impact analysis"],
        "validation_plan": "Run task blocker tests",
        "definition_of_done": ["CLI lists blocked task impact"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
