import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.brief_plan_coherence import audit_brief_plan_coherence
from blueprint.cli import cli
from blueprint.store import init_db


def test_brief_plan_coherence_accepts_coherent_plan():
    result = audit_brief_plan_coherence(_coherent_plan(), _coherent_brief())

    assert result.ok is True
    assert result.error_count == 0
    assert result.warning_count == 0
    assert result.issues == []


def test_brief_plan_coherence_flags_uncovered_scope_items():
    plan = _coherent_plan()
    plan["milestones"].pop(2)
    plan["tasks"].pop(2)

    result = audit_brief_plan_coherence(plan, _coherent_brief())

    assert result.ok is False
    assert result.error_count == 1
    assert _issue_codes(result) == {"scope_item_uncovered"}
    [issue] = result.issues
    assert issue.scope_item == "Task metrics"


def test_brief_plan_coherence_flags_surface_conflict_and_cli_json(tmp_path, monkeypatch):
    _seed_plan(
        tmp_path,
        monkeypatch,
        _coherent_plan(project_type="web_app"),
        _coherent_brief(),
    )

    result = CliRunner().invoke(cli, ["plan", "coherence", "plan-test", "--json"])

    assert result.exit_code == 1, result.output
    payload = json.loads(result.output)
    assert payload["plan_id"] == "plan-test"
    assert payload["implementation_brief_id"] == "ib-test"
    assert payload["ok"] is False
    assert payload["summary"] == {"errors": 1, "warnings": 0}
    assert payload["issues"][0]["code"] == "product_surface_conflict"
    assert payload["issues"][0]["project_type"] == "web_app"
    assert payload["issues"][0]["product_surface"] == "CLI"


def _issue_codes(result):
    return {issue.code for issue in result.issues}


def _seed_plan(tmp_path, monkeypatch, plan, brief):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(brief)
    store.insert_execution_plan(plan, plan["tasks"])


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


def _coherent_plan(*, project_type: str = "cli_tool"):
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": project_type,
        "milestones": [
            {"name": "Commands", "description": "Implement task commands"},
            {"name": "Blockers", "description": "Implement task blockers"},
            {"name": "Metrics", "description": "Track task metrics"},
            {"name": "Validation", "description": "Run task CLI tests"},
        ],
        "test_strategy": "Run task CLI tests",
        "handoff_prompt": "Build the task CLI and keep the scope aligned",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": _coherent_tasks(),
    }


def _coherent_tasks():
    return [
        {
            "id": "task-commands",
            "title": "Implement task commands",
            "description": "Task commands are implemented",
            "milestone": "Commands",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/blueprint/cli.py"],
            "acceptance_criteria": ["Task commands are implemented"],
            "estimated_complexity": "medium",
            "status": "completed",
        },
        {
            "id": "task-blockers",
            "title": "Implement task blockers",
            "description": "Task blockers are implemented",
            "milestone": "Blockers",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-commands"],
            "files_or_modules": ["src/blueprint/cli.py"],
            "acceptance_criteria": ["Task blockers are implemented"],
            "estimated_complexity": "medium",
            "status": "completed",
        },
        {
            "id": "task-metrics",
            "title": "Track task metrics",
            "description": "Track task metrics for the plan",
            "milestone": "Metrics",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-blockers"],
            "files_or_modules": ["src/blueprint/audits/brief_plan_coherence.py"],
            "acceptance_criteria": ["Task metrics are visible"],
            "estimated_complexity": "low",
            "status": "completed",
        },
        {
            "id": "task-tests",
            "title": "Add task CLI tests",
            "description": "Run task CLI tests and confirm they pass",
            "milestone": "Validation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-metrics"],
            "files_or_modules": ["tests/test_brief_plan_coherence.py"],
            "acceptance_criteria": ["Task CLI tests pass"],
            "estimated_complexity": "medium",
            "status": "completed",
        },
    ]


def _coherent_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Task CLI",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need task management",
        "mvp_goal": "Expose task commands in the CLI",
        "product_surface": "CLI",
        "scope": ["Task commands", "Task blockers", "Task metrics"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use store methods",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Incorrect status updates"],
        "validation_plan": "Run task CLI tests",
        "definition_of_done": [
            "Task commands are implemented",
            "Task blockers are implemented",
            "Task CLI tests pass",
        ],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
