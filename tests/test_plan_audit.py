import json
from copy import deepcopy

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.plan_audit import audit_execution_plan
from blueprint.cli import cli
from blueprint.store import init_db


def test_plan_audit_accepts_clean_plan():
    result = audit_execution_plan(_plan_with_tasks())

    assert result.ok is True
    assert result.error_count == 0
    assert result.warning_count == 0
    assert result.issues == []


def test_plan_audit_reports_missing_dependencies():
    plan = _plan_with_tasks()
    plan["tasks"][1]["depends_on"] = ["task-missing"]

    result = audit_execution_plan(plan)

    assert result.ok is False
    assert _issue_codes(result) == {"unknown_dependency"}
    [issue] = result.issues
    assert issue.task_id == "task-api"
    assert issue.dependency_id == "task-missing"


def test_plan_audit_reports_dependency_cycles():
    plan = _plan_with_tasks()
    plan["tasks"][0]["depends_on"] = ["task-ui"]

    result = audit_execution_plan(plan)

    cycle_issues = [issue for issue in result.issues if issue.code == "dependency_cycle"]
    assert result.ok is False
    assert len(cycle_issues) == 1
    assert cycle_issues[0].cycle == ["task-api", "task-setup", "task-ui", "task-api"]


def test_plan_audit_reports_milestone_mismatch():
    plan = _plan_with_tasks()
    plan["tasks"][2]["milestone"] = "Launch"

    result = audit_execution_plan(plan)

    assert result.ok is False
    assert _issue_codes(result) == {"unknown_milestone"}
    [issue] = result.issues
    assert issue.task_id == "task-ui"
    assert issue.milestone == "Launch"


def test_plan_audit_reports_duplicate_ids_empty_acceptance_and_missing_blocked_reason():
    plan = _plan_with_tasks()
    plan["tasks"].append(deepcopy(plan["tasks"][2]))
    plan["tasks"][1]["depends_on"] = ["task-api"]
    plan["tasks"][1]["acceptance_criteria"] = []
    plan["tasks"][1]["status"] = "blocked"

    result = audit_execution_plan(plan)

    assert result.ok is False
    assert "duplicate_task_id" in _issue_codes(result)
    assert "self_dependency" in _issue_codes(result)
    assert "empty_acceptance_criteria" in _issue_codes(result)
    assert "blocked_without_reason" in _issue_codes(result)
    assert result.warning_count == 1


def test_plan_audit_cli_outputs_json(tmp_path, monkeypatch):
    _seed_plan(tmp_path, monkeypatch, _tasks())

    result = CliRunner().invoke(cli, ["plan", "audit", "plan-test", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == {
        "plan_id": "plan-test",
        "ok": True,
        "summary": {"errors": 0, "warnings": 0},
        "issues": [],
    }


def test_plan_audit_cli_exits_nonzero_for_errors(tmp_path, monkeypatch):
    tasks = _tasks()
    tasks[1]["depends_on"] = ["task-missing"]
    _seed_plan(tmp_path, monkeypatch, tasks)

    result = CliRunner().invoke(cli, ["plan", "audit", "plan-test"])

    assert result.exit_code == 1
    assert "Execution plan audit: plan-test" in result.output
    assert "Result: failed (1 errors, 0 warnings)" in result.output
    assert "Errors:" in result.output
    assert "[unknown_dependency] Task task-api depends on missing task task-missing" in (
        result.output
    )


def _issue_codes(result):
    return {issue.code for issue in result.issues}


def _seed_plan(tmp_path, monkeypatch, tasks):
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


def _plan_with_tasks():
    plan = _execution_plan()
    plan["tasks"] = _tasks()
    return plan


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
        "validation_plan": "Run task CLI tests",
        "definition_of_done": ["CLI lists, inspects, and updates tasks"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
