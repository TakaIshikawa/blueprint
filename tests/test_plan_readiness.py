import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.plan_readiness import evaluate_plan_readiness
from blueprint.cli import cli
from blueprint.store import init_db


def test_plan_readiness_accepts_all_pass_plan():
    result = evaluate_plan_readiness(_plan(), _brief())

    assert result.ready is True
    assert result.blocking_reasons == []
    assert result.plan_audit.ok is True
    assert result.task_completeness.passed is True
    assert result.brief_plan_coherence.ok is True
    assert result.risk_coverage.ok is True
    assert result.env_inventory_counts.to_dict() == {
        "required": 0,
        "optional": 0,
        "unknown": 0,
        "missing_required": 0,
    }


def test_plan_readiness_fails_for_structural_plan_errors():
    plan = _plan()
    plan["tasks"][1]["depends_on"] = ["task-missing"]

    result = evaluate_plan_readiness(plan, _brief())

    assert result.ready is False
    assert ("plan_audit", "unknown_dependency") in _blocking_codes(result)


def test_plan_readiness_fails_for_uncovered_implementation_risk():
    brief = _brief()
    brief["risks"] = ["OAuth token expiry"]

    result = evaluate_plan_readiness(_plan(), brief)

    assert result.ready is False
    assert ("risk_coverage", "uncovered_risk") in _blocking_codes(result)
    assert "OAuth token expiry" in result.blocking_reasons[0].message


def test_plan_readiness_fails_for_missing_required_env_var(monkeypatch):
    monkeypatch.delenv("REQUIRED_SERVICE_TOKEN", raising=False)
    brief = _brief()
    brief["data_requirements"] = "Requires REQUIRED_SERVICE_TOKEN for validation."

    result = evaluate_plan_readiness(_plan(), brief)

    assert result.ready is False
    assert result.env_inventory_counts.required == 1
    assert result.env_inventory_counts.missing_required == 1
    assert ("env_inventory", "missing_required_env_var") in _blocking_codes(result)


def test_plan_readiness_cli_json_shape(tmp_path, monkeypatch):
    _seed_plan(tmp_path, monkeypatch, _plan(), _brief())

    result = CliRunner().invoke(cli, ["plan", "readiness", "plan-test", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["plan_id"] == "plan-test"
    assert payload["implementation_brief_id"] == "ib-test"
    assert payload["ready"] is True
    assert payload["blocking_reasons"] == []
    assert set(payload["components"]) == {
        "plan_audit",
        "task_completeness",
        "brief_plan_coherence",
        "risk_coverage",
        "env_inventory",
        "env_inventory_counts",
    }
    assert payload["components"]["env_inventory_counts"] == {
        "required": 0,
        "optional": 0,
        "unknown": 0,
        "missing_required": 0,
    }


def test_plan_readiness_cli_missing_plan_exits_nonzero(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(cli, ["plan", "readiness", "plan-missing"])

    assert result.exit_code == 1
    assert "Execution plan not found: plan-missing" in result.output


def test_plan_readiness_cli_missing_linked_brief_exits_nonzero(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    plan = _plan()
    plan["implementation_brief_id"] = "ib-missing"
    store.insert_execution_plan(plan, plan["tasks"])

    result = CliRunner().invoke(cli, ["plan", "readiness", "plan-test"])

    assert result.exit_code == 1
    assert "Implementation brief not found: ib-missing" in result.output


def _blocking_codes(result):
    return {
        (reason.component, reason.code)
        for reason in result.blocking_reasons
    }


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


def _brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Task CLI",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need task readiness checks",
        "mvp_goal": "Expose task readiness in the CLI",
        "product_surface": "CLI",
        "scope": ["Task readiness command", "Risk mitigation task"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use existing audit helpers",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Risk mitigation task may miss edge cases"],
        "validation_plan": "Run readiness CLI tests",
        "definition_of_done": [
            "Task readiness command is implemented",
            "Readiness CLI tests pass",
        ],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _plan():
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Command", "description": "Build the task readiness command"},
            {"name": "Validation", "description": "Run readiness CLI tests"},
        ],
        "test_strategy": "Run readiness CLI tests",
        "handoff_prompt": "Implement the task readiness command and validate it",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": [
            {
                "id": "task-command",
                "title": "Implement task readiness command",
                "description": (
                    "Implement the task readiness command and aggregate audit output."
                ),
                "milestone": "Command",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["src/blueprint/cli.py"],
                "acceptance_criteria": [
                    "Task readiness command is implemented",
                ],
                "estimated_complexity": "medium",
                "status": "pending",
            },
            {
                "id": "task-risk",
                "title": "Cover risk mitigation task",
                "description": (
                    "Cover risk mitigation task edge cases in readiness checks."
                ),
                "milestone": "Validation",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": ["task-command"],
                "files_or_modules": ["tests/test_plan_readiness.py"],
                "acceptance_criteria": [
                    "Risk mitigation task may miss edge cases is covered",
                    "Readiness CLI tests pass",
                ],
                "estimated_complexity": "medium",
                "status": "pending",
            },
        ],
    }
