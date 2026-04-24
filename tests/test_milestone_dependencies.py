import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.milestone_dependencies import audit_milestone_dependencies
from blueprint.cli import cli
from blueprint.store import init_db


def test_milestone_dependencies_accepts_valid_milestone_order():
    result = audit_milestone_dependencies(_plan())

    assert result.ok is True
    assert result.findings == []
    assert result.to_dict()["summary"] == {
        "errors": 0,
        "warnings": 0,
        "findings": 0,
    }


def test_milestone_dependencies_detects_reversed_dependency():
    plan = _plan()
    plan["tasks"][0]["depends_on"] = ["task-validate"]

    result = audit_milestone_dependencies(plan)

    assert result.ok is False
    assert result.findings[0].to_dict() == {
        "code": "reversed_milestone_dependency",
        "severity": "error",
        "message": (
            "Task task-design in milestone Design depends on task-validate in "
            "later milestone Validation."
        ),
        "milestone": "Design",
        "task_id": "task-design",
        "dependency_task_id": "task-validate",
        "dependency_milestone": "Validation",
    }


def test_milestone_dependencies_reports_missing_and_empty_milestones():
    plan = _plan()
    plan["tasks"][1]["milestone"] = "Implementation"

    result = audit_milestone_dependencies(plan)

    assert [finding.code for finding in result.findings] == [
        "missing_milestone",
        "empty_milestone",
    ]
    assert result.findings[0].to_dict()["task_id"] == "task-build"
    assert result.findings[1].milestone == "Build"


def test_milestone_dependencies_reports_cross_milestone_dependency_chains():
    result = audit_milestone_dependencies(_plan())

    assert result.ok is True
    assert result.findings == []

    plan = _plan()
    plan["tasks"][2]["depends_on"] = ["task-build"]
    result = audit_milestone_dependencies(plan)

    assert result.ok is False
    assert result.findings[0].code == "cross_milestone_chain"
    assert result.findings[0].chain_task_ids == [
        "task-validate",
        "task-build",
        "task-design",
    ]
    assert result.findings[0].chain_milestones == ["Validation", "Build", "Design"]


def test_milestone_dependencies_cli_outputs_stable_json(tmp_path, monkeypatch):
    plan = _plan()
    plan["tasks"][0]["depends_on"] = ["task-validate"]
    _seed_plan(tmp_path, monkeypatch, plan)

    result = CliRunner().invoke(
        cli,
        ["plan", "milestone-dependencies", "plan-test", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["plan_id"] == "plan-test"
    assert payload["ok"] is False
    assert payload["summary"] == {"errors": 1, "warnings": 1, "findings": 2}
    assert payload["findings"][0]["code"] == "reversed_milestone_dependency"
    assert payload["findings"][0]["task_id"] == "task-design"
    assert payload["findings"][0]["dependency_task_id"] == "task-validate"
    assert payload["findings"][1]["code"] == "cross_milestone_chain"
    assert payload["findings"][1]["task_id"] == "task-build"
    assert payload["findings"][1]["chain_task_ids"] == [
        "task-build",
        "task-design",
        "task-validate",
    ]


def test_milestone_dependencies_cli_human_output_groups_by_milestone(
    tmp_path,
    monkeypatch,
):
    plan = _plan()
    plan["milestones"].append(
        {"name": "Rollout", "description": "Launch the completed work"}
    )
    plan["tasks"][0]["depends_on"] = ["task-validate"]
    _seed_plan(tmp_path, monkeypatch, plan)

    result = CliRunner().invoke(
        cli,
        ["plan", "milestone-dependencies", "plan-test"],
    )

    assert result.exit_code == 1
    assert "Milestone dependency audit: plan-test" in result.output
    assert "Result: failed (1 errors, 2 warnings)" in result.output
    assert "Findings by milestone:" in result.output
    assert "  Design:" in result.output
    assert "reversed_milestone_dependency" in result.output
    assert "  Rollout:" in result.output
    assert "empty_milestone" in result.output


def _seed_plan(tmp_path, monkeypatch, plan):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())
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


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Milestone ordering",
        "domain": "planning",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "Plan review",
        "problem_statement": "Need milestone dependency checks",
        "mvp_goal": "Audit milestone dependency order",
        "product_surface": "CLI",
        "scope": ["Milestone audit"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use audit helper",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Invalid milestone order"],
        "validation_plan": "Run milestone dependency tests",
        "definition_of_done": ["Milestone dependency tests pass"],
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
            {"name": "Design", "description": "Design the change"},
            {"name": "Build", "description": "Build the change"},
            {"name": "Validation", "description": "Validate the change"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Implement and validate the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": [
            {
                "id": "task-design",
                "title": "Design audit",
                "description": "Design the milestone dependency audit",
                "milestone": "Design",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["src/blueprint/audits"],
                "acceptance_criteria": ["Audit design is complete"],
                "estimated_complexity": "low",
                "status": "pending",
            },
            {
                "id": "task-build",
                "title": "Build audit",
                "description": "Implement the milestone dependency audit",
                "milestone": "Build",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": ["task-design"],
                "files_or_modules": ["src/blueprint/audits"],
                "acceptance_criteria": ["Audit implementation exists"],
                "estimated_complexity": "medium",
                "status": "pending",
            },
            {
                "id": "task-validate",
                "title": "Validate audit",
                "description": "Test the milestone dependency audit",
                "milestone": "Validation",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["tests/test_milestone_dependencies.py"],
                "acceptance_criteria": ["Audit tests pass"],
                "estimated_complexity": "medium",
                "status": "pending",
            },
        ],
    }
