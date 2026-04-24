import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.risk_coverage import audit_risk_coverage
from blueprint.cli import cli
from blueprint.store import init_db


def test_risk_coverage_matches_description_acceptance_criteria_and_mitigation_metadata():
    result = audit_risk_coverage(_brief(), _plan())

    assert result.ok is True
    assert result.coverage_ratio == 1.0
    assert [risk.risk for risk in result.covered_risks] == [
        "API timeouts during export",
        "Duplicate tasks are queued",
        "Sensitive tokens leak into logs",
    ]
    assert result.covered_risks[0].matching_task_ids == ["task-timeouts"]
    assert result.covered_risks[1].matching_task_ids == ["task-duplicates"]
    assert result.covered_risks[2].matching_task_ids == ["task-secrets"]
    assert result.uncovered_risks == []


def test_risk_coverage_reports_uncovered_risks():
    plan = _plan()
    plan["tasks"] = plan["tasks"][:2]

    result = audit_risk_coverage(_brief(), plan)

    assert result.ok is False
    assert result.coverage_ratio == 2 / 3
    assert [risk.to_dict() for risk in result.uncovered_risks] == [
        {
            "risk": "Sensitive tokens leak into logs",
            "matching_task_ids": [],
        }
    ]


def test_risk_coverage_cli_json_lists_covered_and_uncovered_risks(tmp_path, monkeypatch):
    plan = _plan()
    plan["tasks"] = plan["tasks"][:2]
    _seed_plan(tmp_path, monkeypatch, plan, _brief())

    result = CliRunner().invoke(
        cli,
        ["brief", "risk-coverage", "ib-risk", "--plan-id", "plan-risk", "--json"],
    )

    assert result.exit_code == 1, result.output
    payload = json.loads(result.output)
    assert payload["brief_id"] == "ib-risk"
    assert payload["plan_id"] == "plan-risk"
    assert payload["coverage_ratio"] == 2 / 3
    assert payload["covered_risks"] == [
        {
            "risk": "API timeouts during export",
            "matching_task_ids": ["task-timeouts"],
        },
        {
            "risk": "Duplicate tasks are queued",
            "matching_task_ids": ["task-duplicates"],
        },
    ]
    assert payload["uncovered_risks"] == [
        {
            "risk": "Sensitive tokens leak into logs",
            "matching_task_ids": [],
        }
    ]


def test_risk_coverage_cli_human_output_lists_uncovered_risks(tmp_path, monkeypatch):
    plan = _plan()
    plan["tasks"] = plan["tasks"][:2]
    _seed_plan(tmp_path, monkeypatch, plan, _brief())

    result = CliRunner().invoke(
        cli,
        ["brief", "risk-coverage", "ib-risk", "--plan-id", "plan-risk"],
    )

    assert result.exit_code == 1, result.output
    assert "Risk coverage audit: ib-risk against plan-risk" in result.output
    assert "Result: failed (2/3 risks covered, 66.67%)" in result.output
    assert "Uncovered risks:" in result.output
    assert "Sensitive tokens leak into logs" in result.output


def test_risk_coverage_cli_rejects_missing_brief(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(
        cli,
        ["brief", "risk-coverage", "missing-brief", "--plan-id", "plan-risk"],
    )

    assert result.exit_code != 0
    assert "Implementation brief not found: missing-brief" in result.output


def test_risk_coverage_cli_rejects_missing_plan(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_brief())

    result = CliRunner().invoke(
        cli,
        ["brief", "risk-coverage", "ib-risk", "--plan-id", "missing-plan"],
    )

    assert result.exit_code != 0
    assert "Execution plan not found: missing-plan" in result.output


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
        "id": "ib-risk",
        "source_brief_id": "sb-risk",
        "title": "Export queue",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "Release workflow",
        "problem_statement": "Exports need safer execution",
        "mvp_goal": "Audit export queue plans before handoff",
        "product_surface": "CLI",
        "scope": ["Risk coverage audit"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": "Use deterministic matching",
        "data_requirements": "Execution tasks and brief risks",
        "integration_points": [],
        "risks": [
            "API timeouts during export",
            "Duplicate tasks are queued",
            "Sensitive tokens leak into logs",
        ],
        "validation_plan": "Run pytest",
        "definition_of_done": ["Risk coverage command reports uncovered risks"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _plan():
    return {
        "id": "plan-risk",
        "implementation_brief_id": "ib-risk",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Audit", "description": "Add risk coverage audit"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Cover implementation risks",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "tasks": [
            {
                "id": "task-timeouts",
                "title": "Add retry handling",
                "description": "Handle API timeout during export with a bounded retry.",
                "milestone": "Audit",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["src/blueprint/cli.py"],
                "acceptance_criteria": ["Retry behavior is tested"],
                "estimated_complexity": "medium",
                "status": "pending",
            },
            {
                "id": "task-duplicates",
                "title": "Prevent duplicate queue entries",
                "description": "Validate queue input",
                "milestone": "Audit",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["src/blueprint/store/db.py"],
                "acceptance_criteria": ["Duplicate tasks are not queued twice"],
                "estimated_complexity": "medium",
                "status": "pending",
            },
            {
                "id": "task-secrets",
                "title": "Redact logs",
                "description": "Add export logging",
                "milestone": "Audit",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["src/blueprint/cli.py"],
                "acceptance_criteria": ["Logs include audit context"],
                "estimated_complexity": "medium",
                "status": "pending",
                "metadata": {
                    "mitigation": "Redact sensitive token values before writing logs."
                },
            },
        ],
    }
