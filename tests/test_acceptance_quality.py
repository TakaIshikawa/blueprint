import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.acceptance_quality import audit_acceptance_quality
from blueprint.cli import cli
from blueprint.store import init_db


def test_acceptance_quality_accepts_observable_criteria():
    result = audit_acceptance_quality(_clean_plan())

    assert result.passed is True
    assert result.high_count == 0
    assert result.medium_count == 0
    assert result.findings == []
    assert [task.passed for task in result.tasks] == [True, True]


def test_acceptance_quality_reports_weak_criteria_per_task():
    result = audit_acceptance_quality(_failing_plan(), min_length=8)

    assert result.passed is False
    assert result.high_count == 10
    assert result.medium_count == 1
    assert _finding_codes(result) == {
        "missing_acceptance_criteria",
        "criterion_too_short",
        "vague_phrase",
        "duplicate_criterion",
        "non_observable_criterion",
    }
    assert all(finding.task_id for finding in result.findings)
    assert all(finding.severity in {"high", "medium"} for finding in result.findings)
    assert all(finding.reason for finding in result.findings)
    assert result.tasks[0].findings[0].criterion_text == ""


def test_acceptance_quality_cli_json_is_machine_readable(tmp_path, monkeypatch):
    _seed_plan(tmp_path, monkeypatch, _clean_plan())

    result = CliRunner().invoke(
        cli,
        ["plan", "acceptance-audit", "plan-quality", "--json"],
    )

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["plan_id"] == "plan-quality"
    assert payload["passed"] is True
    assert payload["summary"] == {"high": 0, "medium": 0, "tasks": 2}
    assert payload["findings"] == []
    assert payload["tasks"][0]["task_id"] == "task-api"


def test_acceptance_quality_cli_groups_human_output_and_exits_on_high(
    tmp_path,
    monkeypatch,
):
    _seed_plan(tmp_path, monkeypatch, _failing_plan())

    result = CliRunner().invoke(
        cli,
        ["plan", "acceptance-audit", "plan-quality", "--min-length", "8"],
    )

    assert result.exit_code == 1, result.output
    assert "Acceptance criteria quality audit: plan-quality" in result.output
    assert "Result: failed (10 high, 1 medium findings)" in result.output
    assert "Findings by task:" in result.output
    assert "task-empty (Empty criteria):" in result.output
    assert "task-weak (Weak criteria):" in result.output
    assert "[high] missing_acceptance_criteria: <missing>" in result.output
    assert "[medium] duplicate_criterion: Works" in result.output
    assert result.output.index("task-empty") < result.output.index("task-weak")


def test_acceptance_quality_cli_rejects_missing_plan(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(cli, ["plan", "acceptance-audit", "missing-plan"])

    assert result.exit_code != 0
    assert "Execution plan not found: missing-plan" in result.output


def _finding_codes(result):
    return {finding.code for finding in result.findings}


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


def _clean_plan():
    plan = _base_plan()
    plan["tasks"] = [
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["Assert API returns serialized task data"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
        {
            "id": "task-cli",
            "title": "Build CLI",
            "description": "Render task data in the CLI",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/cli.py"],
            "acceptance_criteria": ["Verify CLI displays the generated report"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
    ]
    return plan


def _failing_plan():
    plan = _base_plan()
    plan["tasks"] = [
        {
            "id": "task-empty",
            "title": "Empty criteria",
            "description": "Create the empty path",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": [],
            "estimated_complexity": "low",
            "status": "pending",
        },
        {
            "id": "task-weak",
            "title": "Weak criteria",
            "description": "Create weak criteria examples",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-empty"],
            "files_or_modules": ["src/cli.py"],
            "acceptance_criteria": [
                "Works",
                "Works",
                "Refactor as needed",
                "Readable docs maybe",
            ],
            "estimated_complexity": "medium",
            "status": "pending",
        },
    ]
    return plan


def _base_plan():
    return {
        "id": "plan-quality",
        "implementation_brief_id": "ib-quality",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation", "description": "Set up the project"}],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _implementation_brief():
    return {
        "id": "ib-quality",
        "source_brief_id": "sb-quality",
        "title": "Quality Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need quality criteria",
        "mvp_goal": "Audit acceptance criteria",
        "product_surface": "CLI",
        "scope": ["Audit command"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tests run locally"],
        "risks": ["Vague tasks may be queued"],
        "definition_of_done": ["Acceptance audit runs"],
        "validation_plan": "Run pytest",
        "status": "draft",
    }
