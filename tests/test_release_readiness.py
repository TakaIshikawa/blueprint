import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.release_readiness import audit_release_readiness
from blueprint.cli import cli
from blueprint.store import init_db


def test_release_readiness_accepts_ready_plan(tmp_path):
    artifact = tmp_path / "plan-test-codex.md"
    artifact.write_text("# handoff\n")
    plan = _plan()
    plan["metadata"] = {"required_exports": ["codex"]}

    result = audit_release_readiness(
        plan,
        _brief(),
        [_export_record("codex", artifact)],
    )

    assert result.ok is True
    assert result.to_dict()["summary"] == {
        "blocking": 0,
        "warnings": 0,
        "findings": 0,
        "by_category": {},
    }


def test_release_readiness_detects_blocked_tasks():
    plan = _plan()
    plan["tasks"][0]["status"] = "blocked"
    plan["tasks"][0]["blocked_reason"] = "Waiting for credentials"

    result = audit_release_readiness(plan, _brief())

    assert result.ok is False
    assert ("blocking", "blocked_task", "task-command") in _finding_keys(result)


def test_release_readiness_detects_missing_acceptance_criteria():
    plan = _plan()
    plan["tasks"][0]["acceptance_criteria"] = []

    result = audit_release_readiness(plan, _brief())

    assert result.ok is False
    assert ("blocking", "missing_acceptance_criteria", "task-command") in _finding_keys(result)


def test_release_readiness_detects_missing_files_on_implementation_tasks():
    plan = _plan()
    plan["tasks"][0]["files_or_modules"] = []

    result = audit_release_readiness(plan, _brief())

    assert result.ok is False
    assert ("blocking", "missing_files_or_modules", "task-command") in _finding_keys(result)


def test_release_readiness_detects_unresolved_dependencies():
    plan = _plan()
    plan["tasks"][1]["depends_on"] = ["task-missing"]

    result = audit_release_readiness(plan, _brief())

    assert result.ok is False
    assert ("blocking", "unresolved_dependency", "task-risk") in _finding_keys(result)


def test_release_readiness_detects_missing_validation_strategy():
    plan = _plan()
    plan["test_strategy"] = ""

    result = audit_release_readiness(plan, _brief())

    assert result.ok is False
    assert ("blocking", "missing_validation_strategy", None) in _finding_keys(result)


def test_release_readiness_detects_uncovered_high_risk_items():
    brief = _brief()
    brief["risks"] = [
        {"severity": "high", "risk": "OAuth token expiry disrupts checkout"},
    ]

    result = audit_release_readiness(_plan(), brief)

    assert result.ok is False
    assert ("blocking", "uncovered_high_risk", None) in _finding_keys(result)
    assert result.findings[0].risk == "OAuth token expiry disrupts checkout"


def test_release_readiness_accepts_covered_high_risk_items():
    brief = _brief()
    brief["risks"] = ["High: Risk mitigation task may miss edge cases"]

    result = audit_release_readiness(_plan(), brief)

    assert result.ok is True


def test_release_readiness_detects_missing_required_exports():
    plan = _plan()
    plan["metadata"] = {"required_exports": ["codex", "relay"]}

    result = audit_release_readiness(plan, _brief())

    assert result.ok is False
    assert {finding.export_target for finding in result.findings} == {"codex", "relay"}
    assert ("blocking", "missing_required_export", None) in _finding_keys(result)


def test_release_readiness_detects_incomplete_required_export(tmp_path):
    plan = _plan()
    plan["metadata"] = {"required_exports": ["codex"]}

    result = audit_release_readiness(
        plan,
        _brief(),
        [_export_record("codex", tmp_path / "missing.md")],
    )

    assert result.ok is False
    assert ("blocking", "incomplete_required_export", None) in _finding_keys(result)


def test_release_readiness_cli_json_shape(tmp_path, monkeypatch):
    artifact = tmp_path / "plan-test-codex.md"
    artifact.write_text("# handoff\n")
    plan = _plan()
    plan["metadata"] = {"required_exports": ["codex"]}
    store = _seed_plan(tmp_path, monkeypatch, plan, _brief())
    store.insert_export_record(_export_record("codex", artifact))

    result = CliRunner().invoke(cli, ["plan", "release-readiness", "plan-test", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload["plan_id"] == "plan-test"
    assert payload["implementation_brief_id"] == "ib-test"
    assert payload["ok"] is True
    assert payload["summary"] == {
        "blocking": 0,
        "warnings": 0,
        "findings": 0,
        "by_category": {},
    }
    assert payload["findings"] == []


def test_release_readiness_cli_human_output_groups_findings(tmp_path, monkeypatch):
    plan = _plan()
    plan["tasks"][0]["status"] = "blocked"
    _seed_plan(tmp_path, monkeypatch, plan, _brief())

    result = CliRunner().invoke(cli, ["plan", "release-readiness", "plan-test"])

    assert result.exit_code == 1
    assert "Plan release readiness: plan-test" in result.output
    assert "Result: failed (1 blocking, 0 warnings)" in result.output
    assert "Blocking Findings:" in result.output
    assert "[tasks:blocked_task] task-command:" in result.output


def test_release_readiness_cli_exits_nonzero_for_blocking_findings(tmp_path, monkeypatch):
    plan = _plan()
    plan["test_strategy"] = None
    _seed_plan(tmp_path, monkeypatch, plan, _brief())

    result = CliRunner().invoke(
        cli,
        ["plan", "release-readiness", "plan-test", "--json"],
    )

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["ok"] is False
    assert payload["summary"]["blocking"] == 1


def _finding_keys(result):
    return {(finding.severity, finding.code, finding.task_id) for finding in result.findings}


def _seed_plan(tmp_path, monkeypatch, plan, brief):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(brief)
    store.insert_execution_plan(plan, plan["tasks"])
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


def _export_record(target, output_path):
    return {
        "id": f"exp-{target}",
        "execution_plan_id": "plan-test",
        "target_engine": target,
        "export_format": "markdown",
        "output_path": str(output_path),
        "export_metadata": {},
    }


def _brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Release Readiness",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need release readiness checks",
        "mvp_goal": "Expose release readiness in the CLI",
        "product_surface": "CLI",
        "scope": ["Release readiness command", "Risk mitigation task"],
        "non_goals": ["Task execution"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use existing audit helpers",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Risk mitigation task may miss edge cases"],
        "validation_plan": "Run release readiness CLI tests",
        "definition_of_done": [
            "Release readiness command is implemented",
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
            {"name": "Command", "description": "Build the release readiness command"},
            {"name": "Validation", "description": "Run release readiness CLI tests"},
        ],
        "test_strategy": "Run release readiness CLI tests",
        "handoff_prompt": "Implement the release readiness command and validate it",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "metadata": {},
        "tasks": [
            {
                "id": "task-command",
                "title": "Implement release readiness command",
                "description": (
                    "Implement the release readiness command and grouped audit output."
                ),
                "milestone": "Command",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["src/blueprint/cli.py"],
                "acceptance_criteria": [
                    "Release readiness command is implemented",
                ],
                "estimated_complexity": "medium",
                "status": "pending",
            },
            {
                "id": "task-risk",
                "title": "Cover risk mitigation task",
                "description": ("Cover risk mitigation task edge cases in readiness checks."),
                "milestone": "Validation",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": ["task-command"],
                "files_or_modules": ["tests/test_release_readiness.py"],
                "acceptance_criteria": [
                    "Risk mitigation task may miss edge cases is covered",
                    "Readiness CLI tests pass",
                ],
                "estimated_complexity": "medium",
                "status": "pending",
            },
        ],
    }
