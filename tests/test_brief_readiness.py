import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.audits.brief_readiness import audit_brief_readiness
from blueprint.cli import cli
from blueprint.store import init_db


def test_brief_readiness_accepts_clean_brief():
    result = audit_brief_readiness(_ready_brief())

    assert result.passed is True
    assert result.blocking_count == 0
    assert result.warning_count == 0
    assert result.findings == []
    assert result.to_dict()["summary"] == {"blocking": 0, "warning": 0}


def test_brief_readiness_reports_actionable_findings():
    brief = _ready_brief()
    brief.update(
        {
            "product_surface": " ",
            "scope": ["Task commands", "task commands", "Task metrics"],
            "non_goals": ["Task metrics"],
            "risks": ["risk"],
            "validation_plan": " ",
            "definition_of_done": [],
        }
    )

    result = audit_brief_readiness(brief)

    assert result.passed is False
    assert result.blocking_count == 3
    assert result.warning_count == 3
    assert _finding_codes(result) == {
        "missing_definition_of_done",
        "missing_validation_plan",
        "missing_product_surface",
        "generic_risk",
        "duplicate_scope_entry",
        "scope_non_goal_overlap",
    }
    assert all(finding.field for finding in result.findings)
    assert all(finding.remediation for finding in result.findings)


def test_brief_readiness_reports_missing_scope_and_risks():
    brief = _ready_brief()
    brief["scope"] = []
    brief["risks"] = []

    result = audit_brief_readiness(brief)

    assert result.passed is False
    assert result.blocking_count == 2
    assert _finding_codes(result) == {"missing_scope", "missing_risks"}


def test_brief_readiness_cli_json_has_stable_machine_readable_output(tmp_path, monkeypatch):
    brief = _ready_brief()
    brief["risks"] = ["risk"]
    _seed_brief(tmp_path, monkeypatch, brief)

    result = CliRunner().invoke(cli, ["brief", "readiness", "ib-ready", "--json"])

    assert result.exit_code == 0, result.output
    payload = json.loads(result.output)
    assert payload == {
        "brief_id": "ib-ready",
        "findings": [
            {
                "code": "generic_risk",
                "field": "risks",
                "message": "Risk is too generic to guide planning: risk",
                "remediation": (
                    "Replace it with a concrete failure mode, constraint, or "
                    "integration concern."
                ),
                "severity": "warning",
                "value": "risk",
            }
        ],
        "passed": True,
        "summary": {
            "blocking": 0,
            "warning": 1,
        },
    }


def test_brief_readiness_cli_human_output_groups_blocking_before_warnings(
    tmp_path,
    monkeypatch,
):
    brief = _ready_brief()
    brief["scope"] = []
    brief["risks"] = ["risk"]
    _seed_brief(tmp_path, monkeypatch, brief)

    result = CliRunner().invoke(cli, ["brief", "readiness", "ib-ready"])

    assert result.exit_code == 1, result.output
    assert "Brief readiness audit: ib-ready" in result.output
    assert "Result: failed (1 blocking, 1 warnings)" in result.output
    assert result.output.index("Blocking findings:") < result.output.index("Warnings:")
    assert "scope: Brief has no in-scope deliverables." in result.output
    assert "risks: Risk is too generic to guide planning: risk" in result.output


def test_brief_readiness_cli_rejects_missing_brief(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    init_db(str(tmp_path / "blueprint.db"))

    result = CliRunner().invoke(cli, ["brief", "readiness", "missing-brief"])

    assert result.exit_code != 0
    assert "Implementation brief not found: missing-brief" in result.output


def _finding_codes(result):
    return {finding.code for finding in result.findings}


def _seed_brief(tmp_path, monkeypatch, brief):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(brief)


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


def _ready_brief():
    return {
        "id": "ib-ready",
        "source_brief_id": "sb-ready",
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
        "risks": [
            "Concurrent status updates could overwrite newer task state",
            "CLI output changes could break existing automation",
        ],
        "validation_plan": "Run task CLI tests and inspect JSON output",
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
