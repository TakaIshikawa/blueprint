import json

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.exporters.sarif_audit import SarifAuditExporter
from blueprint.store import Store, init_db


def test_sarif_audit_exporter_renders_clean_plan_with_zero_results(tmp_path):
    output_path = tmp_path / "plan-audit.sarif.json"

    SarifAuditExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    payload = json.loads(output_path.read_text())
    run = payload["runs"][0]
    assert payload["version"] == "2.1.0"
    assert run["tool"]["driver"]["name"] == "Blueprint Plan Audit"
    assert run["tool"]["driver"]["rules"] == []
    assert run["results"] == []
    assert run["properties"]["auditErrorCount"] == 0
    assert run["properties"]["auditWarningCount"] == 0


def test_sarif_audit_exporter_renders_audit_findings(tmp_path):
    plan = _execution_plan()
    plan["tasks"][1]["depends_on"] = ["task-missing"]
    plan["tasks"][1]["status"] = "blocked"
    output_path = tmp_path / "plan-audit.sarif.json"

    SarifAuditExporter().export(plan, _implementation_brief(), str(output_path))

    payload = json.loads(output_path.read_text())
    run = payload["runs"][0]
    rules = {rule["id"]: rule for rule in run["tool"]["driver"]["rules"]}
    results = run["results"]

    assert set(rules) == {"blocked_without_reason", "unknown_dependency"}
    assert rules["unknown_dependency"]["defaultConfiguration"]["level"] == "error"
    assert rules["blocked_without_reason"]["defaultConfiguration"]["level"] == "warning"
    assert {(result["ruleId"], result["level"]) for result in results} == {
        ("unknown_dependency", "error"),
        ("blocked_without_reason", "warning"),
    }
    unknown_dependency = next(
        result for result in results if result["ruleId"] == "unknown_dependency"
    )
    assert unknown_dependency["message"]["text"] == (
        "Task task-api depends on missing task task-missing"
    )
    assert unknown_dependency["locations"][0]["physicalLocation"]["artifactLocation"] == {
        "uri": "execution-plan.json"
    }
    assert unknown_dependency["properties"]["taskId"] == "task-api"
    assert unknown_dependency["properties"]["dependencyId"] == "task-missing"


def test_sarif_audit_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "sarif-audit")

    assert result.passed
    assert result.findings == []


def test_sarif_audit_validation_rejects_invalid_structure_and_count_mismatch(tmp_path):
    artifact_path = tmp_path / "plan-audit.sarif.json"
    artifact_path.write_text(
        json.dumps(
            {
                "version": "2.0.0",
                "runs": [
                    {
                        "tool": {
                            "driver": {
                                "name": "Other Tool",
                                "rules": [{"id": "unknown_dependency"}],
                            }
                        },
                        "results": [],
                    }
                ],
            }
        )
    )
    plan = _execution_plan()
    plan["tasks"][1]["depends_on"] = ["task-missing"]

    findings = validate_rendered_export(
        target="sarif-audit",
        artifact_path=artifact_path,
        execution_plan=plan,
        implementation_brief=_implementation_brief(),
    )

    codes = {finding.code for finding in findings}
    assert "sarif_audit.version_mismatch" in codes
    assert "sarif_audit.tool.name_mismatch" in codes
    assert "sarif_audit.finding_count_mismatch" in codes


def test_export_run_records_sarif_audit_json_export(tmp_path, monkeypatch):
    store = _write_config_and_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan = _execution_plan(include_tasks=False)
    plan_id = store.insert_execution_plan(plan, _tasks())

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "sarif-audit"])

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-sarif-audit.json"
    assert output_path.exists()
    payload = json.loads(output_path.read_text())
    assert payload["version"] == "2.1.0"

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "sarif-audit"
    assert records[0]["export_format"] == "json"


def _write_config_and_store(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "blueprint.db"
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {db_path}
exports:
  output_dir: {tmp_path / "exports"}
"""
    )
    blueprint_config.reload_config()
    return init_db(str(db_path))


def _execution_plan(include_tasks=True):
    plan = {
        "id": "plan-sarif-audit",
        "implementation_brief_id": "ib-sarif-audit",
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
    if include_tasks:
        plan["tasks"] = _tasks()
    return plan


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
        "id": "ib-sarif-audit",
        "source_brief_id": "sb-sarif-audit",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need code-scanning plan audit output",
        "mvp_goal": "Render audit findings as SARIF",
        "product_surface": "CLI",
        "scope": ["SARIF audit exporter"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": "Use plan audit findings",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Audit findings are not visible in code scanning"],
        "validation_plan": "Run exporter validation tests",
        "definition_of_done": ["SARIF audit target renders and validates"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
