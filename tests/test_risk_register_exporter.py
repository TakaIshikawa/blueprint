from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.exporters.risk_register import RiskRegisterExporter
from blueprint.store import Store, init_db


def test_risk_register_exporter_renders_markdown_with_metadata_matches(tmp_path):
    output_path = tmp_path / "risk-register.md"

    RiskRegisterExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    register = output_path.read_text()
    assert register.startswith("# Risk Register: Test Brief\n")
    assert "- Plan ID: `plan-risk-register`" in register
    assert "- Implementation Brief: `ib-risk-register`" in register
    assert (
        "| Risk ID | Source Risk | Affected Milestones/Tasks | Mitigation Evidence | "
        "Owner/Suggested Engine | Status |"
    ) in register
    assert (
        "| `RISK-001` | API timeouts during export | Delivery: `task-timeouts` | "
        "Retry behavior covers export API timeout failures | agent: codex | tracked |"
    ) in register
    assert (
        "| `RISK-002` | Duplicate tasks are queued | Delivery: `task-duplicates` | "
        "Queue deduplication is covered by regression tests | agent: codex | tracked |"
    ) in register
    assert (
        "| `RISK-003` | Sensitive tokens leak into logs | Security: `task-secrets` | "
        "Logs include redaction assertions; Redact sensitive token values before writing logs. | "
        "human: manual | blocked |"
    ) in register


def test_risk_register_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "risk-register")

    assert result.passed
    assert result.findings == []


def test_risk_register_validation_reports_unmatched_risks():
    plan = _execution_plan()
    plan["tasks"] = plan["tasks"][:2]

    result = validate_export(plan, _implementation_brief(), "risk-register")

    assert not result.passed
    assert [finding.code for finding in result.findings] == [
        "risk_register.missing_risk_coverage"
    ]
    assert "RISK-003" in result.findings[0].message


def test_risk_register_validation_rejects_invalid_task_references(tmp_path):
    artifact_path = tmp_path / "risk-register.md"
    artifact_path.write_text(
        "# Risk Register: Test Brief\n"
        "\n"
        "## Plan Metadata\n"
        "- Plan ID: `plan-risk-register`\n"
        "- Implementation Brief: `ib-risk-register`\n"
        "\n"
        "## Register\n"
        "\n"
        "| Risk ID | Source Risk | Affected Milestones/Tasks | Mitigation Evidence | "
        "Owner/Suggested Engine | Status |\n"
        "| --- | --- | --- | --- | --- | --- |\n"
        "| `RISK-001` | API timeouts during export | Delivery: `task-missing` | "
        "Retry behavior covers export API timeout failures | agent: codex | tracked |\n"
        "| `RISK-002` | Duplicate tasks are queued | Delivery: `task-duplicates` | "
        "Queue deduplication is covered by regression tests | agent: codex | tracked |\n"
        "| `RISK-003` | Sensitive tokens leak into logs | Security: `task-secrets` | "
        "Logs include redaction assertions | human: manual | blocked |\n"
    )

    findings = validate_rendered_export(
        target="risk-register",
        artifact_path=artifact_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    codes = {finding.code for finding in findings}
    assert "risk_register.invalid_task_reference" in codes
    assert "risk_register.missing_risk_coverage" in codes


def test_export_preview_run_and_validate_support_risk_register(tmp_path, monkeypatch):
    store = _write_config_and_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    preview = CliRunner().invoke(cli, ["export", "preview", plan_id, "risk-register"])
    assert preview.exit_code == 0, preview.output
    assert "# Risk Register: Test Brief" in preview.output
    assert "| `RISK-002` | Duplicate tasks are queued | Delivery: `task-duplicates` |" in preview.output

    run = CliRunner().invoke(cli, ["export", "run", plan_id, "risk-register"])
    assert run.exit_code == 0, run.output
    output_path = tmp_path / "exports" / f"{plan_id}-risk-register.md"
    assert output_path.exists()
    assert "# Risk Register: Test Brief" in output_path.read_text()

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "risk-register"
    assert records[0]["export_format"] == "markdown"

    validation = CliRunner().invoke(
        cli,
        ["export", "validate", plan_id, "--target", "risk-register"],
    )
    assert validation.exit_code == 0, validation.output
    assert "Validation passed for risk-register" in validation.output


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


def _implementation_brief():
    return {
        "id": "ib-risk-register",
        "source_brief_id": "sb-risk-register",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need portable risk traceability",
        "mvp_goal": "Render a reviewable implementation risk register",
        "product_surface": "CLI",
        "scope": ["Risk register exporter"],
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
        "validation_plan": "Run exporter validation tests",
        "definition_of_done": ["risk-register is accepted as an export target"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _execution_plan(include_tasks=True):
    plan = {
        "id": "plan-risk-register",
        "implementation_brief_id": "ib-risk-register",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Delivery", "description": "Build the exporter"},
            {"name": "Security", "description": "Review sensitive output"},
        ],
        "test_strategy": "Run risk register exporter tests",
        "handoff_prompt": "Build the risk register",
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
            "id": "task-timeouts",
            "title": "Add retry handling",
            "description": "Handle API timeout during export with bounded retries.",
            "milestone": "Delivery",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/blueprint/exporters/risk_register.py"],
            "acceptance_criteria": ["Retry behavior covers export API timeout failures"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
        {
            "id": "task-duplicates",
            "title": "Prevent duplicate queue entries",
            "description": "Validate queue input before enqueue.",
            "milestone": "Delivery",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-timeouts"],
            "files_or_modules": ["src/blueprint/cli.py"],
            "acceptance_criteria": ["Queue deduplication is covered by regression tests"],
            "estimated_complexity": "medium",
            "status": "pending",
            "metadata": {"risk_ids": ["RISK-002"]},
        },
        {
            "id": "task-secrets",
            "title": "Redact logs",
            "description": "Add export logging safeguards.",
            "milestone": "Security",
            "owner_type": "human",
            "suggested_engine": "manual",
            "depends_on": [],
            "files_or_modules": ["src/blueprint/cli.py"],
            "acceptance_criteria": ["Logs include redaction assertions"],
            "estimated_complexity": "medium",
            "status": "blocked",
            "metadata": {
                "risks": ["Sensitive tokens leak into logs"],
                "mitigation": "Redact sensitive token values before writing logs.",
            },
        },
    ]
