from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.coverage_matrix import CoverageMatrixExporter
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.store import Store, init_db


def test_coverage_matrix_exporter_renders_grouped_markdown_tables(tmp_path):
    output_path = tmp_path / "coverage.md"

    CoverageMatrixExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    matrix = output_path.read_text()
    assert matrix.startswith("# Coverage Matrix: Test Brief\n")
    assert "- Plan ID: `plan-test`" in matrix
    assert "- Implementation Brief: `ib-test`" in matrix

    scope_index = matrix.index("## Scope Coverage")
    risk_index = matrix.index("## Risk Coverage")
    validation_index = matrix.index("## Validation Coverage")
    dod_index = matrix.index("## Definition of Done Coverage")
    assert scope_index < risk_index < validation_index < dod_index

    assert (
        "| Coverage matrix exporter | covered | "
        "`task-exporter`, `task-cli`, `task-tests` |"
    ) in matrix
    assert "| CLI export command registration | covered | `task-cli` |" in matrix
    assert "| Missing traceability review | covered | `task-exporter` |" in matrix
    assert "| Budget regression | partial | `task-budget` |" in matrix
    assert (
        "| Run coverage matrix exporter tests | covered | "
        "`task-exporter`, `task-cli`, `task-tests` |"
    ) in matrix
    assert "| Stakeholder signoff checklist | uncovered | none |" in matrix


def test_coverage_matrix_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "coverage-matrix")

    assert result.passed
    assert result.findings == []


def test_coverage_matrix_validation_catches_missing_and_duplicate_items(tmp_path):
    artifact_path = tmp_path / "coverage.md"
    artifact_path.write_text(
        "# Coverage Matrix: Test Brief\n"
        "\n"
        "## Plan Metadata\n"
        "- Plan ID: `plan-test`\n"
        "- Implementation Brief: `ib-test`\n"
        "\n"
        "## Scope Coverage\n"
        "| Item | Status | Matching Tasks |\n"
        "| --- | --- | --- |\n"
        "| Coverage matrix exporter | covered | `task-exporter` |\n"
        "| Coverage matrix exporter | covered | `task-exporter` |\n"
        "\n"
        "## Risk Coverage\n"
        "| Item | Status | Matching Tasks |\n"
        "| --- | --- | --- |\n"
        "| Missing traceability review | maybe | `task-exporter` |\n"
        "\n"
        "## Validation Coverage\n"
        "| Item | Status | Matching Tasks |\n"
        "| --- | --- | --- |\n"
        "| Run coverage matrix exporter tests | covered | `task-tests` |\n"
        "\n"
        "## Definition of Done Coverage\n"
        "| Item | Status | Matching Tasks |\n"
        "| --- | --- | --- |\n"
        "| Stakeholder signoff checklist | uncovered | `task-tests` |\n"
    )

    findings = validate_rendered_export(
        target="coverage-matrix",
        artifact_path=artifact_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    codes = {finding.code for finding in findings}
    assert "coverage_matrix.item_occurrence_mismatch" in codes
    assert "coverage_matrix.invalid_status" in codes
    assert "coverage_matrix.uncovered_has_matches" in codes


def test_export_preview_run_and_validate_support_coverage_matrix(tmp_path, monkeypatch):
    store = _write_config_and_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    preview = CliRunner().invoke(cli, ["export", "preview", plan_id, "coverage-matrix"])
    assert preview.exit_code == 0, preview.output
    assert "# Coverage Matrix: Test Brief" in preview.output
    assert "| Stakeholder signoff checklist | uncovered | none |" in preview.output
    assert not (tmp_path / "exports").exists()

    run = CliRunner().invoke(cli, ["export", "run", plan_id, "coverage-matrix"])
    assert run.exit_code == 0, run.output
    output_path = tmp_path / "exports" / f"{plan_id}-coverage-matrix.md"
    assert output_path.exists()
    assert "# Coverage Matrix: Test Brief" in output_path.read_text()

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "coverage-matrix"
    assert records[0]["export_format"] == "markdown"

    validation = CliRunner().invoke(
        cli,
        ["export", "validate", plan_id, "--target", "coverage-matrix"],
    )
    assert validation.exit_code == 0, validation.output
    assert "Validation passed for coverage-matrix" in validation.output


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
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Coverage", "description": "Build traceability"}],
        "test_strategy": "Run coverage matrix exporter tests",
        "handoff_prompt": "Build the coverage matrix",
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
            "id": "task-exporter",
            "title": "Build coverage matrix exporter",
            "description": "Render traceability tables and mitigate missing traceability review.",
            "milestone": "Coverage",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/blueprint/exporters/coverage_matrix.py"],
            "acceptance_criteria": ["Scope and risks are mapped to task ids"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
        {
            "id": "task-cli",
            "title": "Register CLI export command target",
            "description": "Support coverage-matrix for export run, preview, and validate.",
            "milestone": "Coverage",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-exporter"],
            "files_or_modules": ["src/blueprint/cli.py"],
            "acceptance_criteria": ["coverage-matrix is accepted as an export target"],
            "estimated_complexity": "low",
            "status": "pending",
        },
        {
            "id": "task-tests",
            "title": "Add exporter validation tests",
            "description": "Run coverage matrix exporter tests and verify export validate support.",
            "milestone": "Coverage",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-exporter", "task-cli"],
            "files_or_modules": ["tests/test_coverage_matrix_exporter.py"],
            "acceptance_criteria": [
                "Run coverage matrix exporter tests",
                "export validate supports coverage matrix",
            ],
            "estimated_complexity": "low",
            "status": "pending",
        },
        {
            "id": "task-budget",
            "title": "Budget review",
            "description": "Check the exporter output size remains predictable.",
            "milestone": "Coverage",
            "owner_type": "human",
            "suggested_engine": "manual",
            "depends_on": [],
            "files_or_modules": None,
            "acceptance_criteria": ["Budget is reviewed"],
            "estimated_complexity": "low",
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
        "problem_statement": "Need plan traceability",
        "mvp_goal": "Render a reviewable plan coverage matrix",
        "product_surface": "CLI",
        "scope": [
            "Coverage matrix exporter",
            "CLI export command registration",
        ],
        "non_goals": ["Graph visualization"],
        "assumptions": ["Markdown is enough"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Implementation brief and execution tasks",
        "integration_points": ["CLI export command"],
        "risks": [
            "Missing traceability review",
            "Budget regression",
        ],
        "validation_plan": "Run coverage matrix exporter tests",
        "definition_of_done": [
            "export validate supports coverage matrix",
            "Stakeholder signoff checklist",
        ],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
