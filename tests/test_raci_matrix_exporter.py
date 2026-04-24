from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.exporters.raci_matrix import RaciMatrixExporter
from blueprint.store import Store, init_db


def test_raci_matrix_exporter_renders_metadata_and_fallback_values(tmp_path):
    output_path = tmp_path / "raci.md"

    RaciMatrixExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    matrix = output_path.read_text()
    assert matrix.startswith("# RACI Matrix: Test Brief\n")
    assert "- Plan ID: `plan-test`" in matrix
    assert "- Implementation Brief: `ib-test`" in matrix
    assert (
        "| `task-api` | Build API | agent: codex | API lead | Security, Support | "
        "Release desk | Foundation | codex |"
    ) in matrix
    assert (
        "| `task-docs` | Write docs | human: manual | Engineering manager | "
        "N/A | Support, Success | Handoff | manual |"
    ) in matrix
    assert (
        "| `task-review` | Review launch notes | human: manual | Plan lead | "
        "N/A | N/A | Handoff | manual |"
    ) in matrix


def test_raci_matrix_escapes_markdown_table_characters(tmp_path):
    output_path = tmp_path / "raci.md"

    RaciMatrixExporter().export(
        _execution_plan(with_pipes=True),
        _implementation_brief(),
        str(output_path),
    )

    matrix = output_path.read_text()
    assert "Build API \\| adapter" in matrix
    assert "Security \\| compliance" in matrix
    assert "Release \\| ops" in matrix


def test_raci_matrix_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "raci-matrix")

    assert result.passed
    assert result.findings == []


def test_raci_matrix_validation_detects_missing_task_rows(tmp_path):
    artifact_path = tmp_path / "raci.md"
    artifact_path.write_text(
        "# RACI Matrix: Test Brief\n"
        "\n"
        "## Plan Metadata\n"
        "- Plan ID: `plan-test`\n"
        "- Implementation Brief: `ib-test`\n"
        "\n"
        "## Responsibility Matrix\n"
        "\n"
        "| Task ID | Task | Responsible | Accountable | Consulted | Informed | Milestone | Suggested Engine |\n"
        "| --- | --- | --- | --- | --- | --- | --- | --- |\n"
        "| `task-api` | Build API | agent: codex | API lead | Security | Release desk | Foundation | codex |\n"
    )

    findings = validate_rendered_export(
        target="raci-matrix",
        artifact_path=artifact_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    codes = {finding.code for finding in findings}
    assert "raci_matrix.task_row_occurrence_mismatch" in codes
    assert any("task-docs" in finding.message for finding in findings)
    assert any("task-review" in finding.message for finding in findings)


def test_export_preview_run_and_validate_support_raci_matrix(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    preview = CliRunner().invoke(cli, ["export", "preview", plan_id, "--target", "raci-matrix"])
    assert preview.exit_code == 0, preview.output
    assert "# RACI Matrix: Test Brief" in preview.output
    assert "| `task-api` | Build API | agent: codex | API lead | Security, Support |" in (
        preview.output
    )
    assert Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id) == []

    run = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "raci-matrix"])
    assert run.exit_code == 0, run.output
    output_path = tmp_path / "exports" / f"{plan_id}-raci-matrix.md"
    assert output_path.exists()
    assert "# RACI Matrix: Test Brief" in output_path.read_text()

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "raci-matrix"
    assert records[0]["export_format"] == "markdown"

    validation = CliRunner().invoke(
        cli,
        ["export", "validate", plan_id, "--target", "raci-matrix"],
    )
    assert validation.exit_code == 0, validation.output
    assert "Validation passed for raci-matrix" in validation.output


def _setup_store(tmp_path, monkeypatch):
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


def _execution_plan(include_tasks=True, with_pipes=False):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Build the base"},
            {"name": "Handoff", "description": "Prepare release alignment"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the RACI matrix",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
        "metadata": {"accountable": "Plan lead"},
    }
    if include_tasks:
        plan["tasks"] = _tasks(with_pipes=with_pipes)
    return plan


def _tasks(with_pipes=False):
    return [
        {
            "id": "task-api",
            "title": "Build API | adapter" if with_pipes else "Build API",
            "description": "Implement the API layer",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
            "estimated_complexity": "medium",
            "status": "pending",
            "metadata": {
                "accountable": "API lead",
                "consulted": ["Security | compliance" if with_pipes else "Security", "Support"],
                "informed": "Release | ops" if with_pipes else "Release desk",
            },
        },
        {
            "id": "task-docs",
            "title": "Write docs",
            "description": "Document the handoff process",
            "milestone": "Handoff",
            "owner_type": "human",
            "suggested_engine": "manual",
            "depends_on": ["task-api"],
            "files_or_modules": ["docs/handoff.md"],
            "acceptance_criteria": ["Docs explain ownership"],
            "estimated_complexity": "low",
            "status": "pending",
            "metadata": {
                "accountable": "Engineering manager",
                "informed": ["Support", "Success"],
            },
        },
        {
            "id": "task-review",
            "title": "Review launch notes",
            "description": "Confirm release communication",
            "milestone": "Handoff",
            "owner_type": "human",
            "suggested_engine": "manual",
            "depends_on": ["task-docs"],
            "files_or_modules": ["docs/release.md"],
            "acceptance_criteria": ["Launch notes are approved"],
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
        "problem_statement": "Need ownership alignment",
        "mvp_goal": "Render a reviewable RACI matrix",
        "product_surface": "CLI",
        "scope": ["RACI matrix exporter"],
        "non_goals": ["Workflow automation"],
        "assumptions": ["Markdown is enough"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution tasks and metadata",
        "integration_points": ["CLI export command"],
        "risks": ["Ownership confusion"],
        "validation_plan": "Run RACI matrix exporter tests",
        "definition_of_done": ["raci-matrix validates"],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
