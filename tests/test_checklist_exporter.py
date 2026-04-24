from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.checklist import ChecklistExporter
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.store import Store, init_db


def test_checklist_exporter_renders_milestone_grouped_task_checkboxes(tmp_path):
    output_path = tmp_path / "checklist.md"

    ChecklistExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    checklist = output_path.read_text()
    assert checklist.startswith("# Execution Checklist: Test Brief\n")
    assert "- Plan ID: `plan-test`" in checklist
    assert "- Implementation Brief: `ib-test`" in checklist
    assert "- Target Engine: codex" in checklist

    foundation_index = checklist.index("### Foundation")
    interface_index = checklist.index("### Interface")
    ungrouped_index = checklist.index("### Ungrouped")
    assert foundation_index < interface_index < ungrouped_index

    assert "- [x] `task-setup` Setup project" in checklist
    assert "- [ ] `task-api` Build API" in checklist
    assert "- [ ] `task-copy` Write copy" in checklist
    assert "- [ ] `task-ops` Schedule rollout" in checklist

    assert "  - Suggested Engine: codex" in checklist
    assert "  - Dependencies: task-setup" in checklist
    assert "  - Affected Files: src/app.py, src/schema.py" in checklist
    assert "    - API returns data" in checklist
    assert "    - Schema validates payloads" in checklist


def test_checklist_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "checklist")

    assert result.passed
    assert result.findings == []


def test_checklist_validation_catches_missing_task_and_context(tmp_path):
    artifact_path = tmp_path / "checklist.md"
    artifact_path.write_text(
        "# Execution Checklist: Test Brief\n"
        "## Plan Metadata\n"
        "- Plan ID: `plan-test`\n"
        "- Implementation Brief: `ib-test`\n"
        "## Milestones\n"
        "### Foundation\n"
        "- [ ] `task-api` Build API\n"
    )

    findings = validate_rendered_export(
        target="checklist",
        artifact_path=artifact_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    codes = {finding.code for finding in findings}
    assert "checklist.task_count_mismatch" in codes
    assert "checklist.task_occurrence_mismatch" in codes
    assert "checklist.missing_dependency" in codes
    assert "checklist.missing_affected_file" in codes


def test_export_preview_checklist_supports_positional_target(tmp_path, monkeypatch):
    store = _write_config_and_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "preview", plan_id, "checklist"])

    assert result.exit_code == 0, result.output
    assert "# Execution Checklist: Test Brief" in result.output
    assert "- [ ] `task-api` Build API" in result.output
    assert not (tmp_path / "exports").exists()


def test_export_run_checklist_writes_file_and_records_export(tmp_path, monkeypatch):
    store = _write_config_and_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "checklist"])

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-checklist.md"
    assert output_path.exists()
    assert "# Execution Checklist: Test Brief" in output_path.read_text()

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "checklist"
    assert records[0]["export_format"] == "markdown"
    assert records[0]["output_path"] == str(output_path)


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
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Interface", "description": "Build the user-facing flow"},
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "in_progress",
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
            "files_or_modules": ["src/app.py", "src/schema.py"],
            "acceptance_criteria": ["API returns data", "Schema validates payloads"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
        {
            "id": "task-copy",
            "title": "Write copy",
            "description": "Draft interface copy",
            "milestone": "Interface",
            "owner_type": "human",
            "suggested_engine": "manual",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/copy.py"],
            "acceptance_criteria": ["Copy is clear"],
            "estimated_complexity": "low",
            "status": "pending",
        },
        {
            "id": "task-ops",
            "title": "Schedule rollout",
            "description": "Coordinate release timing",
            "milestone": None,
            "owner_type": "human",
            "suggested_engine": "manual",
            "depends_on": [],
            "files_or_modules": None,
            "acceptance_criteria": ["Rollout owner is assigned"],
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
        "problem_statement": "Need task checklists",
        "mvp_goal": "Expose execution tasks as markdown checkboxes",
        "product_surface": "CLI",
        "scope": ["Checklist exports"],
        "non_goals": ["Task execution"],
        "assumptions": ["Markdown is enough"],
        "architecture_notes": "Use shared exporter validation helpers",
        "data_requirements": "Execution plans and tasks",
        "integration_points": ["CLI export command"],
        "risks": ["Missing handoff context"],
        "validation_plan": "Run checklist exporter tests",
        "definition_of_done": ["Checklist target renders"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
