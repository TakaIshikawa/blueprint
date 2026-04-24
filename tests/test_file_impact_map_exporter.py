from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_rendered_export
from blueprint.exporters.file_impact_map import FileImpactMapExporter
from blueprint.store import Store, init_db


def test_file_impact_map_payload_groups_tasks_by_every_declared_file():
    payload = FileImpactMapExporter().render_payload(_execution_plan(include_tasks=True))

    assert payload["plan_id"] == "plan-test"
    assert payload["impacted_files_or_modules"] == 3
    sections = {section["file_or_module"]: section for section in payload["sections"]}
    assert [task["id"] for task in sections["src/api.py"]["tasks"]] == [
        "task-api",
        "task-ui",
    ]
    assert [task["id"] for task in sections["src/ui.py"]["tasks"]] == ["task-ui"]
    assert sections["src/api.py"]["tasks"][0] == {
        "id": "task-api",
        "title": "Build API",
        "status": "in_progress",
        "milestone": "Foundation",
        "dependency_count": 1,
        "suggested_engine": "codex",
    }
    assert [task["id"] for task in payload["unassigned"]["tasks"]] == [
        "task-copy",
        "task-ops",
    ]


def test_file_impact_map_markdown_includes_statuses_dependencies_and_unassigned(tmp_path):
    output_path = tmp_path / "impact-map.md"

    FileImpactMapExporter().export(
        _execution_plan(include_tasks=True),
        _implementation_brief(),
        str(output_path),
    )

    content = output_path.read_text()
    assert content.startswith("# File Impact Map: plan-test\n")
    assert "- Plan ID: `plan-test`" in content
    assert "## Files and Modules" in content
    assert "### `src/api.py`" in content
    assert "- `task-ui` Build UI" in content
    assert "  - Status: pending" in content
    assert "  - Milestone: Interface" in content
    assert "  - Dependency Count: 2" in content
    assert "  - Suggested Engine: codex" in content
    assert "## Unassigned" in content
    assert "_Tasks without files_or_modules: 2_" in content
    assert "- `task-copy` Write copy" in content
    assert "- `task-ops` Schedule rollout" in content


def test_file_impact_map_validation_catches_missing_task_from_section(tmp_path):
    artifact_path = tmp_path / "impact-map.md"
    artifact_path.write_text(
        "# File Impact Map: plan-test\n"
        "- Plan ID: `plan-test`\n"
        "- Implementation Brief: `ib-test`\n"
        "## Files and Modules\n"
        "### `src/api.py`\n"
        "- `task-api` Build API\n"
        "## Unassigned\n"
    )

    findings = validate_rendered_export(
        target="file-impact-map",
        artifact_path=artifact_path,
        execution_plan=_execution_plan(include_tasks=True),
        implementation_brief=_implementation_brief(),
    )

    assert any(
        finding.code == "file_impact_map.missing_task"
        and "task-ui" in finding.message
        and "src/api.py" in finding.message
        for finding in findings
    )
    assert any(
        finding.code == "file_impact_map.missing_task"
        and "task-copy" in finding.message
        and "unassigned" in finding.message
        for finding in findings
    )


def test_export_preview_run_and_validate_support_file_impact_map(tmp_path, monkeypatch):
    _write_config(tmp_path, monkeypatch)
    db_path = tmp_path / "blueprint.db"
    export_dir = tmp_path / "exports"
    store = init_db(str(db_path))
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    preview = CliRunner().invoke(
        cli,
        ["export", "preview", plan_id, "--target", "file-impact-map"],
    )

    assert preview.exit_code == 0, preview.output
    assert "# File Impact Map: plan-test" in preview.output
    assert "### `src/api.py`" in preview.output
    assert "## Unassigned" in preview.output

    run = CliRunner().invoke(
        cli,
        ["export", "run", plan_id, "--target", "file-impact-map"],
    )

    assert run.exit_code == 0, run.output
    output_path = export_dir / f"{plan_id}-file-impact-map.md"
    assert output_path.exists()
    assert "Exported to:" in run.output

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "file-impact-map"
    assert records[0]["export_format"] == "markdown"
    assert records[0]["output_path"] == str(output_path)

    validate = CliRunner().invoke(
        cli,
        ["export", "validate", plan_id, "--target", "file-impact-map"],
    )

    assert validate.exit_code == 0, validate.output
    assert "Validation passed for file-impact-map" in validate.output


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
exports:
  output_dir: {tmp_path / "exports"}
"""
    )
    blueprint_config.reload_config()


def _execution_plan(include_tasks=False):
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
        "status": "ready",
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
            "id": "task-api",
            "title": "Build API",
            "description": "Implement API routes",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup"],
            "files_or_modules": ["src/api.py"],
            "acceptance_criteria": ["API returns data"],
            "estimated_complexity": "medium",
            "status": "in_progress",
        },
        {
            "id": "task-copy",
            "title": "Write copy",
            "description": "Draft user-facing copy",
            "milestone": "Interface",
            "owner_type": "human",
            "suggested_engine": "manual",
            "depends_on": [],
            "files_or_modules": [],
            "acceptance_criteria": ["Copy is approved"],
            "estimated_complexity": "low",
            "status": "blocked",
        },
        {
            "id": "task-ops",
            "title": "Schedule rollout",
            "description": "Coordinate release timing",
            "milestone": None,
            "owner_type": "human",
            "suggested_engine": None,
            "depends_on": [],
            "files_or_modules": None,
            "acceptance_criteria": ["Rollout window is booked"],
            "estimated_complexity": "low",
            "status": "pending",
        },
        {
            "id": "task-setup",
            "title": "Setup project",
            "description": "Create baseline project structure",
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
            "id": "task-ui",
            "title": "Build UI",
            "description": "Render the interface",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup", "task-api"],
            "files_or_modules": ["src/ui.py", "src/api.py"],
            "acceptance_criteria": ["UI displays API data"],
            "estimated_complexity": "medium",
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
        "problem_statement": "Need impact planning",
        "mvp_goal": "Expose file-level task impact",
        "product_surface": "CLI",
        "scope": ["Export file impact map"],
        "non_goals": ["Modify tasks"],
        "assumptions": ["Tasks already declare files"],
        "architecture_notes": "Use exporter factory",
        "data_requirements": "Execution tasks and file/module declarations",
        "integration_points": [],
        "risks": ["Unclear ownership hotspots"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Export target renders and validates"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
