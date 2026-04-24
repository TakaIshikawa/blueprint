from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.exporters.gantt import GanttExporter
from blueprint.store import Store, init_db


def test_gantt_exporter_renders_dated_tasks_by_milestone(tmp_path):
    output_path = tmp_path / "plan-gantt.mmd"

    GanttExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    content = output_path.read_text()
    assert content.startswith("gantt\n")
    assert "    section Foundation" in content
    assert "    section Interface" in content
    assert "Setup project :done, task_setup, 2026-05-01, 2026-05-03" in content
    assert "Build API :task_api, after task_setup, 2d" in content
    assert "Build UI :crit, task_ui, 2026-05-08, 3d" in content


def test_gantt_exporter_uses_deterministic_fallback_schedule(tmp_path):
    output_path = tmp_path / "fallback-gantt.mmd"
    plan = _execution_plan()
    plan["tasks"] = [
        _task("task-alpha", "Alpha", metadata={}),
        _task("task-beta", "Beta", depends_on=["task-alpha"], metadata={"duration_days": 2}),
        _task("task-gamma", "Gamma", metadata={}),
    ]

    GanttExporter().export(plan, _implementation_brief(), str(output_path))

    content = output_path.read_text()
    assert "Alpha :task_alpha, 2026-01-05, 1d" in content
    assert "Beta :task_beta, after task_alpha, 2d" in content
    assert "Gamma :task_gamma, 2026-01-08, 1d" in content


def test_gantt_exporter_renders_completed_and_blocked_state_tags(tmp_path):
    output_path = tmp_path / "state-gantt.mmd"

    GanttExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    content = output_path.read_text()
    assert "Setup project :done, task_setup" in content
    assert "Build UI :crit, task_ui" in content


def test_gantt_validation_passes_for_rendered_export(tmp_path):
    output_path = tmp_path / "plan-gantt.mmd"
    plan = _execution_plan()
    brief = _implementation_brief()
    GanttExporter().export(plan, brief, str(output_path))

    findings = validate_rendered_export(
        target="gantt",
        artifact_path=output_path,
        execution_plan=plan,
        implementation_brief=brief,
    )

    assert findings == []


def test_gantt_validation_catches_missing_header(tmp_path):
    output_path = tmp_path / "broken.mmd"
    output_path.write_text("flowchart TD\n")

    findings = validate_rendered_export(
        target="gantt",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == ["gantt.missing_header"]


def test_export_validate_supports_gantt():
    result = validate_export(_execution_plan(), _implementation_brief(), "gantt")

    assert result.passed
    assert result.findings == []


def test_export_run_preview_and_validate_support_gantt(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "blueprint.db"
    export_dir = tmp_path / "exports"
    Path(".blueprint.yaml").write_text(
        f"""
database:
  path: {db_path}
exports:
  output_dir: {export_dir}
"""
    )
    blueprint_config.reload_config()

    store = init_db(str(db_path))
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    run_result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "gantt"])
    assert run_result.exit_code == 0, run_result.output
    output_path = export_dir / f"{plan_id}-gantt.mmd"
    assert output_path.exists()
    assert output_path.read_text().startswith("gantt\n")

    preview_result = CliRunner().invoke(cli, ["export", "preview", plan_id, "gantt"])
    assert preview_result.exit_code == 0, preview_result.output
    assert "Build API :task_api, after task_setup, 2d" in preview_result.output

    validate_result = CliRunner().invoke(
        cli,
        ["export", "validate", plan_id, "--target", "gantt"],
    )
    assert validate_result.exit_code == 0, validate_result.output
    assert "Validation passed for gantt" in validate_result.output

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "gantt"
    assert records[0]["export_format"] == "mermaid"
    assert records[0]["output_path"] == str(output_path)


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
        _task(
            "task-setup",
            "Setup project",
            status="completed",
            metadata={"start_date": "2026-05-01", "due_date": "2026-05-03"},
        ),
        _task(
            "task-api",
            "Build API",
            depends_on=["task-setup"],
            metadata={"duration_days": 2},
        ),
        _task(
            "task-ui",
            "Build UI",
            milestone="Interface",
            status="blocked",
            depends_on=["task-api"],
            metadata={"start_date": "2026-05-08", "duration_days": 3},
        ),
    ]


def _task(
    task_id,
    title,
    *,
    milestone="Foundation",
    status="pending",
    depends_on=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title,
        "description": f"{title} description",
        "milestone": milestone,
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": [f"{title} works"],
        "estimated_complexity": "medium",
        "status": status,
        "metadata": metadata or {},
    }


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need a timeline visualization",
        "mvp_goal": "Export plans as Mermaid Gantt charts",
        "product_surface": "CLI",
        "scope": ["Gantt exporter"],
        "non_goals": ["Rendering Mermaid"],
        "assumptions": ["Mermaid consumers parse Gantt charts"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Invalid chart syntax"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Gantt chart contains milestones and dependencies"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
