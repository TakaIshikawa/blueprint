from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.critical_path_report import CriticalPathReportExporter
from blueprint.exporters.export_validation import validate_export
from blueprint.store import Store, init_db


def test_critical_path_report_renders_branching_dependency_graph(tmp_path):
    output_path = tmp_path / "critical-path-report.md"

    CriticalPathReportExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    report = output_path.read_text()
    assert report.startswith("# Critical Path Report: Test Brief\n")
    assert "- Critical Path Length: 3 tasks" in report
    assert "- Critical Path Weight: 6" in report
    assert (
        "1. `task-foundation` Foundation (status: completed, complexity: low, "
        "weight: 1, cumulative: 1, depends on: none)"
    ) in report
    assert "2. `task-heavy` Heavy implementation" in report
    assert "3. `task-release` Release" in report
    assert report.index("`task-foundation` Foundation") < report.index(
        "`task-heavy` Heavy implementation"
    )
    assert report.index("`task-heavy` Heavy implementation") < report.index(
        "`task-release` Release"
    )
    assert "- Non-Critical Task Count: 2" in report
    assert "  - `task-fast` Fast implementation" in report
    assert "  - `task-docs` Write docs" in report
    assert (
        "- `task-release` Release: depends on `task-fast`, `task-heavy`; "
        "unblocks none"
    ) in report


def test_critical_path_report_highlights_blocked_and_incomplete_path_tasks(tmp_path):
    output_path = tmp_path / "critical-path-report.md"

    CriticalPathReportExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    report = output_path.read_text()
    assert (
        "2. `task-heavy` Heavy implementation **BLOCKED** "
        "(status: blocked"
    ) in report
    assert (
        "- **BLOCKED** `task-heavy` Heavy implementation: Waiting for schema signoff"
        in report
    )
    assert "- **INCOMPLETE** `task-release` Release: status is pending" in report
    assert "`task-fast` Fast implementation **BLOCKED**" not in report


def test_critical_path_report_validation_passes():
    result = validate_export(
        _execution_plan(),
        _implementation_brief(),
        "critical-path-report",
    )

    assert result.passed
    assert result.findings == []


def test_export_render_critical_path_report_writes_file_and_records_export(
    tmp_path,
    monkeypatch,
):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(
        cli,
        ["export", "render", plan_id, "critical-path-report"],
    )

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-critical-path-report.md"
    assert output_path.exists()
    assert "# Critical Path Report: Test Brief" in output_path.read_text()

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "critical-path-report"
    assert records[0]["export_format"] == "markdown"
    assert records[0]["output_path"] == str(output_path)


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


def _execution_plan(include_tasks=True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Build", "description": "Build the implementation"},
            {"name": "Launch", "description": "Release the work"},
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
        _task(
            "task-foundation",
            "Foundation",
            "Foundation",
            "low",
            status="completed",
        ),
        _task(
            "task-fast",
            "Fast implementation",
            "Build",
            "medium",
            depends_on=["task-foundation"],
            status="blocked",
            blocked_reason="Waiting for fixture cleanup",
        ),
        _task(
            "task-heavy",
            "Heavy implementation",
            "Build",
            "high",
            depends_on=["task-foundation"],
            status="blocked",
            blocked_reason="Waiting for schema signoff",
        ),
        _task(
            "task-release",
            "Release",
            "Launch",
            "medium",
            depends_on=["task-fast", "task-heavy"],
        ),
        _task(
            "task-docs",
            "Write docs",
            "Launch",
            "low",
            depends_on=["task-fast"],
        ),
    ]


def _task(
    task_id,
    title,
    milestone,
    estimated_complexity,
    *,
    depends_on=None,
    status="pending",
    blocked_reason=None,
):
    metadata = {}
    if blocked_reason:
        metadata["blocked_reason"] = blocked_reason
    return {
        "id": task_id,
        "title": title,
        "description": f"Implement {title}",
        "milestone": milestone,
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": depends_on or [],
        "files_or_modules": ["src/app.py"],
        "acceptance_criteria": [f"{title} works"],
        "estimated_complexity": estimated_complexity,
        "status": status,
        "metadata": metadata,
        "blocked_reason": blocked_reason,
    }


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Execution leads",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need critical path handoff reports",
        "mvp_goal": "Expose critical path report artifacts",
        "product_surface": "CLI",
        "scope": ["Critical path report exporter"],
        "non_goals": ["HTML dashboard"],
        "assumptions": ["Tasks already exist"],
        "architecture_notes": "Use critical path audit APIs",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Incorrect critical path"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["critical-path-report renders"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
