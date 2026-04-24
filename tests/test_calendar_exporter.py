from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.calendar import CalendarExporter
from blueprint.exporters.export_validation import validate_rendered_export
from blueprint.store import Store, init_db


def test_calendar_exporter_renders_dated_tasks_and_milestones(tmp_path):
    output_path = tmp_path / "plan.ics"

    CalendarExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    calendar = output_path.read_text()
    lines = _unfold_lines(calendar)
    assert lines[:3] == [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//Blueprint//Execution Plan Calendar//EN",
    ]
    assert "END:VCALENDAR" in lines
    assert "X-BLUEPRINT-PLAN-ID:plan-test" in lines
    assert "X-BLUEPRINT-SKIPPED-TASK-COUNT:1" in lines
    assert "X-BLUEPRINT-SKIPPED-TASK-IDS:task-undated" in lines

    uids = [line for line in lines if line.startswith("UID:")]
    assert "UID:blueprint-plan-test-task-due@blueprint" in uids
    assert "UID:blueprint-plan-test-task-range@blueprint" in uids
    assert "UID:blueprint-plan-test-milestone-Foundation@blueprint" in uids
    assert not any("task-undated" in uid for uid in uids)

    assert "SUMMARY:task-due: Build API" in lines
    assert "DTSTART;VALUE=DATE:20260510" in lines
    assert "DTEND;VALUE=DATE:20260511" in lines
    assert "DTSTART;VALUE=DATE:20260512" in lines
    assert "DTEND;VALUE=DATE:20260516" in lines
    assert "SUMMARY:Milestone: Foundation" in lines


def test_calendar_validation_passes_for_rendered_export(tmp_path):
    output_path = tmp_path / "plan.ics"
    CalendarExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    findings = validate_rendered_export(
        target="calendar",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert findings == []


def test_calendar_validation_catches_empty_export(tmp_path):
    artifact_path = tmp_path / "empty.ics"
    artifact_path.write_text("")

    findings = validate_rendered_export(
        target="calendar",
        artifact_path=artifact_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == ["calendar.empty"]


def test_calendar_validation_catches_malformed_export(tmp_path):
    artifact_path = tmp_path / "bad.ics"
    artifact_path.write_text("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nEND:VCALENDAR\r\n")

    findings = validate_rendered_export(
        target="calendar",
        artifact_path=artifact_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert any(finding.code == "calendar.empty" for finding in findings)
    assert any(finding.code == "calendar.missing_task_event" for finding in findings)


def test_export_run_calendar_writes_ics_file_and_records_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "calendar"])

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-calendar.ics"
    assert output_path.exists()
    assert "Exported to:" in result.output
    assert "BEGIN:VCALENDAR" in output_path.read_text()

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "calendar"
    assert records[0]["export_format"] == "icalendar"
    assert records[0]["output_path"] == str(output_path)


def _unfold_lines(content):
    lines = []
    for line in content.replace("\r\n", "\n").splitlines():
        if line.startswith(" ") and lines:
            lines[-1] += line[1:]
        else:
            lines.append(line)
    return lines


def _execution_plan(include_tasks=True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {
                "name": "Foundation",
                "description": "Set up the project",
                "metadata": {"target_date": "2026-05-20"},
            },
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
            "id": "task-due",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
            "estimated_complexity": "medium",
            "status": "pending",
            "metadata": {"due_date": "2026-05-10"},
        },
        {
            "id": "task-range",
            "title": "Build UI",
            "description": "Implement the board view",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-due"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI renders"],
            "estimated_complexity": "medium",
            "status": "in_progress",
            "metadata": {"start_date": "2026-05-12", "due_date": "2026-05-15"},
        },
        {
            "id": "task-undated",
            "title": "Write docs",
            "description": "Document the report output",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["README.md"],
            "acceptance_criteria": ["Docs describe usage"],
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
        "problem_statement": "Need an iCalendar export",
        "mvp_goal": "Export execution tasks as calendar events",
        "product_surface": "CLI",
        "scope": ["Calendar exporter"],
        "non_goals": ["Calendar sync service"],
        "assumptions": ["Tasks may include scheduling metadata"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Undated tasks should not crash export"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Calendar contains dated tasks"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
