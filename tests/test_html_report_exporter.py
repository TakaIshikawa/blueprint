from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_rendered_export
from blueprint.exporters.html_report import HtmlReportExporter
from blueprint.store import Store, init_db


def test_html_report_exporter_renders_expected_sections(tmp_path):
    output_path = tmp_path / "report.html"

    HtmlReportExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    report = output_path.read_text()
    assert report.startswith("<!doctype html>\n")
    assert 'data-section="summary"' in report
    assert 'data-section="brief-summary"' in report
    assert 'data-section="status-counts"' in report
    assert 'data-section="milestones"' in report
    assert 'data-section="tasks"' in report
    assert 'data-section="dependencies"' in report
    assert 'data-section="risks"' in report
    assert 'data-section="validation-plan"' in report
    assert '<table id="task-table" data-task-table="true">' in report
    assert 'data-task-id="task-setup"' in report
    assert 'data-task-id="task-api"' in report
    assert "<h2>Dependency Summary</h2>" in report
    assert "<h2>Validation Plan</h2>" in report


def test_html_report_exporter_escapes_user_content(tmp_path):
    output_path = tmp_path / "report.html"
    brief = _implementation_brief()
    brief["title"] = "<script>alert('x')</script>"
    brief["risks"] = ["Use <unsafe> & quoted \"input\""]
    plan = _execution_plan()
    plan["tasks"][0]["title"] = "Setup <b>project</b>"
    plan["tasks"][0]["id"] = 'task-"setup"'
    plan["tasks"][1]["depends_on"] = ['task-"setup"']

    HtmlReportExporter().export(plan, brief, str(output_path))

    report = output_path.read_text()
    assert "<script>" not in report
    assert "&lt;script&gt;alert(&#x27;x&#x27;)&lt;/script&gt;" in report
    assert "Setup &lt;b&gt;project&lt;/b&gt;" in report
    assert "Use &lt;unsafe&gt; &amp; quoted" in report
    assert 'data-task-id="task-&quot;setup&quot;"' in report


def test_html_report_validation_passes_for_rendered_export(tmp_path):
    output_path = tmp_path / "report.html"
    HtmlReportExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    findings = validate_rendered_export(
        target="html-report",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert findings == []


def test_html_report_validation_catches_missing_section_and_task_count_mismatch(tmp_path):
    output_path = tmp_path / "report.html"
    HtmlReportExporter().export(_execution_plan(), _implementation_brief(), str(output_path))
    report = output_path.read_text()
    report = report.replace(' data-section="risks"', "", 1)
    start = report.index('      <tr data-task-id="task-api"')
    end = report.index("</tr>", start) + len("</tr>")
    output_path.write_text(report[:start] + report[end:])

    findings = validate_rendered_export(
        target="html-report",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    codes = [finding.code for finding in findings]
    assert "html_report.missing_section" in codes
    assert "html_report.task_count_mismatch" in codes
    assert "html_report.task_occurrence_mismatch" in codes


def test_export_run_html_report_writes_file_and_records_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(cli, ["export", "render", plan_id, "html-report"])

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-html-report.html"
    assert output_path.exists()
    assert "Exported to:" in result.output
    assert "<!doctype html>" in output_path.read_text()

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "html-report"
    assert records[0]["export_format"] == "html"
    assert records[0]["output_path"] == str(output_path)


def test_export_preview_html_report_prints_html(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db_path = tmp_path / "blueprint.db"
    Path(".blueprint.yaml").write_text(
        f"""
database:
  path: {db_path}
exports:
  output_dir: {tmp_path / "exports"}
"""
    )
    blueprint_config.reload_config()

    store = init_db(str(db_path))
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "preview", plan_id, "--target", "html-report"])

    assert result.exit_code == 0, result.output
    assert result.output.startswith("<!doctype html>\n")
    assert 'data-section="tasks"' in result.output
    assert not Store(str(db_path)).list_export_records(plan_id=plan_id)


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
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup"],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
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
        "problem_statement": "Need stakeholder-ready reports",
        "mvp_goal": "Share execution plans as portable HTML",
        "product_surface": "CLI",
        "scope": ["HTML report rendering"],
        "non_goals": ["Interactive dashboard"],
        "assumptions": ["Static HTML is enough"],
        "architecture_notes": "Use standard library escaping",
        "data_requirements": "Briefs, plans, and tasks",
        "integration_points": ["CLI export command"],
        "risks": ["User content may include HTML"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Report includes all stakeholder sections"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
