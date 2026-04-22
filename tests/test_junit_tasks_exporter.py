from pathlib import Path
from xml.etree import ElementTree

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.junit_tasks import JUnitTasksExporter
from blueprint.store import Store, init_db


def test_junit_tasks_exporter_writes_valid_xml(tmp_path):
    output_path = tmp_path / "junit.xml"

    JUnitTasksExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    root = ElementTree.parse(output_path).getroot()
    assert root.tag == "testsuites"
    assert root.attrib["name"] == "plan-test"
    assert root.attrib["tests"] == "6"
    assert root.attrib["failures"] == "4"
    assert root.attrib["skipped"] == "1"


def test_junit_tasks_exporter_maps_tasks_to_testcases_by_milestone(tmp_path):
    output_path = tmp_path / "junit.xml"

    JUnitTasksExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    root = ElementTree.parse(output_path).getroot()
    suites = {suite.attrib["name"]: suite for suite in root.findall("testsuite")}

    assert list(suites) == ["Foundation", "Interface", "Unplanned"]
    assert [case.attrib["name"] for case in suites["Foundation"].findall("testcase")] == [
        "task-setup: Setup project",
        "task-schema: Build schema",
        "task-api: Build API",
    ]
    assert [case.attrib["name"] for case in suites["Interface"].findall("testcase")] == [
        "task-copy: Write copy",
        "task-docs: Write docs",
    ]
    assert suites["Unplanned"].find("testcase").attrib["name"] == ("task-lint: Add linting")


def test_junit_tasks_exporter_status_handling(tmp_path):
    output_path = tmp_path / "junit.xml"

    JUnitTasksExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    root = ElementTree.parse(output_path).getroot()
    cases = {
        case.find("properties/property[@name='task_id']").attrib["value"]: case
        for case in root.findall(".//testcase")
    }

    assert cases["task-setup"].find("failure") is None
    assert cases["task-setup"].find("skipped") is None
    assert cases["task-schema"].find("skipped").attrib["message"] == "Task was skipped."
    assert cases["task-copy"].find("failure").attrib == {
        "message": "Waiting for product direction",
        "type": "blocked",
    }
    assert cases["task-lint"].find("failure").attrib == {
        "message": "Task is blocked.",
        "type": "blocked",
    }
    assert cases["task-api"].find("failure").attrib == {
        "message": "Task is not complete: status is pending.",
        "type": "incomplete",
    }
    assert cases["task-docs"].find("failure").attrib == {
        "message": "Task is not complete: status is in_progress.",
        "type": "incomplete",
    }


def test_export_run_junit_tasks_writes_file_and_records_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "junit-tasks"])

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-junit-tasks.xml"
    assert output_path.exists()
    assert "Exported to:" in result.output
    assert ElementTree.parse(output_path).getroot().tag == "testsuites"

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "junit-tasks"
    assert records[0]["export_format"] == "xml"
    assert records[0]["output_path"] == str(output_path)
    assert records[0]["export_metadata"] == {
        "brief_id": "ib-test",
        "brief_title": "Test Brief",
    }


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
            "id": "task-schema",
            "title": "Build schema",
            "description": "Create persistence schema",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/schema.py"],
            "acceptance_criteria": ["Schema validates payloads"],
            "estimated_complexity": "medium",
            "status": "skipped",
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup", "task-schema"],
            "files_or_modules": ["src/app.py"],
            "acceptance_criteria": ["API returns data"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
        {
            "id": "task-copy",
            "title": "Write copy",
            "description": "Draft interface copy",
            "milestone": "Interface",
            "owner_type": "human",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["Copy is approved"],
            "estimated_complexity": "low",
            "status": "blocked",
            "metadata": {"blocked_reason": "Waiting for product direction"},
        },
        {
            "id": "task-docs",
            "title": "Write docs",
            "description": "Document the report output",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["README.md"],
            "acceptance_criteria": ["Docs describe usage"],
            "estimated_complexity": "low",
            "status": "in_progress",
        },
        {
            "id": "task-lint",
            "title": "Add linting",
            "description": "Add project lint configuration",
            "milestone": "Unplanned",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["pyproject.toml"],
            "acceptance_criteria": ["Lint command exists"],
            "estimated_complexity": "low",
            "status": "blocked",
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
        "problem_statement": "Need a JUnit task export",
        "mvp_goal": "Export tasks for CI workflows",
        "product_surface": "CLI",
        "scope": ["JUnit exporter"],
        "non_goals": ["CI service integration"],
        "assumptions": ["CI can ingest JUnit XML"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Invalid XML output"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Tasks export as JUnit test cases"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
