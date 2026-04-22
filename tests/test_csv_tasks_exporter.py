import csv
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.csv_tasks import CsvTasksExporter
from blueprint.store import Store, init_db


def test_csv_tasks_exporter_escapes_csv_values(tmp_path):
    output_path = tmp_path / "tasks.csv"
    plan = _execution_plan()
    plan["tasks"][0]["title"] = 'Investigate "CSV", commas'

    CsvTasksExporter().export(plan, _implementation_brief(), str(output_path))

    content = output_path.read_text()
    assert '"Investigate ""CSV"", commas"' in content

    rows = _read_csv(output_path)
    assert rows[0]["title"] == 'Investigate "CSV", commas'


def test_csv_tasks_exporter_joins_list_fields(tmp_path):
    output_path = tmp_path / "tasks.csv"

    CsvTasksExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert rows[1]["depends_on"] == "task-setup; task-schema"
    assert rows[1]["files_or_modules"] == "src/app.py; src/schema.py"
    assert rows[1]["acceptance_criteria"] == "API returns data; Schema validates payloads"


def test_csv_tasks_exporter_writes_empty_optional_fields(tmp_path):
    output_path = tmp_path / "tasks.csv"
    plan = _execution_plan()
    plan["tasks"] = [
        {
            "id": "task-minimal",
            "title": "Minimal task",
            "description": "Only required task fields",
            "depends_on": [],
            "acceptance_criteria": ["It exports"],
        }
    ]

    CsvTasksExporter().export(plan, _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert rows == [
        {
            "plan_id": "plan-test",
            "task_id": "task-minimal",
            "title": "Minimal task",
            "milestone": "",
            "status": "pending",
            "suggested_engine": "",
            "depends_on": "",
            "files_or_modules": "",
            "estimated_complexity": "",
            "acceptance_criteria": "It exports",
        }
    ]


def test_export_run_csv_tasks_writes_file_and_records_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "csv-tasks"])

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-csv-tasks.csv"
    assert output_path.exists()
    assert "Exported to:" in result.output

    rows = _read_csv(output_path)
    assert rows[0]["plan_id"] == plan_id
    assert rows[0]["task_id"] == "task-setup"

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "csv-tasks"
    assert records[0]["export_format"] == "csv"
    assert records[0]["output_path"] == str(output_path)
    assert records[0]["export_metadata"] == {
        "brief_id": "ib-test",
        "brief_title": "Test Brief",
    }


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _execution_plan(include_tasks=True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation", "description": "Set up the project"}],
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
            "status": "pending",
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup", "task-schema"],
            "files_or_modules": ["src/app.py", "src/schema.py"],
            "acceptance_criteria": ["API returns data", "Schema validates payloads"],
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
        "problem_statement": "Need a CSV task export",
        "mvp_goal": "Export tasks for spreadsheet workflows",
        "product_surface": "CLI",
        "scope": ["CSV exporter"],
        "non_goals": ["Spreadsheet API integration"],
        "assumptions": ["CSV consumers parse standard quoting"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Invalid CSV escaping"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Tasks export as CSV rows"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
