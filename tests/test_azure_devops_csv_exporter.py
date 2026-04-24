import csv
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.azure_devops_csv import AzureDevOpsCsvExporter
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.store import Store, init_db


def test_azure_devops_csv_exporter_writes_one_row_per_task_with_required_headers(tmp_path):
    output_path = tmp_path / "azure.csv"

    AzureDevOpsCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert _read_headers(output_path) == AzureDevOpsCsvExporter.FIELDNAMES
    assert len(rows) == 3
    assert [row["Title"] for row in rows] == [
        "Setup project",
        "Build API",
        "Wire UI",
    ]
    assert [row["Work Item Type"] for row in rows] == ["User Story", "Task", "User Story"]
    assert [row["Iteration Path"] for row in rows] == ["Foundation", "Delivery", "Delivery"]
    assert rows[0]["Priority"] == "3"
    assert rows[1]["Priority"] == "1"
    assert rows[1]["Depends On"] == "task-setup"
    assert rows[2]["Depends On"] == "task-setup\ntask-api"


def test_azure_devops_csv_exporter_renders_tags_context_and_dependency_fields(tmp_path):
    output_path = tmp_path / "azure.csv"

    AzureDevOpsCsvExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    rows = _read_csv(output_path)
    assert rows[1]["Tags"] == (
        "blueprint-plan:plan-test; blueprint-brief:ib-test; codex; agent; "
        "Delivery; api; planning"
    )
    assert rows[1]["Area Path"] == "Platform\\API"
    assert rows[1]["Parent"] == "Feature 42"
    assert rows[1]["Acceptance Criteria"] == "API returns data\nSchema validates payloads"
    assert "Plan: plan-test" in rows[1]["Description"]
    assert "Implementation brief: ib-test - Test Brief" in rows[1]["Description"]
    assert "Files/Modules:\n- src/app.py\n- src/schema.py" in rows[1]["Description"]


def test_azure_devops_csv_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "azure-devops-csv")

    assert result.passed
    assert result.findings == []


def test_azure_devops_csv_rendered_validation_fails_for_missing_required_header(tmp_path):
    output_path = tmp_path / "azure.csv"
    output_path.write_text(
        "Work Item Type,Title,Description,Acceptance Criteria,Area Path,"
        "Iteration Path,Parent,Priority,Depends On\n"
        "User Story,Setup project,Description,Done,Area,Foundation,,3,\n"
        "Task,Build API,Description,Done,Area,Delivery,,1,task-setup\n"
        "User Story,Wire UI,Description,Done,Area,Delivery,,1,task-setup\n"
    )

    findings = validate_rendered_export(
        target="azure-devops-csv",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == ["azure_devops_csv.missing_column"]
    assert "Tags" in findings[0].message


def test_azure_devops_csv_rendered_validation_fails_for_row_count_mismatch(tmp_path):
    output_path = tmp_path / "azure.csv"
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=AzureDevOpsCsvExporter.FIELDNAMES)
        writer.writeheader()
        writer.writerow(
            {
                "Work Item Type": "User Story",
                "Title": "Only task",
                "Description": "Description",
                "Acceptance Criteria": "Done",
                "Tags": "blueprint-plan:plan-test",
                "Area Path": "Area",
                "Iteration Path": "Foundation",
                "Parent": "",
                "Priority": "2",
                "Depends On": "",
            }
        )

    findings = validate_rendered_export(
        target="azure-devops-csv",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == ["azure_devops_csv.row_count_mismatch"]


def test_export_render_azure_devops_csv_writes_file_and_records_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(cli, ["export", "render", plan_id, "azure-devops-csv"])

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-azure-devops-csv.csv"
    assert output_path.exists()
    assert "Exported to:" in result.output

    rows = _read_csv(output_path)
    assert len(rows) == 3
    assert rows[0]["Title"] == "Setup project"

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "azure-devops-csv"
    assert records[0]["export_format"] == "csv"
    assert records[0]["output_path"] == str(output_path)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _read_headers(path: Path) -> list[str]:
    with path.open(newline="") as f:
        return list(csv.reader(f))[0]


def _execution_plan(include_tasks=True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [
            {"name": "Foundation", "description": "Set up the project"},
            {"name": "Delivery", "description": "Ship the API"},
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
            "metadata": {"labels": ["setup"], "components": ["backend"]},
        },
        {
            "id": "task-api",
            "title": "Build API",
            "description": "Implement the command API",
            "milestone": "Delivery",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup"],
            "files_or_modules": ["src/app.py", "src/schema.py"],
            "acceptance_criteria": ["API returns data", "Schema validates payloads"],
            "estimated_complexity": "medium",
            "status": "pending",
            "metadata": {
                "azure_area_path": "Platform\\API",
                "azure_parent": "Feature 42",
                "azure_priority": "1",
                "azure_work_item_type": "Task",
                "labels": ["api"],
                "tags": ["planning"],
            },
        },
        {
            "id": "task-ui",
            "title": "Wire UI",
            "description": "Connect the web UI",
            "milestone": "Delivery",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup", "task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI renders API data"],
            "estimated_complexity": "high",
            "status": "pending",
            "metadata": {},
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
        "problem_statement": "Need an Azure DevOps CSV issue export",
        "mvp_goal": "Export tasks for Azure Boards import",
        "product_surface": "CLI",
        "scope": ["Azure DevOps CSV exporter"],
        "non_goals": ["Azure DevOps API integration"],
        "assumptions": ["Azure Boards import parses standard CSV quoting"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Invalid CSV escaping"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Tasks export as Azure work item rows"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
