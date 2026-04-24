import csv
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export, validate_rendered_export
from blueprint.exporters.github_projects_csv import GitHubProjectsCsvExporter
from blueprint.store import Store, init_db


def test_github_projects_csv_exporter_writes_stable_headers_and_task_rows(tmp_path):
    output_path = tmp_path / "github-projects.csv"

    GitHubProjectsCsvExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    rows = _read_csv(output_path)
    assert _read_headers(output_path) == [
        "Title",
        "Body",
        "Status",
        "Milestone",
        "Labels",
        "Assignees",
        "Repository",
        "Task ID",
        "Dependencies",
        "Acceptance Criteria",
        "Files",
        "Estimate",
        "Suggested Engine",
    ]
    assert len(rows) == 3
    assert [row["Task ID"] for row in rows] == ["task-setup", "task-api", "task-ui"]
    assert [row["Title"] for row in rows] == ["Setup project", "Build API", "Wire UI"]


def test_github_projects_csv_exporter_preserves_execution_context(tmp_path):
    output_path = tmp_path / "github-projects.csv"

    GitHubProjectsCsvExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    rows = _read_csv(output_path)
    assert rows[0]["Labels"] == (
        "milestone:Foundation, engine:codex, complexity:low, setup, backend"
    )
    assert rows[0]["Assignees"] == ""
    assert rows[1]["Labels"] == (
        "milestone:Delivery, engine:codex, complexity:medium, risk:high, api, planning"
    )
    assert rows[1]["Assignees"] == "api-owner, reviewer"
    assert rows[1]["Repository"] == "acme/widgets"
    assert rows[1]["Dependencies"] == "task-setup"
    assert rows[1]["Acceptance Criteria"] == "API returns data\nSchema validates payloads"
    assert rows[1]["Files"] == "src/app.py\nsrc/schema.py"
    assert rows[1]["Estimate"] == "5"
    assert rows[1]["Suggested Engine"] == "codex"
    assert rows[2]["Dependencies"] == "task-setup\ntask-api"
    assert rows[2]["Status"] == "blocked"
    assert "Dependencies:\n- task-setup" in rows[1]["Body"]
    assert "Files/Modules:\n- src/app.py\n- src/schema.py" in rows[1]["Body"]


def test_github_projects_csv_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "github-projects-csv")

    assert result.passed
    assert result.findings == []


def test_github_projects_csv_validation_fails_for_malformed_csv(tmp_path):
    output_path = tmp_path / "github-projects.csv"
    output_path.write_text('"Title","Body"\n"Unclosed body\n')

    findings = validate_rendered_export(
        target="github-projects-csv",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == ["github_projects_csv.invalid_structure"]


def test_github_projects_csv_validation_fails_for_missing_tasks_and_duplicate_ids(tmp_path):
    output_path = tmp_path / "github-projects.csv"
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=GitHubProjectsCsvExporter.FIELDNAMES)
        writer.writeheader()
        writer.writerow(_row(task_id="task-setup", dependencies=""))
        writer.writerow(_row(task_id="task-setup", dependencies="task-missing"))

    findings = validate_rendered_export(
        target="github-projects-csv",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == [
        "github_projects_csv.row_count_mismatch",
        "github_projects_csv.missing_tasks",
        "github_projects_csv.duplicate_task_id",
        "github_projects_csv.unknown_dependency_id",
    ]


def test_github_projects_csv_validation_fails_for_exact_header_mismatch(tmp_path):
    output_path = tmp_path / "github-projects.csv"
    with output_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["Title", "Body", "Task ID"] + GitHubProjectsCsvExporter.FIELDNAMES[2:],
        )
        writer.writeheader()
        writer.writerow(_row(task_id="task-setup", dependencies=""))
        writer.writerow(_row(task_id="task-api", dependencies="task-setup"))
        writer.writerow(_row(task_id="task-ui", dependencies="task-api"))

    findings = validate_rendered_export(
        target="github-projects-csv",
        artifact_path=output_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert [finding.code for finding in findings] == ["github_projects_csv.header_mismatch"]


def test_export_run_github_projects_csv_writes_file_and_records_csv_export(tmp_path, monkeypatch):
    store = _setup_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(
        cli,
        ["export", "run", plan_id, "--target", "github-projects-csv"],
    )

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-github-projects-csv.csv"
    assert output_path.exists()
    assert "Exported to:" in result.output

    rows = _read_csv(output_path)
    assert len(rows) == 3
    assert rows[1]["Task ID"] == "task-api"

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "github-projects-csv"
    assert records[0]["export_format"] == "csv"
    assert records[0]["output_path"] == str(output_path)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def _read_headers(path: Path) -> list[str]:
    with path.open(newline="") as f:
        return list(csv.reader(f))[0]


def _row(task_id: str, dependencies: str) -> dict[str, str]:
    return {
        "Title": f"Title {task_id}",
        "Body": f"Body {task_id}",
        "Status": "pending",
        "Milestone": "Delivery",
        "Labels": "engine:codex",
        "Assignees": "",
        "Repository": "acme/widgets",
        "Task ID": task_id,
        "Dependencies": dependencies,
        "Acceptance Criteria": "Done",
        "Files": "src/app.py",
        "Estimate": "medium",
        "Suggested Engine": "codex",
    }


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


def _execution_plan(include_tasks: bool = True):
    plan = {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "relay",
        "target_repo": "acme/widgets",
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
                "assignees": ["api-owner", "reviewer"],
                "estimate": "5",
                "labels": ["api"],
                "tags": ["planning"],
                "risk": "high",
            },
        },
        {
            "id": "task-ui",
            "title": "Wire UI",
            "description": "Connect the project view",
            "milestone": "Delivery",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup", "task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI displays API data"],
            "estimated_complexity": "high",
            "status": "blocked",
            "metadata": {"assignee": "frontend-owner"},
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
        "problem_statement": "Need a GitHub Projects CSV export",
        "mvp_goal": "Export tasks for GitHub Projects import",
        "product_surface": "CLI",
        "scope": ["GitHub Projects CSV exporter"],
        "non_goals": ["Project API automation"],
        "assumptions": ["CSV consumers can import task rows"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Plan and task dictionaries",
        "integration_points": ["CLI export command"],
        "risks": ["Task IDs must remain stable"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Tasks export as CSV rows"],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
