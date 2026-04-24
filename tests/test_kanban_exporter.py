import re
from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_rendered_export
from blueprint.exporters.kanban import KanbanExporter
from blueprint.store import Store, init_db


def test_kanban_exporter_renders_status_columns_and_task_cards(tmp_path):
    output_path = tmp_path / "kanban.md"

    KanbanExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    board = output_path.read_text()
    assert board.startswith("# Execution Plan Kanban Board: plan-test\n")
    assert "- Plan ID: `plan-test`" in board
    assert "- Implementation Brief: `ib-test`" in board
    for status in ["pending", "in_progress", "blocked", "completed", "skipped"]:
        assert f"## {status}" in board

    assert "### `task-copy` - Write copy" in board
    assert "- Milestone: Interface" in board
    assert "- Suggested Engine: codex" in board
    assert "- Dependencies: task-api (pending)" in board
    assert "- Blocked Reason: Waiting for product direction" in board
    assert "- Acceptance Criteria: 2" in board


def test_kanban_exporter_places_each_task_once_in_correct_status_column(tmp_path):
    output_path = tmp_path / "kanban.md"

    KanbanExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    rendered_tasks = _rendered_tasks_by_column(output_path.read_text())
    assert rendered_tasks == {
        "pending": ["task-api", "task-docs"],
        "in_progress": ["task-ui"],
        "blocked": ["task-copy"],
        "completed": ["task-setup"],
        "skipped": ["task-schema"],
    }


def test_kanban_validation_catches_missing_columns(tmp_path):
    artifact_path = tmp_path / "kanban.md"
    artifact_path.write_text(
        "# Execution Plan Kanban Board: plan-test\n"
        "- Plan ID: `plan-test`\n"
        "- Implementation Brief: `ib-test`\n"
        "## pending\n"
        "## in_progress\n"
        "## blocked\n"
        "## completed\n"
    )

    findings = validate_rendered_export(
        target="kanban",
        artifact_path=artifact_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert any(finding.code == "kanban.missing_column" for finding in findings)


def test_kanban_validation_catches_task_count_mismatches(tmp_path):
    artifact_path = tmp_path / "kanban.md"
    artifact_path.write_text(
        "# Execution Plan Kanban Board: plan-test\n"
        "- Plan ID: `plan-test`\n"
        "- Implementation Brief: `ib-test`\n"
        "## pending\n"
        "### `task-api` - Build API\n"
        "## in_progress\n"
        "## blocked\n"
        "## completed\n"
        "## skipped\n"
    )

    findings = validate_rendered_export(
        target="kanban",
        artifact_path=artifact_path,
        execution_plan=_execution_plan(),
        implementation_brief=_implementation_brief(),
    )

    assert any(finding.code == "kanban.task_count_mismatch" for finding in findings)


def test_export_run_kanban_writes_file_and_records_export(tmp_path, monkeypatch):
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

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "--target", "kanban"])

    assert result.exit_code == 0, result.output
    output_path = export_dir / f"{plan_id}-kanban.md"
    assert output_path.exists()
    assert "Exported to:" in result.output
    assert "# Execution Plan Kanban Board:" in output_path.read_text()

    records = Store(str(db_path)).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "kanban"
    assert records[0]["export_format"] == "markdown"
    assert records[0]["output_path"] == str(output_path)


def _rendered_tasks_by_column(content):
    rendered_tasks = {}
    current_column = None
    for line in content.splitlines():
        stripped = line.strip()
        if stripped.startswith("## "):
            current_column = stripped[3:]
            rendered_tasks[current_column] = []
            continue
        match = re.match(r"^### `([^`]+)` - .+", stripped)
        if match and current_column:
            rendered_tasks[current_column].append(match.group(1))
    return rendered_tasks


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
            "acceptance_criteria": ["Copy is approved", "Copy has owner signoff"],
            "estimated_complexity": "low",
            "status": "blocked",
            "blocked_reason": "Waiting for product direction",
        },
        {
            "id": "task-ui",
            "title": "Build UI",
            "description": "Implement the board view",
            "milestone": "Interface",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/ui.py"],
            "acceptance_criteria": ["UI renders"],
            "estimated_complexity": "medium",
            "status": "in_progress",
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
        "problem_statement": "Need a Markdown Kanban board",
        "mvp_goal": "Export execution tasks as a board",
        "product_surface": "CLI",
        "scope": ["Kanban exporter"],
        "non_goals": ["HTML dashboard"],
        "assumptions": ["Tasks already have statuses"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Missing task status data"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Board contains all tasks"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
