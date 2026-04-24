from pathlib import Path

from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.exporters.export_validation import validate_export
from blueprint.exporters.notion_markdown import NotionMarkdownExporter
from blueprint.store import Store, init_db


def test_notion_markdown_exporter_renders_notion_sections_and_task_table(tmp_path):
    output_path = tmp_path / "notion.md"

    NotionMarkdownExporter().export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    content = output_path.read_text()
    assert content.startswith("# Execution Plan: Test Brief\n")
    assert "## Plan Overview" in content
    assert "## Milestones" in content
    assert "## Task Database" in content
    assert "## Dependency Table" in content
    assert "## Risks" in content
    assert "## Validation Checklist" in content
    assert "- Plan ID: `plan-test`" in content
    assert "- Implementation Brief: `ib-test`" in content

    rows = _task_table_rows(content)
    assert [row[0] for row in rows] == ["`task-setup`", "`task-api`", "`task-copy`"]
    assert len(rows) == len(_tasks())
    assert rows[1] == [
        "`task-api`",
        "Build API",
        "pending",
        "agent",
        "codex",
        "Foundation",
        "`task-setup`",
        "src/app.py<br>src/schema.py",
        "API returns data<br>Schema validates payloads",
    ]


def test_notion_markdown_exporter_escapes_table_pipes_and_newlines():
    content = NotionMarkdownExporter().render(
        _execution_plan(tasks=_tasks_with_escaped_content()),
        _implementation_brief(),
    )

    rows = _task_table_rows(content)
    assert len(rows) == 1
    assert rows[0][1] == "Build A\\|B"
    assert rows[0][7] == "src/a\\|b.py<br>docs/line<br>break.md"
    assert rows[0][8] == "Supports A\\|B<br>Handles line<br>breaks"


def test_notion_markdown_exporter_handles_empty_optional_fields():
    content = NotionMarkdownExporter().render(
        _execution_plan(tasks=[_empty_optional_task()]),
        _implementation_brief(risks=[]),
    )

    rows = _task_table_rows(content)
    assert rows == [
        [
            "`task-empty`",
            "Empty optional fields",
            "pending",
            "Unassigned",
            "Unassigned",
            "Ungrouped",
            "none",
            "none",
            "None",
        ]
    ]
    assert "| none | No implementation risks listed | N/A |" in content


def test_notion_markdown_validation_passes_for_rendered_export():
    result = validate_export(_execution_plan(), _implementation_brief(), "notion-markdown")

    assert result.passed
    assert result.findings == []


def test_export_validate_notion_markdown_cli_passes(tmp_path, monkeypatch):
    store = _write_config_and_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(
        cli,
        ["export", "validate", plan_id, "--target", "notion-markdown"],
    )

    assert result.exit_code == 0, result.output
    assert "Validation passed for notion-markdown" in result.output


def test_export_run_notion_markdown_writes_file_and_records_export(tmp_path, monkeypatch):
    store = _write_config_and_store(tmp_path, monkeypatch)
    store.insert_implementation_brief(_implementation_brief())
    plan_id = store.insert_execution_plan(_execution_plan(include_tasks=False), _tasks())

    result = CliRunner().invoke(cli, ["export", "run", plan_id, "notion-markdown"])

    assert result.exit_code == 0, result.output
    output_path = tmp_path / "exports" / f"{plan_id}-notion-markdown.md"
    assert output_path.exists()
    assert "# Execution Plan: Test Brief" in output_path.read_text()

    records = Store(str(tmp_path / "blueprint.db")).list_export_records(plan_id=plan_id)
    assert len(records) == 1
    assert records[0]["target_engine"] == "notion-markdown"
    assert records[0]["export_format"] == "markdown"
    assert records[0]["output_path"] == str(output_path)


def _task_table_rows(content: str) -> list[list[str]]:
    lines = []
    in_section = False
    for line in content.splitlines():
        if line == "## Task Database":
            in_section = True
            continue
        if in_section and line.startswith("## "):
            break
        if in_section and line.startswith("|"):
            lines.append(line)
    return [_split_markdown_table_row(line) for line in lines[2:]]


def _split_markdown_table_row(row: str) -> list[str]:
    inner = row.strip().strip("|")
    cells = []
    current = []
    for index, char in enumerate(inner):
        if char == "|" and (index == 0 or inner[index - 1] != "\\"):
            cells.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    cells.append("".join(current).strip())
    return cells


def _write_config_and_store(tmp_path, monkeypatch):
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
    return init_db(str(db_path))


def _execution_plan(include_tasks=True, tasks=None):
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
        plan["tasks"] = tasks if tasks is not None else _tasks()
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
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": ["task-setup"],
            "files_or_modules": ["src/app.py", "src/schema.py"],
            "acceptance_criteria": ["API returns data", "Schema validates payloads"],
            "estimated_complexity": "medium",
            "status": "pending",
        },
        {
            "id": "task-copy",
            "title": "Write copy",
            "description": "Draft interface copy",
            "milestone": "Interface",
            "owner_type": "human",
            "suggested_engine": "manual",
            "depends_on": ["task-api"],
            "files_or_modules": ["src/copy.py"],
            "acceptance_criteria": ["Copy is clear"],
            "estimated_complexity": "low",
            "status": "pending",
        },
    ]


def _tasks_with_escaped_content():
    return [
        {
            "id": "task-escaped",
            "title": "Build A|B",
            "description": "Render escaped content",
            "milestone": "Foundation",
            "owner_type": "agent",
            "suggested_engine": "codex",
            "depends_on": [],
            "files_or_modules": ["src/a|b.py", "docs/line\nbreak.md"],
            "acceptance_criteria": ["Supports A|B", "Handles line\nbreaks"],
            "estimated_complexity": "medium",
            "status": "pending",
        }
    ]


def _empty_optional_task():
    return {
        "id": "task-empty",
        "title": "Empty optional fields",
        "description": "Render empty optional fields",
        "milestone": None,
        "owner_type": None,
        "suggested_engine": None,
        "depends_on": [],
        "files_or_modules": None,
        "acceptance_criteria": [],
        "estimated_complexity": None,
        "status": "pending",
    }


def _implementation_brief(risks=None):
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need Notion workspace planning exports",
        "mvp_goal": "Expose execution tasks as importable Markdown",
        "product_surface": "CLI",
        "scope": ["Notion exports"],
        "non_goals": ["Task execution"],
        "assumptions": ["Markdown import is enough"],
        "architecture_notes": "Use shared exporter validation helpers",
        "data_requirements": "Execution plans and tasks",
        "integration_points": ["CLI export command"],
        "risks": ["Missing planning context"] if risks is None else risks,
        "validation_plan": "Run Notion Markdown exporter tests",
        "definition_of_done": ["Notion target renders"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
