from click.testing import CliRunner

from blueprint import config as blueprint_config
from blueprint.cli import cli
from blueprint.importers.plan_markdown_importer import (
    PlanMarkdownImportError,
    parse_plan_markdown,
)
from blueprint.store import Store, init_db


def test_plan_markdown_frontmatter_fields_are_preserved():
    parsed = parse_plan_markdown(
        """
---
id: plan-manual
implementation_brief_id: ib-frontmatter
target_engine: codex
target_repo: example/repo
project_type: cli_tool
test_strategy: Run pytest
milestones:
  - name: Foundation
    description: Build the baseline
---

## task-setup: Setup importer

Description: Add the importer module.

Acceptance Criteria:
- Importer parses markdown files

Files/Modules:
- src/blueprint/importers/plan_markdown_importer.py
"""
    )

    assert parsed.plan["id"] == "plan-manual"
    assert parsed.plan["implementation_brief_id"] == "ib-frontmatter"
    assert parsed.plan["target_engine"] == "codex"
    assert parsed.plan["target_repo"] == "example/repo"
    assert parsed.plan["project_type"] == "cli_tool"
    assert parsed.plan["test_strategy"] == "Run pytest"
    assert parsed.plan["milestones"] == [
        {"name": "Foundation", "description": "Build the baseline"}
    ]
    assert parsed.plan["metadata"]["frontmatter"]["id"] == "plan-manual"
    assert parsed.tasks[0]["files_or_modules"] == [
        "src/blueprint/importers/plan_markdown_importer.py"
    ]


def test_plan_markdown_table_parses_task_fields():
    parsed = parse_plan_markdown(
        """
---
implementation_brief_id: ib-table
---

| id | title | description | acceptance criteria | depends_on | files/modules | milestone |
| --- | --- | --- | --- | --- | --- | --- |
| task-setup | Setup project | Create the baseline | Project installs; Tests run | | pyproject.toml | Foundation |
| task-api | Build API | Store imported plans | API returns data | task-setup | src/app.py, src/db.py | Foundation |
"""
    )

    assert len(parsed.tasks) == 2
    assert parsed.tasks[1]["id"] == "task-api"
    assert parsed.tasks[1]["depends_on"] == ["task-setup"]
    assert parsed.tasks[1]["files_or_modules"] == ["src/app.py", "src/db.py"]
    assert parsed.tasks[1]["acceptance_criteria"] == ["API returns data"]
    assert parsed.plan["milestones"] == [{"name": "Foundation"}]


def test_plan_markdown_validation_errors_are_actionable():
    try:
        parse_plan_markdown(
            """
---
implementation_brief_id: ib-invalid
---

| id | title | description | acceptance criteria | depends_on |
| --- | --- | --- | --- | --- |
| task-api | Build API | | | task-missing |
"""
        )
    except PlanMarkdownImportError as exc:
        message = str(exc)
    else:
        raise AssertionError("Expected PlanMarkdownImportError")

    assert "task-api: missing required description" in message
    assert "task-api: missing required acceptance criteria" in message
    assert "task-api: depends_on references unknown task id 'task-missing'" in message


def test_cli_import_markdown_inserts_plan_and_rejects_invalid_without_partial_insert(
    tmp_path,
    monkeypatch,
):
    _write_config(tmp_path, monkeypatch)
    store = init_db(str(tmp_path / "blueprint.db"))
    store.insert_implementation_brief(_implementation_brief())

    valid_path = tmp_path / "plan.md"
    valid_path.write_text(
        """
---
id: plan-imported
implementation_brief_id: ignored-by-cli
target_engine: codex
target_repo: example/repo
project_type: cli_tool
test_strategy: poetry run pytest
---

## task-setup: Setup project

Description: Create the project baseline.

Acceptance Criteria:
- Project installs

Files/Modules:
- pyproject.toml

## task-api: Build API

Description: Store imported execution plans.

Depends On: task-setup
Files/Modules: src/blueprint/cli.py, src/blueprint/store/db.py
Acceptance Criteria:
- Plan and tasks are inserted
""",
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        cli,
        ["plan", "import-markdown", str(valid_path), "--brief", "ib-test"],
    )

    assert result.exit_code == 0, result.output
    assert "Imported execution plan plan-imported" in result.output

    imported = Store(str(tmp_path / "blueprint.db")).get_execution_plan("plan-imported")
    assert imported["implementation_brief_id"] == "ib-test"
    assert imported["target_engine"] == "codex"
    assert imported["tasks"][1]["depends_on"] == ["task-setup"]
    assert imported["tasks"][1]["files_or_modules"] == [
        "src/blueprint/cli.py",
        "src/blueprint/store/db.py",
    ]

    invalid_path = tmp_path / "invalid.md"
    invalid_path.write_text(
        """
---
id: plan-invalid
implementation_brief_id: ib-test
---

| id | title | description | acceptance criteria |
| --- | --- | --- | --- |
| task-bad | Bad task | Missing criteria | |
""",
        encoding="utf-8",
    )

    invalid_result = CliRunner().invoke(
        cli,
        ["plan", "import-markdown", str(invalid_path), "--brief", "ib-test"],
    )

    assert invalid_result.exit_code != 0
    assert "missing required acceptance criteria" in invalid_result.output
    assert Store(str(tmp_path / "blueprint.db")).get_execution_plan("plan-invalid") is None


def _write_config(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    (tmp_path / ".blueprint.yaml").write_text(
        f"""
database:
  path: {tmp_path / "blueprint.db"}
exports:
  output_dir: {tmp_path}
""",
        encoding="utf-8",
    )
    blueprint_config.reload_config()


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Import markdown plans",
        "domain": "developer-tools",
        "problem_statement": "Blueprint needs offline plan ingestion.",
        "mvp_goal": "Import a markdown execution plan.",
        "scope": ["Plan import"],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Run pytest",
        "definition_of_done": ["Markdown plans can be imported"],
        "status": "ready_for_planning",
    }
