from blueprint.exporters import ConfluenceMarkdownExporter
from blueprint.exporters.registry import create_exporter, get_exporter_registration


def test_confluence_markdown_exporter_renders_complete_plan(tmp_path):
    output_path = tmp_path / "confluence.md"

    ConfluenceMarkdownExporter().export(_execution_plan(), _implementation_brief(), str(output_path))

    content = output_path.read_text()
    assert content.endswith("\n")
    assert "# Implementation Plan: Test Brief" in content
    assert "## Source Brief Summary" in content
    assert "- Source Brief ID: `sb-test`" in content
    assert "- Problem: Need a Confluence wiki export" in content
    assert "## Task Table" in content
    assert "| `task-api` | Build API | pending | agent | codex | Foundation | `task-setup` | src/app.py | API returns data<br>Schema validates payloads |" in content
    assert "## Dependencies" in content
    assert "- `task-api` depends on `task-setup`." in content
    assert "## Risks" in content
    assert "| `RISK-001` | Missing task status data | Run exporter tests |" in content
    assert "## Acceptance Criteria" in content
    assert "### `task-api` Build API" in content
    assert "- API returns data" in content
    assert "### Definition of Done" in content
    assert "- Wiki page captures execution plan" in content
    assert "## Validation Commands" in content
    assert "- Test Strategy: Run pytest" in content
    assert "  - test: `poetry run pytest`" in content
    assert "  - lint: `poetry run ruff check`" in content


def test_confluence_markdown_exporter_handles_sparse_plan():
    plan = _execution_plan()
    plan.update(
        {
            "target_engine": None,
            "target_repo": None,
            "project_type": None,
            "milestones": [],
            "test_strategy": None,
            "metadata": {},
            "tasks": [],
        }
    )
    brief = _implementation_brief()
    brief["risks"] = []
    brief["definition_of_done"] = []

    content = ConfluenceMarkdownExporter().render(plan, brief)

    assert "- Target Engine: N/A" in content
    assert "| none | No tasks defined | N/A | N/A | N/A | N/A | none | none | None |" in content
    assert "No task dependencies defined." in content
    assert "| none | No implementation risks listed | N/A |" in content
    assert "No task acceptance criteria defined." in content
    assert "- Commands: None detected" in content


def test_confluence_markdown_exporter_renders_dependencies_for_each_task():
    content = ConfluenceMarkdownExporter().render(_execution_plan(), _implementation_brief())

    assert "- `task-setup` has no dependencies." in content
    assert "- `task-api` depends on `task-setup`." in content
    assert "- `task-ui` depends on `task-api`, `task-copy`." in content


def test_confluence_markdown_exporter_escapes_table_pipes_and_newlines():
    plan = _execution_plan()
    plan["tasks"] = [
        {
            "id": "task-copy",
            "title": "Write | copy\nfor page",
            "description": "Draft interface copy",
            "milestone": "Interface | Docs",
            "owner_type": "human",
            "suggested_engine": "codex",
            "depends_on": ["task|api"],
            "files_or_modules": ["docs/wiki | page.md", "src/ui.py\nsrc/copy.py"],
            "acceptance_criteria": ["Copy has | approval", "No broken\nlines"],
            "estimated_complexity": "low",
            "status": "pending",
        }
    ]

    content = ConfluenceMarkdownExporter().render(plan, _implementation_brief())
    row = next(line for line in content.splitlines() if line.startswith("| `task-copy` |"))

    assert "Write \\| copy<br>for page" in row
    assert "Interface \\| Docs" in row
    assert "`task\\|api`" in row
    assert "docs/wiki \\| page.md" in row
    assert "src/ui.py<br>src/copy.py" in row
    assert "Copy has \\| approval<br>No broken<br>lines" in row
    assert len(_split_markdown_table_row(row)) == 9


def test_confluence_markdown_exporter_is_registered_and_importable():
    registration = get_exporter_registration("confluence-markdown")

    assert registration.default_format == "markdown"
    assert registration.extension == ".md"
    assert isinstance(create_exporter("confluence_markdown"), ConfluenceMarkdownExporter)


def _execution_plan():
    return {
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
        "metadata": {
            "validation_commands": {
                "test": ["poetry run pytest"],
                "lint": ["poetry run ruff check"],
            }
        },
        "tasks": [
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
                "files_or_modules": ["src/app.py"],
                "acceptance_criteria": ["API returns data", "Schema validates payloads"],
                "estimated_complexity": "medium",
                "status": "pending",
                "test_command": "poetry run pytest tests/test_api.py",
            },
            {
                "id": "task-copy",
                "title": "Write copy",
                "description": "Draft interface copy",
                "milestone": "Interface",
                "owner_type": "human",
                "suggested_engine": "codex",
                "depends_on": [],
                "files_or_modules": ["docs/wiki.md"],
                "acceptance_criteria": ["Copy is approved"],
                "estimated_complexity": "low",
                "status": "pending",
            },
            {
                "id": "task-ui",
                "title": "Build UI",
                "description": "Create the interface",
                "milestone": "Interface",
                "owner_type": "agent",
                "suggested_engine": "codex",
                "depends_on": ["task-api", "task-copy"],
                "files_or_modules": ["src/ui.py"],
                "acceptance_criteria": ["UI renders"],
                "estimated_complexity": "medium",
                "status": "in_progress",
            },
        ],
    }


def _implementation_brief():
    return {
        "id": "ib-test",
        "source_brief_id": "sb-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "CLI workflow",
        "problem_statement": "Need a Confluence wiki export",
        "mvp_goal": "Export page-ready markdown",
        "product_surface": "CLI",
        "scope": ["Confluence markdown exporter"],
        "non_goals": ["Confluence API publishing"],
        "assumptions": ["Markdown import is available"],
        "architecture_notes": "Use the exporter interface",
        "data_requirements": "Execution plans and tasks",
        "integration_points": [],
        "risks": ["Missing task status data"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Wiki page captures execution plan"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }


def _split_markdown_table_row(row: str) -> list[str]:
    inner = row.strip().strip("|")
    cells: list[str] = []
    current: list[str] = []
    for index, char in enumerate(inner):
        if char == "|" and (index == 0 or inner[index - 1] != "\\"):
            cells.append("".join(current).strip())
            current = []
            continue
        current.append(char)
    cells.append("".join(current).strip())
    return cells
