import pytest

from blueprint.config import Config
from blueprint.exporters.claude_code import ClaudeCodeExporter
from blueprint.exporters.codex import CodexExporter
from blueprint.exporters.smoothie import SmoothieExporter
from blueprint.exporters.templates import TemplateRenderError


def test_exporters_fall_back_to_built_in_markdown(tmp_path):
    output_path = tmp_path / "codex.md"

    CodexExporter(config=Config()).export(
        _execution_plan(), _implementation_brief(), str(output_path)
    )

    content = output_path.read_text()
    assert content.startswith("# BUILD: Test Brief")
    assert "## Build Plan" in content
    assert "Task 1.1: Setup project" in content


def test_codex_exporter_uses_configured_templates(tmp_path):
    main_template = tmp_path / "codex-template.md"
    task_template = tmp_path / "task-template.md"
    main_template.write_text("# {brief.title}\n\nRepo: {plan.target_repo}\n\nTasks:\n{tasks}\n")
    task_template.write_text("- {task.id}: {task.title}\n  Files:\n{task.files_or_modules}")
    config = Config()
    config.data["exports"]["templates"]["codex"] = {
        "path": str(main_template),
        "task_path": str(task_template),
    }
    output_path = tmp_path / "codex.md"

    CodexExporter(config=config).export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    assert output_path.read_text() == (
        "# Test Brief\n\n"
        "Repo: example/repo\n\n"
        "Tasks:\n"
        "- task-setup: Setup project\n"
        "  Files:\n"
        "- pyproject.toml\n\n"
        "- task-api: Build API\n"
        "  Files:\n"
        "- src/app.py\n"
    )


def test_template_paths_can_be_relative_to_config_file(tmp_path):
    templates_dir = tmp_path / "templates"
    templates_dir.mkdir()
    (templates_dir / "smoothie.md").write_text("Smoothie: {brief.title}\n{tasks}")
    (templates_dir / "task.md").write_text("{task.title}")
    config_path = tmp_path / ".blueprint.yaml"
    config_path.write_text(
        """
exports:
  templates:
    smoothie:
      path: templates/smoothie.md
      task_path: templates/task.md
"""
    )
    output_path = tmp_path / "smoothie.md"

    SmoothieExporter(config=Config(str(config_path))).export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    assert output_path.read_text() == "Smoothie: Test Brief\nSetup project\n\nBuild API"


def test_missing_placeholder_reports_template_and_placeholder(tmp_path):
    main_template = tmp_path / "claude.md"
    main_template.write_text("# {brief.missing_title}\n")
    config = Config()
    config.data["exports"]["templates"]["claude_code"] = {"path": str(main_template)}

    with pytest.raises(TemplateRenderError) as exc_info:
        ClaudeCodeExporter(config=config).export(
            _execution_plan(),
            _implementation_brief(),
            str(tmp_path / "claude.md"),
        )

    message = str(exc_info.value)
    assert "{brief.missing_title}" in message
    assert str(main_template) in message


def test_task_template_supports_brief_plan_and_task_fields(tmp_path):
    main_template = tmp_path / "smoothie.md"
    task_template = tmp_path / "task.md"
    main_template.write_text("{tasks}")
    task_template.write_text("{brief.id}/{plan.id}/{task.id}: {task.acceptance_criteria}")
    config = Config()
    config.data["exports"]["templates"]["smoothie"] = {
        "path": str(main_template),
        "task_path": str(task_template),
    }
    output_path = tmp_path / "smoothie.md"

    SmoothieExporter(config=config).export(
        _execution_plan(),
        _implementation_brief(),
        str(output_path),
    )

    assert output_path.read_text() == (
        "ib-test/plan-test/task-setup: - Project installs\n\n"
        "ib-test/plan-test/task-api: - API returns data"
    )


def _execution_plan():
    return {
        "id": "plan-test",
        "implementation_brief_id": "ib-test",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Foundation", "description": "Set up the project"}],
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
                "status": "pending",
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
                "acceptance_criteria": ["API returns data"],
                "estimated_complexity": "medium",
                "status": "pending",
            },
        ],
        "test_strategy": "Run pytest",
        "handoff_prompt": "Build the plan",
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
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
        "problem_statement": "Need configurable exports",
        "mvp_goal": "Render configured markdown templates",
        "product_surface": "CLI",
        "scope": ["Template rendering"],
        "non_goals": ["Template conditionals"],
        "assumptions": ["Simple placeholders are enough"],
        "architecture_notes": "Use shared exporter template renderer",
        "data_requirements": "Briefs, plans, and tasks",
        "integration_points": [],
        "risks": ["Missing placeholders"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Templates render"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
