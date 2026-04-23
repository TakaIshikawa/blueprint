from blueprint.exporters.smoothie import SmoothieExporter


def test_smoothie_exporter_uses_api_language_for_library_briefs(tmp_path):
    output_path = tmp_path / "library.md"

    SmoothieExporter().export(
        _execution_plan(),
        _implementation_brief(
            product_surface="Python library",
            scope=["Configure authentication", "Emit webhooks"],
        ),
        str(output_path),
    )

    content = output_path.read_text()
    assert "API Surfaces to Prototype" in content
    assert "Developer flow" in content
    assert "Screens/Views to Prototype" not in content
    assert "User lands on main screen" not in content


def test_smoothie_exporter_uses_command_language_for_cli_briefs(tmp_path):
    output_path = tmp_path / "cli.md"

    SmoothieExporter().export(
        _execution_plan(),
        _implementation_brief(
            product_surface="CLI",
            scope=["Initialize project", "Run checks"],
        ),
        str(output_path),
    )

    content = output_path.read_text()
    assert "Commands to Prototype" in content
    assert "Command-line workflow" in content
    assert "```bash" in content
    assert "Screens/Views to Prototype" not in content


def test_smoothie_exporter_keeps_screen_framing_for_web_briefs(tmp_path):
    output_path = tmp_path / "web.md"

    SmoothieExporter().export(
        _execution_plan(),
        _implementation_brief(
            product_surface="Web app",
            scope=["Dashboard", "Settings"],
        ),
        str(output_path),
    )

    content = output_path.read_text()
    assert "Screens/Views to Prototype" in content
    assert "User lands on main screen" in content
    assert "Commands to Prototype" not in content
    assert "API Surfaces to Prototype" not in content


def _execution_plan():
    return {
        "id": "plan-test",
        "implementation_brief_id": "brief-test",
        "target_engine": "smoothie",
        "milestones": [{"name": "Foundation", "description": "Set up the project"}],
        "tasks": [],
        "status": "draft",
        "test_strategy": "Run exporter tests",
        "handoff_prompt": "Create a Smoothie brief",
    }


def _implementation_brief(product_surface: str, scope: list[str]):
    return {
        "id": "brief-test",
        "source_brief_id": "source-test",
        "title": "Test Brief",
        "domain": "testing",
        "target_user": "Developers",
        "buyer": "Engineering",
        "workflow_context": "Workflow context",
        "problem_statement": "Need clearer exports",
        "mvp_goal": "Render a product brief",
        "product_surface": product_surface,
        "scope": scope,
        "non_goals": ["Conditionals"],
        "assumptions": ["Simple placeholders are enough"],
        "architecture_notes": "Use the Markdown template renderer",
        "data_requirements": "Briefs and plans",
        "integration_points": [],
        "risks": ["Missing placeholders"],
        "validation_plan": "Run exporter tests",
        "definition_of_done": ["Templates render"],
        "status": "planned",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
