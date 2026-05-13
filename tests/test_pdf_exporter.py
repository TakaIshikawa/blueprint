"""Smoke tests for the PDF exporter."""

from blueprint.exporters.pdf_exporter import (
    PDFExporter,
    TEMPLATES,
    _count_statuses,
    _pdf_escape,
    _wrap_text,
)


def test_pdf_exporter_generates_pdf_file(tmp_path):
    plan = _sample_plan()
    brief = _sample_brief()
    output_path = tmp_path / "plan.pdf"

    result_path = PDFExporter().export(plan, brief, str(output_path))

    assert result_path == str(output_path)
    assert output_path.read_bytes().startswith(b"%PDF")


def test_pdf_helpers_and_templates_are_stable():
    tasks = _sample_plan()["tasks"]

    assert "executive" in TEMPLATES
    assert _count_statuses(tasks) == {"completed": 1, "in_progress": 1, "pending": 1}
    assert _pdf_escape("a(b)\\c") == "a\\(b\\)\\\\c"
    assert _wrap_text("alpha beta gamma", 8) == ["alpha", "beta", "gamma"]


def _sample_plan() -> dict:
    return {
        "id": "plan-001",
        "implementation_brief_id": "brief-1",
        "tasks": [
            {
                "id": "task-1",
                "title": "Implement auth",
                "description": "Build OAuth2 login flow",
                "status": "completed",
                "milestone": "Sprint 1",
                "estimated_hours": 4,
                "files_or_modules": ["src/auth.py"],
                "acceptance_criteria": ["Users can log in"],
            },
            {
                "id": "task-2",
                "title": "Add dashboard",
                "description": "Create user dashboard page",
                "status": "in_progress",
                "milestone": "Sprint 1",
                "estimated_hours": 2,
                "acceptance_criteria": ["Dashboard shows metrics"],
            },
            {
                "id": "task-3",
                "title": "Deploy",
                "description": "Deploy app",
                "status": "pending",
                "acceptance_criteria": ["App is live"],
            },
        ],
        "milestones": [{"name": "Sprint 1"}, {"name": "Sprint 2"}],
    }


def _sample_brief() -> dict:
    return {
        "id": "brief-1",
        "source_brief_id": "source-1",
        "title": "Brief title",
        "problem_statement": "Export the plan.",
        "mvp_goal": "Create an export.",
        "scope": ["PDF export"],
        "non_goals": [],
        "assumptions": [],
        "risks": ["Deployment needs rollback"],
        "validation_plan": "Run exporter tests.",
        "definition_of_done": ["Exporter works."],
    }
