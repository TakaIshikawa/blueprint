"""Smoke tests for the DOCX exporter."""

import zipfile

from blueprint.exporters.docx_exporter import DOCXExporter, STYLE_CONFIGS, _count_statuses


def test_docx_exporter_generates_valid_docx_bytes(tmp_path):
    plan = _sample_plan()
    brief = _sample_brief()
    output_path = tmp_path / "plan.docx"

    result_path = DOCXExporter().export(plan, brief, str(output_path))

    assert result_path == str(output_path)
    assert output_path.read_bytes().startswith(b"PK")
    with zipfile.ZipFile(output_path) as archive:
        assert "word/document.xml" in archive.namelist()
        document_xml = archive.read("word/document.xml").decode("utf-8")
    assert "Implement auth" in document_xml
    assert "Brief title" in document_xml


def test_docx_export_plan_styles_and_status_counts_are_stable():
    plan = _sample_plan()
    exporter = DOCXExporter()

    payload = exporter.export_plan(plan, brief={"title": "Brief"}, style="corporate")

    assert payload.startswith(b"PK")
    assert exporter.apply_styles("missing") == STYLE_CONFIGS["default"]
    assert _count_statuses(plan["tasks"]) == {
        "completed": 1,
        "in_progress": 1,
        "pending": 1,
    }


def _sample_plan() -> dict:
    return {
        "id": "plan-001",
        "implementation_brief_id": "brief-1",
        "target_engine": "relay",
        "target_repo": "acme/widgets",
        "project_type": "web",
        "tasks": [
            {
                "id": "task-1",
                "title": "Implement auth",
                "description": "Build OAuth2 login flow",
                "status": "completed",
                "milestone": "Sprint 1",
                "acceptance_criteria": ["Users can log in"],
            },
            {
                "id": "task-2",
                "title": "Add dashboard",
                "description": "Create user dashboard page",
                "status": "in_progress",
                "milestone": "Sprint 1",
                "acceptance_criteria": ["Dashboard shows metrics"],
            },
            {
                "id": "task-3",
                "title": "Deploy",
                "description": "Deploy to staging",
                "status": "pending",
                "milestone": "Sprint 2",
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
        "scope": ["DOCX export"],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Run exporter tests.",
        "definition_of_done": ["Exporter works."],
    }
