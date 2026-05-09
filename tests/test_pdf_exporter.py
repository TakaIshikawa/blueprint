"""Tests for the PDF exporter."""

import os

from reportlab.platypus import TableStyle

from blueprint.exporters.pdf_exporter import (
    PDFExporter,
    STATUS_COLORS,
    STATUS_LABELS,
    TEMPLATES,
    _build_styles,
    _table_style,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _sample_plan() -> dict:
    return {
        "id": "plan-001",
        "implementation_brief_id": "ib-001",
        "target_engine": "relay",
        "target_repo": "acme/widgets",
        "project_type": "web",
        "milestones": [
            {"name": "Sprint 1", "description": "First sprint"},
            {"name": "Sprint 2", "description": "Second sprint"},
        ],
        "test_strategy": "pytest",
        "handoff_prompt": None,
        "status": "in_progress",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
        "metadata": {},
        "tasks": [
            {
                "id": "task-1",
                "execution_plan_id": "plan-001",
                "title": "Implement auth",
                "description": "Build OAuth2 login flow",
                "milestone": "Sprint 1",
                "owner_type": "developer",
                "suggested_engine": None,
                "depends_on": [],
                "files_or_modules": ["src/auth.py"],
                "acceptance_criteria": ["Users can log in", "OAuth tokens refresh"],
                "estimated_complexity": "high",
                "estimated_hours": 16.0,
                "risk_level": "medium",
                "test_command": "pytest tests/test_auth.py",
                "status": "completed",
                "metadata": {},
                "blocked_reason": None,
                "created_at": None,
                "updated_at": None,
            },
            {
                "id": "task-2",
                "execution_plan_id": "plan-001",
                "title": "Add dashboard",
                "description": "Create user dashboard page",
                "milestone": "Sprint 1",
                "owner_type": "developer",
                "suggested_engine": None,
                "depends_on": ["task-1"],
                "files_or_modules": ["src/dashboard.py"],
                "acceptance_criteria": ["Dashboard shows metrics"],
                "estimated_complexity": "medium",
                "estimated_hours": 8.0,
                "risk_level": "low",
                "test_command": None,
                "status": "in_progress",
                "metadata": {},
                "blocked_reason": None,
                "created_at": None,
                "updated_at": None,
            },
            {
                "id": "task-3",
                "execution_plan_id": "plan-001",
                "title": "Deploy to staging",
                "description": "Deploy application to staging environment",
                "milestone": "Sprint 2",
                "owner_type": "devops",
                "suggested_engine": None,
                "depends_on": ["task-2"],
                "files_or_modules": None,
                "acceptance_criteria": ["App is live on staging"],
                "estimated_complexity": "low",
                "estimated_hours": 2.0,
                "risk_level": "low",
                "test_command": None,
                "status": "pending",
                "metadata": {},
                "blocked_reason": None,
                "created_at": None,
                "updated_at": None,
            },
        ],
    }


def _sample_brief() -> dict:
    return {
        "id": "ib-001",
        "source_brief_id": "sb-001",
        "title": "Widget Authentication System",
        "domain": "web",
        "target_user": "developers",
        "buyer": None,
        "workflow_context": None,
        "problem_statement": "Users need secure authentication for the widget platform",
        "mvp_goal": "Working OAuth2 login",
        "product_surface": "web",
        "scope": ["auth", "dashboard"],
        "non_goals": ["mobile"],
        "assumptions": ["OAuth provider is configured"],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": ["OAuth provider downtime"],
        "validation_plan": "Integration tests",
        "definition_of_done": ["All tests pass"],
        "status": "planned",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }


# ---------------------------------------------------------------------------
# Tests: basic generation
# ---------------------------------------------------------------------------


def test_export_plan_returns_bytes():
    exporter = PDFExporter()
    result = exporter.export_plan(_sample_plan())
    assert isinstance(result, bytes)
    assert len(result) > 0


def test_export_plan_is_valid_pdf():
    exporter = PDFExporter()
    data = exporter.export_plan(_sample_plan())
    # PDF files start with %PDF
    assert data[:5] == b"%PDF-"


def test_format_and_extension():
    exporter = PDFExporter()
    assert exporter.get_format() == "pdf"
    assert exporter.get_extension() == ".pdf"


# ---------------------------------------------------------------------------
# Tests: cover page
# ---------------------------------------------------------------------------


def test_cover_page_in_pdf():
    exporter = PDFExporter()
    data = exporter.export_plan(_sample_plan(), brief=_sample_brief())
    assert isinstance(data, bytes)
    assert len(data) > 1000  # Non-trivial PDF


# ---------------------------------------------------------------------------
# Tests: executive summary
# ---------------------------------------------------------------------------


def test_executive_summary_template():
    exporter = PDFExporter()
    data = exporter.export_executive_summary(_sample_plan(), brief=_sample_brief())
    assert isinstance(data, bytes)
    assert data[:5] == b"%PDF-"


def test_executive_summary_sections():
    exporter = PDFExporter()
    data = exporter.export_plan(
        _sample_plan(),
        brief=_sample_brief(),
        sections=["cover", "summary"],
    )
    assert isinstance(data, bytes)
    assert len(data) > 0


# ---------------------------------------------------------------------------
# Tests: templates
# ---------------------------------------------------------------------------


def test_executive_template():
    exporter = PDFExporter()
    data = exporter.export_plan(_sample_plan(), template="executive")
    assert data[:5] == b"%PDF-"


def test_detailed_template():
    exporter = PDFExporter()
    data = exporter.export_plan(_sample_plan(), template="detailed")
    assert data[:5] == b"%PDF-"


def test_status_report_template():
    exporter = PDFExporter()
    data = exporter.export_plan(_sample_plan(), template="status_report")
    assert data[:5] == b"%PDF-"


def test_detailed_export():
    exporter = PDFExporter()
    data = exporter.export_detailed(_sample_plan())
    assert data[:5] == b"%PDF-"


def test_unknown_template_falls_back():
    exporter = PDFExporter()
    data = exporter.export_plan(_sample_plan(), template="nonexistent")
    assert data[:5] == b"%PDF-"


# ---------------------------------------------------------------------------
# Tests: section selection
# ---------------------------------------------------------------------------


def test_custom_sections():
    exporter = PDFExporter()
    data = exporter.export_plan(
        _sample_plan(),
        sections=["cover", "tasks", "risks"],
    )
    assert data[:5] == b"%PDF-"
    assert len(data) > 0


def test_all_sections_included():
    exporter = PDFExporter()
    data = exporter.export_plan(_sample_plan(), brief=_sample_brief())
    # Full document should be larger than a summary-only version
    summary_only = exporter.export_plan(
        _sample_plan(),
        brief=_sample_brief(),
        sections=["summary"],
    )
    assert len(data) > len(summary_only)


# ---------------------------------------------------------------------------
# Tests: tasks
# ---------------------------------------------------------------------------


def test_task_table_in_pdf():
    exporter = PDFExporter()
    data = exporter.export_plan(
        _sample_plan(),
        sections=["tasks"],
    )
    assert isinstance(data, bytes)
    assert data[:5] == b"%PDF-"


def test_empty_tasks():
    exporter = PDFExporter()
    plan = _sample_plan()
    plan["tasks"] = []
    data = exporter.export_plan(plan, sections=["tasks"])
    assert data[:5] == b"%PDF-"


# ---------------------------------------------------------------------------
# Tests: timeline
# ---------------------------------------------------------------------------


def test_timeline_section():
    exporter = PDFExporter()
    data = exporter.export_plan(
        _sample_plan(),
        sections=["timeline"],
    )
    assert data[:5] == b"%PDF-"


def test_timeline_no_milestones():
    exporter = PDFExporter()
    plan = _sample_plan()
    plan["milestones"] = []
    data = exporter.export_plan(plan, sections=["timeline"])
    assert data[:5] == b"%PDF-"


def test_timeline_empty():
    exporter = PDFExporter()
    plan = _sample_plan()
    plan["milestones"] = []
    plan["tasks"] = []
    data = exporter.export_plan(plan, sections=["timeline"])
    assert data[:5] == b"%PDF-"


# ---------------------------------------------------------------------------
# Tests: dependencies
# ---------------------------------------------------------------------------


def test_dependency_section():
    exporter = PDFExporter()
    data = exporter.export_plan(
        _sample_plan(),
        sections=["dependencies"],
    )
    assert data[:5] == b"%PDF-"


def test_no_dependencies():
    exporter = PDFExporter()
    plan = _sample_plan()
    for t in plan["tasks"]:
        t["depends_on"] = []
    data = exporter.export_plan(plan, sections=["dependencies"])
    assert data[:5] == b"%PDF-"


# ---------------------------------------------------------------------------
# Tests: risks
# ---------------------------------------------------------------------------


def test_risk_section():
    exporter = PDFExporter()
    data = exporter.export_plan(
        _sample_plan(),
        sections=["risks"],
    )
    assert data[:5] == b"%PDF-"


def test_no_risk_levels():
    exporter = PDFExporter()
    plan = _sample_plan()
    for t in plan["tasks"]:
        t["risk_level"] = None
    data = exporter.export_plan(plan, sections=["risks"])
    assert data[:5] == b"%PDF-"


# ---------------------------------------------------------------------------
# Tests: resources
# ---------------------------------------------------------------------------


def test_resource_section():
    exporter = PDFExporter()
    data = exporter.export_plan(
        _sample_plan(),
        sections=["resources"],
    )
    assert data[:5] == b"%PDF-"


# ---------------------------------------------------------------------------
# Tests: appendix
# ---------------------------------------------------------------------------


def test_appendix_section():
    exporter = PDFExporter()
    data = exporter.export_plan(
        _sample_plan(),
        sections=["appendix"],
    )
    assert data[:5] == b"%PDF-"


def test_appendix_empty_tasks():
    exporter = PDFExporter()
    plan = _sample_plan()
    plan["tasks"] = []
    data = exporter.export_plan(plan, sections=["appendix"])
    assert data[:5] == b"%PDF-"


# ---------------------------------------------------------------------------
# Tests: file export
# ---------------------------------------------------------------------------


def test_export_to_file(tmp_path):
    exporter = PDFExporter()
    output_path = str(tmp_path / "plan.pdf")
    result = exporter.export(_sample_plan(), _sample_brief(), output_path)
    assert result == output_path
    assert os.path.exists(output_path)

    with open(output_path, "rb") as f:
        content = f.read()
    assert content[:5] == b"%PDF-"


# ---------------------------------------------------------------------------
# Tests: watermark
# ---------------------------------------------------------------------------


def test_watermark():
    exporter = PDFExporter()
    data = exporter.export_plan(_sample_plan())
    watermarked = exporter.add_watermark(data, "DRAFT")
    assert isinstance(watermarked, bytes)
    assert watermarked[:5] == b"%PDF-"


# ---------------------------------------------------------------------------
# Tests: merge
# ---------------------------------------------------------------------------


def test_merge_pdfs():
    exporter = PDFExporter()
    pdf1 = exporter.export_plan(_sample_plan(), sections=["cover"])
    pdf2 = exporter.export_plan(_sample_plan(), sections=["tasks"])
    merged = exporter.merge_pdfs([pdf1, pdf2])
    assert isinstance(merged, bytes)
    assert len(merged) > 0


def test_merge_empty():
    exporter = PDFExporter()
    assert exporter.merge_pdfs([]) == b""


def test_merge_single():
    exporter = PDFExporter()
    pdf1 = exporter.export_plan(_sample_plan(), sections=["cover"])
    assert exporter.merge_pdfs([pdf1]) == pdf1


# ---------------------------------------------------------------------------
# Tests: empty plan
# ---------------------------------------------------------------------------


def test_empty_plan():
    exporter = PDFExporter()
    plan = {
        "id": "plan-empty",
        "implementation_brief_id": "ib-001",
        "target_engine": None,
        "target_repo": None,
        "project_type": None,
        "milestones": [],
        "test_strategy": None,
        "handoff_prompt": None,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
        "metadata": {},
        "tasks": [],
    }
    data = exporter.export_plan(plan)
    assert isinstance(data, bytes)
    assert data[:5] == b"%PDF-"


# ---------------------------------------------------------------------------
# Tests: helper functions
# ---------------------------------------------------------------------------


def test_build_styles_default():
    styles = _build_styles()
    assert "cover_title" in styles
    assert "heading1" in styles
    assert "body" in styles
    assert "bullet" in styles


def test_build_styles_detailed():
    styles = _build_styles("detailed")
    assert styles["cover_title"].fontSize == 24


def test_build_styles_status_report():
    styles = _build_styles("status_report")
    assert styles["cover_title"].fontSize == 22


def test_table_style():
    style = _table_style()
    assert isinstance(style, TableStyle)


# ---------------------------------------------------------------------------
# Tests: constants
# ---------------------------------------------------------------------------


def test_status_labels_complete():
    expected = {"pending", "in_progress", "completed", "blocked", "skipped"}
    assert set(STATUS_LABELS.keys()) == expected


def test_status_colors_complete():
    expected = {"pending", "in_progress", "completed", "blocked", "skipped"}
    assert set(STATUS_COLORS.keys()) == expected


def test_templates_have_required_keys():
    required = {"page_size", "margin_left", "margin_right", "margin_top",
                "margin_bottom", "title_size", "heading_size", "body_size"}
    for name, t in TEMPLATES.items():
        missing = required - set(t.keys())
        assert not missing, f"Template '{name}' missing keys: {missing}"


def test_all_templates_present():
    assert "executive" in TEMPLATES
    assert "detailed" in TEMPLATES
    assert "status_report" in TEMPLATES


# ---------------------------------------------------------------------------
# Tests: PDF metadata
# ---------------------------------------------------------------------------


def test_pdf_has_metadata():
    exporter = PDFExporter()
    data = exporter.export_plan(_sample_plan(), brief=_sample_brief())
    # ReportLab embeds title and author in PDF metadata
    # We can check the raw bytes contain our metadata strings
    assert b"Blueprint" in data
    assert b"plan-001" in data
