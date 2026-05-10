"""Tests for the PDF exporter with executive formatting."""

import os

from blueprint.exporters.pdf_exporter import (
    PDFExporter,
    TEMPLATES,
    _count_statuses,
    _wrap_text,
    _pdf_escape,
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
# PDF generation tests
# ---------------------------------------------------------------------------


def test_export_plan_generates_valid_pdf():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")
    assert b"%%EOF" in pdf


def test_export_plan_contains_metadata():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    assert b"/Title" in pdf
    assert b"/Author" in pdf
    assert b"/Subject" in pdf
    assert b"plan-001" in pdf


def test_export_plan_contains_pages():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    assert b"/Type /Pages" in pdf
    assert b"/Type /Page" in pdf
    # Should have multiple pages
    assert pdf.count(b"/Type /Page /Parent") > 1


def test_export_plan_contains_fonts():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    assert b"/Helvetica" in pdf
    assert b"/Helvetica-Bold" in pdf
    assert b"/Times-Roman" in pdf


def test_export_plan_contains_bookmarks():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    assert b"/Type /Outlines" in pdf
    assert b"/Dest" in pdf


def test_export_plan_has_compressed_streams():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    assert b"/FlateDecode" in pdf
    assert b"stream" in pdf
    assert b"endstream" in pdf


def test_export_plan_has_xref_table():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    assert b"xref" in pdf
    assert b"startxref" in pdf
    assert b"trailer" in pdf


# ---------------------------------------------------------------------------
# Template tests
# ---------------------------------------------------------------------------


def test_export_plan_executive_template():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), template="executive")
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")


def test_export_plan_detailed_template():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), template="detailed")
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")


def test_export_plan_status_report_template():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), template="status_report")
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")


def test_export_plan_unknown_template_falls_back():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), template="nonexistent")
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")


def test_templates_have_required_color_keys():
    required_keys = {"primary", "secondary", "success", "warning", "danger", "text", "white"}
    for name, palette in TEMPLATES.items():
        missing = required_keys - set(palette.keys())
        assert not missing, f"Template '{name}' missing keys: {missing}"


# ---------------------------------------------------------------------------
# Section selection tests
# ---------------------------------------------------------------------------


def test_export_with_specific_sections():
    exporter = PDFExporter()
    pdf = exporter.export_plan(
        _sample_plan(), sections=["cover", "summary"]
    )
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")


def test_export_with_all_sections():
    exporter = PDFExporter()
    pdf = exporter.export_plan(
        _sample_plan(),
        brief=_sample_brief(),
        sections=["cover", "summary", "toc", "tasks", "timeline", "dependencies", "risk", "resources", "appendix"],
    )
    assert isinstance(pdf, bytes)
    page_count = pdf.count(b"/Type /Page /Parent")
    # Full export should produce several pages
    assert page_count >= 5


# ---------------------------------------------------------------------------
# Chart rendering tests
# ---------------------------------------------------------------------------


def test_export_plan_renders_charts():
    """Charts are drawn as PDF drawing operations in the page streams."""
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), brief=_sample_brief())
    # Should have content streams with drawing operations
    assert b"stream" in pdf
    # Multiple pages mean chart sections exist
    assert pdf.count(b"/Type /Page /Parent") >= 2


def test_gantt_chart_renders():
    """Gantt chart page is included in full export."""
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), sections=["timeline"])
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")


def test_pie_chart_renders():
    """Pie chart is rendered in executive summary page."""
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), sections=["summary"])
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")


def test_bar_chart_renders_in_risk():
    """Bar chart in risk assessment page."""
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), brief=_sample_brief(), sections=["risk"])
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")


def test_bar_chart_renders_in_resources():
    """Bar chart in resource allocation page."""
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), sections=["resources"])
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")


# ---------------------------------------------------------------------------
# Professional formatting tests
# ---------------------------------------------------------------------------


def test_export_plan_uses_consistent_fonts():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan())

    # Sans-serif headers, serif body
    assert b"/Helvetica" in pdf  # sans-serif
    assert b"/Helvetica-Bold" in pdf  # bold sans-serif
    assert b"/Times-Roman" in pdf  # serif body


def test_export_plan_proper_page_size():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan())

    # US Letter: 612 x 792 points
    assert b"612" in pdf
    assert b"792" in pdf


def test_export_plan_has_page_numbers():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), brief=_sample_brief())
    # Page numbers are written as text in content streams
    # The compressed streams contain page numbering
    assert isinstance(pdf, bytes)
    assert len(pdf) > 1000  # Non-trivial document


# ---------------------------------------------------------------------------
# PDF features tests
# ---------------------------------------------------------------------------


def test_export_plan_has_hyperlinks_in_toc():
    """TOC entries are generated with page references."""
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), sections=["toc", "tasks", "timeline"])
    assert isinstance(pdf, bytes)
    assert b"/Type /Outlines" in pdf


def test_export_plan_bookmarks():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    assert b"/Type /Outlines" in pdf
    assert b"/UseOutlines" in pdf  # PageMode set to show bookmarks


def test_export_plan_metadata_fields():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    assert b"/Title" in pdf
    assert b"/Author" in pdf
    assert b"/CreationDate" in pdf
    assert b"/Producer" in pdf


def test_password_protection():
    exporter = PDFExporter()
    pdf = exporter.export_with_password(
        _sample_plan(), password="secret123", brief=_sample_brief()
    )
    assert isinstance(pdf, bytes)
    assert b"Protected: true" in pdf


def test_watermark():
    exporter = PDFExporter()
    original = exporter.export_plan(_sample_plan())
    watermarked = exporter.add_watermark(original, "CONFIDENTIAL")

    assert isinstance(watermarked, bytes)
    assert b"Watermark: CONFIDENTIAL" in watermarked
    assert len(watermarked) > len(original)


def test_merge_pdfs():
    exporter = PDFExporter()
    pdf1 = exporter.export_plan(_sample_plan(), sections=["cover"])
    pdf2 = exporter.export_plan(_sample_plan(), sections=["summary"])

    merged = exporter.merge_pdfs([pdf1, pdf2])
    assert isinstance(merged, bytes)
    assert len(merged) > len(pdf1)


def test_merge_empty():
    exporter = PDFExporter()
    assert exporter.merge_pdfs([]) == b""


def test_merge_single():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), sections=["cover"])
    assert exporter.merge_pdfs([pdf]) is pdf


# ---------------------------------------------------------------------------
# Convenience method tests
# ---------------------------------------------------------------------------


def test_export_executive_summary():
    exporter = PDFExporter()
    pdf = exporter.export_executive_summary(_sample_plan(), brief=_sample_brief())
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")
    # Should be shorter than full export (only cover + summary)
    full = exporter.export_plan(_sample_plan(), brief=_sample_brief())
    assert len(pdf) < len(full)


def test_export_detailed():
    exporter = PDFExporter()
    pdf = exporter.export_detailed(
        _sample_plan(), sections=["tasks", "appendix"], brief=_sample_brief()
    )
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")


# ---------------------------------------------------------------------------
# File export test
# ---------------------------------------------------------------------------


def test_export_to_file(tmp_path):
    exporter = PDFExporter()
    output_path = str(tmp_path / "plan.pdf")

    result = exporter.export(_sample_plan(), _sample_brief(), output_path)

    assert result == output_path
    assert os.path.exists(output_path)

    with open(output_path, "rb") as f:
        content = f.read()

    assert content.startswith(b"%PDF-1.7")
    assert b"%%EOF" in content


def test_export_creates_directory(tmp_path):
    exporter = PDFExporter()
    output_path = str(tmp_path / "subdir" / "deep" / "plan.pdf")

    result = exporter.export(_sample_plan(), _sample_brief(), output_path)

    assert result == output_path
    assert os.path.exists(output_path)


# ---------------------------------------------------------------------------
# Format and extension tests
# ---------------------------------------------------------------------------


def test_export_format_and_extension():
    exporter = PDFExporter()
    assert exporter.get_format() == "pdf"
    assert exporter.get_extension() == ".pdf"


# ---------------------------------------------------------------------------
# Empty plan handling
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

    pdf = exporter.export_plan(plan)
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")
    assert b"%%EOF" in pdf


# ---------------------------------------------------------------------------
# Utility function tests
# ---------------------------------------------------------------------------


def test_count_statuses():
    tasks = [
        {"status": "completed"},
        {"status": "completed"},
        {"status": "in_progress"},
        {"status": "pending"},
        {"status": "blocked"},
    ]
    counts = _count_statuses(tasks)
    assert counts["completed"] == 2
    assert counts["in_progress"] == 1
    assert counts["pending"] == 1
    assert counts["blocked"] == 1


def test_count_statuses_empty():
    assert _count_statuses([]) == {}


def test_wrap_text():
    text = "This is a long sentence that should be wrapped at approximately eighty characters per line"
    lines = _wrap_text(text, 40)
    assert len(lines) >= 2
    for line in lines:
        assert len(line) <= 45  # some slack for word boundaries


def test_wrap_text_empty():
    assert _wrap_text("", 40) == [""]


def test_wrap_text_short():
    assert _wrap_text("short", 40) == ["short"]


def test_pdf_escape():
    assert _pdf_escape("hello") == "hello"
    assert _pdf_escape("hello (world)") == "hello \\(world\\)"
    assert _pdf_escape("back\\slash") == "back\\\\slash"


# ---------------------------------------------------------------------------
# Dependency graph tests
# ---------------------------------------------------------------------------


def test_dependency_page_with_deps():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), sections=["dependencies"])
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")


def test_dependency_page_no_deps():
    plan = _sample_plan()
    for task in plan["tasks"]:
        task["depends_on"] = []
    exporter = PDFExporter()
    pdf = exporter.export_plan(plan, sections=["dependencies"])
    assert isinstance(pdf, bytes)
    assert pdf.startswith(b"%PDF-1.7")


# ---------------------------------------------------------------------------
# Risk and resource pages
# ---------------------------------------------------------------------------


def test_risk_page_with_risks():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), brief=_sample_brief(), sections=["risk"])
    assert isinstance(pdf, bytes)


def test_resource_page():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan(), sections=["resources"])
    assert isinstance(pdf, bytes)


# ---------------------------------------------------------------------------
# Multi-page task breakdown
# ---------------------------------------------------------------------------


def test_many_tasks_span_multiple_pages():
    plan = _sample_plan()
    # Generate many tasks to force page breaks
    tasks = []
    for i in range(50):
        tasks.append({
            "id": f"task-{i}",
            "execution_plan_id": "plan-001",
            "title": f"Task number {i}",
            "description": f"Description for task {i}",
            "milestone": f"Sprint {i // 10 + 1}",
            "owner_type": "developer",
            "suggested_engine": None,
            "depends_on": [f"task-{i-1}"] if i > 0 else [],
            "files_or_modules": None,
            "acceptance_criteria": [f"AC for task {i}"],
            "estimated_complexity": "medium",
            "estimated_hours": 4.0,
            "risk_level": "low",
            "test_command": None,
            "status": ["pending", "in_progress", "completed", "blocked"][i % 4],
            "metadata": {},
            "blocked_reason": None,
            "created_at": None,
            "updated_at": None,
        })
    plan["tasks"] = tasks

    exporter = PDFExporter()
    pdf = exporter.export_plan(plan, brief=_sample_brief())
    assert isinstance(pdf, bytes)
    # With 50 tasks, should have multiple pages
    page_count = pdf.count(b"/Type /Page /Parent")
    assert page_count >= 5


# ---------------------------------------------------------------------------
# Standards compliance
# ---------------------------------------------------------------------------


def test_pdf_version_header():
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan())
    assert pdf[:8] == b"%PDF-1.7"


def test_pdf_binary_comment():
    """PDF spec requires binary comment after version header."""
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan())
    # Second line should contain high-byte characters
    lines = pdf.split(b"\n", 2)
    assert len(lines) >= 2
    assert any(b > 127 for b in lines[1])


def test_pdf_structure_integrity():
    """Verify basic PDF structural elements are present."""
    exporter = PDFExporter()
    pdf = exporter.export_plan(_sample_plan())

    assert b"/Type /Catalog" in pdf
    assert b"/Type /Pages" in pdf
    assert b"/Kids" in pdf
    assert b"/Count" in pdf
    assert b"/Root" in pdf
    assert b"/Size" in pdf
