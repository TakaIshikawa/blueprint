"""Tests for the Word DOCX exporter."""

import io
import os

from docx import Document

from blueprint.exporters.docx_exporter import (
    DOCXExporter,
    STATUS_COLORS,
    STATUS_LABELS,
    DEFAULT_STYLE_CONFIG,
    _set_cell_shading,
    _format_header_row,
    _add_alternating_rows,
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


def _load_docx(data: bytes) -> Document:
    """Load a Document from raw bytes."""
    return Document(io.BytesIO(data))


def _all_text(doc: Document) -> str:
    """Extract all text from a Document."""
    parts = []
    for p in doc.paragraphs:
        parts.append(p.text)
    for table in doc.tables:
        for row in table.rows:
            for cell in row.cells:
                parts.append(cell.text)
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Tests: basic generation
# ---------------------------------------------------------------------------


def test_export_plan_returns_bytes():
    exporter = DOCXExporter()
    result = exporter.export_plan(_sample_plan())
    assert isinstance(result, bytes)
    assert len(result) > 0


def test_export_plan_is_valid_docx():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    doc = _load_docx(data)
    assert len(doc.paragraphs) > 0


def test_format_and_extension():
    exporter = DOCXExporter()
    assert exporter.get_format() == "docx"
    assert exporter.get_extension() == ".docx"


# ---------------------------------------------------------------------------
# Tests: cover page
# ---------------------------------------------------------------------------


def test_cover_page_contains_plan_id():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "plan-001" in text


def test_cover_page_contains_brief_title():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan(), brief=_sample_brief())
    text = _all_text(_load_docx(data))
    assert "Widget Authentication System" in text


# ---------------------------------------------------------------------------
# Tests: executive summary
# ---------------------------------------------------------------------------


def test_executive_summary_present():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan(), brief=_sample_brief())
    text = _all_text(_load_docx(data))
    assert "Executive Summary" in text


def test_executive_summary_progress():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    # 1 completed out of 3 = 33%
    assert "33%" in text


def test_executive_summary_problem_statement():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan(), brief=_sample_brief())
    text = _all_text(_load_docx(data))
    assert "secure authentication" in text


def test_executive_summary_risks():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan(), brief=_sample_brief())
    text = _all_text(_load_docx(data))
    assert "OAuth provider downtime" in text


def test_executive_summary_status_overview():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Completed" in text
    assert "In Progress" in text
    assert "Pending" in text


# ---------------------------------------------------------------------------
# Tests: task table
# ---------------------------------------------------------------------------


def test_task_table_contains_all_tasks():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Implement auth" in text
    assert "Add dashboard" in text
    assert "Deploy to staging" in text


def test_task_table_has_headers():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Task Breakdown" in text
    for header in ["ID", "Title", "Status", "Assignee"]:
        assert header in text


def test_task_table_uses_table_grid_style():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    doc = _load_docx(data)
    table_styles = [t.style.name for t in doc.tables if t.style]
    assert "Table Grid" in table_styles


def test_task_table_status_labels():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Completed" in text
    assert "In Progress" in text
    assert "Pending" in text


# ---------------------------------------------------------------------------
# Tests: timeline
# ---------------------------------------------------------------------------


def test_timeline_section_present():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Timeline" in text


def test_timeline_contains_milestones():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Sprint 1" in text
    assert "Sprint 2" in text


def test_timeline_contains_task_schedule():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Task Schedule" in text


# ---------------------------------------------------------------------------
# Tests: dependency matrix
# ---------------------------------------------------------------------------


def test_dependency_matrix_present():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Dependency Matrix" in text


def test_dependency_matrix_shows_deps():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    # task-2 depends on task-1
    assert "task-1" in text
    assert "task-2" in text


# ---------------------------------------------------------------------------
# Tests: risk register
# ---------------------------------------------------------------------------


def test_risk_register_present():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Risk Register" in text


def test_risk_register_shows_risk_levels():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Medium" in text
    assert "Low" in text


# ---------------------------------------------------------------------------
# Tests: resource allocation
# ---------------------------------------------------------------------------


def test_resource_allocation_present():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Resource Allocation" in text


def test_resource_allocation_owners():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "developer" in text
    assert "devops" in text


# ---------------------------------------------------------------------------
# Tests: appendix
# ---------------------------------------------------------------------------


def test_appendix_present():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Appendix" in text


def test_appendix_descriptions():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Build OAuth2 login flow" in text
    assert "Create user dashboard page" in text


def test_appendix_acceptance_criteria():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Users can log in" in text
    assert "OAuth tokens refresh" in text


def test_appendix_files():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "src/auth.py" in text


def test_appendix_test_command():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "pytest tests/test_auth.py" in text


# ---------------------------------------------------------------------------
# Tests: Word styles
# ---------------------------------------------------------------------------


def test_heading_styles_applied():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    doc = _load_docx(data)
    heading_styles = set()
    for p in doc.paragraphs:
        if p.style and p.style.name and p.style.name.startswith("Heading"):
            heading_styles.add(p.style.name)
    assert "Heading 1" in heading_styles
    assert "Heading 2" in heading_styles


def test_normal_style_font():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    doc = _load_docx(data)
    normal = doc.styles["Normal"]
    assert normal.font.name == "Calibri"


def test_custom_style_config():
    from docx.shared import RGBColor as RC

    exporter = DOCXExporter()
    custom_cfg = {
        "font_name": "Arial",
        "heading_color": RC(0xFF, 0x00, 0x00),
        "header_bg": RC(0x00, 0x00, 0x00),
        "header_text": RC(0xFF, 0xFF, 0xFF),
        "stripe_bg": RC(0xEE, 0xEE, 0xEE),
    }
    data = exporter.export_plan(_sample_plan(), style_config=custom_cfg)
    doc = _load_docx(data)
    assert doc.styles["Normal"].font.name == "Arial"


# ---------------------------------------------------------------------------
# Tests: document properties
# ---------------------------------------------------------------------------


def test_document_properties():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan(), brief=_sample_brief())
    doc = _load_docx(data)
    props = doc.core_properties
    assert "plan-001" in props.title
    assert props.author == "Blueprint"
    assert "secure authentication" in props.subject


# ---------------------------------------------------------------------------
# Tests: table of contents
# ---------------------------------------------------------------------------


def test_toc_field_present():
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    text = _all_text(_load_docx(data))
    assert "Table of Contents" in text


# ---------------------------------------------------------------------------
# Tests: file export
# ---------------------------------------------------------------------------


def test_export_to_file(tmp_path):
    exporter = DOCXExporter()
    output_path = str(tmp_path / "plan.docx")
    result = exporter.export(_sample_plan(), _sample_brief(), output_path)
    assert result == output_path
    assert os.path.exists(output_path)

    doc = Document(output_path)
    text = _all_text(doc)
    assert "plan-001" in text


# ---------------------------------------------------------------------------
# Tests: empty plan
# ---------------------------------------------------------------------------


def test_empty_plan():
    exporter = DOCXExporter()
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
    text = _all_text(_load_docx(data))
    assert "plan-empty" in text
    assert "No tasks defined" in text


# ---------------------------------------------------------------------------
# Tests: template support
# ---------------------------------------------------------------------------


def test_export_with_template_from_bytes():
    exporter = DOCXExporter()
    # Create a minimal template document
    template_doc = Document()
    template_doc.add_heading("Template Header", level=1)
    buf = io.BytesIO()
    template_doc.save(buf)
    template_bytes = buf.getvalue()

    result = exporter.export_with_template(_sample_plan(), template_bytes)
    assert isinstance(result, bytes)
    doc = _load_docx(result)
    text = _all_text(doc)
    assert "Template Header" in text
    assert "Task Breakdown" in text


def test_export_with_template_from_path(tmp_path):
    exporter = DOCXExporter()
    template_path = tmp_path / "template.docx"
    template_doc = Document()
    template_doc.add_heading("My Template", level=1)
    template_doc.save(str(template_path))

    result = exporter.export_with_template(_sample_plan(), str(template_path))
    assert isinstance(result, bytes)
    doc = _load_docx(result)
    text = _all_text(doc)
    assert "My Template" in text


# ---------------------------------------------------------------------------
# Tests: helper functions
# ---------------------------------------------------------------------------


def test_set_cell_shading():
    doc = Document()
    table = doc.add_table(rows=1, cols=1)
    cell = table.rows[0].cells[0]
    from docx.shared import RGBColor as RC
    _set_cell_shading(cell, RC(0xFF, 0x00, 0x00))
    # Verify shading element was added
    tc_pr = cell._tc.tcPr
    assert tc_pr is not None


def test_format_header_row():
    doc = Document()
    table = doc.add_table(rows=1, cols=2)
    table.rows[0].cells[0].text = "Col A"
    table.rows[0].cells[1].text = "Col B"
    _format_header_row(table.rows[0])
    # Verify formatting was applied (cells have tcPr with shading)
    for cell in table.rows[0].cells:
        assert cell._tc.tcPr is not None


def test_add_alternating_rows():
    doc = Document()
    table = doc.add_table(rows=4, cols=2)
    for i in range(4):
        table.rows[i].cells[0].text = f"Row {i}"
    _add_alternating_rows(table)
    # Row 0 (header) should not be striped, row 2 (even) should
    assert table.rows[2].cells[0]._tc.tcPr is not None


# ---------------------------------------------------------------------------
# Tests: accessibility
# ---------------------------------------------------------------------------


def test_heading_hierarchy():
    """Verify proper heading hierarchy: H1 before H2 before H3."""
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan(), brief=_sample_brief())
    doc = _load_docx(data)

    heading_levels = []
    for p in doc.paragraphs:
        if p.style and p.style.name and p.style.name.startswith("Heading"):
            level = int(p.style.name.split()[-1])
            heading_levels.append(level)

    assert len(heading_levels) > 0
    # First heading should be level 1
    assert heading_levels[0] == 1
    # No heading should skip more than one level
    for i in range(1, len(heading_levels)):
        assert heading_levels[i] <= heading_levels[i - 1] + 1 or heading_levels[i] == 1


def test_table_headers_marked():
    """Verify that tables have header rows with bold text."""
    exporter = DOCXExporter()
    data = exporter.export_plan(_sample_plan())
    doc = _load_docx(data)

    for table in doc.tables:
        if len(table.rows) > 1:
            header_row = table.rows[0]
            has_bold = False
            for cell in header_row.cells:
                for p in cell.paragraphs:
                    for run in p.runs:
                        if run.bold:
                            has_bold = True
                            break
            assert has_bold, "Table header row should have bold text"


# ---------------------------------------------------------------------------
# Tests: constants
# ---------------------------------------------------------------------------


def test_status_labels_complete():
    expected = {"pending", "in_progress", "completed", "blocked", "skipped"}
    assert set(STATUS_LABELS.keys()) == expected


def test_status_colors_complete():
    expected = {"pending", "in_progress", "completed", "blocked", "skipped"}
    assert set(STATUS_COLORS.keys()) == expected


def test_default_style_config_keys():
    required = {"font_name", "heading_color", "header_bg", "header_text", "stripe_bg"}
    assert required.issubset(set(DEFAULT_STYLE_CONFIG.keys()))
