"""Tests for the Word DOCX exporter with editable tables."""

import io
import os
import zipfile

from blueprint.exporters.docx_exporter import (
    DOCXExporter,
    STYLE_CONFIGS,
    _count_statuses,
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
# DOCX generation tests
# ---------------------------------------------------------------------------


def test_export_plan_generates_valid_docx():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    assert isinstance(docx, bytes)
    # DOCX files are ZIP archives
    assert zipfile.is_zipfile(io.BytesIO(docx))


def test_export_plan_contains_required_parts():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        names = zf.namelist()
        assert "[Content_Types].xml" in names
        assert "_rels/.rels" in names
        assert "word/document.xml" in names
        assert "word/styles.xml" in names
        assert "word/numbering.xml" in names
        assert "word/_rels/document.xml.rels" in names
        assert "docProps/core.xml" in names


def test_export_plan_document_xml_valid():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert '<?xml version="1.0"' in doc
        assert "<w:document" in doc
        assert "<w:body>" in doc
        assert "</w:document>" in doc


# ---------------------------------------------------------------------------
# Content tests
# ---------------------------------------------------------------------------


def test_export_plan_contains_cover_page():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert "Execution Plan" in doc
        assert "plan-001" in doc


def test_export_plan_contains_executive_summary():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert "Executive Summary" in doc
        assert "Progress:" in doc


def test_export_plan_contains_task_data():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert "Implement auth" in doc
        assert "Add dashboard" in doc
        assert "Deploy to staging" in doc
        assert "task-1" in doc
        assert "task-2" in doc
        assert "task-3" in doc


def test_export_plan_contains_task_table():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert "<w:tbl>" in doc
        assert "Task Breakdown" in doc
        # Table header columns
        assert "Status" in doc
        assert "Milestone" in doc
        assert "Est. Hours" in doc


def test_export_plan_contains_acceptance_criteria():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert "Users can log in" in doc
        assert "OAuth tokens refresh" in doc


def test_export_plan_contains_dependency_matrix():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert "Dependency Matrix" in doc


def test_export_plan_contains_risk_register():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert "Risk Register" in doc
        assert "OAuth provider downtime" in doc


def test_export_plan_contains_resource_allocation():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert "Resource Allocation" in doc
        assert "developer" in doc
        assert "devops" in doc


def test_export_plan_contains_timeline():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert "Timeline" in doc
        assert "Sprint 1" in doc
        assert "Sprint 2" in doc


def test_export_plan_contains_appendix():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert "Appendix: Detailed Task Descriptions" in doc
        assert "Build OAuth2 login flow" in doc


# ---------------------------------------------------------------------------
# Word styles tests
# ---------------------------------------------------------------------------


def test_export_plan_uses_heading_styles():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert 'w:val="Heading1"' in doc
        assert 'w:val="Heading2"' in doc


def test_styles_xml_contains_heading_definitions():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        styles = zf.read("word/styles.xml").decode("utf-8")
        assert 'w:styleId="Heading1"' in styles
        assert 'w:styleId="Heading2"' in styles
        assert 'w:styleId="Heading3"' in styles
        assert 'w:styleId="Normal"' in styles


def test_styles_xml_contains_table_grid():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        styles = zf.read("word/styles.xml").decode("utf-8")
        assert 'w:styleId="TableGrid"' in styles


def test_table_has_formatting():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        # Alternating row colors (shading)
        assert "w:shd" in doc
        # Table borders
        assert "w:tblBorders" in doc


def test_status_colors_in_table():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        # Status colors should be present
        assert "008000" in doc  # completed green
        assert "0066CC" in doc or "CC9900" in doc  # in_progress blue or pending yellow


# ---------------------------------------------------------------------------
# Style configuration tests
# ---------------------------------------------------------------------------


def test_default_style():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan(), style="default")
    assert isinstance(docx, bytes)
    assert zipfile.is_zipfile(io.BytesIO(docx))


def test_corporate_style():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan(), style="corporate")
    assert isinstance(docx, bytes)

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        styles = zf.read("word/styles.xml").decode("utf-8")
        assert "1B365D" in styles  # corporate primary color


def test_modern_style():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan(), style="modern")
    assert isinstance(docx, bytes)

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        styles = zf.read("word/styles.xml").decode("utf-8")
        assert "2D3748" in styles  # modern primary color


def test_unknown_style_falls_back():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan(), style="nonexistent")
    assert isinstance(docx, bytes)
    assert zipfile.is_zipfile(io.BytesIO(docx))


def test_style_configs_have_required_keys():
    required_keys = {"primary", "header_bg", "header_text", "stripe_bg", "border"}
    for name, config in STYLE_CONFIGS.items():
        missing = required_keys - set(config.keys())
        assert not missing, f"Style '{name}' missing keys: {missing}"


def test_apply_styles():
    exporter = DOCXExporter()
    config = exporter.apply_styles("corporate")
    assert config["primary"] == "1B365D"


def test_apply_styles_unknown():
    exporter = DOCXExporter()
    config = exporter.apply_styles("unknown")
    assert config == STYLE_CONFIGS["default"]


# ---------------------------------------------------------------------------
# Editable task table tests
# ---------------------------------------------------------------------------


def test_add_task_table():
    exporter = DOCXExporter()
    tasks = _sample_plan()["tasks"]
    xml = exporter.add_task_table(tasks)

    assert "<w:tbl>" in xml
    assert "Implement auth" in xml
    assert "Add dashboard" in xml
    assert "Deploy to staging" in xml


def test_task_table_has_sortable_columns():
    exporter = DOCXExporter()
    tasks = _sample_plan()["tasks"]
    xml = exporter.add_task_table(tasks)

    # All column headers present
    for header in ["ID", "Title", "Status", "Milestone", "Owner", "Complexity", "Est. Hours"]:
        assert header in xml


# ---------------------------------------------------------------------------
# Template support
# ---------------------------------------------------------------------------


def test_export_with_template():
    exporter = DOCXExporter()
    template_bytes = b"fake template data"
    docx = exporter.export_with_template(_sample_plan(), template_bytes, brief=_sample_brief())

    assert isinstance(docx, bytes)
    assert zipfile.is_zipfile(io.BytesIO(docx))


# ---------------------------------------------------------------------------
# Metadata tests
# ---------------------------------------------------------------------------


def test_core_properties():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan(), brief=_sample_brief())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        core = zf.read("docProps/core.xml").decode("utf-8")
        assert "plan-001" in core  # title
        assert "Widget Authentication System" in core  # author
        assert "dcterms:created" in core
        assert "dcterms:modified" in core


# ---------------------------------------------------------------------------
# File export test
# ---------------------------------------------------------------------------


def test_export_to_file(tmp_path):
    exporter = DOCXExporter()
    output_path = str(tmp_path / "plan.docx")

    result = exporter.export(_sample_plan(), _sample_brief(), output_path)

    assert result == output_path
    assert os.path.exists(output_path)

    with open(output_path, "rb") as f:
        content = f.read()

    assert zipfile.is_zipfile(io.BytesIO(content))


def test_export_creates_directory(tmp_path):
    exporter = DOCXExporter()
    output_path = str(tmp_path / "subdir" / "deep" / "plan.docx")

    result = exporter.export(_sample_plan(), _sample_brief(), output_path)

    assert result == output_path
    assert os.path.exists(output_path)


# ---------------------------------------------------------------------------
# Format and extension tests
# ---------------------------------------------------------------------------


def test_export_format_and_extension():
    exporter = DOCXExporter()
    assert exporter.get_format() == "docx"
    assert exporter.get_extension() == ".docx"


# ---------------------------------------------------------------------------
# Empty plan handling
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

    docx = exporter.export_plan(plan)
    assert isinstance(docx, bytes)
    assert zipfile.is_zipfile(io.BytesIO(docx))

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert "plan-empty" in doc
        assert "0%" in doc


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


# ---------------------------------------------------------------------------
# Security / XSS prevention
# ---------------------------------------------------------------------------


def test_xml_escaping():
    exporter = DOCXExporter()
    plan = _sample_plan()
    plan["tasks"][0]["title"] = '<script>alert("xss")</script>'

    docx = exporter.export_plan(plan)

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert '<script>alert("xss")</script>' not in doc
        assert "&lt;script&gt;" in doc


# ---------------------------------------------------------------------------
# Dependency table tests
# ---------------------------------------------------------------------------


def test_dependency_table_with_deps():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        # task-2 depends on task-1
        assert "Depends On" in doc or "Source Task" in doc


def test_dependency_table_no_deps():
    plan = _sample_plan()
    for task in plan["tasks"]:
        task["depends_on"] = []

    exporter = DOCXExporter()
    docx = exporter.export_plan(plan)

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert "No dependencies defined" in doc


# ---------------------------------------------------------------------------
# Page layout tests
# ---------------------------------------------------------------------------


def test_page_layout():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        # US Letter page size (12240 x 15840 twips)
        assert "w:w=\"12240\"" in doc
        assert "w:h=\"15840\"" in doc
        # 1-inch margins (1440 twips)
        assert "w:top=\"1440\"" in doc


def test_page_breaks():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        doc = zf.read("word/document.xml").decode("utf-8")
        assert 'w:type="page"' in doc


# ---------------------------------------------------------------------------
# Numbering (list) support
# ---------------------------------------------------------------------------


def test_numbering_xml():
    exporter = DOCXExporter()
    docx = exporter.export_plan(_sample_plan())

    with zipfile.ZipFile(io.BytesIO(docx)) as zf:
        numbering = zf.read("word/numbering.xml").decode("utf-8")
        assert "w:abstractNum" in numbering
        assert "w:num" in numbering
