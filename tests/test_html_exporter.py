"""Tests for the interactive HTML exporter."""

import os
import tempfile

from blueprint.exporters.html_exporter import (
    HTMLExporter,
    THEMES,
    STATUS_LABELS,
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
# Tests
# ---------------------------------------------------------------------------


def test_export_plan_generates_valid_html():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan(), _sample_brief())

    assert "<!DOCTYPE html>" in html
    assert "<html" in html
    assert "</html>" in html
    assert "Execution Plan: plan-001" in html


def test_export_plan_contains_all_sections():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan(), _sample_brief())

    assert "Executive Summary" in html
    assert "Metrics Dashboard" in html
    assert "Tasks" in html
    assert "Timeline" in html
    assert "Dependencies" in html
    assert "Table of Contents" in html


def test_export_plan_contains_task_data():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan())

    assert "Implement auth" in html
    assert "Add dashboard" in html
    assert "Deploy to staging" in html
    assert "task-1" in html
    assert "task-2" in html
    assert "task-3" in html


def test_export_plan_contains_status_badges():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan())

    assert "status-completed" in html
    assert "status-in_progress" in html
    assert "status-pending" in html


def test_export_plan_contains_search_and_filter():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan())

    assert 'id="searchInput"' in html
    assert "searchTasks()" in html
    assert "filterByStatus" in html
    assert 'class="filter-btn' in html


def test_export_plan_contains_sort_functionality():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan())

    assert "sortTable(" in html
    assert "sort-icon" in html


def test_export_plan_contains_expand_collapse():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan())

    assert "toggleDetails" in html
    assert "task-details" in html
    assert "details-task-1" in html


def test_export_plan_contains_chart():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan())

    assert "statusChart" in html
    assert "chart.js" in html.lower() or "Chart" in html
    assert "doughnut" in html


def test_export_plan_contains_timeline():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan())

    assert "timeline" in html
    assert "Sprint 1" in html
    assert "Sprint 2" in html


def test_export_plan_contains_dependencies():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan())

    assert "dep-graph" in html
    # task-2 depends on task-1
    assert "task-1" in html
    assert "→" in html or "&rarr;" in html


def test_export_plan_contains_metrics():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan())

    assert "Total Tasks" in html
    assert ">3<" in html  # 3 total tasks
    assert "Est. Hours" in html


def test_export_plan_contains_acceptance_criteria():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan())

    assert "Users can log in" in html
    assert "OAuth tokens refresh" in html


def test_export_plan_dark_theme():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan(), theme="dark")

    assert "#1a1a2e" in html  # dark bg color
    assert "<!DOCTYPE html>" in html


def test_export_plan_corporate_theme():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan(), theme="corporate")

    assert "#003366" in html  # corporate primary


def test_export_with_charts():
    exporter = HTMLExporter()
    html = exporter.export_with_charts(_sample_plan())
    assert "<!DOCTYPE html>" in html
    assert "Chart" in html


def test_export_printable():
    exporter = HTMLExporter()
    html = exporter.export_printable(_sample_plan())

    assert "<!DOCTYPE html>" in html
    assert "print-mode" in html
    # Print mode should still have task table
    assert "Implement auth" in html


def test_generate_standalone():
    exporter = HTMLExporter()
    html = exporter.generate_standalone(_sample_plan())
    assert "<!DOCTYPE html>" in html


def test_apply_custom_theme():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan())

    custom_theme = {"bg": "#ff0000", "text": "#00ff00"}
    themed = exporter.apply_custom_theme(html, custom_theme)

    assert "--bg: #ff0000" in themed
    assert "--text: #00ff00" in themed


def test_export_to_file(tmp_path):
    exporter = HTMLExporter()
    output_path = str(tmp_path / "plan.html")

    result = exporter.export(_sample_plan(), _sample_brief(), output_path)

    assert result == output_path
    assert os.path.exists(output_path)

    with open(output_path, encoding="utf-8") as f:
        content = f.read()

    assert "<!DOCTYPE html>" in content
    assert "plan-001" in content


def test_export_format_and_extension():
    exporter = HTMLExporter()
    assert exporter.get_format() == "html"
    assert exporter.get_extension() == ".html"


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


def test_themes_have_required_keys():
    required_keys = {"bg", "text", "primary", "success", "warning", "danger", "header_bg"}
    for theme_name, theme in THEMES.items():
        missing = required_keys - set(theme.keys())
        assert not missing, f"Theme '{theme_name}' missing keys: {missing}"


def test_empty_plan():
    exporter = HTMLExporter()
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

    html = exporter.export_plan(plan)
    assert "<!DOCTYPE html>" in html
    assert "plan-empty" in html
    assert "0%" in html  # 0 progress


def test_html_escaping():
    exporter = HTMLExporter()
    plan = _sample_plan()
    plan["tasks"][0]["title"] = '<script>alert("xss")</script>'

    html = exporter.export_plan(plan)
    assert '<script>alert("xss")</script>' not in html
    assert "&lt;script&gt;" in html


def test_search_highlight_javascript():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan())

    assert "highlightText" in html
    assert "highlight" in html


def test_progress_calculation():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan())

    # 1 completed out of 3 = 33%
    assert "33%" in html


def test_brief_summary_in_output():
    exporter = HTMLExporter()
    html = exporter.export_plan(_sample_plan(), _sample_brief())

    assert "Widget Authentication System" in html or "secure authentication" in html
