from blueprint.source_ui_requirements import (
    SourceUiRequirement,
    SourceUiRequirementsReport,
    extract_source_ui_requirements,
)


def test_extract_component_specifications():
    result = extract_source_ui_requirements(
        {
            "id": "brief-ui-components",
            "title": "Build dashboard UI components",
            "description": (
                "Create reusable components including buttons, input fields, forms, modals, dialogs, "
                "dropdowns, cards, tables, and navigation menus for the admin dashboard."
            ),
        }
    )

    assert isinstance(result, SourceUiRequirementsReport)
    assert result.source_brief_id == "brief-ui-components"
    assert len(result.requirements) > 0
    component_req = next((r for r in result.requirements if r.requirement_type == "component_specification"), None)
    assert component_req is not None
    assert "button" in " ".join(component_req.matched_terms).lower()
    assert "component_specification" in result.summary["type_counts"]


def test_extract_interaction_patterns():
    result = extract_source_ui_requirements(
        {
            "id": "brief-interactions",
            "description": (
                "Implement user interactions including click handlers, hover effects, drag and drop, "
                "scroll events, keyboard navigation, and touch gestures."
            ),
        }
    )

    interaction_req = next((r for r in result.requirements if r.requirement_type == "interaction_pattern"), None)
    assert interaction_req is not None
    assert any("click" in term.lower() or "hover" in term.lower() for term in interaction_req.matched_terms)


def test_extract_visual_design_requirements():
    result = extract_source_ui_requirements(
        {
            "id": "brief-design",
            "description": (
                "Apply visual design with color palette, typography, spacing, padding, margins, borders, "
                "shadows, and layout using flexbox and grid."
            ),
        }
    )

    design_req = next((r for r in result.requirements if r.requirement_type == "visual_design"), None)
    assert design_req is not None
    assert "typography" in " ".join(design_req.matched_terms).lower() or "color" in " ".join(design_req.matched_terms).lower()


def test_extract_responsive_breakpoints():
    result = extract_source_ui_requirements(
        {
            "id": "brief-responsive",
            "description": (
                "Implement responsive design with breakpoints for mobile (320px), tablet (768px), "
                "and desktop (1024px) using media queries and adaptive layouts."
            ),
        }
    )

    responsive_req = next((r for r in result.requirements if r.requirement_type == "responsive_breakpoints"), None)
    assert responsive_req is not None
    assert "responsive" in " ".join(responsive_req.matched_terms).lower() or "breakpoint" in " ".join(responsive_req.matched_terms).lower()


def test_extract_animations():
    result = extract_source_ui_requirements(
        {
            "id": "brief-animations",
            "description": (
                "Add animations and transitions including fade, slide, zoom, rotate effects "
                "with spring easing and microinteractions."
            ),
        }
    )

    animation_req = next((r for r in result.requirements if r.requirement_type == "animations"), None)
    assert animation_req is not None
    assert any("animation" in term.lower() or "transition" in term.lower() for term in animation_req.matched_terms)


def test_extract_design_system_alignment():
    result = extract_source_ui_requirements(
        {
            "id": "brief-design-system",
            "description": (
                "Use Material UI component library and design system with design tokens "
                "following brand guidelines for consistency."
            ),
        }
    )

    design_system_req = next((r for r in result.requirements if r.requirement_type == "design_system_alignment"), None)
    assert design_system_req is not None
    assert "material ui" in " ".join(design_system_req.matched_terms).lower() or "design system" in " ".join(design_system_req.matched_terms).lower()


def test_extract_component_reusability():
    result = extract_source_ui_requirements(
        {
            "id": "brief-reusability",
            "description": (
                "Build reusable shared components with composable modular design following atomic design principles."
            ),
        }
    )

    reusability_req = next((r for r in result.requirements if r.requirement_type == "component_reusability"), None)
    assert reusability_req is not None
    assert "reusabl" in " ".join(reusability_req.matched_terms).lower() or "modular" in " ".join(reusability_req.matched_terms).lower()


def test_extract_state_management():
    result = extract_source_ui_requirements(
        {
            "id": "brief-state",
            "description": (
                "Implement state management using Redux for global state and useState for local UI state and form state."
            ),
        }
    )

    state_req = next((r for r in result.requirements if r.requirement_type == "state_management"), None)
    assert state_req is not None
    assert "redux" in " ".join(state_req.matched_terms).lower() or "state" in " ".join(state_req.matched_terms).lower()


def test_extract_accessibility_compliance():
    result = extract_source_ui_requirements(
        {
            "id": "brief-a11y",
            "description": (
                "Ensure accessibility compliance with WCAG 2.1 AA, ARIA attributes, semantic HTML, "
                "keyboard navigation, screen reader support, and proper contrast ratios."
            ),
        }
    )

    accessibility_req = next((r for r in result.requirements if r.requirement_type == "accessibility_compliance"), None)
    assert accessibility_req is not None
    assert "wcag" in " ".join(accessibility_req.matched_terms).lower() or "aria" in " ".join(accessibility_req.matched_terms).lower() or "accessibility" in " ".join(accessibility_req.matched_terms).lower()


def test_extract_browser_support():
    result = extract_source_ui_requirements(
        {
            "id": "brief-browsers",
            "description": (
                "Support cross-browser compatibility for Chrome, Firefox, Safari, and Edge with polyfills and autoprefixer."
            ),
        }
    )

    browser_req = next((r for r in result.requirements if r.requirement_type == "browser_support"), None)
    assert browser_req is not None
    assert "chrome" in " ".join(browser_req.matched_terms).lower() or "browser" in " ".join(browser_req.matched_terms).lower()


def test_comprehensive_ui_requirements():
    result = extract_source_ui_requirements(
        {
            "id": "brief-comprehensive",
            "description": (
                "Build responsive dashboard with Material UI components, including buttons, forms, and tables. "
                "Implement animations, accessibility with WCAG compliance, and cross-browser support. "
                "Use Redux for state management and follow design system guidelines."
            ),
        }
    )

    assert len(result.requirements) >= 5
    assert result.summary["requirement_count"] >= 5
    assert result.summary["type_counts"]["component_specification"] >= 1
    assert result.summary["type_counts"]["accessibility_compliance"] >= 1


def test_follow_up_questions_present():
    result = extract_source_ui_requirements(
        {
            "id": "brief-questions",
            "description": "Build responsive UI components with accessibility support.",
        }
    )

    for requirement in result.requirements:
        assert len(requirement.follow_up_questions) > 0


def test_empty_source():
    result = extract_source_ui_requirements({"id": "brief-empty", "description": "Backend API work."})

    assert result.source_brief_id == "brief-empty"
    assert len(result.requirements) == 0
    assert result.summary["requirement_count"] == 0


def test_evidence_truncation():
    long_desc = (
        "Build comprehensive UI components with all features " * 30
    )
    result = extract_source_ui_requirements(
        {
            "id": "brief-long",
            "description": long_desc,
        }
    )

    for requirement in result.requirements:
        for evidence in requirement.evidence:
            assert len(evidence) <= 200


def test_to_dict_serialization():
    result = extract_source_ui_requirements(
        {
            "id": "brief-dict",
            "description": "Build accessible UI components with responsive design.",
        }
    )

    result_dict = result.to_dict()
    assert result_dict["source_brief_id"] == "brief-dict"
    assert isinstance(result_dict["requirements"], list)
    assert isinstance(result_dict["summary"], dict)
    assert "records" in result_dict

    dicts = result.to_dicts()
    assert isinstance(dicts, list)


def test_records_property():
    result = extract_source_ui_requirements(
        {
            "id": "brief-records",
            "description": "UI components with animations.",
        }
    )

    assert result.records == result.requirements
