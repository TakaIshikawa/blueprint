import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_accessibility_requirements import (
    SourceAccessibilityRequirement,
    SourceAccessibilityRequirementInventory,
    build_source_accessibility_requirements,
    extract_source_accessibility_requirements,
    source_accessibility_requirements_to_dict,
)


def test_source_brief_mapping_extracts_accessibility_from_text_and_structured_payload():
    source = _source_brief(
        summary=(
            "The dashboard must support keyboard navigation, screen reader announcements, "
            "high contrast text, and reduced motion."
        ),
        source_payload={
            "accessibility": {
                "captions": True,
                "focus_management": "Modal restores focus to the launching button.",
                "wcag": "Target WCAG 2.2 AA conformance.",
                "aria_labels": "Icon-only buttons need accessible names.",
            },
            "constraints": ["Do not ship video without captions or transcripts."],
        },
    )

    result = build_source_accessibility_requirements(source)
    by_signal = {requirement.signal: requirement for requirement in result.requirements}

    assert isinstance(result, SourceAccessibilityRequirementInventory)
    assert {
        "keyboard",
        "screen_reader",
        "contrast",
        "reduced_motion",
        "captions",
        "focus_management",
        "wcag_conformance",
        "aria_labels",
    } <= set(by_signal)
    assert by_signal["captions"].confidence >= 0.84
    assert any("source_payload.accessibility.focus_management" in item for item in by_signal["focus_management"].evidence)
    assert "Keyboard users" in by_signal["keyboard"].suggested_acceptance_criterion
    assert result.source_id == "source-accessibility"


def test_implementation_brief_mapping_extracts_definition_of_done_and_validation_plan():
    result = build_source_accessibility_requirements(
        _implementation_brief(
            workflow_context="Assistive technology users need screen reader friendly status messages.",
            validation_plan="Verify tab order, visible focus indicator, and color contrast.",
            definition_of_done=[
                "WCAG AA acceptance testing is documented.",
                "Reduced motion disables non-essential animations.",
                "Closed captions are available for tutorial videos.",
            ],
        )
    )

    by_signal = {requirement.signal: requirement for requirement in result.requirements}

    assert [requirement.signal for requirement in result.requirements] == [
        "keyboard",
        "screen_reader",
        "contrast",
        "reduced_motion",
        "captions",
        "focus_management",
        "wcag_conformance",
    ]
    assert by_signal["reduced_motion"].confidence == 0.9
    assert "definition_of_done" in by_signal["wcag_conformance"].evidence[0]


def test_model_inputs_match_mapping_inputs_without_mutation():
    source = _source_brief(
        summary="Keyboard and screen reader support are required.",
        source_payload={"accessibility_requirements": ["Focus order follows the visual layout."]},
    )
    original = copy.deepcopy(source)
    source_model = SourceBrief.model_validate(source)

    implementation = _implementation_brief(
        validation_plan="Check contrast ratio and prefers-reduced-motion.",
        definition_of_done=["ARIA labels are present on icon buttons."],
    )
    implementation_model = ImplementationBrief.model_validate(implementation)

    assert (
        build_source_accessibility_requirements(source).to_dict()
        == build_source_accessibility_requirements(source_model).to_dict()
    )
    assert (
        build_source_accessibility_requirements(implementation).to_dict()
        == build_source_accessibility_requirements(implementation_model).to_dict()
    )
    assert source == original


def test_duplicate_signals_merge_without_losing_evidence():
    result = extract_source_accessibility_requirements(
        _source_brief(
            summary="Keyboard users can complete checkout.",
            source_payload={
                "acceptance_criteria": [
                    "Keyboard navigation covers every checkout control.",
                    "Tab order follows the form layout.",
                ]
            },
        )
    )

    keyboard = [requirement for requirement in result.requirements if requirement.signal == "keyboard"]

    assert len(keyboard) == 1
    assert len(keyboard[0].evidence) == 3
    assert keyboard[0].confidence == 0.92
    assert result.summary["signal_counts"]["keyboard"] == 1


def test_no_accessibility_signals_returns_empty_inventory_with_summary_metadata():
    result = build_source_accessibility_requirements(
        _source_brief(
            summary="Add account settings filters and reporting columns.",
            source_payload={"notes": ["Prioritize CSV labels and account metadata."]},
        )
    )

    assert result.requirements == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "requirement_count": 0,
        "signals": [],
        "signal_counts": {
            "keyboard": 0,
            "screen_reader": 0,
            "contrast": 0,
            "reduced_motion": 0,
            "captions": 0,
            "focus_management": 0,
            "wcag_conformance": 0,
            "aria_labels": 0,
        },
        "evidence_count": 0,
    }


def test_stable_serialization_shape_and_json_round_trip():
    result = build_source_accessibility_requirements(
        _implementation_brief(
            validation_plan="Verify keyboard navigation and focus management before release.",
            definition_of_done=["Screen reader announcements are covered."],
        )
    )
    payload = source_accessibility_requirements_to_dict(result)

    assert isinstance(result.requirements[0], SourceAccessibilityRequirement)
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "requirements", "summary"]
    assert list(payload["requirements"][0]) == [
        "signal",
        "confidence",
        "evidence",
        "suggested_acceptance_criterion",
    ]
    assert [item["signal"] for item in payload["requirements"]] == ["keyboard", "screen_reader", "focus_management"]
    assert result.records == result.requirements
    assert result.to_dicts() == payload["requirements"]


def _source_brief(*, summary, source_payload=None):
    return {
        "id": "source-accessibility",
        "title": "Accessibility brief",
        "domain": "product",
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "ticket",
        "source_id": "ticket-accessibility",
        "source_payload": source_payload or {},
        "source_links": {},
    }


def _implementation_brief(
    *,
    workflow_context="Users manage account settings.",
    validation_plan="Manual QA verifies critical accessibility paths.",
    definition_of_done=None,
):
    return {
        "id": "implementation-accessibility",
        "source_brief_id": "source-accessibility",
        "title": "Implement accessibility requirements",
        "domain": "product",
        "target_user": "Users",
        "buyer": None,
        "workflow_context": workflow_context,
        "problem_statement": "Users need accessible account settings.",
        "mvp_goal": "Ship account settings.",
        "product_surface": "web app",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": validation_plan,
        "definition_of_done": definition_of_done or [],
        "status": "draft",
    }
