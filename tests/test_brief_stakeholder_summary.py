import json

from blueprint.brief_stakeholder_summary import (
    StakeholderSummary,
    build_stakeholder_summary,
    stakeholder_summary_to_dict,
)
from blueprint.domain.models import ImplementationBrief


def test_summary_represents_required_brief_fields_in_dict_and_markdown():
    summary = build_stakeholder_summary(_brief())

    assert isinstance(summary, StakeholderSummary)
    assert summary.to_dict() == {
        "brief_id": "ib-summary",
        "title": "Stakeholder Summary",
        "target_audience": "Support managers",
        "buyer": "Customer operations leadership",
        "workflow_context": "Teams review escalations during weekly planning.",
        "problem_statement": "Escalation work is hard to prioritize before planning.",
        "mvp_goal": "Provide a concise pre-planning packet.",
        "scope_highlights": [
            "Summarize impact by stakeholder group",
            "Keep source language visible",
        ],
        "non_goals": ["Replace execution planning"],
        "risks": ["Stakeholders may miss edge-case validation needs"],
        "validation_plan": "Review the packet with product, design, and engineering.",
        "definition_of_done": [
            "Stakeholders can approve the brief",
            "Planning can begin from the summary",
        ],
    }

    markdown = summary.to_markdown()

    assert markdown == """# Stakeholder Summary

## Target Audience
Support managers

## Buyer
Customer operations leadership

## Workflow Context
Teams review escalations during weekly planning.

## Problem Statement
Escalation work is hard to prioritize before planning.

## MVP Goal
Provide a concise pre-planning packet.

## Scope Highlights
- Summarize impact by stakeholder group
- Keep source language visible

## Non-Goals
- Replace execution planning

## Risks
- Stakeholders may miss edge-case validation needs

## Validation Plan
Review the packet with product, design, and engineering.

## Definition of Done
- Stakeholders can approve the brief
- Planning can begin from the summary
"""


def test_optional_empty_fields_are_omitted_from_markdown_without_placeholders():
    brief = _brief()
    brief.update(
        {
            "target_user": " ",
            "buyer": None,
            "workflow_context": "",
            "scope": [],
            "non_goals": [],
            "risks": [],
        }
    )

    markdown = build_stakeholder_summary(brief).to_markdown()

    assert "## Target Audience" not in markdown
    assert "## Buyer" not in markdown
    assert "## Workflow Context" not in markdown
    assert "## Scope Highlights" not in markdown
    assert "## Non-Goals" not in markdown
    assert "## Risks" not in markdown
    assert "\n\n\n" not in markdown
    assert "## Problem Statement" in markdown
    assert "## MVP Goal" in markdown
    assert "## Validation Plan" in markdown
    assert "## Definition of Done" in markdown


def test_list_fields_render_in_stable_input_order():
    summary = build_stakeholder_summary(
        {
            **_brief(),
            "scope": ["First scope item", "Second scope item", "First scope item"],
            "definition_of_done": ["Done A", "Done B"],
        }
    )

    assert summary.scope_highlights == (
        "First scope item",
        "Second scope item",
        "First scope item",
    )
    assert summary.definition_of_done == ("Done A", "Done B")
    assert "- First scope item\n- Second scope item\n- First scope item" in summary.to_markdown()


def test_accepts_implementation_brief_models_and_serializes_stably():
    brief_model = ImplementationBrief.model_validate(_brief())

    summary = build_stakeholder_summary(brief_model)
    payload = stakeholder_summary_to_dict(summary)

    assert payload == summary.to_dict()
    assert list(payload) == [
        "brief_id",
        "title",
        "target_audience",
        "buyer",
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "scope_highlights",
        "non_goals",
        "risks",
        "validation_plan",
        "definition_of_done",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_partial_mapping_input_uses_available_fields_after_validation_fallback():
    summary = build_stakeholder_summary(
        {
            "id": "ib-partial",
            "title": "Partial Brief",
            "problem_statement": "Only the problem is known.",
            "scope": ["Known scope"],
            "definition_of_done": "not a list",
            "unexpected": "forces fallback because brief models forbid extras",
        }
    )

    assert summary.to_dict() == {
        "brief_id": "ib-partial",
        "title": "Partial Brief",
        "target_audience": None,
        "buyer": None,
        "workflow_context": None,
        "problem_statement": "Only the problem is known.",
        "mvp_goal": None,
        "scope_highlights": ["Known scope"],
        "non_goals": [],
        "risks": [],
        "validation_plan": None,
        "definition_of_done": [],
    }
    assert summary.to_markdown() == """# Partial Brief

## Problem Statement
Only the problem is known.

## Scope Highlights
- Known scope
"""


def _brief():
    return {
        "id": "ib-summary",
        "source_brief_id": "sb-summary",
        "title": "Stakeholder Summary",
        "domain": "planning",
        "target_user": "Support managers",
        "buyer": "Customer operations leadership",
        "workflow_context": "Teams review escalations during weekly planning.",
        "problem_statement": "Escalation work is hard to prioritize before planning.",
        "mvp_goal": "Provide a concise pre-planning packet.",
        "product_surface": "Planning workspace",
        "scope": [
            "Summarize impact by stakeholder group",
            "Keep source language visible",
        ],
        "non_goals": ["Replace execution planning"],
        "assumptions": ["Stakeholders review summaries before planning"],
        "architecture_notes": "Use deterministic local rendering.",
        "data_requirements": "Implementation brief fields only.",
        "integration_points": ["Brief export workflow"],
        "risks": ["Stakeholders may miss edge-case validation needs"],
        "validation_plan": "Review the packet with product, design, and engineering.",
        "definition_of_done": [
            "Stakeholders can approve the brief",
            "Planning can begin from the summary",
        ],
        "status": "draft",
        "generation_model": "test-model",
        "generation_tokens": 1,
        "generation_prompt": "test prompt",
    }
