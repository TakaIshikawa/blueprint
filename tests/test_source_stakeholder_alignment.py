import json

from blueprint.domain.models import SourceBrief
from blueprint.source_stakeholder_alignment import (
    SourceStakeholderAlignmentReport,
    SourceStakeholderRecord,
    build_source_stakeholder_alignment,
    source_stakeholder_alignment_to_dict,
    source_stakeholder_alignment_to_markdown,
    summarize_source_stakeholder_alignment,
)


def test_structured_metadata_extracts_alignment_groups_with_evidence():
    result = build_source_stakeholder_alignment(
        {
            "id": "source-align",
            "title": "Checkout approvals",
            "summary": "Capture stakeholder readiness before planning.",
            "metadata": {
                "stakeholders": [
                    {
                        "name": "Legal",
                        "role": "approver",
                        "status": "approved",
                        "evidence": "Legal approved the consent copy.",
                    },
                    {
                        "group": "Security",
                        "status": "blocking",
                        "objection": "Security blocks launch until threat review is complete.",
                    },
                    {
                        "team": "Support",
                        "status": "informational",
                        "comment": "Keep Support informed for help center updates.",
                    },
                ],
                "alignment_questions": ["Who approves the final retention copy?"],
            },
        }
    )

    assert isinstance(result, SourceStakeholderAlignmentReport)
    assert result.brief_id == "source-align"
    assert isinstance(result.aligned_stakeholders[0], SourceStakeholderRecord)
    assert result.aligned_stakeholders[0].stakeholder == "Legal"
    assert result.aligned_stakeholders[0].role == "approver"
    assert result.aligned_stakeholders[0].signals == ("explicit_approval",)
    assert "metadata.stakeholders[0]: Legal approved the consent copy." in result.aligned_stakeholders[0].evidence
    assert result.blocking_objections[0].stakeholder == "Security"
    assert result.informational_stakeholders[0].stakeholder == "Support"
    assert result.unresolved_alignment_questions == (
        "metadata.alignment_questions: Who approves the final retention copy?",
    )
    assert result.summary["ready_for_planning"] is False


def test_source_brief_model_frontmatter_and_markdown_headings_are_supported():
    source_result = summarize_source_stakeholder_alignment(
        SourceBrief.model_validate(
            {
                "id": "source-model",
                "title": "Settings rollout",
                "domain": "admin",
                "summary": "Product approved account settings and Ops should be notified.",
                "source_project": "notion",
                "source_entity_type": "page",
                "source_id": "SET-7",
                "source_payload": {
                    "body": "Security: signed off on the access log copy.\nOps: FYI for rollout timing."
                },
                "source_links": {},
            }
        )
    )
    markdown_result = build_source_stakeholder_alignment(
        """---
id: md-brief
title: Launch alignment
approvals:
  - Design: approved final modal copy
informational_stakeholders:
  - Sales: keep informed for enablement
---
# Launch alignment

## Approvals
- Product: signed off on the default workflow.

## Objections
- Privacy: blocks release until data deletion language is clear.

## Open Questions
- Does Finance need approval before rollout?
"""
    )

    assert [record.stakeholder for record in source_result.aligned_stakeholders] == ["Product", "Security"]
    assert source_result.informational_stakeholders[0].stakeholder == "Ops"
    assert markdown_result.brief_id == "md-brief"
    assert [record.stakeholder for record in markdown_result.aligned_stakeholders] == ["Design", "Product"]
    assert markdown_result.blocking_objections[0].stakeholder == "Privacy"
    assert markdown_result.informational_stakeholders[0].stakeholder == "Sales"
    assert markdown_result.unresolved_alignment_questions == (
        "body:line10: Does Finance need approval before rollout?",
    )


def test_conflicting_signals_and_priorities_are_reported_deterministically():
    result = build_source_stakeholder_alignment(
        {
            "id": "conflict-brief",
            "body": (
                "Product: approved the fast-launch scope.\n"
                "Product: blocks release unless onboarding remains in scope.\n"
                "Sales wants speed while Security requires manual review before launch.\n"
                "Confirm whether Leadership accepts the risk?"
            ),
        }
    )

    assert [record.stakeholder for record in result.aligned_stakeholders] == ["Product"]
    assert [record.stakeholder for record in result.blocking_objections] == ["Product"]
    assert result.conflicting_priorities == (
        "body:line3: Sales wants speed while Security requires manual review before launch.",
    )
    assert result.unresolved_alignment_questions == (
        "body:line4: Confirm whether Leadership accepts the risk?",
    )
    assert result.summary["has_blocking_objections"] is True


def test_empty_and_invalid_inputs_return_stable_empty_report():
    empty = build_source_stakeholder_alignment({"id": "empty", "title": "Polish", "body": "Update labels."})
    invalid = build_source_stakeholder_alignment(17)

    assert empty.brief_id == "empty"
    assert empty.aligned_stakeholders == ()
    assert empty.blocking_objections == ()
    assert empty.informational_stakeholders == ()
    assert empty.unresolved_alignment_questions == ()
    assert empty.summary == {
        "aligned_count": 0,
        "blocking_count": 0,
        "informational_count": 0,
        "unresolved_question_count": 0,
        "conflicting_priority_count": 0,
        "stakeholder_count": 0,
        "has_blocking_objections": False,
        "ready_for_planning": True,
    }
    assert "No stakeholder alignment signals were found" in empty.to_markdown()
    assert invalid.brief_id is None
    assert invalid.summary["ready_for_planning"] is True


def test_dict_serialization_is_json_safe_and_stable():
    result = build_source_stakeholder_alignment(
        {
            "id": "dict-brief",
            "metadata": {
                "approvals": [{"name": "Design", "decision": "approved visual QA"}],
                "objections": [{"name": "Legal", "reason": "Legal objects to implied consent."}],
                "informational_stakeholders": ["Support: FYI for macro updates."],
            },
        }
    )
    payload = source_stakeholder_alignment_to_dict(result)

    assert payload == result.to_dict()
    assert result.to_dicts() == {
        "aligned_stakeholders": payload["aligned_stakeholders"],
        "blocking_objections": payload["blocking_objections"],
        "informational_stakeholders": payload["informational_stakeholders"],
    }
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "brief_id",
        "aligned_stakeholders",
        "blocking_objections",
        "informational_stakeholders",
        "unresolved_alignment_questions",
        "conflicting_priorities",
        "summary",
    ]
    assert list(payload["aligned_stakeholders"][0]) == [
        "stakeholder",
        "status",
        "role",
        "signals",
        "evidence",
    ]


def test_markdown_rendering_escapes_tables_and_uses_helper():
    result = build_source_stakeholder_alignment(
        {
            "id": "pipes",
            "body": "Design | UX: approved copy | flow.\nSecurity: blocks launch until review.",
        }
    )
    markdown = source_stakeholder_alignment_to_markdown(result)

    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source Stakeholder Alignment: pipes")
    assert "## Aligned Stakeholders" in markdown
    assert "Design \\| UX" in markdown
    assert "copy \\| flow" in markdown
    assert "## Blocking Objections" in markdown
