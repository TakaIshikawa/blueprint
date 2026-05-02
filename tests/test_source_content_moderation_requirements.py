import json

from blueprint.domain.models import SourceBrief
from blueprint.source_content_moderation_requirements import (
    SourceContentModerationRequirement,
    SourceContentModerationRequirementsReport,
    build_source_content_moderation_requirements,
    derive_source_content_moderation_requirements,
    extract_source_content_moderation_requirements,
    generate_source_content_moderation_requirements,
    source_content_moderation_requirements_to_dict,
    source_content_moderation_requirements_to_dicts,
    source_content_moderation_requirements_to_markdown,
    summarize_source_content_moderation_requirements,
)


def test_extracts_moderation_categories_in_deterministic_order_from_brief_and_fields():
    result = build_source_content_moderation_requirements(
        _source_brief(
            summary=(
                "User-generated content includes posts, comments, and media uploads. "
                "Automated detection should score spam and toxicity before publication."
            ),
            source_payload={
                "moderation_requirements": [
                    "Users must report abuse with a report reason from the content menu.",
                    "Reviewer workflow needs a human review queue for flagged posts.",
                    "Policy taxonomy must include harassment, hate speech, and self-harm labels.",
                    "Moderation decisions need audit history with who reviewed each case.",
                    "Safety escalation must route credible threats to Trust and Safety.",
                ]
            },
        )
    )

    assert isinstance(result, SourceContentModerationRequirementsReport)
    assert all(isinstance(record, SourceContentModerationRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "user_generated_content",
        "abuse_reporting",
        "automated_detection",
        "human_review_queue",
        "policy_taxonomy",
        "audit_history",
        "safety_escalation",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["abuse_reporting"].suggested_owner == "trust_and_safety"
    assert by_category["human_review_queue"].suggested_owner == "operations"
    assert by_category["audit_history"].suggested_owner == "compliance"
    assert (
        "source_payload.moderation_requirements[0]"
        in by_category["abuse_reporting"].source_field_paths
    )
    assert "report reason" in by_category["abuse_reporting"].matched_terms
    assert any(
        "spam and toxicity" in evidence for evidence in by_category["automated_detection"].evidence
    )
    assert result.summary["requirement_count"] == 7
    assert result.summary["category_counts"]["policy_taxonomy"] == 1
    assert result.summary["confidence_counts"]["high"] == 7
    assert result.summary["status"] == "ready_for_planning"


def test_appeal_review_queue_and_reporting_are_separate_records_with_notes():
    result = build_source_content_moderation_requirements(
        _source_brief(
            summary=(
                "Users must report abuse from every message. "
                "Moderators need a review queue for reported messages. "
                "Authors can appeal moderation decisions within 14 days."
            )
        )
    )

    assert [record.category for record in result.records] == [
        "abuse_reporting",
        "human_review_queue",
        "appeal_flow",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["abuse_reporting"].suggested_owner == "trust_and_safety"
    assert by_category["human_review_queue"].suggested_owner == "operations"
    assert by_category["appeal_flow"].suggested_owner == "trust_and_safety"
    assert "intake states" in by_category["abuse_reporting"].suggested_planning_note
    assert "queue assignment" in by_category["human_review_queue"].suggested_planning_note
    assert "second-review ownership" in by_category["appeal_flow"].suggested_planning_note


def test_duplicate_evidence_merges_predictably_and_serialization_helpers_are_stable():
    source = _source_brief(
        source_id="moderation-duplicates",
        summary="Users must report abuse with a report reason.",
        source_payload={
            "requirements": [
                "Users must report abuse with a report reason.",
                "Users must report abuse with a report reason.",
                "Appeals must support second review.",
            ],
            "acceptance_criteria": [
                "Appeals must support second review.",
            ],
        },
    )
    model = SourceBrief.model_validate(source)

    result = build_source_content_moderation_requirements(model)
    generated = generate_source_content_moderation_requirements(model)
    extracted = extract_source_content_moderation_requirements(model)
    derived = derive_source_content_moderation_requirements(model)
    payload = source_content_moderation_requirements_to_dict(result)
    markdown = source_content_moderation_requirements_to_markdown(result)

    assert generated.to_dict() == result.to_dict()
    assert extracted.to_dict() == result.to_dict()
    assert derived.to_dict() == result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert source_content_moderation_requirements_to_dicts(result) == payload["requirements"]
    assert source_content_moderation_requirements_to_dicts(result.records) == payload["records"]
    assert summarize_source_content_moderation_requirements(result) == result.summary
    assert [record.category for record in result.records] == [
        "abuse_reporting",
        "appeal_flow",
    ]
    by_category = {record.category: record for record in result.records}
    assert len(by_category["abuse_reporting"].evidence) == 1
    assert len(by_category["appeal_flow"].evidence) == 1
    assert by_category["abuse_reporting"].confidence == "high"
    assert by_category["appeal_flow"].confidence == "high"
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "evidence",
        "source_field_paths",
        "matched_terms",
        "confidence",
        "suggested_owner",
        "suggested_planning_note",
    ]
    assert markdown.startswith(
        "# Source Content Moderation Requirements Report: moderation-duplicates"
    )


def test_empty_invalid_and_unrelated_inputs_return_stable_empty_reports():
    empty = build_source_content_moderation_requirements(
        _source_brief(
            summary="No content moderation or abuse reporting changes are in scope.",
            source_payload={"requirements": []},
        )
    )
    malformed = build_source_content_moderation_requirements(
        {"source_payload": {"notes": object()}}
    )
    blank_text = build_source_content_moderation_requirements("")
    unrelated = build_source_content_moderation_requirements(
        _source_brief(
            title="Profile settings",
            summary="Improve onboarding copy and button labels.",
            source_payload={"requirements": ["Keep form submission behavior unchanged."]},
        )
    )

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "user_generated_content": 0,
            "abuse_reporting": 0,
            "automated_detection": 0,
            "human_review_queue": 0,
            "policy_taxonomy": 0,
            "appeal_flow": 0,
            "audit_history": 0,
            "safety_escalation": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "owner_counts": {
            "compliance": 0,
            "operations": 0,
            "policy": 0,
            "product": 0,
            "trust_and_safety": 0,
        },
        "categories": [],
        "status": "no_moderation_language",
    }
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert malformed.summary == expected_summary
    assert blank_text.summary == expected_summary
    assert unrelated.summary == expected_summary
    assert "No source content moderation requirements were inferred" in empty.to_markdown()


def _source_brief(
    *,
    source_id="moderation-source",
    title="Content moderation requirements",
    domain="trust-and-safety",
    summary="General content moderation requirements.",
    source_payload=None,
    source_links=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }
