import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_consent_management_requirements import (
    SourceConsentManagementRequirement,
    SourceConsentManagementRequirementsReport,
    build_source_consent_management_requirements,
    derive_source_consent_management_requirements,
    extract_source_consent_management_requirements,
    generate_source_consent_management_requirements,
    source_consent_management_requirements_to_dict,
    source_consent_management_requirements_to_dicts,
    source_consent_management_requirements_to_markdown,
    summarize_source_consent_management_requirements,
)


def test_extracts_consent_management_categories_from_markdown_in_stable_order():
    result = build_source_consent_management_requirements(
        _source_brief(
            source_payload={
                "body": """
# Consent management requirements

- Signup must capture explicit consent with an unchecked checkbox before data processing begins.
- Consent must be purpose-specific for analytics, marketing, personalization, and data sharing.
- Users must withdraw consent from settings and revoke processing.
- Store consent history with consented_at, withdrawn_at, policy version, and channel.
- Cookie banner must show Accept all, Reject all, and Manage choices before tracking cookies run.
- Marketing opt-in is required before promotional emails and newsletter subscriptions.
- Privacy preference center must allow users to manage consent settings.
- Export proof of consent with consent receipt and audit log evidence for compliance.
"""
            }
        )
    )

    assert isinstance(result, SourceConsentManagementRequirementsReport)
    assert all(isinstance(record, SourceConsentManagementRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "consent_capture",
        "purpose_specific_consent",
        "withdrawal",
        "consent_history",
        "cookie_banner_consent",
        "marketing_opt_in",
        "privacy_preference_center",
        "proof_of_consent_audit",
    ]
    by_category = {record.category: record for record in result.records}
    assert "explicit user consent" in by_category["consent_capture"].required_capability
    assert by_category["withdrawal"].requirement_category == "withdrawal"
    assert by_category["marketing_opt_in"].requirement_type == "marketing_opt_in"
    assert all(record.confidence == "high" for record in result.records)
    assert by_category["consent_history"].evidence == (
        "source_payload.body: Store consent history with consented_at, withdrawn_at, policy version, and channel.",
    )
    assert result.summary["requirement_count"] == 8
    assert result.summary["category_counts"]["marketing_opt_in"] == 1
    assert result.summary["confidence_counts"]["high"] == 8
    assert result.summary["status"] == "ready_for_consent_management_planning"


def test_structured_payload_implementation_brief_and_objects_are_supported():
    structured = build_source_consent_management_requirements(
        {
            "id": "structured-consent",
            "title": "Consent management rollout",
            "metadata": {
                "consent_management": {
                    "capture": "Consent capture must use an affirmative checkbox.",
                    "purposes": "Purpose-specific consent is required for analytics and marketing.",
                    "withdrawal": "Users can revoke consent from the privacy preference center.",
                    "history": "Consent history stores timestamp, channel, and policy version.",
                    "cookie_banner": "Cookie banner must block tracking scripts until Accept all or Manage choices.",
                    "marketing": "Marketing opt-in is required before SMS messages.",
                    "proof": "Consent receipt export and audit evidence are required.",
                }
            },
        }
    )
    implementation = generate_source_consent_management_requirements(
        ImplementationBrief.model_validate(
            _implementation_brief(
                scope=[
                    "Signup must collect consent with an unchecked checkbox.",
                    "Privacy preference center supports withdrawal and marketing opt-in controls.",
                ],
                definition_of_done=[
                    "Proof of consent export includes consent receipt, timestamp, and policy version.",
                ],
            )
        )
    )
    object_result = build_source_consent_management_requirements(
        SimpleNamespace(id="object-consent", requirements="Marketing consent must be opt-in for newsletter emails.")
    )

    assert [record.category for record in structured.records] == [
        "consent_capture",
        "purpose_specific_consent",
        "withdrawal",
        "consent_history",
        "cookie_banner_consent",
        "marketing_opt_in",
        "privacy_preference_center",
        "proof_of_consent_audit",
    ]
    by_category = {record.category: record for record in structured.records}
    assert by_category["consent_capture"].source_field == "metadata.consent_management.capture"
    assert by_category["proof_of_consent_audit"].source_field == "metadata.consent_management.proof"
    assert implementation.source_id == "implementation-consent"
    assert [record.category for record in implementation.records] == [
        "consent_capture",
        "withdrawal",
        "marketing_opt_in",
        "privacy_preference_center",
        "proof_of_consent_audit",
    ]
    assert object_result.records[0].category == "marketing_opt_in"


def test_generic_privacy_mentions_and_negated_scope_are_not_over_classified():
    generic = build_source_consent_management_requirements(
        _source_brief(
            title="Privacy copy update",
            summary="Update the privacy policy and privacy notice copy for the footer.",
            source_payload={"requirements": ["Review data privacy language with legal."]},
        )
    )
    negated = build_source_consent_management_requirements(
        _source_brief(
            title="Settings cleanup",
            summary="No consent management, opt-in, withdrawal, cookie banner, or preference center work is required.",
            source_payload={"requirements": ["Rename account settings labels."]},
        )
    )
    malformed = build_source_consent_management_requirements({"source_payload": {"notes": object()}})
    blank = build_source_consent_management_requirements("")

    assert generic.records == ()
    assert negated.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert generic.summary["status"] == "no_consent_management_language"
    assert "No source consent management requirements were inferred." in generic.to_markdown()


def test_duplicate_merging_preserves_evidence_source_fields_and_markdown_escaping():
    result = build_source_consent_management_requirements(
        _source_brief(
            source_id="consent-dedupe",
            source_payload={
                "requirements": [
                    "Marketing opt-in must be captured before partner | customer newsletter emails.",
                    "Marketing opt-in must be captured before partner | customer newsletter emails.",
                    "Marketing consent must cover promotional messages.",
                ],
                "acceptance_criteria": [
                    "Marketing opt-in must be captured before partner | customer newsletter emails.",
                ],
            },
        )
    )

    assert [record.category for record in result.records] == ["marketing_opt_in"]
    marketing = result.records[0]
    assert marketing.evidence == (
        "source_payload.acceptance_criteria[0]: Marketing opt-in must be captured before partner | customer newsletter emails.",
        "source_payload.requirements[2]: Marketing consent must cover promotional messages.",
    )
    assert marketing.source_fields == (
        "source_payload.acceptance_criteria[0]",
        "source_payload.requirements[0]",
        "source_payload.requirements[1]",
        "source_payload.requirements[2]",
    )
    markdown = result.to_markdown()
    assert "partner \\| customer newsletter emails" in markdown


def test_aliases_serialization_json_ordering_confidence_and_no_input_mutation_are_stable():
    source = _source_brief(
        source_id="consent-model",
        source_payload={
            "requirements": [
                "Consent capture should use a checkbox.",
                "Consent history and proof of consent are needed for audit review.",
                "Marketing opt-in planned for newsletter emails.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_consent_management_requirements(source)
    model_result = extract_source_consent_management_requirements(model)
    derived = derive_source_consent_management_requirements(model)
    generated = generate_source_consent_management_requirements(model)
    text_result = build_source_consent_management_requirements(
        "Cookie consent banner must show Reject all and Manage choices."
    )
    payload = source_consent_management_requirements_to_dict(model_result)
    markdown = source_consent_management_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_consent_management_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert generated.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_consent_management_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_consent_management_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_consent_management_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "required_capability",
        "requirement_text",
        "evidence",
        "confidence",
        "source_field",
        "source_fields",
        "matched_terms",
    ]
    assert [record.category for record in model_result.records] == [
        "consent_capture",
        "consent_history",
        "marketing_opt_in",
        "proof_of_consent_audit",
    ]
    assert [record.confidence for record in model_result.records] == ["high", "high", "medium", "high"]
    assert markdown == model_result.to_markdown()
    assert markdown.startswith("# Source Consent Management Requirements Report: consent-model")
    assert text_result.records[0].category == "cookie_banner_consent"


def _source_brief(
    *,
    source_id="source-consent",
    title="Consent management requirements",
    domain="privacy",
    summary="General consent management requirements.",
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


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "implementation-consent",
        "source_brief_id": "source-consent",
        "title": "Consent management rollout",
        "domain": "privacy",
        "target_user": "privacy ops",
        "buyer": "legal",
        "workflow_context": "Teams need consent management requirements before execution planning.",
        "problem_statement": "Consent requirements need to be extracted early.",
        "mvp_goal": "Plan consent management work from source briefs.",
        "product_surface": "web app",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run consent management extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
