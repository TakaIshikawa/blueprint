import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_consent_policy_requirements import (
    SourceConsentPolicyRequirement,
    SourceConsentPolicyRequirementsReport,
    build_source_consent_policy_requirements,
    derive_source_consent_policy_requirements,
    extract_source_consent_policy_requirements,
    generate_source_consent_policy_requirements,
    source_consent_policy_requirements_to_dict,
    source_consent_policy_requirements_to_dicts,
    source_consent_policy_requirements_to_markdown,
    summarize_source_consent_policy_requirements,
)


def test_extracts_all_consent_policy_categories_from_source_brief_text():
    result = build_source_consent_policy_requirements(
        _source_brief(
            source_payload={
                "privacy": (
                    "Consent for product analytics must be purpose-specific before tracking. "
                    "Signup must capture explicit consent with an unchecked checkbox. "
                    "Users must be able to withdraw consent from the preference center. "
                    "Consent audit evidence must include timestamp, actor, and policy version. "
                    "Consent records must be retained for 7 years and deleted after account closure. "
                    "GDPR requires consent or documented lawful basis for EU analytics. "
                    "COPPA parental consent is required for children under 13. "
                    "Third-party CRM sharing requires consent and opt-out propagation."
                )
            }
        )
    )

    assert isinstance(result, SourceConsentPolicyRequirementsReport)
    assert result.source_id == "sb-consent"
    assert all(isinstance(record, SourceConsentPolicyRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "consent_purpose",
        "capture_mechanism",
        "withdrawal",
        "audit_evidence",
        "retention",
        "regional_legal_basis",
        "minor_consent",
        "third_party_sharing",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["consent_purpose"].detail == "product analytics must be purpose-specific before tracking"
    assert by_category["capture_mechanism"].detail == "unchecked checkbox"
    assert by_category["retention"].detail == "for 7 years"
    assert by_category["regional_legal_basis"].detail == "GDPR, lawful basis, EU"
    assert by_category["minor_consent"].detail == "parental consent"
    assert by_category["third_party_sharing"].detail == "Third-party"
    assert result.summary["requirement_count"] == 8
    assert result.summary["category_counts"] == {
        "consent_purpose": 1,
        "capture_mechanism": 1,
        "withdrawal": 1,
        "audit_evidence": 1,
        "retention": 1,
        "regional_legal_basis": 1,
        "minor_consent": 1,
        "third_party_sharing": 1,
    }


def test_structured_source_and_implementation_brief_inputs_are_supported():
    structured = build_source_consent_policy_requirements(
        _source_brief(
            source_payload={
                "consent_policy": [
                    {
                        "purpose": "Marketing consent is required for promotional email.",
                        "capture_mechanism": "Checkout must show a granular consent toggle.",
                        "withdrawal": "Unsubscribe must revoke consent immediately.",
                    },
                    {
                        "audit_evidence": "Consent receipt must store consented_at and policy version.",
                        "regional_basis": "CCPA opt-out and GDPR lawful basis must be documented.",
                        "third_party_sharing": "Analytics provider sharing requires consent.",
                    },
                ]
            }
        )
    )
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Account settings must capture consent with a consent screen.",
                "Minor consent requires age verification and guardian consent.",
            ],
            data_requirements="Consent history must be retained for 24 months.",
        )
    )
    object_result = build_source_consent_policy_requirements(
        SimpleNamespace(
            id="object-consent",
            summary="Consent for personalization must be collected through a banner.",
        )
    )

    assert [record.category for record in structured.records] == [
        "consent_purpose",
        "capture_mechanism",
        "withdrawal",
        "audit_evidence",
        "regional_legal_basis",
        "third_party_sharing",
    ]
    assert structured.records[1].detail == "granular consent toggle"
    assert structured.records[4].detail == "CCPA, GDPR, lawful basis"

    model_result = extract_source_consent_policy_requirements(brief)
    assert model_result.source_id == "impl-consent"
    assert [record.category for record in model_result.records] == [
        "capture_mechanism",
        "retention",
        "minor_consent",
    ]
    assert object_result.records[0].category == "consent_purpose"


def test_empty_no_signal_and_malformed_inputs_return_deterministic_empty_reports():
    empty = build_source_consent_policy_requirements(
        _source_brief(summary="Improve onboarding copy.", source_payload={})
    )
    no_signal = build_source_consent_policy_requirements(
        {"id": "brief-empty", "source_payload": {"notes": object()}}
    )
    invalid = build_source_consent_policy_requirements(42)

    assert empty.records == ()
    assert empty.requirements == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "consent_purpose": 0,
            "capture_mechanism": 0,
            "withdrawal": 0,
            "audit_evidence": 0,
            "retention": 0,
            "regional_legal_basis": 0,
            "minor_consent": 0,
            "third_party_sharing": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
    }
    assert "No consent policy requirements were found" in empty.to_markdown()
    assert no_signal.records == ()
    assert invalid.records == ()


def test_aliases_serialization_markdown_ordering_and_no_mutation():
    source = _source_brief(
        source_id="consent-model",
        summary="General consent policy requirements.",
        source_payload={
            "requirements": [
                "Third-party marketing platform sharing requires consent.",
                "Consent for analytics must be captured by a banner.",
                "Consent audit trail must record timestamp and policy version.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_consent_policy_requirements(source)
    model_result = generate_source_consent_policy_requirements(model)
    derived = derive_source_consent_policy_requirements(model)
    payload = source_consent_policy_requirements_to_dict(model_result)
    markdown = source_consent_policy_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_consent_policy_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_consent_policy_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_consent_policy_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_consent_policy_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "requirement_text",
        "detail",
        "planning_note",
        "confidence",
        "evidence",
    ]
    assert [record.category for record in model_result.records] == [
        "consent_purpose",
        "capture_mechanism",
        "audit_evidence",
        "third_party_sharing",
    ]
    assert model_result.records[0].requirement_category == "consent_purpose"
    assert model_result.records[0].planning_notes == model_result.records[0].planning_note
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Category | Requirement | Detail | Confidence | Planning Note | Evidence |" in markdown
    assert "Third-party marketing platform" in markdown


def _source_brief(
    *,
    source_id="sb-consent",
    title="Consent requirements",
    domain="privacy",
    summary="General privacy requirements.",
    source_payload=None,
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
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, scope=None, data_requirements=None):
    return {
        "id": "impl-consent",
        "source_brief_id": "source-consent",
        "title": "Consent rollout",
        "domain": "privacy",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need consent policy requirements before task generation.",
        "problem_statement": "Consent requirements need to be extracted early.",
        "mvp_goal": "Plan consent policy work from source briefs.",
        "product_surface": "privacy",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": data_requirements,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review generated plan for consent coverage.",
        "definition_of_done": ["Consent policy requirements are represented."],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
