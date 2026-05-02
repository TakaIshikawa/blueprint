import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_consent_revocation_requirements import (
    SourceConsentRevocationRequirement,
    SourceConsentRevocationRequirementsReport,
    build_source_consent_revocation_requirements,
    derive_source_consent_revocation_requirements,
    extract_source_consent_revocation_requirements,
    generate_source_consent_revocation_requirements,
    source_consent_revocation_requirements_to_dict,
    source_consent_revocation_requirements_to_dicts,
    source_consent_revocation_requirements_to_markdown,
    summarize_source_consent_revocation_requirements,
)


def test_extracts_all_revocation_categories_from_source_brief_text():
    result = build_source_consent_revocation_requirements(
        _source_brief(
            source_payload={
                "privacy": (
                    "Users must withdraw consent through the preference center. "
                    "Opt-out must take effect immediately after submission. "
                    "Revocation must propagate to the CRM and downstream marketing platform. "
                    "After unsubscribe we must stop processing analytics and marketing targeting. "
                    "Revocation audit evidence must include timestamp, actor, and policy version. "
                    "Send a confirmation receipt after consent withdrawal. "
                    "Stop processing except for legal obligation and transactional service messages. "
                    "Users must re-consent with a new opt-in before marketing is re-enabled."
                )
            }
        )
    )

    assert isinstance(result, SourceConsentRevocationRequirementsReport)
    assert result.source_id == "sb-revocation"
    assert all(isinstance(record, SourceConsentRevocationRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "withdrawal_channel",
        "immediate_effect",
        "downstream_propagation",
        "data_processing_stop",
        "audit_evidence",
        "user_confirmation",
        "exception_handling",
        "reconsent_flow",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["withdrawal_channel"].revocation_channel == "preference center"
    assert by_category["immediate_effect"].timing == "immediately"
    assert by_category["downstream_propagation"].propagation == "CRM, downstream, marketing platform"
    assert by_category["data_processing_stop"].timing is None
    assert by_category["audit_evidence"].audit_evidence == "timestamp, actor, policy version"
    assert by_category["user_confirmation"].user_confirmation == "confirmation receipt"
    assert by_category["exception_handling"].exception_handling == "legal obligation"
    assert by_category["reconsent_flow"].reconsent_flow == "re-consent"
    assert result.summary["requirement_count"] == 8
    assert result.summary["category_counts"] == {
        "withdrawal_channel": 1,
        "immediate_effect": 1,
        "downstream_propagation": 1,
        "data_processing_stop": 1,
        "audit_evidence": 1,
        "user_confirmation": 1,
        "exception_handling": 1,
        "reconsent_flow": 1,
    }


def test_structured_source_and_implementation_brief_inputs_are_supported():
    structured = build_source_consent_revocation_requirements(
        _source_brief(
            source_payload={
                "consent_revocation": [
                    {
                        "withdrawal_channel": "Unsubscribe link and privacy settings must revoke consent.",
                        "timing": "Revocation must be effective within 5 minutes.",
                        "downstream_propagation": "Publish opt-out event to vendors and suppression list.",
                    },
                    {
                        "audit_evidence": "Withdrawal receipt stores revoked_at and user id.",
                        "confirmation": "Show a success message after unsubscribe.",
                        "exceptions": "Transactional account notices continue unless prohibited by law.",
                        "reconsent_flow": "Resubscribe requires fresh consent.",
                    },
                ]
            }
        )
    )
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Preference center must allow users to opt out of personalization.",
                "After consent withdrawal, processing must stop within 24 hours.",
            ],
            definition_of_done=[
                "Revocation audit log captures timestamp and policy version.",
            ],
        )
    )
    object_result = build_source_consent_revocation_requirements(
        SimpleNamespace(
            id="object-revocation",
            summary="Unsubscribe must stop processing immediately and send confirmation.",
        )
    )

    assert [record.category for record in structured.records] == [
        "withdrawal_channel",
        "immediate_effect",
        "downstream_propagation",
        "audit_evidence",
        "user_confirmation",
        "exception_handling",
        "reconsent_flow",
    ]
    assert structured.records[0].revocation_channel == "Unsubscribe link, privacy settings"
    assert structured.records[1].timing == "within 5 minutes"
    assert structured.records[2].propagation == "vendors, suppression list"
    assert any("source_payload.consent_revocation[0]" in item for item in structured.records[0].evidence)

    model_result = extract_source_consent_revocation_requirements(brief)
    assert model_result.source_id == "impl-revocation"
    assert [record.category for record in model_result.records] == [
        "withdrawal_channel",
        "data_processing_stop",
        "audit_evidence",
    ]
    assert model_result.records[1].timing == "within 24 hours"
    assert object_result.records[0].category == "immediate_effect"


def test_general_consent_capture_without_withdrawal_semantics_is_ignored():
    capture_only = build_source_consent_revocation_requirements(
        _source_brief(
            summary="Consent capture requirements.",
            source_payload={
                "requirements": [
                    "Signup must capture explicit consent with an unchecked checkbox.",
                    "Consent for analytics must be purpose-specific before tracking.",
                    "Consent audit evidence must include consented_at and policy version.",
                ]
            },
        )
    )
    empty = build_source_consent_revocation_requirements(
        _source_brief(summary="Improve onboarding copy.", source_payload={})
    )
    malformed = build_source_consent_revocation_requirements(
        {"id": "brief-empty", "source_payload": {"notes": object()}}
    )
    invalid = build_source_consent_revocation_requirements(42)

    assert capture_only.records == ()
    assert empty.records == ()
    assert empty.requirements == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "withdrawal_channel": 0,
            "immediate_effect": 0,
            "downstream_propagation": 0,
            "data_processing_stop": 0,
            "audit_evidence": 0,
            "user_confirmation": 0,
            "exception_handling": 0,
            "reconsent_flow": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
    }
    assert "No consent revocation requirements were found" in empty.to_markdown()
    assert malformed.records == ()
    assert invalid.records == ()


def test_aliases_serialization_markdown_ordering_and_no_mutation():
    source = _source_brief(
        source_id="revocation-model",
        summary="General revocation requirements.",
        source_payload={
            "requirements": [
                "Revocation must propagate to vendors and the suppression list.",
                "Users must unsubscribe through the preference center.",
                "Consent withdrawal audit log must store timestamp.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_consent_revocation_requirements(source)
    model_result = generate_source_consent_revocation_requirements(model)
    derived = derive_source_consent_revocation_requirements(model)
    payload = source_consent_revocation_requirements_to_dict(model_result)
    markdown = source_consent_revocation_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_consent_revocation_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_consent_revocation_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_consent_revocation_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_consent_revocation_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "requirement_text",
        "revocation_channel",
        "propagation",
        "timing",
        "audit_evidence",
        "user_confirmation",
        "exception_handling",
        "reconsent_flow",
        "source_field",
        "evidence",
        "confidence",
        "unresolved_questions",
        "suggested_plan_impacts",
    ]
    assert [record.category for record in model_result.records] == [
        "withdrawal_channel",
        "downstream_propagation",
        "audit_evidence",
    ]
    assert model_result.records[0].requirement_category == "withdrawal_channel"
    assert model_result.records[0].planning_notes == model_result.records[0].suggested_plan_impacts
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Category | Requirement | Channel | Propagation | Timing | Audit | Confirmation | Exceptions | Reconsent | Source Field | Confidence | Unresolved Questions | Suggested Plan Impacts | Evidence |" in markdown
    assert "Revocation must propagate" in markdown


def _source_brief(
    *,
    source_id="sb-revocation",
    title="Consent revocation requirements",
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


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "impl-revocation",
        "source_brief_id": "source-revocation",
        "title": "Consent revocation rollout",
        "domain": "privacy",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need revocation requirements before task generation.",
        "problem_statement": "Revocation requirements need to be extracted early.",
        "mvp_goal": "Plan consent revocation work from source briefs.",
        "product_surface": "privacy",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review generated plan for revocation coverage.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
    }
