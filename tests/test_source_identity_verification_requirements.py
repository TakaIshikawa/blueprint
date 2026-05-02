import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_identity_verification_requirements import (
    SourceIdentityVerificationRequirement,
    SourceIdentityVerificationRequirementsReport,
    build_source_identity_verification_requirements,
    derive_source_identity_verification_requirements,
    extract_source_identity_verification_requirements,
    generate_source_identity_verification_requirements,
    source_identity_verification_requirements_to_dict,
    source_identity_verification_requirements_to_dicts,
    source_identity_verification_requirements_to_markdown,
    summarize_source_identity_verification_requirements,
)


def test_structured_kyc_methods_extract_stable_records_and_details():
    result = build_source_identity_verification_requirements(
        _source_brief(
            source_payload={
                "identity_verification": {
                    "onboarding": (
                        "KYC verification must require government ID document upload, selfie liveness check, "
                        "phone OTP verification, provider: Persona, manual review: compliance review queue "
                        "approves or rejects within SLA, failure_handling: allow two resubmits then support escalation, "
                        "accessibility_fallback: support-assisted manual fallback when camera is unavailable, "
                        "retention_rule: retain verification evidence for 30 days then redact, "
                        "re_verification_trigger: expired document or suspicious activity trigger, "
                        "audit_evidence: verification log includes timestamp, reviewer id, provider reference, and case id."
                    )
                }
            }
        )
    )

    assert isinstance(result, SourceIdentityVerificationRequirementsReport)
    assert all(isinstance(record, SourceIdentityVerificationRequirement) for record in result.records)
    assert result.source_id == "source-identity-verification"
    assert [record.method for record in result.records] == [
        "document_upload",
        "selfie_liveness",
        "phone_email_otp",
        "manual_review",
        "third_party_kyc_provider",
        "re_verification_trigger",
        "retention_rule",
        "failure_handling",
        "accessibility_fallback",
        "audit_evidence",
    ]
    by_method = {record.method: record for record in result.records}
    assert by_method["document_upload"].source_field == "source_payload.identity_verification.onboarding"
    assert "government id" in by_method["document_upload"].matched_terms
    assert by_method["third_party_kyc_provider"].provider == "persona"
    assert "support escalation" in by_method["failure_handling"].failure_handling
    assert "camera is unavailable" in by_method["accessibility_fallback"].fallback
    assert "expired document" in by_method["re_verification_trigger"].re_verification_trigger
    assert "30 days" in by_method["retention_rule"].retention_rule
    assert "reviewer id" in by_method["audit_evidence"].audit_evidence
    assert result.summary["requirement_count"] == 10
    assert result.summary["requires_document_upload"] is True
    assert result.summary["requires_liveness_check"] is True
    assert result.summary["requires_otp"] is True
    assert result.summary["requires_manual_review"] is True
    assert result.summary["requires_kyc_provider"] is True
    assert result.summary["requires_failure_handling"] is True
    assert result.summary["requires_accessibility_fallback"] is True
    assert result.summary["requires_audit_evidence"] is True
    assert result.summary["status"] == "ready_for_identity_verification_planning"


def test_natural_language_and_implementation_brief_inputs_are_supported():
    text_result = build_source_identity_verification_requirements(
        """
# Identity verification

- Applicants must upload a passport or driver's license before payouts.
- Knowledge-based verification is required when document checks fail.
- Email OTP verification should expire after 10 minutes.
"""
    )
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "KYC onboarding must use Onfido as the third-party KYC provider.",
                "Manual review queue should record audit evidence for approve or reject decisions.",
            ],
            definition_of_done=[
                "Re-verification is triggered when a document expires or the user's legal name changes.",
                "Verification failures must allow appeal and support escalation.",
            ],
        )
    )

    assert [record.method for record in text_result.records] == [
        "document_upload",
        "knowledge_based_verification",
        "phone_email_otp",
    ]
    implementation_result = generate_source_identity_verification_requirements(implementation)
    assert implementation_result.source_id == "implementation-identity-verification"
    assert {record.method for record in implementation_result.records} == {
        "manual_review",
        "third_party_kyc_provider",
        "re_verification_trigger",
        "failure_handling",
        "audit_evidence",
    }
    assert any(record.provider == "onfido" for record in implementation_result.records)
    assert any(record.audit_evidence and "approve or reject" in record.audit_evidence for record in implementation_result.records)


def test_duplicate_evidence_serialization_markdown_aliases_and_no_mutation_are_stable():
    source = _source_brief(
        source_id="identity-dedupe",
        source_payload={
            "requirements": [
                "Identity verification must require document upload and manual review | operator note.",
                "Identity verification must require document upload and manual review | operator note.",
                "KYC provider: Veriff must return provider reference for audit evidence.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_identity_verification_requirements(source)
    model_result = extract_source_identity_verification_requirements(model)
    generated = generate_source_identity_verification_requirements(model)
    derived = derive_source_identity_verification_requirements(model)
    payload = source_identity_verification_requirements_to_dict(model_result)
    markdown = source_identity_verification_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert generated.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_identity_verification_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_identity_verification_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_identity_verification_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "method",
        "requirement_text",
        "provider",
        "failure_handling",
        "fallback",
        "re_verification_trigger",
        "retention_rule",
        "manual_review",
        "audit_evidence",
        "source_field",
        "evidence",
        "matched_terms",
        "confidence",
        "planning_note",
    ]
    assert [record.method for record in model_result.records] == [
        "document_upload",
        "manual_review",
        "third_party_kyc_provider",
        "audit_evidence",
    ]
    assert model_result.records[0].evidence == (
        "source_payload.requirements[0]: Identity verification must require document upload and manual review | operator note.",
    )
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Method | Requirement | Provider |" in markdown
    assert "manual review \\| operator note" in markdown
    assert model_result.records[0].requirement_category == "document_upload"
    assert model_result.records[0].planning_notes == (model_result.records[0].planning_note,)


def test_auth_only_identity_provider_negated_invalid_and_object_inputs_are_handled():
    class BriefLike:
        id = "object-no-identity-verification"
        summary = "No identity verification, KYC, document upload, or liveness checks are required."

    object_result = build_source_identity_verification_requirements(
        SimpleNamespace(
            id="object-identity-verification",
            summary="KYC verification must require document upload.",
            compliance="Manual review is required when verification fails.",
        )
    )
    negated = build_source_identity_verification_requirements(BriefLike())
    auth_only = build_source_identity_verification_requirements(
        _source_brief(
            title="Authentication rollout",
            summary="Use Okta as the identity provider for SSO login and require MFA OTP for authentication.",
            source_payload={"requirements": ["OIDC identity provider configuration is required for login."]},
        )
    )
    no_scope = build_source_identity_verification_requirements(
        _source_brief(summary="KYC and identity verification are out of scope and no verification work is planned.")
    )
    malformed = build_source_identity_verification_requirements({"source_payload": {"notes": object()}})
    invalid = build_source_identity_verification_requirements(42)
    blank = build_source_identity_verification_requirements("")

    assert [record.method for record in object_result.records] == [
        "document_upload",
        "manual_review",
        "failure_handling",
    ]
    assert negated.records == ()
    assert auth_only.records == ()
    assert no_scope.records == ()
    assert malformed.records == ()
    assert invalid.records == ()
    assert blank.records == ()
    assert auth_only.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "method_counts": {
            "document_upload": 0,
            "selfie_liveness": 0,
            "knowledge_based_verification": 0,
            "phone_email_otp": 0,
            "manual_review": 0,
            "third_party_kyc_provider": 0,
            "re_verification_trigger": 0,
            "retention_rule": 0,
            "failure_handling": 0,
            "accessibility_fallback": 0,
            "audit_evidence": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "methods": [],
        "requires_document_upload": False,
        "requires_liveness_check": False,
        "requires_otp": False,
        "requires_manual_review": False,
        "requires_kyc_provider": False,
        "requires_re_verification": False,
        "requires_retention_rule": False,
        "requires_failure_handling": False,
        "requires_accessibility_fallback": False,
        "requires_audit_evidence": False,
        "status": "no_identity_verification_language",
    }
    assert invalid.to_dicts() == []
    assert "No identity verification requirements were found" in auth_only.to_markdown()


def _source_brief(
    *,
    source_id="source-identity-verification",
    title="Identity verification requirements",
    domain="compliance",
    summary="General identity verification requirements.",
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
        "id": "implementation-identity-verification",
        "source_brief_id": "source-identity-verification",
        "title": "Identity verification rollout",
        "domain": "compliance",
        "target_user": "compliance operators",
        "buyer": None,
        "workflow_context": "Teams need identity verification requirements before task generation.",
        "problem_statement": "Verification semantics need to be extracted early.",
        "mvp_goal": "Plan KYC behavior from source briefs.",
        "product_surface": "onboarding",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review generated plan for identity verification coverage.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
    }
