import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_authentication_requirements import (
    SourceAuthenticationRequirement,
    SourceAuthenticationRequirementsReport,
    build_source_authentication_requirements,
    derive_source_authentication_requirements,
    extract_source_authentication_requirements,
    generate_source_authentication_requirements,
    source_authentication_requirements_to_dict,
    source_authentication_requirements_to_dicts,
    source_authentication_requirements_to_markdown,
    summarize_source_authentication_requirements,
)


def test_extracts_authentication_requirements_from_brief_fields_and_nested_payloads():
    result = build_source_authentication_requirements(
        _source_brief(
            summary=(
                "Enterprise admins must use SAML SSO with Okta before launch. "
                "Customers need MFA for login."
            ),
            source_payload={
                "goals": [
                    "Support OIDC federated login for enterprise customers.",
                    "Passkeys should be available for members.",
                ],
                "constraints": [
                    "Password reset links expire after 30 minutes.",
                    "Session lifetime is limited to 8 hours with idle timeout after 30 minutes.",
                ],
                "acceptance_criteria": [
                    "Service accounts require client credentials and cannot use interactive login.",
                    "Anonymous access is allowed for public product pages.",
                ],
                "metadata": {
                    "security": {
                        "device_trust": "Trusted devices require step-up auth every 30 days."
                    }
                },
            },
        )
    )

    assert isinstance(result, SourceAuthenticationRequirementsReport)
    assert result.source_id == "auth-source"
    assert all(isinstance(record, SourceAuthenticationRequirement) for record in result.records)

    by_surface = {record.auth_surface: record for record in result.records}
    assert {
        "sso",
        "saml",
        "oidc",
        "mfa",
        "passkey",
        "password_reset",
        "session",
        "device_trust",
        "service_account",
        "anonymous_access",
    } <= set(by_surface)
    assert by_surface["saml"].requirement_type == "sso"
    assert by_surface["saml"].actor == "enterprise admins"
    assert by_surface["mfa"].actor == "customers"
    assert by_surface["device_trust"].requirement_type == "session"
    assert by_surface["service_account"].confidence == "high"
    assert any("source_payload.metadata.security.device_trust" in record.evidence for record in result.records)
    assert result.summary["type_counts"]["sso"] >= 2
    assert result.summary["surface_counts"]["anonymous_access"] == 1


def test_models_objects_and_aliases_are_supported_without_source_mutation():
    source = _source_brief(
        source_id="auth-model",
        summary="Users must login with MFA.",
        source_payload={"requirements": ["Admins require password reset recovery controls."]},
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=["Enterprise customers must use SSO."],
            definition_of_done=["Session timeout behavior is documented."],
        )
    )
    object_result = build_source_authentication_requirements(
        SimpleNamespace(id="object-auth", summary="Guests can use anonymous access.")
    )

    mapping_result = build_source_authentication_requirements(source)
    model_result = summarize_source_authentication_requirements(model)
    generated = generate_source_authentication_requirements(model)
    derived = derive_source_authentication_requirements(model)
    extracted = extract_source_authentication_requirements(model)
    implementation_result = build_source_authentication_requirements(implementation)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert generated.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert extracted == model_result.requirements
    assert implementation_result.source_id == "impl-auth"
    assert {record.auth_surface for record in implementation_result.records} >= {"sso", "session"}
    assert object_result.source_id == "object-auth"
    assert object_result.records[0].auth_surface == "anonymous_access"


def test_list_level_extraction_keeps_record_source_ids_and_deduplicates_evidence():
    result = build_source_authentication_requirements(
        [
            _source_brief(
                source_id="source-a",
                summary="Admins must use SAML SSO. Admins must use SAML SSO.",
                source_payload={
                    "requirements": [
                        "Admins must use SAML SSO.",
                        "admins shall use saml sso",
                    ]
                },
            ),
            _source_brief(
                source_id="source-b",
                summary="Service accounts require API keys.",
            ),
        ]
    )

    saml_records = [record for record in result.records if record.source_id == "source-a" and record.auth_surface == "saml"]
    assert result.source_id is None
    assert result.summary["source_count"] == 2
    assert result.summary["source_ids"] == ["source-a", "source-b"]
    assert len(saml_records) == 1
    assert saml_records[0].actor == "admins"
    assert any(record.source_id == "source-b" and record.auth_surface == "service_account" for record in result.records)


def test_serialization_markdown_empty_and_invalid_inputs_are_stable():
    empty = build_source_authentication_requirements(
        _source_brief(title="Copy", summary="Improve dashboard copy.", source_payload={})
    )
    invalid = extract_source_authentication_requirements(object())
    text = build_source_authentication_requirements(
        "Members should use passkeys for login. Public docs allow anonymous access."
    )
    payload = source_authentication_requirements_to_dict(text)
    markdown = source_authentication_requirements_to_markdown(text)

    assert empty.records == ()
    assert empty.summary["requirement_count"] == 0
    assert "No authentication requirements were found" in empty.to_markdown()
    assert invalid == ()
    assert json.loads(json.dumps(payload)) == payload
    assert text.to_dicts() == payload["requirements"]
    assert source_authentication_requirements_to_dicts(text) == payload["requirements"]
    assert source_authentication_requirements_to_dicts(text.records) == payload["records"]
    assert list(payload) == ["source_id", "requirements", "records", "summary"]
    assert list(payload["requirements"][0]) == [
        "source_id",
        "auth_surface",
        "requirement_type",
        "actor",
        "evidence",
        "confidence",
    ]
    assert markdown.startswith("# Source Authentication Requirements")
    assert "| Source | Surface | Type | Actor | Confidence | Evidence |" in markdown


def _source_brief(
    *,
    source_id="auth-source",
    title="Authentication requirements",
    domain="identity",
    summary="General authentication requirements.",
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
        "id": "impl-auth",
        "source_brief_id": "source-auth",
        "title": "Authentication rollout",
        "domain": "identity",
        "target_user": "Admins",
        "buyer": "Security",
        "workflow_context": "Authentication controls",
        "problem_statement": "Security needs enforceable authentication requirements.",
        "mvp_goal": "Ship auth requirement extraction.",
        "product_surface": "Auth settings",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run authentication requirement tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
    }
