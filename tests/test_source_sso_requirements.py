import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_sso_requirements import (
    SourceSSORequirement,
    SourceSSORequirementsReport,
    build_source_sso_requirements,
    extract_source_sso_requirements,
    source_sso_requirements_to_dict,
    source_sso_requirements_to_dicts,
)


def test_detects_sso_identity_provider_requirements_across_brief_fields():
    result = build_source_sso_requirements(
        _source_brief(
            summary=(
                "Enterprise admins need SSO with SAML before launch, including Okta "
                "and Azure AD setup."
            ),
            source_payload={
                "requirements": [
                    "OIDC must support Google Workspace customers with issuer discovery.",
                    "SCIM 2.0 directory sync should provision and deprovision users.",
                    "JIT provisioning creates users on first login after domain verification.",
                    "Group mapping maps IdP groups to app roles.",
                ],
                "risks": [
                    "Logout and session lifetime requirements are unclear for federated login."
                ],
                "metadata": {"idp": "Identity provider setup must include certificate rotation."},
            },
        )
    )

    assert isinstance(result, SourceSSORequirementsReport)
    assert result.source_brief_id == "sb-sso"
    assert all(isinstance(record, SourceSSORequirement) for record in result.records)
    by_type = {record.requirement_type: record for record in result.requirements}

    assert by_type["sso"].confidence == "high"
    assert by_type["saml"].confidence == "high"
    assert by_type["oidc"].confidence == "high"
    assert by_type["scim"].confidence == "high"
    assert by_type["jit_provisioning"].confidence == "high"
    assert by_type["domain_verification"].confidence == "high"
    assert by_type["group_mapping"].confidence == "medium"
    assert by_type["idp"].confidence == "high"
    assert by_type["logout"].confidence == "medium"
    assert by_type["session_lifetime"].confidence == "high"
    assert "SAML" in by_type["saml"].matched_terms
    assert any("Okta" in term for term in by_type["idp"].matched_terms)
    assert any(
        "source_payload.requirements[2]" in item
        for item in by_type["jit_provisioning"].evidence
    )
    assert any(
        "maximum and idle session" in item
        for item in by_type["session_lifetime"].recommended_questions
    )
    assert result.summary["requirement_count"] == len(result.requirements)
    assert result.summary["type_counts"]["scim"] == 1


def test_duplicate_signals_are_merged_with_deduplicated_terms_and_evidence():
    result = build_source_sso_requirements(
        {
            "id": "dupe-sso",
            "summary": "Admins require SAML SSO. Admins require SAML SSO.",
            "source_payload": {
                "requirements": [
                    "SAML SSO must work for Okta.",
                    "saml sso must work for Okta.",
                ],
                "metadata": {"saml": "SAML SSO must work for Okta."},
            },
        }
    )
    saml = next(record for record in result.requirements if record.requirement_type == "saml")

    assert saml.evidence == tuple(sorted(set(saml.evidence), key=lambda item: item.casefold()))
    assert saml.matched_terms == tuple(
        sorted(set(saml.matched_terms), key=lambda item: item.casefold())
    )
    assert len(saml.evidence) == len(set(saml.evidence))


def test_mapping_and_sourcebrief_inputs_match_and_serialize_to_json_compatible_payload():
    source = _source_brief(
        source_id="sso-model",
        summary="Customers require SSO through SAML and Okta.",
        source_payload={
            "requirements": ["SCIM provisioning must sync users and groups."],
            "metadata": {"session_lifetime": "SSO sessions expire after 8 hours."},
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_sso_requirements(source)
    model_result = extract_source_sso_requirements(model)
    payload = source_sso_requirements_to_dict(model_result)

    assert source == original
    assert payload == source_sso_requirements_to_dict(mapping_result)
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_sso_requirements_to_dicts(model_result.records) == payload["records"]
    assert list(payload) == ["source_brief_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "requirement_type",
        "matched_terms",
        "evidence",
        "confidence",
        "recommended_questions",
    ]


def test_empty_and_invalid_inputs_are_handled_gracefully():
    empty = build_source_sso_requirements(
        _source_brief(
            title="Dashboard copy",
            summary="Improve dashboard copy.",
            source_payload={"body": "No authentication changes."},
        )
    )
    invalid = build_source_sso_requirements("not a source brief")

    assert empty.source_brief_id == "sb-sso"
    assert empty.requirements == ()
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "requirement_count": 0,
        "type_counts": {
            "sso": 0,
            "saml": 0,
            "oidc": 0,
            "scim": 0,
            "idp": 0,
            "jit_provisioning": 0,
            "group_mapping": 0,
            "domain_verification": 0,
            "logout": 0,
            "session_lifetime": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "requirement_types": [],
    }
    assert invalid.source_brief_id is None
    assert invalid.requirements == ()


def _source_brief(
    *,
    source_id="sb-sso",
    title="SSO requirements",
    domain="identity",
    summary="General identity requirements.",
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
