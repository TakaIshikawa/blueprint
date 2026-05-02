import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_identity_provider_requirements import (
    SourceIdentityProviderRequirement,
    SourceIdentityProviderRequirementsReport,
    build_source_identity_provider_requirements,
    extract_source_identity_provider_requirements,
    generate_source_identity_provider_requirements,
    source_identity_provider_requirements_to_dict,
    source_identity_provider_requirements_to_dicts,
    source_identity_provider_requirements_to_markdown,
)


def test_provider_hints_and_identity_areas_are_extracted_from_source_brief():
    result = build_source_identity_provider_requirements(
        _source_brief(
            summary=(
                "Enterprise admins require SSO with SAML for Okta and Azure AD before launch. "
                "OIDC must support Auth0 and Google Workspace customers."
            ),
            source_payload={
                "requirements": [
                    "SCIM 2.0 directory sync must provision users and groups.",
                    "Group claims map IdP groups to application roles.",
                    "Just-in-time provisioning creates users on first login.",
                    "SCIM deprovisioning must revoke access for terminated employees.",
                ]
            },
        )
    )

    assert isinstance(result, SourceIdentityProviderRequirementsReport)
    assert result.source_brief_id == "sb-idp"
    assert all(isinstance(record, SourceIdentityProviderRequirement) for record in result.records)
    assert {"Okta", "Azure AD", "Auth0", "Google Workspace"} <= {
        record.provider_hint for record in result.records
    }
    assert {
        "sso",
        "saml",
        "oidc",
        "scim",
        "identity_provider",
        "group_claim",
        "role_mapping",
        "jit_provisioning",
        "deprovisioning",
    } <= {record.identity_area for record in result.records}
    assert result.summary["area_counts"]["saml"] == 2
    assert result.summary["provider_counts"]["Okta"] >= 1
    assert result.summary["provider_counts"]["Google Workspace"] >= 1
    assert any(record.affected_audience == "enterprise admins" for record in result.records)
    assert any(record.risk_level == "high" for record in result.records if record.identity_area == "scim")


def test_metadata_traversal_and_deduped_evidence_are_stable():
    result = build_source_identity_provider_requirements(
        {
            "id": "metadata-idp",
            "title": "Identity | setup",
            "summary": "Admins require SAML SSO through Okta.",
            "source_payload": {
                "metadata": {
                    "identity": {
                        "saml": "Admins require SAML SSO through Okta.",
                        "claims": [
                            "memberOf group claim maps to application roles.",
                            "memberOf group claim maps to application roles.",
                        ],
                    },
                    "lifecycle": {"deprovisioning": "Disable users when SCIM deprovisioning arrives."},
                }
            },
        }
    )
    repeat = build_source_identity_provider_requirements(
        {
            "id": "metadata-idp",
            "title": "Identity | setup",
            "summary": "Admins require SAML SSO through Okta.",
            "source_payload": {
                "metadata": {
                    "identity": {
                        "saml": "Admins require SAML SSO through Okta.",
                        "claims": [
                            "memberOf group claim maps to application roles.",
                            "memberOf group claim maps to application roles.",
                        ],
                    },
                    "lifecycle": {"deprovisioning": "Disable users when SCIM deprovisioning arrives."},
                }
            },
        }
    )

    assert result.to_dict() == repeat.to_dict()
    assert any(
        "source_payload.metadata.identity.saml" in evidence
        for record in result.records
        for evidence in record.evidence
    )
    assert any(
        "source_payload.metadata.lifecycle.deprovisioning" in evidence
        for record in result.records
        for evidence in record.evidence
    )
    for record in result.records:
        assert len(record.evidence) == len({item.casefold() for item in record.evidence})


def test_sourcebrief_model_serialization_markdown_and_aliases_do_not_mutate_input():
    source = _source_brief(
        source_id="model-idp",
        title="Enterprise | identity",
        summary="Enterprise customers require SAML SSO with Azure AD.",
        source_payload={
            "requirements": ["OIDC must support Auth0 issuer discovery."],
            "metadata": {"scim": "SCIM deprovisioning revokes access."},
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_identity_provider_requirements(source)
    model_result = generate_source_identity_provider_requirements(model)
    extracted = extract_source_identity_provider_requirements(model)
    payload = source_identity_provider_requirements_to_dict(model_result)
    markdown = source_identity_provider_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_identity_provider_requirements_to_dict(mapping_result)
    assert json.loads(json.dumps(payload)) == payload
    assert extracted == model_result.requirements
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_identity_provider_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_identity_provider_requirements_to_dicts(model_result.records) == payload["records"]
    assert list(payload) == ["source_brief_id", "title", "summary", "requirements", "records"]
    assert list(payload["requirements"][0]) == [
        "identity_area",
        "provider_hint",
        "affected_audience",
        "evidence",
        "acceptance_criteria_hints",
        "risk_level",
    ]
    assert markdown == model_result.to_markdown()
    assert markdown.startswith("# Source Identity Provider Requirements Report: model-idp")
    assert "Enterprise \\| identity" not in markdown
    assert "| Area | Provider | Audience | Risk | Evidence | Acceptance Criteria Hints |" in markdown
    assert "Azure AD" in markdown


def test_empty_invalid_and_plain_text_inputs_are_handled():
    empty = build_source_identity_provider_requirements(
        _source_brief(
            title="Dashboard copy",
            summary="Update dashboard copy.",
            source_payload={"body": "No authentication changes."},
        )
    )
    invalid = build_source_identity_provider_requirements(object())
    text = build_source_identity_provider_requirements("Okta SSO must support SAML group claims.")

    assert empty.source_brief_id == "sb-idp"
    assert empty.records == ()
    assert empty.summary == {
        "requirement_count": 0,
        "area_counts": {
            "sso": 0,
            "saml": 0,
            "oidc": 0,
            "scim": 0,
            "identity_provider": 0,
            "group_claim": 0,
            "role_mapping": 0,
            "jit_provisioning": 0,
            "deprovisioning": 0,
        },
        "provider_counts": {},
        "audience_counts": {},
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "identity_areas": [],
        "provider_hints": [],
    }
    assert "No source identity provider requirements were found" in empty.to_markdown()
    assert invalid.source_brief_id is None
    assert invalid.records == ()
    assert text.source_brief_id is None
    assert {"Okta"} <= {record.provider_hint for record in text.records}


def _source_brief(
    *,
    source_id="sb-idp",
    title="Identity provider requirements",
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
