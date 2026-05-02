import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_saml_scim_requirements import (
    SourceSAMLSCIMRequirement,
    SourceSAMLSCIMRequirementsReport,
    build_source_saml_scim_requirements,
    derive_source_saml_scim_requirements,
    extract_source_saml_scim_requirements,
    generate_source_saml_scim_requirements,
    source_saml_scim_requirements_to_dict,
    source_saml_scim_requirements_to_dicts,
    source_saml_scim_requirements_to_markdown,
    summarize_source_saml_scim_requirements,
)


def test_extracts_saml_scim_identity_setup_requirements_with_evidence():
    result = build_source_saml_scim_requirements(
        _source_brief(
            summary=(
                "Enterprise launch requires SAML SSO with IdP-initiated and "
                "SP-initiated login before launch."
            ),
            source_payload={
                "requirements": [
                    "Admins must upload SAML metadata XML and configure ACS URL plus Entity ID.",
                    "Signing certificate rotation must be documented for each customer.",
                    "Just-in-Time provisioning creates users on first login.",
                    "SCIM 2.0 must provision users and SCIM groups.",
                    "Deprovisioning must suspend users and revoke access.",
                    "Group mapping maps IdP groups to app roles.",
                    "Tenant-specific identity settings store per-tenant SSO and SCIM tokens.",
                ],
            },
        )
    )

    assert isinstance(result, SourceSAMLSCIMRequirementsReport)
    assert result.source_brief_id == "saml-scim-source"
    assert all(isinstance(record, SourceSAMLSCIMRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "saml_sso",
        "saml_metadata",
        "idp_initiated_login",
        "sp_initiated_login",
        "acs_url",
        "entity_id",
        "certificate_rotation",
        "jit_provisioning",
        "scim_users",
        "scim_groups",
        "deprovisioning",
        "group_mapping",
        "tenant_identity_settings",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["saml_sso"].confidence == "high"
    assert by_type["scim_users"].confidence == "high"
    assert by_type["deprovisioning"].confidence == "high"
    assert by_type["group_mapping"].confidence == "high"
    assert "SAML metadata XML" in by_type["saml_metadata"].requirement_text
    assert by_type["acs_url"].source_field == "source_payload.requirements[0]"
    assert any("Entity ID" in item for item in by_type["entity_id"].evidence)
    assert "access revocation" in by_type["deprovisioning"].planning_note
    assert result.summary["requirement_count"] == 13
    assert result.summary["type_counts"]["scim_groups"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_out_of_scope_identity_provisioning_suppresses_false_positives():
    result = build_source_saml_scim_requirements(
        _source_brief(
            title="Admin copy update",
            summary="Identity provisioning is out of scope for this release.",
            source_payload={
                "requirements": [
                    "Keep the existing SAML and SCIM help text unchanged.",
                    "No SCIM provisioning or group mapping work is required.",
                ],
                "metadata": {"note": "Mention SAML only in migration copy."},
            },
        )
    )
    repeat = build_source_saml_scim_requirements(
        _source_brief(
            title="Admin copy update",
            summary="Identity provisioning is out of scope for this release.",
            source_payload={
                "requirements": [
                    "Keep the existing SAML and SCIM help text unchanged.",
                    "No SCIM provisioning or group mapping work is required.",
                ],
                "metadata": {"note": "Mention SAML only in migration copy."},
            },
        )
    )

    assert result.to_dict() == repeat.to_dict()
    assert result.requirements == ()
    assert result.records == ()
    assert result.findings == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "type_counts": {
            "saml_sso": 0,
            "saml_metadata": 0,
            "idp_initiated_login": 0,
            "sp_initiated_login": 0,
            "acs_url": 0,
            "entity_id": 0,
            "certificate_rotation": 0,
            "jit_provisioning": 0,
            "scim_users": 0,
            "scim_groups": 0,
            "deprovisioning": 0,
            "group_mapping": 0,
            "tenant_identity_settings": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "requirement_types": [],
        "status": "no_saml_scim_language",
    }
    assert "No source SAML/SCIM requirements were inferred" in result.to_markdown()


def test_implementation_brief_object_and_alias_serialization_are_stable():
    source = _source_brief(
        source_id="saml-scim-model",
        summary="SAML SSO must support SP-initiated login.",
        source_payload={
            "identity": {
                "scim_users": "SCIM user provisioning must sync active and suspended users.",
                "group_mapping": "Role mapping must use the groups claim.",
            }
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            data_requirements=(
                "Tenant-specific identity settings must store the ACS URL and Entity ID."
            ),
            integration_points=[
                "SCIM groups must sync for directory provisioning.",
                "Deprovisioning must revoke application access.",
            ],
        )
    )
    obj = SimpleNamespace(
        id="object-identity",
        requirements=["Certificate rotation must be supported for SAML signing certificates."],
    )

    mapping_result = build_source_saml_scim_requirements(source)
    model_result = generate_source_saml_scim_requirements(model)
    derived = derive_source_saml_scim_requirements(model)
    extracted = extract_source_saml_scim_requirements(model)
    iterable_result = build_source_saml_scim_requirements([model, brief, obj])
    payload = source_saml_scim_requirements_to_dict(model_result)
    markdown = source_saml_scim_requirements_to_markdown(model_result)
    invalid = build_source_saml_scim_requirements(42)

    assert source == original
    assert payload == source_saml_scim_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert extracted.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert source_saml_scim_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_saml_scim_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_saml_scim_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_brief_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "requirement_type",
        "requirement_text",
        "source_field",
        "evidence",
        "confidence",
        "planning_note",
    ]
    assert iterable_result.source_brief_id is None
    assert iterable_result.summary["source_count"] == 3
    assert "scim_groups" in iterable_result.summary["requirement_types"]
    assert "certificate_rotation" in iterable_result.summary["requirement_types"]
    assert invalid.records == ()
    assert markdown == model_result.to_markdown()
    assert markdown.startswith("# Source SAML SCIM Requirements Report: saml-scim-model")


def _source_brief(
    *,
    source_id="saml-scim-source",
    title="SAML and SCIM requirements",
    domain="identity",
    summary="General identity setup requirements.",
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


def _implementation_brief(*, data_requirements=None, integration_points=None):
    return {
        "id": "impl-identity",
        "source_brief_id": "source-identity",
        "title": "Enterprise identity rollout",
        "domain": "identity",
        "target_user": "enterprise admins",
        "buyer": None,
        "workflow_context": "Enterprise setup requires identity-provider tasks before launch.",
        "problem_statement": "Identity provider setup requirements need planning coverage.",
        "mvp_goal": "Plan SAML and SCIM setup.",
        "product_surface": "admin identity",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": data_requirements,
        "integration_points": [] if integration_points is None else integration_points,
        "risks": [],
        "validation_plan": "Review generated plan for identity setup coverage.",
        "definition_of_done": ["SAML and SCIM requirements are represented."],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
