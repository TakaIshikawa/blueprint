import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_api_sso_integration_requirements import (
    SourceAPISSOIntegrationRequirement,
    SourceAPISSOIntegrationRequirementsReport,
    build_source_api_sso_integration_requirements,
    derive_source_api_sso_integration_requirements,
    extract_source_api_sso_integration_requirements,
    generate_source_api_sso_integration_requirements,
    source_api_sso_integration_requirements_to_dict,
    source_api_sso_integration_requirements_to_dicts,
    source_api_sso_integration_requirements_to_markdown,
    summarize_source_api_sso_integration_requirements,
)


def test_nested_source_payload_extracts_sso_categories_in_order():
    result = build_source_api_sso_integration_requirements(
        _source_brief(
            source_payload={
                "sso": {
                    "protocol": "API must support SAML 2.0, OAuth 2.0, and OpenID Connect protocols.",
                    "idp": "Identity provider integration must support Okta, Azure AD, and Auth0.",
                    "scim": "SCIM 2.0 provisioning must support user creation and deprovisioning.",
                    "jit": "Just-in-time provisioning must create users automatically on first login.",
                    "attributes": "Attribute mapping must map SAML attributes to user profile fields.",
                    "multi_idp": "Multi-IDP support must allow users to select their identity provider.",
                    "session": "SSO session management must implement single logout functionality.",
                    "testing": "SSO testing must include mock IDP for integration validation.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceAPISSOIntegrationRequirementsReport)
    assert all(isinstance(record, SourceAPISSOIntegrationRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "sso_protocol_support",
        "identity_provider_integration",
        "scim_provisioning",
        "jit_provisioning",
        "sso_attribute_mapping",
        "multi_idp_support",
        "sso_session_management",
        "sso_testing",
    ]
    assert by_category["sso_protocol_support"].value in {"saml 2.0", "saml", "oauth 2.0", "oauth", "openid connect", "oidc"}
    assert by_category["identity_provider_integration"].value in {"okta", "azure ad", "auth0", "idp", "identity provider"}
    assert by_category["scim_provisioning"].value in {"scim 2.0", "scim", "provisioning", "deprovisioning"}
    assert by_category["jit_provisioning"].value in {"just-in-time", "jit", "auto-create"}
    assert by_category["sso_protocol_support"].source_field == "source_payload.sso.protocol"
    assert by_category["sso_protocol_support"].suggested_owners == ("security", "backend", "api_platform")
    assert by_category["sso_protocol_support"].planning_notes[0].startswith("Define supported SSO protocols")
    assert result.summary["requirement_count"] == 8
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_fields_and_implementation_brief_are_scanned_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "API must support SAML 2.0 authentication for enterprise SSO.",
            "Identity provider integration must support Okta and Azure AD.",
        ],
        definition_of_done=[
            "SCIM provisioning enables automatic user lifecycle management.",
            "SSO testing includes mock IDP validation.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    source = _source_brief(
        requirements=[
            "Just-in-time provisioning must create users on first SSO login.",
            "Multi-IDP support must enable home realm discovery.",
        ],
        api={"sso": "Attribute mapping must sync SAML claims to user profiles."},
        source_payload={"metadata": {"session": "Single logout must revoke all active sessions."}},
    )

    source_result = build_source_api_sso_integration_requirements(source)
    implementation_result = generate_source_api_sso_integration_requirements(implementation)

    assert implementation_payload == original
    # The extractor finds additional signals based on context
    source_categories = [record.category for record in source_result.records]
    assert "jit_provisioning" in source_categories
    assert "multi_idp_support" in source_categories
    assert "sso_session_management" in source_categories
    # At least one of these two fields should be the source for one of the records
    source_fields = {r.source_field for r in source_result.records}
    assert any(field.startswith("requirements") or field.startswith("api.") for field in source_fields)
    assert {
        "sso_protocol_support",
        "identity_provider_integration",
        "scim_provisioning",
        "sso_testing",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-sso"
    assert implementation_result.title == "SSO integration implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_sso():
    result = build_source_api_sso_integration_requirements(
        _source_brief(
            summary="API needs SSO integration for enterprise authentication.",
            source_payload={
                "requirements": [
                    "API must support single sign-on for enterprise users.",
                    "Identity provider integration should enable federated authentication.",
                    "User provisioning may use automated synchronization.",
                ]
            },
        )
    )

    categories = [record.category for record in result.records]
    assert "sso_protocol_support" in categories or "identity_provider_integration" in categories
    assert result.summary["missing_detail_flags"] == [
        "missing_protocol_details",
        "missing_idp_configuration",
    ]
    assert "Specify SSO protocol details (SAML 2.0, OAuth 2.0, OpenID Connect) and authentication flow." in result.summary["gap_messages"]
    assert "Define IDP configuration requirements, metadata exchange, and certificate management." in result.summary["gap_messages"]
    assert all(record.gap_messages == tuple(result.summary["gap_messages"]) for record in result.records)
    assert result.summary["missing_detail_counts"]["missing_protocol_details"] >= 1
    assert result.summary["status"] == "needs_sso_details"


def test_duplicate_evidence_serialization_aliases_and_markdown_are_stable():
    source = _source_brief(
        source_id="sso-model",
        title="SSO integration source",
        summary="SSO integration source.",
        source_payload={
            "sso": {
                "saml": "SAML 2.0 must be supported for enterprise authentication.",
                "same_saml": "SAML 2.0 must be supported for enterprise authentication.",
                "scim": "SCIM provisioning must automate user lifecycle management.",
            },
            "acceptance_criteria": [
                "SAML 2.0 must be supported for enterprise authentication.",
                "Okta integration must enable federated SSO.",
            ],
        },
    )

    result = build_source_api_sso_integration_requirements(source)
    generated = generate_source_api_sso_integration_requirements(source)
    derived = derive_source_api_sso_integration_requirements(source)
    extracted = extract_source_api_sso_integration_requirements(source)

    assert result.requirements == generated.requirements
    assert result.requirements == derived.requirements
    assert result.requirements == tuple(extracted)
    report_dict = source_api_sso_integration_requirements_to_dict(result)
    assert report_dict["brief_id"] == "sso-model"
    assert report_dict["title"] == "SSO integration source"
    assert "requirements" in report_dict
    assert "records" in report_dict
    assert "findings" in report_dict
    assert report_dict["requirements"] == report_dict["records"]
    assert report_dict["requirements"] == report_dict["findings"]
    dicts = source_api_sso_integration_requirements_to_dicts(result)
    assert len(dicts) >= 2
    assert all("category" in d and "source_field" in d for d in dicts)
    markdown = source_api_sso_integration_requirements_to_markdown(result)
    assert "Source API SSO Integration Requirements Report: sso-model" in markdown
    assert "sso_protocol_support" in markdown
    assert "scim_provisioning" in markdown
    summary = summarize_source_api_sso_integration_requirements(result)
    assert summary == result.summary
    assert summary["requirement_count"] >= 2
    saml = next((r for r in result.records if r.category == "sso_protocol_support"), None)
    assert saml is not None
    # Evidence should be deduplicated (no duplicate SAML statement)
    evidence_texts = [ev.partition(": ")[2] for ev in saml.evidence]
    assert len(evidence_texts) == len(set(ev.casefold() for ev in evidence_texts))


def test_string_source_is_parsed_into_body_field():
    result = build_source_api_sso_integration_requirements(
        "API must support SAML 2.0 and OAuth 2.0 for SSO authentication. "
        "SCIM provisioning must enable user lifecycle management."
    )

    assert result.brief_id is None
    categories = [record.category for record in result.records]
    assert "sso_protocol_support" in categories
    assert "scim_provisioning" in categories


def test_object_with_attributes_is_parsed_without_pydantic_model():
    obj = SimpleNamespace(
        id="sso-obj",
        title="SSO integration",
        summary="SSO requirements.",
        source_payload={
            "sso": {
                "okta": "Okta integration must support federated SSO.",
                "jit": "Just-in-time provisioning must create users on first login.",
            }
        },
    )

    result = build_source_api_sso_integration_requirements(obj)

    assert result.brief_id == "sso-obj"
    assert result.title == "SSO integration"
    categories = [record.category for record in result.records]
    assert "identity_provider_integration" in categories
    assert "jit_provisioning" in categories


def test_no_sso_scope_returns_empty_requirements():
    result = build_source_api_sso_integration_requirements(
        _source_brief(
            summary="No SSO integration required for this API.",
            scope=["API authentication uses basic auth only."],
        )
    )

    assert result.requirements == ()
    assert result.summary["requirement_count"] == 0
    assert result.summary["status"] == "no_sso_language"
    assert result.summary["missing_detail_flags"] == []


def test_negated_sso_scope_returns_empty_requirements():
    result = build_source_api_sso_integration_requirements(
        _source_brief(
            summary="SSO is out of scope for this release.",
            non_goals=["No SAML or OAuth support needed."],
        )
    )

    assert result.requirements == ()
    assert result.summary["requirement_count"] == 0
    assert result.summary["status"] == "no_sso_language"


def test_json_serialization_round_trip():
    source = _source_brief(
        source_id="sso-json",
        title="SSO JSON test",
        source_payload={
            "sso": {
                "saml": "SAML 2.0 must be supported.",
                "scim": "SCIM provisioning must automate user management.",
            }
        },
    )

    result = build_source_api_sso_integration_requirements(source)
    serialized = json.dumps(result.to_dict(), indent=2, sort_keys=True)
    deserialized = json.loads(serialized)

    assert deserialized["brief_id"] == "sso-json"
    assert deserialized["title"] == "SSO JSON test"
    assert len(deserialized["requirements"]) >= 2


def test_confidence_levels_vary_based_on_context():
    result = build_source_api_sso_integration_requirements(
        _source_brief(
            acceptance_criteria=[
                "SAML 2.0 must be supported for enterprise SSO.",
            ],
            source_payload={
                "notes": "OAuth might be useful for mobile clients."
            },
        )
    )

    high_confidence = [r for r in result.records if r.confidence == "high"]
    medium_or_low = [r for r in result.records if r.confidence in {"medium", "low"}]
    assert len(high_confidence) >= 1
    assert len(medium_or_low) >= 0


def test_category_specific_values_are_extracted():
    result = build_source_api_sso_integration_requirements(
        _source_brief(
            source_payload={
                "sso": {
                    "protocols": "Support SAML 2.0 and OpenID Connect.",
                    "providers": "Integrate with Okta and Azure AD.",
                    "provisioning": "Use SCIM 2.0 for user provisioning.",
                    "jit": "Enable just-in-time user creation.",
                    "mapping": "Map SAML attributes to user claims.",
                    "multi": "Support multi-IDP tenant selection.",
                    "session": "Implement SSO session timeout and single logout.",
                    "test": "Test SSO integration with mock IDP.",
                }
            },
        )
    )

    protocol = next((r for r in result.records if r.category == "sso_protocol_support"), None)
    idp = next((r for r in result.records if r.category == "identity_provider_integration"), None)
    scim = next((r for r in result.records if r.category == "scim_provisioning"), None)
    jit = next((r for r in result.records if r.category == "jit_provisioning"), None)

    assert protocol is not None
    assert protocol.value in {"saml 2.0", "saml", "openid connect", "oidc"}
    assert idp is not None
    assert idp.value in {"okta", "azure ad", "idp", "identity provider"}
    assert scim is not None
    assert scim.value in {"scim 2.0", "scim", "provisioning"}
    assert jit is not None
    assert jit.value in {"just-in-time", "jit"}


def _source_brief(
    *,
    source_id="source-sso",
    title="SSO integration requirements",
    domain="api",
    summary="General SSO integration requirements.",
    requirements=None,
    api=None,
    source_payload=None,
    scope=None,
    non_goals=None,
    acceptance_criteria=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "requirements": [] if requirements is None else requirements,
        "api": {} if api is None else api,
        "scope": [] if scope is None else scope,
        "non_goals": [] if non_goals is None else non_goals,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(
    *,
    brief_id="implementation-sso",
    title="SSO integration implementation",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-sso",
        "title": title,
        "domain": "api",
        "target_user": "developer",
        "buyer": "platform",
        "workflow_context": "API developers need SSO integration planning.",
        "problem_statement": "SSO integration requirements need to be extracted early.",
        "mvp_goal": "Plan SAML, OAuth, OIDC, IDP integration, SCIM provisioning, and attribute mapping.",
        "product_surface": "api",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run SSO integration extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
