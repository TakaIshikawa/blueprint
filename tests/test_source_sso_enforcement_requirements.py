import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_sso_enforcement_requirements import (
    SourceSSOEnforcementRequirement,
    SourceSSOEnforcementRequirementsReport,
    build_source_sso_enforcement_requirements,
    derive_source_sso_enforcement_requirements,
    extract_source_sso_enforcement_requirements,
    generate_source_sso_enforcement_requirements,
    source_sso_enforcement_requirements_to_dict,
    source_sso_enforcement_requirements_to_dicts,
    source_sso_enforcement_requirements_to_markdown,
    summarize_source_sso_enforcement_requirements,
)


def test_nested_source_payload_extracts_sso_enforcement_categories_in_order():
    result = build_source_sso_enforcement_requirements(
        _source_brief(
            source_payload={
                "sso_enforcement": {
                    "providers": "Identity provider support must include Okta and SAML metadata setup.",
                    "scope": "SSO enforcement requires all managed users to use SSO; password login is disabled.",
                    "fallback": "Break-glass access must allow one emergency admin with support override approval.",
                    "domains": "Domain rules must require verified domains and domain claim conflict handling.",
                    "sessions": "Session behavior requires reauthentication after 8 hours and single logout.",
                    "audit": "Audit logging records security events, bypass events, actor, and timestamp.",
                    "rollout": "Rollout constraints require a phased rollout behind a feature flag with a 14 day grace period.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceSSOEnforcementRequirementsReport)
    assert all(isinstance(record, SourceSSOEnforcementRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "identity_providers",
        "enforcement_scope",
        "fallback_access",
        "domain_rules",
        "session_behavior",
        "audit_logging",
        "rollout_constraints",
    ]
    assert by_category["identity_providers"].value == "okta"
    assert by_category["enforcement_scope"].value == "all managed users"
    assert by_category["fallback_access"].value == "break-glass"
    assert by_category["domain_rules"].value == "verified domains"
    assert by_category["session_behavior"].value == "8 hours"
    assert by_category["audit_logging"].source_field == "source_payload.sso_enforcement.audit"
    assert by_category["rollout_constraints"].suggested_owners == ("product", "customer_success")
    assert result.summary["requirement_count"] == 7
    assert result.summary["category_counts"]["fallback_access"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_missing_sso_enforcement_signals_return_stable_defaults():
    result = build_source_sso_enforcement_requirements(
        _source_brief(
            title="Login copy",
            summary="Improve the authentication page labels and remember me copy.",
            source_payload={"requirements": ["Show a profile menu after users sign in."]},
        )
    )
    invalid = build_source_sso_enforcement_requirements(object())
    negated = build_source_sso_enforcement_requirements(
        _source_brief(summary="SSO enforcement is out of scope and no break-glass workflow is required.")
    )

    assert result.brief_id == "sb-sso-enforcement"
    assert result.requirements == ()
    assert result.records == ()
    assert result.findings == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "identity_providers": 0,
            "enforcement_scope": 0,
            "fallback_access": 0,
            "domain_rules": 0,
            "session_behavior": 0,
            "audit_logging": 0,
            "rollout_constraints": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_sso_enforcement_language",
    }
    assert invalid.brief_id is None
    assert invalid.requirements == ()
    assert negated.requirements == ()


def test_mixed_case_free_form_text_and_implementation_brief_are_supported_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "Require SSO for enterprise tenants and managed users.",
            "Password fallback must be limited to a break glass admin.",
        ],
        definition_of_done=[
            "Audit log records SSO enforcement events.",
            "Rollout uses a pilot cohort and customer communications.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    text_result = build_source_sso_enforcement_requirements(
        """
# EnFoRcEd SSO

- ENTRA ID is the launch IdP.
- Domain Claim rules must match @example.com and verified domains.
- Sessions must ReAuth after 12 HOURS.
"""
    )
    implementation_result = generate_source_sso_enforcement_requirements(implementation)

    assert implementation_payload == original
    assert [record.category for record in text_result.records] == [
        "identity_providers",
        "domain_rules",
        "session_behavior",
    ]
    assert text_result.records[0].value == "entra id"
    assert text_result.records[1].value == "@example.com"
    assert text_result.records[2].value == "12 hours"
    assert {
        "enforcement_scope",
        "fallback_access",
        "audit_logging",
        "rollout_constraints",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-sso-enforcement"
    assert implementation_result.title == "SSO enforcement implementation"


def test_sourcebrief_serialization_markdown_aliases_and_object_input_are_stable():
    source = _source_brief(
        source_id="sso-enforcement-model",
        summary="SSO enforcement requires Okta for all employees.",
        source_payload={
            "requirements": [
                "Break-glass access must be audited.",
                "Domain rules require verified domains.",
            ]
        },
    )
    model = SourceBrief.model_validate(source)
    object_result = build_source_sso_enforcement_requirements(
        SimpleNamespace(
            id="object-sso-enforcement",
            summary="Mandatory SSO for admins needs password fallback and audit log evidence.",
        )
    )

    result = build_source_sso_enforcement_requirements(model)
    extracted = extract_source_sso_enforcement_requirements(model)
    derived = derive_source_sso_enforcement_requirements(model)
    payload = source_sso_enforcement_requirements_to_dict(result)
    markdown = source_sso_enforcement_requirements_to_markdown(result)

    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_sso_enforcement_requirements(result) == result.summary
    assert source_sso_enforcement_requirements_to_dicts(result) == payload["requirements"]
    assert source_sso_enforcement_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.requirements
    assert result.findings == result.requirements
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "suggested_owners",
        "suggested_plan_impacts",
    ]
    assert "fallback_access" in {record.category for record in object_result.records}
    assert markdown.startswith("# Source SSO Enforcement Requirements Report: sso-enforcement-model")
    assert "| Category | Value | Confidence | Source Field | Owners | Evidence | Suggested Plan Impacts |" in markdown


def _source_brief(
    *,
    source_id="sb-sso-enforcement",
    title="SSO enforcement requirements",
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


def _implementation_brief(**overrides):
    payload = {
        "id": "implementation-sso-enforcement",
        "source_brief_id": "sb-sso-enforcement",
        "title": "SSO enforcement implementation",
        "domain": "identity",
        "target_user": "enterprise admin",
        "buyer": "security operations",
        "workflow_context": "Enterprise authentication controls",
        "problem_statement": "Implement source-backed SSO enforcement controls.",
        "mvp_goal": "Ship SSO enforcement planning support.",
        "product_surface": "admin identity settings",
        "scope": [],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run SSO enforcement extractor tests.",
        "definition_of_done": [],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
    payload.update(overrides)
    return payload
