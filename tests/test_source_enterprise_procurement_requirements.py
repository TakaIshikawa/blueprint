import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_enterprise_procurement_requirements import (
    SourceEnterpriseProcurementGap,
    SourceEnterpriseProcurementRequirement,
    SourceEnterpriseProcurementRequirementsReport,
    build_source_enterprise_procurement_requirements,
    derive_source_enterprise_procurement_requirements,
    extract_source_enterprise_procurement_requirements,
    generate_source_enterprise_procurement_requirements,
    source_enterprise_procurement_requirements_to_dict,
    source_enterprise_procurement_requirements_to_dicts,
    source_enterprise_procurement_requirements_to_markdown,
    summarize_source_enterprise_procurement_requirements,
)


def test_nested_enterprise_payload_extracts_procurement_categories_in_order():
    result = build_source_enterprise_procurement_requirements(
        _source_brief(
            source_payload={
                "enterprise_procurement": {
                    "security": "Security review must complete SOC 2 packet approval by 2026-06-01; owner: security.",
                    "legal": "Legal review requires MSA redlines approved by legal before go-live; DRI: legal.",
                    "po": "Purchase order must be issued with PO number by launch; owner: finance ops.",
                    "vendor": "Vendor onboarding requires supplier portal vendor id approved before launch; owner: procurement.",
                    "identity": "SSO and SCIM prerequisites require Okta SAML approval by customer IT before go-live; owner: identity.",
                    "privacy": "Data processing agreement must be signed with subprocessor approval by 2026-05-20; owner: privacy.",
                    "sla": "SLA requirements include 99.9 uptime and support response sign-off by launch; owner: support ops.",
                    "timeline": "Procurement timeline has a 30 days lead time and is the critical path for go-live.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceEnterpriseProcurementRequirementsReport)
    assert all(isinstance(record, SourceEnterpriseProcurementRequirement) for record in result.records)
    assert result.gaps == ()
    assert [record.category for record in result.records] == [
        "security_review",
        "legal_review",
        "purchase_order",
        "vendor_onboarding",
        "sso_scim_prerequisites",
        "data_processing_agreement",
        "sla_requirements",
        "procurement_timeline",
    ]
    assert by_category["security_review"].value == "soc 2"
    assert by_category["purchase_order"].value == "po number"
    assert by_category["sso_scim_prerequisites"].value == "sso, scim, okta"
    assert by_category["data_processing_agreement"].source_field == "source_payload.enterprise_procurement.privacy"
    assert by_category["sla_requirements"].suggested_owners == ("support_ops", "legal")
    assert result.summary["requirement_count"] == 8
    assert result.summary["evidence_gap_count"] == 0
    assert result.summary["status"] == "ready_for_planning"


def test_procurement_gates_flag_missing_owner_due_date_and_approval_evidence():
    result = build_source_enterprise_procurement_requirements(
        _source_brief(
            summary="Enterprise launch procurement requirements.",
            requirements=[
                "Security review is required before launch.",
                "Legal review requires contract review by 2026-06-15.",
                "Vendor onboarding must be approved by procurement.",
            ],
        )
    )

    gaps = {(gap.category, gap.missing_field) for gap in result.evidence_gaps}

    assert [record.category for record in result.records] == [
        "security_review",
        "legal_review",
        "vendor_onboarding",
    ]
    assert all(isinstance(gap, SourceEnterpriseProcurementGap) for gap in result.evidence_gaps)
    assert ("security_review", "owner") in gaps
    assert ("security_review", "approval_evidence") in gaps
    assert ("legal_review", "owner") in gaps
    assert ("legal_review", "approval_evidence") in gaps
    assert ("vendor_onboarding", "due_date") in gaps
    assert ("vendor_onboarding", "approval_evidence") not in gaps
    assert result.summary["status"] == "needs_procurement_approval_evidence"
    assert "security_review:owner" in result.summary["evidence_gaps"]


def test_plain_text_and_implementation_brief_capture_timeline_risks_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "Purchase order must be issued by go-live; owner: finance.",
            "Data processing agreement requires signed approval by 2026-05-30; owner: legal.",
        ],
        risks=[
            "Procurement timeline risk: vendor onboarding has a 45 days lead time and blocks launch.",
            "SLA requirements need support response sign-off by launch; owner: support.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    text_result = build_source_enterprise_procurement_requirements(
        """
# Enterprise procurement

- SSO and SCIM prerequisites require SAML approval by customer IT before launch; owner: identity.
- Procurement timeline has a 6 weeks approval window and may delay go-live.
"""
    )
    implementation_result = generate_source_enterprise_procurement_requirements(implementation)

    assert implementation_payload == original
    assert [record.category for record in text_result.records] == [
        "sso_scim_prerequisites",
        "procurement_timeline",
    ]
    assert text_result.records[0].source_field == "body"
    assert {
        "purchase_order",
        "vendor_onboarding",
        "data_processing_agreement",
        "sla_requirements",
        "procurement_timeline",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-procurement"


def test_serialization_markdown_aliases_and_helpers_are_stable():
    source = _source_brief(
        source_id="procurement-model",
        title="Enterprise procurement source",
        source_payload={
            "requirements": [
                "Security review must complete security questionnaire approval by 2026-06-01; owner: security.",
                "Security review must complete security questionnaire approval by 2026-06-01; owner: security.",
                "Legal review requires MSA approval by launch; owner: legal.",
                "Purchase order requires PO number issued by go-live; owner: finance.",
            ]
        },
    )
    model = SourceBrief.model_validate(
        {
            key: value
            for key, value in source.items()
            if key not in {"description", "requirements", "acceptance_criteria"}
        }
    )

    result = build_source_enterprise_procurement_requirements(model)
    extracted = extract_source_enterprise_procurement_requirements(model)
    derived = derive_source_enterprise_procurement_requirements(model)
    payload = source_enterprise_procurement_requirements_to_dict(result)
    markdown = source_enterprise_procurement_requirements_to_markdown(result)

    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_enterprise_procurement_requirements(result) == result.summary
    assert source_enterprise_procurement_requirements_to_dicts(result) == payload["requirements"]
    assert source_enterprise_procurement_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.requirements
    assert result.gaps == result.evidence_gaps
    assert result.to_dicts() == payload["requirements"]
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "evidence_gaps", "records", "gaps"]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "suggested_owners",
        "suggested_plan_impacts",
    ]
    assert [record["category"] for record in payload["requirements"]] == [
        "security_review",
        "legal_review",
        "purchase_order",
    ]
    assert len(payload["requirements"][0]["evidence"]) == 1
    assert markdown.startswith("# Source Enterprise Procurement Requirements Report: procurement-model")
    assert "| Category | Value | Confidence | Source Field | Owners | Evidence | Suggested Plan Impacts |" in markdown


def test_unrelated_consumer_negated_invalid_and_object_inputs_are_stable_low_noise():
    class BriefLike:
        id = "object-no-procurement"
        summary = "Enterprise procurement is out of scope and no security review is required for this release."

    object_result = build_source_enterprise_procurement_requirements(
        SimpleNamespace(
            id="object-procurement",
            summary="Enterprise launch requires security review approval by launch; owner: security.",
            metadata={"sso": "SSO prerequisites require Okta approval by customer IT before go-live; owner: identity."},
        )
    )
    unrelated = build_source_enterprise_procurement_requirements(
        _source_brief(
            title="Consumer purchase history",
            summary="Consumer checkout should show purchase order history, legal copy, and security tips.",
            source_payload={"requirements": ["Show order status, receipts, and delivery timeline."]},
        )
    )
    negated = build_source_enterprise_procurement_requirements(BriefLike())
    malformed = build_source_enterprise_procurement_requirements({"source_payload": {"notes": object()}})
    invalid = build_source_enterprise_procurement_requirements(b"not text")

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "security_review": 0,
            "legal_review": 0,
            "purchase_order": 0,
            "vendor_onboarding": 0,
            "sso_scim_prerequisites": 0,
            "data_processing_agreement": 0,
            "sla_requirements": 0,
            "procurement_timeline": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "evidence_gap_count": 0,
        "evidence_gaps": [],
        "status": "no_enterprise_procurement_language",
    }
    assert [record.category for record in object_result.records] == [
        "security_review",
        "sso_scim_prerequisites",
    ]
    assert unrelated.records == ()
    assert negated.records == ()
    assert malformed.records == ()
    assert invalid.records == ()
    assert unrelated.summary == expected_summary
    assert unrelated.to_dicts() == []
    assert "No source enterprise procurement requirements were inferred" in unrelated.to_markdown()


def _source_brief(
    *,
    source_id="source-procurement",
    title="Enterprise procurement requirements",
    domain="enterprise",
    summary="General enterprise procurement requirements.",
    description=None,
    requirements=None,
    acceptance_criteria=None,
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "description": description,
        "requirements": [] if requirements is None else requirements,
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
    brief_id="implementation-procurement",
    title="Enterprise procurement implementation",
    scope=None,
    risks=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-procurement",
        "title": title,
        "domain": "enterprise",
        "target_user": "enterprise buyer",
        "buyer": "enterprise procurement",
        "workflow_context": "Enterprise sales launch",
        "problem_statement": "Implement source-backed enterprise procurement planning constraints.",
        "mvp_goal": "Ship procurement constraint extraction.",
        "product_surface": "implementation planning",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [] if risks is None else risks,
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run enterprise procurement extractor tests.",
        "definition_of_done": [],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
