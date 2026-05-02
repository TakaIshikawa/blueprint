import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_pricing_model_constraints import (
    SourcePricingModelConstraint,
    SourcePricingModelConstraintsReport,
    build_source_pricing_model_constraints,
    source_pricing_model_constraints_to_dict,
    source_pricing_model_constraints_to_markdown,
)


def test_structured_fields_extract_explicit_constraints_with_plan_impacts():
    result = build_source_pricing_model_constraints(
        {
            "id": "pricing-explicit",
            "title": "Checkout packaging",
            "metadata": {
                "pricing_model_constraints": [
                    {
                        "type": "quota",
                        "constraint": "Pro plan exports are capped at 10,000 rows per month.",
                        "confidence": "high",
                        "suggested_plan_impacts": ["Show quota usage and block exports after the Pro cap."],
                        "unresolved_questions": ["Do Enterprise plans receive unlimited exports?"],
                    },
                    {
                        "type": "tax",
                        "constraint": "VAT invoices must show buyer tax ID for EU customers.",
                    },
                ],
                "billing_constraints": ["Annual plans require proration when a customer upgrades mid-cycle."],
            },
        }
    )

    assert isinstance(result, SourcePricingModelConstraintsReport)
    assert result.brief_id == "pricing-explicit"
    assert result.summary["explicit_constraint_count"] == 3
    assert result.summary["constraint_counts_by_type"] == {"billing": 1, "quota": 1, "tax": 1}

    quota = _finding(result, "quota")
    assert isinstance(quota, SourcePricingModelConstraint)
    assert quota.confidence == "high"
    assert quota.source_field == "metadata.pricing_model_constraints[0]"
    assert quota.constraint == "Pro plan exports are capped at 10,000 rows per month."
    assert quota.suggested_plan_impacts == ("Show quota usage and block exports after the Pro cap.",)
    assert quota.unresolved_questions == ("Do Enterprise plans receive unlimited exports?",)

    billing = _finding(result, "billing")
    assert billing.source_field == "metadata.billing_constraints[0]"
    assert "proration" in billing.suggested_plan_impacts[0]


def test_inferred_text_signals_cover_pricing_billing_entitlement_invoice_discount_tax_and_trial():
    result = build_source_pricing_model_constraints(
        {
            "id": "pricing-inferred",
            "summary": "Usage-based pricing must charge $0.05 per invoice after the free tier.",
            "constraints": [
                "Enterprise plan users get SSO as an entitlement and feature gate.",
                "Trial accounts expire after 14 days unless a paid subscription starts.",
                "Discount coupons cannot stack with contract price overrides.",
                "Sales tax must be calculated for US invoices.",
            ],
            "acceptance_criteria": ["Quota limit is 100 credits per month with no overage."],
        }
    )

    assert result.summary["confidence_counts"]["high"] >= 4
    assert {constraint.constraint_type for constraint in result.constraints} == {
        "pricing",
        "billing",
        "plan_tier",
        "entitlement",
        "quota",
        "discount",
        "invoice",
        "tax",
        "trial",
    }
    assert any(
        constraint.constraint_type == "pricing" and constraint.source_field == "summary"
        for constraint in result.constraints
    )
    assert _finding(result, "entitlement").source_field == "constraints[0]"
    assert _finding(result, "trial").unresolved_questions == ()
    assert any("tax jurisdiction" in impact for impact in _finding(result, "tax").suggested_plan_impacts)


def test_source_and_implementation_brief_models_and_objects_are_supported():
    source_result = build_source_pricing_model_constraints(
        SourceBrief.model_validate(
            _source_brief(
                source_id="source-model",
                summary="Free trial access should last 30 days before conversion.",
                source_payload={
                    "requirements": ["Plan tier changes must update licensed seats immediately."],
                    "metadata": {"quotas": ["Starter quota is limited to 5 projects."]},
                },
            )
        )
    )
    implementation_result = build_source_pricing_model_constraints(
        ImplementationBrief.model_validate(
            _implementation_brief(
                scope=["Invoices require a purchase order number for annual billing."],
                data_requirements="Tax exemption status must be stored for Enterprise accounts.",
            )
        )
    )
    object_result = build_source_pricing_model_constraints(
        SimpleNamespace(
            id="object-brief",
            title="Discount handling",
            body="Promo codes should expire after launch and cannot stack with discounts.",
        )
    )

    assert _finding(source_result, "trial").confidence == "high"
    assert _finding(source_result, "quota").explicit is True
    assert _finding(implementation_result, "invoice").source_field == "scope[0]"
    assert _finding(implementation_result, "tax").source_field == "data_requirements"
    assert _finding(object_result, "discount").confidence == "medium"


def test_duplicate_constraints_collapse_by_type_and_evidence():
    result = build_source_pricing_model_constraints(
        {
            "id": "dupes",
            "summary": "Quota limit is 100 exports per month.",
            "constraints": [
                "Quota limit is 100 exports per month.",
                "quota limit is 100 exports per month.",
                "Plan tier must be Enterprise for audit exports.",
            ],
            "metadata": {"quotas": ["Quota limit is 100 exports per month."]},
        }
    )

    assert [(constraint.constraint_type, constraint.evidence, constraint.source_field) for constraint in result.constraints] == [
        ("plan_tier", "Plan tier must be Enterprise for audit exports.", "constraints[2]"),
        ("quota", "Quota limit is 100 exports per month.", "metadata.quotas[0]"),
    ]


def test_serialization_markdown_empty_state_and_json_ordering_are_stable():
    result = build_source_pricing_model_constraints(
        {
            "id": "pipes",
            "summary": "Enterprise | Pro plans require tax review?",
        }
    )
    payload = source_pricing_model_constraints_to_dict(result)
    markdown = source_pricing_model_constraints_to_markdown(result)
    empty = build_source_pricing_model_constraints({"id": "empty", "title": "Copy polish", "body": "Update labels."})
    invalid = build_source_pricing_model_constraints(17)

    assert result.to_dicts() == payload["constraints"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "constraints", "summary"]
    assert list(payload["constraints"][0]) == [
        "constraint_type",
        "constraint",
        "confidence",
        "source_field",
        "evidence",
        "suggested_plan_impacts",
        "unresolved_questions",
        "explicit",
    ]
    assert markdown.startswith("# Source Pricing Model Constraints Report: pipes")
    assert "## Summary" in markdown
    assert "| Type | Constraint | Confidence | Source | Evidence | Suggested Plan Impacts | Questions |" in markdown
    assert "Enterprise \\| Pro plans require tax review?" in markdown
    assert result.summary["unresolved_question_count"] >= 1
    assert empty.brief_id == "empty"
    assert empty.constraints == ()
    assert empty.summary["confidence_counts"] == {"high": 0, "medium": 0, "low": 0}
    assert "No pricing model constraints were found" in empty.to_markdown()
    assert invalid.brief_id is None
    assert invalid.constraints == ()


def _finding(result, constraint_type):
    return next(
        constraint for constraint in result.constraints if constraint.constraint_type == constraint_type
    )


def _source_brief(
    *,
    source_id="source-pricing",
    title="Pricing requirements",
    domain="billing",
    summary="General pricing requirements.",
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


def _implementation_brief(*, scope=None, data_requirements=None):
    return {
        "id": "impl-brief",
        "source_brief_id": "source-brief",
        "title": "Billing admin",
        "domain": "billing",
        "target_user": "Ops",
        "buyer": "Finance",
        "workflow_context": "Billing review",
        "problem_statement": "Finance needs accurate billing controls.",
        "mvp_goal": "Ship plan constraints.",
        "product_surface": "Admin dashboard",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": data_requirements,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run billing smoke tests.",
        "definition_of_done": [],
        "status": "draft",
    }
