import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_pricing_model_assumptions import (
    SourcePricingModelAssumption,
    SourcePricingModelAssumptionsReport,
    build_source_pricing_model_assumptions,
    derive_source_pricing_model_assumptions,
    extract_source_pricing_model_assumptions,
    generate_source_pricing_model_assumptions,
    source_pricing_model_assumptions_to_dict,
    source_pricing_model_assumptions_to_dicts,
    source_pricing_model_assumptions_to_markdown,
)


def test_detects_plan_tier_language_and_plan_limits():
    result = build_source_pricing_model_assumptions(
        _source_brief(
            summary="Free tier includes 3 projects while the Pro plan includes 25 projects.",
            source_payload={
                "pricing": [
                    "Enterprise tier pricing starts after 500 users.",
                    "Plan limits must show upgrade prompts when the included credits are exhausted.",
                ]
            },
        )
    )

    by_type = {record.assumption_type: record for record in result.records}

    assert isinstance(result, SourcePricingModelAssumptionsReport)
    assert all(isinstance(record, SourcePricingModelAssumption) for record in result.records)
    assert "tiered_plan" in by_type
    assert "plan_limit" in by_type
    assert by_type["tiered_plan"].audience_or_plan == "free"
    assert by_type["tiered_plan"].source_field == "summary"
    assert any("summary:" in item for item in by_type["tiered_plan"].evidence)
    assert "limit enforcement" in by_type["plan_limit"].planning_note
    assert result.summary["assumption_type_counts"]["tiered_plan"] >= 1


def test_detects_usage_metering_seat_pricing_and_overages():
    result = build_source_pricing_model_assumptions(
        {
            "id": "usage-pricing",
            "requirements": [
                "Usage-based billing records billable API calls for paid customers.",
                "Seats are charged per member with a $12 seat price.",
                "Overage fees apply above quota after included usage is exhausted.",
            ],
        }
    )

    types = {record.assumption_type for record in result.records}

    assert {"usage_based_pricing", "seat_based_pricing", "overage"} <= types
    assert any(record.confidence == "high" for record in result.records)
    assert any(record.audience_or_plan == "paid" for record in result.records)


def test_detects_trial_and_discount_assumptions():
    result = build_source_pricing_model_assumptions(
        {
            "id": "trial-discount",
            "summary": "The free trial converts after 14 days.",
            "pricing": "Annual customers receive a 20% discount through a coupon code.",
        }
    )

    by_type = {record.assumption_type: record for record in result.records}

    assert by_type["trial"].audience_or_plan == "free"
    assert by_type["discount"].audience_or_plan == "annual customers"
    assert by_type["discount"].confidence == "high"


def test_detects_tax_invoice_and_grandfathering_wording():
    result = build_source_pricing_model_assumptions(
        _source_brief(
            source_id="tax-invoices",
            source_payload={
                "billing": [
                    "Invoices must include VAT ID and sales tax details.",
                    "Existing customers keep legacy pricing and are grandfathered for renewal.",
                ]
            },
        )
    )

    types = {record.assumption_type for record in result.records}

    assert {"invoice", "tax_vat", "grandfathering"} <= types
    assert result.summary["confidence_counts"]["high"] == len(result.records)
    assert all(record.source_field.startswith("source_payload.billing") for record in result.records)


def test_deduplicates_evidence_and_ignores_entitlement_only_text():
    result = build_source_pricing_model_assumptions(
        {
            "id": "dedupe-pricing",
            "summary": (
                "Entitlement checks gate premium reports. "
                "Plan limits must show upgrade prompts when quota is exceeded."
            ),
            "requirements": [
                "Plan limits must show upgrade prompts when quota is exceeded.",
                "plan limits must show upgrade prompts when quota is exceeded.",
                "Feature gates and entitlement checks decide report access for paid users.",
            ],
        }
    )

    assert {record.assumption_type for record in result.records} == {"plan_limit"}
    for record in result.records:
        assert record.evidence == tuple(sorted(set(record.evidence), key=lambda item: item.casefold()))
        assert len(record.evidence) == len(set(record.evidence))

    entitlement_only = build_source_pricing_model_assumptions(
        {"id": "entitlement-only", "summary": "Entitlement checks gate access for paid users."}
    )
    assert entitlement_only.records == ()


def test_brief_like_object_sourcebrief_and_serialization_helpers_are_supported():
    source = _source_brief(
        source_id="pricing-model",
        summary="Pro plan uses per-seat pricing at $15 per user.",
        source_payload={"billing": "Tax invoices are required for Enterprise customers."},
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    brief_like = BriefLike(
        id="brief-like",
        summary="Starter plan has a free trial and annual discount.",
        pricing="Usage-based pricing applies to billable events.",
    )

    mapping_result = build_source_pricing_model_assumptions(source)
    model_result = generate_source_pricing_model_assumptions(model)
    derived = derive_source_pricing_model_assumptions(model)
    extracted = extract_source_pricing_model_assumptions(model)
    object_result = build_source_pricing_model_assumptions(brief_like)
    payload = source_pricing_model_assumptions_to_dict(model_result)
    markdown = source_pricing_model_assumptions_to_markdown(model_result)

    assert source == original
    assert payload == source_pricing_model_assumptions_to_dict(mapping_result)
    assert derived == model_result
    assert extracted == model_result.assumptions
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.assumptions
    assert model_result.to_dicts() == payload["assumptions"]
    assert source_pricing_model_assumptions_to_dicts(model_result.records) == payload["records"]
    assert list(payload) == ["source_id", "assumptions", "summary", "records"]
    assert list(payload["assumptions"][0]) == [
        "assumption_type",
        "audience_or_plan",
        "evidence",
        "source_field",
        "confidence",
        "planning_note",
    ]
    assert markdown.startswith("# Source Pricing Model Assumptions Report: pricing-model")
    assert "| Type | Audience Or Plan | Confidence | Source Field | Evidence | Planning Note |" in markdown
    assert {record.assumption_type for record in object_result.records} >= {
        "tiered_plan",
        "trial",
        "discount",
        "usage_based_pricing",
    }


class BriefLike:
    def __init__(self, *, id, summary, pricing):
        self.id = id
        self.summary = summary
        self.pricing = pricing


def _source_brief(
    *,
    source_id="source-pricing",
    title="Pricing model assumptions",
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
