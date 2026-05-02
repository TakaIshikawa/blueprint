import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_billing_proration_requirements import (
    SourceBillingProrationRequirement,
    SourceBillingProrationRequirementsReport,
    build_source_billing_proration_requirements,
    derive_source_billing_proration_requirements,
    extract_source_billing_proration_requirements,
    generate_source_billing_proration_requirements,
    source_billing_proration_requirements_to_dict,
    source_billing_proration_requirements_to_dicts,
    source_billing_proration_requirements_to_markdown,
    summarize_source_billing_proration_requirements,
)


def test_nested_source_payload_extracts_all_billing_proration_categories():
    result = build_source_billing_proration_requirements(
        _source_brief(
            source_payload={
                "billing_rules": {
                    "upgrade": "Plan upgrades must prorate the remaining billing period and charge the difference immediately.",
                    "downgrade": "Downgrades should create account credit for unused time.",
                    "seats": "Added or removed seats must prorate charges and credits by seat quantity.",
                    "cycle": "Billing cycle alignment must co-term changes to the renewal date.",
                    "trial": "Free trial conversion to paid subscription starts billing on the first invoice.",
                    "refunds": "Refund policy defines partial refund eligibility for cancelled subscriptions.",
                    "invoice_adjustments": "Invoice adjustment rules issue credit memos for invoice line corrections.",
                    "tax": "Sales tax and VAT must be recalculated for prorated credits and invoice adjustments.",
                }
            }
        )
    )

    assert isinstance(result, SourceBillingProrationRequirementsReport)
    assert result.source_id == "sb-billing"
    assert all(isinstance(record, SourceBillingProrationRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "plan_upgrade_proration",
        "downgrade_credit",
        "seat_count_change",
        "billing_cycle_alignment",
        "trial_conversion",
        "refund_policy",
        "invoice_adjustment",
        "tax_interaction",
    ]
    assert all(record.confidence == "high" for record in result.records)
    assert result.summary["requirement_count"] == 8
    assert result.summary["category_counts"] == {
        "plan_upgrade_proration": 1,
        "downgrade_credit": 1,
        "seat_count_change": 1,
        "billing_cycle_alignment": 1,
        "trial_conversion": 1,
        "refund_policy": 1,
        "invoice_adjustment": 1,
        "tax_interaction": 1,
    }
    assert result.summary["confidence_counts"] == {"high": 8, "medium": 0, "low": 0}
    assert any(
        "source_payload.billing_rules.upgrade" in evidence
        for record in result.records
        for evidence in record.evidence
    )


def test_refund_and_invoice_adjustment_evidence_are_kept_as_separate_categories():
    result = build_source_billing_proration_requirements(
        _source_brief(
            source_payload={
                "accounting": [
                    "Refund policy must allow partial refunds during the refund window.",
                    "Invoice adjustment must issue a debit memo or credit memo without treating it as a refund.",
                ]
            }
        )
    )

    by_category = {record.category: record for record in result.records}

    assert set(by_category) == {"refund_policy", "invoice_adjustment"}
    assert by_category["refund_policy"].owner_suggestion == "support"
    assert by_category["invoice_adjustment"].owner_suggestion == "finance"
    assert any("Refund policy" in item for item in by_category["refund_policy"].evidence)
    assert any("Invoice adjustment" in item for item in by_category["invoice_adjustment"].evidence)
    assert all(by_category["refund_policy"].evidence != record.evidence for record in [by_category["invoice_adjustment"]])
    assert "refund" in by_category["refund_policy"].planning_notes[0].casefold()
    assert "invoice" in by_category["invoice_adjustment"].planning_notes[0].casefold()


def test_implementation_brief_plain_text_and_object_inputs_are_supported():
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Subscription plan upgrade proration must be calculated mid-cycle.",
                "Seat count changes charge added seats and credit removed seats.",
            ],
            definition_of_done=[
                "Billing cycle alignment uses the customer's renewal date.",
                "Tax calculation is validated for prorated invoice adjustments.",
            ],
        )
    )
    text = build_source_billing_proration_requirements(
        "Trial conversion should create the first paid subscription invoice after the free trial."
    )
    object_result = build_source_billing_proration_requirements(
        SimpleNamespace(
            id="object-billing",
            summary="Downgrade credit rules carry forward unused subscription value.",
            metadata={"refund_policy": "Refund policy documents non-refundable add-ons."},
        )
    )

    model_result = extract_source_billing_proration_requirements(brief)

    assert model_result.source_id == "impl-billing"
    assert {
        "plan_upgrade_proration",
        "seat_count_change",
        "billing_cycle_alignment",
        "tax_interaction",
    } <= {record.category for record in model_result.records}
    assert text.records[0].category == "trial_conversion"
    assert [record.category for record in object_result.records] == [
        "downgrade_credit",
        "refund_policy",
    ]


def test_empty_no_signal_negated_and_malformed_inputs_return_stable_empty_reports():
    class BriefLike:
        id = "object-empty"
        summary = "No billing or subscription proration changes are in scope."

    empty = build_source_billing_proration_requirements(
        _source_brief(summary="This admin copy update has no billing requirements.")
    )
    negated = build_source_billing_proration_requirements(BriefLike())
    malformed = build_source_billing_proration_requirements({"source_payload": {"notes": object()}})
    invalid = build_source_billing_proration_requirements(42)

    assert empty.source_id == "sb-billing"
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "plan_upgrade_proration": 0,
            "downgrade_credit": 0,
            "seat_count_change": 0,
            "billing_cycle_alignment": 0,
            "trial_conversion": 0,
            "refund_policy": 0,
            "invoice_adjustment": 0,
            "tax_interaction": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
    }
    assert "No billing proration requirements were found" in empty.to_markdown()
    assert negated.records == ()
    assert malformed.records == ()
    assert invalid.records == ()


def test_sourcebrief_aliases_json_serialization_ordering_markdown_and_no_mutation():
    source = _source_brief(
        source_id="billing-model",
        summary="Subscription billing must support plan upgrades and downgrade credits.",
        source_payload={
            "acceptance_criteria": [
                "Plan upgrade proration must charge the remaining billing period.",
                "Downgrade credit must carry forward unused value.",
                "Invoice adjustment must escape account | plan notes.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_billing_proration_requirements(source)
    model_result = generate_source_billing_proration_requirements(model)
    derived = derive_source_billing_proration_requirements(model)
    payload = source_billing_proration_requirements_to_dict(model_result)
    markdown = source_billing_proration_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_billing_proration_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert source_billing_proration_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_billing_proration_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_billing_proration_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "evidence",
        "confidence",
        "owner_suggestion",
        "planning_notes",
    ]
    assert [
        (record.source_brief_id, record.category)
        for record in model_result.records
    ] == [
        ("billing-model", "plan_upgrade_proration"),
        ("billing-model", "downgrade_credit"),
        ("billing-model", "invoice_adjustment"),
    ]
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Category | Confidence | Owner | Planning Notes | Evidence |" in markdown
    assert "account \\| plan notes" in markdown


def _source_brief(
    *,
    source_id="sb-billing",
    title="Billing proration requirements",
    domain="billing",
    summary="General billing proration requirements.",
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


def _implementation_brief(
    *,
    scope=None,
    definition_of_done=None,
):
    return {
        "id": "impl-billing",
        "source_brief_id": "source-billing",
        "title": "Subscription billing",
        "domain": "billing",
        "target_user": "customers",
        "buyer": None,
        "workflow_context": "Subscription lifecycle billing.",
        "problem_statement": "Customers need correct subscription change billing.",
        "mvp_goal": "Ship subscription change billing.",
        "product_surface": "billing settings",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run billing proration validation.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
