import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_vat_tax_requirements import (
    SourceVatTaxEvidenceGap,
    SourceVatTaxRequirement,
    SourceVatTaxRequirementsReport,
    build_source_vat_tax_requirements,
    derive_source_vat_tax_requirements,
    extract_source_vat_tax_requirements,
    generate_source_vat_tax_requirements,
    source_vat_tax_requirements_to_dict,
    source_vat_tax_requirements_to_dicts,
    source_vat_tax_requirements_to_markdown,
    summarize_source_vat_tax_requirements,
)


def test_structured_billing_brief_extracts_vat_tax_requirements_in_order():
    result = build_source_vat_tax_requirements(
        _source_brief(
            source_payload={
                "vat_tax": {
                    "jurisdictions": "Taxable jurisdictions must include EU VAT, UK VAT, and Canada GST using billing country and place of supply.",
                    "timing": "Calculate VAT at checkout and snapshot final tax rates during invoice finalization.",
                    "exemptions": "Exemption handling must validate VAT ID, tax ID, and exemption certificate status for reverse charge.",
                    "invoice": "Invoice tax display must show VAT number on invoice, tax line items, tax rate, subtotal, and tax total.",
                    "refunds": "Refund tax treatment must reverse VAT on partial refunds and issue credit notes with tax adjustments.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceVatTaxRequirementsReport)
    assert all(isinstance(record, SourceVatTaxRequirement) for record in result.records)
    assert result.gaps == ()
    assert [record.category for record in result.records] == [
        "taxable_jurisdictions",
        "tax_calculation_timing",
        "exemption_handling",
        "invoice_tax_display",
        "refund_tax_treatment",
    ]
    assert by_category["taxable_jurisdictions"].value == "EU, UK, Canada"
    assert by_category["tax_calculation_timing"].value == "checkout, snapshot, final tax"
    assert by_category["exemption_handling"].value == "Exemption, VAT ID, tax ID"
    assert by_category["invoice_tax_display"].source_field == "source_payload.vat_tax.invoice"
    assert by_category["refund_tax_treatment"].suggested_owner == "finance_ops"
    assert result.summary["requirement_count"] == 5
    assert result.summary["evidence_gap_count"] == 0
    assert result.summary["status"] == "ready_for_planning"


def test_partial_vat_brief_flags_missing_jurisdiction_exemption_refund_and_invoice_details():
    result = build_source_vat_tax_requirements(
        _source_brief(
            summary="VAT launch requires tax calculation before payment capture.",
            requirements=[
                "Billing service must calculate VAT at checkout and persist a tax rate snapshot.",
            ],
        )
    )

    assert [record.category for record in result.records] == ["tax_calculation_timing"]
    assert [gap.category for gap in result.evidence_gaps] == [
        "missing_jurisdiction_details",
        "missing_exemption_details",
        "missing_refund_details",
        "missing_invoice_display_details",
    ]
    assert all(isinstance(gap, SourceVatTaxEvidenceGap) for gap in result.evidence_gaps)
    assert result.summary["status"] == "needs_tax_detail"
    assert result.summary["evidence_gap_count"] == 4


def test_implementation_brief_and_plain_text_extract_without_mutation():
    implementation_payload = _implementation_brief(
        architecture_notes="VAT calculation runs at checkout before payment capture.",
        risks=[
            "Taxable jurisdictions are unresolved for billing country, nexus, and place of supply.",
            "Refund tax treatment must support credit notes and tax reversals.",
        ],
        definition_of_done=[
            "Invoice tax display shows tax line items and tax total.",
            "Exemption handling validates VAT ID and reverse charge eligibility.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    text_result = build_source_vat_tax_requirements(
        """
# VAT billing

- Taxable jurisdictions must include EU VAT and UK VAT by billing country.
- Invoice tax display includes VAT number on invoice and tax breakdown.
"""
    )
    implementation_result = generate_source_vat_tax_requirements(implementation)

    assert implementation_payload == original
    assert [record.category for record in text_result.records] == [
        "taxable_jurisdictions",
        "invoice_tax_display",
    ]
    assert text_result.records[0].source_field == "body"
    assert [record.category for record in implementation_result.records] == [
        "taxable_jurisdictions",
        "tax_calculation_timing",
        "exemption_handling",
        "invoice_tax_display",
        "refund_tax_treatment",
    ]
    assert implementation_result.source_id == "impl-vat"


def test_duplicate_evidence_serialization_markdown_aliases_and_helpers_are_stable():
    source = _source_brief(
        source_id="source-vat-model",
        summary="VAT rollout source.",
        source_payload={
            "requirements": [
                "Taxable jurisdictions must include EU VAT and UK VAT by billing country.",
                "Taxable jurisdictions must include EU VAT and UK VAT by billing country.",
                "Invoice tax display must show VAT number on invoice and tax line items.",
                "Refund tax treatment must reverse VAT on refunds.",
            ],
            "metadata": {"exemptions": "Exemption handling validates VAT ID and reverse charge."},
        },
    )
    original = copy.deepcopy(source)
    mapping_result = build_source_vat_tax_requirements(source)
    model = SourceBrief.model_validate({key: value for key, value in source.items() if key != "requirements"})
    model_result = generate_source_vat_tax_requirements(model)
    extracted = extract_source_vat_tax_requirements(model)
    derived = derive_source_vat_tax_requirements(model)
    payload = source_vat_tax_requirements_to_dict(model_result)
    markdown = source_vat_tax_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert extracted == model_result.requirements
    assert derived.to_dict() == model_result.to_dict()
    assert model_result.records == model_result.requirements
    assert model_result.gaps == model_result.evidence_gaps
    assert model_result.to_dicts() == payload["requirements"]
    assert source_vat_tax_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_vat_tax_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_vat_tax_requirements(model_result) == model_result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "requirements", "evidence_gaps", "summary", "records", "gaps"]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "suggested_owner",
        "planning_notes",
    ]
    assert mapping_result.records[0].evidence == (
        "source_payload.requirements[0]: Taxable jurisdictions must include EU VAT and UK VAT by billing country.",
    )
    assert markdown.startswith("# Source VAT Tax Requirements Report: source-vat-model")
    assert "| Category | Value | Confidence | Source Field | Owner | Evidence | Planning Notes |" in markdown
    assert "No VAT or tax requirements were found" not in markdown


def test_irrelevant_negated_invalid_and_object_inputs_are_stable_empty_or_low_noise():
    class BriefLike:
        id = "object-no-vat"
        summary = "No VAT or tax changes are required for this copy-only release."

    object_result = build_source_vat_tax_requirements(
        SimpleNamespace(
            id="object-vat",
            summary="VAT requirements include tax calculation at checkout.",
            metadata={"invoice": "Invoice tax display must show tax total."},
        )
    )
    empty = build_source_vat_tax_requirements(
        _source_brief(source_id="empty-vat", summary="Update checkout copy and button labels only.")
    )
    repeat = build_source_vat_tax_requirements(
        _source_brief(source_id="empty-vat", summary="Update checkout copy and button labels only.")
    )
    negated = build_source_vat_tax_requirements(BriefLike())
    invalid = build_source_vat_tax_requirements(b"not text")

    expected_summary = {
        "requirement_count": 0,
        "category_counts": {
            "taxable_jurisdictions": 0,
            "tax_calculation_timing": 0,
            "exemption_handling": 0,
            "invoice_tax_display": 0,
            "refund_tax_treatment": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
        "evidence_gap_count": 0,
        "evidence_gaps": [],
        "status": "no_vat_tax_requirements_found",
    }
    assert object_result.source_id == "object-vat"
    assert [record.category for record in object_result.records] == [
        "tax_calculation_timing",
        "invoice_tax_display",
    ]
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "empty-vat"
    assert empty.requirements == ()
    assert empty.evidence_gaps == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No VAT or tax requirements were found" in empty.to_markdown()
    assert negated.requirements == ()
    assert negated.summary == expected_summary
    assert invalid.source_id is None
    assert invalid.requirements == ()
    assert invalid.summary == expected_summary


def _source_brief(
    *,
    source_id="source-vat",
    title="VAT tax requirements",
    domain="commerce",
    summary="General VAT tax requirements.",
    requirements=None,
    source_payload=None,
    source_links=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "requirements": [] if requirements is None else requirements,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, architecture_notes=None, risks=None, definition_of_done=None):
    return {
        "id": "impl-vat",
        "source_brief_id": "source-vat",
        "title": "VAT tax implementation",
        "domain": "commerce",
        "target_user": "finance admins",
        "buyer": "enterprise",
        "workflow_context": "Normalize VAT and tax obligations before task generation.",
        "problem_statement": "Finance needs compliant VAT handling.",
        "mvp_goal": "Capture VAT obligations in the execution plan.",
        "product_surface": "billing",
        "scope": ["Checkout", "Invoices", "Refunds"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": architecture_notes,
        "data_requirements": None,
        "integration_points": [],
        "risks": risks or [],
        "validation_plan": "Review VAT scenarios.",
        "definition_of_done": definition_of_done or [],
    }
