import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_billing_tax_requirements import (
    SourceBillingTaxRequirement,
    SourceBillingTaxRequirementsReport,
    build_source_billing_tax_requirements,
    extract_source_billing_tax_requirements,
    generate_source_billing_tax_requirements,
    source_billing_tax_requirements_to_dict,
    source_billing_tax_requirements_to_dicts,
    source_billing_tax_requirements_to_markdown,
    summarize_source_billing_tax_requirements,
)


def test_markdown_and_structured_source_payload_extract_tax_categories_with_evidence():
    result = build_source_billing_tax_requirements(
        _source_brief(
            source_payload={
                "body": """
# Billing Tax Requirements

- Calculate sales tax from tax rates before checkout completes.
- VAT/GST collection must support tax IDs for EU and Canada customers.
- Tax exemption certificates are required for nonprofit customers.
- Invoice tax display must include tax line items and VAT number on invoice.
- Jurisdiction rules depend on billing country, state tax, and nexus.
- Reverse charge applies when the customer accounts for VAT.
- Tax reporting exports are needed for remittance and audit review.
""",
                "tax": {
                    "invoice_tax_display": "Receipts show GST and total tax amount.",
                    "jurisdiction_rules": "Place of supply determines regional tax.",
                },
            }
        )
    )

    assert isinstance(result, SourceBillingTaxRequirementsReport)
    assert all(isinstance(record, SourceBillingTaxRequirement) for record in result.records)
    assert [record.category for record in result.requirements] == [
        "tax_calculation",
        "vat_gst_collection",
        "tax_exemption",
        "invoice_tax_display",
        "jurisdiction_rules",
        "reverse_charge",
        "tax_reporting",
    ]
    by_category = {record.category: record for record in result.records}
    assert any("source_payload.body" in item for item in by_category["tax_calculation"].evidence)
    assert any(
        "source_payload.tax.invoice_tax_display" in item
        for item in by_category["invoice_tax_display"].evidence
    )
    assert by_category["jurisdiction_rules"].suggested_owner == "tax_compliance"
    assert "remittance exports" in by_category["tax_reporting"].suggested_planning_note
    assert result.summary["requirement_count"] == 7
    assert result.summary["category_counts"]["reverse_charge"] == 1
    assert result.summary["high_confidence_count"] >= 4


def test_implementation_brief_risks_architecture_notes_and_done_contribute_evidence():
    model = ImplementationBrief.model_validate(
        _implementation(
            architecture_notes=(
                "Billing service must calculate taxes with a tax engine and persist tax rate snapshots."
            ),
            risks=[
                "VAT collection can fail if tax ID validation is skipped.",
                "Jurisdiction rules and nexus review are required before launch.",
            ],
            definition_of_done=[
                "Invoices show tax breakdown and VAT number on invoice.",
                "Tax reporting export supports filing and remittance reconciliation.",
            ],
        )
    )

    result = build_source_billing_tax_requirements(model)

    assert result.source_id == "impl-tax"
    assert [record.category for record in result.records] == [
        "tax_calculation",
        "vat_gst_collection",
        "invoice_tax_display",
        "jurisdiction_rules",
        "tax_reporting",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["tax_calculation"].evidence == (
        "architecture_notes: Billing service must calculate taxes with a tax engine and persist tax rate snapshots.",
    )
    assert (
        "risks[0]: VAT collection can fail if tax ID validation is skipped."
        in by_category["vat_gst_collection"].evidence
    )
    assert by_category["invoice_tax_display"].evidence == (
        "definition_of_done[0]: Invoices show tax breakdown and VAT number on invoice.",
    )
    assert by_category["jurisdiction_rules"].confidence >= 0.85


def test_duplicate_categories_merge_deterministically_with_stable_confidence():
    result = build_source_billing_tax_requirements(
        {
            "id": "dupe-tax",
            "source_payload": {
                "tax": {
                    "sales_tax": "Calculate sales tax from tax rates before checkout.",
                    "same_sales_tax": "Calculate sales tax from tax rates before checkout.",
                    "vat": "VAT collection must include tax ID validation.",
                },
                "acceptance_criteria": [
                    "Calculate sales tax from tax rates before checkout.",
                    "VAT collection must include tax ID validation.",
                ],
            },
        }
    )

    assert [record.category for record in result.records] == [
        "tax_calculation",
        "vat_gst_collection",
    ]
    assert result.records[0].evidence == (
        "source_payload.acceptance_criteria[0]: Calculate sales tax from tax rates before checkout.",
    )
    assert result.records[0].confidence == 0.95
    assert result.records[1].confidence == 0.95
    assert result.summary["categories"] == ["tax_calculation", "vat_gst_collection"]


def test_sourcebrief_object_serialization_markdown_and_summary_helpers_are_stable():
    source = _source_brief(
        source_id="source-tax-model",
        summary="Checkout needs tax calculation and invoice tax display.",
        source_payload={
            "requirements": [
                "Tax exemption certificates must be captured for exempt customers.",
                "Reverse charge invoice language is required for eligible VAT customers.",
            ],
            "metadata": {"tax_reporting": "Tax reports support remittance review."},
        },
    )
    original = copy.deepcopy(source)
    mapping_result = build_source_billing_tax_requirements(source)
    model_result = generate_source_billing_tax_requirements(SourceBrief.model_validate(source))
    extracted = extract_source_billing_tax_requirements(SourceBrief.model_validate(source))
    payload = source_billing_tax_requirements_to_dict(model_result)
    markdown = source_billing_tax_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert extracted == model_result.requirements
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_billing_tax_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_billing_tax_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_billing_tax_requirements(model_result) == model_result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "category",
        "confidence",
        "evidence",
        "suggested_owner",
        "suggested_planning_note",
    ]
    assert markdown.startswith("# Source Billing Tax Requirements Report: source-tax-model")
    assert (
        "| Category | Confidence | Evidence | Suggested Owner | Suggested Planning Note |"
        in markdown
    )


def test_empty_invalid_and_negated_inputs_return_stable_empty_reports():
    empty = build_source_billing_tax_requirements(
        _source_brief(source_id="empty-tax", summary="Update billing copy only.")
    )
    repeat = build_source_billing_tax_requirements(
        _source_brief(source_id="empty-tax", summary="Update billing copy only.")
    )
    negated = build_source_billing_tax_requirements(
        {"id": "negated-tax", "summary": "No tax reporting changes are required for this copy update."}
    )
    invalid = build_source_billing_tax_requirements("not a source brief")

    expected_summary = {
        "requirement_count": 0,
        "category_counts": {
            "tax_calculation": 0,
            "vat_gst_collection": 0,
            "tax_exemption": 0,
            "invoice_tax_display": 0,
            "jurisdiction_rules": 0,
            "reverse_charge": 0,
            "tax_reporting": 0,
        },
        "high_confidence_count": 0,
        "categories": [],
        "suggested_owner_counts": {},
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "empty-tax"
    assert empty.requirements == ()
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No billing tax requirements were found" in empty.to_markdown()
    assert negated.requirements == ()
    assert invalid.source_id is None
    assert invalid.requirements == ()
    assert invalid.summary == expected_summary


def _source_brief(
    *,
    source_id="source-tax",
    title="Billing tax requirements",
    domain="commerce",
    summary="General billing tax requirements.",
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


def _implementation(*, architecture_notes=None, risks=None, definition_of_done=None):
    return {
        "id": "impl-tax",
        "source_brief_id": "source-tax",
        "title": "Checkout tax handling",
        "domain": "commerce",
        "target_user": "finance admins",
        "buyer": "enterprise",
        "workflow_context": "Normalize billing tax obligations before task generation.",
        "problem_statement": "Finance needs compliant tax handling.",
        "mvp_goal": "Capture tax obligations in the execution plan.",
        "product_surface": "billing",
        "scope": ["Checkout", "Invoices"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": architecture_notes,
        "data_requirements": None,
        "integration_points": [],
        "risks": risks or [],
        "validation_plan": "Review billing tax scenarios.",
        "definition_of_done": definition_of_done or [],
    }
