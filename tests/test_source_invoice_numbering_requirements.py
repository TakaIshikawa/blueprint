import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_invoice_numbering_requirements import (
    SourceInvoiceNumberingRequirement,
    SourceInvoiceNumberingRequirementsReport,
    build_source_invoice_numbering_requirements,
    derive_source_invoice_numbering_requirements,
    extract_source_invoice_numbering_requirements,
    generate_source_invoice_numbering_requirements,
    source_invoice_numbering_requirements_to_dict,
    source_invoice_numbering_requirements_to_dicts,
    source_invoice_numbering_requirements_to_markdown,
    summarize_source_invoice_numbering_requirements,
)


def test_structured_billing_metadata_extracts_invoice_numbering_categories():
    result = build_source_invoice_numbering_requirements(
        _source_brief(
            source_payload={
                "invoice_numbering": {
                    "sequence": "Invoice numbers must be sequential and allocated from a monotonic invoice sequence.",
                    "prefix": "Invoice prefix must include country prefix and zero-padded number format.",
                    "fiscal": "Fiscal year reset should restart the invoice number series for each seller entity.",
                    "credit": "Credit note numbering must reference the original invoice.",
                    "voids": "Voided invoice and canceled invoice numbers must be preserved and never reused.",
                    "jurisdiction": "Jurisdiction rules must support VAT invoice and e-invoice numbering by country.",
                    "archive": "Invoice PDF archive must retain invoice documents in immutable storage.",
                    "duplicates": "Duplicate invoice number prevention requires idempotency and sequence locks.",
                    "audit": "Audit evidence must log numbering history, issuer history, and timestamps.",
                }
            }
        )
    )

    assert isinstance(result, SourceInvoiceNumberingRequirementsReport)
    assert result.source_id == "sb-invoice"
    assert all(isinstance(record, SourceInvoiceNumberingRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "invoice_sequence",
        "prefix_format",
        "fiscal_year_reset",
        "credit_note",
        "void_cancel",
        "jurisdiction",
        "retention",
        "duplicate_prevention",
        "audit_evidence",
    ]
    assert all(record.confidence == "high" for record in result.records)
    by_category = {record.category: record for record in result.records}
    assert by_category["jurisdiction"].owner_suggestion == "tax_compliance"
    assert "immutable storage" in by_category["retention"].planning_notes[0]
    assert result.summary["requirement_count"] == 9
    assert result.summary["category_counts"]["duplicate_prevention"] == 1
    assert any(
        "source_payload.invoice_numbering.sequence" in evidence
        for evidence in by_category["invoice_sequence"].evidence
    )


def test_model_like_object_and_implementation_brief_inputs_are_supported():
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Invoice numbers must use sequential numbering with no duplicate document number.",
                "Credit notes require credit memo numbering that references the original invoice.",
            ],
            definition_of_done=[
                "Canceled invoice handling preserves the original invoice number.",
                "Invoice PDF archive retention keeps invoice documents for audit evidence.",
            ],
        )
    )
    object_result = build_source_invoice_numbering_requirements(
        SimpleNamespace(
            id="object-invoice",
            summary="Fiscal year reset must reset the invoice sequence per tax year.",
            metadata={"prefix_format": "Invoice number format uses a country prefix."},
        )
    )

    model_result = extract_source_invoice_numbering_requirements(implementation)

    assert model_result.source_id == "impl-invoice"
    assert {
        "invoice_sequence",
        "credit_note",
        "void_cancel",
        "retention",
        "duplicate_prevention",
        "audit_evidence",
    } <= {record.category for record in model_result.records}
    assert [record.category for record in object_result.records] == [
        "invoice_sequence",
        "prefix_format",
        "fiscal_year_reset",
    ]


def test_raw_markdown_orders_categories_and_suppresses_duplicate_evidence():
    markdown = """
# Invoice Numbering

- Invoice numbers must be sequential for every tax invoice.
- Invoice numbers must be sequential for every tax invoice.
- Duplicate invoice number prevention must use idempotency keys.
- Voided invoice numbers must be preserved.
"""

    result = build_source_invoice_numbering_requirements(markdown)

    assert result.source_id is None
    assert [record.category for record in result.records] == [
        "invoice_sequence",
        "void_cancel",
        "duplicate_prevention",
    ]
    assert result.records[0].evidence == (
        "body: Invoice numbers must be sequential for every tax invoice.",
    )
    assert result.summary["categories"] == [
        "invoice_sequence",
        "void_cancel",
        "duplicate_prevention",
    ]


def test_serialization_markdown_summary_helpers_are_stable_and_do_not_mutate_source():
    source = _source_brief(
        source_id="invoice-model",
        summary="Invoice numbering must prevent duplicate invoice numbers.",
        source_payload={
            "acceptance_criteria": [
                "Invoice prefix must include seller | country code.",
                "Audit evidence must export invoice number allocation history.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_invoice_numbering_requirements(source)
    model_result = generate_source_invoice_numbering_requirements(model)
    derived = derive_source_invoice_numbering_requirements(model)
    payload = source_invoice_numbering_requirements_to_dict(model_result)
    markdown = source_invoice_numbering_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_invoice_numbering_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_invoice_numbering_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_invoice_numbering_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_invoice_numbering_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "evidence",
        "confidence",
        "owner_suggestion",
        "planning_notes",
    ]
    assert markdown == model_result.to_markdown()
    assert markdown.startswith("# Source Invoice Numbering Requirements Report: invoice-model")
    assert "| Source Brief | Category | Confidence | Owner | Planning Notes | Evidence |" in markdown
    assert "seller \\| country code" in markdown
    assert source_invoice_numbering_requirements_to_markdown(model_result) == markdown


def test_empty_negated_generic_pricing_and_tax_text_do_not_match():
    class BriefLike:
        id = "object-empty"
        summary = "No invoice numbering or document sequence changes are required."

    empty = build_source_invoice_numbering_requirements(
        _source_brief(summary="Update billing copy only.")
    )
    negated = build_source_invoice_numbering_requirements(BriefLike())
    generic_pricing = build_source_invoice_numbering_requirements(
        "Pricing must show discounts, sales tax, VAT, and the final billing total at checkout."
    )
    generic_tax = build_source_invoice_numbering_requirements(
        {"id": "tax-only", "summary": "Tax rates must calculate VAT and GST for invoices."}
    )
    malformed = build_source_invoice_numbering_requirements({"source_payload": {"notes": object()}})
    invalid = build_source_invoice_numbering_requirements(42)

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "invoice_sequence": 0,
            "prefix_format": 0,
            "fiscal_year_reset": 0,
            "credit_note": 0,
            "void_cancel": 0,
            "jurisdiction": 0,
            "retention": 0,
            "duplicate_prevention": 0,
            "audit_evidence": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
    }
    assert empty.source_id == "sb-invoice"
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No invoice numbering requirements were found" in empty.to_markdown()
    assert negated.records == ()
    assert generic_pricing.records == ()
    assert generic_tax.records == ()
    assert malformed.records == ()
    assert invalid.records == ()


def _source_brief(
    *,
    source_id="sb-invoice",
    title="Invoice numbering requirements",
    domain="billing",
    summary="General invoice numbering requirements.",
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


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "impl-invoice",
        "source_brief_id": "source-invoice",
        "title": "Invoice numbering",
        "domain": "billing",
        "target_user": "finance admins",
        "buyer": None,
        "workflow_context": "Invoice document numbering compliance.",
        "problem_statement": "Finance needs compliant invoice numbers.",
        "mvp_goal": "Ship invoice numbering requirements.",
        "product_surface": "billing",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review invoice number compliance scenarios.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
