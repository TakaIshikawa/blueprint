import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_invoice_adjustment_requirements import (
    SourceInvoiceAdjustmentRequirement,
    SourceInvoiceAdjustmentRequirementsReport,
    build_source_invoice_adjustment_requirements,
    derive_source_invoice_adjustment_requirements,
    extract_source_invoice_adjustment_requirements,
    generate_source_invoice_adjustment_requirements,
    source_invoice_adjustment_requirements_to_dict,
    source_invoice_adjustment_requirements_to_dicts,
    source_invoice_adjustment_requirements_to_markdown,
    summarize_source_invoice_adjustment_requirements,
)


def test_prose_and_structured_fields_extract_invoice_adjustment_categories():
    result = build_source_invoice_adjustment_requirements(
        _source_brief(
            source_payload={
                "body": """
# Invoice Adjustments

- Invoice adjustments over $500 must require finance approval before posting.
- Credit memos must reference the original invoice number and include line item credit amount.
- Tax recalculation must recompute VAT and sales tax by jurisdiction with rounding rules.
- Audit trail records who approved the adjustment reason and the timestamp.
- Customer notification emails should be sent to the billing contact after the credit memo is issued.
- Accounting export posts journal entries to NetSuite and updates accounts receivable.
""",
                "metadata": {
                    "invoice_adjustments": {
                        "accounting_export": "Adjusted invoices export ledger mapping changes after approval.",
                    }
                },
            }
        )
    )

    assert isinstance(result, SourceInvoiceAdjustmentRequirementsReport)
    assert all(isinstance(record, SourceInvoiceAdjustmentRequirement) for record in result.records)
    assert sorted(record.category for record in result.records) == sorted(
        [
            "adjustment_authorization",
            "credit_memo",
            "tax_recalculation",
            "audit_trail",
            "customer_notification",
            "accounting_export",
        ]
    )
    by_category = {record.category: record for record in result.records}
    assert any("source_payload.body" in item for item in by_category["credit_memo"].evidence)
    assert (
        "source_payload.metadata.invoice_adjustments.accounting_export"
        in by_category["accounting_export"].source_fields
    )
    assert by_category["adjustment_authorization"].missing_detail_flags == ()
    assert by_category["tax_recalculation"].suggested_owner == "tax_compliance"
    assert "ledger mappings" in by_category["accounting_export"].suggested_planning_note
    assert result.summary["requirement_count"] == 6
    assert result.summary["category_counts"]["audit_trail"] == 1
    assert result.summary["high_confidence_count"] >= 4


def test_nested_metadata_missing_details_and_implementation_brief_paths_are_reported():
    implementation = ImplementationBrief.model_validate(
        _implementation(
            architecture_notes="Invoice adjustment service must create credit memos for adjusted invoices.",
            risks=[
                "Tax recalculation is risky without VAT line item basis.",
                "Customer notification is required for each invoice adjustment.",
            ],
            definition_of_done=[
                "Accounting export writes journal entries for approved adjustments.",
                "Audit trail captures invoice adjustment activity.",
            ],
        )
    )

    result = build_source_invoice_adjustment_requirements(
        {
            "id": "nested-adjustment",
            "source_payload": {
                "metadata": {
                    "billing": {
                        "invoice_adjustments": {
                            "authorization": "Manual invoice adjustment approval is required."
                        }
                    }
                }
            },
        }
    )
    implementation_result = build_source_invoice_adjustment_requirements(implementation)

    authorization = _record(result, "adjustment_authorization")
    assert authorization.source_fields == (
        "source_payload.metadata.billing.invoice_adjustments.authorization",
    )
    assert authorization.missing_detail_flags == (
        "missing_authorizer",
        "missing_authorization_threshold",
    )
    assert result.summary["missing_detail_flags"] == [
        "missing_authorizer",
        "missing_authorization_threshold",
    ]
    assert implementation_result.source_id == "impl-adjustment"
    assert "architecture_notes" in _record(implementation_result, "credit_memo").source_fields
    assert "risks[0]" in _record(implementation_result, "tax_recalculation").source_fields
    assert _record(implementation_result, "accounting_export").source_fields == (
        "definition_of_done[0]",
    )


def test_deduplication_confidence_ordering_and_raw_text_are_deterministic():
    result = build_source_invoice_adjustment_requirements(
        _source_brief(
            source_payload={
                "requirements": [
                    "Invoice adjustment approval must capture finance approver and amount threshold.",
                    "invoice adjustment approval must capture finance approver and amount threshold",
                    "Credit memo posting.",
                ],
                "metadata": {
                    "credit_memo": "Credit memo posting.",
                    "audit_trail": "Audit trail records who changed invoice adjustment records and when.",
                },
            }
        )
    )
    text_result = build_source_invoice_adjustment_requirements(
        "Credit memo adjustments require controller approval and an audit trail."
    )

    assert {record.category for record in result.records} == {
        "adjustment_authorization",
        "credit_memo",
        "audit_trail",
    }
    assert _record(result, "adjustment_authorization").evidence == (
        "source_payload.requirements[0]: Invoice adjustment approval must capture finance approver and amount threshold.",
    )
    assert (
        "source_payload.metadata.credit_memo: Credit memo posting."
        in _record(result, "credit_memo").evidence
    )
    assert result.records[0].confidence >= result.records[1].confidence >= result.records[2].confidence
    assert text_result.source_id is None
    assert {"adjustment_authorization", "credit_memo", "audit_trail"} <= {
        record.category for record in text_result.records
    }


def test_serialization_helpers_markdown_models_and_source_immutability_are_stable():
    source = _source_brief(
        source_id="source-adjustment-model",
        summary="Invoice adjustments need approval before credit memo posting.",
        source_payload={
            "requirements": [
                "Customer notification must be sent after invoice adjustment approval."
            ],
            "metadata": {
                "audit_trail": "Audit trail records who changed adjusted invoices and when."
            },
        },
    )
    original = copy.deepcopy(source)
    mapping_result = build_source_invoice_adjustment_requirements(source)
    model_result = generate_source_invoice_adjustment_requirements(SourceBrief.model_validate(source))
    derived = derive_source_invoice_adjustment_requirements(SourceBrief.model_validate(source))
    extracted = extract_source_invoice_adjustment_requirements(SourceBrief.model_validate(source))
    payload = source_invoice_adjustment_requirements_to_dict(model_result)
    markdown = source_invoice_adjustment_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert extracted == model_result.requirements
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.to_dicts() == payload["requirements"]
    assert source_invoice_adjustment_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_invoice_adjustment_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_invoice_adjustment_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "category",
        "confidence",
        "evidence",
        "source_fields",
        "missing_detail_flags",
        "suggested_owner",
        "suggested_planning_note",
    ]
    assert markdown.startswith(
        "# Source Invoice Adjustment Requirements Report: source-adjustment-model"
    )
    assert "| Category | Confidence | Source Fields | Missing Details | Evidence | Suggested Owner | Suggested Planning Note |" in markdown


def test_unrelated_refund_only_and_explicit_out_of_scope_language_are_ignored():
    refund_only = build_source_invoice_adjustment_requirements(
        _source_brief(
            source_id="refund-only",
            summary="Refund requests must notify customers and require support approval.",
            source_payload={"requirements": ["Refund workflow should sync refund status to accounting."]},
        )
    )
    mixed = build_source_invoice_adjustment_requirements(
        _source_brief(
            source_id="mixed",
            summary="Refunds are handled elsewhere. Invoice adjustments require credit memos for overcharges.",
        )
    )
    out_of_scope = build_source_invoice_adjustment_requirements(
        _source_brief(
            source_id="out-of-scope",
            source_payload={
                "requirements": [
                    "Invoice adjustments and credit memos are out of scope for this release.",
                    "No accounting export changes are required.",
                ]
            },
        )
    )
    empty = build_source_invoice_adjustment_requirements(
        _source_brief(source_id="empty", summary="Update billing page copy.")
    )
    repeat = build_source_invoice_adjustment_requirements(
        _source_brief(source_id="empty", summary="Update billing page copy.")
    )
    invalid = build_source_invoice_adjustment_requirements(42)

    assert refund_only.records == ()
    assert [record.category for record in mixed.records] == ["credit_memo"]
    assert out_of_scope.records == ()
    assert empty.to_dict() == repeat.to_dict()
    assert empty.summary == {
        "requirement_count": 0,
        "category_counts": {
            "adjustment_authorization": 0,
            "credit_memo": 0,
            "tax_recalculation": 0,
            "audit_trail": 0,
            "customer_notification": 0,
            "accounting_export": 0,
        },
        "high_confidence_count": 0,
        "categories": [],
        "missing_detail_flags": [],
        "suggested_owner_counts": {},
    }
    assert "No invoice adjustment requirements were found" in empty.to_markdown()
    assert invalid.source_id is None
    assert invalid.records == ()


def _record(result, category):
    return next(record for record in result.records if record.category == category)


def _source_brief(
    *,
    source_id="source-adjustment",
    title="Invoice adjustment requirements",
    domain="billing",
    summary="General invoice adjustment requirements.",
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
        "id": "impl-adjustment",
        "source_brief_id": "source-adjustment",
        "title": "Invoice adjustment handling",
        "domain": "billing",
        "target_user": "finance admins",
        "buyer": "enterprise",
        "workflow_context": "Normalize invoice adjustments before task generation.",
        "problem_statement": "Finance needs adjustment controls.",
        "mvp_goal": "Capture adjustment and credit memo requirements in the execution plan.",
        "product_surface": "billing",
        "scope": ["Invoices", "Accounting"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": architecture_notes,
        "data_requirements": None,
        "integration_points": [],
        "risks": [] if risks is None else risks,
        "validation_plan": "Run invoice adjustment smoke tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
    }
