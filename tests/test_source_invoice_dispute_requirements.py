import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_invoice_dispute_requirements import (
    SourceInvoiceDisputeRequirement,
    SourceInvoiceDisputeRequirementsReport,
    build_source_invoice_dispute_requirements,
    derive_source_invoice_dispute_requirements,
    extract_source_invoice_dispute_requirements,
    generate_source_invoice_dispute_requirements,
    source_invoice_dispute_requirements_to_dict,
    source_invoice_dispute_requirements_to_dicts,
    source_invoice_dispute_requirements_to_markdown,
    summarize_source_invoice_dispute_requirements,
)


def test_prose_and_structured_fields_extract_invoice_dispute_categories():
    result = build_source_invoice_dispute_requirements(
        _source_brief(
            source_payload={
                "body": """
# Invoice Disputes

- Invoice dispute intake must capture invoice number, dispute reason, and disputed amount.
- Billing correction calculation needs credit memo and charge reversal rules by line item.
- Finance approval is required when adjustment amount exceeds $500.
- Customer notification emails should explain correction status after approval.
- Accounting sync posts journal entries to NetSuite and updates accounts receivable.
- Evidence attachments must retain usage export and contract excerpts for 7 years.
- Audit trail records who approved the correction and the timestamp.
""",
                "metadata": {
                    "invoice_corrections": {
                        "accounting_sync": "Corrected invoices sync to the ledger after approval.",
                    }
                },
            }
        )
    )

    assert isinstance(result, SourceInvoiceDisputeRequirementsReport)
    assert all(isinstance(record, SourceInvoiceDisputeRequirement) for record in result.records)
    assert sorted(record.category for record in result.records) == sorted([
        "dispute_intake",
        "correction_calculation",
        "approval",
        "customer_notification",
        "accounting_sync",
        "evidence",
        "audit_trail",
    ])
    by_category = {record.category: record for record in result.records}
    assert any("source_payload.body" in item for item in by_category["dispute_intake"].evidence)
    assert "source_payload.metadata.invoice_corrections.accounting_sync" in by_category["accounting_sync"].source_fields
    assert by_category["approval"].missing_detail_flags == ()
    assert by_category["accounting_sync"].suggested_owner == "finance_systems"
    assert "ledger posting" in by_category["accounting_sync"].suggested_planning_note
    assert result.summary["requirement_count"] == 7
    assert result.summary["category_counts"]["evidence"] == 1
    assert result.summary["high_confidence_count"] >= 4


def test_nested_metadata_missing_details_and_implementation_brief_paths_are_reported():
    implementation = ImplementationBrief.model_validate(
        _implementation(
            architecture_notes="Invoice correction service must calculate adjustment deltas and create credit memos.",
            risks=[
                "Billing disputes are risky without customer notification.",
                "Evidence is required for each disputed invoice.",
            ],
            definition_of_done=[
                "Accounting sync writes journal entries for approved corrections.",
                "Audit trail captures invoice correction activity.",
            ],
        )
    )

    result = build_source_invoice_dispute_requirements(
        {
            "id": "nested-dispute",
            "source_payload": {
                "metadata": {
                    "billing": {
                        "dispute_intake": {
                            "fields": "Disputed invoice intake must capture invoice id only."
                        }
                    }
                }
            },
        }
    )
    implementation_result = build_source_invoice_dispute_requirements(implementation)

    intake = _record(result, "dispute_intake")
    assert intake.source_fields == (
        "source_payload.metadata.billing.dispute_intake.fields",
    )
    assert intake.missing_detail_flags == ("missing_dispute_reason",)
    assert result.summary["missing_detail_flags"] == ["missing_dispute_reason"]
    assert implementation_result.source_id == "impl-dispute"
    assert "architecture_notes" in _record(implementation_result, "correction_calculation").source_fields
    assert "risks[0]" in _record(implementation_result, "customer_notification").source_fields
    assert _record(implementation_result, "accounting_sync").source_fields == ("definition_of_done[0]",)


def test_deduplication_and_confidence_ordering_are_deterministic():
    result = build_source_invoice_dispute_requirements(
        _source_brief(
            source_payload={
                "requirements": [
                    "Invoice dispute intake must capture invoice number and dispute reason.",
                    "invoice dispute intake must capture invoice number and dispute reason",
                    "Credit memo correction.",
                ],
                "metadata": {
                    "correction_calculation": "Credit memo correction.",
                    "approval": "Approval is required for invoice correction adjustments over $1000.",
                },
            }
        )
    )

    assert {record.category for record in result.records} == {
        "approval",
        "dispute_intake",
        "correction_calculation",
    }
    assert _record(result, "dispute_intake").evidence == (
        "source_payload.requirements[0]: Invoice dispute intake must capture invoice number and dispute reason.",
    )
    assert (
        "source_payload.metadata.correction_calculation: Credit memo correction."
        in _record(result, "correction_calculation").evidence
    )
    assert result.records[0].confidence >= result.records[1].confidence >= result.records[2].confidence


def test_serialization_helpers_markdown_models_and_source_immutability_are_stable():
    source = _source_brief(
        source_id="source-dispute-model",
        summary="Invoice disputes need approval before credit memo posting.",
        source_payload={
            "requirements": ["Customer notification must be sent after billing correction approval."],
            "metadata": {"audit_trail": "Audit trail records who changed corrected invoices and when."},
        },
    )
    original = copy.deepcopy(source)
    mapping_result = build_source_invoice_dispute_requirements(source)
    model_result = generate_source_invoice_dispute_requirements(SourceBrief.model_validate(source))
    derived = derive_source_invoice_dispute_requirements(SourceBrief.model_validate(source))
    extracted = extract_source_invoice_dispute_requirements(SourceBrief.model_validate(source))
    payload = source_invoice_dispute_requirements_to_dict(model_result)
    markdown = source_invoice_dispute_requirements_to_markdown(model_result)
    text_result = build_source_invoice_dispute_requirements(
        "Charge reversal corrections require finance approval and an audit trail."
    )

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert extracted == model_result.requirements
    assert text_result.source_id is None
    assert {"approval", "audit_trail", "correction_calculation"} <= {
        record.category for record in text_result.records
    }
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.to_dicts() == payload["requirements"]
    assert source_invoice_dispute_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_invoice_dispute_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_invoice_dispute_requirements(model_result) == model_result.summary
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
    assert markdown.startswith("# Source Invoice Dispute Requirements Report: source-dispute-model")
    assert "| Category | Confidence | Source Fields | Missing Details | Evidence | Suggested Owner | Suggested Planning Note |" in markdown


def test_refund_only_and_explicit_out_of_scope_language_are_ignored():
    refund_only = build_source_invoice_dispute_requirements(
        _source_brief(
            source_id="refund-only",
            summary="Refund requests must notify customers and require support approval.",
            source_payload={"requirements": ["Refund workflow should sync refund status to accounting."]},
        )
    )
    mixed = build_source_invoice_dispute_requirements(
        _source_brief(
            source_id="mixed",
            summary="Refunds are handled elsewhere. Invoice corrections require credit memos for overcharges.",
        )
    )
    out_of_scope = build_source_invoice_dispute_requirements(
        _source_brief(
            source_id="out-of-scope",
            source_payload={
                "requirements": [
                    "Invoice disputes and billing corrections are out of scope for this release.",
                    "No credit memo adjustments are required.",
                ]
            },
        )
    )
    empty = build_source_invoice_dispute_requirements(
        _source_brief(source_id="empty", summary="Update billing page copy.")
    )
    repeat = build_source_invoice_dispute_requirements(
        _source_brief(source_id="empty", summary="Update billing page copy.")
    )
    invalid = build_source_invoice_dispute_requirements(42)

    assert refund_only.records == ()
    assert [record.category for record in mixed.records] == ["correction_calculation"]
    assert out_of_scope.records == ()
    assert empty.to_dict() == repeat.to_dict()
    assert empty.summary == {
        "requirement_count": 0,
        "category_counts": {
            "dispute_intake": 0,
            "correction_calculation": 0,
            "approval": 0,
            "customer_notification": 0,
            "accounting_sync": 0,
            "evidence": 0,
            "audit_trail": 0,
        },
        "high_confidence_count": 0,
        "categories": [],
        "missing_detail_flags": [],
        "suggested_owner_counts": {},
    }
    assert "No invoice dispute requirements were found" in empty.to_markdown()
    assert invalid.source_id is None
    assert invalid.records == ()


def _record(result, category):
    return next(record for record in result.records if record.category == category)


def _source_brief(
    *,
    source_id="source-dispute",
    title="Invoice dispute requirements",
    domain="billing",
    summary="General invoice dispute requirements.",
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
        "id": "impl-dispute",
        "source_brief_id": "source-dispute",
        "title": "Invoice dispute handling",
        "domain": "billing",
        "target_user": "finance admins",
        "buyer": "enterprise",
        "workflow_context": "Normalize invoice disputes before task generation.",
        "problem_statement": "Finance needs correction controls.",
        "mvp_goal": "Capture dispute and correction requirements in the execution plan.",
        "product_surface": "billing",
        "scope": ["Invoices", "Accounting"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": architecture_notes,
        "data_requirements": None,
        "integration_points": [],
        "risks": [] if risks is None else risks,
        "validation_plan": "Run billing correction smoke tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
    }
