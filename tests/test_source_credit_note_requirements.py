import json

from blueprint.source_credit_note_requirements import (
    build_source_credit_note_requirements,
    derive_source_credit_note_requirements,
    extract_source_credit_note_requirements,
    generate_source_credit_note_requirements,
    source_credit_note_requirements_to_dict,
    source_credit_note_requirements_to_dicts,
    source_credit_note_requirements_to_markdown,
    summarize_source_credit_note_requirements,
)


def test_extracts_credit_note_categories():
    result = build_source_credit_note_requirements(_source([
        "Credit note reason must capture refund, discount, and billing error reason code categories.",
        "Credit note amount calculation must prorate line item amounts by formula.",
        "Credit note approval threshold must require manager approval over 500.",
        "Credit note invoice linkage must store original invoice id and line item association.",
        "Credit note customer delivery must send a PDF by email and portal notification.",
        "Credit note accounting posting must create ledger journal entries in the ERP.",
        "Credit note tax treatment must reverse VAT and sales tax by jurisdiction.",
        "Credit note audit evidence must record approver, timestamp, attachment, and audit log.",
    ]))

    assert [record.requirement_type for record in result.records] == ["credit_note_reason", "amount_calculation", "approval_threshold", "invoice_linkage", "customer_delivery", "accounting_posting", "tax_treatment", "audit_evidence"]
    assert result.summary["missing_detail_flags"] == []


def test_partial_credit_note_requirements_need_detail():
    result = derive_source_credit_note_requirements("Credit note reason is required. Credit note amount calculation is required. Credit note approval threshold is required. Credit note invoice linkage is required. Credit note customer delivery is required. Credit note accounting posting is required. Credit note tax treatment is required.")

    assert result.summary["missing_detail_flags"] == ["missing_reason", "missing_calculation", "missing_approval", "missing_invoice_linkage", "missing_delivery", "missing_accounting", "missing_tax_treatment"]


def test_helpers_serializers_negation_and_invalid_inputs_are_stable():
    report = extract_source_credit_note_requirements(_source(["Credit note audit evidence must record approver and timestamp evidence."], "credit-model"))
    payload = source_credit_note_requirements_to_dict(report)

    assert generate_source_credit_note_requirements("Credit note tax treatment must reverse VAT by jurisdiction.").summary["requirement_count"] == 1
    assert summarize_source_credit_note_requirements(report)["requirement_count"] == 1
    assert build_source_credit_note_requirements("").records == ()
    assert build_source_credit_note_requirements(3.14).records == ()
    assert build_source_credit_note_requirements("No credit note changes are required.").records == ()
    assert json.loads(json.dumps(payload))["source_id"] == "credit-model"
    assert source_credit_note_requirements_to_dicts(report) == payload["records"]
    assert "Source Credit Note Requirements Report" in source_credit_note_requirements_to_markdown(report)


def _source(lines, source_id="credit-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "source_id": f"{source_id}-upstream", "title": "Credit note", "summary": "Credit note planning", "source_payload": {"requirements": lines}, "source_links": {}}
