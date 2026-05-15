import json

from blueprint.source_cash_application_requirements import (
    build_source_cash_application_requirements,
    derive_source_cash_application_requirements,
    extract_source_cash_application_requirements,
    generate_source_cash_application_requirements,
    source_cash_application_requirements_to_dict,
    source_cash_application_requirements_to_dicts,
    source_cash_application_requirements_to_markdown,
    summarize_source_cash_application_requirements,
)


def test_extracts_cash_application_categories_from_structured_sections():
    result = build_source_cash_application_requirements(_source({
        "finance": ["Cash application payment matching must auto match by invoice number, customer id, and amount tolerance.", "Cash application unapplied cash queue must age unmatched cash for review."],
        "remittance": ["Cash application remittance ingestion must import lockbox, EDI, email, and ACH addenda sources.", "Cash application short payment handling must create deduction reason codes within tolerance."],
        "bank": ["Cash application bank reconciliation must reconcile daily against bank statement deposit id.", "Cash application overpayment handling must apply forward customer credit by threshold."],
        "accounting": ["Cash application exception workflow must assign an owner, SLA, and escalation queue.", "Cash application accounting sync must post ERP ledger journal entries."],
    }))

    assert [record.requirement_type for record in result.records] == ["payment_matching", "remittance_ingestion", "unapplied_cash_queue", "short_payment_handling", "overpayment_handling", "bank_reconciliation", "exception_workflow", "accounting_sync"]
    assert result.summary["missing_detail_flags"] == []


def test_partial_cash_application_requirements_need_detail():
    result = derive_source_cash_application_requirements("Cash application payment matching is required. Cash application remittance ingestion is required. Cash application exception workflow is required. Cash application bank reconciliation is required. Cash application accounting sync is required.")

    assert result.summary["missing_detail_flags"] == ["missing_matching_rules", "missing_remittance_sources", "missing_exception_ownership", "missing_reconciliation", "missing_accounting_sync"]


def test_helpers_serializers_negation_and_invalid_inputs_are_stable():
    report = extract_source_cash_application_requirements(_source({"finance": ["Cash application overpayment handling must apply forward customer credit."]}, "cash-model"))
    payload = source_cash_application_requirements_to_dict(report)

    assert generate_source_cash_application_requirements("Cash application accounting sync must post GL journal entries.").summary["requirement_count"] == 1
    assert summarize_source_cash_application_requirements(report)["requirement_count"] == 1
    assert build_source_cash_application_requirements("").records == ()
    assert build_source_cash_application_requirements(3.14).records == ()
    assert build_source_cash_application_requirements("No cash application changes are required.").records == ()
    assert json.loads(json.dumps(payload))["source_id"] == "cash-model"
    assert source_cash_application_requirements_to_dicts(report) == payload["records"]
    assert "Source Cash Application Requirements Report" in source_cash_application_requirements_to_markdown(report)


def _source(payload, source_id="cash-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "source_id": f"{source_id}-upstream", "title": "Cash application", "summary": "Cash application planning", "source_payload": payload, "source_links": {}}
