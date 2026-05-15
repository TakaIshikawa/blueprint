import json

from blueprint.source_chargeback_management_requirements import (
    build_source_chargeback_management_requirements,
    derive_source_chargeback_management_requirements,
    extract_source_chargeback_management_requirements,
    generate_source_chargeback_management_requirements,
    source_chargeback_management_requirements_to_dict,
    source_chargeback_management_requirements_to_dicts,
    source_chargeback_management_requirements_to_markdown,
    summarize_source_chargeback_management_requirements,
)


def test_extracts_chargeback_categories_from_structured_fields():
    result = build_source_chargeback_management_requirements(_source({
        "billing": ["Chargeback management dispute intake must create a case from processor webhooks.", "Chargeback management fee tracking must post processor fee amount to the ledger."],
        "support": ["Chargeback management evidence collection must include receipt, invoice, usage log, and customer message.", "Chargeback management customer account hold must suspend entitlement access until review."],
        "processor": ["Chargeback management processor integration must sync Stripe dispute status through webhooks.", "Chargeback management representment deadline must submit by 10 days before processor cutoff."],
        "compliance": ["Chargeback management win loss reporting must show win rate and reason code dashboard.", "Chargeback management audit trail must record actor, timestamp, and decision events."],
    }))

    assert [record.requirement_type for record in result.records] == ["dispute_intake", "evidence_collection", "representment_deadline", "processor_integration", "customer_account_hold", "fee_tracking", "win_loss_reporting", "audit_trail"]
    assert result.summary["missing_detail_flags"] == []


def test_partial_chargeback_requirements_need_detail():
    result = derive_source_chargeback_management_requirements("Chargeback management evidence collection is required. Chargeback management representment deadline is required. Chargeback management processor integration is required. Chargeback management customer account hold is required. Chargeback management fee tracking is required. Chargeback management win loss reporting is required.")

    assert result.summary["missing_detail_flags"] == ["missing_evidence", "missing_deadline", "missing_processor_integration", "missing_account_impact", "missing_fee", "missing_reporting"]


def test_helpers_serializers_negation_and_invalid_inputs_are_stable():
    report = extract_source_chargeback_management_requirements(_source({"processor": ["Chargeback management dispute intake must create a processor case."]}, "chargeback-model"))
    payload = source_chargeback_management_requirements_to_dict(report)

    assert generate_source_chargeback_management_requirements("Chargeback management audit trail must record audit log events.") .summary["requirement_count"] == 1
    assert summarize_source_chargeback_management_requirements(report)["requirement_count"] == 1
    assert build_source_chargeback_management_requirements("").records == ()
    assert build_source_chargeback_management_requirements(3.14).records == ()
    assert build_source_chargeback_management_requirements("No chargeback management changes are required.").records == ()
    assert json.loads(json.dumps(payload))["source_id"] == "chargeback-model"
    assert source_chargeback_management_requirements_to_dicts(report) == payload["records"]
    assert "Source Chargeback Management Requirements Report" in source_chargeback_management_requirements_to_markdown(report)


def _source(payload, source_id="chargeback-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "source_id": f"{source_id}-upstream", "title": "Chargeback management", "summary": "Chargeback management planning", "source_payload": payload, "source_links": {}}
