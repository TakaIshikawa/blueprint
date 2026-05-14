import json

from blueprint.source_payment_failure_recovery_requirements import (
    build_source_payment_failure_recovery_requirements,
    derive_source_payment_failure_recovery_requirements,
    extract_source_payment_failure_recovery_requirements,
    generate_source_payment_failure_recovery_requirements,
    source_payment_failure_recovery_requirements_to_dict,
    source_payment_failure_recovery_requirements_to_dicts,
    source_payment_failure_recovery_requirements_to_markdown,
    summarize_source_payment_failure_recovery_requirements,
)


def test_extracts_all_payment_failure_recovery_categories():
    result = build_source_payment_failure_recovery_requirements(_source([
        "Payment failure recovery failure classification must separate hard decline, soft decline, and expired card reason codes.",
        "Payment failure recovery retry schedule must retry after 1 day and 3 days.",
        "Payment failure recovery payment method update must send a hosted billing portal link.",
        "Payment failure recovery customer notification must send email and in-app message templates.",
        "Payment failure recovery dunning state must track past due, grace, suspended, and canceled states.",
        "Payment failure recovery grace period must last 7 days.",
        "Payment failure recovery subscription impact must suspend entitlement access after grace.",
        "Payment failure recovery metrics must report recovered revenue and collection rate.",
    ]))

    assert [record.requirement_type for record in result.records] == ["failure_classification", "retry_schedule", "payment_method_update", "customer_notification", "dunning_state", "grace_period", "subscription_impact", "recovery_metrics"]
    assert result.summary["missing_detail_flags"] == []


def test_partial_brief_flags_retry_notification_and_subscription_impact():
    result = derive_source_payment_failure_recovery_requirements("Payment failure recovery retry schedule is required. Payment failure recovery customer notification is required. Payment failure recovery subscription impact is required.")

    assert result.summary["missing_detail_flags"] == ["missing_retry_schedule", "missing_notification", "missing_subscription_impact"]


def test_aliases_serializers_and_json_safe_output():
    result = extract_source_payment_failure_recovery_requirements(_source(["Payment failure recovery payment method update must use a billing portal."], "pay-1"))
    payload = source_payment_failure_recovery_requirements_to_dict(result)

    assert generate_source_payment_failure_recovery_requirements("Payment failure recovery grace period must last 3 days.").summary["requirement_count"] == 1
    assert summarize_source_payment_failure_recovery_requirements(result)["requirement_count"] == 1
    assert json.loads(json.dumps(payload, sort_keys=True))["source_id"] == "pay-1"
    assert source_payment_failure_recovery_requirements_to_dicts(result) == payload["records"]
    assert source_payment_failure_recovery_requirements_to_dicts(result.records) == payload["records"]
    assert "Source Payment Failure Recovery Requirements Report" in source_payment_failure_recovery_requirements_to_markdown(result)
    assert build_source_payment_failure_recovery_requirements("No payment failure recovery changes are required.").records == ()


def _source(lines, source_id="pay-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "title": "Payment failure recovery", "summary": "Payment failure recovery planning", "source_payload": {"requirements": lines}, "source_links": {}}
