import json

from blueprint.source_payment_method_expiration_requirements import (
    build_source_payment_method_expiration_requirements,
    derive_source_payment_method_expiration_requirements,
    extract_source_payment_method_expiration_requirements,
    generate_source_payment_method_expiration_requirements,
    source_payment_method_expiration_requirements_to_dict,
    source_payment_method_expiration_requirements_to_dicts,
    source_payment_method_expiration_requirements_to_markdown,
    summarize_source_payment_method_expiration_requirements,
)


def test_extracts_payment_method_expiration_categories():
    result = build_source_payment_method_expiration_requirements(_source([
        "Payment method expiration detection must run a daily scan for cards that expire in the current month.",
        "Payment method expiration pre-expiry notification must send email 14 days before expiration.",
        "Payment method expiration update link must use a secure hosted link to the billing portal.",
        "Payment method expiration retry after expiry must reattempt after update on the next invoice.",
        "Payment method expiration account grace period must last 7 days.",
        "Payment method expiration subscription impact must suspend access when renewal status fails after grace.",
        "Payment method expiration processor sync must consume Stripe card updater webhooks.",
        "Payment method expiration recovery metrics must track updated card rate on a dashboard.",
    ]))

    assert [record.requirement_type for record in result.records] == ["expiration_detection", "pre_expiry_notification", "update_link", "retry_after_expiry", "account_grace_period", "subscription_impact", "processor_sync", "recovery_metrics"]
    assert result.summary["missing_detail_flags"] == []


def test_partial_expiration_requirements_need_detail_and_avoid_failure_recovery_terms():
    result = derive_source_payment_method_expiration_requirements("Payment method expiration pre-expiry notification is required. Payment method expiration update link is required. Payment method expiration grace period is required. Payment method expiration processor sync is required. Payment method expiration subscription impact is required.")

    assert result.summary["missing_detail_flags"] == ["missing_notification_timing", "missing_update_path", "missing_grace_period", "missing_processor_sync", "missing_subscription_impact"]
    assert build_source_payment_method_expiration_requirements("Payment failure recovery retry schedule is required for dunning.").records == ()


def test_helpers_serializers_negation_and_invalid_inputs_are_stable():
    report = extract_source_payment_method_expiration_requirements(_source(["Payment method expiration recovery metrics must track card update conversion."], "expiry-model"))
    payload = source_payment_method_expiration_requirements_to_dict(report)

    assert generate_source_payment_method_expiration_requirements("Payment method expiration detection must detect expired card month and year.") .summary["requirement_count"] == 1
    assert summarize_source_payment_method_expiration_requirements(report)["requirement_count"] == 1
    assert build_source_payment_method_expiration_requirements("").records == ()
    assert build_source_payment_method_expiration_requirements(3.14).records == ()
    assert build_source_payment_method_expiration_requirements("No payment method expiration changes are required.").records == ()
    assert json.loads(json.dumps(payload))["source_id"] == "expiry-model"
    assert source_payment_method_expiration_requirements_to_dicts(report) == payload["records"]
    assert "Source Payment Method Expiration Requirements Report" in source_payment_method_expiration_requirements_to_markdown(report)


def _source(lines, source_id="expiry-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "source_id": f"{source_id}-upstream", "title": "Payment method expiration", "summary": "Payment method expiration planning", "source_payload": {"requirements": lines}, "source_links": {}}
