import json

from blueprint.task_third_party_webhook_readiness import (
    TaskThirdPartyWebhookReadinessPlan,
    TaskThirdPartyWebhookReadinessRecord,
    analyze_task_third_party_webhook_readiness,
    build_task_third_party_webhook_readiness_plan,
    extract_task_third_party_webhook_readiness,
    generate_task_third_party_webhook_readiness,
    recommend_task_third_party_webhook_readiness,
    task_third_party_webhook_readiness_plan_to_dict,
    task_third_party_webhook_readiness_plan_to_dicts,
    task_third_party_webhook_readiness_plan_to_markdown,
)


def test_ready_third_party_webhook_consumer_has_all_criteria():
    result = build_task_third_party_webhook_readiness_plan(
        _plan(
            [
                _task(
                    "webhook-ready",
                    "Consume Stripe invoice webhooks",
                    (
                        "Build third-party webhook consumer for Stripe invoice.paid and invoice.failed events. "
                        "Signature verification uses HMAC signing secret and timestamp tolerance. "
                        "Idempotency deduplication stores event id and delivery id. "
                        "Retry behavior returns 2xx only after persistence and handles provider redelivery backoff. "
                        "Payload schema handling validates versioned JSON schema and malformed payloads. "
                        "Dead-letter queue supports manual replay. "
                        "Observability includes metrics, logs, dashboard, alerts, and failure reason. "
                        "Security owner is AppSec integration owner. "
                        "Tests cover signature, idempotency, schema, retry, and replay behavior."
                    ),
                    files_or_modules=["src/integrations/stripe/webhooks.py"],
                )
            ]
        )
    )

    record = result.records[0]

    assert isinstance(result, TaskThirdPartyWebhookReadinessPlan)
    assert isinstance(record, TaskThirdPartyWebhookReadinessRecord)
    assert record.readiness == "ready"
    assert record.present_criteria == (
        "provider_event_scope",
        "signature_verification",
        "idempotency_deduplication",
        "retry_behavior",
        "payload_schema_handling",
        "dead_letter_replay",
        "observability",
        "security_owner",
        "validation_evidence",
    )
    assert record.missing_criteria == ()


def test_partial_webhook_consumer_reports_missing_security_replay_observability_and_tests():
    result = analyze_task_third_party_webhook_readiness(
        _plan(
            [
                _task(
                    "webhook-partial",
                    "Handle GitHub provider webhook",
                    "Consume provider webhook push events with signature verification and idempotency by event id.",
                )
            ]
        )
    )

    record = result.records[0]

    assert record.readiness == "partial"
    assert record.present_criteria == (
        "provider_event_scope",
        "signature_verification",
        "idempotency_deduplication",
    )
    assert "retry_behavior" in record.missing_criteria
    assert "payload_schema_handling" in record.missing_criteria
    assert "dead_letter_replay" in record.missing_criteria
    assert "observability" in record.missing_criteria
    assert "security_owner" in record.missing_criteria
    assert "validation_evidence" in record.missing_criteria


def test_absent_no_impact_and_serialization_are_stable():
    plan = _plan(
        [
            _task("webhook-none", "Copy update", "No third-party webhook consumer changes are in scope."),
            _task("copy", "Settings labels", "Adjust label text."),
        ]
    )

    result = recommend_task_third_party_webhook_readiness(plan)
    payload = task_third_party_webhook_readiness_plan_to_dict(result)
    markdown = task_third_party_webhook_readiness_plan_to_markdown(result)

    assert result.records == ()
    assert result.impacted_task_ids == ()
    assert result.ignored_task_ids == ("webhook-none", "copy")
    assert result.summary["impacted_task_count"] == 0
    assert json.loads(json.dumps(payload)) == payload
    assert task_third_party_webhook_readiness_plan_to_dicts(result) == []
    assert extract_task_third_party_webhook_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_third_party_webhook_readiness(plan).to_dict() == result.to_dict()
    assert markdown.startswith("# Task Third Party Webhook Readiness")


def _plan(tasks):
    return {"id": "plan-third-party-webhook", "tasks": tasks}


def _task(task_id, title, description, **extra):
    return {"id": task_id, "title": title, "description": description, **extra}
