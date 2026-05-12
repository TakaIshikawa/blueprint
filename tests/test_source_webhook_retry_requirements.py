import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import SourceBrief
from blueprint.source_webhook_retry_requirements import (
    SourceWebhookRetryRequirement,
    SourceWebhookRetryRequirementsReport,
    build_source_webhook_retry_requirements,
    derive_source_webhook_retry_requirements,
    extract_source_webhook_retry_requirements,
    generate_source_webhook_retry_requirements,
    source_webhook_retry_requirements_to_dict,
    source_webhook_retry_requirements_to_dicts,
    source_webhook_retry_requirements_to_markdown,
    summarize_source_webhook_retry_requirements,
)


def test_extracts_webhook_retry_requirements_in_stable_order():
    result = build_source_webhook_retry_requirements(
        _source_brief(
            source_payload={
                "webhook_retry": {
                    "policy": "Webhook retry policy must redeliver failed webhook deliveries.",
                    "attempts": "Stop after 5 attempts.",
                    "backoff": "Use exponential backoff with full jitter.",
                    "idempotency": "Receivers must dedupe using the delivery id idempotency key.",
                    "dlq": "Exhausted webhook deliveries move to a dead-letter queue.",
                    "status": "Delivery status visibility shows attempt history and failure reason.",
                    "replay": "Manual replay window keeps events for 7 days.",
                    "ordering": "Ordering is not guaranteed during retry or replay.",
                    "provider": "Stripe provider-specific evidence says retries continue for 3 days.",
                }
            }
        )
    )

    assert isinstance(result, SourceWebhookRetryRequirementsReport)
    assert all(isinstance(record, SourceWebhookRetryRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "retry_policy",
        "max_attempts",
        "backoff_strategy",
        "idempotency_key",
        "dead_letter_handling",
        "delivery_status_visibility",
        "replay_window",
        "ordering_caveat",
        "provider_specific_evidence",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["max_attempts"].value == "5 attempts"
    assert by_type["backoff_strategy"].value == "exponential backoff"
    assert by_type["dead_letter_handling"].source_field == "source_payload.webhook_retry.dlq"
    assert result.summary["delivery_reliability_coverage"] == 100
    assert result.summary["replay_coverage"] == 100
    assert result.summary["observability_coverage"] == 100


def test_mapping_and_source_brief_inputs_are_equivalent_without_mutation():
    source = _source_brief(
        source_payload={
            "requirements": [
                "Webhook retry policy must retry 500 responses with exponential backoff.",
                "Webhook delivery status must show success and failure attempt history.",
            ],
            "metadata": {"replay": "Manual replay window must allow resend webhook for 24 hours."},
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_webhook_retry_requirements(source)
    model_result = derive_source_webhook_retry_requirements(model)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert extract_source_webhook_retry_requirements(model) == model_result.requirements
    assert generate_source_webhook_retry_requirements(model).to_dict() == model_result.to_dict()
    assert summarize_source_webhook_retry_requirements(mapping_result) == mapping_result.summary


def test_evidence_is_deduped_bounded_sorted_and_serializable():
    result = build_source_webhook_retry_requirements(
        _source_brief(
            source_payload={
                "a": "Webhook retry policy must retry 500 responses.",
                "b": "Webhook retry policy must retry 500 responses.",
                "c": "Webhook retry policy retries 503 responses.",
                "d": "Webhook retry policy retries network failures.",
                "e": "Webhook retry policy retries timeouts.",
                "f": "Webhook retry policy retries rate limits.",
                "g": "Webhook retry policy retries transient provider failures.",
            }
        )
    )
    record = result.records[0]
    payload = source_webhook_retry_requirements_to_dict(result)

    assert len(record.evidence) == 5
    assert record.evidence == tuple(sorted(record.evidence, key=str.casefold))
    assert source_webhook_retry_requirements_to_dicts(result) == payload["requirements"]
    assert source_webhook_retry_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert "| Type | Confidence |" in source_webhook_retry_requirements_to_markdown(result)


def test_empty_negated_and_object_inputs_are_stable():
    empty = build_source_webhook_retry_requirements(_source_brief(summary="Update billing copy."))
    negated = build_source_webhook_retry_requirements(
        _source_brief(summary="No webhook retry or replay changes are in scope.")
    )
    obj = build_source_webhook_retry_requirements(
        SimpleNamespace(
            id="object-webhook",
            summary="Webhook retry policy must use exponential backoff.",
            source_payload={"retry": "Delivery status visibility must show failures."},
        )
    )

    assert empty.records == ()
    assert negated.records == ()
    assert empty.summary["status"] == "no_webhook_retry_language"
    assert [record.requirement_type for record in obj.records] == [
        "retry_policy",
        "backoff_strategy",
        "delivery_status_visibility",
    ]


def _source_brief(**overrides):
    payload = {
        "id": "source-webhook-retry",
        "source_project": "blueprint",
        "source_entity_type": "brief",
        "source_id": "source-webhook-retry",
        "source_links": {},
        "title": "Webhook retry source",
        "summary": "Webhook delivery reliability.",
        "source_payload": {},
    }
    payload.update(overrides)
    return payload
