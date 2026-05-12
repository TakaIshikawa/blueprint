import json

from blueprint.source_webhook_ordering_requirements import (
    build_source_webhook_ordering_requirements,
    extract_source_webhook_ordering_requirements,
    source_webhook_ordering_requirements_to_dict,
    source_webhook_ordering_requirements_to_dicts,
    source_webhook_ordering_requirements_to_markdown,
)


def test_extracts_webhook_ordering_requirements_in_stable_order():
    result = build_source_webhook_ordering_requirements(
        _source(
            source_payload={
                "webhook_ordering": [
                    "Webhook ordering is guaranteed per aggregate and tenant for subscription events.",
                    "Each webhook event includes an aggregate id, ordering key, and monotonic sequence number.",
                    "Consumers must dedupe duplicate events and discard stale out-of-order versions.",
                    "Replay and backfill preserve sequence order for historical redelivery.",
                    "Existing webhook consumers require backward compatible schema versions.",
                    "Monitoring dashboards alert on sequence gaps and out-of-order rate.",
                ]
            }
        )
    )

    assert [record.requirement_type for record in result.records] == [
        "ordering_scope",
        "sequence_metadata",
        "duplicate_out_of_order_handling",
        "replay_backfill_behavior",
        "consumer_compatibility",
        "monitoring_evidence",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["ordering_scope"].source_field == "source_payload.webhook_ordering[0]"
    assert by_type["sequence_metadata"].confidence == "high"
    assert result.summary["requirement_count"] == 6
    assert result.summary["has_ordering_requirements"] is True


def test_detects_sequencing_out_of_order_replay_and_per_entity_language():
    result = build_source_webhook_ordering_requirements(
        _source(
            summary="Webhook sequencing must support per-entity ordering for account updates.",
            requirements=[
                "Out-of-order delivery is buffered until the missing sequence id arrives.",
                "Replay semantics must not break existing consumers.",
            ],
        )
    )

    assert {"ordering_scope", "sequence_metadata", "duplicate_out_of_order_handling", "replay_backfill_behavior", "consumer_compatibility"} <= {
        record.requirement_type for record in result.records
    }
    assert extract_source_webhook_ordering_requirements(_source(summary="Webhook ordering uses sequence metadata."))[0].requirement_type == "sequence_metadata"


def test_retry_only_signing_only_and_negated_webhook_briefs_are_empty():
    retry_only = build_source_webhook_ordering_requirements(
        _source(summary="Webhook retry policy uses exponential backoff and max attempts.")
    )
    signing_only = build_source_webhook_ordering_requirements(
        _source(summary="Webhook signing secret rotation and HMAC signature verification are required.")
    )
    negated = build_source_webhook_ordering_requirements(
        _source(summary="No webhook ordering or sequencing changes are required.")
    )

    assert retry_only.records == ()
    assert signing_only.records == ()
    assert negated.records == ()
    assert retry_only.summary["has_ordering_requirements"] is False


def test_serialization_and_markdown_are_stable():
    result = build_source_webhook_ordering_requirements(_source(summary="Webhook ordering requires per account sequence number monitoring."))
    payload = source_webhook_ordering_requirements_to_dict(result)

    assert source_webhook_ordering_requirements_to_dicts(result) == payload["requirements"]
    assert source_webhook_ordering_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "# Source Webhook Ordering Requirements Report: source-webhook-ordering" in source_webhook_ordering_requirements_to_markdown(result)


def _source(**overrides):
    payload = {
        "id": "source-webhook-ordering",
        "title": "Webhook delivery ordering",
        "summary": "",
        "source_payload": {},
    }
    payload.update(overrides)
    return payload
