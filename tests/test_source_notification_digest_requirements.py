import json

from blueprint.source_notification_digest_requirements import (
    build_source_notification_digest_requirements,
    derive_source_notification_digest_requirements,
    extract_source_notification_digest_requirements,
    generate_source_notification_digest_requirements,
    source_notification_digest_requirements_to_dict,
    source_notification_digest_requirements_to_dicts,
    source_notification_digest_requirements_to_markdown,
    summarize_source_notification_digest_requirements,
)


def test_extracts_all_notification_digest_categories():
    result = build_source_notification_digest_requirements(_source([
        "Notification digest schedule must send weekly at 09:00 by time zone.",
        "Notification digest recipient segmentation must target role, plan, and tenant segments.",
        "Notification digest channel delivery must support email and Slack channels.",
        "Notification digest grouping and deduplication must group by entity and collapse duplicates.",
        "Notification digest preference controls must let users set frequency and opt out in settings.",
        "Notification digest unsubscribe suppression must honor global unsubscribe and complaint suppressions.",
        "Notification digest preview testing must provide sample preview and test send.",
        "Notification digest delivery metrics must track delivered, open rate, click rate, and bounce metrics.",
    ]))

    assert [record.requirement_type for record in result.records] == ["digest_schedule", "recipient_segmentation", "channel_delivery", "grouping_deduplication", "preference_controls", "unsubscribe_suppression", "preview_testing", "delivery_metrics"]
    assert result.summary["missing_detail_flags"] == []


def test_partial_brief_flags_schedule_preference_and_suppression_details():
    result = derive_source_notification_digest_requirements("Notification digest schedule is required. Notification digest preference controls are required. Notification digest unsubscribe suppression is required.")

    assert result.summary["missing_detail_flags"] == ["missing_schedule", "missing_preference_controls", "missing_suppression"]


def test_serializers_and_public_aliases_match_api_shape():
    result = extract_source_notification_digest_requirements(_source(["Notification digest channel delivery must use email channels."], "digest-1"))
    payload = source_notification_digest_requirements_to_dict(result)

    assert generate_source_notification_digest_requirements("Notification digest preview testing must allow a sample test send.").summary["requirement_count"] == 1
    assert summarize_source_notification_digest_requirements(result)["requirement_count"] == 1
    assert json.loads(json.dumps(payload))["source_id"] == "digest-1"
    assert source_notification_digest_requirements_to_dicts(result) == payload["records"]
    assert source_notification_digest_requirements_to_dicts(result.records) == payload["records"]
    assert "# Source Notification Digest Requirements Report: digest-1" in source_notification_digest_requirements_to_markdown(result)
    assert build_source_notification_digest_requirements("No notification digest changes are required.").records == ()


def _source(lines, source_id="digest-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "title": "Notification digest", "summary": "Notification digest planning", "source_payload": {"requirements": lines}, "source_links": {}}
