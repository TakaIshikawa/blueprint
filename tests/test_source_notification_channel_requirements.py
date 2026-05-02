import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_notification_channel_requirements import (
    SourceNotificationChannelRequirement,
    SourceNotificationChannelRequirementsReport,
    build_source_notification_channel_requirements,
    derive_source_notification_channel_requirements,
    extract_source_notification_channel_requirements,
    generate_source_notification_channel_requirements,
    source_notification_channel_requirements_to_dict,
    source_notification_channel_requirements_to_dicts,
    source_notification_channel_requirements_to_markdown,
    summarize_source_notification_channel_requirements,
)


def test_extracts_mixed_markdown_notification_channel_requirements():
    result = build_source_notification_channel_requirements(
        _source_brief(
            source_payload={
                "requirements": """
                - Email must notify affected customers within 15 minutes using localized incident copy.
                - SMS must alert account owners immediately and include STOP keyword preference handling.
                - Push notifications should notify mobile users when export completes.
                - In-app notification center must show admins a banner notification after billing failure.
                - Slack and Microsoft Teams must page on-call operators when sync fails.
                - Webhook event delivery must notify partners after invoice payment.
                - Status page must publish customer-facing copy before planned maintenance.
                - Support ticket creation must open a Zendesk support case for the support team within 1 hour.
                """
            }
        )
    )

    assert isinstance(result, SourceNotificationChannelRequirementsReport)
    assert result.source_id == "sb-notifications"
    assert all(isinstance(record, SourceNotificationChannelRequirement) for record in result.records)
    assert [record.channel for record in result.records] == [
        "email",
        "sms",
        "push",
        "in_app",
        "slack_teams",
        "webhook",
        "status_page",
        "support_ticket",
    ]
    by_channel = {record.channel: record for record in result.records}
    assert by_channel["email"].timing == "within 15 minutes"
    assert by_channel["email"].audience == "affected customers"
    assert by_channel["email"].locale == "localized"
    assert by_channel["sms"].unsubscribe_or_preference == "STOP keyword"
    assert by_channel["push"].timing == "when export completes"
    assert by_channel["support_ticket"].audience == "support team"
    assert by_channel["support_ticket"].source_field == "source_payload.requirements"
    assert result.summary["requirement_count"] == 8
    assert result.summary["channel_counts"] == {
        "email": 1,
        "sms": 1,
        "push": 1,
        "in_app": 1,
        "slack_teams": 1,
        "webhook": 1,
        "status_page": 1,
        "support_ticket": 1,
    }


def test_structured_metadata_source_brief_and_object_inputs_are_supported():
    source = _source_brief(
        source_payload={
            "notification_channels": [
                {
                    "channel": "email",
                    "timing": "within 30 minutes",
                    "audience": "customers",
                    "template": "renewal reminder",
                    "locale": "en-US and fr-FR",
                    "preferences": "unsubscribe link required",
                },
                {
                    "channel": "Slack",
                    "timing": "immediately",
                    "audience": "operators",
                    "copy": "quota warning",
                },
            ]
        }
    )
    model_result = build_source_notification_channel_requirements(SourceBrief.model_validate(source))
    object_result = build_source_notification_channel_requirements(
        SimpleNamespace(
            id="object-notifications",
            summary="Webhook notifications must alert partners within 5 minutes.",
        )
    )

    assert [record.channel for record in model_result.records] == ["email", "slack_teams"]
    assert model_result.records[0].timing == "within 30 minutes"
    assert model_result.records[0].template_copy == "renewal reminder"
    assert model_result.records[0].locale == "locale, en-US, fr-FR"
    assert model_result.records[0].unsubscribe_or_preference == "preferences"
    assert any("source_payload.notification_channels[0]" in item for item in model_result.records[0].evidence)
    assert object_result.records[0].channel == "webhook"


def test_implementation_brief_input_and_timing_value_extraction():
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Email must notify customers at least 2 days before subscription renewal.",
                "Status page should publish updates after incident commander approval.",
            ],
            definition_of_done=[
                "SMS alerts must notify admins within 10 minutes of fraud lockout.",
            ],
        )
    )

    result = extract_source_notification_channel_requirements(brief)
    by_channel = {record.channel: record for record in result.records}

    assert result.source_id == "impl-notifications"
    assert by_channel["email"].timing == "at least 2 days before"
    assert by_channel["sms"].timing == "within 10 minutes"
    assert by_channel["status_page"].timing == "after incident commander approval"


def test_multichannel_deduplication_and_summary_counts_are_stable():
    result = build_source_notification_channel_requirements(
        _source_brief(
            source_payload={
                "requirements": [
                    "Email and SMS must notify customers within 10 minutes.",
                    "Email must notify customers within 10 minutes.",
                    "SMS must notify customers within 10 minutes.",
                    "Webhook must notify partners when billing changes.",
                ]
            }
        )
    )

    assert [record.channel for record in result.records] == ["email", "sms", "webhook"]
    assert result.summary["requirement_count"] == 3
    assert result.summary["confidence_counts"] == {"high": 3, "medium": 0, "low": 0}
    assert result.summary["channels"] == ["email", "sms", "webhook"]


def test_aliases_serialization_markdown_ordering_and_no_mutation():
    source = _source_brief(
        source_id="notifications-model",
        source_payload={
            "requirements": [
                "Webhook must notify partners when payment succeeds.",
                "Email must notify customers within 15 minutes with localized copy.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_notification_channel_requirements(source)
    model_result = generate_source_notification_channel_requirements(model)
    derived = derive_source_notification_channel_requirements(model)
    payload = source_notification_channel_requirements_to_dict(model_result)
    markdown = source_notification_channel_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_notification_channel_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_notification_channel_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_notification_channel_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_notification_channel_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "channel",
        "requirement_text",
        "timing",
        "audience",
        "template_copy",
        "locale",
        "unsubscribe_or_preference",
        "source_field",
        "evidence",
        "confidence",
        "planning_note",
    ]
    assert [record.channel for record in model_result.records] == ["email", "webhook"]
    assert model_result.records[0].notification_channel == "email"
    assert model_result.records[0].planning_notes == model_result.records[0].planning_note
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Channel | Requirement | Timing | Audience | Template / Copy | Locale | Preference | Source Field | Confidence | Planning Note | Evidence |" in markdown
    assert "Webhook must notify partners" in markdown


def test_empty_no_signal_and_malformed_inputs_return_deterministic_empty_reports():
    empty = build_source_notification_channel_requirements(
        _source_brief(summary="Improve onboarding copy.", source_payload={})
    )
    no_signal = build_source_notification_channel_requirements(
        _source_brief(summary="No notification channel changes are required.", source_payload={})
    )
    malformed = build_source_notification_channel_requirements({"id": "brief-empty", "source_payload": {"notes": object()}})
    invalid = build_source_notification_channel_requirements(42)

    assert empty.records == ()
    assert empty.requirements == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "channel_counts": {
            "email": 0,
            "sms": 0,
            "push": 0,
            "in_app": 0,
            "slack_teams": 0,
            "webhook": 0,
            "status_page": 0,
            "support_ticket": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "channels": [],
    }
    assert "No notification channel requirements were found" in empty.to_markdown()
    assert no_signal.records == ()
    assert malformed.records == ()
    assert invalid.records == ()


def _source_brief(
    *,
    source_id="sb-notifications",
    title="Notification requirements",
    domain="communications",
    summary="General notification requirements.",
    source_payload=None,
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
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "impl-notifications",
        "source_brief_id": "source-notifications",
        "title": "Notification rollout",
        "domain": "communications",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need notification channel requirements before task generation.",
        "problem_statement": "Notification requirements need to be extracted early.",
        "mvp_goal": "Plan notification work from source briefs.",
        "product_surface": "notifications",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review generated plan for notification coverage.",
        "definition_of_done": (
            ["Notification channel requirements are represented."]
            if definition_of_done is None
            else definition_of_done
        ),
        "status": "draft",
        "created_at": None,
        "updated_at": None,
    }
