import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_user_journeys import (
    SourceUserJourney,
    extract_source_user_journeys,
    source_user_journeys_to_dicts,
    summarize_source_user_journeys,
)


def test_user_can_admin_can_and_named_flows_extract_journey_fields():
    records = extract_source_user_journeys(
        _source_brief(
            title="Checkout flow refresh",
            summary="User can save a draft application when identity verification is pending.",
            source_payload={
                "body": (
                    "## Workflow\n"
                    "- Admin can approve refunds after risk review.\n"
                    "- Customer journey starts when the cart is ready so that buyers complete checkout."
                )
            },
        )
    )

    assert all(isinstance(record, SourceUserJourney) for record in records)
    assert [
        (
            record.journey_name,
            record.actor,
            record.trigger,
            record.expected_outcome,
            record.source_field,
            record.confidence,
        )
        for record in records
    ] == [
        (
            "Checkout Flow",
            "customer",
            "source brief workflow signal",
            "complete checkout",
            "title",
            "high",
        ),
        (
            "Customer Journey",
            "customer",
            "the cart is ready",
            "buyers complete checkout",
            "source_payload.body",
            "high",
        ),
        (
            "User can save a draft application",
            "user",
            "identity verification is pending",
            "save a draft application",
            "summary",
            "medium",
        ),
        (
            "Admin can approve refunds",
            "admin",
            "risk review",
            "approve refunds",
            "source_payload.body",
            "medium",
        ),
    ]
    assert "Customer journey starts" in records[1].evidence[0]


def test_structured_journeys_extract_high_confidence_records_and_serialize():
    records = extract_source_user_journeys(
        _source_brief(
            source_payload={
                "user_journeys": [
                    {
                        "name": "Support escalation workflow",
                        "persona": "Support agent",
                        "trigger": "customer reports failed payment",
                        "outcome": "handoff reaches billing ops",
                        "evidence": "Support workflow covers triage through billing handoff.",
                    },
                    {
                        "flow": "Approval flow",
                        "actor": "Reviewer",
                        "starts_when": "expense exceeds policy",
                        "expected_outcome": "manager approves or rejects the request",
                    },
                ]
            }
        )
    )
    payload = source_user_journeys_to_dicts(records)

    assert [(record.journey_name, record.actor, record.confidence) for record in records] == [
        ("Approval flow", "reviewer", "high"),
        ("Support escalation workflow", "support agent", "high"),
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload[0]) == [
        "journey_name",
        "actor",
        "trigger",
        "expected_outcome",
        "source_field",
        "confidence",
        "evidence",
    ]


def test_duplicate_journey_signals_are_deduped_deterministically():
    records = extract_source_user_journeys(
        _source_brief(
            summary="Checkout flow lets customers complete checkout.",
            source_payload={
                "body": "- Checkout flow lets customers complete checkout.",
                "metadata": {"scenario": "checkout flow lets customers complete checkout."},
            },
        )
    )

    assert [(record.journey_name, record.source_field, record.confidence) for record in records] == [
        ("Checkout Flow", "summary", "high")
    ]


def test_handoff_support_onboarding_and_approval_language_are_detected():
    records = extract_source_user_journeys(
        _source_brief(
            source_payload={
                "goals": [
                    "Onboarding flow begins when an invited teammate accepts the email.",
                    "Support workflow routes tickets to tier two.",
                    "Approval flow is required before purchase order submission.",
                    "Define handoff steps from sales to customer success.",
                ]
            }
        )
    )

    assert [(record.journey_name, record.actor, record.source_field) for record in records] == [
        ("Approval Flow", "approver", "source_payload.goals[2]"),
        ("Handoff Steps", "team", "source_payload.goals[3]"),
        ("Handoff to Customer Success", "sales", "source_payload.goals[3]"),
        ("Onboarding Flow", "new user", "source_payload.goals[0]"),
        ("Support Workflow", "support agent", "source_payload.goals[1]"),
    ]
    assert records[3].trigger == "an invited teammate accepts the email"
    assert records[4].expected_outcome == "resolve the support request"


def test_model_input_matches_mapping_input_without_mutation_and_summarizes():
    source = _source_brief(
        summary="Customer journey should support account recovery.",
        source_payload={"constraints": ["User can reset MFA when device access is lost."]},
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_records = extract_source_user_journeys(source)
    model_records = extract_source_user_journeys(model)

    assert source == original
    assert source_user_journeys_to_dicts(model_records) == source_user_journeys_to_dicts(
        mapping_records
    )
    assert summarize_source_user_journeys(model_records) == {
        "journey_count": 2,
        "confidence_counts": {"high": 1, "medium": 1, "low": 0},
        "actors": ["customer", "user"],
        "journey_names": ["Customer Journey", "User can reset MFA"],
    }


def test_empty_unrelated_or_malformed_sources_return_empty_results():
    assert extract_source_user_journeys(_source_brief(summary="General background only.")) == ()
    assert extract_source_user_journeys(_source_brief(source_payload="not a mapping")) == ()
    assert extract_source_user_journeys("not a source brief") == ()
    assert summarize_source_user_journeys(()) == {
        "journey_count": 0,
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "actors": [],
        "journey_names": [],
    }


def _source_brief(
    *,
    source_id="sb-user-journey",
    title="User journey",
    domain="payments",
    summary="General user journey notes.",
    source_payload=None,
    source_links=None,
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
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }
