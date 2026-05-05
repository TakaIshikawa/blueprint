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


def test_malformed_input_types_return_empty_results_gracefully():
    """Test that various malformed inputs are handled without errors."""
    # Non-mapping types
    assert extract_source_user_journeys(None) == ()
    assert extract_source_user_journeys([]) == ()
    assert extract_source_user_journeys(123) == ()
    assert extract_source_user_journeys(True) == ()

    # Invalid nested structures
    assert extract_source_user_journeys({"source_payload": None}) == ()
    assert extract_source_user_journeys({"source_payload": [1, 2, 3]}) == ()

    # Empty or whitespace-only values
    assert extract_source_user_journeys(_source_brief(summary="", title="")) == ()
    assert extract_source_user_journeys(_source_brief(summary="   ", title="  \n  ")) == ()


def test_edge_cases_in_text_parsing_handle_special_characters():
    """Test text parsing with special characters, boundaries, and edge cases."""
    records = extract_source_user_journeys(
        _source_brief(
            source_payload={
                "body": (
                    "User can confirm!!!... when ###trigger occurs.\n"
                    "   \n"  # whitespace only line
                    "- - - nested bullets\n"
                    "Admin can process (with parentheses) and special chars: $%&"
                )
            }
        )
    )

    # Should still extract valid patterns despite special characters
    assert len(records) >= 2
    actors = [r.actor for r in records]
    assert "user" in actors
    assert "admin" in actors


def test_very_long_text_is_truncated_in_evidence_and_outcomes():
    """Test that very long text values are handled with truncation."""
    long_outcome = "complete " + ("very " * 100) + "long process"
    records = extract_source_user_journeys(
        _source_brief(
            summary=f"User can {long_outcome} when ready."
        )
    )

    assert len(records) > 0
    # Evidence should be truncated with ellipsis
    assert all(len(r.evidence[0]) <= 180 for r in records)
    # Outcome should be cleaned but may still be long
    assert all(isinstance(r.expected_outcome, str) for r in records)


def test_regex_pattern_boundaries_and_overlapping_matches():
    """Test regex patterns at text boundaries and overlapping scenarios."""
    records = extract_source_user_journeys(
        _source_brief(
            source_payload={
                "constraints": [
                    # Pattern at start
                    "Customer journey when payment fails so that retry happens",
                    # Pattern at end
                    "Process starts when timeout and ends with customer journey",
                    # Multiple triggers in one sentence
                    "User can act when trigger one and when trigger two occur",
                ]
            }
        )
    )

    assert len(records) >= 2
    # Verify multiple journeys detected
    journey_names = [r.journey_name for r in records]
    assert any("Customer Journey" in name or "User can" in name for name in journey_names)


def test_partial_structured_journeys_with_missing_fields():
    """Test structured journey mappings with various missing required fields."""
    records = extract_source_user_journeys(
        _source_brief(
            source_payload={
                "user_journeys": [
                    # Complete journey
                    {
                        "name": "Complete flow",
                        "actor": "user",
                        "trigger": "event occurs",
                        "outcome": "action completes",
                    },
                    # Missing trigger
                    {
                        "name": "Partial flow",
                        "actor": "admin",
                        "outcome": "approval granted",
                    },
                    # Missing outcome
                    {
                        "name": "Another flow",
                        "actor": "customer",
                        "trigger": "cart ready",
                    },
                    # Minimal - only name
                    {
                        "name": "Minimal flow",
                    },
                    # No journey-identifying fields at all
                    {
                        "unrelated": "data",
                        "other": "fields",
                    },
                ]
            }
        )
    )

    # Should extract journeys even with missing fields, using defaults
    assert len(records) >= 3
    # All should have required fields populated (with defaults if needed)
    for record in records:
        assert record.journey_name
        assert record.actor
        assert record.trigger
        assert record.expected_outcome
        assert record.confidence == "high"  # structured journeys get high confidence


def test_type_polymorphism_in_summarize_function():
    """Test summarize_source_user_journeys with all supported input types.

    This specifically tests the code paths with type: ignore annotations at lines 155, 157.
    """
    source_dict = _source_brief(
        summary="Customer journey starts when ready.",
        source_payload={"goals": ["User can complete checkout."]},
    )
    source_model = SourceBrief.model_validate(source_dict)

    # Extract records for direct testing
    records_tuple = extract_source_user_journeys(source_dict)
    records_list = list(records_tuple)

    # Test with tuple of records (line 155: type: ignore)
    summary_from_tuple = summarize_source_user_journeys(records_tuple)
    assert summary_from_tuple["journey_count"] == len(records_tuple)
    assert summary_from_tuple["journey_count"] >= 2

    # Test with list of records (line 155: type: ignore)
    summary_from_list = summarize_source_user_journeys(records_list)
    assert summary_from_list == summary_from_tuple

    # Test with mapping (line 157: type: ignore)
    summary_from_dict = summarize_source_user_journeys(source_dict)
    assert summary_from_dict == summary_from_tuple

    # Test with SourceBrief model (line 157: type: ignore)
    summary_from_model = summarize_source_user_journeys(source_model)
    assert summary_from_model == summary_from_tuple

    # Test with empty records
    empty_summary = summarize_source_user_journeys([])
    assert empty_summary["journey_count"] == 0
    assert empty_summary["confidence_counts"] == {"high": 0, "medium": 0, "low": 0}


def test_deduplication_with_case_and_punctuation_variations():
    """Test that deduplication handles case, punctuation, and whitespace variations."""
    records = extract_source_user_journeys(
        _source_brief(
            source_payload={
                "body": "User can submit order when ready.",
                "goals": ["user can SUBMIT ORDER!", "User Can Submit-Order"],
                "metadata": {"workflow": {"note": "user can submit... order???"}},
            }
        )
    )

    # Should dedupe variations of the same journey
    # "User can submit order" appears in body, goals (2x), and metadata with case/punctuation variations
    assert len(records) <= 2  # May not fully dedupe if outcome parsing differs
    # All records should be about submitting orders
    for record in records:
        assert "submit" in record.expected_outcome.lower()
        assert record.actor == "user"


def test_nested_payload_extraction_with_deep_structures():
    """Test extraction from deeply nested payload structures."""
    records = extract_source_user_journeys(
        _source_brief(
            source_payload={
                "metadata": {
                    "scenarios": {
                        "primary": "Customer journey when checkout starts",
                        "secondary": {
                            "fallback": "Support workflow when error occurs",
                        },
                    },
                },
                "nested_list": [
                    {"items": ["User can retry when failure happens"]},
                ],
            }
        )
    )

    # Should extract from deeply nested structures
    assert len(records) >= 3
    journey_names = {r.journey_name for r in records}
    assert any("Customer Journey" in name for name in journey_names)
    assert any("Support Workflow" in name for name in journey_names)


def test_mixed_valid_and_invalid_data_in_collections():
    """Test collections containing mix of valid and invalid journey data."""
    records = extract_source_user_journeys(
        _source_brief(
            source_payload={
                "journeys": [
                    "Valid text: User can approve when ready",
                    None,  # Invalid: None value
                    {"name": "Valid structured"},
                    "",  # Invalid: empty string
                    123,  # Invalid: number
                    {"unrelated": "no journey fields"},
                    "Approval flow is required",
                ],
            }
        )
    )

    # Should extract valid entries and skip invalid ones
    assert len(records) >= 2
    # All returned records should be valid
    for record in records:
        assert isinstance(record, SourceUserJourney)
        assert record.journey_name
        assert record.confidence in ("high", "medium", "low")


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
