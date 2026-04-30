from blueprint.domain.models import SourceBrief
from blueprint.source_acceptance_signals import (
    SourceAcceptanceSignal,
    build_source_acceptance_signals,
    extract_source_acceptance_signals,
    source_acceptance_signals_to_dicts,
)


def test_markdown_checkbox_lines_become_checklist_acceptance_signals():
    signals = extract_source_acceptance_signals(
        _source_brief(
            summary=(
                "Checkout retry acceptance:\n"
                "- [ ] Retry button is shown after failure\n"
                "- [x] Final failure message is visible"
            )
        )
    )

    assert [(signal.text, signal.signal_type, signal.source_field) for signal in signals] == [
        (
            "Checkout retry acceptance:",
            "acceptance",
            "summary",
        ),
        ("Retry button is shown after failure", "checklist", "summary"),
        ("Final failure message is visible", "checklist", "summary"),
    ]
    assert all(isinstance(signal, SourceAcceptanceSignal) for signal in signals)


def test_must_should_and_done_when_sentences_are_classified_separately():
    signals = extract_source_acceptance_signals(
        _source_brief(
            title="Checkout retry",
            summary=(
                "Users must be able to retry failed payment submissions. "
                "The retry action should keep the cart intact. "
                "Done when analytics records the final retry outcome."
            ),
        )
    )

    assert [(signal.text, signal.signal_type, signal.confidence) for signal in signals] == [
        (
            "Users must be able to retry failed payment submissions.",
            "must",
            0.88,
        ),
        (
            "The retry action should keep the cart intact.",
            "should",
            0.82,
        ),
        (
            "Done when analytics records the final retry outcome.",
            "done_when",
            0.9,
        ),
    ]


def test_scans_body_description_checklist_and_normalized_payload_fields_in_source_order():
    signals = extract_source_acceptance_signals(
        _source_brief(
            summary="Retry copy should be concise.",
            source_payload={
                "body": "Payment retry must preserve the selected payment method.",
                "description": "Acceptance: support staff can see retry status.",
                "checklist": ["- [ ] Retry audit row is written"],
                "normalized": {
                    "expectations": [
                        "Done when the retry button disappears after success.",
                    ]
                },
            },
        )
    )

    assert [(signal.signal_type, signal.text, signal.source_field) for signal in signals] == [
        ("should", "Retry copy should be concise.", "summary"),
        (
            "must",
            "Payment retry must preserve the selected payment method.",
            "source_payload.body",
        ),
        (
            "acceptance",
            "Acceptance: support staff can see retry status.",
            "source_payload.description",
        ),
        ("checklist", "Retry audit row is written", "source_payload.checklist[0]"),
        (
            "done_when",
            "Done when the retry button disappears after success.",
            "source_payload.normalized.expectations[0]",
        ),
    ]


def test_duplicate_signals_across_summary_and_payload_are_removed():
    signals = extract_source_acceptance_signals(
        _source_brief(
            summary="Users must be able to retry failed payment submissions.",
            source_payload={
                "description": "Users must be able to retry failed payment submissions.",
                "normalized": {
                    "acceptance": [
                        "Users must be able to retry failed payment submissions.",
                        "Retry status should be visible to support.",
                    ]
                },
            },
        )
    )

    assert [(signal.text, signal.source_field) for signal in signals] == [
        (
            "Users must be able to retry failed payment submissions.",
            "summary",
        ),
        (
            "Retry status should be visible to support.",
            "source_payload.normalized.acceptance[1]",
        ),
    ]


def test_model_inputs_dict_serialization_and_batch_extraction_are_deterministic():
    source_brief = SourceBrief.model_validate(
        _source_brief(
            source_payload={
                "acceptance": ["Done when retry state is persisted."],
            }
        )
    )

    first = build_source_acceptance_signals([source_brief])
    second = build_source_acceptance_signals([source_brief])
    payload = source_acceptance_signals_to_dicts(first)

    assert payload == source_acceptance_signals_to_dicts(second)
    assert payload == [
        {
            "source_brief_id": "sb-checkout-retry",
            "text": "Done when retry state is persisted.",
            "signal_type": "done_when",
            "confidence": 0.9,
            "source_field": "source_payload.acceptance[0]",
        }
    ]


def test_empty_or_malformed_payloads_return_empty_list_without_raising():
    assert extract_source_acceptance_signals(
        _source_brief(summary="General background only.", source_payload={})
    ) == ()
    assert extract_source_acceptance_signals(
        _source_brief(summary="General background only.", source_payload="not a mapping")
    ) == ()
    assert extract_source_acceptance_signals("not a source brief") == ()


def _source_brief(
    *,
    source_id="sb-checkout-retry",
    title="Checkout Retry",
    domain="payments",
    summary="Retry failed payment submissions.",
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
