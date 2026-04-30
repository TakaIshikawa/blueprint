from datetime import datetime, timezone
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_evidence_index import (
    SourceEvidenceEntry,
    build_source_evidence_index,
    source_evidence_index_to_dicts,
)


def test_plain_text_summary_title_domain_and_timestamps_become_evidence_entries():
    entries = build_source_evidence_index(
        [
            _source_brief(
                title="Checkout Retry",
                domain="payments",
                summary="Retry failed payment submissions.",
                source_links={"html_url": "https://example.test/briefs/checkout"},
                created_at=datetime(2026, 5, 1, 3, 4, 5, tzinfo=timezone.utc),
            )
        ]
    )

    assert [(entry.kind, entry.label, entry.value, entry.link) for entry in entries] == [
        (
            "title",
            "title",
            "Checkout Retry",
            "https://example.test/briefs/checkout",
        ),
        (
            "summary",
            "summary",
            "Retry failed payment submissions.",
            "https://example.test/briefs/checkout",
        ),
        ("domain", "domain", "payments", "https://example.test/briefs/checkout"),
        (
            "timestamp",
            "created_at",
            "2026-05-01T03:04:05",
            "https://example.test/briefs/checkout",
        ),
        (
            "source_link",
            "source_link.html_url",
            "https://example.test/briefs/checkout",
            "https://example.test/briefs/checkout",
        ),
    ]
    assert all(isinstance(entry, SourceEvidenceEntry) for entry in entries)
    assert entries[0].to_dict() == {
        "evidence_id": entries[0].evidence_id,
        "source_brief_id": "sb-checkout-retry",
        "kind": "title",
        "label": "title",
        "value": "Checkout Retry",
        "confidence": 0.9,
        "link": "https://example.test/briefs/checkout",
    }


def test_nested_source_payload_is_flattened_without_parsing_string_payloads():
    entries = build_source_evidence_index(
        [
            _source_brief(
                source_payload={
                    "normalized": {
                        "scope": ["Add retry button", "Show final failure"],
                        "limits": {"attempts": 3, "enabled": True},
                    },
                    "raw_json": '{"do_not": "parse me"}',
                },
                source_links={"path": "briefs/checkout.md"},
            )
        ]
    )

    payload_entries = [
        (entry.label, entry.value)
        for entry in entries
        if entry.kind == "source_payload"
    ]

    assert payload_entries == [
        ("source_payload.normalized.limits.attempts", 3),
        ("source_payload.normalized.limits.enabled", True),
        ("source_payload.normalized.scope[0]", "Add retry button"),
        ("source_payload.normalized.scope[1]", "Show final failure"),
        ("source_payload.raw_json", '{"do_not": "parse me"}'),
    ]
    assert "source_payload.raw_json.do_not" not in [label for label, _ in payload_entries]


def test_source_links_are_evidence_and_missing_links_leave_link_none():
    entries = build_source_evidence_index(
        [
            _source_brief(
                source_links={
                    "links": ["https://example.test/spec", "briefs/local.md"],
                    "row_number": 7,
                }
            ),
            _source_brief(source_id="sb-no-link", source_links={}),
        ]
    )

    first_links = [
        (entry.label, entry.value, entry.link)
        for entry in entries
        if entry.source_brief_id == "sb-checkout-retry"
        and entry.kind == "source_link"
    ]
    second_entry_links = {
        entry.link for entry in entries if entry.source_brief_id == "sb-no-link"
    }

    assert first_links == [
        (
            "source_link.links[0]",
            "https://example.test/spec",
            "https://example.test/spec",
        ),
        ("source_link.links[1]", "briefs/local.md", "briefs/local.md"),
        ("source_link.row_number", 7, "https://example.test/spec"),
    ]
    assert second_entry_links == {None}


def test_duplicate_evidence_values_are_collapsed_per_source_brief():
    entries = build_source_evidence_index(
        [
            _source_brief(
                title="Shared value",
                domain=None,
                summary="Shared value",
                source_payload={
                    "feature": "Shared value",
                    "notes": ["Unique value", "Shared value"],
                },
            )
        ]
    )

    assert [(entry.kind, entry.label, entry.value) for entry in entries] == [
        ("title", "title", "Shared value"),
        ("source_payload", "source_payload.notes[0]", "Unique value"),
    ]


def test_model_inputs_and_empty_optional_fields_produce_deterministic_json_payload():
    source_brief = SourceBrief.model_validate(
        _source_brief(
            domain=None,
            source_payload={"empty": "", "none": None, "rank": 1},
            source_links={},
            created_at=None,
            updated_at=None,
        )
    )

    first = build_source_evidence_index([source_brief])
    second = build_source_evidence_index([source_brief])
    payload = source_evidence_index_to_dicts(first)

    assert payload == source_evidence_index_to_dicts(second)
    assert [(entry.kind, entry.label, entry.value, entry.link) for entry in first] == [
        ("title", "title", "Checkout Retry", None),
        ("summary", "summary", "Retry failed payment submissions.", None),
        ("source_payload", "source_payload.rank", 1, None),
    ]
    assert list(payload[0]) == [
        "evidence_id",
        "source_brief_id",
        "kind",
        "label",
        "value",
        "confidence",
        "link",
    ]
    assert json.loads(json.dumps(payload)) == payload


def _source_brief(
    *,
    source_id="sb-checkout-retry",
    title="Checkout Retry",
    domain="payments",
    summary="Retry failed payment submissions.",
    source_payload=None,
    source_links=None,
    created_at=None,
    updated_at=None,
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
        "created_at": created_at,
        "updated_at": updated_at,
    }
