import pytest

from blueprint.domain import SourceBrief
from blueprint.source_brief_merge import (
    MERGED_CONFLICTS_KEY,
    MERGED_PROVENANCE_KEY,
    merge_source_briefs,
)


def test_merge_source_briefs_consolidates_two_briefs_with_primary_identity():
    merged = merge_source_briefs(
        [
            _source_brief(
                "sb-primary",
                title="Primary brief",
                domain="ops",
                summary="Normalize intake rows.\n\nKeep audit metadata.",
                source_project="manual",
                source_entity_type="note",
                source_id="manual-1",
                source_payload={"priority": "high"},
                source_links={
                    "html_url": "https://example.test/shared",
                    "spec": "https://example.test/spec",
                },
            ),
            _source_brief(
                "sb-related",
                title="Related brief",
                domain="ops",
                summary="Normalize intake rows.\n\nAdd retry handling.",
                source_project="github",
                source_entity_type="issue",
                source_id="42",
                source_links={
                    "html_url": "https://example.test/shared",
                    "issue": "https://example.test/issues/42",
                },
            ),
        ]
    )

    SourceBrief.model_validate(merged)
    assert merged["id"] == "sb-primary"
    assert merged["title"] == "Primary brief"
    assert merged["domain"] == "ops"
    assert merged["source_project"] == "manual"
    assert merged["source_entity_type"] == "note"
    assert merged["source_id"] == "manual-1"
    assert merged["source_payload"]["priority"] == "high"
    assert merged["source_payload"][MERGED_PROVENANCE_KEY] == [
        {
            "id": "sb-primary",
            "source_project": "manual",
            "source_entity_type": "note",
            "source_id": "manual-1",
            "title": "Primary brief",
        },
        {
            "id": "sb-related",
            "source_project": "github",
            "source_entity_type": "issue",
            "source_id": "42",
            "title": "Related brief",
        },
    ]
    assert merged["source_payload"][MERGED_CONFLICTS_KEY] == {
        "source_project": [
            {"id": "sb-primary", "value": "manual"},
            {"id": "sb-related", "value": "github"},
        ],
        "title": [
            {"id": "sb-primary", "value": "Primary brief"},
            {"id": "sb-related", "value": "Related brief"},
        ],
    }
    assert merged["source_links"] == {
        "html_url": "https://example.test/shared",
        "issue": "https://example.test/issues/42",
        "spec": "https://example.test/spec",
    }
    assert merged["summary"] == (
        "sb-primary: Normalize intake rows.\n\n"
        "sb-primary: Keep audit metadata.\n\n"
        "sb-related: Add retry handling."
    )


def test_merge_source_briefs_merges_multiple_briefs_deterministically():
    merged = merge_source_briefs(
        [
            _source_brief(
                "sb-1",
                domain="support",
                summary="Shared context.",
                source_project="manual",
                source_links={
                    "doc": "https://example.test/doc",
                    "refs": ["https://example.test/a", "https://example.test/b"],
                },
            ),
            _source_brief(
                "sb-2",
                domain="customer-success",
                summary="Shared context.\n\nEscalate stale tickets.",
                source_project="manual",
                source_entity_type="issue",
                source_id="2",
                source_links={
                    "doc": "https://example.test/doc",
                    "refs": ["https://example.test/b", "https://example.test/c"],
                },
            ),
            _source_brief(
                "sb-3",
                domain="support",
                summary="Escalate stale tickets.\n\nPreserve owner notes.",
                source_project="slack",
                source_entity_type="thread",
                source_id="thread-3",
                source_links={"doc": "https://example.test/alternate"},
            ),
        ]
    )

    SourceBrief.model_validate(merged)
    assert [entry["id"] for entry in merged["source_payload"][MERGED_PROVENANCE_KEY]] == [
        "sb-1",
        "sb-2",
        "sb-3",
    ]
    assert merged["source_payload"][MERGED_CONFLICTS_KEY] == {
        "domain": [
            {"id": "sb-1", "value": "support"},
            {"id": "sb-2", "value": "customer-success"},
            {"id": "sb-3", "value": "support"},
        ],
        "source_project": [
            {"id": "sb-1", "value": "manual"},
            {"id": "sb-2", "value": "manual"},
            {"id": "sb-3", "value": "slack"},
        ],
    }
    assert merged["source_links"] == {
        "doc": ["https://example.test/doc", "https://example.test/alternate"],
        "refs": [
            "https://example.test/a",
            "https://example.test/b",
            "https://example.test/c",
        ],
    }
    assert merged["summary"] == (
        "sb-1: Shared context.\n\n"
        "sb-2: Escalate stale tickets.\n\n"
        "sb-3: Preserve owner notes."
    )


def test_merge_source_briefs_requires_at_least_two_briefs():
    with pytest.raises(ValueError, match="at least two"):
        merge_source_briefs([_source_brief("sb-1")])


def _source_brief(
    source_brief_id,
    *,
    title="Source Brief",
    domain="testing",
    summary="Normalize source data.",
    source_project="manual",
    source_entity_type="note",
    source_id=None,
    source_payload=None,
    source_links=None,
):
    return {
        "id": source_brief_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": source_project,
        "source_entity_type": source_entity_type,
        "source_id": source_brief_id if source_id is None else source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }
