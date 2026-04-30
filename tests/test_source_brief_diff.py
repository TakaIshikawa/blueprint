from blueprint.source_brief_diff import diff_source_briefs


def test_source_brief_diff_ignores_semantically_identical_payloads():
    before = _source_brief(
        source_payload={"tags": ["ops"], "priority": "high"},
        source_links={"path": "briefs/source.md"},
        created_at="2026-04-20T00:00:00Z",
        updated_at="2026-04-20T00:00:00Z",
    )
    after = {
        **_source_brief(
            source_payload={"priority": "high", "tags": ["ops"]},
            source_links={"path": "briefs/source.md"},
            created_at="2026-04-21T00:00:00Z",
            updated_at="2026-04-22T00:00:00Z",
        ),
    }

    result = diff_source_briefs(before, after)

    assert result.has_changes is False
    assert result.changes == []
    assert result.to_dict() == {
        "has_changes": False,
        "summary": {"added": 0, "removed": 0, "changed": 0, "total": 0},
        "changes": [],
    }


def test_source_brief_diff_reports_top_level_scalar_changes():
    result = diff_source_briefs(
        _source_brief(title="Original title", domain="ops"),
        _source_brief(title="Updated title", domain="clinical"),
    )

    assert [change.to_dict() for change in result.changes] == [
        {
            "path": "title",
            "field": "title",
            "scope": "top_level",
            "status": "changed",
            "before": "Original title",
            "after": "Updated title",
        },
        {
            "path": "domain",
            "field": "domain",
            "scope": "top_level",
            "status": "changed",
            "before": "ops",
            "after": "clinical",
        },
    ]


def test_source_brief_diff_reports_nested_payload_keys_deterministically():
    result = diff_source_briefs(
        _source_brief(
            source_payload={
                "body": "Keep",
                "priority": "high",
                "scope": ["import"],
            }
        ),
        _source_brief(
            source_payload={
                "body": "Keep",
                "owner": "Team A",
                "priority": "low",
            }
        ),
    )

    assert [change.to_dict() for change in result.changes] == [
        {
            "path": "source_payload.owner",
            "field": "owner",
            "scope": "source_payload",
            "status": "added",
            "before": None,
            "after": "Team A",
        },
        {
            "path": "source_payload.priority",
            "field": "priority",
            "scope": "source_payload",
            "status": "changed",
            "before": "high",
            "after": "low",
        },
        {
            "path": "source_payload.scope",
            "field": "scope",
            "scope": "source_payload",
            "status": "removed",
            "before": ["import"],
            "after": None,
        },
    ]


def test_source_brief_diff_reports_nested_link_keys_deterministically():
    result = diff_source_briefs(
        _source_brief(
            source_links={
                "html_url": "https://example.test/old",
                "path": "briefs/source.md",
            }
        ),
        _source_brief(
            source_links={
                "html_url": "https://example.test/new",
                "spec": "https://example.test/spec",
            }
        ),
    )

    assert [change.to_dict() for change in result.changes] == [
        {
            "path": "source_links.html_url",
            "field": "html_url",
            "scope": "source_links",
            "status": "changed",
            "before": "https://example.test/old",
            "after": "https://example.test/new",
        },
        {
            "path": "source_links.path",
            "field": "path",
            "scope": "source_links",
            "status": "removed",
            "before": "briefs/source.md",
            "after": None,
        },
        {
            "path": "source_links.spec",
            "field": "spec",
            "scope": "source_links",
            "status": "added",
            "before": None,
            "after": "https://example.test/spec",
        },
    ]


def test_source_brief_diff_normalizes_missing_optional_fields():
    before = _source_brief()
    before.pop("domain")
    before.pop("created_at")
    before.pop("updated_at")
    before.pop("source_payload")
    before.pop("source_links")

    after = _source_brief(domain=None, source_payload={}, source_links={})

    assert diff_source_briefs(before, after).changes == []


def test_source_brief_diff_includes_timestamps_when_requested():
    result = diff_source_briefs(
        _source_brief(
            created_at="2026-04-20T00:00:00Z",
            updated_at="2026-04-21T00:00:00Z",
        ),
        _source_brief(
            created_at="2026-04-22T00:00:00Z",
            updated_at="2026-04-23T00:00:00Z",
        ),
        include_timestamps=True,
    )

    assert [change.to_dict() for change in result.changes] == [
        {
            "path": "created_at",
            "field": "created_at",
            "scope": "top_level",
            "status": "changed",
            "before": "2026-04-20T00:00:00Z",
            "after": "2026-04-22T00:00:00Z",
        },
        {
            "path": "updated_at",
            "field": "updated_at",
            "scope": "top_level",
            "status": "changed",
            "before": "2026-04-21T00:00:00Z",
            "after": "2026-04-23T00:00:00Z",
        },
    ]


def _source_brief(
    *,
    title="Source Brief",
    domain=None,
    source_payload=None,
    source_links=None,
    created_at=None,
    updated_at=None,
):
    return {
        "id": "source-1",
        "title": title,
        "domain": domain,
        "summary": "Normalize upstream work into a plan-ready brief.",
        "source_project": "manual",
        "source_entity_type": "brief",
        "source_id": "brief-1",
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {} if source_links is None else source_links,
        "created_at": created_at,
        "updated_at": updated_at,
    }
