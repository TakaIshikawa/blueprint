from blueprint.source_priority import (
    SourcePriorityComponent,
    SourcePriorityScore,
    score_source_briefs,
)


def test_high_priority_and_customer_blocking_signals_rank_above_neutral_briefs():
    results = score_source_briefs(
        [
            _source_brief("sb-neutral", title="Add settings page"),
            _source_brief(
                "sb-urgent",
                title="Customer blocked by checkout outage",
                source_payload={
                    "normalized": {
                        "priority": "high",
                        "tags": ["customer-blocking", "urgent"],
                    }
                },
                source_links={"zendesk_ticket": "https://support.example.test/tickets/123"},
            ),
        ]
    )

    assert [result.source_brief_id for result in results] == ["sb-urgent", "sb-neutral"]
    assert results[0].score > results[1].score
    assert [component.code for component in results[0].components] == [
        "explicit_priority",
        "tag_signals",
        "source_links",
        "customer_source_link",
        "urgency_keywords",
    ]


def test_scores_include_human_readable_component_reasons():
    result = score_source_briefs(
        [
            _source_brief(
                "sb-reasoned",
                title="Urgent customer escalation",
                source_payload={
                    "normalized": {
                        "severity": "critical",
                        "labels": ["customer", "p1"],
                    }
                },
            )
        ]
    )[0]

    assert isinstance(result, SourcePriorityScore)
    assert all(isinstance(component, SourcePriorityComponent) for component in result.components)
    assert result.to_dict() == {
        "source_brief_id": "sb-reasoned",
        "title": "Urgent customer escalation",
        "score": 160,
        "components": [
            {
                "code": "explicit_severity",
                "points": 50,
                "reason": "Explicit severity is critical.",
            },
            {
                "code": "tag_signals",
                "points": 50,
                "reason": "Priority-like tags are present: customer, p1.",
            },
            {
                "code": "urgency_keywords",
                "points": 60,
                "reason": (
                    "Title or summary contains urgency keywords: "
                    "customer, escalation, urgent."
                ),
            },
        ],
    }


def test_missing_optional_fields_are_handled_without_exceptions():
    results = score_source_briefs(
        [
            {
                "id": "sb-minimal",
                "title": "Minimal brief",
                "summary": "No optional source metadata.",
            },
            {
                "id": "sb-empty-payload",
                "title": "Empty payload",
                "summary": "No signals.",
                "source_payload": None,
                "source_links": None,
            },
        ]
    )

    assert [result.to_dict() for result in results] == [
        {
            "source_brief_id": "sb-empty-payload",
            "title": "Empty payload",
            "score": 0,
            "components": [],
        },
        {
            "source_brief_id": "sb-minimal",
            "title": "Minimal brief",
            "score": 0,
            "components": [],
        },
    ]


def test_sorting_is_deterministic_when_scores_tie():
    briefs = [
        _source_brief("sb-c", title="Same score C"),
        _source_brief("sb-a", title="Same score A"),
        _source_brief("sb-b", title="Same score B"),
    ]

    assert [result.source_brief_id for result in score_source_briefs(briefs)] == [
        "sb-a",
        "sb-b",
        "sb-c",
    ]
    assert [result.source_brief_id for result in score_source_briefs(list(reversed(briefs)))] == [
        "sb-a",
        "sb-b",
        "sb-c",
    ]


def test_recency_uses_newest_available_timestamp_without_wall_clock_dependency():
    results = score_source_briefs(
        [
            _source_brief(
                "sb-old-urgent",
                title="Urgent request",
                created_at="2026-01-01T00:00:00Z",
            ),
            _source_brief(
                "sb-new-neutral",
                title="Neutral request",
                created_at="2026-01-08T00:00:00Z",
            ),
        ]
    )

    by_id = {result.source_brief_id: result for result in results}
    assert [component.to_dict() for component in by_id["sb-new-neutral"].components] == [
        {
            "code": "recency",
            "points": 10,
            "reason": "Timestamp is within 1 day of the newest source brief timestamp.",
        }
    ]
    assert by_id["sb-old-urgent"].score > by_id["sb-new-neutral"].score


def _source_brief(
    source_brief_id,
    *,
    title,
    summary="Neutral source brief.",
    source_payload=None,
    source_links=None,
    created_at=None,
):
    brief = {
        "id": source_brief_id,
        "title": title,
        "domain": "test",
        "summary": summary,
        "source_project": "manual",
        "source_entity_type": "brief",
        "source_id": source_brief_id,
        "source_payload": source_payload
        if source_payload is not None
        else {"normalized": {"title": title, "summary": summary}},
        "source_links": source_links or {},
    }
    if created_at is not None:
        brief["created_at"] = created_at
    return brief
