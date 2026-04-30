from datetime import datetime, timezone

from blueprint.domain.models import SourceBrief
from blueprint.source_timeline import (
    SourceTimelineEvent,
    extract_source_timeline,
    source_timeline_to_dicts,
)


def test_source_brief_timestamps_become_timeline_events():
    events = extract_source_timeline(
        SourceBrief(
            id="sb-timeline",
            title="Timeline brief",
            summary="Surface timeline metadata.",
            source_project="manual",
            source_entity_type="brief",
            source_id="source-1",
            source_payload={},
            source_links={},
            created_at=datetime(2026, 5, 1, 9, 30, tzinfo=timezone.utc),
            updated_at=datetime(2026, 5, 2, 10, 45),
        )
    )

    assert [event.to_dict() for event in events] == [
        {
            "label": "Source brief created",
            "date": "2026-05-01T09:30:00",
            "source_key": "created_at",
            "metadata": {},
        },
        {
            "label": "Source brief updated",
            "date": "2026-05-02T10:45:00",
            "source_key": "updated_at",
            "metadata": {},
        },
    ]
    assert all(isinstance(event, SourceTimelineEvent) for event in events)


def test_payload_arrays_are_parsed_into_stable_chronological_events():
    events = extract_source_timeline(
        _source_brief(
            source_payload={
                "events": [
                    {
                        "title": "Beta release",
                        "date": "2026-05-10",
                        "owner": "release",
                    }
                ],
                "milestones": [
                    {
                        "name": "Design lock",
                        "target_date": "2026-05-05T18:00:00+09:00",
                        "status": "planned",
                    }
                ],
                "timeline": [
                    {
                        "label": "Kickoff",
                        "timestamp": "2026-05-01T12:00:00Z",
                    },
                    "Confirm rollout owner",
                ],
            }
        )
    )

    assert source_timeline_to_dicts(events) == [
        {
            "label": "Kickoff",
            "date": "2026-05-01T12:00:00",
            "source_key": "timeline[0]",
            "metadata": {},
        },
        {
            "label": "Design lock",
            "date": "2026-05-05T09:00:00",
            "source_key": "milestones[0]",
            "metadata": {"status": "planned"},
        },
        {
            "label": "Beta release",
            "date": "2026-05-10T00:00:00",
            "source_key": "events[0]",
            "metadata": {"owner": "release"},
        },
        {
            "label": "Confirm rollout owner",
            "date": None,
            "source_key": "timeline[1]",
            "metadata": {},
        },
    ]


def test_common_deadline_fields_are_included_as_dated_events():
    events = extract_source_timeline(
        _source_brief(
            source_payload={
                "deadline": "2026-06-01",
                "due_date": "2026-05-30T15:00:00Z",
                "target_date": "2026-05-31",
            }
        )
    )

    assert source_timeline_to_dicts(events) == [
        {
            "label": "Due date",
            "date": "2026-05-30T15:00:00",
            "source_key": "due_date",
            "metadata": {},
        },
        {
            "label": "Target date",
            "date": "2026-05-31T00:00:00",
            "source_key": "target_date",
            "metadata": {},
        },
        {
            "label": "Deadline",
            "date": "2026-06-01T00:00:00",
            "source_key": "deadline",
            "metadata": {},
        },
    ]


def test_invalid_dates_are_retained_as_undated_metadata():
    events = extract_source_timeline(
        _source_brief(
            created_at="not a timestamp",
            source_payload={
                "deadline": "next Friday",
                "timeline": [
                    {
                        "label": "External dependency",
                        "date": "after procurement",
                        "owner": "ops",
                    }
                ],
            },
        )
    )

    assert source_timeline_to_dicts(events) == [
        {
            "label": "Source brief created",
            "date": None,
            "source_key": "created_at",
            "metadata": {"raw_date": "not a timestamp"},
        },
        {
            "label": "External dependency",
            "date": None,
            "source_key": "timeline[0]",
            "metadata": {"owner": "ops", "raw_date": "after procurement"},
        },
        {
            "label": "Deadline",
            "date": None,
            "source_key": "deadline",
            "metadata": {"raw_date": "next Friday"},
        },
    ]


def test_missing_or_non_mapping_payload_returns_empty_timeline():
    assert extract_source_timeline({"id": "sb-minimal", "source_payload": None}) == ()


def _source_brief(*, source_payload, created_at=None, updated_at=None):
    return {
        "id": "sb-timeline",
        "title": "Timeline brief",
        "domain": "test",
        "summary": "Surface timeline metadata.",
        "source_project": "manual",
        "source_entity_type": "brief",
        "source_id": "source-1",
        "source_payload": source_payload,
        "source_links": {},
        "created_at": created_at,
        "updated_at": updated_at,
    }
