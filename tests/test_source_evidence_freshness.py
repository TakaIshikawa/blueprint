import json

import pytest

from blueprint.source_evidence_freshness import (
    SourceEvidenceFreshnessScore,
    build_source_evidence_freshness_score,
    score_source_evidence_freshness,
    source_evidence_freshness_score_to_dict,
)


def test_recent_iso_dates_score_as_fresh_with_deterministic_today():
    result = score_source_evidence_freshness(
        [
            _source_brief(
                "sb-fresh",
                evidence=[
                    {
                        "claim": "Checkout retry is still required.",
                        "metadata": {"observed_at": "2026-04-25T09:30:00Z"},
                    }
                ],
            )
        ],
        today="2026-05-01",
    )

    assert isinstance(result, SourceEvidenceFreshnessScore)
    assert result.score == 100
    assert result.band == "fresh"
    assert result.reference_date == "2026-05-01"
    assert result.stale_evidence == ()
    assert result.missing_date_evidence == ()
    assert result.malformed_date_evidence == ()
    assert result.remediation_notes == ("Evidence dates are fresh enough for planning.",)
    assert result.evidence[0].to_dict() == {
        "source_brief_id": "sb-fresh",
        "label": "evidence[0]",
        "value": {
            "claim": "Checkout retry is still required.",
            "metadata": {"observed_at": "2026-04-25T09:30:00Z"},
        },
        "band": "fresh",
        "score": 100,
        "evidence_date": "2026-04-25",
        "age_days": 6,
        "error": None,
    }


def test_stale_evidence_is_reported_with_remediation_note():
    result = score_source_evidence_freshness(
        _source_brief(
            "sb-stale",
            references=[
                {
                    "title": "Legacy vendor requirements",
                    "published_at": "2025-01-15",
                }
            ],
        ),
        today="2026-05-01",
    )

    assert result.score == 15
    assert result.band == "stale"
    assert [(item.label, item.band, item.age_days) for item in result.stale_evidence] == [
        ("references[0]", "stale", 471)
    ]
    assert result.missing_date_evidence == ()
    assert result.remediation_notes == (
        "Refresh stale evidence before marking the brief execution-ready.",
    )


def test_missing_date_evidence_scores_zero_without_exception():
    result = build_source_evidence_freshness_score(
        [
            _source_brief(
                "sb-undated",
                evidence=["Stakeholder said dashboard copy is confusing."],
                references=[{"title": "Support note", "url": "https://example.test/tickets/1"}],
            )
        ],
        today="2026-05-01",
    )

    assert result.score == 0
    assert result.band == "missing"
    assert [(item.label, item.band) for item in result.missing_date_evidence] == [
        ("evidence[0]", "missing_date"),
        ("references[0]", "missing_date"),
    ]
    assert result.remediation_notes == (
        "Add collection, publication, or last-reviewed dates to undated evidence.",
    )


def test_malformed_dates_are_reported_without_raising_unexpected_exceptions():
    result = score_source_evidence_freshness(
        _source_brief(
            "sb-malformed",
            timestamps={"last_reviewed_at": "2026-14-99"},
        ),
        today="2026-05-01",
    )

    assert result.score == 0
    assert result.band == "missing"
    assert result.missing_date_evidence == ()
    assert [(item.label, item.band, item.error) for item in result.malformed_date_evidence] == [
        (
            "timestamps.last_reviewed_at",
            "malformed_date",
            "Could not parse date value: 2026-14-99",
        )
    ]
    assert result.remediation_notes == (
        "Correct malformed date values so freshness can be scored deterministically.",
    )


def test_mixed_source_behavior_tracks_stale_missing_malformed_and_embedded_dates():
    result = score_source_evidence_freshness(
        [
            _source_brief(
                "sb-mixed",
                evidence=[
                    {"claim": "Fresh item", "metadata": {"captured_at": "2026-04-20"}},
                    {"claim": "Old item", "date": "2025-08-01"},
                    "Undated stakeholder comment",
                ],
                references=[{"title": "Bad date reference", "published_at": "2026-02-30"}],
                metadata={"last_reviewed_at": "2026-03-15"},
            )
        ],
        today="2026-05-01",
    )

    assert result.score == 38
    assert result.band == "aging"
    assert [(item.label, item.band) for item in result.evidence] == [
        ("evidence[0]", "fresh"),
        ("evidence[1]", "stale"),
        ("evidence[2]", "missing_date"),
        ("metadata.last_reviewed_at", "current"),
        ("references[0]", "malformed_date"),
    ]
    assert [item.label for item in result.stale_evidence] == ["evidence[1]"]
    assert [item.label for item in result.missing_date_evidence] == ["evidence[2]"]
    assert [item.label for item in result.malformed_date_evidence] == ["references[0]"]

    payload = source_evidence_freshness_score_to_dict(result)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["evidence"]
    assert list(payload) == [
        "score",
        "band",
        "evidence",
        "stale_evidence",
        "missing_date_evidence",
        "malformed_date_evidence",
        "remediation_notes",
        "reference_date",
    ]
    assert json.loads(json.dumps(payload)) == payload


def test_invalid_today_value_raises_clear_value_error():
    with pytest.raises(ValueError, match="today must be a date"):
        score_source_evidence_freshness(_source_brief("sb-invalid"), today="nope")


def _source_brief(
    source_brief_id,
    *,
    evidence=None,
    references=None,
    timestamps=None,
    metadata=None,
):
    brief = {
        "id": source_brief_id,
        "title": "Checkout Retry",
        "domain": "payments",
        "summary": "Retry failed checkout submissions.",
        "source_project": "manual",
        "source_entity_type": "brief",
        "source_id": source_brief_id,
        "source_payload": {"normalized": {"title": "Checkout Retry"}},
        "source_links": {},
    }
    if evidence is not None:
        brief["evidence"] = evidence
    if references is not None:
        brief["references"] = references
    if timestamps is not None:
        brief["timestamps"] = timestamps
    if metadata is not None:
        brief["metadata"] = metadata
    return brief
