import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_offline_expectations import (
    SourceOfflineExpectation,
    SourceOfflineExpectationsReport,
    build_source_offline_expectations,
    extract_source_offline_expectations,
    generate_source_offline_expectations,
    source_offline_expectations_to_dict,
    source_offline_expectations_to_dicts,
    source_offline_expectations_to_markdown,
)


def test_detects_offline_sync_expectations_across_brief_fields():
    result = build_source_offline_expectations(
        _source_brief(
            summary=(
                "Field reps need offline mode in airplane mode and sync when the connection returns."
            ),
            source_payload={
                "requirements": [
                    "Queued changes go to an outbox and retry when online.",
                    "Conflict resolution is required for concurrent edits during background sync.",
                    "Use a local cache so forms can load with no connectivity.",
                ],
                "risks": [
                    "Stale data must show a last synced warning before validation.",
                    "Intermittent connection drops cannot lose pending writes.",
                ],
            },
        )
    )

    by_type = {record.expectation_type: record for record in result.expectations}

    assert isinstance(result, SourceOfflineExpectationsReport)
    assert all(isinstance(record, SourceOfflineExpectation) for record in result.records)
    assert list(by_type) == [
        "offline_mode",
        "intermittent_connectivity",
        "sync",
        "conflict_resolution",
        "local_cache",
        "outbox",
        "retry_when_online",
        "stale_data",
    ]
    assert by_type["offline_mode"].source_brief_id == "source-offline"
    assert by_type["offline_mode"].confidence == "high"
    assert "airplane mode" in by_type["offline_mode"].detected_signals
    assert "offline mode" in by_type["offline_mode"].detected_signals
    assert any("summary" in item for item in by_type["sync"].evidence)
    assert "sync protocol" in by_type["sync"].planning_implications[0]
    assert "conflict detection" in by_type["conflict_resolution"].planning_implications[0]
    assert "cache schema" in by_type["local_cache"].planning_implications[0]
    assert "retry backoff" in by_type["retry_when_online"].planning_implications[0]
    assert result.summary["expectation_count"] == 8
    assert result.summary["expectation_type_counts"]["outbox"] == 1


def test_duplicate_signals_are_merged_with_deduplicated_evidence():
    result = build_source_offline_expectations(
        {
            "id": "dupe-offline",
            "summary": "Local cache is required for offline mode. Local cache is required for offline mode.",
            "requirements": [
                "Local cache is required for offline mode.",
                "local cache is required for offline mode.",
            ],
            "metadata": {"local_cache": "Local cache is required for offline mode."},
        }
    )

    local_cache = next(
        record for record in result.expectations if record.expectation_type == "local_cache"
    )

    assert local_cache.evidence == tuple(
        sorted(set(local_cache.evidence), key=lambda item: item.casefold())
    )
    assert len(local_cache.evidence) == len(set(local_cache.evidence))
    assert local_cache.detected_signals == ("local cache",)
    assert result.summary["expectation_count"] == len(result.expectations)


def test_mapping_and_sourcebrief_inputs_match_and_serialize_to_json_compatible_payload():
    source = _source_brief(
        source_id="offline-model",
        summary="Mobile users need offline access and sync after reconnect.",
        source_payload={
            "requirements": ["Queued writes should retry when online through an outbox."],
            "metadata": {"stale_data": "Show stale data warnings with last synced time."},
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_offline_expectations(source)
    model_result = generate_source_offline_expectations(model)
    extracted = extract_source_offline_expectations(model)
    payload = source_offline_expectations_to_dict(model_result)
    markdown = source_offline_expectations_to_markdown(model_result)

    assert source == original
    assert payload == source_offline_expectations_to_dict(mapping_result)
    assert extracted == model_result.expectations
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.expectations
    assert model_result.to_dicts() == payload["expectations"]
    assert source_offline_expectations_to_dicts(model_result.records) == payload["records"]
    assert list(payload) == ["source_id", "expectations", "summary", "records"]
    assert list(payload["expectations"][0]) == [
        "source_brief_id",
        "expectation_type",
        "detected_signals",
        "evidence",
        "confidence",
        "planning_implications",
    ]
    assert markdown.startswith("# Source Offline Expectations Report: offline-model")
    assert (
        "| Source Brief | Type | Confidence | Signals | Evidence | Planning Implications |"
        in markdown
    )


def test_multiple_briefs_are_handled_with_stable_ordering():
    result = build_source_offline_expectations(
        [
            _source_brief(
                source_id="brief-b",
                summary="Sync must retry when online after intermittent connectivity.",
            ),
            _source_brief(
                source_id="brief-a",
                summary="Offline mode stores queued changes in an outbox.",
            ),
        ]
    )

    assert [(record.source_brief_id, record.expectation_type) for record in result.records] == [
        ("brief-a", "offline_mode"),
        ("brief-a", "outbox"),
        ("brief-b", "intermittent_connectivity"),
        ("brief-b", "sync"),
        ("brief-b", "retry_when_online"),
    ]
    assert result.source_id is None
    assert result.summary["source_count"] == 2


def test_no_signal_empty_and_invalid_inputs_return_no_records():
    empty = build_source_offline_expectations(
        {"id": "empty", "summary": "Update onboarding copy only."}
    )
    invalid = build_source_offline_expectations(object())

    assert empty.source_id == "empty"
    assert empty.expectations == ()
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary["expectation_count"] == 0
    assert "No offline or sync expectations were found" in empty.to_markdown()
    assert invalid.source_id is None
    assert invalid.expectations == ()


def _source_brief(
    *,
    source_id="source-offline",
    title="Offline expectations",
    domain="field",
    summary="General offline requirements.",
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
