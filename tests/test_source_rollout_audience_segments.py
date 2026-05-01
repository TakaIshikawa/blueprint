import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_rollout_audience_segments import (
    SourceRolloutAudienceSegment,
    extract_source_rollout_audience_segments,
    source_rollout_audience_segments_to_dicts,
    summarize_source_rollout_audience_segments,
)


def test_structured_persona_arrays_extract_segments_and_dedupe_text_mentions():
    records = extract_source_rollout_audience_segments(
        _source_brief(
            summary="Roll out to finance managers first.",
            source_payload={
                "personas": ["Finance managers", {"name": "Support agents", "type": "role"}],
                "target_audiences": ["finance managers"],
            },
        )
    )

    assert all(isinstance(record, SourceRolloutAudienceSegment) for record in records)
    assert [
        (record.segment_label, record.segment_type, record.source_field) for record in records
    ] == [
        ("Finance managers", "persona", "source_payload.personas[0]"),
        ("Support agents", "role", "source_payload.personas[1]"),
    ]
    assert records[0].confidence == "high"
    assert records[0].inclusion_status == "included"
    assert "Segment: Finance managers." in records[0].rollout_implication


def test_markdown_free_text_extracts_rollout_audiences():
    records = extract_source_rollout_audience_segments(
        _source_brief(
            source_payload={
                "body": (
                    "## Rollout\n"
                    "- Launch to beta users on iOS first.\n"
                    "- Target enterprise customers and APAC during wave 1."
                )
            }
        )
    )

    assert [
        (record.segment_label, record.segment_type, record.inclusion_status, record.source_field)
        for record in records
    ] == [
        ("enterprise customers", "customer_tier", "included", "source_payload.body"),
        ("APAC", "region", "included", "source_payload.body"),
        ("iOS", "platform", "included", "source_payload.body"),
        ("beta users", "beta_group", "included", "source_payload.body"),
        ("wave 1", "cohort", "included", "source_payload.body"),
    ]


def test_exclusion_language_is_preserved_as_exclusions():
    records = extract_source_rollout_audience_segments(
        _source_brief(
            summary="Launch to enterprise customers, not for admins, and exclude EU customers.",
            source_payload={"excluded_audiences": ["Internal users"]},
        )
    )

    excluded = [
        (record.segment_label, record.segment_type, record.inclusion_status)
        for record in records
        if record.inclusion_status == "excluded"
    ]
    assert excluded == [
        ("EU customers", "region", "excluded"),
        ("admins", "role", "excluded"),
        ("Internal users", "internal_user", "excluded"),
    ]
    assert all(
        "Keep" in record.rollout_implication
        for record in records
        if record.inclusion_status == "excluded"
    )


def test_region_platform_tier_plan_and_internal_classification():
    records = extract_source_rollout_audience_segments(
        _source_brief(
            source_payload={
                "regions": ["US", "EMEA"],
                "platforms": ["Android", "web"],
                "customer_tiers": ["Enterprise", "SMB"],
                "plans": ["Pro plan"],
                "internal_users": ["Support staff"],
            }
        )
    )

    assert [(record.segment_label, record.segment_type) for record in records] == [
        ("Enterprise", "customer_tier"),
        ("SMB", "customer_tier"),
        ("EMEA", "region"),
        ("US", "region"),
        ("Android", "platform"),
        ("web", "platform"),
        ("Pro plan", "plan"),
        ("Support staff", "internal_user"),
    ]
    assert summarize_source_rollout_audience_segments(records) == {
        "segment_count": 8,
        "included_count": 8,
        "excluded_count": 0,
        "type_counts": {
            "customer_tier": 2,
            "region": 2,
            "platform": 2,
            "plan": 1,
            "internal_user": 1,
        },
        "confidence_counts": {"high": 8, "medium": 0, "low": 0},
        "rollout_implications": [record.rollout_implication for record in records],
    }


def test_source_brief_model_input_matches_mapping_input_without_mutation_and_serializes():
    source = _source_brief(
        summary="Enable for admins on mobile. Excluding Canada until legal review.",
        source_payload={"beta_groups": ["Early access group"]},
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_records = extract_source_rollout_audience_segments(source)
    model_records = extract_source_rollout_audience_segments(model)
    payload = source_rollout_audience_segments_to_dicts(model_records)

    assert source == original
    assert payload == source_rollout_audience_segments_to_dicts(mapping_records)
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload[0]) == [
        "segment_label",
        "segment_type",
        "inclusion_status",
        "source_field",
        "confidence",
        "evidence",
        "rollout_implication",
    ]
    assert summarize_source_rollout_audience_segments(model)["segment_count"] == len(model_records)


def test_empty_unrelated_or_malformed_sources_return_empty_results():
    assert (
        extract_source_rollout_audience_segments(_source_brief(summary="General background only."))
        == ()
    )
    assert (
        extract_source_rollout_audience_segments(_source_brief(source_payload="not a mapping"))
        == ()
    )
    assert extract_source_rollout_audience_segments("not a source brief") == ()
    assert summarize_source_rollout_audience_segments(()) == {
        "segment_count": 0,
        "included_count": 0,
        "excluded_count": 0,
        "type_counts": {},
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "rollout_implications": [],
    }


def _source_brief(
    *,
    source_id="sb-rollout-audience",
    title="Rollout audience",
    domain="payments",
    summary="General rollout audience notes.",
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
