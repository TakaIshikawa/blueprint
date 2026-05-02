import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_data_freshness_expectations import (
    SourceDataFreshnessExpectation,
    SourceDataFreshnessExpectationsReport,
    build_source_data_freshness_expectations,
    extract_source_data_freshness_expectations,
    generate_source_data_freshness_expectations,
    source_data_freshness_expectations_to_dict,
    source_data_freshness_expectations_to_dicts,
    source_data_freshness_expectations_to_markdown,
)


def test_detects_realtime_batch_polling_cache_webhook_and_sync_cadence_expectations():
    result = build_source_data_freshness_expectations(
        _source_brief(
            summary=(
                "Inventory must update in real-time for operators and show a warning if stale. "
                "Customer dashboards are near real-time with updates within 30 seconds."
            ),
            source_payload={
                "requirements": [
                    "Orders sync every 5 minutes and display last updated time to users.",
                    "Partner catalog polling runs every 15 minutes.",
                    "Pricing cache TTL is 2 hours with a visible stale badge.",
                    "Webhook update should reflect order changes within 10 seconds.",
                    "Daily batch import refreshes reports for admins.",
                ]
            },
        )
    )

    by_mode = {record.expectation_mode: record for record in result.records}

    assert isinstance(result, SourceDataFreshnessExpectationsReport)
    assert all(isinstance(record, SourceDataFreshnessExpectation) for record in result.records)
    assert {
        "real_time",
        "near_real_time",
        "batch",
        "polling",
        "cache_age",
        "webhook",
        "sync_cadence",
        "last_updated",
        "staleness_tolerance",
    } <= set(by_mode)
    assert by_mode["real_time"].freshness_surface == "inventory"
    assert by_mode["near_real_time"].max_age == "30 seconds"
    orders_sync = next(
        record
        for record in result.records
        if record.expectation_mode == "sync_cadence" and record.freshness_surface == "orders"
    )
    assert orders_sync.expected_cadence == "5 minutes"
    assert by_mode["polling"].expected_cadence == "15 minutes"
    assert by_mode["cache_age"].max_age == "2 hours"
    assert by_mode["webhook"].max_age == "10 seconds"
    assert by_mode["batch"].expected_cadence == "Daily batch"
    assert by_mode["cache_age"].user_visible_impact is not None
    assert by_mode["real_time"].confidence in {"high", "medium"}
    assert result.summary["mode_counts"]["webhook"] == 1
    assert "inventory" in result.summary["freshness_surfaces"]


def test_structured_fields_and_free_form_text_are_supported_with_missing_detail_flags():
    result = build_source_data_freshness_expectations(
        {
            "id": "structured-freshness",
            "title": "Data freshness | dashboard",
            "summary": "Dashboard should show last updated timestamp.",
            "source_payload": {
                "freshness": {
                    "search_index": "Search index freshness SLA of 20 minutes.",
                    "crm_sync": "CRM sync cadence is hourly for customer data.",
                    "live_metrics": "Metrics need real-time updates.",
                },
                "cache": {"inventory_cache_age": "No older than 10 minutes for inventory validation."},
            },
        }
    )

    by_surface = {(record.freshness_surface, record.expectation_mode): record for record in result.records}
    live_metrics = by_surface[("metrics", "real_time")]

    assert by_surface[("search index", "staleness_tolerance")].max_age == "20 minutes"
    assert by_surface[("customer data", "sync_cadence")].expected_cadence == "hourly"
    assert by_surface[("inventory", "staleness_tolerance")].max_age == "10 minutes"
    assert "missing_user_visible_impact" in live_metrics.missing_detail_flags
    assert any(
        "source_payload.freshness.search_index" in evidence
        for record in result.records
        for evidence in record.evidence
    )
    assert result.summary["missing_detail_counts"]["missing_user_visible_impact"] >= 1


def test_sourcebrief_model_aliases_serialization_markdown_and_no_source_mutation():
    source = _source_brief(
        source_id="freshness-model",
        title="Freshness | orders",
        summary="Orders sync every 10 minutes and show last synced time to users.",
        source_payload={
            "metadata": {
                "cache_age": "Order cache max age 30 minutes.",
                "webhook": "Webhook update visible within 5 seconds.",
            }
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_data_freshness_expectations(source)
    model_result = generate_source_data_freshness_expectations(model)
    extracted = extract_source_data_freshness_expectations(model)
    payload = source_data_freshness_expectations_to_dict(model_result)
    markdown = source_data_freshness_expectations_to_markdown(model_result)

    assert source == original
    assert payload == source_data_freshness_expectations_to_dict(mapping_result)
    assert extracted == model_result.expectations
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.expectations
    assert model_result.to_dicts() == payload["expectations"]
    assert source_data_freshness_expectations_to_dicts(model_result) == payload["expectations"]
    assert source_data_freshness_expectations_to_dicts(model_result.records) == payload["records"]
    assert list(payload) == ["source_brief_id", "title", "summary", "expectations", "records"]
    assert list(payload["expectations"][0]) == [
        "source_brief_id",
        "freshness_surface",
        "expectation_mode",
        "expected_cadence",
        "max_age",
        "user_visible_impact",
        "missing_detail_flags",
        "confidence",
        "evidence",
    ]
    assert markdown == model_result.to_markdown()
    assert markdown.startswith("# Source Data Freshness Expectations Report: freshness-model")
    assert "| Surface | Mode | Cadence | Max Age | Impact | Missing Details | Confidence | Evidence |" in markdown
    assert "Freshness \\| orders" not in markdown
    assert "last synced time" in markdown


def test_no_expectation_invalid_and_plain_text_inputs_are_deterministic():
    empty = build_source_data_freshness_expectations(
        _source_brief(
            title="Copy update",
            summary="Update dashboard empty state copy.",
            source_payload={"body": "No data timing changes."},
        )
    )
    repeat = build_source_data_freshness_expectations(
        _source_brief(
            title="Copy update",
            summary="Update dashboard empty state copy.",
            source_payload={"body": "No data timing changes."},
        )
    )
    invalid = build_source_data_freshness_expectations(object())
    text = build_source_data_freshness_expectations("Poll inventory every 2 minutes.")

    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_brief_id == "source-freshness"
    assert empty.records == ()
    assert empty.summary == {
        "expectation_count": 0,
        "mode_counts": {
            "real_time": 0,
            "near_real_time": 0,
            "batch": 0,
            "polling": 0,
            "cache_age": 0,
            "webhook": 0,
            "sync_cadence": 0,
            "manual_refresh": 0,
            "last_updated": 0,
            "staleness_tolerance": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_counts": {
            "missing_cadence_or_max_age": 0,
            "missing_update_mechanism": 0,
            "missing_user_visible_impact": 0,
        },
        "freshness_surfaces": [],
    }
    assert empty.to_markdown() == repeat.to_markdown()
    assert "No source data freshness expectations were found" in empty.to_markdown()
    assert invalid.source_brief_id is None
    assert invalid.records == ()
    assert text.source_brief_id is None
    assert text.records[0].expectation_mode == "polling"
    assert text.records[0].expected_cadence == "2 minutes"


def _source_brief(
    *,
    source_id="source-freshness",
    title="Data freshness expectations",
    domain="data",
    summary="General data freshness requirements.",
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
