import json

from blueprint.source_data_warehouse_sync_requirements import (
    build_source_data_warehouse_sync_requirements,
    derive_source_data_warehouse_sync_requirements,
    extract_source_data_warehouse_sync_requirements,
    generate_source_data_warehouse_sync_requirements,
    source_data_warehouse_sync_requirements_to_dict,
    source_data_warehouse_sync_requirements_to_dicts,
    source_data_warehouse_sync_requirements_to_markdown,
    summarize_source_data_warehouse_sync_requirements,
)


def test_extracts_all_data_warehouse_sync_categories_from_structured_brief():
    result = build_source_data_warehouse_sync_requirements(_source([
        "Data warehouse sync cadence must run hourly using cron.",
        "Data warehouse sync incremental cursor watermark must use updated_at timestamp.",
        "Data warehouse sync schema mapping must map source fields to destination table columns.",
        "Data warehouse sync backfill behavior must reprocess historical date ranges in batches.",
        "Data warehouse sync failure retry must resume with retry backoff and alerts.",
        "Data warehouse sync reconciliation checks must compare row count and checksum control totals.",
        "Data warehouse sync freshness SLA must keep data within 30 minutes.",
        "Data warehouse sync audit logging must record run id, status, timestamp, and row count.",
    ]))

    assert [record.requirement_type for record in result.records] == ["sync_cadence", "incremental_cursor_watermark", "schema_mapping", "backfill_behavior", "failure_retry", "reconciliation_checks", "freshness_sla", "audit_logging"]
    assert result.summary["missing_detail_flags"] == []


def test_free_text_and_partial_brief_flags_cursor_backfill_and_reconciliation():
    free_text = build_source_data_warehouse_sync_requirements("Data warehouse sync freshness SLA must remain within 1 hour. Data warehouse sync audit logging must record run history.")
    partial = derive_source_data_warehouse_sync_requirements("Data warehouse sync incremental cursor is required. Data warehouse sync backfill behavior is required. Data warehouse sync reconciliation checks are required.")

    assert free_text.summary["requirement_count"] == 2
    assert partial.summary["missing_detail_flags"] == ["missing_cursor_or_watermark", "missing_backfill_behavior", "missing_reconciliation_checks"]


def test_dict_dicts_markdown_aliases_and_negated_scope():
    result = extract_source_data_warehouse_sync_requirements(_source(["Data warehouse sync cadence must run daily."], "dwh-1"))
    payload = source_data_warehouse_sync_requirements_to_dict(result)

    assert generate_source_data_warehouse_sync_requirements("Data warehouse sync failure retry must retry failed export attempts.").summary["requirement_count"] == 1
    assert summarize_source_data_warehouse_sync_requirements(result)["requirement_count"] == 1
    assert json.loads(json.dumps(payload))["source_id"] == "dwh-1"
    assert source_data_warehouse_sync_requirements_to_dicts(result) == payload["records"]
    assert source_data_warehouse_sync_requirements_to_dicts(result.records) == payload["records"]
    assert "# Source Data Warehouse Sync Requirements Report: dwh-1" in source_data_warehouse_sync_requirements_to_markdown(result)
    assert build_source_data_warehouse_sync_requirements("No data warehouse sync changes are required.").records == ()


def _source(lines, source_id="dwh-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "title": "Data warehouse sync", "summary": "Data warehouse sync planning", "source_payload": {"requirements": lines}, "source_links": {}}
