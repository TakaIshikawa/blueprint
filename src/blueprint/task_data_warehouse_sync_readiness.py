"""Analyze data warehouse sync readiness for execution-plan tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, build_simple_readiness_plan


_SIGNAL_PATTERNS = {
    "warehouse": re.compile(r"\b(?:data[_\s-]+warehouse|warehouse[_\s-]+sync|analytics[_\s-]+warehouse|bi[_\s-]+sync|snowflake|bigquery|redshift|databricks)\b", re.I),
    "etl": re.compile(r"\b(?:etl|elt|reverse[_\s-]+etl|extract[_\s-]+transform[_\s-]+load|load[_\s-]+warehouse|sync[_\s-]+to[_\s-]+warehouse)\b", re.I),
    "destination_sync": re.compile(r"\b(?:destination|sink|target).{0,60}\b(?:schema|warehouse|table|dataset)\b|\b(?:warehouse|bi|analytics).{0,60}\b(?:sync|replication|load)\b", re.I),
}
_PATH_SIGNAL_PATTERNS = {
    "warehouse_path": re.compile(r"\b(?:warehouse|bi|analytics|etl|elt|reverse[_\s-]*etl|snowflake|bigquery|redshift|databricks).*(?:sync|load|destination|schema)\b|\b(?:sync|load|destination|schema).*(?:warehouse|bi|analytics|etl|elt|snowflake|bigquery|redshift|databricks)\b", re.I),
}
_CRITERIA_PATTERNS = {
    "source_tables_events": re.compile(r"\b(?:source[_\s-]+tables?|source[_\s-]+events?|event[_\s-]+stream|cdc|change[_\s-]+data|upstream[_\s-]+tables?|source[_\s-]+models?)\b", re.I),
    "destination_schema": re.compile(r"\b(?:destination[_\s-]+schema|target[_\s-]+schema|warehouse[_\s-]+schema|destination[_\s-]+table|target[_\s-]+table|dataset|marts?)\b", re.I),
    "sync_mode": re.compile(r"\b(?:sync[_\s-]+mode|incremental|full[_\s-]+refresh|snapshot|append|upsert|cdc|batch|streaming|reverse[_\s-]+etl)\b", re.I),
    "freshness_sla": re.compile(r"\b(?:freshness|sla|latency|lag|within[_\s-]+\d+|near[_\s-]+real[_\s-]+time|hourly|daily)\b", re.I),
    "backfill_boundary": re.compile(r"\b(?:backfill|historical[_\s-]+load|start[_\s-]+date|cutoff|watermark|boundary|lookback)\b", re.I),
    "idempotency_deduplication": re.compile(r"\b(?:idempotent|idempotency|dedupe|deduplication|duplicate[_\s-]+handling|primary[_\s-]+key|merge[_\s-]+key|exactly[_\s-]+once)\b", re.I),
    "privacy_filtering": re.compile(r"\b(?:privacy[_\s-]+filter|pii|personal[_\s-]+data|redact|mask|tokenize|consent|data[_\s-]+minimization|sensitive[_\s-]+fields?)\b", re.I),
    "reconciliation_tests": re.compile(r"\b(?:reconciliation|row[_\s-]+count|checksum|data[_\s-]+quality|validation|tests?|integration[_\s-]+tests?|fixture)\b", re.I),
}
_GUIDANCE = {
    "source_tables_events": "List source tables, events, CDC streams, or upstream models included in the sync.",
    "destination_schema": "Define destination warehouse schema, tables, datasets, and ownership.",
    "sync_mode": "Specify sync mode such as incremental, full refresh, snapshot, append, upsert, batch, or streaming.",
    "freshness_sla": "State freshness SLA, latency bounds, lag alerts, and expected cadence.",
    "backfill_boundary": "Set backfill boundaries, historical start date, watermark, and cutoff behavior.",
    "idempotency_deduplication": "Document idempotency, merge keys, deduplication, and duplicate handling.",
    "privacy_filtering": "Apply privacy filtering, PII masking, consent checks, and sensitive-field exclusions.",
    "reconciliation_tests": "Add reconciliation tests for row counts, checksums, schema, and data quality.",
}


def build_task_data_warehouse_sync_readiness_plan(source: Any) -> SimpleReadinessPlan:
    """Build data warehouse sync readiness records from task-shaped input."""
    return build_simple_readiness_plan(
        source,
        title="Task Data Warehouse Sync Readiness",
        signal_patterns=_SIGNAL_PATTERNS,
        path_signal_patterns=_PATH_SIGNAL_PATTERNS,
        criteria_patterns=_CRITERIA_PATTERNS,
        criterion_guidance=_GUIDANCE,
    )


analyze_task_data_warehouse_sync_readiness = build_task_data_warehouse_sync_readiness_plan
summarize_task_data_warehouse_sync_readiness = build_task_data_warehouse_sync_readiness_plan
generate_task_data_warehouse_sync_readiness = build_task_data_warehouse_sync_readiness_plan
extract_task_data_warehouse_sync_readiness = build_task_data_warehouse_sync_readiness_plan
recommend_task_data_warehouse_sync_readiness = build_task_data_warehouse_sync_readiness_plan


def task_data_warehouse_sync_readiness_plan_to_dict(plan: SimpleReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


def task_data_warehouse_sync_readiness_plan_to_dicts(plan: SimpleReadinessPlan) -> list[dict[str, Any]]:
    return plan.to_dicts()


def task_data_warehouse_sync_readiness_plan_to_markdown(plan: SimpleReadinessPlan) -> str:
    return plan.to_markdown()


__all__ = [
    "SimpleReadinessPlan",
    "analyze_task_data_warehouse_sync_readiness",
    "build_task_data_warehouse_sync_readiness_plan",
    "extract_task_data_warehouse_sync_readiness",
    "generate_task_data_warehouse_sync_readiness",
    "recommend_task_data_warehouse_sync_readiness",
    "summarize_task_data_warehouse_sync_readiness",
    "task_data_warehouse_sync_readiness_plan_to_dict",
    "task_data_warehouse_sync_readiness_plan_to_dicts",
    "task_data_warehouse_sync_readiness_plan_to_markdown",
]
