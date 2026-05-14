"""Extract source-level data warehouse sync requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceDataWarehouseSyncRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceDataWarehouseSyncRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("sync_cadence", re.compile(r"\b(?:sync cadence|sync schedule|warehouse sync schedule|replication cadence|export cadence)\b", re.I), ("cadence",), {"cadence": re.compile(r"\b(?:hourly|daily|near real[- ]?time|every|\d+\s*(?:minute|hour|day)s?|cron)\b", re.I)}),
    KeywordRequirementSpec("incremental_cursor_watermark", re.compile(r"\b(?:incremental cursor|watermark|high[- ]water mark|cursor field|updated_at cursor|cdc cursor)\b", re.I), ("cursor/watermark",), {"cursor/watermark": re.compile(r"\b(?:updated_at|created_at|sequence|lsn|timestamp|cdc)\b", re.I)}),
    KeywordRequirementSpec("schema_mapping", re.compile(r"\b(?:schema mapping|field mapping|column mapping|warehouse schema|destination schema)\b", re.I), ("mapping rules",), {"mapping rules": re.compile(r"\b(?:column|field|type|mapping|rename|transform|destination table)\b", re.I)}),
    KeywordRequirementSpec("backfill_behavior", re.compile(r"\b(?:backfill behavior|backfill|historical load|reprocess history|initial load)\b", re.I), ("backfill plan",), {"backfill plan": re.compile(r"\b(?:historical|initial load|reprocess|date range|batch size|rerun)\b", re.I)}),
    KeywordRequirementSpec("failure_retry", re.compile(r"\b(?:failure retry|sync retry|retry failed sync|retry policy|failed export)\b", re.I), ("retry behavior",), {"retry behavior": re.compile(r"\b(?:retry|backoff|attempt|dlq|alert|resume|recover)\b", re.I)}),
    KeywordRequirementSpec("reconciliation_checks", re.compile(r"\b(?:reconciliation checks?|reconcile|row count|checksum|control totals?|source totals?|target totals?)\b", re.I), ("reconciliation method",), {"reconciliation method": re.compile(r"\b(?:row count|checksum|control total|source total|target total|sum|compare|balance)\b", re.I)}),
    KeywordRequirementSpec("freshness_sla", re.compile(r"\b(?:freshness sla|freshness target|latency sla|staleness|data age)\b", re.I), ("freshness target",), {"freshness target": re.compile(r"\b(?:within|under|less than|sla|\d+\s*(?:minute|hour|day)s?|stale after)\b", re.I)}),
    KeywordRequirementSpec("audit_logging", re.compile(r"\b(?:audit logging|audit log|sync audit|run history|change log)\b", re.I), ("audit events",), {"audit events": re.compile(r"\b(?:audit|run id|actor|timestamp|row count|status|history|log)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:data warehouse sync|warehouse sync|warehouse export|warehouse replication|analytics warehouse)\b", re.I)
_STRUCTURED = re.compile(r"(?:data|warehouse|sync|export|replication|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:data warehouse sync|warehouse sync|warehouse export|warehouse replication)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:data warehouse sync|warehouse sync|warehouse export|warehouse replication)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_cursor_or_watermark": ("cursor/watermark",), "missing_backfill_behavior": ("backfill plan",), "missing_reconciliation_checks": ("reconciliation method",)}


def build_source_data_warehouse_sync_requirements(source: Any) -> SourceDataWarehouseSyncRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Data Warehouse Sync Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_data_warehouse_sync_requirements(source: Any) -> SourceDataWarehouseSyncRequirementsReport:
    return build_source_data_warehouse_sync_requirements(source)


def generate_source_data_warehouse_sync_requirements(source: Any) -> SourceDataWarehouseSyncRequirementsReport:
    return build_source_data_warehouse_sync_requirements(source)


def derive_source_data_warehouse_sync_requirements(source: Any) -> SourceDataWarehouseSyncRequirementsReport:
    return build_source_data_warehouse_sync_requirements(source)


def summarize_source_data_warehouse_sync_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceDataWarehouseSyncRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_data_warehouse_sync_requirements(source_or_result).summary


def source_data_warehouse_sync_requirements_to_dict(report: SourceDataWarehouseSyncRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_data_warehouse_sync_requirements_to_dict.__test__ = False


def source_data_warehouse_sync_requirements_to_dicts(requirements: SourceDataWarehouseSyncRequirementsReport | list[SourceDataWarehouseSyncRequirement] | tuple[SourceDataWarehouseSyncRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceDataWarehouseSyncRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_data_warehouse_sync_requirements_to_dicts.__test__ = False


def source_data_warehouse_sync_requirements_to_markdown(report: SourceDataWarehouseSyncRequirementsReport) -> str:
    return report.to_markdown()


source_data_warehouse_sync_requirements_to_markdown.__test__ = False
