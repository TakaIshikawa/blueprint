"""Extract source-level data quality requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import (
    KeywordRequirement as SourceDataQualityRequirement,
    KeywordRequirementSpec,
    KeywordRequirementsReport as SourceDataQualityRequirementsReport,
    build_keyword_requirements_report,
)


_SPECS = (
    KeywordRequirementSpec("freshness_sla", re.compile(r"\b(?:freshness sla|freshness|staleness|late arriving|data age|last updated)\b", re.I), ("threshold value",), {"threshold value": re.compile(r"\b(?:sla|within|older than|stale after|every|\d+\s*(?:minute|hour|day)s?)\b", re.I)}),
    KeywordRequirementSpec("completeness_thresholds", re.compile(r"\b(?:completeness threshold|field coverage|missing values?|null rate|required field coverage|row count threshold)\b", re.I), ("threshold value",), {"threshold value": re.compile(r"\b(?:\d+(?:\.\d+)?\s*%|\d+\s*percent|threshold|minimum|maximum|below|above|null rate|coverage)\b", re.I)}),
    KeywordRequirementSpec("duplicate_handling", re.compile(r"\b(?:duplicate handling|dedupe|de-duplicate|duplicate detection|unique key|uniqueness|duplicate records?)\b", re.I), ("duplicate policy",), {"duplicate policy": re.compile(r"\b(?:dedupe|merge|reject|quarantine|unique key|survivorship|latest wins|first wins)\b", re.I)}),
    KeywordRequirementSpec("schema_drift", re.compile(r"\b(?:schema drift|schema change|unexpected column|column drift|contract drift|schema evolution)\b", re.I), ("drift response",), {"drift response": re.compile(r"\b(?:alert|block|fail|quarantine|compatibility|contract|notify|ticket)\b", re.I)}),
    KeywordRequirementSpec("reconciliation_checks", re.compile(r"\b(?:reconciliation checks?|reconcile|control totals?|source totals?|target totals?|checksum|balance check)\b", re.I), ("reconciliation method",), {"reconciliation method": re.compile(r"\b(?:source totals?|target totals?|checksum|count|sum|balance|ledger|control total)\b", re.I)}),
    KeywordRequirementSpec("anomaly_alerts", re.compile(r"\b(?:anomaly alerts?|anomaly detection|outlier|volume spike|volume drop|unexpected spike|unexpected drop)\b", re.I), ("alert channel",), {"alert channel": re.compile(r"\b(?:alert|page|pagerduty|slack|email|dashboard|notify|ticket)\b", re.I)}),
    KeywordRequirementSpec("backfill_correction", re.compile(r"\b(?:backfill correction|backfill|correction|repair historical|reprocess|data repair|fix forward)\b", re.I), ("correction plan",), {"correction plan": re.compile(r"\b(?:backfill|reprocess|repair|rerun|correction|restore|fix forward|rollback)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:data quality|dataset|warehouse|pipeline|table|freshness|completeness|schema drift|reconciliation|anomaly|backfill|duplicates?)\b", re.I)
_STRUCTURED = re.compile(r"(?:data|quality|dataset|requirements?|acceptance|source_payload|validation)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:data quality|freshness|completeness|schema drift|reconciliation|anomaly|backfill|duplicates?)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:data quality|freshness|completeness|schema drift|reconciliation|anomaly|backfill|duplicates?)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {
    "missing_threshold_values": ("threshold value",),
    "missing_duplicate_policy": ("duplicate policy",),
    "missing_drift_response": ("drift response",),
    "missing_reconciliation_method": ("reconciliation method",),
    "missing_alert_channel": ("alert channel",),
    "missing_correction_plan": ("correction plan",),
}
_THRESHOLD_RE = re.compile(r"\b\d+(?:\.\d+)?\s*(?:%|percent|minutes?|hours?|days?|rows?|records?)\b", re.I)
_DATASET_RE = re.compile(r"\b(?:dataset|table|pipeline|feed|warehouse table)\s+([A-Za-z][\w.:-]*)", re.I)


def build_source_data_quality_requirements(source: Any) -> SourceDataQualityRequirementsReport:
    report = build_keyword_requirements_report(
        source,
        title="Source Data Quality Requirements Report",
        specs=_SPECS,
        context_pattern=_CONTEXT,
        structured_field_pattern=_STRUCTURED,
        negated_pattern=_NEGATED,
        summary_flag_groups=_FLAGS,
    )
    summary = dict(report.summary)
    evidence_text = " ".join(evidence for requirement in report.requirements for evidence in requirement.evidence)
    summary["category_counts"] = dict(summary.get("type_counts", {}))
    summary["threshold_values"] = sorted(set(match.group(0) for match in _THRESHOLD_RE.finditer(evidence_text)), key=str.casefold)
    summary["affected_datasets"] = sorted(set(match.group(1).strip(".,") for match in _DATASET_RE.finditer(evidence_text)), key=str.casefold)
    summary["status"] = "no_data_quality_requirements" if not report.requirements else ("needs_detail" if summary.get("missing_detail_flags") else "ready_for_data_quality_planning")
    return SourceDataQualityRequirementsReport(source_id=report.source_id, requirements=report.requirements, summary=summary, title=report.title)


extract_source_data_quality_requirements = build_source_data_quality_requirements
generate_source_data_quality_requirements = build_source_data_quality_requirements
derive_source_data_quality_requirements = build_source_data_quality_requirements


def summarize_source_data_quality_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceDataQualityRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_data_quality_requirements(source_or_result).summary


def source_data_quality_requirements_to_dict(report: SourceDataQualityRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_data_quality_requirements_to_dict.__test__ = False


def source_data_quality_requirements_to_dicts(report: SourceDataQualityRequirementsReport | tuple[SourceDataQualityRequirement, ...] | list[SourceDataQualityRequirement]) -> list[dict[str, Any]]:
    if isinstance(report, SourceDataQualityRequirementsReport):
        return report.to_dicts()
    return [requirement.to_dict() for requirement in report]


source_data_quality_requirements_to_dicts.__test__ = False


def source_data_quality_requirements_to_markdown(report: SourceDataQualityRequirementsReport) -> str:
    return report.to_markdown()


source_data_quality_requirements_to_markdown.__test__ = False

