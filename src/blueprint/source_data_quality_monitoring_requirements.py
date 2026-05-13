"""Extract source-level data quality monitoring requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import (
    KeywordRequirement as SourceDataQualityMonitoringRequirement,
    KeywordRequirementSpec,
    KeywordRequirementsReport as SourceDataQualityMonitoringRequirementsReport,
    build_keyword_requirements_report,
)


_SPECS = (
    KeywordRequirementSpec("quality_dimensions", re.compile(r"\b(?:quality dimensions?|accuracy|validity|consistency|uniqueness|timeliness|data quality dimensions?)\b", re.I), ("named dimensions",), {"named dimensions": re.compile(r"\b(?:accuracy|validity|consistency|uniqueness|timeliness|completeness)\b", re.I)}),
    KeywordRequirementSpec("validation_rules", re.compile(r"\b(?:validation rules?|data validation|schema validation|constraint checks?|range checks?|referential integrity)\b", re.I), ("validation rule definitions",), {"validation rule definitions": re.compile(r"\b(?:rule|schema|constraint|range|regex|referential|valid values?)\b", re.I)}),
    KeywordRequirementSpec("freshness_checks", re.compile(r"\b(?:freshness checks?|data freshness|staleness|late arriving|last updated|age of data|freshness sla)\b", re.I), ("freshness threshold",), {"freshness threshold": re.compile(r"\b(?:within|older than|stale after|sla|\d+\s*(?:minute|hour|day)s?)\b", re.I)}),
    KeywordRequirementSpec("completeness_thresholds", re.compile(r"\b(?:completeness thresholds?|missing values?|null rate|row count threshold|coverage threshold|required field coverage)\b", re.I), ("threshold value",), {"threshold value": re.compile(r"\b(?:threshold|percent|%|\d+(?:\.\d+)?\s*%|minimum|maximum|null rate|row count)\b", re.I)}),
    KeywordRequirementSpec("anomaly_detection", re.compile(r"\b(?:anomaly detection|outlier detection|drift detection|volume anomaly|statistical anomaly|unexpected spike|unexpected drop)\b", re.I), ("anomaly method",), {"anomaly method": re.compile(r"\b(?:baseline|statistical|z-score|seasonal|drift|outlier|spike|drop)\b", re.I)}),
    KeywordRequirementSpec("quarantine_remediation", re.compile(r"\b(?:quarantine|remediation|bad records?|invalid records?|dead letter|reject records?|repair workflow|backfill correction)\b", re.I), ("remediation path",), {"remediation path": re.compile(r"\b(?:quarantine|repair|remediate|ticket|backfill|reject|dead letter|retry|workflow)\b", re.I)}),
    KeywordRequirementSpec("owner_escalation", re.compile(r"\b(?:data owner|owner escalation|escalation|steward|accountable owner|responsible team|on-call owner)\b", re.I), ("owner/escalation",), {"owner/escalation": re.compile(r"\b(?:steward|responsible team|on-call|pager|slack|ticket|assigned to)\b", re.I)}),
    KeywordRequirementSpec("dashboards_alerts", re.compile(r"\b(?:dashboard|alert|alerts|monitoring dashboard|quality dashboard|pagerduty|slack alert|notification)\b", re.I), ("dashboard or alert channel",), {"dashboard or alert channel": re.compile(r"\b(?:dashboard|alert|pagerduty|slack|email|notification|metric)\b", re.I)}),
    KeywordRequirementSpec("audit_history", re.compile(r"\b(?:audit history|quality history|audit log|historical quality|trend history|run history|check history)\b", re.I), ("audit history retention",), {"audit history retention": re.compile(r"\b(?:history|audit|retain|retention|trend|run log|\d+\s*(?:day|month|year)s?)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:data quality|quality monitoring|quality checks?|data validation|freshness|completeness|dq)\b", re.I)
_STRUCTURED = re.compile(r"(?:data|quality|validation|monitoring|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:data quality|quality monitoring|data validation|freshness|completeness)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:data quality|quality monitoring|data validation|freshness|completeness)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_OBSERVABILITY_ONLY = re.compile(r"\b(?:service observability|api monitoring|uptime|latency|cpu|memory|logs|traces)\b", re.I)
_FLAGS = {
    "missing_thresholds": ("freshness threshold", "threshold value"),
    "missing_remediation_path": ("remediation path",),
    "missing_ownership_escalation": ("owner/escalation",),
}


def build_source_data_quality_monitoring_requirements(source: Any) -> SourceDataQualityMonitoringRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Data Quality Monitoring Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS, unrelated_pattern=_OBSERVABILITY_ONLY)


def extract_source_data_quality_monitoring_requirements(source: Any) -> SourceDataQualityMonitoringRequirementsReport:
    return build_source_data_quality_monitoring_requirements(source)


def generate_source_data_quality_monitoring_requirements(source: Any) -> SourceDataQualityMonitoringRequirementsReport:
    return build_source_data_quality_monitoring_requirements(source)


def derive_source_data_quality_monitoring_requirements(source: Any) -> SourceDataQualityMonitoringRequirementsReport:
    return build_source_data_quality_monitoring_requirements(source)


def summarize_source_data_quality_monitoring_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceDataQualityMonitoringRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_data_quality_monitoring_requirements(source_or_result).summary


def source_data_quality_monitoring_requirements_to_dict(report: SourceDataQualityMonitoringRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_data_quality_monitoring_requirements_to_dict.__test__ = False


def source_data_quality_monitoring_requirements_to_dicts(requirements: SourceDataQualityMonitoringRequirementsReport | list[SourceDataQualityMonitoringRequirement] | tuple[SourceDataQualityMonitoringRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceDataQualityMonitoringRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_data_quality_monitoring_requirements_to_dicts.__test__ = False


def source_data_quality_monitoring_requirements_to_markdown(report: SourceDataQualityMonitoringRequirementsReport) -> str:
    return report.to_markdown()


source_data_quality_monitoring_requirements_to_markdown.__test__ = False
