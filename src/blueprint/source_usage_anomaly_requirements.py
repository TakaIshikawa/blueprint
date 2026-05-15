"""Extract source-level usage anomaly detection requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceUsageAnomalyRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceUsageAnomalyRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("baseline_window", re.compile(r"\b(?:baseline window|lookback window|baseline period|comparison window|historical baseline)\b", re.I), ("baseline window",), {"baseline window": re.compile(r"\b(?:days?|weeks?|months?|rolling|lookback|previous|historical|\d+)\b", re.I)}),
    KeywordRequirementSpec("anomaly_threshold", re.compile(r"\b(?:anomaly threshold|deviation threshold|spike threshold|drop threshold|z-score|standard deviations?)\b", re.I), ("threshold",), {"threshold": re.compile(r"\b(?:percent|%|sigma|standard deviation|z-score|above|below|\d+)\b", re.I)}),
    KeywordRequirementSpec("monitored_metric", re.compile(r"\b(?:monitored metric|usage metric|metric monitored|event volume|api calls?|active users?)\b", re.I), ("metric",), {"metric": re.compile(r"\b(?:api calls?|events?|sessions?|active users?|seats?|logins?|usage)\b", re.I)}),
    KeywordRequirementSpec("entity_dimension", re.compile(r"\b(?:account dimension|entity dimension|tenant dimension|per account|per tenant|customer dimension)\b", re.I), ("entity dimension",), {"entity dimension": re.compile(r"\b(?:account|tenant|workspace|user|customer|organization|entity)\b", re.I)}),
    KeywordRequirementSpec("alert_destination", re.compile(r"\b(?:alert destination|alert routing|notify destination|slack alert|email alert|pager alert)\b", re.I), ("alert destination",), {"alert destination": re.compile(r"\b(?:slack|email|pager|webhook|queue|channel|destination)\b", re.I)}),
    KeywordRequirementSpec("suppression_rules", re.compile(r"\b(?:suppression rules?|suppress alerts?|mute window|maintenance window|dedupe rule)\b", re.I), ("suppression rule",), {"suppression rule": re.compile(r"\b(?:mute|maintenance|dedupe|cooldown|suppress|ignore|window)\b", re.I)}),
    KeywordRequirementSpec("investigation_workflow", re.compile(r"\b(?:investigation workflow|triage workflow|investigation playbook|root cause workflow|analyst workflow)\b", re.I), ("investigation workflow",), {"investigation workflow": re.compile(r"\b(?:triage|owner|playbook|root cause|analyst|task|runbook)\b", re.I)}),
    KeywordRequirementSpec("reporting_retention", re.compile(r"\b(?:reporting retention|audit retention|anomaly report|anomaly dashboard|retention period)\b", re.I), ("reporting retention",), {"reporting retention": re.compile(r"\b(?:dashboard|report|audit|retain|retention|days?|months?|export)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:usage anomaly|anomaly detection|usage spike|usage drop|anomalous usage)\b", re.I)
_STRUCTURED = re.compile(r"(?:usage|anomaly|detection|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:usage anomaly|anomaly detection|anomalous usage)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:usage anomaly|anomaly detection|anomalous usage)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_baseline_window": ("baseline window",), "missing_threshold": ("threshold",), "missing_investigation_workflow": ("investigation workflow",)}


def build_source_usage_anomaly_requirements(source: Any) -> SourceUsageAnomalyRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Usage Anomaly Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_usage_anomaly_requirements(source: Any) -> SourceUsageAnomalyRequirementsReport:
    return build_source_usage_anomaly_requirements(source)


def generate_source_usage_anomaly_requirements(source: Any) -> SourceUsageAnomalyRequirementsReport:
    return build_source_usage_anomaly_requirements(source)


def derive_source_usage_anomaly_requirements(source: Any) -> SourceUsageAnomalyRequirementsReport:
    return build_source_usage_anomaly_requirements(source)


def summarize_source_usage_anomaly_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceUsageAnomalyRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_usage_anomaly_requirements(source_or_result).summary


def source_usage_anomaly_requirements_to_dict(report: SourceUsageAnomalyRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_usage_anomaly_requirements_to_dict.__test__ = False


def source_usage_anomaly_requirements_to_dicts(requirements: SourceUsageAnomalyRequirementsReport | list[SourceUsageAnomalyRequirement] | tuple[SourceUsageAnomalyRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceUsageAnomalyRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_usage_anomaly_requirements_to_dicts.__test__ = False


def source_usage_anomaly_requirements_to_markdown(report: SourceUsageAnomalyRequirementsReport) -> str:
    return report.to_markdown()


source_usage_anomaly_requirements_to_markdown.__test__ = False
