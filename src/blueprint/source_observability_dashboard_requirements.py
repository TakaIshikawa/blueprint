"""Extract source-level observability dashboard requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import (
    KeywordRequirement as SourceObservabilityDashboardRequirement,
    KeywordRequirementSpec,
    KeywordRequirementsReport as SourceObservabilityDashboardRequirementsReport,
    build_keyword_requirements_report,
)


_SPECS = (
    KeywordRequirementSpec("dashboard_audience", re.compile(r"\b(?:dashboard audience|audience|viewer persona|stakeholders?|operators?|executives?|support team)\b", re.I), ("audience",), {"audience": re.compile(r"\b(?:operators?|executives?|support|sre|engineering|product|stakeholders?|viewer persona)\b", re.I)}),
    KeywordRequirementSpec("key_metrics", re.compile(r"\b(?:key metrics?|golden signals?|latency|error rate|traffic|saturation|availability|uptime|slo burn)\b", re.I), ("metric list",), {"metric list": re.compile(r"\b(?:latency|error rate|traffic|saturation|availability|uptime|throughput|slo|burn rate|p\d+)\b", re.I)}),
    KeywordRequirementSpec("data_source", re.compile(r"\b(?:data sources?|metric source|telemetry source|prometheus|datadog|grafana|logs?|traces?|events?)\b", re.I), ("data source",), {"data source": re.compile(r"\b(?:prometheus|datadog|grafana|cloudwatch|logs?|traces?|metrics?|events?|warehouse)\b", re.I)}),
    KeywordRequirementSpec("filters_dimensions", re.compile(r"\b(?:filters?|dimensions?|breakdowns?|environment filter|region filter|service filter|tenant filter)\b", re.I), ("filters/dimensions",), {"filters/dimensions": re.compile(r"\b(?:environment|region|service|tenant|plan|version|dimension|filter|breakdown)\b", re.I)}),
    KeywordRequirementSpec("alert_links", re.compile(r"\b(?:alert links?|linked alerts?|pagerduty|alert rules?|incident links?|on-call links?)\b", re.I), ("alert link",), {"alert link": re.compile(r"\b(?:pagerduty|alert rule|incident|on-call|slack|link|runbook)\b", re.I)}),
    KeywordRequirementSpec("refresh_cadence", re.compile(r"\b(?:refresh cadence|refresh interval|update cadence|auto-refresh|real[- ]?time|near real[- ]?time)\b", re.I), ("refresh cadence",), {"refresh cadence": re.compile(r"\b(?:real[- ]?time|near real[- ]?time|auto-refresh|every|\d+\s*(?:second|minute|hour)s?)\b", re.I)}),
    KeywordRequirementSpec("ownership", re.compile(r"\b(?:dashboard owner|ownership|owning team|maintainer|steward|accountable team)\b", re.I), ("owner",), {"owner": re.compile(r"\b(?:owner|owning team|maintainer|steward|sre|data team|accountable)\b", re.I)}),
    KeywordRequirementSpec("runbook_links", re.compile(r"\b(?:runbook links?|runbooks?|playbooks?|remediation guide|operational guide)\b", re.I), ("runbook link",), {"runbook link": re.compile(r"\b(?:runbook|playbook|remediation|guide|wiki|link|docs?)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:observability dashboard|monitoring dashboard|ops dashboard|service dashboard|sre dashboard|telemetry dashboard)\b", re.I)
_STRUCTURED = re.compile(r"(?:observability|dashboard|monitoring|telemetry|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:observability dashboard|monitoring dashboard|ops dashboard|telemetry dashboard)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:observability dashboard|monitoring dashboard|ops dashboard|telemetry dashboard)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {
    "missing_metric_detail": ("metric list",),
    "missing_data_source": ("data source",),
    "missing_ownership": ("owner",),
}


def build_source_observability_dashboard_requirements(source: Any) -> SourceObservabilityDashboardRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Observability Dashboard Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_observability_dashboard_requirements(source: Any) -> SourceObservabilityDashboardRequirementsReport:
    return build_source_observability_dashboard_requirements(source)


def generate_source_observability_dashboard_requirements(source: Any) -> SourceObservabilityDashboardRequirementsReport:
    return build_source_observability_dashboard_requirements(source)


def derive_source_observability_dashboard_requirements(source: Any) -> SourceObservabilityDashboardRequirementsReport:
    return build_source_observability_dashboard_requirements(source)


def summarize_source_observability_dashboard_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceObservabilityDashboardRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_observability_dashboard_requirements(source_or_result).summary


def source_observability_dashboard_requirements_to_dict(report: SourceObservabilityDashboardRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_observability_dashboard_requirements_to_dict.__test__ = False


def source_observability_dashboard_requirements_to_dicts(requirements: SourceObservabilityDashboardRequirementsReport | list[SourceObservabilityDashboardRequirement] | tuple[SourceObservabilityDashboardRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceObservabilityDashboardRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_observability_dashboard_requirements_to_dicts.__test__ = False


def source_observability_dashboard_requirements_to_markdown(report: SourceObservabilityDashboardRequirementsReport) -> str:
    return report.to_markdown()


source_observability_dashboard_requirements_to_markdown.__test__ = False
