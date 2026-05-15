"""Extract source-level customer health score requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import (
    KeywordRequirement as SourceCustomerHealthScoreRequirement,
    KeywordRequirementSpec,
    KeywordRequirementsReport as SourceCustomerHealthScoreRequirementsReport,
    build_keyword_requirements_report,
)


_SPECS = (
    KeywordRequirementSpec("score_inputs", re.compile(r"\b(?:score inputs?|health inputs?|input signals?|usage signals?|support signals?)\b", re.I), ("input signals",), {"input signals": re.compile(r"\b(?:usage|support|billing|nps|csat|tickets?|renewal|engagement|product)\b", re.I)}),
    KeywordRequirementSpec("weighting_model", re.compile(r"\b(?:weighting model|score weights?|weighted model|factor weights?|scoring formula)\b", re.I), ("weighting model",), {"weighting model": re.compile(r"\b(?:weight|weighted|formula|coefficient|percent|percentage|points?)\b", re.I)}),
    KeywordRequirementSpec("risk_thresholds", re.compile(r"\b(?:risk thresholds?|health thresholds?|red yellow green|r/y/g|churn risk bands?|risk bands?)\b", re.I), ("risk thresholds",), {"risk thresholds": re.compile(r"\b(?:red|yellow|green|low|medium|high|critical|score below|band|threshold|0-100|\d+)\b", re.I)}),
    KeywordRequirementSpec("account_segments", re.compile(r"\b(?:account segments?|customer segments?|segment health|plan segments?|enterprise segment)\b", re.I), ("account segments",), {"account segments": re.compile(r"\b(?:enterprise|smb|mid-market|plan|tier|region|arr|segment)\b", re.I)}),
    KeywordRequirementSpec("refresh_cadence", re.compile(r"\b(?:refresh cadence|recalculation cadence|score refresh|recompute|daily refresh|weekly refresh)\b", re.I), ("refresh cadence",), {"refresh cadence": re.compile(r"\b(?:hourly|daily|weekly|monthly|real[- ]?time|cadence|schedule|recompute)\b", re.I)}),
    KeywordRequirementSpec("owner_workflow", re.compile(r"\b(?:owner workflow|account owner workflow|csm workflow|success owner|playbook workflow|intervention workflow)\b", re.I), ("owner workflow",), {"owner workflow": re.compile(r"\b(?:csm|task|playbook|follow-up|handoff|intervention|next best action)\b", re.I)}),
    KeywordRequirementSpec("alert_routing", re.compile(r"\b(?:alert routing|health alerts?|risk alerts?|slack alert|email alert|notify owner)\b", re.I), ("alert routing",), {"alert routing": re.compile(r"\b(?:slack|email|pager|queue|owner|routing|notification|channel)\b", re.I)}),
    KeywordRequirementSpec("historical_trend", re.compile(r"\b(?:historical trend|score history|health trend|trendline|score movement|history chart)\b", re.I), ("historical trend",), {"historical trend": re.compile(r"\b(?:history|trend|trendline|movement|previous|delta|snapshot|timeline)\b", re.I)}),
    KeywordRequirementSpec("reporting_surface", re.compile(r"\b(?:reporting surface|health dashboard|customer health report|executive report|score dashboard)\b", re.I), ("reporting surface",), {"reporting surface": re.compile(r"\b(?:dashboard|report|crm|workspace|executive|surface|export)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:customer health score|health scoring|account health score|customer health planning|health score planning)\b", re.I)
_STRUCTURED = re.compile(r"(?:customer|health|score|scoring|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:customer health score|health scoring|account health score|health score planning)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:customer health score|health scoring|account health score|health score planning)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {
    "missing_score_inputs": ("input signals",),
    "missing_risk_thresholds": ("risk thresholds",),
    "missing_owner_workflow": ("owner workflow",),
}


def build_source_customer_health_score_requirements(source: Any) -> SourceCustomerHealthScoreRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Customer Health Score Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_customer_health_score_requirements(source: Any) -> SourceCustomerHealthScoreRequirementsReport:
    return build_source_customer_health_score_requirements(source)


def generate_source_customer_health_score_requirements(source: Any) -> SourceCustomerHealthScoreRequirementsReport:
    return build_source_customer_health_score_requirements(source)


def derive_source_customer_health_score_requirements(source: Any) -> SourceCustomerHealthScoreRequirementsReport:
    return build_source_customer_health_score_requirements(source)


def summarize_source_customer_health_score_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceCustomerHealthScoreRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_customer_health_score_requirements(source_or_result).summary


def source_customer_health_score_requirements_to_dict(report: SourceCustomerHealthScoreRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_customer_health_score_requirements_to_dict.__test__ = False


def source_customer_health_score_requirements_to_dicts(requirements: SourceCustomerHealthScoreRequirementsReport | list[SourceCustomerHealthScoreRequirement] | tuple[SourceCustomerHealthScoreRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceCustomerHealthScoreRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_customer_health_score_requirements_to_dicts.__test__ = False


def source_customer_health_score_requirements_to_markdown(report: SourceCustomerHealthScoreRequirementsReport) -> str:
    return report.to_markdown()


source_customer_health_score_requirements_to_markdown.__test__ = False
