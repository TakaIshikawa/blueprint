"""Extract source-level customer support workflow requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceCustomerSupportRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceCustomerSupportRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("ticket_intake", re.compile(r"\b(?:ticket intake|support intake|case intake|inbound tickets?|support form)\b", re.I), ("intake channel",), {"intake channel": re.compile(r"\b(?:email|chat|form|portal|api|webhook|channel)\b", re.I)}),
    KeywordRequirementSpec("routing_assignment", re.compile(r"\b(?:routing|assignment|assign tickets?|queue routing|triage queue)\b", re.I), ("routing rule",), {"routing rule": re.compile(r"\b(?:queue|skill|region|tier|round robin|team|routing rule|assignment rule)\b", re.I)}),
    KeywordRequirementSpec("sla_priority", re.compile(r"\b(?:sla priority|priority sla|response sla|priority rules?|severity)\b", re.I), ("sla threshold",), {"sla threshold": re.compile(r"\b(?:p[0-3]|sev|severity|hours?|minutes?|response time)\b", re.I)}),
    KeywordRequirementSpec("customer_context", re.compile(r"\b(?:customer context|account context|customer profile|plan context|support history)\b", re.I), ("context fields",), {"context fields": re.compile(r"\b(?:plan|arr|account|history|tickets?|orders?|subscription|profile)\b", re.I)}),
    KeywordRequirementSpec("internal_notes", re.compile(r"\b(?:internal notes?|private notes?|agent notes?|collaboration notes?)\b", re.I), ("note visibility",), {"note visibility": re.compile(r"\b(?:internal|private|agent|visibility|permissions?|collaboration)\b", re.I)}),
    KeywordRequirementSpec("escalation", re.compile(r"\b(?:escalation|escalate|manager review|tier 2|tier two|handoff)\b", re.I), ("escalation path",), {"escalation path": re.compile(r"\b(?:tier 2|manager|engineering|handoff|pager|path|approval|escalate)\b", re.I)}),
    KeywordRequirementSpec("resolution_notification", re.compile(r"\b(?:resolution notification|resolution email|close notification|customer update|notify customer)\b", re.I), ("notification channel",), {"notification channel": re.compile(r"\b(?:email|sms|push|in-app|template|message|notification)\b", re.I)}),
    KeywordRequirementSpec("reporting_metrics", re.compile(r"\b(?:reporting metrics?|support metrics?|csat|first response|resolution time|dashboard)\b", re.I), ("metric definition",), {"metric definition": re.compile(r"\b(?:csat|first response|resolution time|backlog|dashboard|metric|volume)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:customer support|support workflow|support ticket|ticket workflow|case management)\b", re.I)
_STRUCTURED = re.compile(r"(?:customer|support|ticket|case|workflow|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:customer support|support workflow|support ticket|ticket workflow)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:customer support|support workflow|support ticket|ticket workflow)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_routing": ("routing rule",), "missing_sla": ("sla threshold",), "missing_escalation": ("escalation path",)}


def build_source_customer_support_requirements(source: Any) -> SourceCustomerSupportRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Customer Support Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_customer_support_requirements(source: Any) -> SourceCustomerSupportRequirementsReport:
    return build_source_customer_support_requirements(source)


def generate_source_customer_support_requirements(source: Any) -> SourceCustomerSupportRequirementsReport:
    return build_source_customer_support_requirements(source)


def derive_source_customer_support_requirements(source: Any) -> SourceCustomerSupportRequirementsReport:
    return build_source_customer_support_requirements(source)


def summarize_source_customer_support_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceCustomerSupportRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_customer_support_requirements(source_or_result).summary


def source_customer_support_requirements_to_dict(report: SourceCustomerSupportRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_customer_support_requirements_to_dict.__test__ = False


def source_customer_support_requirements_to_dicts(requirements: SourceCustomerSupportRequirementsReport | list[SourceCustomerSupportRequirement] | tuple[SourceCustomerSupportRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceCustomerSupportRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_customer_support_requirements_to_dicts.__test__ = False


def source_customer_support_requirements_to_markdown(report: SourceCustomerSupportRequirementsReport) -> str:
    return report.to_markdown()


source_customer_support_requirements_to_markdown.__test__ = False
