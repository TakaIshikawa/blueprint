"""Assess readiness for third-party vendor, provider, and external API tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskThirdPartyVendorReadinessPlan = SimpleReadinessPlan
TaskThirdPartyVendorReadinessRecord = SimpleReadinessRecord
TaskThirdPartyVendorReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "vendor_integration": re.compile(
        r"\b(?:third[- ]party|vendor|external provider|provider integration|partner integration|"
        r"saas integration|supplier integration)\b",
        re.I,
    ),
    "external_api": re.compile(
        r"\b(?:external api|vendor api|provider api|partner api|webhook provider|sdk integration|"
        r"api client|api integration)\b",
        re.I,
    ),
    "named_provider": re.compile(
        r"\b(?:stripe|adyen|paypal|twilio|sendgrid|mailchimp|salesforce|hubspot|slack|github|"
        r"google|microsoft|aws|s3|openai|algolia|datadog|zendesk)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "vendor_integration": re.compile(r"vendor|third[_-]?party|provider|partner|integrations?", re.I),
    "external_api": re.compile(r"api[_-]?client|webhook|sdk|external[_-]?api", re.I),
    "named_provider": re.compile(
        r"stripe|adyen|paypal|twilio|sendgrid|salesforce|hubspot|slack|github|google|microsoft|aws|openai|algolia|zendesk",
        re.I,
    ),
}
_CRITERIA = {
    "contract_ownership": re.compile(
        r"\b(?:contract owner|vendor owner|service owner|business owner|dri|sla|contract|"
        r"data processing agreement|dpa|terms owner)\b",
        re.I,
    ),
    "credential_handling": re.compile(
        r"\b(?:credential|credentials|api key|secret|token|oauth|key rotation|secret storage|"
        r"vault|kms|environment variable)\b",
        re.I,
    ),
    "rate_limits": re.compile(
        r"\b(?:rate limit|rate limits|quota|throttle|throttling|backoff|retry-after|"
        r"concurrency limit|request limit)\b",
        re.I,
    ),
    "sandbox_testing": re.compile(
        r"\b(?:sandbox|test account|staging account|mock provider|contract test|integration test|"
        r"test mode|fixture|vendor test)\b",
        re.I,
    ),
    "failure_fallback": re.compile(
        r"\b(?:fallback|degrade|degraded mode|circuit breaker|timeout|retry|queue for retry|"
        r"manual fallback|disable integration|fail closed|fail open)\b",
        re.I,
    ),
    "support_escalation": re.compile(
        r"\b(?:support escalation|escalation path|vendor support|support ticket|on-call|"
        r"incident contact|account manager|status page|runbook)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "contract_ownership": "Identify the vendor contract, SLA, data agreement, and business or technical owner.",
    "credential_handling": "Define secure credential storage, rotation, OAuth, token, or API key handling.",
    "rate_limits": "Account for vendor quotas, throttling, retry-after, and backoff behavior.",
    "sandbox_testing": "Exercise the integration against sandbox, staging, mocks, or contract tests.",
    "failure_fallback": "Define fallback, timeout, retry, circuit breaker, or degraded-mode behavior.",
    "support_escalation": "Document vendor support, escalation contacts, runbooks, and incident paths.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:vendor|provider|third[- ]party|external api|partner api)\b"
    r".{0,80}\b(?:required|needed|planned|scope|impact|changes?)\b",
    re.I,
)


def build_task_third_party_vendor_readiness_plan(source: Any) -> TaskThirdPartyVendorReadinessPlan:
    """Build third-party vendor readiness records for task-shaped input."""
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Third-Party Vendor Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_third_party_vendor_readiness(source: Any) -> TaskThirdPartyVendorReadinessPlan:
    return build_task_third_party_vendor_readiness_plan(source)


def extract_task_third_party_vendor_readiness(source: Any) -> TaskThirdPartyVendorReadinessPlan:
    return build_task_third_party_vendor_readiness_plan(source)


def generate_task_third_party_vendor_readiness(source: Any) -> TaskThirdPartyVendorReadinessPlan:
    return build_task_third_party_vendor_readiness_plan(source)


def derive_task_third_party_vendor_readiness(source: Any) -> TaskThirdPartyVendorReadinessPlan:
    return build_task_third_party_vendor_readiness_plan(source)


def summarize_task_third_party_vendor_readiness(source: Any) -> TaskThirdPartyVendorReadinessPlan:
    return build_task_third_party_vendor_readiness_plan(source)


def recommend_task_third_party_vendor_readiness(source: Any) -> tuple[TaskThirdPartyVendorReadinessRecord, ...]:
    return build_task_third_party_vendor_readiness_plan(source).records


def task_third_party_vendor_readiness_plan_to_dict(result: TaskThirdPartyVendorReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_third_party_vendor_readiness_plan_to_dict.__test__ = False


def task_third_party_vendor_readiness_plan_to_dicts(
    result: TaskThirdPartyVendorReadinessPlan | Iterable[TaskThirdPartyVendorReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_third_party_vendor_readiness_plan_to_dicts.__test__ = False
task_third_party_vendor_readiness_to_dicts = task_third_party_vendor_readiness_plan_to_dicts
task_third_party_vendor_readiness_to_dicts.__test__ = False


def task_third_party_vendor_readiness_plan_to_markdown(result: TaskThirdPartyVendorReadinessPlan) -> str:
    return result.to_markdown()


task_third_party_vendor_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskThirdPartyVendorReadinessPlan",
    "TaskThirdPartyVendorReadinessRecord",
    "TaskThirdPartyVendorReadinessRecommendation",
    "analyze_task_third_party_vendor_readiness",
    "build_task_third_party_vendor_readiness_plan",
    "derive_task_third_party_vendor_readiness",
    "extract_task_third_party_vendor_readiness",
    "generate_task_third_party_vendor_readiness",
    "recommend_task_third_party_vendor_readiness",
    "summarize_task_third_party_vendor_readiness",
    "task_third_party_vendor_readiness_plan_to_dict",
    "task_third_party_vendor_readiness_plan_to_dicts",
    "task_third_party_vendor_readiness_plan_to_markdown",
    "task_third_party_vendor_readiness_to_dicts",
]
