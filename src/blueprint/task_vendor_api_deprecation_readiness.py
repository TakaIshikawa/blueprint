"""Analyze vendor API deprecation readiness for execution-plan tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, build_simple_readiness_plan


_SIGNAL_PATTERNS = {
    "vendor_deprecation": re.compile(r"\b(?:vendor|third[_\s-]+party|partner|external|provider|sdk).{0,80}\b(?:deprecat(?:e|ed|ion)|retire(?:d|ment)|sunset|endpoint[_\s-]+removal|api[_\s-]+removal)\b|\b(?:deprecat(?:e|ed|ion)|retire(?:d|ment)|sunset|endpoint[_\s-]+removal).{0,80}\b(?:vendor|third[_\s-]+party|partner|external|provider|sdk)\b", re.I),
    "third_party_integration": re.compile(r"\b(?:stripe|salesforce|zendesk|slack|github|google|microsoft|shopify|vendor[_\s-]+api|third[_\s-]+party[_\s-]+api|external[_\s-]+integration)\b", re.I),
}
_PATH_SIGNAL_PATTERNS = {
    "vendor_integration_path": re.compile(r"\b(?:integrations?|vendors?|providers?|clients?|sdk).*(?:deprecation|sunset|migration|retirement|replacement)\b|\b(?:stripe|salesforce|zendesk|shopify|slack|github|google|microsoft).*(?:client|sdk|integration)\b", re.I),
}
_CRITERIA_PATTERNS = {
    "vendor_version": re.compile(r"\b(?:vendor[_\s-]+version|api[_\s-]+version|sdk[_\s-]+version|v\d+(?:\.\d+)?|deprecated[_\s-]+version|retiring[_\s-]+version)\b", re.I),
    "impacted_integration_paths": re.compile(r"\b(?:impacted[_\s-]+integration[_\s-]+paths?|call[_\s-]+sites?|endpoints?|routes?|webhooks?|clients?|integration[_\s-]+paths?)\b", re.I),
    "replacement_api": re.compile(r"\b(?:replacement[_\s-]+api|new[_\s-]+api|replacement[_\s-]+endpoint|successor[_\s-]+endpoint|migrate[_\s-]+to|new[_\s-]+sdk)\b", re.I),
    "migration_sequencing": re.compile(r"\b(?:migration[_\s-]+sequencing|sequence|phases?|rollout|cutover|dual[_\s-]+write|dual[_\s-]+read|canary|timeline)\b", re.I),
    "compatibility_tests": re.compile(r"\b(?:compatibility[_\s-]+tests?|contract[_\s-]+tests?|sandbox[_\s-]+tests?|integration[_\s-]+tests?|fixture|mock[_\s-]+vendor)\b", re.I),
    "observability": re.compile(r"\b(?:observability|monitoring|metrics?|dashboard|alerts?|error[_\s-]+rate|vendor[_\s-]+errors?|latency)\b", re.I),
    "rollback_fallback": re.compile(r"\b(?:rollback|fallback|backout|feature[_\s-]+flag|kill[_\s-]+switch|legacy[_\s-]+path|graceful[_\s-]+degradation)\b", re.I),
    "customer_support_communication": re.compile(r"\b(?:customer[_\s-]+communication|support[_\s-]+communication|release[_\s-]+notes?|status[_\s-]+page|migration[_\s-]+notice|support[_\s-]+runbook|customer[_\s-]+impact)\b", re.I),
}
_GUIDANCE = {
    "vendor_version": "Name the vendor, deprecated API or SDK version, retirement date, and replacement version.",
    "impacted_integration_paths": "List impacted integration paths, endpoints, webhooks, clients, and call sites.",
    "replacement_api": "Document the replacement API, endpoint, SDK, payload contract, and auth changes.",
    "migration_sequencing": "Define migration sequencing, rollout phases, cutover, canary, and timeline.",
    "compatibility_tests": "Add compatibility, contract, sandbox, and integration tests for old and new vendor behavior.",
    "observability": "Add metrics, dashboards, and alerts for vendor errors, latency, and migration health.",
    "rollback_fallback": "Provide rollback, fallback, feature-flag, or graceful-degradation behavior.",
    "customer_support_communication": "Prepare customer/support communication, release notes, status updates, and runbook details.",
}


def build_task_vendor_api_deprecation_readiness_plan(source: Any) -> SimpleReadinessPlan:
    """Build vendor API deprecation readiness records from task-shaped input."""
    return build_simple_readiness_plan(
        source,
        title="Task Vendor API Deprecation Readiness",
        signal_patterns=_SIGNAL_PATTERNS,
        path_signal_patterns=_PATH_SIGNAL_PATTERNS,
        criteria_patterns=_CRITERIA_PATTERNS,
        criterion_guidance=_GUIDANCE,
    )


analyze_task_vendor_api_deprecation_readiness = build_task_vendor_api_deprecation_readiness_plan
summarize_task_vendor_api_deprecation_readiness = build_task_vendor_api_deprecation_readiness_plan
generate_task_vendor_api_deprecation_readiness = build_task_vendor_api_deprecation_readiness_plan
extract_task_vendor_api_deprecation_readiness = build_task_vendor_api_deprecation_readiness_plan
recommend_task_vendor_api_deprecation_readiness = build_task_vendor_api_deprecation_readiness_plan


def task_vendor_api_deprecation_readiness_plan_to_dict(plan: SimpleReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


def task_vendor_api_deprecation_readiness_plan_to_dicts(plan: SimpleReadinessPlan) -> list[dict[str, Any]]:
    return plan.to_dicts()


def task_vendor_api_deprecation_readiness_plan_to_markdown(plan: SimpleReadinessPlan) -> str:
    return plan.to_markdown()


__all__ = [
    "SimpleReadinessPlan",
    "analyze_task_vendor_api_deprecation_readiness",
    "build_task_vendor_api_deprecation_readiness_plan",
    "extract_task_vendor_api_deprecation_readiness",
    "generate_task_vendor_api_deprecation_readiness",
    "recommend_task_vendor_api_deprecation_readiness",
    "summarize_task_vendor_api_deprecation_readiness",
    "task_vendor_api_deprecation_readiness_plan_to_dict",
    "task_vendor_api_deprecation_readiness_plan_to_dicts",
    "task_vendor_api_deprecation_readiness_plan_to_markdown",
]
