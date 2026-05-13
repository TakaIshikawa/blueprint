"""Analyze API version deprecation readiness for execution-plan tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, build_simple_readiness_plan


_SIGNAL_PATTERNS = {
    "api_version_deprecation": re.compile(
        r"\b(?:api|rest|graphql|grpc)[_\s-]+(?:version|v\d+)[_\s-]+(?:deprecation|sunset|retirement|removal|shutdown)\b|"
        r"\b(?:deprecat(?:e|ion)|sunset|retire|remove).{0,100}\b(?:api|endpoint|version|v\d+)\b",
        re.I,
    ),
    "endpoint_sunset": re.compile(r"\b(?:endpoint|route|resource|operation)[_\s-]+(?:sunset|deprecation|retirement|removal)\b|\bsunset.{0,80}\b(?:endpoint|route|resource|operation)\b", re.I),
    "field_removal": re.compile(r"\b(?:field|parameter|param|attribute|property)[_\s-]+(?:removal|deprecation|sunset|deleted?)\b|\bremove.{0,80}\b(?:field|parameter|param|attribute|property)\b", re.I),
    "sdk_breaking_change": re.compile(r"\b(?:sdk|client|contract|schema)[_\s-]+(?:breaking[_\s-]+change|deprecation|removal|major[_\s-]+version)\b|\bbreaking[_\s-]+change.{0,80}\b(?:sdk|client|contract|schema)\b", re.I),
    "compatibility_window": re.compile(r"\b(?:compatibility|backward[_\s-]+compatibility|support)[_\s-]+(?:window|period|fallback|mode)\b|\bdual[_\s-]+support\b", re.I),
    "migration_deadline": re.compile(r"\b(?:migration|upgrade|customer)[_\s-]+deadline\b|\bdeadline.{0,80}\b(?:migrate|upgrade|v\d+)\b|\b(?:eol|end[_\s-]+of[_\s-]+life|end[_\s-]+date)\b", re.I),
}
_PATH_SIGNAL_PATTERNS = {
    "api_version_deprecation": re.compile(r"(?:api|rest|graphql|grpc|v\d+).*(?:deprecat|sunset|retire|remove|eol)|(?:deprecat|sunset|retire|remove|eol).*(?:api|rest|graphql|grpc|v\d+)", re.I),
    "endpoint_sunset": re.compile(r"(?:endpoint|route|resource|operation).*(?:sunset|deprecat|retire|remove)|(?:sunset|deprecat|retire|remove).*(?:endpoint|route|resource|operation)", re.I),
    "field_removal": re.compile(r"(?:field|parameter|param|attribute|property).*(?:remove|deprecat|sunset)", re.I),
    "sdk_breaking_change": re.compile(r"(?:sdk|client|contract|schema).*(?:breaking|deprecat|remove|v\d+)", re.I),
    "compatibility_window": re.compile(r"(?:compat|fallback|dual[_-]?support|support[_-]?window)", re.I),
    "migration_deadline": re.compile(r"(?:migration|upgrade).*(?:deadline|eol|end[_-]?date)|(?:deadline|eol|end[_-]?date).*(?:migration|upgrade)", re.I),
}
_CRITERIA_PATTERNS = {
    "usage_inventory": re.compile(r"\b(?:usage[_\s-]+inventory|consumer[_\s-]+inventory|customer[_\s-]+inventory|client[_\s-]+inventory|traffic[_\s-]+audit|endpoint[_\s-]+usage|adoption|call[_\s-]+volume)\b", re.I),
    "migration_guide": re.compile(r"\b(?:migration[_\s-]+guide|upgrade[_\s-]+guide|migration[_\s-]+docs?|replacement[_\s-]+endpoint|mapping[_\s-]+guide|code[_\s-]+samples?|sdk[_\s-]+guide)\b", re.I),
    "customer_notice": re.compile(r"\b(?:customer[_\s-]+notice|developer[_\s-]+notice|partner[_\s-]+notice|changelog|release[_\s-]+notes?|email[_\s-]+notice|announce(?:ment)?|communication[_\s-]+plan)\b", re.I),
    "compatibility_fallback": re.compile(r"\b(?:compatibility[_\s-]+fallback|fallback|legacy[_\s-]+mode|dual[_\s-]+support|compatibility[_\s-]+window|grace[_\s-]+period|backward[_\s-]+compatible|shim)\b", re.I),
    "telemetry": re.compile(r"\b(?:telemetry|metrics?|dashboard|monitor(?:ing)?|alerts?|logs?|usage[_\s-]+tracking|remaining[_\s-]+traffic|error[_\s-]+rate)\b", re.I),
    "owner": re.compile(r"\b(?:owner|owned[_\s-]+by|responsible[_\s-]+team|dri|on[_\s-]+call|service[_\s-]+owner|api[_\s-]+owner)\b", re.I),
    "removal_criteria": re.compile(r"\b(?:removal[_\s-]+criteria|sunset[_\s-]+criteria|exit[_\s-]+criteria|deprecation[_\s-]+gate|zero[_\s-]+traffic|migration[_\s-]+complete|delete[_\s-]+after|final[_\s-]+removal)\b", re.I),
}
_GUIDANCE = {
    "usage_inventory": "Inventory customers, clients, endpoints, traffic, and call volume still using the deprecated API surface.",
    "migration_guide": "Publish migration or upgrade guidance with replacement endpoints, field mappings, SDK notes, and examples.",
    "customer_notice": "Prepare customer, developer, partner, changelog, release-note, or email notice.",
    "compatibility_fallback": "Define compatibility fallback, grace period, legacy mode, shim, or dual-support behavior.",
    "telemetry": "Track usage, remaining traffic, errors, dashboards, alerts, and removal readiness telemetry.",
    "owner": "Name the API owner, responsible team, DRI, or on-call path for the deprecation.",
    "removal_criteria": "Set concrete removal criteria such as zero traffic, migration complete, gates, or final removal date.",
}


def build_task_api_version_deprecation_readiness_plan(source: Any) -> SimpleReadinessPlan:
    """Build API version deprecation readiness records from task-shaped input."""
    return build_simple_readiness_plan(
        source,
        title="Task API Version Deprecation Readiness",
        signal_patterns=_SIGNAL_PATTERNS,
        path_signal_patterns=_PATH_SIGNAL_PATTERNS,
        criteria_patterns=_CRITERIA_PATTERNS,
        criterion_guidance=_GUIDANCE,
    )


analyze_task_api_version_deprecation_readiness = build_task_api_version_deprecation_readiness_plan
summarize_task_api_version_deprecation_readiness = build_task_api_version_deprecation_readiness_plan
generate_task_api_version_deprecation_readiness = build_task_api_version_deprecation_readiness_plan
extract_task_api_version_deprecation_readiness = build_task_api_version_deprecation_readiness_plan
recommend_task_api_version_deprecation_readiness = build_task_api_version_deprecation_readiness_plan


def task_api_version_deprecation_readiness_plan_to_dict(plan: SimpleReadinessPlan) -> dict[str, Any]:
    """Serialize an API version deprecation readiness plan."""
    return plan.to_dict()


def task_api_version_deprecation_readiness_plan_to_dicts(plan: SimpleReadinessPlan) -> list[dict[str, Any]]:
    """Serialize API version deprecation readiness records."""
    return plan.to_dicts()


def task_api_version_deprecation_readiness_plan_to_markdown(plan: SimpleReadinessPlan) -> str:
    """Render API version deprecation readiness as Markdown."""
    return plan.to_markdown()


__all__ = [
    "SimpleReadinessPlan",
    "analyze_task_api_version_deprecation_readiness",
    "build_task_api_version_deprecation_readiness_plan",
    "extract_task_api_version_deprecation_readiness",
    "generate_task_api_version_deprecation_readiness",
    "recommend_task_api_version_deprecation_readiness",
    "summarize_task_api_version_deprecation_readiness",
    "task_api_version_deprecation_readiness_plan_to_dict",
    "task_api_version_deprecation_readiness_plan_to_dicts",
    "task_api_version_deprecation_readiness_plan_to_markdown",
]
