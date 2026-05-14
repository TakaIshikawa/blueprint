"""Assess readiness for task API endpoint or field deprecation work."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import (
    SimpleReadinessPlan,
    SimpleReadinessRecord,
    build_simple_readiness_plan,
)


TaskApiDeprecationReadinessPlan = SimpleReadinessPlan
TaskApiDeprecationReadinessRecord = SimpleReadinessRecord
TaskApiDeprecationReadinessFinding = SimpleReadinessRecord
TaskApiDeprecationReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "api_deprecation": re.compile(
        r"\b(?:api|rest|graphql|grpc|sdk|schema|contract).{0,80}\b(?:deprecat|sunset|retire|remove|eol)\b|"
        r"\b(?:deprecat|sunset|retire|remove).{0,80}\b(?:api|endpoint|route|field|parameter|sdk|schema|contract)\b",
        re.I,
    ),
    "deprecated_endpoint": re.compile(
        r"\b(?:deprecated endpoint|endpoint deprecation|endpoint sunset|route deprecation|"
        r"route sunset|legacy endpoint|legacy route|remove endpoint|retire endpoint)\b",
        re.I,
    ),
    "deprecated_field": re.compile(
        r"\b(?:deprecated field|field deprecation|field sunset|remove field|retire field|"
        r"deprecated parameter|parameter deprecation|remove parameter|schema field removal)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "api_deprecation": re.compile(
        r"(?:api|rest|graphql|grpc|openapi|sdk|schema|contract).*(?:deprecat|sunset|retire|remov|eol)|"
        r"(?:deprecat|sunset|retire|remov|eol).*(?:api|rest|graphql|grpc|openapi|sdk|schema|contract)",
        re.I,
    ),
    "deprecated_endpoint": re.compile(r"(?:endpoint|route|routes|resource|operation).*(?:deprecat|sunset|retire|remove|legacy)", re.I),
    "deprecated_field": re.compile(r"(?:field|parameter|param|property|schema).*(?:deprecat|sunset|remov|legacy)", re.I),
}
_CRITERIA = {
    "replacement_path": re.compile(
        r"\b(?:replacement path|replacement endpoint|replacement field|replacement route|"
        r"successor endpoint|successor field|migrate to|use v\d+|field mapping|route mapping|"
        r"migration guide|upgrade guide)\b",
        re.I,
    ),
    "client_communication": re.compile(
        r"\b(?:client communication|customer communication|developer notice|partner notice|"
        r"changelog|release notes?|email notice|announcement|migration notice|sdk notice)\b",
        re.I,
    ),
    "sunset_timeline": re.compile(
        r"\b(?:sunset timeline|sunset date|deprecation timeline|deprecation date|"
        r"removal date|deadline|eol|end of life|grace period ends?|notice period)\b",
        re.I,
    ),
    "compatibility_behavior": re.compile(
        r"\b(?:compatibility behavior|backward compatible|backwards compatible|compatibility fallback|"
        r"dual support|legacy mode|shim|fallback|grace period|warning header|deprecation header)\b",
        re.I,
    ),
    "telemetry": re.compile(
        r"\b(?:telemetry|metrics?|dashboard|monitor(?:ing)?|alerts?|logs?|usage tracking|"
        r"remaining traffic|client adoption|call volume|error rate)\b",
        re.I,
    ),
    "removal_gate": re.compile(
        r"\b(?:removal gate|removal criteria|sunset gate|deprecation gate|delete gate|"
        r"zero traffic|migration complete|adoption threshold|final removal|remove after)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "replacement_path": "Document the replacement endpoint, field, route, SDK, or migration mapping clients should use.",
    "client_communication": "Prepare client, customer, developer, partner, changelog, release-note, or email communication.",
    "sunset_timeline": "Set the sunset, deprecation, EOL, deadline, notice period, or final removal timeline.",
    "compatibility_behavior": "Define compatibility behavior such as dual support, legacy mode, shim, fallback, or warning headers.",
    "telemetry": "Track telemetry for usage, remaining traffic, client adoption, errors, dashboards, logs, or alerts.",
    "removal_gate": "Define the removal gate such as zero traffic, migration complete, adoption threshold, or final approval.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:api|endpoint|route|field|sdk|schema)\b"
    r".{0,80}\b(?:deprecation|sunset|retirement|removal|eol|changes?|impact)\b",
    re.I,
)


def build_task_api_deprecation_readiness_plan(source: Any) -> TaskApiDeprecationReadinessPlan:
    """Build API deprecation readiness records for task-shaped input."""
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task API Deprecation Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_api_deprecation_readiness(source: Any) -> TaskApiDeprecationReadinessPlan:
    return build_task_api_deprecation_readiness_plan(source)


def extract_task_api_deprecation_readiness(source: Any) -> TaskApiDeprecationReadinessPlan:
    return build_task_api_deprecation_readiness_plan(source)


def generate_task_api_deprecation_readiness(source: Any) -> TaskApiDeprecationReadinessPlan:
    return build_task_api_deprecation_readiness_plan(source)


def derive_task_api_deprecation_readiness(source: Any) -> TaskApiDeprecationReadinessPlan:
    return build_task_api_deprecation_readiness_plan(source)


def summarize_task_api_deprecation_readiness(source: Any) -> TaskApiDeprecationReadinessPlan:
    return build_task_api_deprecation_readiness_plan(source)


def summarize_task_api_deprecation_readiness_plan(source: Any) -> TaskApiDeprecationReadinessPlan:
    return build_task_api_deprecation_readiness_plan(source)


def recommend_task_api_deprecation_readiness(source: Any) -> tuple[TaskApiDeprecationReadinessRecord, ...]:
    return build_task_api_deprecation_readiness_plan(source).records


def task_api_deprecation_readiness_plan_to_dict(result: TaskApiDeprecationReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_api_deprecation_readiness_plan_to_dict.__test__ = False


def task_api_deprecation_readiness_plan_to_dicts(
    result: TaskApiDeprecationReadinessPlan | Iterable[TaskApiDeprecationReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_api_deprecation_readiness_plan_to_dicts.__test__ = False
task_api_deprecation_readiness_to_dicts = task_api_deprecation_readiness_plan_to_dicts
task_api_deprecation_readiness_to_dicts.__test__ = False


def task_api_deprecation_readiness_plan_to_markdown(result: TaskApiDeprecationReadinessPlan) -> str:
    return result.to_markdown()


task_api_deprecation_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskApiDeprecationReadinessFinding",
    "TaskApiDeprecationReadinessPlan",
    "TaskApiDeprecationReadinessRecord",
    "TaskApiDeprecationReadinessRecommendation",
    "analyze_task_api_deprecation_readiness",
    "build_task_api_deprecation_readiness_plan",
    "derive_task_api_deprecation_readiness",
    "extract_task_api_deprecation_readiness",
    "generate_task_api_deprecation_readiness",
    "recommend_task_api_deprecation_readiness",
    "summarize_task_api_deprecation_readiness",
    "summarize_task_api_deprecation_readiness_plan",
    "task_api_deprecation_readiness_plan_to_dict",
    "task_api_deprecation_readiness_plan_to_dicts",
    "task_api_deprecation_readiness_plan_to_markdown",
    "task_api_deprecation_readiness_to_dicts",
]
