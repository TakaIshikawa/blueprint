"""Assess readiness for API version sunset execution tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskApiVersionSunsetReadinessPlan = SimpleReadinessPlan
TaskApiVersionSunsetReadinessRecord = SimpleReadinessRecord
TaskApiVersionSunsetReadinessFinding = SimpleReadinessRecord
TaskApiVersionSunsetReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "api_version_sunset": re.compile(r"\b(?:api|rest|graphql|grpc|sdk).{0,80}\b(?:version|v\d+).{0,80}\b(?:sunset|deprecat|retire|eol|end of life)|\b(?:sunset|deprecat|retire|eol).{0,80}\b(?:api|version|v\d+)\b", re.I),
    "versioned_endpoint": re.compile(r"\b(?:v\d+|version \d+|versioned endpoint|legacy api version|old api version)\b", re.I),
    "sunset_execution": re.compile(r"\b(?:sunset plan|sunset execution|version sunset|deprecation execution|retirement plan|remove v\d+)\b", re.I),
}
_PATH_SIGNALS = {
    "api_version_sunset": re.compile(r"(?:api|rest|graphql|grpc|sdk|openapi).*(?:v\d+|version).*(?:sunset|deprecat|retire|eol)|(?:sunset|deprecat|retire|eol).*(?:api|v\d+|version)", re.I),
    "versioned_endpoint": re.compile(r"(?:^|/)(?:v\d+|version[_-]?\d+)(?:/|_|-|\\.|$)|openapi|routes?|sdk|schema", re.I),
    "sunset_execution": re.compile(r"sunset|deprecat|retire|eol|migration", re.I),
}
_CRITERIA = {
    "migration_guidance": re.compile(r"\b(?:migration guidance|migration guide|upgrade guide|replacement endpoint|replacement path|field mapping|code sample|sdk guide|migrate to)\b", re.I),
    "customer_communication": re.compile(r"\b(?:customer communication|client communication|developer notice|partner notice|changelog|release notes?|email notice|announcement|communication plan)\b", re.I),
    "sunset_timeline": re.compile(r"\b(?:sunset timeline|sunset date|deprecation date|migration deadline|removal date|eol|end of life|notice period|grace period)\b", re.I),
    "compatibility_tests": re.compile(r"\b(?:compatibility tests?|contract tests?|backward compatible|backwards compatible|legacy mode test|dual support test|regression test)\b", re.I),
    "metrics_usage_tracking": re.compile(r"\b(?:metrics?|telemetry|usage tracking|call volume|remaining traffic|dashboard|alerts?|logs?|client adoption)\b", re.I),
    "rollback_extension_criteria": re.compile(r"\b(?:rollback|roll back|extension criteria|extend sunset|pause removal|rollback criteria|kill switch|fallback|revert)\b", re.I),
    "documentation_updates": re.compile(r"\b(?:documentation updates?|docs update|api docs|openapi docs|developer docs|sdk docs|reference docs|runbook)\b", re.I),
}
_GUIDANCE = {
    "migration_guidance": "Publish migration guidance with replacement endpoints, mappings, SDK notes, or examples.",
    "customer_communication": "Prepare customer, client, developer, partner, changelog, release-note, or email communication.",
    "sunset_timeline": "Set the sunset timeline, deadline, notice period, grace period, EOL, or removal date.",
    "compatibility_tests": "Add compatibility, contract, regression, dual-support, or legacy-mode tests.",
    "metrics_usage_tracking": "Track metrics, usage, remaining traffic, adoption, dashboards, alerts, or logs.",
    "rollback_extension_criteria": "Define rollback, fallback, extension, pause, or revert criteria before removal.",
    "documentation_updates": "Update API, OpenAPI, SDK, developer, reference, or runbook documentation.",
}
_NO_IMPACT = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:api|version|v\d+|endpoint)\b.{0,80}\b(?:sunset|deprecat|retire|eol|removal|impact|changes?)\b", re.I)


def build_task_api_version_sunset_readiness_plan(source: Any) -> TaskApiVersionSunsetReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task API Version Sunset Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_api_version_sunset_readiness = build_task_api_version_sunset_readiness_plan
extract_task_api_version_sunset_readiness = build_task_api_version_sunset_readiness_plan
generate_task_api_version_sunset_readiness = build_task_api_version_sunset_readiness_plan
derive_task_api_version_sunset_readiness = build_task_api_version_sunset_readiness_plan
summarize_task_api_version_sunset_readiness = build_task_api_version_sunset_readiness_plan
summarize_task_api_version_sunset_readiness_plan = build_task_api_version_sunset_readiness_plan


def recommend_task_api_version_sunset_readiness(source: Any) -> tuple[TaskApiVersionSunsetReadinessRecord, ...]:
    return build_task_api_version_sunset_readiness_plan(source).records


def task_api_version_sunset_readiness_plan_to_dict(plan: TaskApiVersionSunsetReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


task_api_version_sunset_readiness_plan_to_dict.__test__ = False


def task_api_version_sunset_readiness_plan_to_dicts(
    plan: TaskApiVersionSunsetReadinessPlan | Iterable[TaskApiVersionSunsetReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(plan, SimpleReadinessPlan):
        return plan.to_dicts()
    return [record.to_dict() for record in plan]


task_api_version_sunset_readiness_plan_to_dicts.__test__ = False
task_api_version_sunset_readiness_to_dicts = task_api_version_sunset_readiness_plan_to_dicts
task_api_version_sunset_readiness_to_dicts.__test__ = False


def task_api_version_sunset_readiness_plan_to_markdown(plan: TaskApiVersionSunsetReadinessPlan) -> str:
    return plan.to_markdown()


task_api_version_sunset_readiness_plan_to_markdown.__test__ = False

