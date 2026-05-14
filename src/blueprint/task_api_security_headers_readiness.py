"""Assess readiness for API security header implementation tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import (
    SimpleReadinessPlan,
    SimpleReadinessRecord,
    build_simple_readiness_plan,
)


TaskAPISecurityHeadersReadinessPlan = SimpleReadinessPlan
TaskAPISecurityHeadersReadinessRecord = SimpleReadinessRecord
TaskAPISecurityHeadersReadinessFinding = SimpleReadinessRecord
TaskAPISecurityHeadersReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "security_headers": re.compile(
        r"\b(?:api security headers?|http security headers?|response hardening|"
        r"secure response headers?|security response headers?)\b",
        re.I,
    ),
    "hsts_csp": re.compile(
        r"\b(?:hsts|strict[- ]transport[- ]security|csp|content[- ]security[- ]policy)\b",
        re.I,
    ),
    "browser_hardening_headers": re.compile(
        r"\b(?:x[- ]frame[- ]options|x[- ]content[- ]type[- ]options|nosniff|"
        r"referrer[- ]policy|permissions[- ]policy|frame ancestors)\b",
        re.I,
    ),
    "cors_response_hardening": re.compile(
        r"\b(?:cors hardening|cors response hardening|cross[- ]origin response hardening|"
        r"access[- ]control headers?|origin allowlist|allowed origins?)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "security_headers": re.compile(r"(?:security[_-]?headers?|response[_-]?hardening|secure[_-]?headers?)", re.I),
    "hsts_csp": re.compile(r"(?:hsts|csp|content[_-]?security[_-]?policy)", re.I),
    "browser_hardening_headers": re.compile(
        r"(?:x[_-]?frame|x[_-]?content[_-]?type|referrer[_-]?policy|permissions[_-]?policy|nosniff)",
        re.I,
    ),
    "cors_response_hardening": re.compile(r"(?:cors|cross[_-]?origin|allowed[_-]?origins?)", re.I),
}
_CRITERIA = {
    "explicit_header_set": re.compile(
        r"\b(?:explicit header set|header set|headers? list|hsts|strict[- ]transport[- ]security|"
        r"content[- ]security[- ]policy|csp|x[- ]frame[- ]options|x[- ]content[- ]type[- ]options|"
        r"nosniff|referrer[- ]policy|permissions[- ]policy)\b",
        re.I,
    ),
    "environment_rollout": re.compile(
        r"\b(?:environment rollout|rollout|staged rollout|staging|production rollout|"
        r"canary|feature flag|environment flag|dev|development|sandbox)\b",
        re.I,
    ),
    "compatibility_testing": re.compile(
        r"\b(?:compatibility test|compatibility testing|browser compatibility|client compatibility|"
        r"legacy clients?|mobile clients?|api consumers?|integration compatibility)\b",
        re.I,
    ),
    "regression_coverage": re.compile(
        r"\b(?:regression coverage|regression tests?|unit tests?|integration tests?|contract tests?|"
        r"pytest|header assertions?|security header tests?)\b",
        re.I,
    ),
    "owner": re.compile(
        r"\b(?:owner|owned by|dri|responsible team|maintainer|security team|platform team|"
        r"approver|accountable)\b",
        re.I,
    ),
    "monitoring_rollback_guidance": re.compile(
        r"\b(?:monitoring|monitor|alerts?|observability|rollback|roll back|disable|revert|"
        r"runbook|incident response|violation reports?)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "explicit_header_set": "Define the explicit security header set, including required HSTS, CSP, X-Frame-Options, X-Content-Type-Options, Referrer-Policy, Permissions-Policy, or CORS-adjacent hardening headers.",
    "environment_rollout": "Document rollout by environment with staging, canary, feature flag, or production rollout controls.",
    "compatibility_testing": "Add compatibility testing for browsers, API consumers, legacy clients, mobile clients, or integrations.",
    "regression_coverage": "Add regression coverage with unit, integration, contract, pytest, or header assertion tests.",
    "owner": "Name the owner, DRI, responsible team, maintainer, security team, platform team, or approver.",
    "monitoring_rollback_guidance": "Add monitoring, alerting, observability, rollback, disable, revert, or runbook guidance.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:security headers?|hsts|csp|response hardening|cors)\b"
    r".{0,80}\b(?:required|needed|planned|scope|impact|changes?)\b",
    re.I,
)


def build_task_api_security_headers_readiness_plan(source: Any) -> TaskAPISecurityHeadersReadinessPlan:
    """Build API security headers readiness records for task-shaped input."""
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task API Security Headers Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_api_security_headers_readiness = build_task_api_security_headers_readiness_plan
extract_task_api_security_headers_readiness = build_task_api_security_headers_readiness_plan
generate_task_api_security_headers_readiness = build_task_api_security_headers_readiness_plan
derive_task_api_security_headers_readiness = build_task_api_security_headers_readiness_plan
summarize_task_api_security_headers_readiness = build_task_api_security_headers_readiness_plan
summarize_task_api_security_headers_readiness_plan = build_task_api_security_headers_readiness_plan


def recommend_task_api_security_headers_readiness(source: Any) -> tuple[TaskAPISecurityHeadersReadinessRecord, ...]:
    return build_task_api_security_headers_readiness_plan(source).records


def task_api_security_headers_readiness_plan_to_dict(result: TaskAPISecurityHeadersReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_api_security_headers_readiness_plan_to_dict.__test__ = False


def task_api_security_headers_readiness_plan_to_dicts(
    result: TaskAPISecurityHeadersReadinessPlan | Iterable[TaskAPISecurityHeadersReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_api_security_headers_readiness_plan_to_dicts.__test__ = False
task_api_security_headers_readiness_to_dicts = task_api_security_headers_readiness_plan_to_dicts
task_api_security_headers_readiness_to_dicts.__test__ = False


def task_api_security_headers_readiness_plan_to_markdown(result: TaskAPISecurityHeadersReadinessPlan) -> str:
    return result.to_markdown()


task_api_security_headers_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskAPISecurityHeadersReadinessFinding",
    "TaskAPISecurityHeadersReadinessPlan",
    "TaskAPISecurityHeadersReadinessRecord",
    "TaskAPISecurityHeadersReadinessRecommendation",
    "analyze_task_api_security_headers_readiness",
    "build_task_api_security_headers_readiness_plan",
    "derive_task_api_security_headers_readiness",
    "extract_task_api_security_headers_readiness",
    "generate_task_api_security_headers_readiness",
    "recommend_task_api_security_headers_readiness",
    "summarize_task_api_security_headers_readiness",
    "summarize_task_api_security_headers_readiness_plan",
    "task_api_security_headers_readiness_plan_to_dict",
    "task_api_security_headers_readiness_plan_to_dicts",
    "task_api_security_headers_readiness_plan_to_markdown",
    "task_api_security_headers_readiness_to_dicts",
]
