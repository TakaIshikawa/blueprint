"""Assess readiness for session timeout and idle-expiration change tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskSessionTimeoutReadinessPlan = SimpleReadinessPlan
TaskSessionTimeoutReadinessRecord = SimpleReadinessRecord
TaskSessionTimeoutReadinessFinding = SimpleReadinessRecord
TaskSessionTimeoutReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "session_timeout": re.compile(
        r"\b(?:session timeout|session ttl|session time[- ]?to[- ]?live|session lifetime|"
        r"session duration|max session age|session max age)\b",
        re.I,
    ),
    "idle_timeout": re.compile(
        r"\b(?:idle timeout|idle expiration|idle expiry|inactivity timeout|inactive session|"
        r"expire inactive sessions?|idle session)\b",
        re.I,
    ),
    "absolute_timeout": re.compile(
        r"\b(?:absolute timeout|absolute session|hard timeout|maximum session|max session|"
        r"reauth(?:entication)? after|force reauth(?:entication)?)\b",
        re.I,
    ),
    "session_expiration": re.compile(
        r"\b(?:session expir(?:e|es|ed|y|ation)|session invalidates? after|auth session expir(?:e|es|y|ation)|"
        r"login session expir(?:e|es|y|ation))\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "session_timeout": re.compile(
        r"(?:auth|sessions?|session)[/_-].*(?:timeout|ttl|lifetime|duration|max[_-]?age)|"
        r"(?:timeout|ttl|lifetime)[/_-].*(?:auth|sessions?)",
        re.I,
    ),
    "idle_timeout": re.compile(r"(?:idle|inactive|inactivity)[_-]?(?:timeout|expiry|expiration|session)", re.I),
    "absolute_timeout": re.compile(r"(?:absolute|hard|max)[_-]?(?:timeout|session|age|lifetime)", re.I),
    "session_expiration": re.compile(r"(?:session|auth)[/_-].*(?:expir|ttl)|(?:expir|ttl)[/_-].*(?:session|auth)", re.I),
}
_CRITERIA = {
    "idle_timeout_policy": re.compile(
        r"\b(?:idle timeout|idle expiration|idle expiry|inactivity timeout|inactive session).{0,120}"
        r"(?:policy|duration|minutes?|hours?|threshold|config|setting|grace period)|"
        r"(?:policy|duration|minutes?|hours?|threshold|config|setting|grace period).{0,120}"
        r"(?:idle timeout|idle expiration|inactivity timeout)\b",
        re.I,
    ),
    "absolute_timeout_policy": re.compile(
        r"\b(?:absolute timeout|absolute session|hard timeout|max(?:imum)? session age|session lifetime|"
        r"session duration).{0,120}(?:policy|duration|minutes?|hours?|days?|threshold|config|setting)|"
        r"(?:policy|duration|minutes?|hours?|days?|threshold|config|setting).{0,120}"
        r"(?:absolute timeout|absolute session|hard timeout|max(?:imum)? session age|session lifetime)\b",
        re.I,
    ),
    "remember_me_behavior": re.compile(
        r"\b(?:remember[- ]?me|remember me|keep me signed in|stay signed in|persistent login|"
        r"persistent session|trusted device).{0,120}(?:timeout|expiration|expiry|duration|policy|override|disabled|separate)\b",
        re.I,
    ),
    "renewal_behavior": re.compile(
        r"\b(?:session renewal|renew session|refresh session|sliding session|sliding expiration|"
        r"extend session|refresh token|token renewal|reauth(?:entication)? prompt)\b",
        re.I,
    ),
    "user_messaging": re.compile(
        r"\b(?:user warning|warning banner|timeout warning|expiration warning|countdown|toast|modal|"
        r"prompt users?|notify users?|user messaging|grace period)\b",
        re.I,
    ),
    "admin_override": re.compile(
        r"\b(?:admin override|administrator override|admin policy|tenant policy|org policy|"
        r"role-based timeout|exemption|exception policy|configurable by admin)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit logs?|audit events?|security logs?|session history|login history|"
        r"timeout event|expiration event|audit.{0,60}(?:timeout|expiration|expiry)|"
        r"log(?:ged)? timeout|log(?:ged)? expiration)\b",
        re.I,
    ),
    "rollout_rollback": re.compile(
        r"\b(?:rollout|phased rollout|canary|feature flag|gradual ramp|dark launch|rollback|"
        r"roll back|revert|kill switch|disable flag)\b",
        re.I,
    ),
    "support_impact": re.compile(
        r"\b(?:support impact|support runbook|help desk|helpdesk|customer support|support tickets?|"
        r"faq|user education|support escalation)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "idle_timeout_policy": "Define the idle timeout policy, threshold, grace period, and configuration owner.",
    "absolute_timeout_policy": "Define the absolute session timeout policy and maximum session lifetime.",
    "remember_me_behavior": "Document how remember-me or persistent sessions interact with timeout rules.",
    "renewal_behavior": "Specify refresh, renewal, sliding-expiration, or reauthentication behavior.",
    "user_messaging": "Add user warnings or messaging before timeout and at expiration.",
    "admin_override": "Clarify admin, tenant, role, or exception override behavior.",
    "audit_logging": "Emit audit or security events for timeout, expiration, renewal, and override outcomes.",
    "rollout_rollback": "Plan rollout and rollback through a flag, canary, staged ramp, or kill switch.",
    "support_impact": "Prepare support impact notes, help desk guidance, FAQs, or escalation paths.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:session timeout|idle timeout|absolute timeout|session expiration|session expiry)\b"
    r".{0,100}\b(?:scope|impact|changes?|required|needed|planned)\b",
    re.I,
)


def build_task_session_timeout_readiness_plan(source: Any) -> TaskSessionTimeoutReadinessPlan:
    """Build session timeout readiness records for task-shaped input."""
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Session Timeout Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_session_timeout_readiness(source: Any) -> TaskSessionTimeoutReadinessPlan:
    return build_task_session_timeout_readiness_plan(source)


def extract_task_session_timeout_readiness(source: Any) -> TaskSessionTimeoutReadinessPlan:
    return build_task_session_timeout_readiness_plan(source)


def generate_task_session_timeout_readiness(source: Any) -> TaskSessionTimeoutReadinessPlan:
    return build_task_session_timeout_readiness_plan(source)


def derive_task_session_timeout_readiness(source: Any) -> TaskSessionTimeoutReadinessPlan:
    return build_task_session_timeout_readiness_plan(source)


def summarize_task_session_timeout_readiness(source: Any) -> TaskSessionTimeoutReadinessPlan:
    return build_task_session_timeout_readiness_plan(source)


def summarize_task_session_timeout_readiness_plan(source: Any) -> TaskSessionTimeoutReadinessPlan:
    return build_task_session_timeout_readiness_plan(source)


def recommend_task_session_timeout_readiness(source: Any) -> tuple[TaskSessionTimeoutReadinessRecord, ...]:
    return build_task_session_timeout_readiness_plan(source).records


def task_session_timeout_readiness_plan_to_dict(result: TaskSessionTimeoutReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_session_timeout_readiness_plan_to_dict.__test__ = False


def task_session_timeout_readiness_plan_to_dicts(
    result: TaskSessionTimeoutReadinessPlan | Iterable[TaskSessionTimeoutReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_session_timeout_readiness_plan_to_dicts.__test__ = False
task_session_timeout_readiness_to_dicts = task_session_timeout_readiness_plan_to_dicts
task_session_timeout_readiness_to_dicts.__test__ = False


def task_session_timeout_readiness_plan_to_markdown(result: TaskSessionTimeoutReadinessPlan) -> str:
    return result.to_markdown()


task_session_timeout_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskSessionTimeoutReadinessFinding",
    "TaskSessionTimeoutReadinessPlan",
    "TaskSessionTimeoutReadinessRecord",
    "TaskSessionTimeoutReadinessRecommendation",
    "analyze_task_session_timeout_readiness",
    "build_task_session_timeout_readiness_plan",
    "derive_task_session_timeout_readiness",
    "extract_task_session_timeout_readiness",
    "generate_task_session_timeout_readiness",
    "recommend_task_session_timeout_readiness",
    "summarize_task_session_timeout_readiness",
    "summarize_task_session_timeout_readiness_plan",
    "task_session_timeout_readiness_plan_to_dict",
    "task_session_timeout_readiness_plan_to_dicts",
    "task_session_timeout_readiness_plan_to_markdown",
    "task_session_timeout_readiness_to_dicts",
]
