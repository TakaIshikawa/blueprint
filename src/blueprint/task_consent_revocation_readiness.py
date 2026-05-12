"""Analyze consent revocation readiness for execution-plan tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, build_simple_readiness_plan


_SIGNAL_PATTERNS = {
    "revocation": re.compile(r"\b(?:consent[_\s-]+revocation|revoke[_\s-]+consent|withdraw(?:al)?[_\s-]+consent|consent[_\s-]+withdrawal|opt[_\s-]?out|unsubscribe|preference[_\s-]+withdrawal)\b", re.I),
    "downstream_propagation": re.compile(r"\b(?:downstream|propagat(?:e|ion)|fan[_\s-]?out|sync[_\s-]+revocation|processors?|partners?)\b.{0,80}\b(?:revocation|withdrawal|opt[_\s-]?out|unsubscribe)\b", re.I),
}
_PATH_SIGNAL_PATTERNS = {
    "consent_revocation_path": re.compile(r"\b(?:consent|privacy|preferences?|marketing|subscriptions?).*(?:revocation|withdrawal|opt[_\s-]*out|unsubscribe)\b|\b(?:revocation|withdrawal|opt[_\s-]*out|unsubscribe).*(?:consent|privacy|preferences?|marketing|subscriptions?)\b", re.I),
}
_CRITERIA_PATTERNS = {
    "revocation_trigger": re.compile(r"\b(?:revocation[_\s-]+trigger|withdrawal[_\s-]+trigger|opt[_\s-]?out[_\s-]+trigger|unsubscribe[_\s-]+link|user[_\s-]+action|api[_\s-]+request|preference[_\s-]+change)\b", re.I),
    "affected_data_actions": re.compile(r"\b(?:affected[_\s-]+data|affected[_\s-]+actions|processing[_\s-]+stops?|suppression|stop[_\s-]+email|stop[_\s-]+processing|consent[_\s-]+scope|purpose)\b", re.I),
    "propagation_targets": re.compile(r"\b(?:propagation[_\s-]+targets?|downstream[_\s-]+systems?|processors?|partners?|crm|esp|warehouse|webhooks?|queues?)\b", re.I),
    "user_confirmation": re.compile(r"\b(?:confirmation|receipt|success[_\s-]+message|user[_\s-]+notice|email[_\s-]+confirmation|preference[_\s-]+center[_\s-]+copy)\b", re.I),
    "audit_trail": re.compile(r"\b(?:audit[_\s-]+trail|audit[_\s-]+log|evidence|record[_\s-]+actor|timestamp|consent[_\s-]+history)\b", re.I),
    "retries_failure_handling": re.compile(r"\b(?:retry|retries|backoff|dead[_\s-]+letter|dlq|failure[_\s-]+handling|poison|replay|error[_\s-]+queue)\b", re.I),
    "privacy_copy": re.compile(r"\b(?:privacy[_\s-]+copy|legal[_\s-]+copy|user[_\s-]+facing[_\s-]+copy|policy[_\s-]+copy|gdpr|ccpa|withdrawal[_\s-]+language)\b", re.I),
    "tests": re.compile(r"\b(?:tests?|unit[_\s-]+tests?|integration[_\s-]+tests?|e2e|fixture|coverage|verification)\b", re.I),
}
_GUIDANCE = {
    "revocation_trigger": "Define the user, API, unsubscribe, or preference-center trigger that starts revocation.",
    "affected_data_actions": "List affected consent scopes, data processing, notifications, and actions to stop.",
    "propagation_targets": "Name downstream systems, processors, partners, queues, or webhooks receiving revocation.",
    "user_confirmation": "Add user-facing confirmation or receipt behavior after revocation completes.",
    "audit_trail": "Record audit history with actor, source, timestamp, scope, and outcome.",
    "retries_failure_handling": "Specify retries, backoff, replay, DLQ, and failure handling for propagation.",
    "privacy_copy": "Include privacy-reviewed copy explaining withdrawal and opt-out effects.",
    "tests": "Cover trigger, propagation, confirmation, auditability, failures, and privacy copy in tests.",
}


def build_task_consent_revocation_readiness_plan(source: Any) -> SimpleReadinessPlan:
    """Build consent revocation readiness records from task-shaped input."""
    return build_simple_readiness_plan(
        source,
        title="Task Consent Revocation Readiness",
        signal_patterns=_SIGNAL_PATTERNS,
        path_signal_patterns=_PATH_SIGNAL_PATTERNS,
        criteria_patterns=_CRITERIA_PATTERNS,
        criterion_guidance=_GUIDANCE,
    )


analyze_task_consent_revocation_readiness = build_task_consent_revocation_readiness_plan
summarize_task_consent_revocation_readiness = build_task_consent_revocation_readiness_plan
generate_task_consent_revocation_readiness = build_task_consent_revocation_readiness_plan
extract_task_consent_revocation_readiness = build_task_consent_revocation_readiness_plan
recommend_task_consent_revocation_readiness = build_task_consent_revocation_readiness_plan


def task_consent_revocation_readiness_plan_to_dict(plan: SimpleReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


def task_consent_revocation_readiness_plan_to_dicts(plan: SimpleReadinessPlan) -> list[dict[str, Any]]:
    return plan.to_dicts()


def task_consent_revocation_readiness_plan_to_markdown(plan: SimpleReadinessPlan) -> str:
    return plan.to_markdown()


__all__ = [
    "SimpleReadinessPlan",
    "analyze_task_consent_revocation_readiness",
    "build_task_consent_revocation_readiness_plan",
    "extract_task_consent_revocation_readiness",
    "generate_task_consent_revocation_readiness",
    "recommend_task_consent_revocation_readiness",
    "summarize_task_consent_revocation_readiness",
    "task_consent_revocation_readiness_plan_to_dict",
    "task_consent_revocation_readiness_plan_to_dicts",
    "task_consent_revocation_readiness_plan_to_markdown",
]
