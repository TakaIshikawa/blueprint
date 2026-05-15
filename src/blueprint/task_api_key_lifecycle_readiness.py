"""Assess readiness for API key lifecycle tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import (
    SimpleReadinessPlan,
    SimpleReadinessRecord,
    build_simple_readiness_plan,
)


TaskAPIKeyLifecycleReadinessPlan = SimpleReadinessPlan
TaskAPIKeyLifecycleReadinessRecord = SimpleReadinessRecord
TaskAPIKeyLifecycleReadinessFinding = SimpleReadinessRecord
TaskAPIKeyLifecycleReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "api_key_lifecycle": re.compile(
        r"\b(?:api key lifecycle|api keys? lifecycle|key lifecycle|credential lifecycle|"
        r"access key lifecycle)\b",
        re.I,
    ),
    "key_creation": re.compile(
        r"\b(?:create|creation|issue|issuance|generate|provision|mint)\b.{0,50}\b(?:api keys?|access keys?|client keys?|credentials?)\b|"
        r"\b(?:api keys?|access keys?|client keys?|credentials?)\b.{0,50}\b(?:create|creation|issue|issuance|generate|provision|mint)\b",
        re.I,
    ),
    "key_rotation": re.compile(
        r"\b(?:api key rotation|rotate api keys?|key rotation|credential rotation|secret rotation|rollover api keys?|rotation|rotating)\b",
        re.I,
    ),
    "key_revocation": re.compile(
        r"\b(?:api key revocation|revoke api keys?|key revocation|credential revocation|disable api keys?|deactivate api keys?|revocation|revoked)\b",
        re.I,
    ),
    "key_expiration": re.compile(
        r"\b(?:api key expiration|key expiration|expires?|expiry|ttl|time[- ]to[- ]live|expiration policy|expiration|expired)\b",
        re.I,
    ),
    "key_scopes": re.compile(
        r"\b(?:api key scopes?|key scopes?|credential scopes?|scoped keys?|permissions?|permission scopes?|least privilege)\b",
        re.I,
    ),
    "key_storage": re.compile(
        r"\b(?:api key storage|credential storage|secure storage|hashed keys?|encrypted keys?|vault|secret store|secrets manager|storage)\b",
        re.I,
    ),
    "key_audit": re.compile(
        r"\b(?:api key audit|key audit|credential audit|audit events?|audit logs?|usage audit|access audit)\b",
        re.I,
    ),
    "client_migration": re.compile(
        r"\b(?:client migration|consumer migration|migrate clients?|client cutover|consumer cutover)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "api_key_lifecycle": re.compile(r"(?:api[_\s-]?key|access[_\s-]?key|credential)[_\s-]?lifecycle", re.I),
    "key_creation": re.compile(r"(?:api[_\s-]?key|access[_\s-]?key|credential).*(?:create|issue|provision|generate)", re.I),
    "key_rotation": re.compile(r"(?:api[_\s-]?key|access[_\s-]?key|credential|key).*(?:rotation|rotate|rollover)", re.I),
    "key_revocation": re.compile(r"(?:api[_\s-]?key|access[_\s-]?key|credential|key).*(?:revocation|revoke|disable|deactivate)", re.I),
    "key_expiration": re.compile(r"(?:api[_\s-]?key|access[_\s-]?key|credential|key).*(?:expiration|expiry|expires|ttl)", re.I),
    "key_scopes": re.compile(r"(?:api[_\s-]?key|access[_\s-]?key|credential|key).*(?:scopes?|permissions?)", re.I),
    "key_storage": re.compile(r"(?:api[_\s-]?key|access[_\s-]?key|credential|key).*(?:storage|store|vault|secrets?)", re.I),
    "key_audit": re.compile(r"(?:api[_\s-]?key|access[_\s-]?key|credential|key).*(?:audit|events?|logs?)", re.I),
    "client_migration": re.compile(r"(?:client|consumer).*(?:migration|cutover)|dual[_\s-]?key|overlap", re.I),
}
_CRITERIA = {
    "lifecycle_state_model": re.compile(
        r"\b(?:lifecycle state model|state model|states?|active|pending|created|issued|rotating|revoked|disabled|expired|"
        r"deactivated|status transitions?|state transitions?)\b",
        re.I,
    ),
    "secure_storage": re.compile(
        r"\b(?:secure storage|credential storage|secret store|secrets manager|vault|kms|encrypted|hashed|hashing|"
        r"token digest|at rest|never store plaintext|no plaintext)\b",
        re.I,
    ),
    "scope_permission_handling": re.compile(
        r"\b(?:scopes?|permissions?|permission handling|scope handling|least privilege|rbac|access policy|entitlements?)\b",
        re.I,
    ),
    "rotation_revocation_path": re.compile(
        r"\b(?:rotation path|revocation path|rotate|rotation|revoke|revocation|disable|deactivate|rollover|dual key|"
        r"overlap window|old and new keys?|cutover)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit logging|audit logs?|audit events?|event log|security events?|usage logs?|access logs?|"
        r"creation event|rotation event|revocation event)\b",
        re.I,
    ),
    "customer_communication": re.compile(
        r"\b(?:customer communication|customer notice|notify customers?|consumer notice|developer notice|migration guide|"
        r"client notice|deprecation notice|release notes?|email notice|docs update)\b",
        re.I,
    ),
    "validation_coverage": re.compile(
        r"\b(?:validation coverage|validation|validate|verify|unit tests?|integration tests?|contract tests?|"
        r"pytest|smoke tests?|regression tests?|acceptance checks?)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "lifecycle_state_model": "Define the lifecycle state model, including created, active, rotating, revoked, disabled, expired, or equivalent state transitions.",
    "secure_storage": "Document secure storage for API keys with hashing, encryption, a vault, secrets manager, KMS, or no plaintext storage.",
    "scope_permission_handling": "Add scope or permission handling with least privilege, RBAC, access policies, entitlements, or scoped keys.",
    "rotation_revocation_path": "Describe the rotation or revocation path, including rollover, disable, deactivate, overlap, cutover, or dual-key behavior.",
    "audit_logging": "Add audit logging for API key creation, rotation, revocation, expiration, usage, or access events.",
    "customer_communication": "Plan customer communication for affected clients, consumers, developers, migration guides, notices, release notes, or documentation updates.",
    "validation_coverage": "Add validation coverage with unit, integration, contract, smoke, regression, pytest, or acceptance checks.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:api keys?|access keys?|credentials?|key lifecycle|key rotation|key revocation)\b"
    r".{0,80}\b(?:required|needed|planned|scope|impact|changes?)\b",
    re.I,
)


def build_task_api_key_lifecycle_readiness_plan(source: Any) -> TaskAPIKeyLifecycleReadinessPlan:
    """Build API key lifecycle readiness records for task-shaped input."""
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task API Key Lifecycle Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_api_key_lifecycle_readiness = build_task_api_key_lifecycle_readiness_plan
extract_task_api_key_lifecycle_readiness = build_task_api_key_lifecycle_readiness_plan
generate_task_api_key_lifecycle_readiness = build_task_api_key_lifecycle_readiness_plan
derive_task_api_key_lifecycle_readiness = build_task_api_key_lifecycle_readiness_plan
summarize_task_api_key_lifecycle_readiness = build_task_api_key_lifecycle_readiness_plan
summarize_task_api_key_lifecycle_readiness_plan = build_task_api_key_lifecycle_readiness_plan


def recommend_task_api_key_lifecycle_readiness(source: Any) -> tuple[TaskAPIKeyLifecycleReadinessRecord, ...]:
    return build_task_api_key_lifecycle_readiness_plan(source).records


def task_api_key_lifecycle_readiness_plan_to_dict(result: TaskAPIKeyLifecycleReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_api_key_lifecycle_readiness_plan_to_dict.__test__ = False


def task_api_key_lifecycle_readiness_plan_to_dicts(
    result: TaskAPIKeyLifecycleReadinessPlan | Iterable[TaskAPIKeyLifecycleReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_api_key_lifecycle_readiness_plan_to_dicts.__test__ = False
task_api_key_lifecycle_readiness_to_dicts = task_api_key_lifecycle_readiness_plan_to_dicts
task_api_key_lifecycle_readiness_to_dicts.__test__ = False


def task_api_key_lifecycle_readiness_plan_to_markdown(result: TaskAPIKeyLifecycleReadinessPlan) -> str:
    return result.to_markdown()


task_api_key_lifecycle_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskAPIKeyLifecycleReadinessFinding",
    "TaskAPIKeyLifecycleReadinessPlan",
    "TaskAPIKeyLifecycleReadinessRecord",
    "TaskAPIKeyLifecycleReadinessRecommendation",
    "analyze_task_api_key_lifecycle_readiness",
    "build_task_api_key_lifecycle_readiness_plan",
    "derive_task_api_key_lifecycle_readiness",
    "extract_task_api_key_lifecycle_readiness",
    "generate_task_api_key_lifecycle_readiness",
    "recommend_task_api_key_lifecycle_readiness",
    "summarize_task_api_key_lifecycle_readiness",
    "summarize_task_api_key_lifecycle_readiness_plan",
    "task_api_key_lifecycle_readiness_plan_to_dict",
    "task_api_key_lifecycle_readiness_plan_to_dicts",
    "task_api_key_lifecycle_readiness_plan_to_markdown",
    "task_api_key_lifecycle_readiness_to_dicts",
]
