"""Assess readiness for permission, role, RBAC, and ACL migration tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskPermissionMigrationReadinessPlan = SimpleReadinessPlan
TaskPermissionMigrationReadinessRecord = SimpleReadinessRecord
TaskPermissionMigrationReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "rbac_migration": re.compile(
        r"\b(?:rbac|role migration|role model|role hierarchy|roles? and permissions?|"
        r"workspace roles?|admin roles?)\b",
        re.I,
    ),
    "acl_migration": re.compile(
        r"\b(?:acl|access control list|resource permissions?|object permissions?|"
        r"permission migration|migrate permissions?|permission model)\b",
        re.I,
    ),
    "scope_or_entitlement_change": re.compile(
        r"\b(?:oauth scopes?|api scopes?|scoped access|entitlements?|subscription access|"
        r"feature access|license access|policy rewrite|authorization policy)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "rbac_migration": re.compile(r"rbac|roles?|authz|authorization", re.I),
    "acl_migration": re.compile(r"acl|permissions?|access[_-]?control", re.I),
    "scope_or_entitlement_change": re.compile(r"scopes?|entitlements?|polic(?:y|ies)|oauth", re.I),
}
_CRITERIA = {
    "principal_inventory": re.compile(
        r"\b(?:principal inventory|user inventory|group inventory|service account inventory|"
        r"existing principals?|affected users?|affected groups?|account inventory)\b",
        re.I,
    ),
    "permission_mapping": re.compile(
        r"\b(?:permission mapping|role mapping|scope mapping|old-to-new|old to new|"
        r"legacy-to-new|legacy to new|mapping matrix|access matrix)\b",
        re.I,
    ),
    "least_privilege_review": re.compile(
        r"\b(?:least privilege|privilege review|access review|security review|over-grant|"
        r"overgrant|privileged role|approval review)\b",
        re.I,
    ),
    "fallback_access": re.compile(
        r"\b(?:fallback access|break glass|emergency access|rollback access|restore previous access|"
        r"support override|admin override|fallback policy)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit log|audit logs|audit logging|audit event|audit events|audit trail|access change log|"
        r"permission change event|security event)\b",
        re.I,
    ),
    "rollout_validation": re.compile(
        r"\b(?:rollout validation|validation|validate|regression test|permission test|"
        r"authorization test|canary|sampled account|post-rollout check|dry run)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "principal_inventory": "Inventory affected users, groups, service accounts, and other principals before migration.",
    "permission_mapping": "Document old-to-new role, ACL, scope, policy, or entitlement mappings.",
    "least_privilege_review": "Review migrated access for least privilege and privileged-role over-grants.",
    "fallback_access": "Define fallback, break-glass, or restore steps for access failures.",
    "audit_logging": "Emit audit logs or security events for migrated grants, revocations, and policy changes.",
    "rollout_validation": "Validate rollout with tests, canaries, dry runs, or sampled account checks.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:rbac|acl|role|permission|scope|entitlement|access)\b"
    r".{0,80}\b(?:migration|change|impact|required|needed|planned|scope)\b",
    re.I,
)


def build_task_permission_migration_readiness_plan(source: Any) -> TaskPermissionMigrationReadinessPlan:
    """Build permission migration readiness records for task-shaped input."""
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Permission Migration Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def build_task_permission_migration_readiness(source: Any) -> TaskPermissionMigrationReadinessPlan:
    return build_task_permission_migration_readiness_plan(source)


def analyze_task_permission_migration_readiness(source: Any) -> TaskPermissionMigrationReadinessPlan:
    return build_task_permission_migration_readiness_plan(source)


def extract_task_permission_migration_readiness(source: Any) -> TaskPermissionMigrationReadinessPlan:
    return build_task_permission_migration_readiness_plan(source)


def generate_task_permission_migration_readiness(source: Any) -> TaskPermissionMigrationReadinessPlan:
    return build_task_permission_migration_readiness_plan(source)


def derive_task_permission_migration_readiness(source: Any) -> TaskPermissionMigrationReadinessPlan:
    return build_task_permission_migration_readiness_plan(source)


def generate_task_permission_migration_readiness_plan(source: Any) -> TaskPermissionMigrationReadinessPlan:
    return build_task_permission_migration_readiness_plan(source)


def derive_task_permission_migration_readiness_plan(source: Any) -> TaskPermissionMigrationReadinessPlan:
    return build_task_permission_migration_readiness_plan(source)


def summarize_task_permission_migration_readiness(source: Any) -> TaskPermissionMigrationReadinessPlan:
    return build_task_permission_migration_readiness_plan(source)


def recommend_task_permission_migration_readiness(source: Any) -> tuple[TaskPermissionMigrationReadinessRecord, ...]:
    return build_task_permission_migration_readiness_plan(source).records


def extract_task_permission_migration_readiness_records(
    source: Any,
) -> tuple[TaskPermissionMigrationReadinessRecord, ...]:
    return build_task_permission_migration_readiness_plan(source).records


def task_permission_migration_readiness_to_dict(plan: TaskPermissionMigrationReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


task_permission_migration_readiness_to_dict.__test__ = False
task_permission_migration_readiness_plan_to_dict = task_permission_migration_readiness_to_dict
task_permission_migration_readiness_plan_to_dict.__test__ = False


def task_permission_migration_readiness_to_dicts(
    result: TaskPermissionMigrationReadinessPlan | Iterable[TaskPermissionMigrationReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_permission_migration_readiness_to_dicts.__test__ = False
task_permission_migration_readiness_plan_to_dicts = task_permission_migration_readiness_to_dicts
task_permission_migration_readiness_plan_to_dicts.__test__ = False


def task_permission_migration_readiness_to_markdown(plan: TaskPermissionMigrationReadinessPlan) -> str:
    return plan.to_markdown()


task_permission_migration_readiness_to_markdown.__test__ = False
task_permission_migration_readiness_plan_to_markdown = task_permission_migration_readiness_to_markdown
task_permission_migration_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskPermissionMigrationReadinessPlan",
    "TaskPermissionMigrationReadinessRecord",
    "TaskPermissionMigrationReadinessRecommendation",
    "analyze_task_permission_migration_readiness",
    "build_task_permission_migration_readiness",
    "build_task_permission_migration_readiness_plan",
    "derive_task_permission_migration_readiness",
    "derive_task_permission_migration_readiness_plan",
    "extract_task_permission_migration_readiness",
    "extract_task_permission_migration_readiness_records",
    "generate_task_permission_migration_readiness",
    "generate_task_permission_migration_readiness_plan",
    "recommend_task_permission_migration_readiness",
    "summarize_task_permission_migration_readiness",
    "task_permission_migration_readiness_plan_to_dict",
    "task_permission_migration_readiness_plan_to_dicts",
    "task_permission_migration_readiness_plan_to_markdown",
    "task_permission_migration_readiness_to_dict",
    "task_permission_migration_readiness_to_dicts",
    "task_permission_migration_readiness_to_markdown",
]
