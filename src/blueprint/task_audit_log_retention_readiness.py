"""Analyze audit log retention readiness for execution-plan tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, build_simple_readiness_plan


_SIGNAL_PATTERNS = {
    "audit_log_retention": re.compile(r"\b(?:audit|security|activity)[_\s-]+logs?.{0,80}\b(?:retention|retain|ttl|archive|purge|prune|legal[_\s-]+hold)\b|\b(?:retention|archive|purge|prune|legal[_\s-]+hold).{0,80}\b(?:audit|security|activity)[_\s-]+logs?\b", re.I),
    "archive_or_purge": re.compile(r"\b(?:archive|archival|cold[_\s-]+storage|purge|prune|expire|delete[_\s-]+after|retention[_\s-]+job)\b", re.I),
    "legal_hold": re.compile(r"\b(?:legal[_\s-]+hold|litigation[_\s-]+hold|compliance[_\s-]+hold|hold[_\s-]+exception)\b", re.I),
    "audit_retention_path": re.compile(r"\b(?:src|app|lib|jobs|workers|services)[_\s/-]+.*(?:audit|activity|security)[_\s-]*(?:log|logs)?.*(?:retention|archive|purge|ttl|legal[_\s-]*hold)\b|\b(?:src|app|lib|jobs|workers|services)[_\s/-]+.*(?:retention|archive|purge|ttl|legal[_\s-]*hold).*(?:audit|activity|security)[_\s-]*(?:log|logs)?\b", re.I),
}
_PATH_SIGNAL_PATTERNS = {
    "audit_retention_path": re.compile(r"\b(?:audit|activity|security)[_\s-]*(?:log|logs)?.*\b(?:retention|archive|purge|ttl|legal[_\s-]*hold)\b|\b(?:retention|archive|purge|ttl|legal[_\s-]*hold).*\b(?:audit|activity|security)[_\s-]*(?:log|logs)?\b", re.I),
}
_CRITERIA_PATTERNS = {
    "retention_period": re.compile(r"\b(?:retention[_\s-]+period|retain(?:ed)?[_\s-]+for|keep[_\s-]+for|ttl|time[_\s-]+to[_\s-]+live|delete[_\s-]+after|expire[_\s-]+after)\b", re.I),
    "storage_archive_target": re.compile(r"\b(?:archive[_\s-]+target|storage[_\s-]+target|cold[_\s-]+storage|s3|object[_\s-]+storage|warehouse|bucket|glacier|w orm|immutable[_\s-]+store)\b".replace("w orm", "worm"), re.I),
    "purge_mechanics": re.compile(r"\b(?:purge[_\s-]+mechanics|purge[_\s-]+job|retention[_\s-]+worker|cleanup[_\s-]+job|batch(?:ed)?[_\s-]+purge|prune|delete[_\s-]+eligible|idempotent[_\s-]+purge)\b", re.I),
    "legal_hold_exceptions": re.compile(r"\b(?:legal[_\s-]+hold|litigation[_\s-]+hold|compliance[_\s-]+hold|hold[_\s-]+exception|do[_\s-]+not[_\s-]+purge)\b", re.I),
    "access_controls": re.compile(r"\b(?:access[_\s-]+control|rbac|iam|least[_\s-]+privilege|permission|authorized|admin[_\s-]+only|security[_\s-]+review)\b", re.I),
    "evidence_export": re.compile(r"\b(?:evidence[_\s-]+export|export[_\s-]+evidence|audit[_\s-]+evidence|csv[_\s-]+export|report[_\s-]+export|compliance[_\s-]+export|chain[_\s-]+of[_\s-]+custody)\b", re.I),
    "monitoring": re.compile(r"\b(?:monitoring|metrics?|dashboard|alerts?|purge[_\s-]+lag|archive[_\s-]+failures?|job[_\s-]+health)\b", re.I),
    "tests": re.compile(r"\b(?:tests?|unit[_\s-]+tests?|integration[_\s-]+tests?|e2e|fixture|coverage|verification)\b", re.I),
}
_GUIDANCE = {
    "retention_period": "Define the audit log retention period, TTL rule, and record classes covered.",
    "storage_archive_target": "Name the archive or storage target and its immutability/encryption expectations.",
    "purge_mechanics": "Describe purge mechanics, batch limits, scheduling, and retry/idempotency behavior.",
    "legal_hold_exceptions": "Document legal-hold and compliance-hold exceptions that prevent purge.",
    "access_controls": "Specify access controls for viewing, archiving, purging, and exporting audit logs.",
    "evidence_export": "Add an evidence export path for compliance review with scope, timing, and actor details.",
    "monitoring": "Add metrics, dashboards, and alerts for archive/purge failures, lag, and skipped holds.",
    "tests": "Cover retention, archive, purge, legal-hold, access-control, export, and monitoring behavior in tests.",
}


def build_task_audit_log_retention_readiness_plan(source: Any) -> SimpleReadinessPlan:
    """Build audit log retention readiness records from task-shaped input."""
    return build_simple_readiness_plan(
        source,
        title="Task Audit Log Retention Readiness",
        signal_patterns=_SIGNAL_PATTERNS,
        path_signal_patterns=_PATH_SIGNAL_PATTERNS,
        criteria_patterns=_CRITERIA_PATTERNS,
        criterion_guidance=_GUIDANCE,
    )


analyze_task_audit_log_retention_readiness = build_task_audit_log_retention_readiness_plan
summarize_task_audit_log_retention_readiness = build_task_audit_log_retention_readiness_plan
generate_task_audit_log_retention_readiness = build_task_audit_log_retention_readiness_plan
extract_task_audit_log_retention_readiness = build_task_audit_log_retention_readiness_plan
recommend_task_audit_log_retention_readiness = build_task_audit_log_retention_readiness_plan


def task_audit_log_retention_readiness_plan_to_dict(plan: SimpleReadinessPlan) -> dict[str, Any]:
    """Serialize an audit log retention readiness plan."""
    return plan.to_dict()


def task_audit_log_retention_readiness_plan_to_dicts(plan: SimpleReadinessPlan) -> list[dict[str, Any]]:
    """Serialize audit log retention readiness records."""
    return plan.to_dicts()


def task_audit_log_retention_readiness_plan_to_markdown(plan: SimpleReadinessPlan) -> str:
    """Render audit log retention readiness as Markdown."""
    return plan.to_markdown()


__all__ = [
    "SimpleReadinessPlan",
    "analyze_task_audit_log_retention_readiness",
    "build_task_audit_log_retention_readiness_plan",
    "extract_task_audit_log_retention_readiness",
    "generate_task_audit_log_retention_readiness",
    "recommend_task_audit_log_retention_readiness",
    "summarize_task_audit_log_retention_readiness",
    "task_audit_log_retention_readiness_plan_to_dict",
    "task_audit_log_retention_readiness_plan_to_dicts",
    "task_audit_log_retention_readiness_plan_to_markdown",
]
