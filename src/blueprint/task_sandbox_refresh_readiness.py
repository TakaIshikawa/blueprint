"""Assess readiness for sandbox or staging environment refresh tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import (
    SimpleReadinessPlan,
    SimpleReadinessRecord,
    build_simple_readiness_plan,
)


TaskSandboxRefreshReadinessPlan = SimpleReadinessPlan
TaskSandboxRefreshReadinessRecord = SimpleReadinessRecord
TaskSandboxRefreshReadinessFinding = SimpleReadinessRecord
TaskSandboxRefreshReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "sandbox_refresh": re.compile(
        r"\b(?:sandbox refresh|refresh sandbox|sandbox data refresh|sandbox restore|"
        r"rebuild sandbox|reset sandbox|sandbox reseed)\b",
        re.I,
    ),
    "staging_refresh": re.compile(
        r"\b(?:staging refresh|refresh staging|staging data refresh|staging restore|"
        r"rebuild staging|reset staging|preprod refresh|pre-production refresh)\b",
        re.I,
    ),
    "environment_refresh": re.compile(
        r"\b(?:environment refresh|refresh environment|non-production refresh|lower environment refresh|"
        r"test environment refresh|environment restore|fixture refresh)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "sandbox_refresh": re.compile(r"sandbox|sandboxes|sandbox[_\s-]?refresh|sandbox[_\s-]?restore", re.I),
    "staging_refresh": re.compile(r"staging|stage[_\s-]?refresh|preprod|pre[_\s-]?production", re.I),
    "environment_refresh": re.compile(r"environments?|env[_\s-]?refresh|fixtures?|restore|snapshot|refresh", re.I),
}
_CRITERIA = {
    "refresh_source": re.compile(
        r"\b(?:refresh source|source environment|source snapshot|source backup|production snapshot|"
        r"prod snapshot|backup source|restore source|source dump|fixture source|baseline source)\b",
        re.I,
    ),
    "data_masking": re.compile(
        r"\b(?:data masking|mask(?:ed|ing)?|redact(?:ed|ion)?|anonymi[sz]e|scrub|saniti[sz]e|"
        r"synthetic data|no pii|pii removed|sensitive data)\b",
        re.I,
    ),
    "downtime_window": re.compile(
        r"\b(?:downtime window|maintenance window|refresh window|outage window|scheduled window|"
        r"freeze window|service interruption|expected downtime|offline window)\b",
        re.I,
    ),
    "service_coordination": re.compile(
        r"\b(?:dependent service|service coordination|coordinate with|downstream service|upstream service|"
        r"integration owner|dependency owner|pause jobs|webhook pause|queue drain|cache warm)\b",
        re.I,
    ),
    "validation_smoke_tests": re.compile(
        r"\b(?:validation|validate|verify|smoke test|health check|post-refresh check|post refresh check|"
        r"login test|sanity check|row count|integration test|acceptance check)\b",
        re.I,
    ),
    "rollback_restore_point": re.compile(
        r"\b(?:rollback|roll back|restore point|pre-refresh backup|pre refresh backup|snapshot before|"
        r"restore plan|revert|recovery point|backup before|undo refresh)\b",
        re.I,
    ),
    "stakeholder_notification": re.compile(
        r"\b(?:stakeholder notification|notify stakeholders|notification plan|announce|announcement|"
        r"status page|slack notice|email notice|release note|user notice|team notice)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "refresh_source": "Identify the refresh source such as production snapshot, backup, dump, fixture, or baseline environment.",
    "data_masking": "Confirm sensitive data is masked, redacted, anonymized, scrubbed, synthetic, or explicitly absent.",
    "downtime_window": "Schedule the downtime, maintenance, refresh, freeze, or service interruption window.",
    "service_coordination": "Coordinate dependent services, integration owners, queues, jobs, webhooks, caches, or upstream/downstream systems.",
    "validation_smoke_tests": "Add validation smoke tests such as health checks, login checks, row counts, or post-refresh verification.",
    "rollback_restore_point": "Document rollback, restore point, pre-refresh backup, snapshot, revert, or recovery steps.",
    "stakeholder_notification": "Notify stakeholders through announcements, Slack, email, status page, release notes, or team notices.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:sandbox|staging|environment|fixture)\b"
    r".{0,80}\b(?:refresh|restore|reset|reseed|changes?|impact|planned|required|needed)\b",
    re.I,
)


def build_task_sandbox_refresh_readiness_plan(source: Any) -> TaskSandboxRefreshReadinessPlan:
    """Build sandbox refresh readiness records for task-shaped input."""
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Sandbox Refresh Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_sandbox_refresh_readiness(source: Any) -> TaskSandboxRefreshReadinessPlan:
    return build_task_sandbox_refresh_readiness_plan(source)


def extract_task_sandbox_refresh_readiness(source: Any) -> TaskSandboxRefreshReadinessPlan:
    return build_task_sandbox_refresh_readiness_plan(source)


def generate_task_sandbox_refresh_readiness(source: Any) -> TaskSandboxRefreshReadinessPlan:
    return build_task_sandbox_refresh_readiness_plan(source)


def derive_task_sandbox_refresh_readiness(source: Any) -> TaskSandboxRefreshReadinessPlan:
    return build_task_sandbox_refresh_readiness_plan(source)


def summarize_task_sandbox_refresh_readiness(source: Any) -> TaskSandboxRefreshReadinessPlan:
    return build_task_sandbox_refresh_readiness_plan(source)


def summarize_task_sandbox_refresh_readiness_plan(source: Any) -> TaskSandboxRefreshReadinessPlan:
    return build_task_sandbox_refresh_readiness_plan(source)


def recommend_task_sandbox_refresh_readiness(source: Any) -> tuple[TaskSandboxRefreshReadinessRecord, ...]:
    return build_task_sandbox_refresh_readiness_plan(source).records


def task_sandbox_refresh_readiness_plan_to_dict(result: TaskSandboxRefreshReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_sandbox_refresh_readiness_plan_to_dict.__test__ = False


def task_sandbox_refresh_readiness_plan_to_dicts(
    result: TaskSandboxRefreshReadinessPlan | Iterable[TaskSandboxRefreshReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_sandbox_refresh_readiness_plan_to_dicts.__test__ = False
task_sandbox_refresh_readiness_to_dicts = task_sandbox_refresh_readiness_plan_to_dicts
task_sandbox_refresh_readiness_to_dicts.__test__ = False


def task_sandbox_refresh_readiness_plan_to_markdown(result: TaskSandboxRefreshReadinessPlan) -> str:
    return result.to_markdown()


task_sandbox_refresh_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskSandboxRefreshReadinessFinding",
    "TaskSandboxRefreshReadinessPlan",
    "TaskSandboxRefreshReadinessRecord",
    "TaskSandboxRefreshReadinessRecommendation",
    "analyze_task_sandbox_refresh_readiness",
    "build_task_sandbox_refresh_readiness_plan",
    "derive_task_sandbox_refresh_readiness",
    "extract_task_sandbox_refresh_readiness",
    "generate_task_sandbox_refresh_readiness",
    "recommend_task_sandbox_refresh_readiness",
    "summarize_task_sandbox_refresh_readiness",
    "summarize_task_sandbox_refresh_readiness_plan",
    "task_sandbox_refresh_readiness_plan_to_dict",
    "task_sandbox_refresh_readiness_plan_to_dicts",
    "task_sandbox_refresh_readiness_plan_to_markdown",
    "task_sandbox_refresh_readiness_to_dicts",
]
