"""Assess readiness for rollback plan execution tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskRollbackPlanReadinessPlan = SimpleReadinessPlan
TaskRollbackPlanReadinessRecord = SimpleReadinessRecord
TaskRollbackPlanReadinessFinding = SimpleReadinessRecord
TaskRollbackPlanReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "rollback_plan": re.compile(r"\b(?:rollback plan|rollback strategy|remediation rollback|emergency rollback|post[- ]deploy rollback)\b", re.I),
    "revert_plan": re.compile(r"\b(?:revert plan|backout plan|revert strategy|backout strategy)\b", re.I),
    "rollback_validation": re.compile(r"\b(?:rollback validation|post[- ]deploy rollback validation|rollback verification|revert validation)\b", re.I),
    "deployment_rollback": re.compile(r"\b(?:deploy(?:ment)? rollback|migration rollback|release rollback|schema rollback)\b", re.I),
}
_PATH_SIGNALS = {
    "rollback_plan": re.compile(r"(?:rollback[_-]?plan|rollback|remediation[_-]?rollback|emergency[_-]?rollback)", re.I),
    "revert_plan": re.compile(r"(?:revert[_-]?plan|backout[_-]?plan|revert|backout)", re.I),
    "rollback_validation": re.compile(r"(?:rollback[_-]?validation|rollback[_-]?verification|revert[_-]?validation)", re.I),
    "deployment_rollback": re.compile(r"(?:deploy|deployment|migration|release|schema)", re.I),
}
_CRITERIA = {
    "rollback_trigger": re.compile(r"\b(?:rollback trigger|trigger condition|rollback criteria|failure threshold|error threshold|go/no[- ]go threshold|abort condition)\b", re.I),
    "rollback_procedure": re.compile(r"\b(?:rollback procedure|rollback steps?|revert procedure|backout steps?|restore procedure|runbook|playbook)\b", re.I),
    "data_schema_compatibility": re.compile(r"\b(?:data compatibility|schema compatibility|backward compatible|forward compatible|migration compatibility|data rollback|schema rollback)\b", re.I),
    "owner_approver": re.compile(r"\b(?:owner|approver|approval|incident commander|release manager|on[- ]call|sign[- ]off)\b", re.I),
    "verification_checks": re.compile(r"\b(?:verification checks?|validation checks?|health checks?|smoke tests?|post[- ]rollback checks?|monitoring checks?|rollback validation)\b", re.I),
    "customer_impact_communication": re.compile(r"\b(?:customer impact communication|customer communication|customer notice|status update|support message|incident update)\b", re.I),
    "time_limit_stop_condition": re.compile(r"\b(?:time limit|stop condition|maximum duration|rollback window|timeout|abort after|stop after)\b", re.I),
    "tests": re.compile(r"\b(?:tests?|pytest|unit tests?|integration tests?|rollback tests?|revert tests?|migration tests?|smoke tests?)\b", re.I),
}
_GUIDANCE = {
    "rollback_trigger": "Define rollback triggers, criteria, failure thresholds, error thresholds, go/no-go thresholds, or abort conditions.",
    "rollback_procedure": "Document rollback, revert, backout, restore, runbook, or playbook steps.",
    "data_schema_compatibility": "Cover data, schema, backward, forward, migration, or rollback compatibility.",
    "owner_approver": "Name the owner, approver, incident commander, release manager, on-call, or sign-off flow.",
    "verification_checks": "Add verification, validation, health, smoke, post-rollback, monitoring, or rollback validation checks.",
    "customer_impact_communication": "Document customer impact communication, notices, status updates, support messages, or incident updates.",
    "time_limit_stop_condition": "Specify time limits, stop conditions, maximum duration, rollback window, timeout, or abort-after rules.",
    "tests": "Add unit, integration, rollback, revert, migration, or smoke tests.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:rollback plans?|revert plans?|remediation rollback|emergency rollback|rollback validation)\b.{0,80}\b(?:impact|changes?|planned|scope|required|needed)\b",
    re.I,
)


def build_task_rollback_plan_readiness_plan(source: Any) -> TaskRollbackPlanReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Rollback Plan Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_rollback_plan_readiness = build_task_rollback_plan_readiness_plan
extract_task_rollback_plan_readiness = build_task_rollback_plan_readiness_plan
generate_task_rollback_plan_readiness = build_task_rollback_plan_readiness_plan
derive_task_rollback_plan_readiness = build_task_rollback_plan_readiness_plan
summarize_task_rollback_plan_readiness = build_task_rollback_plan_readiness_plan
summarize_task_rollback_plan_readiness_plan = build_task_rollback_plan_readiness_plan


def recommend_task_rollback_plan_readiness(source: Any) -> tuple[TaskRollbackPlanReadinessRecord, ...]:
    return build_task_rollback_plan_readiness_plan(source).records


def task_rollback_plan_readiness_plan_to_dict(plan: TaskRollbackPlanReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


task_rollback_plan_readiness_plan_to_dict.__test__ = False


def task_rollback_plan_readiness_plan_to_dicts(
    plan: TaskRollbackPlanReadinessPlan | Iterable[TaskRollbackPlanReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(plan, SimpleReadinessPlan):
        return plan.to_dicts()
    return [record.to_dict() for record in plan]


task_rollback_plan_readiness_plan_to_dicts.__test__ = False
task_rollback_plan_readiness_to_dicts = task_rollback_plan_readiness_plan_to_dicts
task_rollback_plan_readiness_to_dicts.__test__ = False


def task_rollback_plan_readiness_plan_to_markdown(plan: TaskRollbackPlanReadinessPlan) -> str:
    return plan.to_markdown()


task_rollback_plan_readiness_plan_to_markdown.__test__ = False
