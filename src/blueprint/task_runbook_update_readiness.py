"""Assess readiness for operational runbook update tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskRunbookUpdateReadinessPlan = SimpleReadinessPlan
TaskRunbookUpdateReadinessRecord = SimpleReadinessRecord
TaskRunbookUpdateReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "runbook": re.compile(r"\b(?:runbook|playbook|sop|standard operating procedure|operational procedure|ops procedure|incident procedure|support procedure)\b", re.I),
    "runbook_update": re.compile(r"\b(?:update|revise|refresh|document|add)\b.{0,80}\b(?:runbook|playbook|sop|operational procedure)\b", re.I),
}
_PATH_SIGNALS = {
    "runbook": re.compile(r"(?:runbook|playbook|sop|operational procedure|ops procedure)", re.I),
}
_CRITERIA = {
    "trigger_conditions": re.compile(r"\b(?:trigger|condition|symptom|when to use|alert fires|threshold|entry criteria|scenario)\b", re.I),
    "owner_escalation": re.compile(r"\b(?:owner|on[- ]call|escalat(?:e|ion)|pager|support tier|responsible team|contact)\b", re.I),
    "verification_steps": re.compile(r"\b(?:verify|verification|check|validation|confirm|test|health check|success criteria)\b", re.I),
    "mitigation_instructions": re.compile(r"\b(?:rollback|roll back|mitigation|remediate|workaround|restore|fallback|containment|recovery)\b", re.I),
    "review_cadence": re.compile(r"\b(?:review cadence|review every|quarterly|monthly|annually|expiration|owner review|recertify|last reviewed|next review)\b", re.I),
}
_GUIDANCE = {
    "trigger_conditions": "Add trigger conditions or symptoms that tell operators when to use the runbook.",
    "owner_escalation": "Add owner contacts and escalation path.",
    "verification_steps": "Add verification steps and success checks.",
    "mitigation_instructions": "Add rollback, mitigation, recovery, or workaround instructions.",
    "review_cadence": "Add review cadence, last-reviewed, or next-review ownership.",
}
_NO_IMPACT = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:runbook|playbook|sop|operational procedure)\b.{0,80}\b(?:required|needed|impact|changes?)\b", re.I)


def build_task_runbook_update_readiness_plan(source: Any) -> TaskRunbookUpdateReadinessPlan:
    return build_simple_readiness_plan(
        source,
        title="Task Runbook Update Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_runbook_update_readiness(source: Any) -> TaskRunbookUpdateReadinessPlan:
    return build_task_runbook_update_readiness_plan(source)


def extract_task_runbook_update_readiness(source: Any) -> TaskRunbookUpdateReadinessPlan:
    return build_task_runbook_update_readiness_plan(source)


def generate_task_runbook_update_readiness(source: Any) -> TaskRunbookUpdateReadinessPlan:
    return build_task_runbook_update_readiness_plan(source)


def derive_task_runbook_update_readiness(source: Any) -> TaskRunbookUpdateReadinessPlan:
    return build_task_runbook_update_readiness_plan(source)


def summarize_task_runbook_update_readiness(source: Any) -> TaskRunbookUpdateReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_task_runbook_update_readiness_plan(source)


def recommend_task_runbook_update_readiness(source: Any) -> tuple[TaskRunbookUpdateReadinessRecord, ...]:
    return build_task_runbook_update_readiness_plan(source).records


def task_runbook_update_readiness_plan_to_dict(result: TaskRunbookUpdateReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_runbook_update_readiness_plan_to_dict.__test__ = False


def task_runbook_update_readiness_plan_to_dicts(
    result: TaskRunbookUpdateReadinessPlan | Iterable[TaskRunbookUpdateReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_runbook_update_readiness_plan_to_dicts.__test__ = False
task_runbook_update_readiness_to_dicts = task_runbook_update_readiness_plan_to_dicts
task_runbook_update_readiness_to_dicts.__test__ = False


def task_runbook_update_readiness_plan_to_markdown(result: TaskRunbookUpdateReadinessPlan) -> str:
    return result.to_markdown()


task_runbook_update_readiness_plan_to_markdown.__test__ = False

