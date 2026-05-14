"""Assess readiness for consent policy implementation tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan

TaskConsentPolicyReadinessPlan = SimpleReadinessPlan
TaskConsentPolicyReadinessRecord = SimpleReadinessRecord

_SIGNALS = {
    "consent_policy": re.compile(r"\b(?:consent policy|privacy consent|consent enforcement|policy copy)\b", re.I),
    "lawful_basis": re.compile(r"\b(?:lawful basis|purpose limitation|processing purpose|consent purpose)\b", re.I),
    "preference_choice": re.compile(r"\b(?:opt[- ]?in|opt[- ]?out|consent versioning|capture consent|revoke consent|revocation)\b", re.I),
}
_PATH_SIGNALS = {
    "consent_policy": re.compile(r"(?:consent[_-]?policy|privacy[_-]?consent|consents?)", re.I),
    "lawful_basis": re.compile(r"(?:lawful[_-]?basis|purpose[_-]?limitation|purposes?)", re.I),
    "preference_choice": re.compile(r"(?:opt[_-]?in|opt[_-]?out|revocation|preference)", re.I),
}
_CRITERIA = {
    "consent_scope": re.compile(r"\b(?:consent scope|scope of consent|processing purposes?|purpose limitation|data categories?|consent purposes?)\b", re.I),
    "policy_version_source": re.compile(r"\b(?:policy version|version source|versioned policy|consent versioning|policy source|source of truth)\b", re.I),
    "capture_or_revocation_path": re.compile(r"\b(?:capture consent|consent capture|revocation path|revoke consent|withdraw consent|opt[- ]?in|opt[- ]?out)\b", re.I),
    "enforcement_points": re.compile(r"\b(?:enforcement points?|consent enforcement|gate processing|deny processing|access checks?|enforce consent)\b", re.I),
    "audit_trail": re.compile(r"\b(?:audit trail|audit log|auditability|consent history|evidence log|traceability)\b", re.I),
    "user_communication": re.compile(r"\b(?:user communication|policy copy|notice copy|email notification|notification|in[- ]?app notice|user notice)\b", re.I),
    "validation_coverage": re.compile(r"\b(?:validation coverage|tests?|unit tests?|integration tests?|pytest|acceptance tests?|consent tests?)\b", re.I),
}
_GUIDANCE = {
    "consent_scope": "Define consent scope, processing purposes, purpose limitation, or data categories covered by consent.",
    "policy_version_source": "Identify the policy version source, versioned policy, consent versioning, or policy source of truth.",
    "capture_or_revocation_path": "Specify capture or revocation paths for opt-in, opt-out, withdrawal, or consent capture.",
    "enforcement_points": "Document enforcement points where consent gates processing, denies processing, or adds access checks.",
    "audit_trail": "Add an audit trail with consent history, audit logs, evidence logs, or traceability.",
    "user_communication": "Define user communication such as policy copy, notices, email notifications, or in-app notices.",
    "validation_coverage": "Add validation coverage with unit, integration, pytest, acceptance, or consent tests.",
}
_NO_IMPACT = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:consent policy|privacy consent|lawful basis|opt[- ]?in|opt[- ]?out)\b.{0,80}\b(?:impact|changes?|planned|scope|required|needed)\b", re.I)


def build_task_consent_policy_readiness_plan(source: Any) -> TaskConsentPolicyReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(source, title="Task Consent Policy Readiness", signal_patterns=_SIGNALS, path_signal_patterns=_PATH_SIGNALS, criteria_patterns=_CRITERIA, criterion_guidance=_GUIDANCE, no_impact_pattern=_NO_IMPACT)


analyze_task_consent_policy_readiness = build_task_consent_policy_readiness_plan
extract_task_consent_policy_readiness = build_task_consent_policy_readiness_plan
generate_task_consent_policy_readiness = build_task_consent_policy_readiness_plan
derive_task_consent_policy_readiness = build_task_consent_policy_readiness_plan
summarize_task_consent_policy_readiness = build_task_consent_policy_readiness_plan
summarize_task_consent_policy_readiness_plan = build_task_consent_policy_readiness_plan


def recommend_task_consent_policy_readiness(source: Any) -> tuple[TaskConsentPolicyReadinessRecord, ...]:
    return build_task_consent_policy_readiness_plan(source).records


def task_consent_policy_readiness_plan_to_dict(result: TaskConsentPolicyReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_consent_policy_readiness_plan_to_dict.__test__ = False


def task_consent_policy_readiness_plan_to_dicts(result: TaskConsentPolicyReadinessPlan | Iterable[TaskConsentPolicyReadinessRecord]) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_consent_policy_readiness_plan_to_dicts.__test__ = False
task_consent_policy_readiness_to_dicts = task_consent_policy_readiness_plan_to_dicts


def task_consent_policy_readiness_plan_to_markdown(result: TaskConsentPolicyReadinessPlan) -> str:
    return result.to_markdown()


task_consent_policy_readiness_plan_to_markdown.__test__ = False
