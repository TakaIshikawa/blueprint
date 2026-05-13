"""Assess readiness for legal-hold and preservation tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskLegalHoldReadinessFinding = SimpleReadinessRecord
TaskLegalHoldReadinessPlan = SimpleReadinessPlan

_SIGNALS = {
    "legal_hold": re.compile(r"\b(?:legal hold|hold order|compliance hold|regulatory hold)\b", re.I),
    "preservation": re.compile(r"\b(?:preservation|preserve records?|preserve data|e[- ]?discovery|ediscovery)\b", re.I),
    "litigation_hold": re.compile(r"\b(?:litigation hold|lawsuit hold|discovery hold|case hold)\b", re.I),
    "purge_suppression": re.compile(r"\b(?:purge suppression|suppress purge|block purge|purge block|do not delete|suspend deletion)\b", re.I),
    "custodian_scope": re.compile(r"\b(?:custodian|custodians|employee mailbox|user mailbox|account scope|custodian scope)\b", re.I),
    "retention_override": re.compile(r"\b(?:retention override|retention exception|retention conflict|override retention|hold exemption)\b", re.I),
}
_PATH_SIGNALS = {
    "legal_hold": re.compile(r"legal[-_ ]?hold|hold[-_ ]?order|compliance[-_ ]?hold", re.I),
    "preservation": re.compile(r"preserv|e[-_]?discovery|ediscovery", re.I),
    "litigation_hold": re.compile(r"litigation[-_ ]?hold|case[-_ ]?hold", re.I),
    "purge_suppression": re.compile(r"purge[-_ ]?suppression|purge[-_ ]?block|delete[-_ ]?block", re.I),
    "custodian_scope": re.compile(r"custodian|mailbox|account[-_ ]?scope", re.I),
    "retention_override": re.compile(r"retention[-_ ]?override|retention[-_ ]?exception|retention[-_ ]?conflict", re.I),
}
_CRITERIA = {
    "authorization": re.compile(
        r"\b(?:authorization|authorize|authorized|approval|approve|legal approval|counsel approval|requestor|requester|approver)\b",
        re.I,
    ),
    "audit_trail": re.compile(
        r"\b(?:audit|audit trail|evidence|case log|hold log|chain of custody|timestamp|who placed|operator log)\b",
        re.I,
    ),
    "release_workflow": re.compile(
        r"\b(?:release workflow|release process|lift hold|hold release|release approval|remove hold|hold removal|expire hold)\b",
        re.I,
    ),
    "purge_blocking_checks": re.compile(
        r"\b(?:purge[- ]?blocking|block purge|purge block|delete block|deletion guard|do not delete|suspend deletion|pre[- ]?purge check)\b",
        re.I,
    ),
    "custodian_scope": re.compile(
        r"\b(?:custodian scope|custodians?|account scope|user scope|mailbox|dataset scope|records? scope|case scope)\b",
        re.I,
    ),
    "retention_conflict_handling": re.compile(
        r"\b(?:retention conflict|retention override|retention exception|ttl conflict|policy conflict|purge exception)\b",
        re.I,
    ),
    "notification": re.compile(
        r"\b(?:notification|notify|notice|hold notice|email notice|slack notice|custodian notice|legal notice)\b",
        re.I,
    ),
    "verification": re.compile(
        r"\b(?:verification|verify|validated?|test|dry run|control check|reconciliation|attestation|proof)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "authorization": "Define who can request, approve, place, and release legal holds.",
    "audit_trail": "Record hold placement, scope changes, release decisions, actors, timestamps, and evidence.",
    "release_workflow": "Document the hold release workflow, approval gate, and post-release cleanup behavior.",
    "purge_blocking_checks": "Add purge-blocking checks so deletion, TTL, and purge jobs skip held records.",
    "custodian_scope": "Specify custodian, account, mailbox, dataset, and case scope boundaries.",
    "retention_conflict_handling": "Resolve retention conflicts and override behavior while a hold is active.",
    "notification": "Notify custodians, legal reviewers, operators, and affected workflow owners.",
    "verification": "Verify placement, purge suppression, evidence capture, and release behavior before rollout.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,120}\b(?:legal hold|litigation hold|preservation|purge suppression|retention override)\b"
    r".{0,120}\b(?:scope|impact|changes?|required|needed|work)\b",
    re.I,
)


def build_task_legal_hold_readiness_plan(source: Any) -> TaskLegalHoldReadinessPlan:
    return build_simple_readiness_plan(
        source,
        title="Task Legal Hold Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_legal_hold_readiness(source: Any) -> TaskLegalHoldReadinessPlan:
    return build_task_legal_hold_readiness_plan(source)


def extract_task_legal_hold_readiness(source: Any) -> TaskLegalHoldReadinessPlan:
    return build_task_legal_hold_readiness_plan(source)


def generate_task_legal_hold_readiness(source: Any) -> TaskLegalHoldReadinessPlan:
    return build_task_legal_hold_readiness_plan(source)


def derive_task_legal_hold_readiness(source: Any) -> TaskLegalHoldReadinessPlan:
    return build_task_legal_hold_readiness_plan(source)


def summarize_task_legal_hold_readiness(source: Any) -> TaskLegalHoldReadinessPlan:
    return build_task_legal_hold_readiness_plan(source)


def recommend_task_legal_hold_readiness(source: Any) -> TaskLegalHoldReadinessPlan:
    return build_task_legal_hold_readiness_plan(source)


def task_legal_hold_readiness_plan_to_dict(report: TaskLegalHoldReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_legal_hold_readiness_plan_to_dict.__test__ = False


def task_legal_hold_readiness_plan_to_dicts(report: TaskLegalHoldReadinessPlan) -> list[dict[str, Any]]:
    return report.to_dicts()


task_legal_hold_readiness_plan_to_dicts.__test__ = False


def task_legal_hold_readiness_plan_to_markdown(report: TaskLegalHoldReadinessPlan) -> str:
    return report.to_markdown()


task_legal_hold_readiness_plan_to_markdown.__test__ = False
