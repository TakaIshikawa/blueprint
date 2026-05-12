"""Assess readiness for access review and recertification tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskAccessReviewReadinessFinding = SimpleReadinessRecord
TaskAccessReviewReadinessPlan = SimpleReadinessPlan

_SIGNALS = {
    "access_review": re.compile(r"\b(?:access review|user access review|admin access review|privileged access review|permission review)\b", re.I),
    "entitlement_review": re.compile(r"\b(?:entitlement review|entitlements?|role review|group membership review|permission recertification)\b", re.I),
    "recertification": re.compile(r"\b(?:recertification|re[- ]?certify|certify access|attestation|reviewer attestation)\b", re.I),
    "privileged_access": re.compile(r"\b(?:privileged access|admin access|elevated access|superuser|breakglass|break glass)\b", re.I),
    "stale_access": re.compile(r"\b(?:stale access|orphaned access|inactive users?|dormant accounts?|unused access)\b", re.I),
}
_PATH_SIGNALS = {
    "access_review": re.compile(r"access[-_]?reviews?|permissions?|reviews?", re.I),
    "entitlement_review": re.compile(r"entitlements?|roles?|groups?", re.I),
    "recertification": re.compile(r"recert|attest", re.I),
    "privileged_access": re.compile(r"privileged|admin|breakglass|break[-_]?glass", re.I),
    "stale_access": re.compile(r"stale|inactive|dormant|orphaned", re.I),
}
_CRITERIA = {
    "reviewer_ownership": re.compile(r"\b(?:reviewer owner|review owner|business owner|manager owner|app owner|system owner|reviewer assignment|approver|attester)\b", re.I),
    "population_scope": re.compile(r"\b(?:population scope|in scope users?|user population|all admins?|all users?|roles? in scope|groups? in scope|privileged population|scope by)\b", re.I),
    "evidence_source": re.compile(r"\b(?:evidence source|source of truth|iam export|idp export|directory export|hris|audit evidence|access evidence|entitlement report)\b", re.I),
    "revocation_workflow": re.compile(r"\b(?:revocation workflow|revoke access|remove access|deprovision|remediation workflow|ticket for removal|access removal|disable access)\b", re.I),
    "review_cadence": re.compile(r"\b(?:review cadence|cadence|quarterly|monthly|annual|annually|recurring review|review schedule|every \d+ (?:days|weeks|months))\b", re.I),
}
_GUIDANCE = {
    "reviewer_ownership": "Assign reviewer ownership for attesting each access population.",
    "population_scope": "Define the user, admin, role, group, and entitlement population in scope.",
    "evidence_source": "Identify the evidence source or system of record for access data.",
    "revocation_workflow": "Define the workflow for revoking or remediating rejected access.",
    "review_cadence": "Set the access review cadence and schedule.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:access review|entitlement review|recertification|stale access|privileged access)\b"
    r".{0,100}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


def build_task_access_review_readiness_plan(source: Any) -> TaskAccessReviewReadinessPlan:
    return build_simple_readiness_plan(
        source,
        title="Task Access Review Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_access_review_readiness(source: Any) -> TaskAccessReviewReadinessPlan:
    return build_task_access_review_readiness_plan(source)


def extract_task_access_review_readiness(source: Any) -> TaskAccessReviewReadinessPlan:
    return build_task_access_review_readiness_plan(source)


def generate_task_access_review_readiness(source: Any) -> TaskAccessReviewReadinessPlan:
    return build_task_access_review_readiness_plan(source)


def derive_task_access_review_readiness(source: Any) -> TaskAccessReviewReadinessPlan:
    return build_task_access_review_readiness_plan(source)


def summarize_task_access_review_readiness(source: Any) -> TaskAccessReviewReadinessPlan:
    return build_task_access_review_readiness_plan(source)


def recommend_task_access_review_readiness(source: Any) -> TaskAccessReviewReadinessPlan:
    return build_task_access_review_readiness_plan(source)


def task_access_review_readiness_plan_to_dict(report: TaskAccessReviewReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_access_review_readiness_plan_to_dict.__test__ = False


def task_access_review_readiness_plan_to_dicts(report: TaskAccessReviewReadinessPlan) -> list[dict[str, Any]]:
    return report.to_dicts()


task_access_review_readiness_plan_to_dicts.__test__ = False


def task_access_review_readiness_plan_to_markdown(report: TaskAccessReviewReadinessPlan) -> str:
    return report.to_markdown()


task_access_review_readiness_plan_to_markdown.__test__ = False
