"""Assess readiness for export access review tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskExportAccessReviewReadinessFinding = SimpleReadinessRecord
TaskExportAccessReviewReadinessPlan = SimpleReadinessPlan

_SIGNALS = {
    "export": re.compile(r"\b(?:export|report export|data export|csv export|xlsx export)\b", re.I),
    "report": re.compile(r"\b(?:report|reporting|analytics report|scheduled report)\b", re.I),
    "download": re.compile(r"\b(?:download|bulk download|file download|downloadable)\b", re.I),
    "data_share": re.compile(r"\b(?:data share|share data|external share|customer data extract|dataset share)\b", re.I),
}
_PATH_SIGNALS = {
    "export": re.compile(r"exports?|csv|xlsx|extract", re.I),
    "report": re.compile(r"reports?|analytics", re.I),
    "download": re.compile(r"downloads?|files?", re.I),
    "data_share": re.compile(r"shares?|datasets?", re.I),
}
_CRITERIA = {
    "reviewer_ownership": re.compile(r"\b(?:reviewer owner|review owner|data owner|business owner|approver|access reviewer|reviewer assignment)\b", re.I),
    "allowed_audience": re.compile(r"\b(?:allowed audience|authorized audience|permitted users?|eligible roles?|allowed recipients?|recipient allowlist)\b", re.I),
    "field_level_access": re.compile(r"\b(?:field-level access|field level access|column permission|per-field permission|attribute access|row and column checks?)\b", re.I),
    "sensitive_column_handling": re.compile(r"\b(?:sensitive columns?|pii columns?|redact(?:ion)?|mask(?:ing)?|tokenize|confidential fields?)\b", re.I),
    "audit_evidence": re.compile(r"\b(?:audit evidence|audit log|access evidence|export log|download log|review evidence)\b", re.I),
    "expiry_revocation": re.compile(r"\b(?:expiry|expiration|revocation|revoke|time limited|ttl|link expires?|access removal)\b", re.I),
    "approval_workflow": re.compile(r"\b(?:approval workflow|approval required|manager approval|access request|request approval|two-person approval)\b", re.I),
}
_GUIDANCE = {
    "reviewer_ownership": "Assign reviewer ownership for export access decisions.",
    "allowed_audience": "Define the allowed audience or recipient allowlist for exported data.",
    "field_level_access": "Specify field-level access checks before report or export generation.",
    "sensitive_column_handling": "Define masking, redaction, or exclusion for sensitive columns.",
    "audit_evidence": "Capture audit evidence for export generation, download, and review decisions.",
    "expiry_revocation": "Define export access expiry and revocation behavior.",
    "approval_workflow": "Add an approval workflow for access to exposed export data.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:export|report|download|data share|csv)\b"
    r".{0,100}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


def build_task_export_access_review_readiness_plan(source: Any) -> TaskExportAccessReviewReadinessPlan:
    return build_simple_readiness_plan(
        source,
        title="Task Export Access Review Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_export_access_review_readiness(source: Any) -> TaskExportAccessReviewReadinessPlan:
    return build_task_export_access_review_readiness_plan(source)


def extract_task_export_access_review_readiness(source: Any) -> TaskExportAccessReviewReadinessPlan:
    return build_task_export_access_review_readiness_plan(source)


def generate_task_export_access_review_readiness(source: Any) -> TaskExportAccessReviewReadinessPlan:
    return build_task_export_access_review_readiness_plan(source)


def derive_task_export_access_review_readiness(source: Any) -> TaskExportAccessReviewReadinessPlan:
    return build_task_export_access_review_readiness_plan(source)


def summarize_task_export_access_review_readiness(source: Any) -> TaskExportAccessReviewReadinessPlan:
    return build_task_export_access_review_readiness_plan(source)


def recommend_task_export_access_review_readiness(source: Any) -> TaskExportAccessReviewReadinessPlan:
    return build_task_export_access_review_readiness_plan(source)


def task_export_access_review_readiness_plan_to_dict(report: TaskExportAccessReviewReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_export_access_review_readiness_plan_to_dict.__test__ = False


def task_export_access_review_readiness_plan_to_dicts(report: TaskExportAccessReviewReadinessPlan) -> list[dict[str, Any]]:
    return report.to_dicts()


task_export_access_review_readiness_plan_to_dicts.__test__ = False


def task_export_access_review_readiness_plan_to_markdown(report: TaskExportAccessReviewReadinessPlan) -> str:
    return report.to_markdown()


task_export_access_review_readiness_plan_to_markdown.__test__ = False
