"""Extract source-level audit evidence export requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceAuditEvidenceExportRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceAuditEvidenceExportRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("evidence_scope", re.compile(r"\b(?:evidence scope|audit evidence scope|export scope|scope of evidence|included evidence)\b", re.I), ("evidence scope",), {"evidence scope": re.compile(r"\b(?:logs?|controls?|tickets?|policies|documents?|systems?)\b", re.I)}),
    KeywordRequirementSpec("export_format", re.compile(r"\b(?:export format|file format|pdf export|csv export|zip package)\b", re.I), ("export format",), {"export format": re.compile(r"\b(?:pdf|csv|json|zip|xlsx|format|package)\b", re.I)}),
    KeywordRequirementSpec("requester_role", re.compile(r"\b(?:requester role|requesting role|auditor role|requester permission|allowed requester)\b", re.I), ("requester role",), {"requester role": re.compile(r"\b(?:auditor|admin|compliance|security|role|permission)\b", re.I)}),
    KeywordRequirementSpec("approval_gate", re.compile(r"\b(?:approval gate|approval workflow|manager approval|compliance approval|pre[- ]?export approval)\b", re.I), ("approval gate",), {"approval gate": re.compile(r"\b(?:approval|manager|compliance|legal|review|gate)\b", re.I)}),
    KeywordRequirementSpec("redaction_policy", re.compile(r"\b(?:redaction policy|redact policy|pii redaction|sensitive data redaction|masking policy)\b", re.I), ("redaction policy",), {"redaction policy": re.compile(r"\b(?:redact|mask|pii|sensitive|secret|token)\b", re.I)}),
    KeywordRequirementSpec("retention_period", re.compile(r"\b(?:retention period|export retention|retain exports?|evidence retention|expiration period)\b", re.I), ("retention period",), {"retention period": re.compile(r"\b(?:days?|months?|years?|retain|delete|expire|\d+)\b", re.I)}),
    KeywordRequirementSpec("chain_of_custody", re.compile(r"\b(?:chain[- ]of[- ]custody|custody metadata|evidence metadata|hash metadata|tamper metadata)\b", re.I), ("chain of custody",), {"chain of custody": re.compile(r"\b(?:hash|timestamp|sign(?:ed|ature)?|actor|metadata|custody)\b", re.I)}),
    KeywordRequirementSpec("delivery_channel", re.compile(r"\b(?:delivery channel|download link|secure delivery|sftp delivery|email delivery)\b", re.I), ("delivery channel",), {"delivery channel": re.compile(r"\b(?:download|sftp|email|secure link|portal|channel)\b", re.I)}),
    KeywordRequirementSpec("access_logging", re.compile(r"\b(?:access logging|download logging|export access log|audit access|view logging)\b", re.I), ("access logging",), {"access logging": re.compile(r"\b(?:log|actor|timestamp|ip|download|view)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:audit evidence export|evidence export|audit export|compliance evidence export)\b", re.I)
_STRUCTURED = re.compile(r"(?:audit|evidence|export|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:audit evidence export|evidence export|audit export)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:audit evidence export|evidence export|audit export)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_evidence_scope": ("evidence scope",), "missing_redaction_policy": ("redaction policy",), "missing_access_logging": ("access logging",)}


def build_source_audit_evidence_export_requirements(source: Any) -> SourceAuditEvidenceExportRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Audit Evidence Export Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_audit_evidence_export_requirements(source: Any) -> SourceAuditEvidenceExportRequirementsReport:
    return build_source_audit_evidence_export_requirements(source)


def generate_source_audit_evidence_export_requirements(source: Any) -> SourceAuditEvidenceExportRequirementsReport:
    return build_source_audit_evidence_export_requirements(source)


def derive_source_audit_evidence_export_requirements(source: Any) -> SourceAuditEvidenceExportRequirementsReport:
    return build_source_audit_evidence_export_requirements(source)


def summarize_source_audit_evidence_export_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceAuditEvidenceExportRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_audit_evidence_export_requirements(source_or_result).summary


def source_audit_evidence_export_requirements_to_dict(report: SourceAuditEvidenceExportRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_audit_evidence_export_requirements_to_dict.__test__ = False


def source_audit_evidence_export_requirements_to_dicts(requirements: SourceAuditEvidenceExportRequirementsReport | list[SourceAuditEvidenceExportRequirement] | tuple[SourceAuditEvidenceExportRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceAuditEvidenceExportRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_audit_evidence_export_requirements_to_dicts.__test__ = False


def source_audit_evidence_export_requirements_to_markdown(report: SourceAuditEvidenceExportRequirementsReport) -> str:
    return report.to_markdown()


source_audit_evidence_export_requirements_to_markdown.__test__ = False
