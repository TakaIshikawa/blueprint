"""Extract source-level revenue recognition requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceRevenueRecognitionRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceRevenueRecognitionRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("recognition_rule", re.compile(r"\b(?:recognition rule|revenue recognition rule|recognize revenue|recognition policy|rev rec rule)\b", re.I), ("recognition rule",), {"recognition rule": re.compile(r"\b(?:ratably|point in time|over time|upon delivery|asc 606|ifrs 15|policy says)\b", re.I)}),
    KeywordRequirementSpec("performance_obligation", re.compile(r"\b(?:performance obligation|obligation mapping|deliverable obligation|revenue obligation)\b", re.I), ("obligation detail",), {"obligation detail": re.compile(r"\b(?:setup|subscription|service|deliverable|bundle|standalone selling price|ssp)\b", re.I)}),
    KeywordRequirementSpec("contract_term", re.compile(r"\b(?:contract term|contract duration|service term|term dates|contract period)\b", re.I), ("term detail",), {"term detail": re.compile(r"\b(?:start date|end date|months?|annual|renewal|term length)\b", re.I)}),
    KeywordRequirementSpec("deferral_schedule", re.compile(r"\b(?:deferral schedule|deferred revenue|recognition schedule|revenue schedule|amortization schedule)\b", re.I), ("deferral schedule",), {"deferral schedule": re.compile(r"\b(?:monthly|daily|schedule by|amortize|deferred revenue account|period)\b", re.I)}),
    KeywordRequirementSpec("modification_handling", re.compile(r"\b(?:modification handling|contract modification|plan change accounting|upgrade accounting|downgrade accounting)\b", re.I), ("modification handling",), {"modification handling": re.compile(r"\b(?:prospective|retrospective|cumulative catch[- ]?up|reallocate|modification date)\b", re.I)}),
    KeywordRequirementSpec("accounting_export", re.compile(r"\b(?:accounting export|ledger export|erp export|gl export|journal export)\b", re.I), ("accounting export",), {"accounting export": re.compile(r"\b(?:erp|gl|journal|ledger|netsuite|csv|export file)\b", re.I)}),
    KeywordRequirementSpec("audit_evidence", re.compile(r"\b(?:audit evidence|revenue audit|recognition evidence|audit packet|evidence trail)\b", re.I), ("audit evidence",), {"audit evidence": re.compile(r"\b(?:contract|invoice|timestamp|calculation|audit log|attachment)\b", re.I)}),
    KeywordRequirementSpec("compliance_review", re.compile(r"\b(?:compliance review|accounting review|rev rec review|controller review|policy review)\b", re.I), ("compliance review",), {"compliance review": re.compile(r"\b(?:controller|accounting approval|reviewer|quarterly|policy owner|sign[- ]?off)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:revenue recognition|rev rec|deferred revenue|recognize revenue|asc 606|ifrs 15)\b", re.I)
_STRUCTURED = re.compile(r"(?:revenue|recognition|accounting|contract|compliance|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:revenue recognition|rev rec|deferred revenue)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:revenue recognition|rev rec|deferred revenue)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_recognition_rules": ("recognition rule",), "missing_obligations": ("obligation detail",), "missing_deferral_schedule": ("deferral schedule",), "missing_modification_handling": ("modification handling",), "missing_accounting_export": ("accounting export",), "missing_compliance_review": ("compliance review",)}


def build_source_revenue_recognition_requirements(source: Any) -> SourceRevenueRecognitionRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Revenue Recognition Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_revenue_recognition_requirements(source: Any) -> SourceRevenueRecognitionRequirementsReport:
    return build_source_revenue_recognition_requirements(source)


def generate_source_revenue_recognition_requirements(source: Any) -> SourceRevenueRecognitionRequirementsReport:
    return build_source_revenue_recognition_requirements(source)


def derive_source_revenue_recognition_requirements(source: Any) -> SourceRevenueRecognitionRequirementsReport:
    return build_source_revenue_recognition_requirements(source)


def summarize_source_revenue_recognition_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceRevenueRecognitionRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_revenue_recognition_requirements(source_or_result).summary


def source_revenue_recognition_requirements_to_dict(report: SourceRevenueRecognitionRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_revenue_recognition_requirements_to_dict.__test__ = False


def source_revenue_recognition_requirements_to_dicts(requirements: SourceRevenueRecognitionRequirementsReport | list[SourceRevenueRecognitionRequirement] | tuple[SourceRevenueRecognitionRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceRevenueRecognitionRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_revenue_recognition_requirements_to_dicts.__test__ = False


def source_revenue_recognition_requirements_to_markdown(report: SourceRevenueRecognitionRequirementsReport) -> str:
    return report.to_markdown()


source_revenue_recognition_requirements_to_markdown.__test__ = False
