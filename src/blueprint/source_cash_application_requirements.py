"""Extract source-level cash application requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceCashApplicationRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceCashApplicationRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("payment_matching", re.compile(r"\b(?:payment matching|cash matching|match payments?|invoice matching|matching rules?)\b", re.I), ("matching rule",), {"matching rule": re.compile(r"\b(?:invoice number|amount tolerance|customer id|reference|auto match|matching by)\b", re.I)}),
    KeywordRequirementSpec("remittance_ingestion", re.compile(r"\b(?:remittance ingestion|remittance import|remittance advice|lockbox file|bank remittance)\b", re.I), ("remittance source",), {"remittance source": re.compile(r"\b(?:lockbox|edi|email|bank file|csv|ach addenda|source)\b", re.I)}),
    KeywordRequirementSpec("unapplied_cash_queue", re.compile(r"\b(?:unapplied cash queue|unapplied cash|unmatched cash|cash suspense|holding queue)\b", re.I), ("queue rule",), {"queue rule": re.compile(r"\b(?:queue|owner|aging|threshold|review|suspense)\b", re.I)}),
    KeywordRequirementSpec("short_payment_handling", re.compile(r"\b(?:short payment handling|short payment|underpayment|partial payment|deduction handling)\b", re.I), ("short payment rule",), {"short payment rule": re.compile(r"\b(?:deduction|write[- ]?off|tolerance|partial|dispute|reason code)\b", re.I)}),
    KeywordRequirementSpec("overpayment_handling", re.compile(r"\b(?:overpayment handling|overpayment|excess payment|customer credit from cash)\b", re.I), ("overpayment rule",), {"overpayment rule": re.compile(r"\b(?:credit memo|refund|apply forward|customer credit|threshold)\b", re.I)}),
    KeywordRequirementSpec("bank_reconciliation", re.compile(r"\b(?:bank reconciliation|bank rec|reconcile bank|cash reconciliation|statement reconciliation)\b", re.I), ("reconciliation detail",), {"reconciliation detail": re.compile(r"\b(?:bank statement|deposit id|settlement|reconcile daily|clearing account)\b", re.I)}),
    KeywordRequirementSpec("exception_workflow", re.compile(r"\b(?:exception workflow|cash exception|exception owner|manual review|research workflow)\b", re.I), ("exception owner",), {"exception owner": re.compile(r"\b(?:owner|queue|sla|assignee|analyst|approval|escalation)\b", re.I)}),
    KeywordRequirementSpec("accounting_sync", re.compile(r"\b(?:accounting sync|ledger sync|erp sync|gl sync|accounts receivable sync)\b", re.I), ("accounting sync",), {"accounting sync": re.compile(r"\b(?:erp|ledger|gl|journal|accounts receivable|sync job|posting)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:cash application|apply cash|payment application|accounts receivable cash|remittance matching)\b", re.I)
_STRUCTURED = re.compile(r"(?:cash|payment|finance|accounting|bank|remittance|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:cash application|apply cash|payment application)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:cash application|apply cash|payment application)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_matching_rules": ("matching rule",), "missing_remittance_sources": ("remittance source",), "missing_exception_ownership": ("exception owner",), "missing_reconciliation": ("reconciliation detail",), "missing_accounting_sync": ("accounting sync",)}


def build_source_cash_application_requirements(source: Any) -> SourceCashApplicationRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Cash Application Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_cash_application_requirements(source: Any) -> SourceCashApplicationRequirementsReport:
    return build_source_cash_application_requirements(source)


def generate_source_cash_application_requirements(source: Any) -> SourceCashApplicationRequirementsReport:
    return build_source_cash_application_requirements(source)


def derive_source_cash_application_requirements(source: Any) -> SourceCashApplicationRequirementsReport:
    return build_source_cash_application_requirements(source)


def summarize_source_cash_application_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceCashApplicationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_cash_application_requirements(source_or_result).summary


def source_cash_application_requirements_to_dict(report: SourceCashApplicationRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_cash_application_requirements_to_dict.__test__ = False


def source_cash_application_requirements_to_dicts(requirements: SourceCashApplicationRequirementsReport | list[SourceCashApplicationRequirement] | tuple[SourceCashApplicationRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceCashApplicationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_cash_application_requirements_to_dicts.__test__ = False


def source_cash_application_requirements_to_markdown(report: SourceCashApplicationRequirementsReport) -> str:
    return report.to_markdown()


source_cash_application_requirements_to_markdown.__test__ = False
