"""Extract source-level credit note requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceCreditNoteRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceCreditNoteRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("credit_note_reason", re.compile(r"\b(?:credit note reason|credit reason|reason code|memo reason)\b", re.I), ("reason detail",), {"reason detail": re.compile(r"\b(?:refund|discount|service issue|billing error|reason code|category)\b", re.I)}),
    KeywordRequirementSpec("amount_calculation", re.compile(r"\b(?:amount calculation|credit amount|calculate credit|amount rules?|prorated credit)\b", re.I), ("calculation detail",), {"calculation detail": re.compile(r"\b(?:formula|prorate|line item|percentage|tax inclusive|amount from)\b", re.I)}),
    KeywordRequirementSpec("approval_threshold", re.compile(r"\b(?:approval threshold|approval rule|credit approval|manager approval)\b", re.I), ("approval detail",), {"approval detail": re.compile(r"\b(?:manager|threshold of|\$|over \d+|approval role|two approvers)\b", re.I)}),
    KeywordRequirementSpec("invoice_linkage", re.compile(r"\b(?:invoice linkage|linked invoice|invoice link|original invoice|invoice association)\b", re.I), ("invoice linkage",), {"invoice linkage": re.compile(r"\b(?:invoice id|original invoice|line item|apply to invoice|association)\b", re.I)}),
    KeywordRequirementSpec("customer_delivery", re.compile(r"\b(?:customer delivery|deliver credit note|customer email|send credit note|customer portal)\b", re.I), ("delivery detail",), {"delivery detail": re.compile(r"\b(?:email|portal|pdf|download|notification|template)\b", re.I)}),
    KeywordRequirementSpec("accounting_posting", re.compile(r"\b(?:accounting posting|ledger posting|general ledger|accounting entry|erp posting)\b", re.I), ("accounting detail",), {"accounting detail": re.compile(r"\b(?:ledger|gl account|erp|journal|revenue account|accounts receivable)\b", re.I)}),
    KeywordRequirementSpec("tax_treatment", re.compile(r"\b(?:tax treatment|tax adjustment|vat adjustment|sales tax credit|tax reversal)\b", re.I), ("tax detail",), {"tax detail": re.compile(r"\b(?:vat|sales tax|tax rate|jurisdiction|reverse tax|tax inclusive)\b", re.I)}),
    KeywordRequirementSpec("audit_evidence", re.compile(r"\b(?:audit evidence|credit note audit|approval evidence|evidence trail|audit packet)\b", re.I), ("audit evidence",), {"audit evidence": re.compile(r"\b(?:approver|timestamp|before after|attachment|audit log|evidence)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:credit note|credit memo|customer credit|invoice credit)\b", re.I)
_STRUCTURED = re.compile(r"(?:credit|invoice|accounting|tax|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:credit note|credit memo|customer credit)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:credit note|credit memo|customer credit)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_reason": ("reason detail",), "missing_calculation": ("calculation detail",), "missing_approval": ("approval detail",), "missing_invoice_linkage": ("invoice linkage",), "missing_delivery": ("delivery detail",), "missing_accounting": ("accounting detail",), "missing_tax_treatment": ("tax detail",)}


def build_source_credit_note_requirements(source: Any) -> SourceCreditNoteRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Credit Note Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_credit_note_requirements(source: Any) -> SourceCreditNoteRequirementsReport:
    return build_source_credit_note_requirements(source)


def generate_source_credit_note_requirements(source: Any) -> SourceCreditNoteRequirementsReport:
    return build_source_credit_note_requirements(source)


def derive_source_credit_note_requirements(source: Any) -> SourceCreditNoteRequirementsReport:
    return build_source_credit_note_requirements(source)


def summarize_source_credit_note_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceCreditNoteRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_credit_note_requirements(source_or_result).summary


def source_credit_note_requirements_to_dict(report: SourceCreditNoteRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_credit_note_requirements_to_dict.__test__ = False


def source_credit_note_requirements_to_dicts(requirements: SourceCreditNoteRequirementsReport | list[SourceCreditNoteRequirement] | tuple[SourceCreditNoteRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceCreditNoteRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_credit_note_requirements_to_dicts.__test__ = False


def source_credit_note_requirements_to_markdown(report: SourceCreditNoteRequirementsReport) -> str:
    return report.to_markdown()


source_credit_note_requirements_to_markdown.__test__ = False
