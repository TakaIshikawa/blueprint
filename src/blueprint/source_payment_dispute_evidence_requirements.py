"""Extract source-level payment dispute evidence requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import (
    KeywordRequirement as SourcePaymentDisputeEvidenceRequirement,
    KeywordRequirementSpec,
    KeywordRequirementsReport as SourcePaymentDisputeEvidenceRequirementsReport,
    build_keyword_requirements_report,
)


_SPECS = (
    KeywordRequirementSpec("evidence_collection", re.compile(r"\b(?:evidence collection|collect evidence|dispute evidence|chargeback evidence|evidence packet|supporting evidence)\b", re.I), ("evidence artifacts",), {"evidence artifacts": re.compile(r"\b(?:receipt|invoice|communication|fulfillment|tracking|screenshot|artifact|packet)\b", re.I)}),
    KeywordRequirementSpec("transaction_timeline", re.compile(r"\b(?:transaction timeline|payment timeline|authorization timeline|capture timeline|dispute timeline|chronology)\b", re.I), ("timeline events",), {"timeline events": re.compile(r"\b(?:authorized|captured|settled|refunded|disputed|submitted|date|timestamp)\b", re.I)}),
    KeywordRequirementSpec("customer_communication", re.compile(r"\b(?:customer communication|customer email|support conversation|chat transcript|message history|communication logs?)\b", re.I), ("communication artifacts",), {"communication artifacts": re.compile(r"\b(?:email|chat|ticket|transcript|message|support)\b", re.I)}),
    KeywordRequirementSpec("receipt_invoice_artifacts", re.compile(r"\b(?:receipt|invoice|order confirmation|billing artifact|payment receipt)\b", re.I), ("receipt or invoice artifacts",), {"receipt or invoice artifacts": re.compile(r"\b(?:receipt|invoice|order confirmation|pdf|artifact)\b", re.I)}),
    KeywordRequirementSpec("fulfillment_proof", re.compile(r"\b(?:fulfillment proof|delivery proof|shipping proof|tracking number|service delivered|usage proof|download proof)\b", re.I), ("fulfillment proof",), {"fulfillment proof": re.compile(r"\b(?:tracking|delivered|signed|usage|download|shipment|carrier)\b", re.I)}),
    KeywordRequirementSpec("processor_submission_deadlines", re.compile(r"\b(?:processor deadlines?|submission deadlines?|representment deadlines?|chargeback deadlines?|respond by|due within)\b", re.I), ("deadline handling",), {"deadline handling": re.compile(r"\b(?:due|sla|\d+\s*(?:business\s+)?(?:day|hour)s?|calendar|business day|reminder)\b", re.I)}),
    KeywordRequirementSpec("representment_status_tracking", re.compile(r"\b(?:representment status|dispute status|chargeback status|processor status|status tracking|case status)\b", re.I), ("status source",), {"status source": re.compile(r"\b(?:processor|stripe|adyen|paypal|status|webhook|sync)\b", re.I)}),
    KeywordRequirementSpec("retention", re.compile(r"\b(?:retention|retain evidence|keep evidence|archive dispute|store dispute)\b", re.I), ("retention policy",), {"retention policy": re.compile(r"\b(?:archive|delete|purge|\d+\s*(?:day|month|year)s?)\b", re.I)}),
    KeywordRequirementSpec("compliance_review", re.compile(r"\b(?:compliance review|legal review|pci review|card network rules|network compliance|regulatory review)\b", re.I), ("compliance review path",), {"compliance review path": re.compile(r"\b(?:legal|pci|card network|regulatory|approval)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:payment dispute|chargeback|representment|processor dispute|card dispute|payment processor|dispute evidence)\b", re.I)
_STRUCTURED = re.compile(r"(?:payment|dispute|chargeback|evidence|processor|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:payment dispute|chargeback|representment|dispute evidence)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:payment dispute|chargeback|representment|dispute evidence)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {
    "missing_evidence_artifacts": ("evidence artifacts", "receipt or invoice artifacts", "fulfillment proof", "communication artifacts"),
    "missing_deadline_handling": ("deadline handling",),
    "missing_retention_compliance_details": ("retention policy", "compliance review path"),
}


def build_source_payment_dispute_evidence_requirements(source: Any) -> SourcePaymentDisputeEvidenceRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Payment Dispute Evidence Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_payment_dispute_evidence_requirements(source: Any) -> SourcePaymentDisputeEvidenceRequirementsReport:
    return build_source_payment_dispute_evidence_requirements(source)


def generate_source_payment_dispute_evidence_requirements(source: Any) -> SourcePaymentDisputeEvidenceRequirementsReport:
    return build_source_payment_dispute_evidence_requirements(source)


def derive_source_payment_dispute_evidence_requirements(source: Any) -> SourcePaymentDisputeEvidenceRequirementsReport:
    return build_source_payment_dispute_evidence_requirements(source)


def summarize_source_payment_dispute_evidence_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourcePaymentDisputeEvidenceRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_payment_dispute_evidence_requirements(source_or_result).summary


def source_payment_dispute_evidence_requirements_to_dict(report: SourcePaymentDisputeEvidenceRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_payment_dispute_evidence_requirements_to_dict.__test__ = False


def source_payment_dispute_evidence_requirements_to_dicts(requirements: SourcePaymentDisputeEvidenceRequirementsReport | list[SourcePaymentDisputeEvidenceRequirement] | tuple[SourcePaymentDisputeEvidenceRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourcePaymentDisputeEvidenceRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_payment_dispute_evidence_requirements_to_dicts.__test__ = False


def source_payment_dispute_evidence_requirements_to_markdown(report: SourcePaymentDisputeEvidenceRequirementsReport) -> str:
    return report.to_markdown()


source_payment_dispute_evidence_requirements_to_markdown.__test__ = False
