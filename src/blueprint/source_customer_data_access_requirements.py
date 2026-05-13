"""Extract source-level customer data access requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal

from blueprint._source_requirement_utils import dedupe, evidence_snippet, markdown_cell, segments, source_payloads

CustomerDataAccessRequirementType = Literal["dsar_request", "identity_verification", "access_scope", "delivery_format", "fulfillment_sla", "audit_evidence", "denial_escalation"]
CustomerDataAccessConfidence = Literal["high", "medium", "low"]

_TYPE_ORDER: tuple[CustomerDataAccessRequirementType, ...] = ("dsar_request", "identity_verification", "access_scope", "delivery_format", "fulfillment_sla", "audit_evidence", "denial_escalation")
_CONFIDENCE_ORDER = {"high": 0, "medium": 1, "low": 2}
_LABELS = {
    "dsar_request": "Data subject access request",
    "identity_verification": "Identity verification",
    "access_scope": "Access scope",
    "delivery_format": "Delivery format",
    "fulfillment_sla": "Fulfillment SLA",
    "audit_evidence": "Audit evidence",
    "denial_escalation": "Denial and escalation handling",
}
_MISSING = {
    "dsar_request": ("intake_channel", "request_owner", "customer_regions"),
    "identity_verification": ("verification_method", "identity_evidence", "fallback_review"),
    "access_scope": ("data_categories", "systems", "exclusions"),
    "delivery_format": ("format", "secure_delivery", "retention"),
    "fulfillment_sla": ("sla", "clock_start", "breach_owner"),
    "audit_evidence": ("evidence_schema", "retention", "review_owner"),
    "denial_escalation": ("denial_reasons", "appeal_path", "escalation_owner"),
}
_PATTERNS: dict[CustomerDataAccessRequirementType, re.Pattern[str]] = {
    "dsar_request": re.compile(r"\b(?:dsar|data subject access request|customer data access request|access request|right of access)\b", re.I),
    "identity_verification": re.compile(r"\b(?:identity verification|verify identity|authenticate requester|proof of identity|id verification)\b", re.I),
    "access_scope": re.compile(r"\b(?:access scope|data scope|scope of access|data categories|personal data export|customer records?)\b", re.I),
    "delivery_format": re.compile(r"\b(?:delivery format|export format|machine[- ]readable|csv|json|pdf|secure download|encrypted delivery)\b", re.I),
    "fulfillment_sla": re.compile(r"\b(?:fulfillment sla|response sla|within \d+ days?|30 days|45 days|deadline|due date)\b", re.I),
    "audit_evidence": re.compile(r"\b(?:audit evidence|audit trail|request log|evidence|case history|retain records?|compliance log)\b", re.I),
    "denial_escalation": re.compile(r"\b(?:deny|denial|reject request|refuse|appeal|escalation|legal review|privacy escalation)\b", re.I),
}
_DETAILS = {
    "intake_channel": re.compile(r"\b(?:portal|email|form|support ticket|intake|channel)\b", re.I),
    "request_owner": re.compile(r"\b(?:owner|privacy team|support|legal|compliance)\b", re.I),
    "customer_regions": re.compile(r"\b(?:gdpr|ccpa|cpra|region|country|eu|uk|california)\b", re.I),
    "verification_method": re.compile(r"\b(?:verify|verification|mfa|email challenge|id check|authenticate)\b", re.I),
    "identity_evidence": re.compile(r"\b(?:id|evidence|proof|document|account match)\b", re.I),
    "fallback_review": re.compile(r"\b(?:manual review|fallback|escalate|exception)\b", re.I),
    "data_categories": re.compile(r"\b(?:profile|billing|usage|activity|messages|files|personal data|categories)\b", re.I),
    "systems": re.compile(r"\b(?:systems?|crm|warehouse|database|billing|support|logs)\b", re.I),
    "exclusions": re.compile(r"\b(?:exclude|redact|third party|security logs|legal hold|exclusion)\b", re.I),
    "format": re.compile(r"\b(?:csv|json|pdf|zip|machine-readable)\b", re.I),
    "secure_delivery": re.compile(r"\b(?:secure download|encrypted|signed url|portal|password)\b", re.I),
    "retention": re.compile(r"\b(?:retain|retention|expire|ttl|delete after)\b", re.I),
    "sla": re.compile(r"\b(?:sla|within \d+ days?|30 days|45 days|deadline)\b", re.I),
    "clock_start": re.compile(r"\b(?:clock starts?|received|verified|intake date)\b", re.I),
    "breach_owner": re.compile(r"\b(?:breach owner|owner|privacy|legal|support)\b", re.I),
    "evidence_schema": re.compile(r"\b(?:schema|fields|ticket|case|log|evidence)\b", re.I),
    "review_owner": re.compile(r"\b(?:review owner|privacy|legal|compliance|owner)\b", re.I),
    "denial_reasons": re.compile(r"\b(?:reason|deny|denial|reject|fraud|cannot verify|legal)\b", re.I),
    "appeal_path": re.compile(r"\b(?:appeal|contest|reopen|customer reply)\b", re.I),
    "escalation_owner": re.compile(r"\b(?:escalation owner|privacy|legal|dpo|compliance)\b", re.I),
}
_CONTEXT_RE = re.compile(r"\b(?:dsar|data subject access|customer data access|right of access|personal data export|access request)\b", re.I)
_REQ_RE = re.compile(r"\b(?:must|shall|required|requires?|need(?:ed|s)?|should|support|define|verify|deliver|fulfill|audit|deny|escalate)\b", re.I)
_NEGATED_RE = re.compile(r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}\b(?:dsar|customer data access|data subject access|access request)\b.{0,120}\b(?:required|needed|planned|in scope|work|support)\b|\b(?:dsar|customer data access|data subject access|access request)\b.{0,120}\b(?:not required|not needed|out of scope|no work|no support)\b", re.I)
_SCANNED_FIELDS = ("title", "summary", "body", "description", "requirements", "scope", "acceptance_criteria", "definition_of_done", "privacy", "compliance", "metadata", "source_payload")


@dataclass(frozen=True, slots=True)
class SourceCustomerDataAccessRequirement:
    source_brief_id: str | None
    requirement_type: CustomerDataAccessRequirementType
    requirement_text: str
    label: str
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    confidence: CustomerDataAccessConfidence = "medium"

    @property
    def category(self) -> CustomerDataAccessRequirementType:
        return self.requirement_type

    @property
    def requirement_category(self) -> CustomerDataAccessRequirementType:
        return self.requirement_type

    @property
    def missing_detail_guidance(self) -> str | None:
        return "; ".join(self.missing_details) if self.missing_details else None

    def to_dict(self) -> dict[str, Any]:
        return {"source_brief_id": self.source_brief_id, "requirement_type": self.requirement_type, "requirement_category": self.requirement_category, "requirement_text": self.requirement_text, "label": self.label, "source_field": self.source_field, "evidence": list(self.evidence), "missing_details": list(self.missing_details), "missing_detail_guidance": self.missing_detail_guidance, "confidence": self.confidence}


@dataclass(frozen=True, slots=True)
class SourceCustomerDataAccessRequirementsReport:
    source_id: str | None = None
    requirements: tuple[SourceCustomerDataAccessRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceCustomerDataAccessRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceCustomerDataAccessRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {"source_id": self.source_id, "requirements": [item.to_dict() for item in self.requirements], "summary": dict(self.summary), "records": [item.to_dict() for item in self.records], "findings": [item.to_dict() for item in self.findings]}

    def to_dicts(self) -> list[dict[str, Any]]:
        return [item.to_dict() for item in self.requirements]

    def to_markdown(self) -> str:
        lines = [f"# Source Customer Data Access Requirements{': ' + self.source_id if self.source_id else ''}", "", f"Requirements found: {self.summary.get('requirement_count', 0)}"]
        if not self.requirements:
            return "\n".join([*lines, "", "No customer data access requirements were inferred."])
        lines.extend(["", "| Type | Requirement | Missing Details | Evidence |", "| --- | --- | --- | --- |"])
        for item in self.requirements:
            lines.append(f"| {markdown_cell(item.requirement_type)} | {markdown_cell(item.requirement_text)} | {markdown_cell('; '.join(item.missing_details))} | {markdown_cell('; '.join(item.evidence))} |")
        return "\n".join(lines)


def build_source_customer_data_access_requirements(source: Any) -> SourceCustomerDataAccessRequirementsReport:
    payloads = source_payloads(source)
    records = tuple(_merge(_candidates(payloads)))
    ids = dedupe(source_id for source_id, _ in payloads if source_id)
    return SourceCustomerDataAccessRequirementsReport(ids[0] if len(ids) == 1 else None, records, _summary(records, len(payloads)))


extract_source_customer_data_access_requirements = build_source_customer_data_access_requirements
generate_source_customer_data_access_requirements = build_source_customer_data_access_requirements
derive_source_customer_data_access_requirements = build_source_customer_data_access_requirements


def summarize_source_customer_data_access_requirements(source_or_report: Any) -> dict[str, Any]:
    if isinstance(source_or_report, SourceCustomerDataAccessRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_customer_data_access_requirements(source_or_report).summary


def source_customer_data_access_requirements_to_dict(report: SourceCustomerDataAccessRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_customer_data_access_requirements_to_dict.__test__ = False


def source_customer_data_access_requirements_to_dicts(items: SourceCustomerDataAccessRequirementsReport | Iterable[SourceCustomerDataAccessRequirement]) -> list[dict[str, Any]]:
    if isinstance(items, SourceCustomerDataAccessRequirementsReport):
        return items.to_dicts()
    return [item.to_dict() for item in items]


source_customer_data_access_requirements_to_dicts.__test__ = False


def source_customer_data_access_requirements_to_markdown(report: SourceCustomerDataAccessRequirementsReport) -> str:
    return report.to_markdown()


source_customer_data_access_requirements_to_markdown.__test__ = False


def _candidates(payloads: Iterable[tuple[str | None, dict[str, Any]]]) -> list[SourceCustomerDataAccessRequirement]:
    out: list[SourceCustomerDataAccessRequirement] = []
    for source_id, payload in payloads:
        for field_name, text in segments(payload, _SCANNED_FIELDS):
            searchable = f"{field_name} {text}"
            if _NEGATED_RE.search(searchable) or not _REQ_RE.search(text):
                continue
            if not _CONTEXT_RE.search(searchable) and not any(pattern.search(searchable) for pattern in _PATTERNS.values()):
                continue
            for requirement_type, pattern in _PATTERNS.items():
                if pattern.search(searchable):
                    missing = tuple(detail for detail in _MISSING[requirement_type] if not _DETAILS[detail].search(searchable))
                    out.append(SourceCustomerDataAccessRequirement(source_id, requirement_type, text, _LABELS[requirement_type], field_name, (evidence_snippet(field_name, text),), missing, "high"))
    return out


def _merge(candidates: Iterable[SourceCustomerDataAccessRequirement]) -> list[SourceCustomerDataAccessRequirement]:
    grouped: dict[CustomerDataAccessRequirementType, list[SourceCustomerDataAccessRequirement]] = {}
    for item in candidates:
        grouped.setdefault(item.requirement_type, []).append(item)
    records: list[SourceCustomerDataAccessRequirement] = []
    for requirement_type in _TYPE_ORDER:
        items = grouped.get(requirement_type, [])
        if items:
            best = min(items, key=lambda item: (len(item.missing_details), item.source_field or ""))
            missing = tuple(detail for detail in _MISSING[requirement_type] if all(detail in item.missing_details for item in items))
            records.append(SourceCustomerDataAccessRequirement(best.source_brief_id, requirement_type, best.requirement_text, best.label, best.source_field, tuple(dedupe(ev for item in items for ev in item.evidence))[:5], missing, "high"))
    return records


def _summary(records: tuple[SourceCustomerDataAccessRequirement, ...], source_count: int) -> dict[str, Any]:
    counts = {item: sum(1 for record in records if record.requirement_type == item) for item in _TYPE_ORDER}
    return {"source_count": source_count, "requirement_count": len(records), "requirement_type_counts": counts, "confidence_counts": {level: sum(1 for record in records if record.confidence == level) for level in _CONFIDENCE_ORDER}, "missing_detail_count": sum(len(record.missing_details) for record in records), "requirement_types": [item for item in _TYPE_ORDER if counts[item]]}


__all__ = [name for name in globals() if name.startswith(("SourceCustomerDataAccess", "build_source_customer", "extract_source_customer", "generate_source_customer", "derive_source_customer", "summarize_source_customer", "source_customer"))] + ["CustomerDataAccessRequirementType", "CustomerDataAccessConfidence"]
