"""Extract invoice dispute and billing correction requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


InvoiceDisputeRequirementCategory = Literal[
    "dispute_intake",
    "correction_calculation",
    "approval",
    "customer_notification",
    "accounting_sync",
    "evidence",
    "audit_trail",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[InvoiceDisputeRequirementCategory, ...] = (
    "dispute_intake",
    "correction_calculation",
    "approval",
    "customer_notification",
    "accounting_sync",
    "evidence",
    "audit_trail",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_DOMAIN_CONTEXT_RE = re.compile(
    r"\b(?:invoice disputes?|disputed invoices?|billing disputes?|bill disputes?|"
    r"invoice corrections?|billing corrections?|correct(?:ed|ing)? invoices?|"
    r"credit memos?|debit memos?|invoice adjustments?|billing adjustments?|"
    r"charge reversals?|reverse charges?|overcharges?|undercharges?|duplicate charges?|"
    r"incorrect charges?|wrong invoice|invoice error|billing error|void invoices?)\b",
    re.I,
)
_REFUND_RE = re.compile(r"\b(?:refund|refunds|refunded|refunding)\b", re.I)
_REFUND_CONTEXT_RE = re.compile(
    r"\b(?:invoice disputes?|disputed invoices?|billing disputes?|invoice corrections?|"
    r"billing corrections?|credit memos?|invoice adjustments?|billing adjustments?|"
    r"charge reversals?|invoice error|billing error|overcharges?|duplicate charges?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:dispute|correction|corrective|credit[-_ ]?memo|debit[-_ ]?memo|adjustment|"
    r"charge[-_ ]?reversal|invoice[-_ ]?error|billing[-_ ]?error|accounting|"
    r"ledger|journal|evidence|audit|approval|notification|definition[-_ ]?of[-_ ]?done|"
    r"risks?|architecture|requirements?|acceptance|metadata)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|capture|calculate|approve|notify|sync|post|attach|retain|record|"
    r"track|log|audit|before launch|cannot ship|done when|acceptance)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|exclude|excluding)\s+(?:invoice disputes?|billing disputes?|"
    r"invoice corrections?|billing corrections?|credit memos?|adjustments?|charge reversals?).*?"
    r"\b(?:in scope|required|requirements?|needed|changes?|support(?:ed)?)\b|"
    r"\b(?:invoice disputes?|billing disputes?|invoice corrections?|billing corrections?|"
    r"credit memos?|adjustments?|charge reversals?)\b.*?\b(?:out of scope|not in scope|non[- ]goal)\b",
    re.I,
)
_SPECIFIC_SIGNAL_RE = re.compile(
    r"\b(?:dispute reason|dispute category|disputed amount|invoice id|invoice number|"
    r"credit memos?|debit memos?|adjustment amount|charge reversal|evidence attachments?|"
    r"attachments?|supporting documents?|approval threshold|finance approval|controller approval|"
    r"customer notice|customer notification|accounting sync|erp sync|ledger|journal entries?|"
    r"audit trail|audit log|before and after|correction calculation)\b",
    re.I,
)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
}

_CATEGORY_PATTERNS: dict[InvoiceDisputeRequirementCategory, re.Pattern[str]] = {
    "dispute_intake": re.compile(
        r"\b(?:dispute intake|invoice dispute|billing dispute|disputed invoice|"
        r"dispute reason|dispute category|dispute queue|case intake|intake form|"
        r"invoice id|invoice number|customer claim|overcharge|duplicate charge|incorrect charge)\b",
        re.I,
    ),
    "correction_calculation": re.compile(
        r"\b(?:correction calculation|invoice corrections?|billing corrections?|corrected invoices?|"
        r"credit memos?|debit memos?|invoice adjustments?|billing adjustments?|adjustment amount|"
        r"charge reversal|reverse charge|recalculate|recalculation|prorat(?:e|ion)|"
        r"before and after|delta amount|tax correction|line item correction)\b",
        re.I,
    ),
    "approval": re.compile(
        r"\b(?:approval|approve|approved|approver|finance approval|controller approval|"
        r"manager approval|approval threshold|approval workflow|dual approval|sign[- ]off)\b",
        re.I,
    ),
    "customer_notification": re.compile(
        r"\b(?:customer notification|customer notice|notify customer|email customer|"
        r"send notice|billing contact|customer email|correction notice|dispute status|"
        r"notification template|statement message)\b",
        re.I,
    ),
    "accounting_sync": re.compile(
        r"\b(?:accounting sync|erp sync|quickbooks|netsuite|xero|ledger|general ledger|"
        r"journal entry|journal entries|accounts receivable|a/r|ar aging|revenue recognition|"
        r"accounting export|sync to accounting)\b",
        re.I,
    ),
    "evidence": re.compile(
        r"\b(?:evidence|attachment|attachments|supporting document|supporting documents|"
        r"proof|invoice copy|contract excerpt|usage export|metering export|screenshots?|"
        r"retain evidence|evidence retention)\b",
        re.I,
    ),
    "audit_trail": re.compile(
        r"\b(?:audit trail|audit log|audit history|change history|who approved|who changed|"
        r"timestamp|timestamps|before and after|immutable log|activity log|revision history)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[InvoiceDisputeRequirementCategory, str] = {
    "dispute_intake": "billing_ops",
    "correction_calculation": "billing_engineering",
    "approval": "finance_ops",
    "customer_notification": "billing_ops",
    "accounting_sync": "finance_systems",
    "evidence": "billing_ops",
    "audit_trail": "finance_compliance",
}
_PLANNING_NOTE_BY_CATEGORY: dict[InvoiceDisputeRequirementCategory, str] = {
    "dispute_intake": "Define intake fields, dispute reasons, invoice references, and ownership before task generation.",
    "correction_calculation": "Confirm credit memo, adjustment, reversal, tax, and line-item calculation rules.",
    "approval": "Model approval thresholds, approver roles, and exception handling with finance operations.",
    "customer_notification": "Plan customer-facing correction notices, timing, recipients, and status updates.",
    "accounting_sync": "Coordinate accounting system sync, ledger posting, and receivables reconciliation.",
    "evidence": "Require evidence attachments, retention, and review visibility for dispute decisions.",
    "audit_trail": "Capture immutable audit events for intake, calculation, approval, sync, and notification changes.",
}


@dataclass(frozen=True, slots=True)
class SourceInvoiceDisputeRequirement:
    """One source-backed invoice dispute or billing correction requirement category."""

    category: InvoiceDisputeRequirementCategory
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_fields: tuple[str, ...] = field(default_factory=tuple)
    missing_detail_flags: tuple[str, ...] = field(default_factory=tuple)
    suggested_owner: str = ""
    suggested_planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "source_fields": list(self.source_fields),
            "missing_detail_flags": list(self.missing_detail_flags),
            "suggested_owner": self.suggested_owner,
            "suggested_planning_note": self.suggested_planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceInvoiceDisputeRequirementsReport:
    """Brief-level invoice dispute and billing correction requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceInvoiceDisputeRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceInvoiceDisputeRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [requirement.to_dict() for requirement in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return invoice dispute requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Invoice Dispute Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        owner_counts = self.summary.get("suggested_owner_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Category counts: "
            + ", ".join(
                f"{category} {category_counts.get(category, 0)}"
                for category in _CATEGORY_ORDER
            ),
            "- Suggested owner counts: "
            + (", ".join(f"{owner} {owner_counts[owner]}" for owner in sorted(owner_counts)) or "none"),
        ]
        if not self.requirements:
            lines.extend(["", "No invoice dispute requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Confidence | Source Fields | Missing Details | Evidence | Suggested Owner | Suggested Planning Note |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{requirement.confidence:.2f} | "
                f"{_markdown_cell(', '.join(requirement.source_fields))} | "
                f"{_markdown_cell(', '.join(requirement.missing_detail_flags) or 'none')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{requirement.suggested_owner} | "
                f"{_markdown_cell(requirement.suggested_planning_note)} |"
            )
        return "\n".join(lines)


def build_source_invoice_dispute_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceInvoiceDisputeRequirementsReport:
    """Build an invoice dispute requirements report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceInvoiceDisputeRequirementsReport(
        source_id=source_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def generate_source_invoice_dispute_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceInvoiceDisputeRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_invoice_dispute_requirements(source)


def derive_source_invoice_dispute_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceInvoiceDisputeRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_invoice_dispute_requirements(source)


def extract_source_invoice_dispute_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceInvoiceDisputeRequirement, ...]:
    """Return invoice dispute requirement records extracted from brief-shaped input."""
    return build_source_invoice_dispute_requirements(source).requirements


def summarize_source_invoice_dispute_requirements(
    source_or_result: Mapping[str, Any]
    | SourceBrief
    | ImplementationBrief
    | SourceInvoiceDisputeRequirementsReport
    | str
    | object,
) -> dict[str, Any]:
    """Return the deterministic invoice dispute requirements summary."""
    if isinstance(source_or_result, SourceInvoiceDisputeRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_invoice_dispute_requirements(source_or_result).summary


def source_invoice_dispute_requirements_to_dict(
    report: SourceInvoiceDisputeRequirementsReport,
) -> dict[str, Any]:
    """Serialize an invoice dispute requirements report to a plain dictionary."""
    return report.to_dict()


source_invoice_dispute_requirements_to_dict.__test__ = False


def source_invoice_dispute_requirements_to_dicts(
    requirements: tuple[SourceInvoiceDisputeRequirement, ...]
    | list[SourceInvoiceDisputeRequirement]
    | SourceInvoiceDisputeRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize invoice dispute requirement records to dictionaries."""
    if isinstance(requirements, SourceInvoiceDisputeRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_invoice_dispute_requirements_to_dicts.__test__ = False


def source_invoice_dispute_requirements_to_markdown(
    report: SourceInvoiceDisputeRequirementsReport,
) -> str:
    """Render an invoice dispute requirements report as Markdown."""
    return report.to_markdown()


source_invoice_dispute_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: InvoiceDisputeRequirementCategory
    confidence: float
    evidence: str
    source_field: str
    text: str


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _brief_id(payload), payload
    if isinstance(source, str):
        return None, {"body": source}
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _brief_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _brief_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _brief_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _brief_id(payload), payload
    return None, {}


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        if not _is_requirement(segment.text, segment.source_field, segment.section_context):
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = [
            category
            for category in _CATEGORY_ORDER
            if _CATEGORY_PATTERNS[category].search(searchable)
        ]
        if not categories:
            continue
        confidence = _confidence(segment.text, segment.source_field, segment.section_context)
        evidence = _evidence_snippet(segment.source_field, segment.text)
        for category in categories:
            candidates.append(
                _Candidate(
                    category=category,
                    confidence=confidence,
                    evidence=evidence,
                    source_field=segment.source_field,
                    text=segment.text,
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceInvoiceDisputeRequirement]:
    by_category: dict[InvoiceDisputeRequirementCategory, list[_Candidate]] = {}
    for candidate in candidates:
        by_category.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceInvoiceDisputeRequirement] = []
    for category in _CATEGORY_ORDER:
        items = by_category.get(category, [])
        if not items:
            continue
        evidence = tuple(_dedupe_evidence(item.evidence for item in items))[:5]
        source_fields = tuple(_dedupe(item.source_field for item in items))[:5]
        text = " ".join(item.text for item in items)
        requirements.append(
            SourceInvoiceDisputeRequirement(
                category=category,
                confidence=round(max(item.confidence for item in items), 2),
                evidence=evidence,
                source_fields=source_fields,
                missing_detail_flags=tuple(_missing_detail_flags(category, text)),
                suggested_owner=_OWNER_BY_CATEGORY[category],
                suggested_planning_note=_PLANNING_NOTE_BY_CATEGORY[category],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            -requirement.confidence,
            _CATEGORY_ORDER.index(requirement.category),
            requirement.source_fields,
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "problem_statement",
        "mvp_goal",
        "workflow_context",
        "requirements",
        "constraints",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "billing",
        "invoice",
        "invoices",
        "disputes",
        "corrections",
        "adjustments",
        "accounting",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(
    segments: list[_Segment],
    source_field: str,
    value: Any,
    section_context: bool,
) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _DOMAIN_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text, segment_context in _segments(text, field_context):
            segments.append(_Segment(source_field, segment_text, segment_context))


def _segments(value: str, inherited_context: bool) -> list[tuple[str, bool]]:
    segments: list[tuple[str, bool]] = []
    section_context = inherited_context
    for raw_line in value.splitlines() or [value]:
        line = raw_line.strip()
        if not line:
            continue
        heading = _HEADING_RE.match(line)
        if heading:
            title = _clean_text(heading.group("title"))
            section_context = inherited_context or bool(
                _DOMAIN_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(text: str, source_field: str, section_context: bool) -> bool:
    if _NEGATED_SCOPE_RE.search(text):
        return False
    if _REFUND_RE.search(text) and not _REFUND_CONTEXT_RE.search(text):
        return False
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    domain_context = bool(_DOMAIN_CONTEXT_RE.search(text))
    if not (domain_context or field_context or section_context):
        return False
    if _REFUND_RE.search(text) and not domain_context:
        return False
    if (field_context or section_context) and (domain_context or _SPECIFIC_SIGNAL_RE.search(text)):
        return True
    if _REQUIREMENT_RE.search(text) and (domain_context or _SPECIFIC_SIGNAL_RE.search(text)):
        return True
    if domain_context and _SPECIFIC_SIGNAL_RE.search(text):
        return True
    return False


def _confidence(text: str, source_field: str, section_context: bool) -> float:
    score = 0.62
    if _STRUCTURED_FIELD_RE.search(_field_words(source_field)):
        score += 0.1
    if section_context or _DOMAIN_CONTEXT_RE.search(text):
        score += 0.08
    if _REQUIREMENT_RE.search(text):
        score += 0.08
    if _SPECIFIC_SIGNAL_RE.search(text):
        score += 0.05
    if re.search(r"\b(?:credit memo|charge reversal|approval threshold|journal entry|audit trail)\b", text, re.I):
        score += 0.04
    return round(min(score, 0.95), 2)


def _missing_detail_flags(
    category: InvoiceDisputeRequirementCategory,
    text: str,
) -> list[str]:
    rules: dict[InvoiceDisputeRequirementCategory, tuple[tuple[str, re.Pattern[str]], ...]] = {
        "dispute_intake": (
            ("missing_invoice_reference", re.compile(r"\b(?:invoice id|invoice number|invoice reference)\b", re.I)),
            ("missing_dispute_reason", re.compile(r"\b(?:reason|category|overcharge|duplicate|incorrect|tax error|usage error)\b", re.I)),
        ),
        "correction_calculation": (
            ("missing_calculation_basis", re.compile(r"\b(?:amount|percent|percentage|delta|line item|tax|prorat|before and after|formula)\b", re.I)),
        ),
        "approval": (
            ("missing_approver", re.compile(r"\b(?:approver|finance|controller|manager|role|owner)\b", re.I)),
            ("missing_approval_threshold", re.compile(r"\b(?:threshold|over|above|greater than|amount|limit|\$|usd|percent)\b", re.I)),
        ),
        "customer_notification": (
            ("missing_notification_channel", re.compile(r"\b(?:emails?|portal|in-app|webhook|statement|billing contact)\b", re.I)),
            ("missing_notification_timing", re.compile(r"\b(?:before|after|when|within|immediately|daily|status)\b", re.I)),
        ),
        "accounting_sync": (
            ("missing_accounting_system", re.compile(r"\b(?:netsuite|quickbooks|xero|erp|accounting system)\b", re.I)),
            ("missing_posting_mapping", re.compile(r"\b(?:ledger|journal|account|mapping|a/r|accounts receivable|revenue recognition)\b", re.I)),
        ),
        "evidence": (
            ("missing_evidence_type", re.compile(r"\b(?:contract|usage export|metering export|invoice copy|screenshot|attachment|document)\b", re.I)),
            ("missing_retention_rule", re.compile(r"\b(?:retain|retention|days|months|years|archive)\b", re.I)),
        ),
        "audit_trail": (
            ("missing_audit_actor", re.compile(r"\b(?:who|actor|user|approver|changed by)\b", re.I)),
            ("missing_audit_timestamp", re.compile(r"\b(?:timestamp|time|date|when)\b", re.I)),
        ),
    }
    return [flag for flag, pattern in rules[category] if not pattern.search(text)]


def _summary(requirements: tuple[SourceInvoiceDisputeRequirement, ...]) -> dict[str, Any]:
    missing_flags = tuple(
        _dedupe(flag for requirement in requirements for flag in requirement.missing_detail_flags)
    )
    return {
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "high_confidence_count": sum(
            1 for requirement in requirements if requirement.confidence >= 0.85
        ),
        "categories": [requirement.category for requirement in requirements],
        "missing_detail_flags": list(missing_flags),
        "suggested_owner_counts": {
            owner: sum(1 for requirement in requirements if requirement.suggested_owner == owner)
            for owner in sorted({requirement.suggested_owner for requirement in requirements})
        },
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "summary",
        "body",
        "description",
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "requirements",
        "constraints",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "billing",
        "invoice",
        "invoices",
        "disputes",
        "corrections",
        "adjustments",
        "accounting",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = str(value).strip()
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        _, _, statement = value.partition(": ")
        key = _clean_text(statement or value).casefold().rstrip(".")
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "InvoiceDisputeRequirementCategory",
    "SourceInvoiceDisputeRequirement",
    "SourceInvoiceDisputeRequirementsReport",
    "build_source_invoice_dispute_requirements",
    "derive_source_invoice_dispute_requirements",
    "extract_source_invoice_dispute_requirements",
    "generate_source_invoice_dispute_requirements",
    "summarize_source_invoice_dispute_requirements",
    "source_invoice_dispute_requirements_to_dict",
    "source_invoice_dispute_requirements_to_dicts",
    "source_invoice_dispute_requirements_to_markdown",
]
