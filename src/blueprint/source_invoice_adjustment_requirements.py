"""Extract invoice adjustment and credit memo requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


InvoiceAdjustmentRequirementCategory = Literal[
    "adjustment_authorization",
    "credit_memo",
    "tax_recalculation",
    "audit_trail",
    "customer_notification",
    "accounting_export",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[InvoiceAdjustmentRequirementCategory, ...] = (
    "adjustment_authorization",
    "credit_memo",
    "tax_recalculation",
    "audit_trail",
    "customer_notification",
    "accounting_export",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_DOMAIN_CONTEXT_RE = re.compile(
    r"\b(?:invoice adjustments?|billing adjustments?|adjusted invoices?|invoice corrections?|"
    r"credit memos?|credit notes?|debit memos?|debit notes?|post[- ]?invoice changes?|"
    r"invoice line adjustments?|manual adjustments?|tax adjustments?|vat adjustments?|"
    r"tax recalculations?|recalculate tax|billing corrections?|customer credits?|"
    r"accounting adjustments?)\b",
    re.I,
)
_REFUND_RE = re.compile(r"\b(?:refund|refunds|refunded|refunding)\b", re.I)
_REFUND_CONTEXT_RE = re.compile(
    r"\b(?:invoice adjustments?|billing adjustments?|credit memos?|credit notes?|"
    r"invoice corrections?|tax adjustments?|customer credits?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:invoice|billing|adjustment|credit[-_ ]?memo|credit[-_ ]?note|debit[-_ ]?memo|"
    r"tax|vat|gst|sales[-_ ]?tax|authorization|approval|approver|audit|history|"
    r"notification|customer|accounting|ledger|journal|export|erp|requirements?|"
    r"acceptance|definition[-_ ]?of[-_ ]?done|metadata|source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|capture|calculate|recalculate|approve|authorize|notify|send|export|"
    r"sync|post|record|track|log|audit|retain|before launch|cannot ship|done when|"
    r"acceptance|create|issue|generate)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|exclude|excluding)\s+(?:invoice adjustments?|billing adjustments?|"
    r"credit memos?|credit notes?|tax recalculation|customer notifications?|accounting exports?).*?"
    r"\b(?:in scope|required|requirements?|needed|changes?|support(?:ed)?)\b|"
    r"\b(?:invoice adjustments?|billing adjustments?|credit memos?|credit notes?|"
    r"tax recalculation|customer notifications?|accounting exports?)\b.*?"
    r"\b(?:out of scope|not in scope|non[- ]goal|not required|not needed)\b",
    re.I,
)
_SPECIFIC_SIGNAL_RE = re.compile(
    r"\b(?:adjustment reason|adjustment amount|manual adjustment|post[- ]?invoice|"
    r"credit memos?|credit notes?|original invoice|authorization|approval threshold|"
    r"finance approval|controller approval|tax recalculation|recalculate tax|vat|gst|"
    r"sales tax|tax delta|audit trail|audit log|before and after|who changed|timestamp|"
    r"customer notice|customer notification|billing contact|accounting export|erp export|"
    r"ledger export|journal entries?|general ledger|netsuite|quickbooks|xero|a/r|"
    r"accounts receivable|revenue recognition)\b",
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

_CATEGORY_PATTERNS: dict[InvoiceAdjustmentRequirementCategory, re.Pattern[str]] = {
    "adjustment_authorization": re.compile(
        r"\b(?:authorization|authorize|authorized|approval|approve|approved|approver|"
        r"finance approval|controller approval|manager approval|approval threshold|"
        r"approval workflow|dual approval|sign[- ]off|maker[- ]checker|permission|"
        r"role(?:s)? allowed|adjustment limit)\b",
        re.I,
    ),
    "credit_memo": re.compile(
        r"\b(?:credit memos?|credit notes?|debit memos?|debit notes?|customer credits?|"
        r"issue credit|credit document|negative invoice|original invoice reference|"
        r"reference original invoice|memo number|credit amount)\b",
        re.I,
    ),
    "tax_recalculation": re.compile(
        r"\b(?:tax recalculation|recalculate tax|tax correction|tax adjustment|tax delta|"
        r"vat recalculation|gst recalculation|sales tax recalculation|tax rate|tax basis|"
        r"taxable amount|jurisdiction(?:al)? tax|inclusive tax|exclusive tax)\b",
        re.I,
    ),
    "audit_trail": re.compile(
        r"\b(?:audit trail|audit log|audit history|change history|revision history|"
        r"activity log|immutable log|before and after|who approved|who changed|"
        r"changed by|timestamp|timestamps|reason code|adjustment reason)\b",
        re.I,
    ),
    "customer_notification": re.compile(
        r"\b(?:customer notification|customer notice|notify customer|email customer|"
        r"send notice|billing contact|customer email|adjustment notice|credit memo email|"
        r"statement message|portal notification)\b",
        re.I,
    ),
    "accounting_export": re.compile(
        r"\b(?:accounting export|erp export|ledger export|accounting sync|erp sync|"
        r"quickbooks|netsuite|xero|general ledger|ledger|journal entry|journal entries|"
        r"accounts receivable|a/r|ar aging|revenue recognition|export impact|"
        r"sync to accounting|post to accounting)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[InvoiceAdjustmentRequirementCategory, str] = {
    "adjustment_authorization": "finance_ops",
    "credit_memo": "billing_engineering",
    "tax_recalculation": "tax_compliance",
    "audit_trail": "finance_compliance",
    "customer_notification": "billing_ops",
    "accounting_export": "finance_systems",
}
_PLANNING_NOTE_BY_CATEGORY: dict[InvoiceAdjustmentRequirementCategory, str] = {
    "adjustment_authorization": "Define who can authorize invoice adjustments, thresholds, role checks, and exception handling.",
    "credit_memo": "Confirm credit memo creation, original invoice references, document numbering, and posting states.",
    "tax_recalculation": "Model tax recalculation rules for adjusted lines, jurisdictions, rates, and rounding deltas.",
    "audit_trail": "Capture immutable adjustment audit events with actor, reason, timestamps, and before/after amounts.",
    "customer_notification": "Plan customer notices for invoice adjustments, recipients, timing, templates, and delivery state.",
    "accounting_export": "Coordinate accounting export impact, ledger mappings, journal entries, and receivables reconciliation.",
}


@dataclass(frozen=True, slots=True)
class SourceInvoiceAdjustmentRequirement:
    """One source-backed invoice adjustment or credit memo requirement category."""

    category: InvoiceAdjustmentRequirementCategory
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
class SourceInvoiceAdjustmentRequirementsReport:
    """Brief-level invoice adjustment and credit memo requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceInvoiceAdjustmentRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceInvoiceAdjustmentRequirement, ...]:
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
        """Return invoice adjustment requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Invoice Adjustment Requirements Report"
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
            lines.extend(["", "No invoice adjustment requirements were found in the source brief."])
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


def build_source_invoice_adjustment_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceInvoiceAdjustmentRequirementsReport:
    """Build an invoice adjustment requirements report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceInvoiceAdjustmentRequirementsReport(
        source_id=source_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def generate_source_invoice_adjustment_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceInvoiceAdjustmentRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_invoice_adjustment_requirements(source)


def derive_source_invoice_adjustment_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceInvoiceAdjustmentRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_invoice_adjustment_requirements(source)


def extract_source_invoice_adjustment_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceInvoiceAdjustmentRequirement, ...]:
    """Return invoice adjustment requirement records extracted from brief-shaped input."""
    return build_source_invoice_adjustment_requirements(source).requirements


def summarize_source_invoice_adjustment_requirements(
    source_or_result: Mapping[str, Any]
    | SourceBrief
    | ImplementationBrief
    | SourceInvoiceAdjustmentRequirementsReport
    | str
    | object,
) -> dict[str, Any]:
    """Return the deterministic invoice adjustment requirements summary."""
    if isinstance(source_or_result, SourceInvoiceAdjustmentRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_invoice_adjustment_requirements(source_or_result).summary


def source_invoice_adjustment_requirements_to_dict(
    report: SourceInvoiceAdjustmentRequirementsReport,
) -> dict[str, Any]:
    """Serialize an invoice adjustment requirements report to a plain dictionary."""
    return report.to_dict()


source_invoice_adjustment_requirements_to_dict.__test__ = False


def source_invoice_adjustment_requirements_to_dicts(
    requirements: tuple[SourceInvoiceAdjustmentRequirement, ...]
    | list[SourceInvoiceAdjustmentRequirement]
    | SourceInvoiceAdjustmentRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize invoice adjustment requirement records to dictionaries."""
    if isinstance(requirements, SourceInvoiceAdjustmentRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_invoice_adjustment_requirements_to_dicts.__test__ = False


def source_invoice_adjustment_requirements_to_markdown(
    report: SourceInvoiceAdjustmentRequirementsReport,
) -> str:
    """Render an invoice adjustment requirements report as Markdown."""
    return report.to_markdown()


source_invoice_adjustment_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: InvoiceAdjustmentRequirementCategory
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


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceInvoiceAdjustmentRequirement]:
    by_category: dict[InvoiceAdjustmentRequirementCategory, list[_Candidate]] = {}
    for candidate in candidates:
        by_category.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceInvoiceAdjustmentRequirement] = []
    for category in _CATEGORY_ORDER:
        items = by_category.get(category, [])
        if not items:
            continue
        evidence = tuple(_dedupe_evidence(item.evidence for item in items))[:5]
        source_fields = tuple(_dedupe(item.source_field for item in items))[:5]
        text = " ".join(item.text for item in items)
        requirements.append(
            SourceInvoiceAdjustmentRequirement(
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
        "adjustments",
        "credit_memos",
        "tax",
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
        if not cleaned or _NEGATED_SCOPE_RE.search(cleaned):
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text and not _NEGATED_SCOPE_RE.search(text):
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
    if re.search(r"\b(?:credit memo|tax recalculation|approval threshold|accounting export|audit trail)\b", text, re.I):
        score += 0.04
    return round(min(score, 0.95), 2)


def _missing_detail_flags(
    category: InvoiceAdjustmentRequirementCategory,
    text: str,
) -> list[str]:
    rules: dict[InvoiceAdjustmentRequirementCategory, tuple[tuple[str, re.Pattern[str]], ...]] = {
        "adjustment_authorization": (
            ("missing_authorizer", re.compile(r"\b(?:approver|finance|controller|manager|role|owner|authorized user)\b", re.I)),
            ("missing_authorization_threshold", re.compile(r"\b(?:threshold|over|above|greater than|amount|limit|\$|usd|percent)\b", re.I)),
        ),
        "credit_memo": (
            ("missing_original_invoice_reference", re.compile(r"\b(?:original invoice|invoice id|invoice number|invoice reference)\b", re.I)),
            ("missing_credit_memo_amount", re.compile(r"\b(?:amount|line item|delta|credit total|tax|subtotal)\b", re.I)),
        ),
        "tax_recalculation": (
            ("missing_tax_basis", re.compile(r"\b(?:taxable amount|tax basis|line item|rate|jurisdiction|vat|gst|sales tax)\b", re.I)),
            ("missing_rounding_rule", re.compile(r"\b(?:rounding|precision|decimal|penny|cent|minor unit)\b", re.I)),
        ),
        "audit_trail": (
            ("missing_audit_actor", re.compile(r"\b(?:who|actor|user|approver|changed by|finance|controller)\b", re.I)),
            ("missing_audit_timestamp", re.compile(r"\b(?:timestamp|time|date|when)\b", re.I)),
            ("missing_adjustment_reason", re.compile(r"\b(?:reason|reason code|why|overcharge|tax error|correction)\b", re.I)),
        ),
        "customer_notification": (
            ("missing_notification_channel", re.compile(r"\b(?:emails?|portal|in-app|webhook|statement|billing contact)\b", re.I)),
            ("missing_notification_timing", re.compile(r"\b(?:before|after|when|within|immediately|daily|status|issued)\b", re.I)),
        ),
        "accounting_export": (
            ("missing_accounting_system", re.compile(r"\b(?:netsuite|quickbooks|xero|erp|accounting system)\b", re.I)),
            ("missing_export_mapping", re.compile(r"\b(?:ledger|journal|account|mapping|a/r|accounts receivable|revenue recognition)\b", re.I)),
        ),
    }
    return [flag for flag, pattern in rules[category] if not pattern.search(text)]


def _summary(requirements: tuple[SourceInvoiceAdjustmentRequirement, ...]) -> dict[str, Any]:
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
        "adjustments",
        "credit_memos",
        "tax",
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
    "InvoiceAdjustmentRequirementCategory",
    "SourceInvoiceAdjustmentRequirement",
    "SourceInvoiceAdjustmentRequirementsReport",
    "build_source_invoice_adjustment_requirements",
    "derive_source_invoice_adjustment_requirements",
    "extract_source_invoice_adjustment_requirements",
    "generate_source_invoice_adjustment_requirements",
    "summarize_source_invoice_adjustment_requirements",
    "source_invoice_adjustment_requirements_to_dict",
    "source_invoice_adjustment_requirements_to_dicts",
    "source_invoice_adjustment_requirements_to_markdown",
]
