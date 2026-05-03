"""Extract source-level invoice numbering requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


InvoiceNumberingCategory = Literal[
    "invoice_sequence",
    "prefix_format",
    "fiscal_year_reset",
    "credit_note",
    "void_cancel",
    "jurisdiction",
    "retention",
    "duplicate_prevention",
    "audit_evidence",
]
InvoiceNumberingConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[InvoiceNumberingCategory, ...] = (
    "invoice_sequence",
    "prefix_format",
    "fiscal_year_reset",
    "credit_note",
    "void_cancel",
    "jurisdiction",
    "retention",
    "duplicate_prevention",
    "audit_evidence",
)
_CONFIDENCE_ORDER: dict[InvoiceNumberingConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_OWNER_BY_CATEGORY: dict[InvoiceNumberingCategory, str] = {
    "invoice_sequence": "billing_engineering",
    "prefix_format": "billing_engineering",
    "fiscal_year_reset": "finance_ops",
    "credit_note": "finance_ops",
    "void_cancel": "finance_ops",
    "jurisdiction": "tax_compliance",
    "retention": "finance_ops",
    "duplicate_prevention": "billing_engineering",
    "audit_evidence": "finance_ops",
}
_PLANNING_NOTES: dict[InvoiceNumberingCategory, str] = {
    "invoice_sequence": "Define monotonic invoice sequence allocation, gap handling, concurrency behavior, and backfill rules.",
    "prefix_format": "Specify invoice number prefixes, tenant or country segments, padding, and display format.",
    "fiscal_year_reset": "Confirm fiscal-period reset rules, year boundaries, and sequence uniqueness across periods.",
    "credit_note": "Plan credit note numbering, references to original invoices, and jurisdiction-specific document labels.",
    "void_cancel": "Define voided or canceled invoice states, preserved numbers, reversal documents, and audit visibility.",
    "jurisdiction": "Map invoice numbering rules by country, tax regime, seller entity, and document type.",
    "retention": "Plan invoice PDF, archive retention, immutable storage, retrieval, and purge exceptions.",
    "duplicate_prevention": "Add duplicate-number prevention controls, idempotency, locks, and reconciliation alerts.",
    "audit_evidence": "Capture numbering audit evidence, issuer history, timestamps, exports, and reviewer traceability.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_INVOICE_NUMBERING_CONTEXT_RE = re.compile(
    r"\b(?:invoice number(?:s|ing)?|invoice numbering|invoice sequence|invoice no\.?|"
    r"invoice id|invoice identifier|document number(?:s|ing)?|document sequence|"
    r"tax invoice number|fiscal invoice|e[- ]?invoice|invoice document|invoice pdf|"
    r"invoice archive|archived invoices?|credit note(?: number(?:s|ing)?)?|"
    r"credit memo(?: number(?:s|ing)?)?|void(?:ed)? invoice|cancel(?:ed|led)? invoice|"
    r"duplicate invoice number|sequential numbering|number range|number series|"
    r"numbering series|fiscal year reset|invoice prefix|number prefix|audit evidence)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:invoice|invoicing|numbering|sequence|prefix|fiscal|credit[_ -]?note|"
    r"credit[_ -]?memo|void|cancel|jurisdiction|country|tax|compliance|retention|"
    r"archive|pdf|duplicate|idempotenc|audit|evidence|requirements?|acceptance|"
    r"criteria|definition[_ -]?of[_ -]?done|source[_ -]?payload|metadata|billing)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|define|allocate|assign|generate|reserve|preserve|retain|archive|store|"
    r"prevent|block|dedupe|deduplicate|lock|validate|record|log|export|audit|"
    r"void|cancel|credit|reset|prefix|acceptance|done when|cannot ship)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:invoice number(?:s|ing)?|document number(?:s|ing)?|invoice sequence|"
    r"credit notes?|void(?:ed)? invoices?|cancel(?:ed|led)? invoices?|invoice archive|"
    r"invoice pdf|duplicate invoice number)\b.{0,120}"
    r"\b(?:required|needed|in scope|support|supported|planned|changes?|work)\b|"
    r"\b(?:invoice number(?:s|ing)?|document number(?:s|ing)?|invoice sequence|"
    r"credit notes?|void(?:ed)? invoices?|cancel(?:ed|led)? invoices?|invoice archive|"
    r"invoice pdf|duplicate invoice number)\b.{0,140}"
    r"\b(?:not required|not needed|out of scope|no changes?|no work|non[- ]?goal)\b",
    re.I,
)
_GENERIC_BILLING_ONLY_RE = re.compile(
    r"\b(?:price|pricing|tax|taxes|vat|gst|sales tax|discount|coupon|subscription|"
    r"checkout|payment|refund|charge|billing)\b",
    re.I,
)
_NUMBERING_SIGNAL_RE = re.compile(
    r"\b(?:number(?:s|ing)?|sequence|sequential|prefix|series|range|reset|fiscal year|"
    r"document|pdf|archive|retain|retention|duplicate|idempotenc|audit|evidence|"
    r"void|cancel(?:ed|led)?|credit note|credit memo|invoice id|invoice no)\b",
    re.I,
)
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "mvp_goal",
    "context",
    "workflow_context",
    "requirements",
    "constraints",
    "scope",
    "non_goals",
    "assumptions",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "billing",
    "invoice",
    "invoicing",
    "tax",
    "finance",
    "compliance",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
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
    "domain",
    "status",
}
_CATEGORY_PATTERNS: dict[InvoiceNumberingCategory, re.Pattern[str]] = {
    "invoice_sequence": re.compile(
        r"\b(?:invoice sequence|sequential numbering|sequential invoice|"
        r"consecutive invoice|monotonic invoice|next invoice number|"
        r"(?:invoice|document|tax invoice).{0,60}\b(?:sequence|sequential|consecutive|monotonic|number range|number series|numbering series)|"
        r"(?:sequence|sequential|consecutive|monotonic|number range|number series|numbering series).{0,60}\b(?:invoice|document|tax invoice))\b",
        re.I,
    ),
    "prefix_format": re.compile(
        r"\b(?:invoice prefix|number prefix|prefix format|document prefix|"
        r"invoice number format|number format|format pattern|padding|zero[- ]?pad|"
        r"country prefix|tenant prefix|seller prefix)\b",
        re.I,
    ),
    "fiscal_year_reset": re.compile(
        r"\b(?:fiscal year reset|fiscal[- ]?year numbering|yearly reset|annual reset|"
        r"reset (?:the )?(?:invoice )?(?:number|sequence)|new fiscal year|"
        r"fiscal period|tax year sequence|calendar year reset)\b",
        re.I,
    ),
    "credit_note": re.compile(
        r"\b(?:credit note(?:s)?|credit memo(?:s)?|credit invoice|negative invoice|"
        r"credit note number(?:s|ing)?|credit memo number(?:s|ing)?|"
        r"reference original invoice|original invoice reference)\b",
        re.I,
    ),
    "void_cancel": re.compile(
        r"\b(?:void(?:ed)? invoice|cancel(?:ed|led)? invoice|invoice cancellation|"
        r"cancel invoice|void number|preserve(?:d)? number|do not reuse(?: numbers?)?|"
        r"reversal document|annul(?:led)? invoice)\b",
        re.I,
    ),
    "jurisdiction": re.compile(
        r"\b(?:jurisdiction(?:al)?|tax regime|local law|vat invoice|gst invoice|"
        r"einvoice|e[- ]?invoice|peppol|fiscal invoice|tax authority|invoice numbering rules?|"
        r"(?:country|countries|region|seller entity|legal entity).{0,80}\b(?:invoice numbering|document number|numbering rules|tax invoice|e[- ]?invoice)|"
        r"(?:invoice numbering|document number|numbering rules|tax invoice|e[- ]?invoice).{0,80}\b(?:country|countries|region|seller entity|legal entity))\b",
        re.I,
    ),
    "retention": re.compile(
        r"\b(?:invoice pdf|pdf archive|invoice archive|archived invoices?|archive invoices?|"
        r"retain invoices?|retention|immutable storage|document storage|store invoice documents?|"
        r"retrievable invoice|downloadable invoice|fiscal archive)\b",
        re.I,
    ),
    "duplicate_prevention": re.compile(
        r"\b(?:duplicate invoice number|duplicate document number|prevent duplicates?|"
        r"dedupe|deduplicate|idempotenc(?:y|e|ent)|unique invoice number|unique document number|"
        r"number collision|collision prevention|sequence lock|allocation lock|concurrency)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit evidence|audit trail|audit log|audit history|numbering history|"
        r"issuer history|who issued|number allocation log|exportable evidence|"
        r"compliance evidence|traceability|timestamps?)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class SourceInvoiceNumberingRequirement:
    """One source-backed invoice numbering requirement."""

    source_brief_id: str | None
    category: InvoiceNumberingCategory
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: InvoiceNumberingConfidence = "medium"
    owner_suggestion: str = ""
    planning_notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "owner_suggestion": self.owner_suggestion,
            "planning_notes": list(self.planning_notes),
        }


@dataclass(frozen=True, slots=True)
class SourceInvoiceNumberingRequirementsReport:
    """Source-level invoice numbering requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceInvoiceNumberingRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceInvoiceNumberingRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return invoice numbering requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Invoice Numbering Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
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
            "- Confidence counts: "
            + ", ".join(
                f"{confidence} {confidence_counts.get(confidence, 0)}"
                for confidence in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No invoice numbering requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Confidence | Owner | Planning Notes | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.category} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.owner_suggestion)} | "
                f"{_markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_invoice_numbering_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceInvoiceNumberingRequirementsReport:
    """Extract source-level invoice numbering requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceInvoiceNumberingRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_invoice_numbering_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceInvoiceNumberingRequirementsReport:
    """Compatibility alias for building an invoice numbering requirements report."""
    return build_source_invoice_numbering_requirements(source)


def generate_source_invoice_numbering_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceInvoiceNumberingRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_invoice_numbering_requirements(source)


def derive_source_invoice_numbering_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceInvoiceNumberingRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_invoice_numbering_requirements(source)


def summarize_source_invoice_numbering_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceInvoiceNumberingRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted invoice numbering requirements."""
    if isinstance(source_or_result, SourceInvoiceNumberingRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_invoice_numbering_requirements(source_or_result).summary


def source_invoice_numbering_requirements_to_dict(
    report: SourceInvoiceNumberingRequirementsReport,
) -> dict[str, Any]:
    """Serialize an invoice numbering requirements report to a plain dictionary."""
    return report.to_dict()


source_invoice_numbering_requirements_to_dict.__test__ = False


def source_invoice_numbering_requirements_to_dicts(
    requirements: (
        tuple[SourceInvoiceNumberingRequirement, ...]
        | list[SourceInvoiceNumberingRequirement]
        | SourceInvoiceNumberingRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize invoice numbering requirement records to dictionaries."""
    if isinstance(requirements, SourceInvoiceNumberingRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_invoice_numbering_requirements_to_dicts.__test__ = False


def source_invoice_numbering_requirements_to_markdown(
    report: SourceInvoiceNumberingRequirementsReport,
) -> str:
    """Render an invoice numbering requirements report as Markdown."""
    return report.to_markdown()


source_invoice_numbering_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: InvoiceNumberingCategory
    evidence: str
    confidence: InvoiceNumberingConfidence


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_id(payload), payload
    return None, {}


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            if not _is_requirement(segment):
                continue
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            categories = [
                category
                for category in _CATEGORY_ORDER
                if _CATEGORY_PATTERNS[category].search(searchable)
            ]
            for category in _dedupe(categories):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(segment, category),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceInvoiceNumberingRequirement]:
    grouped: dict[tuple[str | None, InvoiceNumberingCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.category), []).append(candidate)

    requirements: list[SourceInvoiceNumberingRequirement] = []
    for (source_brief_id, category), items in grouped.items():
        evidence = tuple(sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold))[:5]
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        requirements.append(
            SourceInvoiceNumberingRequirement(
                source_brief_id=source_brief_id,
                category=category,
                evidence=evidence,
                confidence=confidence,
                owner_suggestion=_OWNER_BY_CATEGORY[category],
                planning_notes=(_PLANNING_NOTES[category],),
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
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
            key_text = _clean_text(str(key).replace("_", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _INVOICE_NUMBERING_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, f"{source_field}.{key}", value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        raw_text = str(value) if isinstance(value, str) else text
        for segment_text, segment_context in _segments(raw_text, field_context):
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
                _INVOICE_NUMBERING_CONTEXT_RE.search(title)
                or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_RE.search(cleaned):
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text and not _NEGATED_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NEGATED_RE.search(searchable):
        return False
    has_context = bool(_INVOICE_NUMBERING_CONTEXT_RE.search(searchable))
    has_numbering_signal = bool(_NUMBERING_SIGNAL_RE.search(searchable))
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category or not has_context or not has_numbering_signal:
        return False
    if _GENERIC_BILLING_ONLY_RE.search(searchable) and not _INVOICE_NUMBERING_CONTEXT_RE.search(searchable):
        return False
    return bool(
        _REQUIREMENT_RE.search(segment.text)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
    )


def _confidence(
    segment: _Segment,
    category: InvoiceNumberingCategory,
) -> InvoiceNumberingConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _REQUIREMENT_RE.search(segment.text) and _INVOICE_NUMBERING_CONTEXT_RE.search(searchable):
        return "high"
    if category in {"duplicate_prevention", "retention", "audit_evidence"} and _INVOICE_NUMBERING_CONTEXT_RE.search(searchable):
        return "high"
    if segment.section_context or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceInvoiceNumberingRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "categories": [requirement.category for requirement in requirements],
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
        "scope",
        "non_goals",
        "assumptions",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "billing",
        "invoice",
        "invoicing",
        "tax",
        "finance",
        "compliance",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(value)
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
        key = _clean_text(statement or value).casefold()
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
    "InvoiceNumberingCategory",
    "InvoiceNumberingConfidence",
    "SourceInvoiceNumberingRequirement",
    "SourceInvoiceNumberingRequirementsReport",
    "build_source_invoice_numbering_requirements",
    "derive_source_invoice_numbering_requirements",
    "extract_source_invoice_numbering_requirements",
    "generate_source_invoice_numbering_requirements",
    "source_invoice_numbering_requirements_to_dict",
    "source_invoice_numbering_requirements_to_dicts",
    "source_invoice_numbering_requirements_to_markdown",
    "summarize_source_invoice_numbering_requirements",
]
