"""Extract source-level VAT and tax requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


VatTaxCategory = Literal[
    "taxable_jurisdictions",
    "tax_calculation_timing",
    "exemption_handling",
    "invoice_tax_display",
    "refund_tax_treatment",
]
VatTaxConfidence = Literal["high", "medium", "low"]
VatTaxGapCategory = Literal[
    "missing_jurisdiction_details",
    "missing_exemption_details",
    "missing_refund_details",
    "missing_invoice_display_details",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[VatTaxCategory, ...] = (
    "taxable_jurisdictions",
    "tax_calculation_timing",
    "exemption_handling",
    "invoice_tax_display",
    "refund_tax_treatment",
)
_GAP_ORDER: tuple[VatTaxGapCategory, ...] = (
    "missing_jurisdiction_details",
    "missing_exemption_details",
    "missing_refund_details",
    "missing_invoice_display_details",
)
_CONFIDENCE_ORDER: dict[VatTaxConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_OWNER_BY_CATEGORY: dict[VatTaxCategory, str] = {
    "taxable_jurisdictions": "tax_compliance",
    "tax_calculation_timing": "billing_engineering",
    "exemption_handling": "finance_ops",
    "invoice_tax_display": "billing_engineering",
    "refund_tax_treatment": "finance_ops",
}
_PLANNING_NOTES_BY_CATEGORY: dict[VatTaxCategory, tuple[str, ...]] = {
    "taxable_jurisdictions": (
        "Confirm taxable countries, regions, nexus, place-of-supply rules, and registration obligations.",
    ),
    "tax_calculation_timing": (
        "Define when VAT or tax is estimated, finalized, snapshotted, and recalculated in billing flows.",
    ),
    "exemption_handling": (
        "Plan tax ID, exemption certificate, reverse-charge, validation, review, and audit behavior.",
    ),
    "invoice_tax_display": (
        "Specify invoice and receipt tax lines, VAT IDs, reverse-charge text, rates, and totals.",
    ),
    "refund_tax_treatment": (
        "Define how refunds, credits, reversals, and partial refunds adjust tax amounts and reporting.",
    ),
}
_GAP_MESSAGES: dict[VatTaxGapCategory, str] = {
    "missing_jurisdiction_details": "Specify taxable jurisdictions, nexus, place of supply, or location rules.",
    "missing_exemption_details": "Specify exemption, tax ID, certificate, reverse-charge, or validation handling.",
    "missing_refund_details": "Specify tax treatment for refunds, credits, reversals, or partial refunds.",
    "missing_invoice_display_details": "Specify invoice or receipt tax display, VAT IDs, rates, and tax totals.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_VAT_CONTEXT_RE = re.compile(
    r"\b(?:vat|value[- ]added tax|gst|hst|pst|qst|sales tax|taxes?|taxation|taxable|"
    r"tax engine|tax rate|tax rates|tax code|tax id|vat id|vat number|tax invoice|"
    r"invoice tax|receipt tax|reverse charge|tax exempt|tax exemption|exemption certificate|"
    r"nexus|place of supply|tax jurisdiction|jurisdiction|remittance|tax reporting|refund tax)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:vat|gst|tax|taxes|taxation|taxable|jurisdiction|nexus|region|country|billing|"
    r"invoice|receipt|exempt|exemption|certificate|reverse[-_ ]?charge|refund|credit|"
    r"reversal|requirements?|acceptance|criteria|definition[-_ ]?of[-_ ]?done|metadata|"
    r"source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"support|allow|provide|calculate|collect|apply|determine|validate|verify|display|"
    r"show|include|itemize|persist|snapshot|refund|reverse|credit|acceptance|done when|"
    r"before launch|cannot ship)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:vat|tax|taxes|taxation|invoice tax|refund tax|tax exemption|tax jurisdiction)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?)\b|"
    r"\b(?:vat|tax|taxes|taxation|invoice tax|refund tax|tax exemption|tax jurisdiction)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|non[- ]?goal)\b",
    re.I,
)
_SPECIFIC_VAT_RE = re.compile(
    r"\b(?:vat|gst|hst|pst|qst|sales tax|tax rate|tax rates|tax engine|taxable subtotal|"
    r"taxable amount|tax code|vat id|vat number|tax id|reverse charge|exemption certificate|"
    r"tax exempt|nexus|place of supply|tax jurisdiction|billing country|billing state|"
    r"billing province|invoice tax|tax line item|tax breakdown|refund tax|tax refund)\b",
    re.I,
)
_VALUE_PATTERNS: dict[VatTaxCategory, re.Pattern[str]] = {
    "taxable_jurisdictions": re.compile(
        r"\b(?:EU|European Union|UK|United Kingdom|Canada|Australia|New Zealand|US|United States|"
        r"California|New York|Germany|France|billing country|billing state|billing province|"
        r"place of supply|nexus|tax jurisdiction)\b",
        re.I,
    ),
    "tax_calculation_timing": re.compile(
        r"\b(?:checkout|quote|renewal|subscription creation|invoice finalization|finali[sz]e invoice|"
        r"payment capture|before payment|before checkout|after proration|snapshot|recalculate|"
        r"estimate|estimated|final tax)\b",
        re.I,
    ),
    "exemption_handling": re.compile(
        r"\b(?:tax id|vat id|vat number|exemption certificate|resale certificate|reverse charge|"
        r"tax exempt|exemption|manual review|approved exemption)\b",
        re.I,
    ),
    "invoice_tax_display": re.compile(
        r"\b(?:invoice|receipt|tax line items?|tax breakdown|vat number|vat id|tax rate|"
        r"tax total|reverse[- ]charge text|subtotal)\b",
        re.I,
    ),
    "refund_tax_treatment": re.compile(
        r"\b(?:refund|refunded|credit note|credit memo|reversal|void|partial refund|"
        r"tax adjustment|tax reversal|refund tax)\b",
        re.I,
    ),
}
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
_SCANNED_FIELDS: tuple[str, ...] = (
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
    "tax",
    "taxes",
    "vat",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[VatTaxCategory, re.Pattern[str]] = {
    "taxable_jurisdictions": re.compile(
        r"\b(?:taxable jurisdictions?|tax jurisdictions?|jurisdiction rules?|jurisdictional tax|"
        r"nexus|place of supply|billing country|billing state|billing province|customer location|"
        r"ship[- ]to|bill[- ]to|tax region|country tax|regional tax|local tax|registered countries|"
        r"eu vat|canada gst|uk vat)\b",
        re.I,
    ),
    "tax_calculation_timing": re.compile(
        r"\b(?:tax calculation timing|calculate(?:s|d|ing)? (?:vat|gst|sales tax|taxes?)|"
        r"(?:vat|gst|sales tax|taxes?).{0,50}(?:calculate|calculation|computed?|estimated|finali[sz]ed)|"
        r"tax estimate|estimated tax|final tax|tax snapshot|snapshot tax|tax rate snapshot|"
        r"checkout.{0,50}(?:vat|tax)|invoice finali[sz]ation.{0,50}(?:vat|tax)|"
        r"before payment.{0,50}(?:vat|tax)|payment capture.{0,50}(?:vat|tax))\b",
        re.I,
    ),
    "exemption_handling": re.compile(
        r"\b(?:exemption handling|tax exempt|tax exemption|exempt customers?|exemption certificate|"
        r"resale certificate|vat id|tax id|validate(?:d|s|ing)? tax id|"
        r"reverse charge|zero[- ]rate|exemption review|manual exemption review)\b",
        re.I,
    ),
    "invoice_tax_display": re.compile(
        r"\b(?:(?:invoice|receipt).{0,60}(?:vat|gst|sales tax|tax line|tax breakdown|tax total|tax rate)|"
        r"(?:vat|gst|sales tax|tax).{0,60}(?:invoice|receipt|line item|breakdown|display|shown?|included)|"
        r"invoice tax display|tax line items?|vat number on invoice|reverse[- ]charge text)\b",
        re.I,
    ),
    "refund_tax_treatment": re.compile(
        r"\b(?:refund tax treatment|tax treatment for refunds?|refund(?:ed|s|ing)?.{0,60}(?:vat|tax)|"
        r"(?:vat|tax).{0,60}(?:refund|credit note|credit memo|reversal|void|partial refund)|"
        r"tax reversal|tax adjustment|credit note.{0,50}(?:vat|tax))\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class SourceVatTaxRequirement:
    """One source-backed VAT or tax implementation requirement."""

    category: VatTaxCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: VatTaxConfidence = "medium"
    value: str = ""
    suggested_owner: str = ""
    planning_notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owner": self.suggested_owner,
            "planning_notes": list(self.planning_notes),
        }


@dataclass(frozen=True, slots=True)
class SourceVatTaxEvidenceGap:
    """One missing VAT or tax detail that should be resolved before planning."""

    category: VatTaxGapCategory
    message: str
    confidence: VatTaxConfidence = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "message": self.message,
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class SourceVatTaxRequirementsReport:
    """Brief-level VAT and tax requirements report before implementation planning."""

    source_id: str | None = None
    requirements: tuple[SourceVatTaxRequirement, ...] = field(default_factory=tuple)
    evidence_gaps: tuple[SourceVatTaxEvidenceGap, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceVatTaxRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def gaps(self) -> tuple[SourceVatTaxEvidenceGap, ...]:
        """Compatibility alias for evidence gaps."""
        return self.evidence_gaps

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "evidence_gaps": [gap.to_dict() for gap in self.evidence_gaps],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "gaps": [gap.to_dict() for gap in self.gaps],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return VAT and tax requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source VAT Tax Requirements Report"
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
            f"- Evidence gaps: {self.summary.get('evidence_gap_count', 0)}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Confidence counts: "
            + ", ".join(
                f"{confidence} {confidence_counts.get(confidence, 0)}"
                for confidence in ("high", "medium", "low")
            ),
        ]
        if self.requirements:
            lines.extend(
                [
                    "",
                    "## Requirements",
                    "",
                    "| Category | Value | Confidence | Source Field | Owner | Evidence | Planning Notes |",
                    "| --- | --- | --- | --- | --- | --- | --- |",
                ]
            )
            for requirement in self.requirements:
                lines.append(
                    "| "
                    f"{requirement.category} | "
                    f"{_markdown_cell(requirement.value)} | "
                    f"{requirement.confidence} | "
                    f"{requirement.source_field} | "
                    f"{requirement.suggested_owner} | "
                    f"{_markdown_cell('; '.join(requirement.evidence))} | "
                    f"{_markdown_cell('; '.join(requirement.planning_notes))} |"
                )
        else:
            lines.extend(["", "No VAT or tax requirements were found in the source brief."])
        if self.evidence_gaps:
            lines.extend(["", "## Evidence Gaps", "", "| Gap | Confidence | Message |", "| --- | --- | --- |"])
            for gap in self.evidence_gaps:
                lines.append(f"| {gap.category} | {gap.confidence} | {_markdown_cell(gap.message)} |")
        return "\n".join(lines)


def build_source_vat_tax_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceVatTaxRequirementsReport:
    """Build a VAT and tax requirements report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    candidates = _requirement_candidates(payload)
    requirements = tuple(_merge_candidates(candidates))
    gaps = tuple(_evidence_gaps(requirements, candidates))
    return SourceVatTaxRequirementsReport(
        source_id=source_id,
        requirements=requirements,
        evidence_gaps=gaps,
        summary=_summary(requirements, gaps),
    )


def generate_source_vat_tax_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceVatTaxRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_vat_tax_requirements(source)


def derive_source_vat_tax_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceVatTaxRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_vat_tax_requirements(source)


def extract_source_vat_tax_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[SourceVatTaxRequirement, ...]:
    """Return VAT and tax requirement records extracted from brief-shaped input."""
    return build_source_vat_tax_requirements(source).requirements


def summarize_source_vat_tax_requirements(
    source_or_result: str
    | Mapping[str, Any]
    | SourceBrief
    | ImplementationBrief
    | SourceVatTaxRequirementsReport
    | object,
) -> dict[str, Any]:
    """Return the deterministic VAT and tax requirements summary."""
    if isinstance(source_or_result, SourceVatTaxRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_vat_tax_requirements(source_or_result).summary


def source_vat_tax_requirements_to_dict(
    report: SourceVatTaxRequirementsReport,
) -> dict[str, Any]:
    """Serialize a VAT and tax requirements report to a plain dictionary."""
    return report.to_dict()


source_vat_tax_requirements_to_dict.__test__ = False


def source_vat_tax_requirements_to_dicts(
    requirements: tuple[SourceVatTaxRequirement, ...]
    | list[SourceVatTaxRequirement]
    | SourceVatTaxRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize source VAT and tax requirement records to dictionaries."""
    if isinstance(requirements, SourceVatTaxRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_vat_tax_requirements_to_dicts.__test__ = False


def source_vat_tax_requirements_to_markdown(
    report: SourceVatTaxRequirementsReport,
) -> str:
    """Render a VAT and tax requirements report as Markdown."""
    return report.to_markdown()


source_vat_tax_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: VatTaxCategory
    confidence: VatTaxConfidence
    evidence: str
    source_field: str
    value: str


def _source_payload(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (bytes, bytearray)):
        return None, {}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _brief_id(payload), payload
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
    payload = _object_payload(source)
    return _brief_id(payload), payload


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = [
            category
            for category in _CATEGORY_ORDER
            if _CATEGORY_PATTERNS[category].search(searchable)
        ]
        if not categories:
            continue
        if not _is_requirement(segment.text, segment.source_field, segment.section_context):
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
                    value=_extract_value(category, segment.text),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceVatTaxRequirement]:
    by_category: dict[VatTaxCategory, list[_Candidate]] = {}
    for candidate in candidates:
        by_category.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceVatTaxRequirement] = []
    for category in _CATEGORY_ORDER:
        items = by_category.get(category, [])
        if not items:
            continue
        best = min(items, key=lambda item: _CONFIDENCE_ORDER[item.confidence])
        requirements.append(
            SourceVatTaxRequirement(
                category=category,
                source_field=best.source_field,
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:5],
                confidence=best.confidence,
                value=_merge_values(item.value for item in items),
                suggested_owner=_OWNER_BY_CATEGORY[category],
                planning_notes=_PLANNING_NOTES_BY_CATEGORY[category],
            )
        )
    return requirements


def _evidence_gaps(
    requirements: tuple[SourceVatTaxRequirement, ...],
    candidates: list[_Candidate],
) -> list[SourceVatTaxEvidenceGap]:
    if not requirements and not candidates:
        return []
    present = {requirement.category for requirement in requirements}
    gap_by_requirement: dict[VatTaxCategory, VatTaxGapCategory] = {
        "taxable_jurisdictions": "missing_jurisdiction_details",
        "exemption_handling": "missing_exemption_details",
        "refund_tax_treatment": "missing_refund_details",
        "invoice_tax_display": "missing_invoice_display_details",
    }
    gaps: list[SourceVatTaxEvidenceGap] = []
    for requirement_category, gap_category in gap_by_requirement.items():
        if requirement_category in present:
            continue
        gaps.append(
            SourceVatTaxEvidenceGap(
                category=gap_category,
                message=_GAP_MESSAGES[gap_category],
                confidence="medium",
            )
        )
    return gaps


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
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _VAT_CONTEXT_RE.search(key_text)
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
                _VAT_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if (field_context or section_context) and _VAT_CONTEXT_RE.search(text):
        return True
    if _REQUIREMENT_RE.search(text) and (_VAT_CONTEXT_RE.search(text) or _SPECIFIC_VAT_RE.search(text)):
        return True
    if _SPECIFIC_VAT_RE.search(text) and _VAT_CONTEXT_RE.search(text):
        return True
    return False


def _confidence(text: str, source_field: str, section_context: bool) -> VatTaxConfidence:
    score = 0
    if _STRUCTURED_FIELD_RE.search(_field_words(source_field)):
        score += 1
    if section_context or _VAT_CONTEXT_RE.search(text):
        score += 1
    if _REQUIREMENT_RE.search(text):
        score += 1
    if _SPECIFIC_VAT_RE.search(text):
        score += 1
    if score >= 4:
        return "high"
    if score >= 2:
        return "medium"
    return "low"


def _extract_value(category: VatTaxCategory, text: str) -> str:
    pattern = _VALUE_PATTERNS[category]
    values = _dedupe(match.group(0).strip() for match in pattern.finditer(text))
    return ", ".join(values[:3])


def _summary(
    requirements: tuple[SourceVatTaxRequirement, ...],
    gaps: tuple[SourceVatTaxEvidenceGap, ...],
) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in ("high", "medium", "low")
        },
        "categories": [requirement.category for requirement in requirements],
        "evidence_gap_count": len(gaps),
        "evidence_gaps": [gap.category for gap in gaps],
        "status": _status(requirements, gaps),
    }


def _status(
    requirements: tuple[SourceVatTaxRequirement, ...],
    gaps: tuple[SourceVatTaxEvidenceGap, ...],
) -> str:
    if not requirements:
        return "no_vat_tax_requirements_found"
    if gaps:
        return "needs_tax_detail"
    return "ready_for_planning"


def _object_payload(value: object) -> dict[str, Any]:
    fields = ("id", "source_brief_id", "source_id", *_SCANNED_FIELDS)
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
        key = _clean_text(statement or value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


def _merge_values(values: Iterable[str]) -> str:
    parts: list[str] = []
    for value in values:
        parts.extend(item.strip() for item in value.split(",") if item.strip())
    return ", ".join(_dedupe(parts)[:3])


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
    "VatTaxCategory",
    "VatTaxConfidence",
    "VatTaxGapCategory",
    "SourceVatTaxRequirement",
    "SourceVatTaxEvidenceGap",
    "SourceVatTaxRequirementsReport",
    "build_source_vat_tax_requirements",
    "derive_source_vat_tax_requirements",
    "extract_source_vat_tax_requirements",
    "generate_source_vat_tax_requirements",
    "summarize_source_vat_tax_requirements",
    "source_vat_tax_requirements_to_dict",
    "source_vat_tax_requirements_to_dicts",
    "source_vat_tax_requirements_to_markdown",
]
