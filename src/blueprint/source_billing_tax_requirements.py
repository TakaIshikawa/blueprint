"""Extract billing tax requirements from source and implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


BillingTaxCategory = Literal[
    "tax_calculation",
    "vat_gst_collection",
    "tax_exemption",
    "invoice_tax_display",
    "jurisdiction_rules",
    "reverse_charge",
    "tax_reporting",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[BillingTaxCategory, ...] = (
    "tax_calculation",
    "vat_gst_collection",
    "tax_exemption",
    "invoice_tax_display",
    "jurisdiction_rules",
    "reverse_charge",
    "tax_reporting",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_TAX_CONTEXT_RE = re.compile(
    r"\b(?:tax|taxes|taxation|billing tax|sales tax|vat|gst|hst|pst|qst|"
    r"tax invoice|invoice tax|tax exempt|tax exemption|exemption certificate|"
    r"reverse charge|nexus|place of supply|tax jurisdiction|tax reporting|"
    r"tax filing|tax remittance|vat number|tax id)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:tax|taxes|taxation|vat|gst|hst|pst|qst|invoice|billing|exempt|exemption|"
    r"jurisdiction|nexus|reverse[-_ ]?charge|reporting|remittance|filing|"
    r"compliance|definition[-_ ]?of[-_ ]?done|risks?|architecture)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"calculate|collect|display|show|include|validate|support|apply|determine|"
    r"before launch|cannot ship|done when|acceptance)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:tax|vat|gst|invoice tax|billing tax|tax reporting|"
    r"jurisdiction).*?\b(?:in scope|required|requirements?|needed|changes?)\b",
    re.I,
)
_SPECIFIC_TAX_RE = re.compile(
    r"\b(?:sales tax|vat|gst|hst|pst|qst|tax rate|tax rates|tax code|tax engine|"
    r"exemption certificate|resale certificate|tax exempt|reverse charge|nexus|"
    r"place of supply|jurisdiction|remittance|filing|tax report|tax invoice|"
    r"vat number|tax id)\b",
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

_CATEGORY_PATTERNS: dict[BillingTaxCategory, re.Pattern[str]] = {
    "tax_calculation": re.compile(
        r"\b(?:tax calculation|calculate taxes?|calculates? sales tax|sales tax|tax rate|"
        r"tax rates|tax code|tax codes|tax engine|tax amount|tax amounts|taxable amount|"
        r"taxable subtotal|apply taxes?|tax logic)\b",
        re.I,
    ),
    "vat_gst_collection": re.compile(
        r"\b(?:vat|gst|hst|pst|qst|value[- ]added tax|goods and services tax|"
        r"collect(?:ion)? (?:vat|gst|tax)|(?:vat|gst) collection|vat registration|"
        r"gst registration|vat number|tax id)\b",
        re.I,
    ),
    "tax_exemption": re.compile(
        r"\b(?:tax exempt|tax exemption|exempt customer|exempt customers|exemption certificate|"
        r"resale certificate|nonprofit exemption|tax[- ]exempt status|validate exemption|"
        r"exemption flow|exemption review)\b",
        re.I,
    ),
    "invoice_tax_display": re.compile(
        r"\b(?:(?:tax|vat|gst|hst|pst|qst)\s+(?:invoice|receipt|line item|breakdown|display|"
        r"shown?|included)|(?:invoice|receipt)\s+(?:shows?|displays?|includes?)\s+"
        r"(?:tax|vat|gst)|invoice tax display|tax breakdown|tax line items?|vat number on invoice)\b",
        re.I,
    ),
    "jurisdiction_rules": re.compile(
        r"\b(?:tax jurisdiction|jurisdiction rules?|jurisdictional billing|nexus|"
        r"place of supply|ship[- ]to|bill[- ]to|billing country|billing state|"
        r"billing province|customer location|local tax|regional tax|country tax|"
        r"state tax|province tax|tax region|tax locale)\b",
        re.I,
    ),
    "reverse_charge": re.compile(
        r"\b(?:reverse charge|reverse[- ]charged|self[- ]assess(?:ed|ment)?|"
        r"customer accounts for (?:vat|gst|tax)|buyer accounts for (?:vat|gst|tax)|"
        r"article 196)\b",
        re.I,
    ),
    "tax_reporting": re.compile(
        r"\b(?:tax reporting|tax report|tax reports|tax filing|tax filings|tax return|"
        r"remittance|tax remittance|tax audit|audit export|vat return|gst return|"
        r"compliance report|tax reconciliation)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[BillingTaxCategory, str] = {
    "tax_calculation": "billing_engineering",
    "vat_gst_collection": "finance_ops",
    "tax_exemption": "finance_ops",
    "invoice_tax_display": "billing_engineering",
    "jurisdiction_rules": "tax_compliance",
    "reverse_charge": "tax_compliance",
    "tax_reporting": "finance_ops",
}
_PLANNING_NOTE_BY_CATEGORY: dict[BillingTaxCategory, str] = {
    "tax_calculation": "Confirm tax-rate source, taxable amounts, and calculation timing before implementation.",
    "vat_gst_collection": "Plan VAT/GST collection, identifiers, and regional collection rules with finance review.",
    "tax_exemption": "Include exemption capture, validation, and auditability in the billing workflow plan.",
    "invoice_tax_display": "Add invoice and receipt tax display requirements to billing acceptance criteria.",
    "jurisdiction_rules": "Resolve jurisdiction, nexus, and location rules before task generation.",
    "reverse_charge": "Confirm reverse-charge eligibility, invoice language, and compliance owner sign-off.",
    "tax_reporting": "Plan tax reporting, reconciliation, and remittance exports with finance operations.",
}


@dataclass(frozen=True, slots=True)
class SourceBillingTaxRequirement:
    """One source-backed billing tax requirement category."""

    category: BillingTaxCategory
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)
    suggested_owner: str = ""
    suggested_planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "suggested_owner": self.suggested_owner,
            "suggested_planning_note": self.suggested_planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceBillingTaxRequirementsReport:
    """Brief-level billing tax requirements report before implementation planning."""

    source_id: str | None = None
    requirements: tuple[SourceBillingTaxRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceBillingTaxRequirement, ...]:
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
        """Return billing tax requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Billing Tax Requirements Report"
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
            lines.extend(["", "No billing tax requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Confidence | Evidence | Suggested Owner | Suggested Planning Note |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{requirement.confidence:.2f} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{requirement.suggested_owner} | "
                f"{_markdown_cell(requirement.suggested_planning_note)} |"
            )
        return "\n".join(lines)


def build_source_billing_tax_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceBillingTaxRequirementsReport:
    """Build a billing tax requirements report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceBillingTaxRequirementsReport(
        source_id=source_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def generate_source_billing_tax_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceBillingTaxRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_billing_tax_requirements(source)


def extract_source_billing_tax_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[SourceBillingTaxRequirement, ...]:
    """Return billing tax requirement records extracted from brief-shaped input."""
    return build_source_billing_tax_requirements(source).requirements


def summarize_source_billing_tax_requirements(
    source_or_result: Mapping[str, Any] | SourceBrief | ImplementationBrief | SourceBillingTaxRequirementsReport | object,
) -> dict[str, Any]:
    """Return the deterministic billing tax requirements summary."""
    if isinstance(source_or_result, SourceBillingTaxRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_billing_tax_requirements(source_or_result).summary


def source_billing_tax_requirements_to_dict(
    report: SourceBillingTaxRequirementsReport,
) -> dict[str, Any]:
    """Serialize a billing tax requirements report to a plain dictionary."""
    return report.to_dict()


source_billing_tax_requirements_to_dict.__test__ = False


def source_billing_tax_requirements_to_dicts(
    requirements: tuple[SourceBillingTaxRequirement, ...]
    | list[SourceBillingTaxRequirement]
    | SourceBillingTaxRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize source billing tax requirement records to dictionaries."""
    if isinstance(requirements, SourceBillingTaxRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_billing_tax_requirements_to_dicts.__test__ = False


def source_billing_tax_requirements_to_markdown(
    report: SourceBillingTaxRequirementsReport,
) -> str:
    """Render a billing tax requirements report as Markdown."""
    return report.to_markdown()


source_billing_tax_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: BillingTaxCategory
    confidence: float
    evidence: str


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[str | None, dict[str, Any]]:
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
    if not isinstance(source, (str, bytes, bytearray)):
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
        evidence = _evidence_snippet(segment.source_field, segment.text)
        confidence = _confidence(segment.text, segment.source_field, segment.section_context)
        for category in categories:
            candidates.append(_Candidate(category=category, confidence=confidence, evidence=evidence))
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceBillingTaxRequirement]:
    by_category: dict[BillingTaxCategory, list[_Candidate]] = {}
    for candidate in candidates:
        by_category.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceBillingTaxRequirement] = []
    for category in _CATEGORY_ORDER:
        items = by_category.get(category, [])
        if not items:
            continue
        requirements.append(
            SourceBillingTaxRequirement(
                category=category,
                confidence=round(max(item.confidence for item in items), 2),
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:5],
                suggested_owner=_OWNER_BY_CATEGORY[category],
                suggested_planning_note=_PLANNING_NOTE_BY_CATEGORY[category],
            )
        )
    return requirements


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
        "tax",
        "taxes",
        "invoice",
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
                _STRUCTURED_FIELD_RE.search(key_text) or _TAX_CONTEXT_RE.search(key_text)
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
                _TAX_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
    if (field_context or section_context) and _TAX_CONTEXT_RE.search(text):
        return True
    if _REQUIREMENT_RE.search(text) and (_TAX_CONTEXT_RE.search(text) or _SPECIFIC_TAX_RE.search(text)):
        return True
    if _SPECIFIC_TAX_RE.search(text) and _TAX_CONTEXT_RE.search(text):
        return True
    return False


def _confidence(text: str, source_field: str, section_context: bool) -> float:
    score = 0.68
    if _STRUCTURED_FIELD_RE.search(_field_words(source_field)):
        score += 0.08
    if section_context or _TAX_CONTEXT_RE.search(text):
        score += 0.07
    if _REQUIREMENT_RE.search(text):
        score += 0.07
    if _SPECIFIC_TAX_RE.search(text):
        score += 0.05
    return round(min(score, 0.95), 2)


def _summary(requirements: tuple[SourceBillingTaxRequirement, ...]) -> dict[str, Any]:
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
        "tax",
        "taxes",
        "invoice",
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
    "BillingTaxCategory",
    "SourceBillingTaxRequirement",
    "SourceBillingTaxRequirementsReport",
    "build_source_billing_tax_requirements",
    "extract_source_billing_tax_requirements",
    "generate_source_billing_tax_requirements",
    "summarize_source_billing_tax_requirements",
    "source_billing_tax_requirements_to_dict",
    "source_billing_tax_requirements_to_dicts",
    "source_billing_tax_requirements_to_markdown",
]
