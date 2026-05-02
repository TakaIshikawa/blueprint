"""Extract source-level tax exemption requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


TaxExemptionCategory = Literal[
    "exemption_certificate_collection",
    "validation_status",
    "renewal_expiration",
    "jurisdiction_scope",
    "invoice_tax_suppression",
    "audit_evidence",
    "support_review",
]
TaxExemptionConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[TaxExemptionCategory, ...] = (
    "exemption_certificate_collection",
    "validation_status",
    "renewal_expiration",
    "jurisdiction_scope",
    "invoice_tax_suppression",
    "audit_evidence",
    "support_review",
)
_CONFIDENCE_ORDER: dict[TaxExemptionConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_OWNER_BY_CATEGORY: dict[TaxExemptionCategory, str] = {
    "exemption_certificate_collection": "tax_compliance",
    "validation_status": "tax_compliance",
    "renewal_expiration": "tax_compliance",
    "jurisdiction_scope": "finance",
    "invoice_tax_suppression": "billing_engineering",
    "audit_evidence": "finance",
    "support_review": "support",
}
_PLANNING_NOTES_BY_CATEGORY: dict[TaxExemptionCategory, tuple[str, ...]] = {
    "exemption_certificate_collection": (
        "Plan certificate upload, required fields, customer attestation, and storage controls.",
    ),
    "validation_status": (
        "Define validation states, rejection reasons, review SLAs, and checkout or billing gates.",
    ),
    "renewal_expiration": (
        "Capture expiration dates, renewal reminders, grace periods, and expired-certificate behavior.",
    ),
    "jurisdiction_scope": (
        "Map exemption applicability by country, state, province, tax type, and seller nexus.",
    ),
    "invoice_tax_suppression": (
        "Specify invoice and checkout tax suppression rules once a valid exemption applies.",
    ),
    "audit_evidence": (
        "Plan immutable evidence retention, reviewer history, and exportable audit records.",
    ),
    "support_review": (
        "Define support review queues, escalation ownership, customer communication, and overrides.",
    ),
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_TAX_CONTEXT_RE = re.compile(
    r"\b(?:tax exemption|tax exempt|exemptions?|certificates?|resale certificates?|exemption certificates?|"
    r"tax certificates?|vat exemption|sales tax|vat|gst|taxable|non[- ]?taxable|tax[- ]?exempt|"
    r"jurisdiction|state|province|country|nexus|invoice tax|tax suppression|tax calculation|"
    r"avalara|taxjar|vertex|compliance|audit|support review)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:tax|exemption|exempt|certificate|resale|validation|status|renewal|expiration|"
    r"expiry|jurisdiction|country|state|province|nexus|invoice|suppression|audit|"
    r"evidence|support|review|requirements?|acceptance|criteria|definition[-_ ]?of[-_ ]?done|"
    r"metadata|source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|provide|collect|upload|store|validate|verify|approve|reject|expire|"
    r"renew|suppress|remove|zero[- ]?rate|exempt|retain|export|review|escalate|override|"
    r"acceptance|done when|cannot ship|gate)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}"
    r"\b(?:tax exemption|tax exempt|exemptions?|certificate|invoice tax|sales tax|vat|audit|support review)\b"
    r".{0,120}\b(?:required|needed|in scope|support|supported|work|planned|changes?)\b|"
    r"\b(?:tax exemption|tax exempt|exemptions?|certificate|invoice tax|sales tax|vat|audit|support review)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no support|unsupported|no work|non[- ]?goal)\b",
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
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
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
    "tax_exemption",
    "compliance",
    "support",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[TaxExemptionCategory, re.Pattern[str]] = {
    "exemption_certificate_collection": re.compile(
        r"\b(?:exemption certificate|resale certificate|tax certificate|certificate upload|"
        r"upload(?:ed)? certificate|collect(?:ion)? certificate|collect(?:s|ed|ing)? tax exemption|"
        r"customer attestation|exemption document|certificate document)\b",
        re.I,
    ),
    "validation_status": re.compile(
        r"\b(?:validation status|validated exemption|validate(?:d|s|ing)? certificate|"
        r"verify(?:ing)? exemption|approved exemption|rejected exemption|pending review|"
        r"approval status|rejection reason|invalid certificate)\b",
        re.I,
    ),
    "renewal_expiration": re.compile(
        r"\b(?:expiration|expiry|expires?|expired|renewal|renew(?:s|ed|ing)?|"
        r"recertification|re[- ]?certification|grace period|renewal reminder)\b",
        re.I,
    ),
    "jurisdiction_scope": re.compile(
        r"\b(?:jurisdiction(?:al)? scope|jurisdictions?|tax type|nexus|exemption scope|"
        r"(?:country|countries|state|states|province|provinces|region|regions|sales tax|vat|gst)"
        r".{0,80}\b(?:applicability|scope|jurisdiction|tax|vat|gst|sales tax)|"
        r"(?:applicability|scope|jurisdiction|tax|vat|gst|sales tax).{0,80}\b"
        r"(?:country|countries|state|states|province|provinces|region|regions|sales tax|vat|gst))\b",
        re.I,
    ),
    "invoice_tax_suppression": re.compile(
        r"\b(?:invoice tax suppression|suppress(?:es|ed|ing)? tax|tax suppression|"
        r"remove(?:s|d)? tax|zero[- ]?rate|tax[- ]?free invoice|do not charge tax|"
        r"tax not charged|exclude tax|tax calculation exemption)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit evidence|audit trail|audit log|audit records?|evidence retention|"
        r"retain(?:ed|s)? certificate|retention|exportable evidence|compliance evidence|"
        r"review history|immutable record)\b",
        re.I,
    ),
    "support_review": re.compile(
        r"\b(?:support review|manual review|review queue|support queue|support escalation|"
        r"escalate(?:d|s)? to support|support override|agent review|backoffice review|"
        r"customer support)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class SourceTaxExemptionRequirement:
    """One source-backed tax exemption requirement."""

    source_brief_id: str | None
    category: TaxExemptionCategory
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: TaxExemptionConfidence = "medium"
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
class SourceTaxExemptionRequirementsReport:
    """Source-level tax exemption requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceTaxExemptionRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceTaxExemptionRequirement, ...]:
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
        """Return tax exemption requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Tax Exemption Requirements Report"
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
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No tax exemption requirements were found in the source brief."])
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


def build_source_tax_exemption_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceTaxExemptionRequirementsReport:
    """Extract source-level tax exemption requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceTaxExemptionRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_tax_exemption_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceTaxExemptionRequirementsReport:
    """Compatibility alias for building a tax exemption requirements report."""
    return build_source_tax_exemption_requirements(source)


def generate_source_tax_exemption_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceTaxExemptionRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_tax_exemption_requirements(source)


def derive_source_tax_exemption_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceTaxExemptionRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_tax_exemption_requirements(source)


def summarize_source_tax_exemption_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceTaxExemptionRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted tax exemption requirements."""
    if isinstance(source_or_result, SourceTaxExemptionRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_tax_exemption_requirements(source_or_result).summary


def source_tax_exemption_requirements_to_dict(
    report: SourceTaxExemptionRequirementsReport,
) -> dict[str, Any]:
    """Serialize a tax exemption requirements report to a plain dictionary."""
    return report.to_dict()


source_tax_exemption_requirements_to_dict.__test__ = False


def source_tax_exemption_requirements_to_dicts(
    requirements: (
        tuple[SourceTaxExemptionRequirement, ...]
        | list[SourceTaxExemptionRequirement]
        | SourceTaxExemptionRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize tax exemption requirement records to dictionaries."""
    if isinstance(requirements, SourceTaxExemptionRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_tax_exemption_requirements_to_dicts.__test__ = False


def source_tax_exemption_requirements_to_markdown(
    report: SourceTaxExemptionRequirementsReport,
) -> str:
    """Render a tax exemption requirements report as Markdown."""
    return report.to_markdown()


source_tax_exemption_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: TaxExemptionCategory
    evidence: str
    confidence: TaxExemptionConfidence


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
    return _optional_text(payload.get("id")) or _optional_text(payload.get("source_brief_id")) or _optional_text(payload.get("source_id"))


def _candidates_for_briefs(brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            if not _is_requirement(segment):
                continue
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]
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


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceTaxExemptionRequirement]:
    grouped: dict[tuple[str | None, TaxExemptionCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.category), []).append(candidate)

    requirements: list[SourceTaxExemptionRequirement] = []
    for (source_brief_id, category), items in grouped.items():
        evidence = tuple(sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold))[:5]
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        requirements.append(
            SourceTaxExemptionRequirement(
                source_brief_id=source_brief_id,
                category=category,
                evidence=evidence,
                confidence=confidence,
                owner_suggestion=_OWNER_BY_CATEGORY[category],
                planning_notes=_PLANNING_NOTES_BY_CATEGORY[category],
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


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _clean_text(str(key).replace("_", " "))
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text) or _TAX_CONTEXT_RE.search(key_text))
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
            section_context = inherited_context or bool(_TAX_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_SCOPE_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text and not _NEGATED_SCOPE_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NEGATED_SCOPE_RE.search(searchable) or not _TAX_CONTEXT_RE.search(searchable):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    return bool(
        _REQUIREMENT_RE.search(segment.text)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
    )


def _confidence(segment: _Segment, category: TaxExemptionCategory) -> TaxExemptionConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _REQUIREMENT_RE.search(segment.text) and (_TAX_CONTEXT_RE.search(searchable) or segment.section_context):
        return "high"
    if category in {"invoice_tax_suppression", "audit_evidence", "validation_status"} and _TAX_CONTEXT_RE.search(searchable):
        return "high"
    if segment.section_context or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceTaxExemptionRequirement, ...], source_count: int) -> dict[str, Any]:
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
        "tax_exemption",
        "compliance",
        "support",
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
    "SourceTaxExemptionRequirement",
    "SourceTaxExemptionRequirementsReport",
    "TaxExemptionCategory",
    "TaxExemptionConfidence",
    "build_source_tax_exemption_requirements",
    "derive_source_tax_exemption_requirements",
    "extract_source_tax_exemption_requirements",
    "generate_source_tax_exemption_requirements",
    "source_tax_exemption_requirements_to_dict",
    "source_tax_exemption_requirements_to_dicts",
    "source_tax_exemption_requirements_to_markdown",
    "summarize_source_tax_exemption_requirements",
]
