"""Extract source-level cookie consent and tracking requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


CookieConsentRequirementType = Literal[
    "consent_banner",
    "cookie_category",
    "tracking_pixel",
    "analytics_opt_in",
    "regional_consent",
    "preference_center",
    "withdrawal",
    "audit_evidence",
    "retention_expiration",
]
CookieConsentConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[CookieConsentRequirementType, ...] = (
    "consent_banner",
    "cookie_category",
    "tracking_pixel",
    "analytics_opt_in",
    "regional_consent",
    "preference_center",
    "withdrawal",
    "audit_evidence",
    "retention_expiration",
)
_CONFIDENCE_ORDER: dict[CookieConsentConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_GUIDANCE: dict[CookieConsentRequirementType, str] = {
    "consent_banner": "Confirm banner copy, placement, buttons, default state, and blocking behavior before consent.",
    "cookie_category": "Confirm category taxonomy, default states, vendor mapping, and required cookie treatment.",
    "tracking_pixel": "Confirm which pixels, tags, and third-party scripts are blocked until consent.",
    "analytics_opt_in": "Confirm analytics consent mode, opt-in event timing, and downstream measurement impact.",
    "regional_consent": "Confirm regional rules for GDPR, ePrivacy, CPRA/CCPA, UK, and other applicable markets.",
    "preference_center": "Confirm preference center entry points, category controls, persistence, and accessibility.",
    "withdrawal": "Confirm withdrawal flow, revocation propagation, cookie deletion, and downstream signal handling.",
    "audit_evidence": "Confirm consent event schema, proof export, retention, and audit ownership.",
    "retention_expiration": "Confirm cookie lifetimes, consent record retention, renewal cadence, and expiry behavior.",
}

_TYPE_PATTERNS: dict[CookieConsentRequirementType, re.Pattern[str]] = {
    "consent_banner": re.compile(
        r"\b(?:cookie banner|consent banner|cookie notice|consent notice|cookie popup|"
        r"banner copy|accept all|reject all|manage choices|cookie wall)\b",
        re.I,
    ),
    "cookie_category": re.compile(
        r"\b(?:cookie categor(?:y|ies)|cookie taxonomy|necessary cookies?|essential cookies?|"
        r"functional cookies?|performance cookies?|analytics cookies?|marketing cookies?|"
        r"advertising cookies?|strictly necessary|category toggles?)\b",
        re.I,
    ),
    "tracking_pixel": re.compile(
        r"\b(?:tracking pixels?|marketing pixels?|ad pixels?|pixel tags?|facebook pixel|meta pixel|"
        r"linkedin insight|google tag|gtm|tag manager|third[- ]party tags?|third[- ]party scripts?|"
        r"session replay|heatmap)\b",
        re.I,
    ),
    "analytics_opt_in": re.compile(
        r"\b(?:analytics opt[- ]?in|opt[- ]?in analytics|analytics consent|measurement consent|"
        r"consent mode|google analytics|ga4|product analytics|tracking default[- ]?off|"
        r"analytics default[- ]?off)\b",
        re.I,
    ),
    "regional_consent": re.compile(
        r"\b(?:gdpr|eprivacy|eu|eea|uk gdpr|uk users?|cpra|ccpa|california|quebec|lgpd|"
        r"regional consent|geo[- ]?specific consent|geolocation|geoIP|region(?:al)? rules?|"
        r"jurisdiction|market[- ]specific)\b",
        re.I,
    ),
    "preference_center": re.compile(
        r"\b(?:preference center|privacy center|cookie settings|consent settings|manage preferences|"
        r"preference panel|category controls?|toggle preferences)\b",
        re.I,
    ),
    "withdrawal": re.compile(
        r"\b(?:withdraw consent|withdrawal|revoke consent|revocation|opt[- ]?out|change consent|"
        r"delete cookies?|clear cookies?|turn off tracking|disable tracking)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit evidence|consent evidence|proof of consent|consent log|consent event|"
        r"consent receipt|audit log|export consent|attestation|record consent|history)\b",
        re.I,
    ),
    "retention_expiration": re.compile(
        r"\b(?:retention|retain|expire|expiration|expires?|ttl|lifetime|renewal|renew consent|"
        r"re[- ]?prompt|cookie duration|consent duration|\d+\s*(?:days?|months?|years?))\b",
        re.I,
    ),
}
_COOKIE_CONTEXT_RE = re.compile(
    r"\b(?:cookie consent|cookie banner|consent management|cmp|cookies?|tracking|analytics consent|"
    r"privacy preferences?|preference center|tracking pixels?|third[- ]party tags?|gdpr|eprivacy|cpra|ccpa)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:cookie[_ -]?consent|cookie[_ -]?banner|consent[_ -]?management|cmp|cookies?|tracking|"
    r"analytics|pixels?|tags?|preference[_ -]?center|privacy[_ -]?center|withdraw(?:al)?|"
    r"revocation|regions?|jurisdictions?|gdpr|ccpa|cpra|audit|evidence|retention|expiration|ttl|"
    r"source[_ -]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|block|"
    r"defer|disable|enable|show|display|collect|capture|record|log|export|retain|expire|"
    r"delete|clear|honou?r|respect|support|provide|allow|acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,140}"
    r"\b(?:cookie consent|cookie banner|cookies?|tracking|analytics|pixels?|tags?|cmp|preference center)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|impact|for this release)\b|"
    r"\b(?:cookie consent|cookie banner|cookies?|tracking|analytics|pixels?|tags?|cmp|preference center)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|no impact|non-goal|non goal)\b",
    re.I,
)
_REGION_RE = re.compile(r"\b(?:gdpr|eprivacy|eu|eea|uk|cpra|ccpa|california|quebec|lgpd|brazil)\b", re.I)
_CATEGORY_RE = re.compile(
    r"\b(?:strictly necessary|necessary|essential|functional|performance|analytics|marketing|advertising|personalization)\b"
    r"(?:\s+cookies?)?",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:accept all|reject all|manage choices|preference center|opt[- ]?in|opt[- ]?out|"
    r"prior consent|default[- ]?off|blocked until consent|consent mode|google analytics|ga4|"
    r"meta pixel|facebook pixel|gtm|tag manager|\d+\s*(?:days?|months?|years?)|ttl|expire|retention)\b",
    re.I,
)
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SPACE_RE = re.compile(r"\s+")
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
    "privacy",
    "compliance",
    "analytics",
    "tracking",
    "cookie_consent",
    "cookies",
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
    "status",
}


@dataclass(frozen=True, slots=True)
class SourceCookieConsentRequirement:
    """One source-backed cookie consent or tracking requirement."""

    source_brief_id: str | None
    requirement_type: CookieConsentRequirementType
    requirement_text: str
    value: str | None = None
    categories: tuple[str, ...] = field(default_factory=tuple)
    regions: tuple[str, ...] = field(default_factory=tuple)
    source_field: str | None = None
    source_fields: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: CookieConsentConfidence = "medium"
    missing_detail_guidance: str | None = None

    @property
    def category(self) -> CookieConsentRequirementType:
        """Compatibility view for extractors that expose category naming."""
        return self.requirement_type

    @property
    def requirement_category(self) -> CookieConsentRequirementType:
        """Compatibility view for extractors that expose requirement_category naming."""
        return self.requirement_type

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "requirement_text": self.requirement_text,
            "value": self.value,
            "categories": list(self.categories),
            "regions": list(self.regions),
            "source_field": self.source_field,
            "source_fields": list(self.source_fields),
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "missing_detail_guidance": self.missing_detail_guidance,
        }


@dataclass(frozen=True, slots=True)
class SourceCookieConsentRequirementsReport:
    """Source-level cookie consent requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceCookieConsentRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceCookieConsentRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceCookieConsentRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return cookie consent requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Cookie Consent Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        type_counts = self.summary.get("requirement_type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Requirement type counts: "
            + ", ".join(f"{item} {type_counts.get(item, 0)}" for item in _TYPE_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{item} {confidence_counts.get(item, 0)}" for item in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source cookie consent requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Requirement Type | Requirement | Value | Categories | Regions | Source Field | Source Fields | Confidence | Missing Detail Guidance | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.requirement_type)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{_markdown_cell(', '.join(requirement.categories))} | "
                f"{_markdown_cell(', '.join(requirement.regions))} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(', '.join(requirement.source_fields))} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.missing_detail_guidance or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_cookie_consent_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceCookieConsentRequirementsReport:
    """Extract source-level cookie consent requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceCookieConsentRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_cookie_consent_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceCookieConsentRequirementsReport:
    """Compatibility alias for building a cookie consent requirements report."""
    return build_source_cookie_consent_requirements(source)


def generate_source_cookie_consent_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceCookieConsentRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_cookie_consent_requirements(source)


def derive_source_cookie_consent_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceCookieConsentRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_cookie_consent_requirements(source)


def summarize_source_cookie_consent_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceCookieConsentRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted cookie consent requirements."""
    if isinstance(source_or_result, SourceCookieConsentRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_cookie_consent_requirements(source_or_result).summary


def source_cookie_consent_requirements_to_dict(
    report: SourceCookieConsentRequirementsReport,
) -> dict[str, Any]:
    """Serialize a cookie consent requirements report to a plain dictionary."""
    return report.to_dict()


source_cookie_consent_requirements_to_dict.__test__ = False


def source_cookie_consent_requirements_to_dicts(
    requirements: (
        tuple[SourceCookieConsentRequirement, ...]
        | list[SourceCookieConsentRequirement]
        | SourceCookieConsentRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize cookie consent requirement records to dictionaries."""
    if isinstance(requirements, SourceCookieConsentRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_cookie_consent_requirements_to_dicts.__test__ = False


def source_cookie_consent_requirements_to_markdown(
    report: SourceCookieConsentRequirementsReport,
) -> str:
    """Render a cookie consent requirements report as Markdown."""
    return report.to_markdown()


source_cookie_consent_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    requirement_type: CookieConsentRequirementType
    requirement_text: str
    value: str | None
    categories: tuple[str, ...]
    regions: tuple[str, ...]
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: CookieConsentConfidence


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


def _candidates_for_briefs(brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        if _brief_out_of_scope(payload):
            continue
        for segment in _candidate_segments(payload):
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            if _NEGATED_RE.search(searchable) or not _is_requirement(segment):
                continue
            requirement_types = [
                requirement_type
                for requirement_type in _TYPE_ORDER
                if _TYPE_PATTERNS[requirement_type].search(searchable)
            ]
            for requirement_type in _dedupe(requirement_types):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        requirement_type=requirement_type,
                        requirement_text=_requirement_text(segment.text),
                        value=_value(requirement_type, segment.text),
                        categories=_categories(segment.text),
                        regions=_regions(searchable),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        matched_terms=tuple(_matched_terms(_TYPE_PATTERNS[requirement_type], searchable)),
                        confidence=_confidence(segment, requirement_type),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceCookieConsentRequirement]:
    grouped: dict[tuple[str | None, CookieConsentRequirementType], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.requirement_type), []).append(candidate)

    requirements: list[SourceCookieConsentRequirement] = []
    for (source_brief_id, requirement_type), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceCookieConsentRequirement(
                source_brief_id=source_brief_id,
                requirement_type=requirement_type,
                requirement_text=best.requirement_text,
                value=_joined_details(item.value for item in items),
                categories=tuple(_dedupe(category for item in items for category in item.categories))[:8],
                regions=tuple(_dedupe(region for item in items for region in item.regions))[:8],
                source_field=best.source_field,
                source_fields=tuple(_dedupe(item.source_field for item in items)),
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                matched_terms=tuple(
                    _dedupe(
                        term
                        for item in sorted(items, key=lambda candidate: candidate.source_field.casefold())
                        for term in item.matched_terms
                    )
                )[:8],
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                missing_detail_guidance=_GUIDANCE[requirement_type],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _TYPE_ORDER.index(requirement.requirement_type),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.source_field or "",
            requirement.requirement_text.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    global_context = _brief_cookie_context(payload)
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], global_context)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], global_context)
    return segments


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        if _has_structured_shape(value):
            text = _structured_text(value)
            if text:
                segments.append(_Segment(source_field, text, True))
            return
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _COOKIE_CONTEXT_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _TYPE_PATTERNS.values())
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
            section_context = inherited_context or bool(_COOKIE_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
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
    has_context = bool(
        _COOKIE_CONTEXT_RE.search(searchable)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
    )
    if not has_context or not any(pattern.search(searchable) for pattern in _TYPE_PATTERNS.values()):
        return False
    return bool(
        _REQUIREMENT_RE.search(searchable)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
        or _VALUE_RE.search(searchable)
    )


def _confidence(segment: _Segment, requirement_type: CookieConsentRequirementType) -> CookieConsentConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    has_requirement = bool(_REQUIREMENT_RE.search(searchable))
    has_structured_context = bool(
        segment.section_context
        or any(
            marker in segment.source_field.replace("-", "_").casefold()
            for marker in (
                "acceptance_criteria",
                "definition_of_done",
                "success_criteria",
                "scope",
                "privacy",
                "compliance",
                "analytics",
                "tracking",
                "cookie",
                "source_payload",
            )
        )
    )
    has_detail = bool(_value(requirement_type, segment.text) or _categories(segment.text) or _regions(searchable))
    if _TYPE_PATTERNS[requirement_type].search(searchable) and has_requirement and (has_structured_context or has_detail):
        return "high"
    if has_requirement or has_structured_context or has_detail:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceCookieConsentRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "requirement_types": [requirement.requirement_type for requirement in requirements],
        "requirement_type_counts": {
            requirement_type: sum(1 for requirement in requirements if requirement.requirement_type == requirement_type)
            for requirement_type in _TYPE_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "category_counts": {
            category: sum(1 for requirement in requirements if category in requirement.categories)
            for category in ("necessary", "functional", "performance", "analytics", "marketing", "advertising")
        },
        "region_counts": {
            region: sum(1 for requirement in requirements if region in requirement.regions)
            for region in ("gdpr", "eprivacy", "eu", "eea", "uk", "ccpa", "cpra", "california")
        },
        "status": "ready_for_cookie_consent_planning" if requirements else "no_cookie_consent_language",
    }


def _brief_out_of_scope(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("title", "summary", "scope", "non_goals", "constraints", "source_payload")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_NEGATED_RE.search(scoped_text))


def _brief_cookie_context(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("title", "domain", "summary", "workflow_context", "product_surface")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_COOKIE_CONTEXT_RE.search(scoped_text) and not _NEGATED_RE.search(scoped_text))


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    return bool(
        keys
        & {
            "banner",
            "consent_banner",
            "categories",
            "cookie_categories",
            "analytics",
            "analytics_opt_in",
            "pixels",
            "tracking_pixels",
            "preference_center",
            "withdrawal",
            "revocation",
            "regions",
            "regional_consent",
            "evidence",
            "audit",
            "retention",
            "expiration",
            "ttl",
        }
    )


def _structured_text(item: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = ", ".join(_strings(value)) if isinstance(value, (list, tuple, set)) else _clean_text(value)
        if text:
            parts.append(f"{key}: {text}")
    return "; ".join(parts)


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "target_user",
        "buyer",
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
        "product_surface",
        "requirements",
        "constraints",
        "scope",
        "non_goals",
        "assumptions",
        "success_criteria",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "privacy",
        "compliance",
        "analytics",
        "tracking",
        "cookie_consent",
        "cookies",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _value(requirement_type: CookieConsentRequirementType, text: str) -> str | None:
    key_patterns: dict[CookieConsentRequirementType, re.Pattern[str]] = {
        "consent_banner": re.compile(r"(?:^|;\s*)(?:banner|consent_banner)\s*:\s*([^;]+)", re.I),
        "cookie_category": re.compile(r"(?:^|;\s*)(?:categories|cookie_categories)\s*:\s*([^;]+)", re.I),
        "tracking_pixel": re.compile(r"(?:^|;\s*)(?:pixels|tracking_pixels|tags)\s*:\s*([^;]+)", re.I),
        "analytics_opt_in": re.compile(r"(?:^|;\s*)(?:analytics|analytics_opt_in)\s*:\s*([^;]+)", re.I),
        "regional_consent": re.compile(r"(?:^|;\s*)(?:regions|regional_consent|jurisdictions)\s*:\s*([^;]+)", re.I),
        "preference_center": re.compile(r"(?:^|;\s*)(?:preference_center|cookie_settings|privacy_center)\s*:\s*([^;]+)", re.I),
        "withdrawal": re.compile(r"(?:^|;\s*)(?:withdrawal|revocation)\s*:\s*([^;]+)", re.I),
        "audit_evidence": re.compile(r"(?:^|;\s*)(?:evidence|audit|audit_evidence)\s*:\s*([^;]+)", re.I),
        "retention_expiration": re.compile(r"(?:^|;\s*)(?:retention|expiration|ttl|duration)\s*:\s*([^;]+)", re.I),
    }
    patterns: dict[CookieConsentRequirementType, tuple[re.Pattern[str], ...]] = {
        "consent_banner": (
            re.compile(r"\b(?:accept all|reject all|manage choices)\b", re.I),
            re.compile(r"\b(?:cookie banner|consent banner)\b", re.I),
        ),
        "cookie_category": (_CATEGORY_RE,),
        "tracking_pixel": (
            re.compile(r"\b(?:meta pixel|facebook pixel|tracking pixel|gtm|google tag|tag manager|session replay)\b", re.I),
        ),
        "analytics_opt_in": (
            re.compile(r"\b(?:analytics opt[- ]?in|consent mode|google analytics|ga4|prior consent|default[- ]?off)\b", re.I),
        ),
        "regional_consent": (_REGION_RE,),
        "preference_center": (
            re.compile(r"\b(?:preference center|cookie settings|manage preferences)\b", re.I),
        ),
        "withdrawal": (
            re.compile(r"\b(?:withdraw consent|revoke consent|opt[- ]?out|delete cookies?|clear cookies?)\b", re.I),
        ),
        "audit_evidence": (
            re.compile(r"\b(?:consent log|consent event|proof of consent|audit evidence|consent receipt|export)\b", re.I),
        ),
        "retention_expiration": (
            re.compile(r"\b\d+\s*(?:days?|months?|years?)\b", re.I),
            re.compile(r"\b(?:ttl|retention|expire|expiration|renew consent)\b", re.I),
        ),
    }
    if match := key_patterns[requirement_type].search(text):
        scoped_text = match.group(1)
        for pattern in patterns[requirement_type]:
            if scoped_match := pattern.search(scoped_text):
                return _detail(scoped_match.group(0)).casefold()
        return _detail(scoped_text)
    for pattern in patterns[requirement_type]:
        if match := pattern.search(text):
            return _detail(match.group(0)).casefold()
    return None


def _categories(text: str) -> tuple[str, ...]:
    values: list[str] = []
    for match in _CATEGORY_RE.finditer(text):
        value = re.sub(r"\s+cookies?$", "", _detail(match.group(0)), flags=re.I).casefold()
        if value == "essential":
            value = "necessary"
        if value == "strictly necessary":
            value = "necessary"
        if value == "performance":
            value = "performance"
        values.append(value)
    return tuple(_dedupe(values))


def _regions(text: str) -> tuple[str, ...]:
    aliases = {
        "uk gdpr": "uk",
        "uk users": "uk",
        "california": "california",
        "brazil": "lgpd",
    }
    values: list[str] = []
    for match in _REGION_RE.finditer(text):
        value = _detail(match.group(0)).casefold()
        values.append(aliases.get(value, value))
    return tuple(_dedupe(values))


def _matched_terms(pattern: re.Pattern[str], text: str) -> list[str]:
    return _dedupe(_detail(match.group(0)).casefold() for match in pattern.finditer(text))


def _joined_details(values: Iterable[str | None]) -> str | None:
    details = _dedupe(value for value in values if value)
    return ", ".join(details) if details else None


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, int, str]:
    detail_count = sum(bool(value) for value in (candidate.value, candidate.categories, candidate.regions, candidate.matched_terms))
    return (
        detail_count,
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int("[" in candidate.source_field),
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        candidate.evidence,
    )


def _strings(value: Any) -> list[str]:
    if value is None or isinstance(value, (bytes, bytearray)):
        return []
    if isinstance(value, str):
        text = _clean_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _clean_text(value)
    return [text] if text else []


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


def _detail(value: Any) -> str:
    return _clean_text(value).strip("`'\" :;,.")[:160].rstrip()


def _requirement_text(value: str) -> str:
    text = _clean_text(value)
    if len(text) > 220:
        text = f"{text[:217].rstrip()}..."
    return text


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
    "CookieConsentConfidence",
    "CookieConsentRequirementType",
    "SourceCookieConsentRequirement",
    "SourceCookieConsentRequirementsReport",
    "build_source_cookie_consent_requirements",
    "derive_source_cookie_consent_requirements",
    "extract_source_cookie_consent_requirements",
    "generate_source_cookie_consent_requirements",
    "source_cookie_consent_requirements_to_dict",
    "source_cookie_consent_requirements_to_dicts",
    "source_cookie_consent_requirements_to_markdown",
    "summarize_source_cookie_consent_requirements",
]
