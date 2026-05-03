"""Extract source-level API CORS preflight requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


CORSPreflightCategory = Literal[
    "options_handling",
    "request_method_validation",
    "request_headers_validation",
    "allow_methods_response",
    "allow_headers_response",
    "max_age_caching",
    "preflight_failure_responses",
    "custom_header_support",
]
CORSPreflightMissingDetail = Literal["missing_allowed_methods", "missing_allowed_headers"]
CORSPreflightConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[CORSPreflightCategory, ...] = (
    "options_handling",
    "request_method_validation",
    "request_headers_validation",
    "allow_methods_response",
    "allow_headers_response",
    "max_age_caching",
    "preflight_failure_responses",
    "custom_header_support",
)
_MISSING_DETAIL_ORDER: tuple[CORSPreflightMissingDetail, ...] = (
    "missing_allowed_methods",
    "missing_allowed_headers",
)
_CONFIDENCE_ORDER: dict[CORSPreflightConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_CORS_PREFLIGHT_CONTEXT_RE = re.compile(
    r"\b(?:cors|cross[- ]origin|preflight|options request|options method|http options|"
    r"access-control-request-method|access-control-request-headers|"
    r"access-control-allow-methods|access-control-allow-headers|"
    r"access-control-max-age|preflight cache|custom headers?|"
    r"allowed methods?|allowed headers?|cors headers?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:cors|preflight|options|headers?|methods?|access[_ -]?control|"
    r"cross[_ -]?origin|api|rest|requirements?)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|include|return|respond|validate|handle|"
    r"preflight|options|acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:cors|preflight|options|cross[- ]origin|access-control)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:cors|preflight|options|cross[- ]origin|access-control)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_CORS_PREFLIGHT_RE = re.compile(
    r"\b(?:no cors|no preflight|no options|cors is out of scope|"
    r"preflight is out of scope|no cors work|no preflight handling)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:email options|user options|configuration options|settings options|"
    r"preferences|dropdown options|select options)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:options|get|post|put|patch|delete|head|"
    r"access-control-request-method|access-control-request-headers|"
    r"access-control-allow-methods|access-control-allow-headers|"
    r"access-control-max-age|content-type|authorization|"
    r"x-api-key|x-custom|bearer|3600|86400)\b",
    re.I,
)
_ALLOWED_METHODS_DETAIL_RE = re.compile(
    r"\b(?:get|post|put|patch|delete|head|options|"
    r"access-control-allow-methods|allowed methods?|http methods?)\b",
    re.I,
)
_ALLOWED_HEADERS_DETAIL_RE = re.compile(
    r"\b(?:content-type|authorization|x-api-key|x-custom|bearer|"
    r"access-control-allow-headers|allowed headers?|custom headers?)\b",
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
    "id",
    "source_id",
    "source_brief_id",
    "status",
    "created_by",
    "updated_by",
    "owner",
    "last_editor",
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
    "api",
    "rest",
    "cors",
    "preflight",
    "headers",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[CORSPreflightCategory, re.Pattern[str]] = {
    "options_handling": re.compile(
        r"\b(?:options request|options method|options endpoint|http options|handle options|"
        r"options handler|options response|respond to options)\b",
        re.I,
    ),
    "request_method_validation": re.compile(
        r"\b(?:access-control-request-method|request method|validate method|"
        r"check method|verify method|method validation)\b",
        re.I,
    ),
    "request_headers_validation": re.compile(
        r"\b(?:access-control-request-headers|request headers?|validate headers?|"
        r"check headers?|verify headers?|header validation)\b",
        re.I,
    ),
    "allow_methods_response": re.compile(
        r"\b(?:access-control-allow-methods|allow(?:ed)? methods?|"
        r"supported methods?|permitted methods?|get|post|put|patch|delete)\b",
        re.I,
    ),
    "allow_headers_response": re.compile(
        r"\b(?:access-control-allow-headers|allow(?:ed)? headers?|"
        r"supported headers?|permitted headers?|custom headers?|"
        r"content-type|authorization|x-api-key)\b",
        re.I,
    ),
    "max_age_caching": re.compile(
        r"\b(?:access-control-max-age|max-age|preflight cache|cache duration|"
        r"cache preflight|ttl|time to live|3600|86400)\b",
        re.I,
    ),
    "preflight_failure_responses": re.compile(
        r"\b(?:preflight fail(?:ure)?s?|preflight errors?|preflight reject|"
        r"invalid preflight|forbidden|403|405|"
        r"method not allowed|header not allowed|reject|return.{0,40}(?:403|405|error))\b",
        re.I,
    ),
    "custom_header_support": re.compile(
        r"\b(?:custom headers?|x-(?:api-key|custom|auth|request)|"
        r"non-standard headers?|proprietary headers?|vendor headers?)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[CORSPreflightCategory, tuple[str, ...]] = {
    "options_handling": ("api_platform", "backend"),
    "request_method_validation": ("api_platform", "backend"),
    "request_headers_validation": ("api_platform", "backend"),
    "allow_methods_response": ("api_platform", "backend"),
    "allow_headers_response": ("api_platform", "backend"),
    "max_age_caching": ("api_platform", "backend"),
    "preflight_failure_responses": ("api_platform", "backend"),
    "custom_header_support": ("api_platform", "backend"),
}
_PLANNING_NOTES: dict[CORSPreflightCategory, tuple[str, ...]] = {
    "options_handling": ("Implement OPTIONS request handlers for all CORS-enabled endpoints, ensuring proper response headers.",),
    "request_method_validation": ("Validate Access-Control-Request-Method against allowed methods before granting preflight approval.",),
    "request_headers_validation": ("Validate Access-Control-Request-Headers against allowed headers, rejecting unsupported custom headers.",),
    "allow_methods_response": ("Return Access-Control-Allow-Methods header with GET, POST, PUT, PATCH, DELETE, or other supported methods.",),
    "allow_headers_response": ("Return Access-Control-Allow-Headers with Content-Type, Authorization, and other permitted custom headers.",),
    "max_age_caching": ("Set Access-Control-Max-Age to cache preflight responses (e.g., 3600 or 86400 seconds) to reduce OPTIONS requests.",),
    "preflight_failure_responses": ("Return appropriate error responses (403/405) when preflight validation fails for method or header mismatch.",),
    "custom_header_support": ("Document and validate custom headers (X-API-Key, X-Custom-Auth) in preflight requests and CORS configuration.",),
}
_GAP_MESSAGES: dict[CORSPreflightMissingDetail, str] = {
    "missing_allowed_methods": "Specify which HTTP methods (GET, POST, PUT, PATCH, DELETE) are allowed in preflight responses.",
    "missing_allowed_headers": "Define which headers (Content-Type, Authorization, X-API-Key) are allowed in preflight responses.",
}


@dataclass(frozen=True, slots=True)
class SourceAPICORSPreflightRequirement:
    """One source-backed API CORS preflight requirement."""

    category: CORSPreflightCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: CORSPreflightConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> CORSPreflightCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> CORSPreflightCategory:
        """Compatibility view for extractors that expose concern naming."""
        return self.category

    @property
    def suggested_plan_impacts(self) -> tuple[str, ...]:
        """Compatibility view matching adjacent source extractors."""
        return self.planning_notes

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owners": list(self.suggested_owners),
            "planning_notes": list(self.planning_notes),
            "gap_messages": list(self.gap_messages),
        }


@dataclass(frozen=True, slots=True)
class SourceAPICORSPreflightRequirementsReport:
    """Source-level API CORS preflight requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceAPICORSPreflightRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAPICORSPreflightRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAPICORSPreflightRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return API CORS preflight requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API CORS Preflight Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
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
            "- Missing detail flags: " + ", ".join(self.summary.get("missing_detail_flags", [])),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source API CORS preflight requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes | Gap Messages |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell(', '.join(requirement.suggested_owners))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{_markdown_cell('; '.join(requirement.gap_messages))} |"
            )
        return "\n".join(lines)


def build_source_api_cors_preflight_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPICORSPreflightRequirementsReport:
    """Build an API CORS preflight requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    gap_flags = tuple(_missing_detail_flags(candidate.evidence for candidate in candidates))
    requirements = tuple(_merge_candidates(candidates, gap_flags))
    return SourceAPICORSPreflightRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements, gap_flags if requirements else ()),
    )


def summarize_source_api_cors_preflight_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAPICORSPreflightRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted API CORS preflight requirements."""
    if isinstance(source, SourceAPICORSPreflightRequirementsReport):
        return dict(source.summary)
    return build_source_api_cors_preflight_requirements(source).summary


def derive_source_api_cors_preflight_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPICORSPreflightRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_api_cors_preflight_requirements(source)


def generate_source_api_cors_preflight_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPICORSPreflightRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_api_cors_preflight_requirements(source)


def extract_source_api_cors_preflight_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceAPICORSPreflightRequirement, ...]:
    """Return API CORS preflight requirement records from brief-shaped input."""
    return build_source_api_cors_preflight_requirements(source).requirements


def source_api_cors_preflight_requirements_to_dict(
    report: SourceAPICORSPreflightRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API CORS preflight requirements report to a plain dictionary."""
    return report.to_dict()


source_api_cors_preflight_requirements_to_dict.__test__ = False


def source_api_cors_preflight_requirements_to_dicts(
    requirements: (
        tuple[SourceAPICORSPreflightRequirement, ...]
        | list[SourceAPICORSPreflightRequirement]
        | SourceAPICORSPreflightRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize API CORS preflight requirement records to dictionaries."""
    if isinstance(requirements, SourceAPICORSPreflightRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_cors_preflight_requirements_to_dicts.__test__ = False


def source_api_cors_preflight_requirements_to_markdown(
    report: SourceAPICORSPreflightRequirementsReport,
) -> str:
    """Render an API CORS preflight requirements report as Markdown."""
    return report.to_markdown()


source_api_cors_preflight_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: CORSPreflightCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: CORSPreflightConfidence


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
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
        if not _is_requirement(segment):
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = _categories(searchable)
        for category in _dedupe(categories):
            candidates.append(
                _Candidate(
                    category=category,
                    value=_value(category, segment.text),
                    source_field=segment.source_field,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    confidence=_confidence(segment),
                )
            )
    return candidates


def _has_global_no_scope(payload: Mapping[str, Any]) -> bool:
    for segment in _candidate_segments(payload):
        if segment.source_field.split("[", 1)[0].split(".", 1)[0] not in {
            "title",
            "summary",
            "body",
            "description",
            "scope",
            "non_goals",
            "constraints",
            "source_payload",
        }:
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        if _NO_CORS_PREFLIGHT_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(
    candidates: Iterable[_Candidate],
    gap_flags: tuple[CORSPreflightMissingDetail, ...],
) -> list[SourceAPICORSPreflightRequirement]:
    grouped: dict[CORSPreflightCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceAPICORSPreflightRequirement] = []
    gap_messages = tuple(_GAP_MESSAGES[flag] for flag in gap_flags)
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda field: (
                min(_CONFIDENCE_ORDER[item.confidence] for item in items if item.source_field == field),
                _field_category_rank(category, field),
                field.casefold(),
            ),
        )[0]
        requirements.append(
            SourceAPICORSPreflightRequirement(
                category=category,
                source_field=source_field,
                evidence=tuple(
                    sorted(
                        _dedupe_evidence(
                            item.evidence
                            for item in sorted(
                                items,
                                key=lambda item: (
                                    _field_category_rank(category, item.source_field),
                                    item.source_field.casefold(),
                                ),
                            )
                        ),
                        key=str.casefold,
                    )
                )[:5],
                confidence=confidence,
                value=_best_value(items),
                suggested_owners=_OWNER_SUGGESTIONS[category],
                planning_notes=_PLANNING_NOTES[category],
                gap_messages=gap_messages,
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.value or "",
            requirement.source_field.casefold(),
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
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            if str(key) in _IGNORED_FIELDS:
                continue
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _CORS_PREFLIGHT_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
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
                _CORS_PREFLIGHT_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = (
                [part]
                if _NEGATED_SCOPE_RE.search(part) and _CORS_PREFLIGHT_CONTEXT_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    field_words = _field_words(segment.source_field)
    if _NO_CORS_PREFLIGHT_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _UNRELATED_RE.search(searchable) and not _CORS_PREFLIGHT_CONTEXT_RE.search(searchable):
        return False
    if not (_CORS_PREFLIGHT_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _CORS_PREFLIGHT_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:include|included|return|returned|expose|exposed|follow|followed|implement|implemented)\b",
            segment.text,
            re.I,
        )
    )


def _categories(searchable: str) -> list[CORSPreflightCategory]:
    categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]
    return categories


def _missing_detail_flags(evidence_values: Iterable[str]) -> list[CORSPreflightMissingDetail]:
    text = " ".join(evidence_values)
    if not text:
        return []
    flags: list[CORSPreflightMissingDetail] = []
    if not _ALLOWED_METHODS_DETAIL_RE.search(text):
        flags.append("missing_allowed_methods")
    if not _ALLOWED_HEADERS_DETAIL_RE.search(text):
        flags.append("missing_allowed_headers")
    return [flag for flag in _MISSING_DETAIL_ORDER if flag in flags]


def _value(category: CORSPreflightCategory, text: str) -> str | None:
    if category == "options_handling":
        if match := re.search(r"\b(?P<value>options|http options|options request)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "request_method_validation":
        if match := re.search(r"\b(?P<value>access-control-request-method|request method)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "request_headers_validation":
        if match := re.search(r"\b(?P<value>access-control-request-headers|request headers?)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "allow_methods_response":
        if match := re.search(r"\b(?P<value>access-control-allow-methods|get|post|put|patch|delete)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "allow_headers_response":
        if match := re.search(r"\b(?P<value>access-control-allow-headers|content-type|authorization|x-api-key)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "max_age_caching":
        if match := re.search(r"\b(?P<value>access-control-max-age|max-age|3600|86400)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "preflight_failure_responses":
        if match := re.search(r"\b(?P<value>preflight fail(?:ure)?|403|405|forbidden|not allowed)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "custom_header_support":
        if match := re.search(r"\b(?P<value>x-api-key|x-custom|x-auth|custom headers?)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    ranked_values = sorted(
        ((index, item.value) for index, item in enumerate(items) if item.value),
        key=lambda indexed_value: (
            0 if _VALUE_RE.search(indexed_value[1]) else 1,
            indexed_value[0],
            len(indexed_value[1]),
            indexed_value[1].casefold(),
        ),
    )
    values = _dedupe(value for _, value in ranked_values)
    return values[0] if values else None


def _confidence(segment: _Segment) -> CORSPreflightConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _REQUIREMENT_RE.search(segment.text) and (
        segment.section_context
        or any(
            marker in segment.source_field.replace("-", "_").casefold()
            for marker in (
                "acceptance_criteria",
                "definition_of_done",
                "success_criteria",
                "scope",
                "api",
                "rest",
                "cors",
                "preflight",
                "headers",
                "requirements",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _CORS_PREFLIGHT_CONTEXT_RE.search(searchable):
        return "medium"
    if _CORS_PREFLIGHT_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(
    requirements: tuple[SourceAPICORSPreflightRequirement, ...],
    gap_flags: tuple[CORSPreflightMissingDetail, ...],
) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "categories": [requirement.category for requirement in requirements],
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "missing_detail_flags": list(gap_flags),
        "missing_detail_counts": {
            flag: sum(1 for requirement in requirements if _GAP_MESSAGES[flag] in requirement.gap_messages)
            for flag in _MISSING_DETAIL_ORDER
        },
        "gap_messages": [_GAP_MESSAGES[flag] for flag in gap_flags],
        "status": "ready_for_planning" if requirements and not gap_flags else "needs_cors_preflight_details" if requirements else "no_cors_preflight_language",
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
        "success_criteria",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "api",
        "rest",
        "cors",
        "preflight",
        "headers",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: CORSPreflightCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[CORSPreflightCategory, tuple[str, ...]] = {
        "options_handling": ("options", "http", "method"),
        "request_method_validation": ("request", "method", "validate"),
        "request_headers_validation": ("request", "headers", "validate"),
        "allow_methods_response": ("allow", "methods", "response"),
        "allow_headers_response": ("allow", "headers", "response"),
        "max_age_caching": ("max", "age", "cache"),
        "preflight_failure_responses": ("preflight", "failure", "error"),
        "custom_header_support": ("custom", "header", "x-"),
    }
    return 0 if any(marker in field_words for marker in markers[category]) else 1


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
    "CORSPreflightCategory",
    "CORSPreflightConfidence",
    "CORSPreflightMissingDetail",
    "SourceAPICORSPreflightRequirement",
    "SourceAPICORSPreflightRequirementsReport",
    "build_source_api_cors_preflight_requirements",
    "derive_source_api_cors_preflight_requirements",
    "extract_source_api_cors_preflight_requirements",
    "generate_source_api_cors_preflight_requirements",
    "summarize_source_api_cors_preflight_requirements",
    "source_api_cors_preflight_requirements_to_dict",
    "source_api_cors_preflight_requirements_to_dicts",
    "source_api_cors_preflight_requirements_to_markdown",
]
