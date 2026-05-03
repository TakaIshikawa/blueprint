"""Extract source-level API hypermedia and HATEOAS requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


HypermediaCategory = Literal[
    "hal_links",
    "jsonapi_relationships",
    "link_relations",
    "uri_templates",
    "hypermedia_controls",
    "state_transitions",
    "link_discovery",
    "embedded_resources",
]
HypermediaMissingDetail = Literal["missing_link_format", "missing_relation_types"]
HypermediaConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[HypermediaCategory, ...] = (
    "hal_links",
    "jsonapi_relationships",
    "link_relations",
    "uri_templates",
    "hypermedia_controls",
    "state_transitions",
    "link_discovery",
    "embedded_resources",
)
_MISSING_DETAIL_ORDER: tuple[HypermediaMissingDetail, ...] = (
    "missing_link_format",
    "missing_relation_types",
)
_CONFIDENCE_ORDER: dict[HypermediaConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_HYPERMEDIA_CONTEXT_RE = re.compile(
    r"\b(?:hypermedia|hateoas|hal|hypertext|json:?api|link relations?|uri templates?|"
    r"_links|_embedded|relationships?|self link|href|templated|"
    r"rfc\s*6570|link headers?|navigation links?|resource links?|"
    r"rest level [34]|rest maturity|discoverable|self-?documenting)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:hypermedia|hateoas|hal|links?|relationships?|uri[_ -]?templates?|"
    r"embedded|resources?|navigation|discoverable|api|rest|requirements?)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|include|return|expose|follow|implement|"
    r"hal|json:?api|hateoas|link|href|templated|relationships?|embedded|"
    r"self link|navigation|discover|acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:hypermedia|hateoas|hal|json:?api|link relations?|uri templates?|"
    r"_links|_embedded|relationships?|discoverable)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:hypermedia|hateoas|hal|json:?api|link relations?|uri templates?|"
    r"_links|_embedded|relationships?|discoverable)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_HYPERMEDIA_RE = re.compile(
    r"\b(?:no hypermedia|no hateoas|no hal|hypermedia is out of scope|"
    r"hateoas is out of scope|no link relations|no uri templates|"
    r"no hypermedia work|no discoverable links)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:hyperlink|web link|anchor tag|email link|permalink|shortlink|"
    r"deep link|magic link|unsubscribe link|reset link|verification link)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:hal|json:?api|_links|_embedded|relationships?|self|next|prev|first|last|"
    r"related|collection|item|href|templated|rfc\s*6570|link header|"
    r"application/hal\+json|application/vnd\.api\+json)\b",
    re.I,
)
_LINK_FORMAT_DETAIL_RE = re.compile(
    r"\b(?:hal|json:?api|_links|_embedded|relationships?|application/hal\+json|"
    r"application/vnd\.api\+json|link format|link structure)\b",
    re.I,
)
_RELATION_TYPE_DETAIL_RE = re.compile(
    r"\b(?:self|next|prev|previous|first|last|related|collection|item|"
    r"parent|child|up|down|edit|delete|create|rel=|link relation)\b",
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
    "hypermedia",
    "hateoas",
    "links",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[HypermediaCategory, re.Pattern[str]] = {
    "hal_links": re.compile(
        r"\b(?:hal|hypertext application language|_links|_embedded|application/hal\+json|"
        r"hal\+json|hal format|hal specification)\b",
        re.I,
    ),
    "jsonapi_relationships": re.compile(
        r"\b(?:json:?api|json api|jsonapi|application/vnd\.api\+json|vnd\.api\+json|"
        r"relationships?|included|type and id|resource identifier)\b",
        re.I,
    ),
    "link_relations": re.compile(
        r"\b(?:link relations?|rel=|relation types?|self link|next link|prev(?:ious)? link|"
        r"related link|collection link|item link|iana link relations?|"
        r"custom relations?|link semantics)\b",
        re.I,
    ),
    "uri_templates": re.compile(
        r"\b(?:uri templates?|rfc\s*6570|templated|template expansion|"
        r"url templates?|path templates?|query templates?|{[^}]+}|variable expansion)\b",
        re.I,
    ),
    "hypermedia_controls": re.compile(
        r"\b(?:hypermedia controls?|affordances?|actions?|forms?|methods?|"
        r"allowed methods?|link methods?|http methods?|operations?|capabilities)\b",
        re.I,
    ),
    "state_transitions": re.compile(
        r"\b(?:state transitions?|resource states?|workflow states?|transitions?|"
        r"state machine|resource lifecycle|status changes?|state changes?)\b",
        re.I,
    ),
    "link_discovery": re.compile(
        r"\b(?:link discovery|discover(?:able|ability)|self[- ]?describing|"
        r"api discovery|resource discovery|link headers?|options response|"
        r"api exploration|browsable|navigable)\b",
        re.I,
    ),
    "embedded_resources": re.compile(
        r"\b(?:_embedded|embedded resources?|embedded entities?|embedded objects?|"
        r"inline(?:d)? resources?|nested resources?|expansion|expand|"
        r"resource embedding|sideload(?:ed|ing)?)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[HypermediaCategory, tuple[str, ...]] = {
    "hal_links": ("api_platform", "backend"),
    "jsonapi_relationships": ("api_platform", "backend"),
    "link_relations": ("api_platform", "backend"),
    "uri_templates": ("api_platform", "backend"),
    "hypermedia_controls": ("api_platform", "backend"),
    "state_transitions": ("api_platform", "backend"),
    "link_discovery": ("api_platform", "developer_experience"),
    "embedded_resources": ("api_platform", "backend"),
}
_PLANNING_NOTES: dict[HypermediaCategory, tuple[str, ...]] = {
    "hal_links": ("Define HAL _links structure, href formatting, and optional templated/type/deprecation properties.",),
    "jsonapi_relationships": ("Specify JSON:API relationships with type/id, links (self/related), and optional included resources.",),
    "link_relations": ("Document link relation types (self, next, prev, related), IANA registration, and custom rel semantics.",),
    "uri_templates": ("Plan URI template expansion per RFC 6570, variable substitution, and client template processing.",),
    "hypermedia_controls": ("Define hypermedia controls, allowed methods, affordances, and conditional action visibility.",),
    "state_transitions": ("Document resource state transitions, available actions per state, and workflow progression links.",),
    "link_discovery": ("Plan link discovery mechanisms, OPTIONS responses, link headers, and self-documenting API entry points.",),
    "embedded_resources": ("Specify embedded resource structure, expansion parameters, circular reference handling, and nesting limits.",),
}
_GAP_MESSAGES: dict[HypermediaMissingDetail, str] = {
    "missing_link_format": "Specify link format (HAL, JSON:API, custom) and link structure conventions.",
    "missing_relation_types": "Define link relation types (self, next, prev, related) and relation semantics.",
}


@dataclass(frozen=True, slots=True)
class SourceAPIHypermediaRequirement:
    """One source-backed API hypermedia requirement."""

    category: HypermediaCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: HypermediaConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> HypermediaCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> HypermediaCategory:
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
class SourceAPIHypermediaRequirementsReport:
    """Source-level API hypermedia requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceAPIHypermediaRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAPIHypermediaRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAPIHypermediaRequirement, ...]:
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
        """Return API hypermedia requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Hypermedia Requirements Report"
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
            lines.extend(["", "No source API hypermedia requirements were inferred."])
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


def build_source_api_hypermedia_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPIHypermediaRequirementsReport:
    """Build an API hypermedia requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    gap_flags = tuple(_missing_detail_flags(candidate.evidence for candidate in candidates))
    requirements = tuple(_merge_candidates(candidates, gap_flags))
    return SourceAPIHypermediaRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements, gap_flags if requirements else ()),
    )


def summarize_source_api_hypermedia_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAPIHypermediaRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted API hypermedia requirements."""
    if isinstance(source, SourceAPIHypermediaRequirementsReport):
        return dict(source.summary)
    return build_source_api_hypermedia_requirements(source).summary


def derive_source_api_hypermedia_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPIHypermediaRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_api_hypermedia_requirements(source)


def generate_source_api_hypermedia_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPIHypermediaRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_api_hypermedia_requirements(source)


def extract_source_api_hypermedia_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceAPIHypermediaRequirement, ...]:
    """Return API hypermedia requirement records from brief-shaped input."""
    return build_source_api_hypermedia_requirements(source).requirements


def source_api_hypermedia_requirements_to_dict(
    report: SourceAPIHypermediaRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API hypermedia requirements report to a plain dictionary."""
    return report.to_dict()


source_api_hypermedia_requirements_to_dict.__test__ = False


def source_api_hypermedia_requirements_to_dicts(
    requirements: (
        tuple[SourceAPIHypermediaRequirement, ...]
        | list[SourceAPIHypermediaRequirement]
        | SourceAPIHypermediaRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize API hypermedia requirement records to dictionaries."""
    if isinstance(requirements, SourceAPIHypermediaRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_hypermedia_requirements_to_dicts.__test__ = False


def source_api_hypermedia_requirements_to_markdown(
    report: SourceAPIHypermediaRequirementsReport,
) -> str:
    """Render an API hypermedia requirements report as Markdown."""
    return report.to_markdown()


source_api_hypermedia_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: HypermediaCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: HypermediaConfidence


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
        if _NO_HYPERMEDIA_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(
    candidates: Iterable[_Candidate],
    gap_flags: tuple[HypermediaMissingDetail, ...],
) -> list[SourceAPIHypermediaRequirement]:
    grouped: dict[HypermediaCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceAPIHypermediaRequirement] = []
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
            SourceAPIHypermediaRequirement(
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
                _STRUCTURED_FIELD_RE.search(key_text) or _HYPERMEDIA_CONTEXT_RE.search(key_text)
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
                _HYPERMEDIA_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
                if _NEGATED_SCOPE_RE.search(part) and _HYPERMEDIA_CONTEXT_RE.search(part)
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
    if _NO_HYPERMEDIA_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _UNRELATED_RE.search(searchable) and not _HYPERMEDIA_CONTEXT_RE.search(searchable):
        return False
    if not (_HYPERMEDIA_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _HYPERMEDIA_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:include|included|return|returned|expose|exposed|follow|followed|implement|implemented)\b",
            segment.text,
            re.I,
        )
    )


def _categories(searchable: str) -> list[HypermediaCategory]:
    categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]
    return categories


def _missing_detail_flags(evidence_values: Iterable[str]) -> list[HypermediaMissingDetail]:
    text = " ".join(evidence_values)
    if not text:
        return []
    flags: list[HypermediaMissingDetail] = []
    if not _LINK_FORMAT_DETAIL_RE.search(text):
        flags.append("missing_link_format")
    if not _RELATION_TYPE_DETAIL_RE.search(text):
        flags.append("missing_relation_types")
    return [flag for flag in _MISSING_DETAIL_ORDER if flag in flags]


def _value(category: HypermediaCategory, text: str) -> str | None:
    if category == "hal_links":
        if match := re.search(r"\b(?P<value>hal|_links|_embedded|application/hal\+json)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "jsonapi_relationships":
        if match := re.search(r"\b(?P<value>json:?api|relationships?|application/vnd\.api\+json)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "link_relations":
        if match := re.search(r"\b(?P<value>self|next|prev(?:ious)?|first|last|related|collection|item|rel=)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "uri_templates":
        if match := re.search(r"\b(?P<value>rfc\s*6570|uri templates?|templated)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "hypermedia_controls":
        if match := re.search(r"\b(?P<value>hypermedia controls?|affordances?|actions?|methods?)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "state_transitions":
        if match := re.search(r"\b(?P<value>state transitions?|resource states?|workflow|lifecycle)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "link_discovery":
        if match := re.search(r"\b(?P<value>discover(?:able|y)?|self[- ]?describing|browsable|navigable)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "embedded_resources":
        if match := re.search(r"\b(?P<value>_embedded|embedded|sideload(?:ed|ing)?|expand(?:ed)?)\b", text, re.I):
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


def _confidence(segment: _Segment) -> HypermediaConfidence:
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
                "hypermedia",
                "hateoas",
                "links",
                "requirements",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _HYPERMEDIA_CONTEXT_RE.search(searchable):
        return "medium"
    if _HYPERMEDIA_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(
    requirements: tuple[SourceAPIHypermediaRequirement, ...],
    gap_flags: tuple[HypermediaMissingDetail, ...],
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
        "status": "ready_for_planning" if requirements and not gap_flags else "needs_hypermedia_details" if requirements else "no_hypermedia_language",
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
        "hypermedia",
        "hateoas",
        "links",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: HypermediaCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[HypermediaCategory, tuple[str, ...]] = {
        "hal_links": ("hal", "links", "embedded"),
        "jsonapi_relationships": ("jsonapi", "json api", "relationships"),
        "link_relations": ("relations", "rel", "link"),
        "uri_templates": ("uri", "template", "rfc"),
        "hypermedia_controls": ("controls", "affordances", "actions"),
        "state_transitions": ("state", "transitions", "workflow"),
        "link_discovery": ("discovery", "discoverable", "browsable"),
        "embedded_resources": ("embedded", "sideload", "expand"),
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
    "HypermediaCategory",
    "HypermediaConfidence",
    "HypermediaMissingDetail",
    "SourceAPIHypermediaRequirement",
    "SourceAPIHypermediaRequirementsReport",
    "build_source_api_hypermedia_requirements",
    "derive_source_api_hypermedia_requirements",
    "extract_source_api_hypermedia_requirements",
    "generate_source_api_hypermedia_requirements",
    "summarize_source_api_hypermedia_requirements",
    "source_api_hypermedia_requirements_to_dict",
    "source_api_hypermedia_requirements_to_dicts",
    "source_api_hypermedia_requirements_to_markdown",
]
