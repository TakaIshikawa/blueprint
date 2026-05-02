"""Extract source-level search indexing requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SearchIndexingRequirementMode = Literal[
    "search",
    "indexing",
    "reindex",
    "ranking",
    "filters",
    "facets",
    "autocomplete",
    "synonyms",
    "typo_tolerance",
    "eventual_consistency",
    "index_freshness",
]
SearchIndexingRequirementConfidence = Literal["high", "medium", "low"]
SearchIndexingMissingDetail = Literal[
    "missing_indexed_fields",
    "missing_freshness_target",
    "missing_ranking_filter_behavior",
    "missing_backfill_strategy",
    "missing_failure_behavior",
]
_T = TypeVar("_T")

_MODE_ORDER: tuple[SearchIndexingRequirementMode, ...] = (
    "search",
    "indexing",
    "reindex",
    "ranking",
    "filters",
    "facets",
    "autocomplete",
    "synonyms",
    "typo_tolerance",
    "eventual_consistency",
    "index_freshness",
)
_MISSING_DETAIL_ORDER: tuple[SearchIndexingMissingDetail, ...] = (
    "missing_indexed_fields",
    "missing_freshness_target",
    "missing_ranking_filter_behavior",
    "missing_backfill_strategy",
    "missing_failure_behavior",
)
_CONFIDENCE_ORDER: dict[SearchIndexingRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"acceptance|done when|before launch|cannot ship|support|allow|provide|"
    r"index|search|rank|filter|facet|autocomplete|suggest|sync|refresh|reindex)\b",
    re.I,
)
_SEARCH_CONTEXT_RE = re.compile(
    r"\b(?:search|searchable|index|indexed|indexing|indices|reindex|backfill|"
    r"ranking|ranked|relevance|relevancy|boost|sort order|filters?|filtering|"
    r"facets?|facet counts?|autocomplete|typeahead|suggestions?|synonyms?|"
    r"typos?|typo tolerance|fuzzy match|fuzziness|spell ?check|eventual consistency|"
    r"eventually consistent|index freshness|freshness|staleness|stale results|"
    r"search results?|query results?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:search|index|indexing|reindex|ranking|relevance|filters?|facets?|autocomplete|"
    r"typeahead|synonyms?|typo|freshness|consistency|requirements?|acceptance|criteria|"
    r"definition_of_done|constraints?|risks?|metadata|source_payload)",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:search|index(?:ing)?|reindex|ranking|filters?|facets?|"
    r"autocomplete|synonyms?|typo tolerance|freshness).*?"
    r"\b(?:in scope|required|needed|changes?)\b",
    re.I,
)
_SURFACE_RE = re.compile(
    r"\b(?P<surface>(?:global|site|app|admin|customer|merchant|operator|support|"
    r"catalog|product|order|ticket|document|knowledge base|workspace|team|user|"
    r"people|message|conversation|checkout|marketplace|inventory|content|help center)"
    r"\s+(?:search|results?|autocomplete|typeahead|index|indexing|catalog|lookup|"
    r"directory|ranking|filters?|facets?))\b",
    re.I,
)
_INDEXED_FIELDS_RE = re.compile(
    r"\b(?:indexed fields?|fields? to index|index fields?|searchable fields?|"
    r"index(?:es|ing)?\s+(?:the\s+)?(?:fields?|columns?|attributes?)|"
    r"index\s+(?:title|name|description|body|sku|tags?|status|owner|email|metadata)\b|"
    r"(?:title|name|description|body|sku|tags?|status|owner|email|metadata)\s+"
    r"(?:must|should|need(?:s)? to)?\s*(?:be\s+)?(?:indexed|searchable))\b",
    re.I,
)
_FRESHNESS_TARGET_RE = re.compile(
    r"\b(?:within|under|less than|no more than|every|after)\s+\d+\s*"
    r"(?:ms|milliseconds?|s|sec(?:ond)?s?|m|min(?:ute)?s?|h|hours?|days?)\b|"
    r"\b(?:near real[- ]time|real[- ]time|same request|immediate(?:ly)?|hourly|daily|"
    r"eventual(?:ly)? consistent within)\b",
    re.I,
)
_RANKING_FILTER_BEHAVIOR_RE = re.compile(
    r"\b(?:rank(?:ing|ed)? by|sort(?:ed)? by|boost|relevance|filter(?:ing)? by|"
    r"filters?\s+(?:must|should|need(?:s)? to)?\s*support|support\s+[^.?!]{0,60}\s+filtering|"
    r"facet(?:ing)? by|facet counts?|pinned results?|exact matches?|recency|popularity|"
    r"permissions? filter|tenant filter)\b",
    re.I,
)
_BACKFILL_RE = re.compile(
    r"\b(?:backfill|rebuild|bulk reindex|full reindex|migration|migrate existing|"
    r"existing records?|historical records?|catch up|replay)\b",
    re.I,
)
_FAILURE_RE = re.compile(
    r"\b(?:fail(?:ure|ed)?|fallback|retry|dead letter|dlq|alert|monitor|partial outage|"
    r"degraded|stale banner|stale results|rollback|repair|poison message)\b",
    re.I,
)
_MODE_PATTERNS: dict[SearchIndexingRequirementMode, re.Pattern[str]] = {
    "search": re.compile(r"\b(?:search|searchable|search results?|query results?)\b", re.I),
    "indexing": re.compile(
        r"\b(?:indexing|indexed|index fields?|search index|indices|"
        r"index\s+(?:title|name|description|body|sku|tags?|status|owner|email|metadata))\b",
        re.I,
    ),
    "reindex": re.compile(r"\b(?:reindex|re-index|rebuild index|bulk reindex|full reindex)\b", re.I),
    "ranking": re.compile(r"\b(?:ranking|ranked|relevance|boost|sort order|pinned results?)\b", re.I),
    "filters": re.compile(r"\b(?:filter|filters|filtering|filtered)\b", re.I),
    "facets": re.compile(r"\b(?:facet|facets|faceted|facet counts?)\b", re.I),
    "autocomplete": re.compile(r"\b(?:autocomplete|auto-complete|typeahead|suggestions?)\b", re.I),
    "synonyms": re.compile(r"\b(?:synonym|synonyms|alias terms?|equivalent terms?)\b", re.I),
    "typo_tolerance": re.compile(
        r"\b(?:typo tolerance|typos?|fuzzy match|fuzziness|spell ?check|misspell(?:ing)?s?)\b",
        re.I,
    ),
    "eventual_consistency": re.compile(
        r"\b(?:eventual consistency|eventually consistent|async(?:hronous)? indexing|"
        r"stale results?|propagation delay)\b",
        re.I,
    ),
    "index_freshness": re.compile(
        r"\b(?:index freshness|freshness target|fresh within|staleness|refresh(?:ed)? within|"
        r"near real[- ]time|real[- ]time indexing)\b",
        re.I,
    ),
}
_GENERIC_SEARCH_REQUIREMENT_RE = re.compile(
    r"^(?:general\s+)?(?:search|search indexing|indexing|discovery|search implementation)\s+"
    r"(?:requirements?|behavior)\.?$|^validate search behavior\.?$",
    re.I,
)
_PLANNING_NOTES: dict[SearchIndexingRequirementMode, str] = {
    "search": "Define searchable surfaces, query semantics, permissions, and empty-result behavior.",
    "indexing": "Specify indexed fields, source of truth, update triggers, and schema ownership.",
    "reindex": "Plan backfill, rebuild safety, replay strategy, throttling, and validation checks.",
    "ranking": "Document ranking inputs, boosts, tie-breakers, and relevance acceptance cases.",
    "filters": "Specify supported filters, default states, permission constraints, and query behavior.",
    "facets": "Define facet fields, count semantics, limits, and interaction with filters.",
    "autocomplete": "Specify suggestion source, latency target, matching rules, and fallback behavior.",
    "synonyms": "Document synonym ownership, rollout process, locale scope, and evaluation examples.",
    "typo_tolerance": "Define fuzzy matching limits, exact-match priority, and precision safeguards.",
    "eventual_consistency": "Set consistency expectations, stale-result handling, and user-visible messaging.",
    "index_freshness": "Set freshness target, monitoring signal, update path, and breach response.",
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
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "data_requirements",
    "architecture_notes",
    "search",
    "indexing",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_IGNORED_FIELDS = {
    "id",
    "source_brief_id",
    "source_id",
    "source_project",
    "source_entity_type",
    "created_at",
    "updated_at",
    "source_links",
}


@dataclass(frozen=True, slots=True)
class SourceSearchIndexingRequirement:
    """One source-backed search indexing requirement."""

    source_brief_id: str | None
    search_surface: str
    requirement_mode: SearchIndexingRequirementMode
    missing_detail_flags: tuple[SearchIndexingMissingDetail, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: SearchIndexingRequirementConfidence = "medium"
    planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "search_surface": self.search_surface,
            "requirement_mode": self.requirement_mode,
            "missing_detail_flags": list(self.missing_detail_flags),
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceSearchIndexingRequirementsReport:
    """Source-level search indexing requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceSearchIndexingRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSearchIndexingRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceSearchIndexingRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
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
        """Return search indexing requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Search Indexing Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        mode_counts = self.summary.get("requirement_mode_counts", {})
        missing_counts = self.summary.get("missing_detail_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Requirement mode counts: "
            + ", ".join(f"{mode} {mode_counts.get(mode, 0)}" for mode in _MODE_ORDER),
            "- Missing detail counts: "
            + ", ".join(
                f"{flag} {missing_counts.get(flag, 0)}" for flag in _MISSING_DETAIL_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source search indexing requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Surface | Mode | Confidence | Missing Details | Source Field Paths | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.search_surface)} | "
                f"{requirement.requirement_mode} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(', '.join(requirement.missing_detail_flags))} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.planning_note)} |"
            )
        return "\n".join(lines)


def build_source_search_indexing_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSearchIndexingRequirementsReport:
    """Extract source-level search indexing requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceSearchIndexingRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_search_indexing_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSearchIndexingRequirementsReport:
    """Compatibility alias for building a search indexing requirements report."""
    return build_source_search_indexing_requirements(source)


def generate_source_search_indexing_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSearchIndexingRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_search_indexing_requirements(source)


def derive_source_search_indexing_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSearchIndexingRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_search_indexing_requirements(source)


def summarize_source_search_indexing_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceSearchIndexingRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted search indexing requirements."""
    if isinstance(source_or_result, SourceSearchIndexingRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_search_indexing_requirements(source_or_result).summary


def source_search_indexing_requirements_to_dict(
    report: SourceSearchIndexingRequirementsReport,
) -> dict[str, Any]:
    """Serialize a search indexing requirements report to a plain dictionary."""
    return report.to_dict()


source_search_indexing_requirements_to_dict.__test__ = False


def source_search_indexing_requirements_to_dicts(
    requirements: (
        tuple[SourceSearchIndexingRequirement, ...]
        | list[SourceSearchIndexingRequirement]
        | SourceSearchIndexingRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize search indexing requirement records to dictionaries."""
    if isinstance(requirements, SourceSearchIndexingRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_search_indexing_requirements_to_dicts.__test__ = False


def source_search_indexing_requirements_to_markdown(
    report: SourceSearchIndexingRequirementsReport,
) -> str:
    """Render a search indexing requirements report as Markdown."""
    return report.to_markdown()


source_search_indexing_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    search_surface: str
    requirement_mode: SearchIndexingRequirementMode
    missing_detail_flags: tuple[SearchIndexingMissingDetail, ...]
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]
    confidence: SearchIndexingRequirementConfidence


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
    if isinstance(
        source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)
    ) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
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
            modes = [
                mode for mode in _MODE_ORDER if _MODE_PATTERNS[mode].search(segment.text)
            ]
            for mode in _dedupe(modes):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        search_surface=_search_surface(segment.source_field, segment.text),
                        requirement_mode=mode,
                        missing_detail_flags=_missing_detail_flags(searchable, mode),
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        source_field_path=segment.source_field,
                        matched_terms=_matched_terms(mode, searchable),
                        confidence=_confidence(segment),
                    )
                )
    return candidates


def _merge_candidates(
    candidates: Iterable[_Candidate],
) -> list[SourceSearchIndexingRequirement]:
    grouped: dict[
        tuple[str | None, str, SearchIndexingRequirementMode], list[_Candidate]
    ] = {}
    for candidate in candidates:
        key = (
            candidate.source_brief_id,
            candidate.search_surface.casefold(),
            candidate.requirement_mode,
        )
        grouped.setdefault(key, []).append(candidate)

    requirements: list[SourceSearchIndexingRequirement] = []
    for (_source_brief_id, _surface_key, mode), items in grouped.items():
        source_brief_id = items[0].source_brief_id
        surface = sorted({item.search_surface for item in items}, key=str.casefold)[0]
        evidence = tuple(
            sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold)
        )[:5]
        source_field_paths = tuple(
            sorted(_dedupe(item.source_field_path for item in items), key=str.casefold)
        )
        matched_terms = tuple(
            sorted(
                _dedupe(term for item in items for term in item.matched_terms),
                key=str.casefold,
            )
        )
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        present_flags = {
            flag for item in items for flag in item.missing_detail_flags if flag
        }
        missing_detail_flags = tuple(
            flag for flag in _MISSING_DETAIL_ORDER if flag in present_flags
        )
        requirements.append(
            SourceSearchIndexingRequirement(
                source_brief_id=source_brief_id,
                search_surface=surface,
                requirement_mode=mode,
                missing_detail_flags=missing_detail_flags,
                evidence=evidence,
                source_field_paths=source_field_paths,
                matched_terms=matched_terms,
                confidence=confidence,
                planning_note=_PLANNING_NOTES[mode],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _MODE_ORDER.index(requirement.requirement_mode),
            requirement.search_surface.casefold(),
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
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _SEARCH_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text in _segments(text):
            segments.append(_Segment(source_field, segment_text, field_context))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for raw_line in value.splitlines() or [value]:
        cleaned = _clean_text(raw_line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(raw_line) or _CHECKBOX_RE.match(raw_line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append(text)
    return segments


def _is_requirement(segment: _Segment) -> bool:
    if _NEGATED_SCOPE_RE.search(segment.text):
        return False
    if _GENERIC_SEARCH_REQUIREMENT_RE.match(segment.text):
        return False
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if not _SEARCH_CONTEXT_RE.search(searchable):
        return False
    if segment.source_field == "title" and not re.search(
        r"\b(?:must|shall|required|requires?|need(?:s)? to|should|ensure|support|"
        r"index|search|rank|filter|facet|autocomplete|suggest|sync|refresh|reindex)\b",
        segment.text,
        re.I,
    ):
        return False
    if segment.section_context:
        return True
    return bool(_REQUIRED_RE.search(segment.text))


def _search_surface(source_field: str, text: str) -> str:
    searchable = f"{_field_words(source_field)} {text}"
    if match := _SURFACE_RE.search(searchable):
        surface = _clean_text(match.group("surface")).casefold()
        surface = re.sub(
            r"\s+(?:results?|index|indexing|lookup|directory|ranking|filters?|facets?)$",
            " search",
            surface,
        )
        return surface
    return "unspecified search surface"


def _missing_detail_flags(
    searchable: str,
    mode: SearchIndexingRequirementMode,
) -> tuple[SearchIndexingMissingDetail, ...]:
    flags: list[SearchIndexingMissingDetail] = []
    if not _INDEXED_FIELDS_RE.search(searchable):
        flags.append("missing_indexed_fields")
    if mode in {"indexing", "eventual_consistency", "index_freshness"} and not (
        _FRESHNESS_TARGET_RE.search(searchable)
    ):
        flags.append("missing_freshness_target")
    if mode in {"search", "ranking", "filters", "facets", "autocomplete"} and not (
        _RANKING_FILTER_BEHAVIOR_RE.search(searchable)
    ):
        flags.append("missing_ranking_filter_behavior")
    if mode in {"indexing", "reindex"} and not _BACKFILL_RE.search(searchable):
        flags.append("missing_backfill_strategy")
    if not _FAILURE_RE.search(searchable):
        flags.append("missing_failure_behavior")
    return tuple(flags)


def _matched_terms(
    mode: SearchIndexingRequirementMode,
    text: str,
) -> tuple[str, ...]:
    return tuple(
        _dedupe(
            _clean_text(match.group(0)).casefold()
            for match in _MODE_PATTERNS[mode].finditer(text)
        )
    )


def _confidence(segment: _Segment) -> SearchIndexingRequirementConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    detail_count = sum(
        1
        for pattern in (
            _INDEXED_FIELDS_RE,
            _FRESHNESS_TARGET_RE,
            _RANKING_FILTER_BEHAVIOR_RE,
            _BACKFILL_RE,
            _FAILURE_RE,
        )
        if pattern.search(searchable)
    )
    if _REQUIRED_RE.search(segment.text) and (detail_count or segment.section_context):
        return "high"
    if segment.section_context or _REQUIRED_RE.search(segment.text) or detail_count:
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceSearchIndexingRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "requirement_modes": [
            requirement.requirement_mode for requirement in requirements
        ],
        "requirement_mode_counts": {
            mode: sum(
                1 for requirement in requirements if requirement.requirement_mode == mode
            )
            for mode in _MODE_ORDER
        },
        "missing_detail_counts": {
            flag: sum(
                1
                for requirement in requirements
                if flag in requirement.missing_detail_flags
            )
            for flag in _MISSING_DETAIL_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "search_surfaces": sorted(
            {requirement.search_surface for requirement in requirements},
            key=str.casefold,
        ),
        "status": "ready_for_planning" if requirements else "no_search_indexing_language",
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
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "data_requirements",
        "architecture_notes",
        "search",
        "indexing",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
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
    "SearchIndexingMissingDetail",
    "SearchIndexingRequirementConfidence",
    "SearchIndexingRequirementMode",
    "SourceSearchIndexingRequirement",
    "SourceSearchIndexingRequirementsReport",
    "build_source_search_indexing_requirements",
    "derive_source_search_indexing_requirements",
    "extract_source_search_indexing_requirements",
    "generate_source_search_indexing_requirements",
    "source_search_indexing_requirements_to_dict",
    "source_search_indexing_requirements_to_dicts",
    "source_search_indexing_requirements_to_markdown",
    "summarize_source_search_indexing_requirements",
]
