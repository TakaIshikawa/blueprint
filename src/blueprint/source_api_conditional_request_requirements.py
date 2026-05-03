"""Extract source-level API conditional request requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ConditionalRequestCategory = Literal[
    "if_match_precondition",
    "if_unmodified_since_validation",
    "precondition_failed_responses",
    "conditional_mutations",
    "optimistic_locking",
    "lost_update_prevention",
    "if_range_requests",
    "conditional_idempotency",
]
ConditionalRequestMissingDetail = Literal["missing_precondition_logic", "missing_conflict_handling"]
ConditionalRequestConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[ConditionalRequestCategory, ...] = (
    "if_match_precondition",
    "if_unmodified_since_validation",
    "precondition_failed_responses",
    "conditional_mutations",
    "optimistic_locking",
    "lost_update_prevention",
    "if_range_requests",
    "conditional_idempotency",
)
_MISSING_DETAIL_ORDER: tuple[ConditionalRequestMissingDetail, ...] = (
    "missing_precondition_logic",
    "missing_conflict_handling",
)
_CONFIDENCE_ORDER: dict[ConditionalRequestConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_CONDITIONAL_REQUEST_CONTEXT_RE = re.compile(
    r"\b(?:conditional request|if-match|if-unmodified-since|if-range|"
    r"412 precondition failed|precondition|optimistic lock|"
    r"lost update|concurrent update|version conflict|"
    r"conditional put|conditional patch|conditional delete|"
    r"idempotent|idempotency|safe retry)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:conditional|precondition|if-?match|optimistic|lock|"
    r"conflict|concurrent|version|idempoten|mutation|"
    r"header|headers?|api|rest|requirements?)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|include|return|expose|follow|implement|"
    r"if-match|if-unmodified-since|412|precondition|optimistic|lost update|"
    r"conditional|idempotent|acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:conditional request|if-match|if-unmodified-since|precondition|"
    r"412|optimistic lock|lost update|conditional put|conditional patch)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:conditional request|if-match|if-unmodified-since|precondition|"
    r"412|optimistic lock|lost update|conditional put|conditional patch)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_CONDITIONAL_REQUEST_RE = re.compile(
    r"\b(?:no conditional requests?|no if-match|no preconditions?|"
    r"conditional requests? are out of scope|if-match is out of scope|"
    r"no optimistic lock|no lost update prevention)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:weather condition|health condition|terms and conditions|"
    r"business condition|filter condition|where condition|sql condition)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:if-match|if-unmodified-since|if-range|412|precondition failed|"
    r"optimistic lock|lost update|version|concurrent|conflict)\b",
    re.I,
)
_PRECONDITION_LOGIC_DETAIL_RE = re.compile(
    r"\b(?:if-match|if-unmodified-since|precondition check|validate|"
    r"compare|comparison|etag match|timestamp check)\b",
    re.I,
)
_CONFLICT_HANDLING_DETAIL_RE = re.compile(
    r"\b(?:412|precondition failed|conflict resolution|retry|"
    r"version conflict|concurrent conflict|error handling)\b",
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
    "conditional",
    "precondition",
    "optimistic_lock",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[ConditionalRequestCategory, re.Pattern[str]] = {
    "if_match_precondition": re.compile(
        r"\b(?:if-match|if match precondition|if match header|if match check|"
        r"if match validation|require if-match|etag precondition|"
        r"match precondition|exact match requirement)\b",
        re.I,
    ),
    "if_unmodified_since_validation": re.compile(
        r"\b(?:if-unmodified-since|if unmodified since|unmodified since check|"
        r"timestamp precondition|modification time check|"
        r"unmodified validation|time-based precondition)\b",
        re.I,
    ),
    "precondition_failed_responses": re.compile(
        r"\b(?:412 precondition failed|412 response|precondition failed|"
        r"precondition error|failed precondition|precondition violation|"
        r"precondition check failed|condition not met)\b",
        re.I,
    ),
    "conditional_mutations": re.compile(
        r"\b(?:conditional put|conditional patch|conditional delete|"
        r"conditional update|conditional mutation|conditional write|"
        r"conditional modify|safe update|guarded update)\b",
        re.I,
    ),
    "optimistic_locking": re.compile(
        r"\b(?:optimistic lock|optimistic locking|optimistic concurrency|"
        r"version-based lock|etag-based lock|last write wins|"
        r"optimistic concurrency control|occ)\b",
        re.I,
    ),
    "lost_update_prevention": re.compile(
        r"\b(?:lost update|lost update prevention|prevent lost update|"
        r"update conflict|concurrent write|write conflict|"
        r"concurrent modification|race condition prevention)\b",
        re.I,
    ),
    "if_range_requests": re.compile(
        r"\b(?:if-range|if range header|if range request|range precondition|"
        r"conditional range|partial content precondition|"
        r"range validation|conditional partial)\b",
        re.I,
    ),
    "conditional_idempotency": re.compile(
        r"\b(?:conditional idempotency|idempotent conditional|safe retry|"
        r"retry safety|idempotent update|duplicate prevention|"
        r"replay protection|request deduplication)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[ConditionalRequestCategory, tuple[str, ...]] = {
    "if_match_precondition": ("api_platform", "backend"),
    "if_unmodified_since_validation": ("api_platform", "backend"),
    "precondition_failed_responses": ("api_platform", "backend"),
    "conditional_mutations": ("api_platform", "backend"),
    "optimistic_locking": ("api_platform", "backend"),
    "lost_update_prevention": ("api_platform", "backend"),
    "if_range_requests": ("api_platform", "backend"),
    "conditional_idempotency": ("api_platform", "backend"),
}
_PLANNING_NOTES: dict[ConditionalRequestCategory, tuple[str, ...]] = {
    "if_match_precondition": ("Define If-Match precondition checking, ETag comparison logic, and exact match semantics.",),
    "if_unmodified_since_validation": ("Specify If-Unmodified-Since validation, timestamp comparison, and time precision handling.",),
    "precondition_failed_responses": ("Document 412 Precondition Failed response conditions, error payload format, and client retry guidance.",),
    "conditional_mutations": ("Plan conditional PUT/PATCH/DELETE operations, precondition requirements, and safe update workflows.",),
    "optimistic_locking": ("Define optimistic locking strategy, version tracking, and concurrent modification detection.",),
    "lost_update_prevention": ("Specify lost update prevention mechanisms, conflict detection, and resolution strategies.",),
    "if_range_requests": ("Plan If-Range partial request handling, range preconditions, and conditional partial content delivery.",),
    "conditional_idempotency": ("Document conditional operation idempotency, safe retry logic, and duplicate request prevention.",),
}
_GAP_MESSAGES: dict[ConditionalRequestMissingDetail, str] = {
    "missing_precondition_logic": "Specify precondition validation logic (If-Match, If-Unmodified-Since) and comparison semantics.",
    "missing_conflict_handling": "Define conflict handling strategy and 412 Precondition Failed response behavior.",
}


@dataclass(frozen=True, slots=True)
class SourceAPIConditionalRequestRequirement:
    """One source-backed API conditional request requirement."""

    category: ConditionalRequestCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: ConditionalRequestConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> ConditionalRequestCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> ConditionalRequestCategory:
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
class SourceAPIConditionalRequestRequirementsReport:
    """Source-level API conditional request requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceAPIConditionalRequestRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAPIConditionalRequestRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAPIConditionalRequestRequirement, ...]:
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
        """Return API conditional request requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Conditional Request Requirements Report"
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
            lines.extend(["", "No source API conditional request requirements were inferred."])
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


def build_source_api_conditional_request_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPIConditionalRequestRequirementsReport:
    """Build an API conditional request requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    gap_flags = tuple(_missing_detail_flags(candidate.evidence for candidate in candidates))
    requirements = tuple(_merge_candidates(candidates, gap_flags))
    return SourceAPIConditionalRequestRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements, gap_flags if requirements else ()),
    )


def summarize_source_api_conditional_request_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAPIConditionalRequestRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted API conditional request requirements."""
    if isinstance(source, SourceAPIConditionalRequestRequirementsReport):
        return dict(source.summary)
    return build_source_api_conditional_request_requirements(source).summary


def derive_source_api_conditional_request_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPIConditionalRequestRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_api_conditional_request_requirements(source)


def generate_source_api_conditional_request_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPIConditionalRequestRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_api_conditional_request_requirements(source)


def extract_source_api_conditional_request_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceAPIConditionalRequestRequirement, ...]:
    """Return API conditional request requirement records from brief-shaped input."""
    return build_source_api_conditional_request_requirements(source).requirements


def source_api_conditional_request_requirements_to_dict(
    report: SourceAPIConditionalRequestRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API conditional request requirements report to a plain dictionary."""
    return report.to_dict()


source_api_conditional_request_requirements_to_dict.__test__ = False


def source_api_conditional_request_requirements_to_dicts(
    requirements: (
        tuple[SourceAPIConditionalRequestRequirement, ...]
        | list[SourceAPIConditionalRequestRequirement]
        | SourceAPIConditionalRequestRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize API conditional request requirement records to dictionaries."""
    if isinstance(requirements, SourceAPIConditionalRequestRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_conditional_request_requirements_to_dicts.__test__ = False


def source_api_conditional_request_requirements_to_markdown(
    report: SourceAPIConditionalRequestRequirementsReport,
) -> str:
    """Render an API conditional request requirements report as Markdown."""
    return report.to_markdown()


source_api_conditional_request_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: ConditionalRequestCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: ConditionalRequestConfidence


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
        if _NO_CONDITIONAL_REQUEST_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(
    candidates: Iterable[_Candidate],
    gap_flags: tuple[ConditionalRequestMissingDetail, ...],
) -> list[SourceAPIConditionalRequestRequirement]:
    grouped: dict[ConditionalRequestCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceAPIConditionalRequestRequirement] = []
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
            SourceAPIConditionalRequestRequirement(
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
                _STRUCTURED_FIELD_RE.search(key_text) or _CONDITIONAL_REQUEST_CONTEXT_RE.search(key_text)
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
                _CONDITIONAL_REQUEST_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
                if _NEGATED_SCOPE_RE.search(part) and _CONDITIONAL_REQUEST_CONTEXT_RE.search(part)
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
    if _NO_CONDITIONAL_REQUEST_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _UNRELATED_RE.search(searchable) and not _CONDITIONAL_REQUEST_CONTEXT_RE.search(searchable):
        return False
    if not (_CONDITIONAL_REQUEST_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _CONDITIONAL_REQUEST_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:include|included|return|returned|expose|exposed|follow|followed|implement|implemented)\b",
            segment.text,
            re.I,
        )
    )


def _categories(searchable: str) -> list[ConditionalRequestCategory]:
    categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]
    return categories


def _missing_detail_flags(evidence_values: Iterable[str]) -> list[ConditionalRequestMissingDetail]:
    text = " ".join(evidence_values)
    if not text:
        return []
    flags: list[ConditionalRequestMissingDetail] = []
    if not _PRECONDITION_LOGIC_DETAIL_RE.search(text):
        flags.append("missing_precondition_logic")
    if not _CONFLICT_HANDLING_DETAIL_RE.search(text):
        flags.append("missing_conflict_handling")
    return [flag for flag in _MISSING_DETAIL_ORDER if flag in flags]


def _value(category: ConditionalRequestCategory, text: str) -> str | None:
    if category == "if_match_precondition":
        if match := re.search(r"\b(?P<value>if-match|precondition|etag)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "if_unmodified_since_validation":
        if match := re.search(r"\b(?P<value>if-unmodified-since|unmodified|timestamp)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "precondition_failed_responses":
        if match := re.search(r"\b(?P<value>412|precondition failed|failed)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "conditional_mutations":
        if match := re.search(r"\b(?P<value>conditional put|conditional patch|conditional delete|conditional)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "optimistic_locking":
        if match := re.search(r"\b(?P<value>optimistic lock|optimistic|occ)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "lost_update_prevention":
        if match := re.search(r"\b(?P<value>lost update|conflict|concurrent)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "if_range_requests":
        if match := re.search(r"\b(?P<value>if-range|range|partial)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "conditional_idempotency":
        if match := re.search(r"\b(?P<value>idempotent|idempotency|safe retry|retry)\b", text, re.I):
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


def _confidence(segment: _Segment) -> ConditionalRequestConfidence:
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
                "conditional",
                "precondition",
                "optimistic_lock",
                "requirements",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _CONDITIONAL_REQUEST_CONTEXT_RE.search(searchable):
        return "medium"
    if _CONDITIONAL_REQUEST_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(
    requirements: tuple[SourceAPIConditionalRequestRequirement, ...],
    gap_flags: tuple[ConditionalRequestMissingDetail, ...],
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
        "status": "ready_for_planning" if requirements and not gap_flags else "needs_conditional_request_details" if requirements else "no_conditional_request_language",
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
        "conditional",
        "precondition",
        "optimistic_lock",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: ConditionalRequestCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[ConditionalRequestCategory, tuple[str, ...]] = {
        "if_match_precondition": ("if match", "precondition", "etag"),
        "if_unmodified_since_validation": ("if unmodified", "unmodified since", "timestamp"),
        "precondition_failed_responses": ("412", "precondition failed", "failed"),
        "conditional_mutations": ("conditional", "mutation", "update"),
        "optimistic_locking": ("optimistic", "lock", "occ"),
        "lost_update_prevention": ("lost update", "conflict", "concurrent"),
        "if_range_requests": ("if range", "range", "partial"),
        "conditional_idempotency": ("idempotent", "idempotency", "retry"),
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
    "ConditionalRequestCategory",
    "ConditionalRequestConfidence",
    "ConditionalRequestMissingDetail",
    "SourceAPIConditionalRequestRequirement",
    "SourceAPIConditionalRequestRequirementsReport",
    "build_source_api_conditional_request_requirements",
    "derive_source_api_conditional_request_requirements",
    "extract_source_api_conditional_request_requirements",
    "generate_source_api_conditional_request_requirements",
    "summarize_source_api_conditional_request_requirements",
    "source_api_conditional_request_requirements_to_dict",
    "source_api_conditional_request_requirements_to_dicts",
    "source_api_conditional_request_requirements_to_markdown",
]
