"""Extract source-level API versioning requirements from implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


APIVersioningFindingType = Literal[
    "versioned_endpoint",
    "compatibility_window",
    "deprecation_timeline",
    "client_migration",
    "backwards_compatibility",
    "unknown_versioning",
]
APIVersioningReadiness = Literal["ready_for_planning", "needs_clarification"]
APIVersioningConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[APIVersioningFindingType, ...] = (
    "versioned_endpoint",
    "compatibility_window",
    "deprecation_timeline",
    "client_migration",
    "backwards_compatibility",
    "unknown_versioning",
)
_CONFIDENCE_ORDER: dict[APIVersioningConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_TIME_WINDOW_RE = re.compile(
    r"\b(?:\d+\s*(?:days?|weeks?|months?|quarters?|years?)|"
    r"\d{4}-\d{2}-\d{2}|q[1-4]\s*\d{4}|"
    r"(?:one|two|three|six|twelve|eighteen)\s+months?)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|need(?:s)?|should|ensure|support|maintain|"
    r"preserve|deprecat(?:e|ed|ion)|sunset|retire|migrate|upgrade|acceptance|"
    r"done when|before launch|cannot ship)\b",
    re.I,
)
_VERSIONING_CONTEXT_RE = re.compile(
    r"\b(?:api versioning|versioned apis?|versioned endpoints?|endpoint versions?|"
    r"version negotiation|api contract versions?|v\d+\s+apis?|/v\d+(?:/|\b)|"
    r"\bv[1-9]\d?\b|deprecated endpoints?|deprecation|sunset|"
    r"compatibility windows?|backwards? compatible|backward compatibility|"
    r"client migration|migrate clients?|consumer migration|old clients?|legacy clients?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:title|summary|requirements?|acceptance|criteria|constraints?|api|contract|"
    r"compatib|deprecat|migration|version|metadata|source[-_ ]?payload)",
    re.I,
)
_UNKNOWN_RE = re.compile(
    r"\b(?:tbd|unknown|unclear|not specified|unspecified|decide later|needs clarification|"
    r"to be defined|to be determined|open question)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:api\s+)?(?:versioning|versioned endpoints?|"
    r"deprecation|client migration|compatibility window).*?\b(?:required|needed|in scope|changes?)\b",
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
_TYPE_PATTERNS: dict[APIVersioningFindingType, re.Pattern[str]] = {
    "versioned_endpoint": re.compile(
        r"\b(?:v\d+\s+apis?|v\d+\s+endpoints?|/v\d+(?:/|\b)|api\s+v\d+|"
        r"versioned apis?|versioned endpoints?|endpoint versions?|api versions?|"
        r"version negotiation)\b",
        re.I,
    ),
    "compatibility_window": re.compile(
        r"\b(?:compatibility windows?|support windows?|migration windows?|"
        r"overlap windows?|sunset windows?|parallel support|support both versions?)\b",
        re.I,
    ),
    "deprecation_timeline": re.compile(
        r"\b(?:deprecated endpoints?|deprecat(?:e|ed|ion) timeline|sunset timeline|"
        r"sunset date|sunset endpoints?|retire endpoints?|retirement date|eol|end of life)\b",
        re.I,
    ),
    "client_migration": re.compile(
        r"\b(?:client migration|migrate clients?|consumer migration|partner migration|"
        r"sdk migration|upgrade clients?|client upgrade|migration guide|notify clients?)\b",
        re.I,
    ),
    "backwards_compatibility": re.compile(
        r"\b(?:backwards? compatible|backward compatibility|backwards compatibility|"
        r"compatibility with old clients?|legacy clients?|existing clients?|"
        r"preserve api contracts?|avoid breaking changes?|non[- ]breaking)\b",
        re.I,
    ),
    "unknown_versioning": _UNKNOWN_RE,
}
_BASE_QUESTIONS: dict[APIVersioningFindingType, tuple[str, ...]] = {
    "versioned_endpoint": (
        "Which API versions, endpoint paths, or negotiation mechanisms are in scope?",
    ),
    "compatibility_window": ("How long must old and new API versions remain supported together?",),
    "deprecation_timeline": ("What deprecation, sunset, or retirement date must clients see?",),
    "client_migration": ("Which client segments, SDKs, or partners need migration support?",),
    "backwards_compatibility": (
        "Which existing client contracts must remain backwards compatible?",
    ),
    "unknown_versioning": (
        "What explicit API versioning, compatibility, deprecation, or migration expectation should planning use?",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceAPIVersioningRequirement:
    """One source-backed API versioning requirement or ambiguity."""

    source_brief_id: str | None
    finding_type: APIVersioningFindingType
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)
    confidence: APIVersioningConfidence = "medium"
    readiness: APIVersioningReadiness = "needs_clarification"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "finding_type": self.finding_type,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "follow_up_questions": list(self.follow_up_questions),
            "confidence": self.confidence,
            "readiness": self.readiness,
        }


@dataclass(frozen=True, slots=True)
class SourceAPIVersioningRequirementsReport:
    """Source-level API versioning requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceAPIVersioningRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAPIVersioningRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return API versioning requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Versioning Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        type_counts = self.summary.get("type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Finding type counts: "
            + ", ".join(
                f"{finding_type} {type_counts.get(finding_type, 0)}" for finding_type in _TYPE_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source API versioning requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Type | Confidence | Readiness | Source Field Paths | Evidence | Follow-up Questions |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.finding_type} | "
                f"{requirement.confidence} | "
                f"{requirement.readiness} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.follow_up_questions))} |"
            )
        return "\n".join(lines)


def build_source_api_versioning_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPIVersioningRequirementsReport:
    """Extract API versioning requirement signals from a source or implementation brief."""
    source_brief_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload), source_brief_id))
    return SourceAPIVersioningRequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_summary(requirements, payload),
    )


def generate_source_api_versioning_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceAPIVersioningRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_api_versioning_requirements(source)


def extract_source_api_versioning_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceAPIVersioningRequirement, ...]:
    """Return API versioning requirement records extracted from brief-shaped input."""
    return build_source_api_versioning_requirements(source).requirements


def summarize_source_api_versioning_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAPIVersioningRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic API versioning requirements summary."""
    if isinstance(source_or_result, SourceAPIVersioningRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_api_versioning_requirements(source_or_result).summary


def source_api_versioning_requirements_to_dict(
    report: SourceAPIVersioningRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API versioning requirements report to a plain dictionary."""
    return report.to_dict()


source_api_versioning_requirements_to_dict.__test__ = False


def source_api_versioning_requirements_to_dicts(
    requirements: (
        tuple[SourceAPIVersioningRequirement, ...]
        | list[SourceAPIVersioningRequirement]
        | SourceAPIVersioningRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source API versioning requirement records to dictionaries."""
    if isinstance(requirements, SourceAPIVersioningRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_versioning_requirements_to_dicts.__test__ = False


def source_api_versioning_requirements_to_markdown(
    report: SourceAPIVersioningRequirementsReport,
) -> str:
    """Render an API versioning requirements report as Markdown."""
    return report.to_markdown()


source_api_versioning_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    finding_type: APIVersioningFindingType
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]
    confidence: APIVersioningConfidence


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = source.model_dump(mode="python")
        return _source_brief_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _source_brief_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _source_brief_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_brief_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_brief_id(payload), payload
    return None, {}


def _source_brief_id(payload: Mapping[str, Any]) -> str | None:
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
        finding_types = [
            finding_type
            for finding_type in _TYPE_ORDER
            if finding_type != "unknown_versioning"
            and _TYPE_PATTERNS[finding_type].search(searchable)
        ]
        if not finding_types and _UNKNOWN_RE.search(searchable):
            finding_types = ["unknown_versioning"]
        for finding_type in finding_types:
            candidates.append(
                _Candidate(
                    finding_type=finding_type,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    source_field_path=segment.source_field,
                    matched_terms=_matched_terms(finding_type, searchable),
                    confidence=_confidence(finding_type, segment),
                )
            )
    return candidates


def _merge_candidates(
    candidates: Iterable[_Candidate],
    source_brief_id: str | None,
) -> list[SourceAPIVersioningRequirement]:
    by_type: dict[APIVersioningFindingType, list[_Candidate]] = {}
    for candidate in candidates:
        by_type.setdefault(candidate.finding_type, []).append(candidate)

    requirements: list[SourceAPIVersioningRequirement] = []
    for finding_type in _TYPE_ORDER:
        items = by_type.get(finding_type, [])
        if not items:
            continue
        evidence = tuple(_dedupe_evidence(item.evidence for item in items))[:5]
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
            (item.confidence for item in items), key=lambda item: _CONFIDENCE_ORDER[item]
        )
        questions = _follow_up_questions(finding_type, " ".join(evidence))
        requirements.append(
            SourceAPIVersioningRequirement(
                source_brief_id=source_brief_id,
                finding_type=finding_type,
                evidence=evidence,
                source_field_paths=source_field_paths,
                matched_terms=matched_terms,
                follow_up_questions=questions,
                confidence=confidence,
                readiness="needs_clarification" if questions else "ready_for_planning",
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
        "problem",
        "problem_statement",
        "goal",
        "goals",
        "requirements",
        "acceptance_criteria",
        "acceptance",
        "constraints",
        "success_criteria",
        "definition_of_done",
        "risks",
        "api",
        "metadata",
        "brief_metadata",
        "implementation_notes",
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
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _VERSIONING_CONTEXT_RE.search(key_text)
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
    if _NEGATED_RE.search(segment.text):
        return False
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if not _VERSIONING_CONTEXT_RE.search(searchable):
        return False
    if _UNKNOWN_RE.search(searchable):
        return True
    if segment.section_context:
        return True
    return bool(_REQUIREMENT_RE.search(segment.text))


def _matched_terms(
    finding_type: APIVersioningFindingType,
    text: str,
) -> tuple[str, ...]:
    return tuple(
        _dedupe(
            _clean_text(match.group(0)) for match in _TYPE_PATTERNS[finding_type].finditer(text)
        )
    )


def _confidence(
    finding_type: APIVersioningFindingType,
    segment: _Segment,
) -> APIVersioningConfidence:
    if finding_type == "unknown_versioning":
        return "low"
    if _REQUIREMENT_RE.search(segment.text) and (
        finding_type != "compatibility_window" or _TIME_WINDOW_RE.search(segment.text)
    ):
        return "high"
    if _TIME_WINDOW_RE.search(segment.text) or segment.section_context:
        return "medium"
    return "low"


def _follow_up_questions(
    finding_type: APIVersioningFindingType,
    evidence_text: str,
) -> tuple[str, ...]:
    questions = list(_BASE_QUESTIONS[finding_type])
    if finding_type == "versioned_endpoint" and re.search(
        r"\b(?:v\d+|/v\d+)\b", evidence_text, re.I
    ):
        questions = []
    if finding_type in {"compatibility_window", "deprecation_timeline"} and _TIME_WINDOW_RE.search(
        evidence_text
    ):
        questions = []
    if finding_type == "client_migration" and re.search(
        r"\b(?:sdk|partner|client|consumer|mobile|web)\b", evidence_text, re.I
    ):
        questions = []
    if finding_type == "backwards_compatibility" and re.search(
        r"\b(?:existing|legacy|old)\s+clients?\b", evidence_text, re.I
    ):
        questions = []
    return tuple(_dedupe(questions))


def _summary(
    requirements: tuple[SourceAPIVersioningRequirement, ...],
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    status = "ready_for_planning" if requirements else "no_versioning_language"
    if any(requirement.finding_type == "unknown_versioning" for requirement in requirements):
        status = "needs_clarification"
    elif not requirements and _payload_has_versioning_language(payload):
        status = "unknown"
    return {
        "requirement_count": len(requirements),
        "type_counts": {
            finding_type: sum(
                1 for requirement in requirements if requirement.finding_type == finding_type
            )
            for finding_type in _TYPE_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "readiness_counts": {
            readiness: sum(1 for requirement in requirements if requirement.readiness == readiness)
            for readiness in ("ready_for_planning", "needs_clarification")
        },
        "finding_types": [requirement.finding_type for requirement in requirements],
        "follow_up_question_count": sum(
            len(requirement.follow_up_questions) for requirement in requirements
        ),
        "status": status,
    }


def _payload_has_versioning_language(payload: Mapping[str, Any]) -> bool:
    return any(
        _VERSIONING_CONTEXT_RE.search(segment.text) for segment in _candidate_segments(payload)
    )


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
        "requirements",
        "acceptance_criteria",
        "acceptance",
        "constraints",
        "success_criteria",
        "definition_of_done",
        "risks",
        "api",
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
    "APIVersioningConfidence",
    "APIVersioningFindingType",
    "APIVersioningReadiness",
    "SourceAPIVersioningRequirement",
    "SourceAPIVersioningRequirementsReport",
    "build_source_api_versioning_requirements",
    "extract_source_api_versioning_requirements",
    "generate_source_api_versioning_requirements",
    "source_api_versioning_requirements_to_dict",
    "source_api_versioning_requirements_to_dicts",
    "source_api_versioning_requirements_to_markdown",
    "summarize_source_api_versioning_requirements",
]
