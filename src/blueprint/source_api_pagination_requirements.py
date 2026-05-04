"""Extract API pagination requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


PaginationRequirementType = Literal[
    "cursor_pagination",
    "offset_pagination",
    "page_size_limits",
    "sort_order_stability",
    "filtering_compatibility",
    "next_previous_tokens",
    "backwards_compatibility",
    "large_result_performance",
]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[PaginationRequirementType, ...] = (
    "cursor_pagination",
    "offset_pagination",
    "page_size_limits",
    "sort_order_stability",
    "filtering_compatibility",
    "next_previous_tokens",
    "backwards_compatibility",
    "large_result_performance",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "acceptance",
    "acceptance_criteria",
    "integration_points",
    "integrations",
    "constraints",
    "metadata",
)

_TYPE_PATTERNS: dict[PaginationRequirementType, re.Pattern[str]] = {
    "cursor_pagination": re.compile(
        r"\b(?:cursor[- ]based pagination|cursor pagination|cursors?|"
        r"keyset pagination|continuation token|seek pagination|"
        r"cursor[- ]based paging|cursor strategy)\b",
        re.I,
    ),
    "offset_pagination": re.compile(
        r"\b(?:offset[- ]based pagination|offset pagination|offset and limit|"
        r"page offset|limit and offset|skip and take|"
        r"offset[- ]based paging|offset strategy)\b",
        re.I,
    ),
    "page_size_limits": re.compile(
        r"\b(?:page size|page limit|max(?:imum)? page size|"
        r"results? per page|items per page|max(?:imum)? results?|"
        r"default page size|pagination limit|max(?:imum)? items)\b",
        re.I,
    ),
    "sort_order_stability": re.compile(
        r"\b(?:sort(?:ing)? order stability|stable sort|"
        r"consistent sort(?:ing)?|deterministic order|"
        r"stable pagination|sort stability|ordering stability)\b",
        re.I,
    ),
    "filtering_compatibility": re.compile(
        r"\b(?:filter(?:ing)? with pagination|pagination with filter(?:s)?|"
        r"combined filter(?:ing)? and paging|filter compat(?:ibility)?|"
        r"filter and page|paginate filtered results?)\b",
        re.I,
    ),
    "next_previous_tokens": re.compile(
        r"\b(?:next token|previous token|next page token|"
        r"prev(?:ious)? page token|pagination tokens?|"
        r"next and prev(?:ious)?(?: page)? tokens?|prev(?:ious)? and next(?: page)? tokens?|"
        r"next link|prev(?:ious)? link|next url|prev(?:ious)? url)\b",
        re.I,
    ),
    "backwards_compatibility": re.compile(
        r"\b(?:backwards? compat(?:ibility)?|pagina(?:tion)? compat(?:ibility)?|"
        r"legacy pagination|migration from (?:cursor|offset)|"
        r"pagination versioning|pagination migration)\b",
        re.I,
    ),
    "large_result_performance": re.compile(
        r"\b(?:large results?|large data sets?|performance with pagination|"
        r"pagina(?:tion)? performance|deep pagination|"
        r"pagination scaling|large response|many results?)\b",
        re.I,
    ),
}

_BASE_QUESTIONS: dict[PaginationRequirementType, tuple[str, ...]] = {
    "cursor_pagination": (
        "Which cursor encoding format and token structure should be used?",
        "How should cursor invalidation and expiration be handled?",
    ),
    "offset_pagination": (
        "What are the maximum offset and limit values allowed?",
        "How should out-of-bounds offsets be handled?",
    ),
    "page_size_limits": (
        "What are the default and maximum page size limits?",
        "How should requests exceeding the maximum page size be handled?",
    ),
    "sort_order_stability": (
        "Which fields guarantee stable sort order for pagination?",
        "How is tie-breaking handled when sort keys are non-unique?",
    ),
    "filtering_compatibility": (
        "Which filters are compatible with pagination?",
        "How do filters affect pagination token validity?",
    ),
    "next_previous_tokens": (
        "What format should next/previous tokens use?",
        "Should tokens be opaque or contain queryable information?",
    ),
    "backwards_compatibility": (
        "What is the migration path from legacy pagination methods?",
        "How long must deprecated pagination methods be supported?",
    ),
    "large_result_performance": (
        "What are the performance requirements for paginating large datasets?",
        "Are there specific limits or optimizations for deep pagination?",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceApiPaginationRequirement:
    """One source-backed API pagination requirement."""

    requirement_type: PaginationRequirementType
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "follow_up_questions": list(self.follow_up_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceApiPaginationRequirementsReport:
    """Source-level API pagination requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceApiPaginationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceApiPaginationRequirement, ...]:
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
        """Return API pagination requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Pagination Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        type_counts = self.summary.get("type_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Source count: {self.summary.get('source_count', 1)}",
            f"- Pagination strategy coverage: {self.summary.get('pagination_strategy_coverage', 0)}%",
            f"- Performance coverage: {self.summary.get('performance_coverage', 0)}%",
            f"- Compatibility coverage: {self.summary.get('compatibility_coverage', 0)}%",
            "- Requirement type counts: "
            + ", ".join(
                f"{req_type} {type_counts.get(req_type, 0)}" for req_type in _TYPE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source API pagination requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Type | Source Field Paths | Evidence | Follow-up Questions |",
                "| --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell('; '.join(requirement.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(requirement.follow_up_questions) or 'none')} |"
            )
        return "\n".join(lines)


def build_source_api_pagination_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceApiPaginationRequirementsReport:
    """Extract API pagination requirement signals from a source brief."""
    source_brief_id, payload = _source_payload(source)
    grouped = _group_requirements(payload)
    requirements = _merge_requirements(grouped, source_brief_id)
    return SourceApiPaginationRequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_api_pagination_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SourceApiPaginationRequirement, ...]:
    """Return API pagination requirement records extracted from brief-shaped input."""
    return build_source_api_pagination_requirements(source).requirements


def summarize_source_api_pagination_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | SourceApiPaginationRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic API pagination requirements summary."""
    if isinstance(source_or_result, SourceApiPaginationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_api_pagination_requirements(source_or_result).summary


def source_api_pagination_requirements_to_dict(
    report: SourceApiPaginationRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API pagination requirements report to a plain dictionary."""
    return report.to_dict()


source_api_pagination_requirements_to_dict.__test__ = False


def source_api_pagination_requirements_to_dicts(
    requirements: (
        tuple[SourceApiPaginationRequirement, ...]
        | list[SourceApiPaginationRequirement]
        | SourceApiPaginationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source API pagination requirement records to dictionaries."""
    if isinstance(requirements, SourceApiPaginationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_pagination_requirements_to_dicts.__test__ = False


def source_api_pagination_requirements_to_markdown(
    report: SourceApiPaginationRequirementsReport,
) -> str:
    """Render an API pagination requirements report as Markdown."""
    return report.to_markdown()


source_api_pagination_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: PaginationRequirementType
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        payload = source.model_dump(mode="python")
        return _source_brief_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _source_brief_id(payload), payload
    if isinstance(source, Mapping):
        try:
            payload = dict(SourceBrief.model_validate(source).model_dump(mode="python"))
            return _source_brief_id(payload), payload
        except (TypeError, ValueError, ValidationError):
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


def _group_requirements(payload: Mapping[str, Any]) -> dict[PaginationRequirementType, list[_Candidate]]:
    grouped: dict[PaginationRequirementType, list[_Candidate]] = {}
    for source_field, text in _candidate_texts(payload):
        for segment in _segments(text):
            for requirement_type in _matched_requirement_types(segment):
                candidate = _Candidate(
                    requirement_type=requirement_type,
                    evidence=_evidence_snippet(source_field, segment),
                    source_field_path=source_field,
                    matched_terms=_matched_terms(requirement_type, segment),
                )
                grouped.setdefault(requirement_type, []).append(candidate)
    return grouped


def _merge_requirements(
    grouped: dict[PaginationRequirementType, list[_Candidate]],
    source_brief_id: str | None,
) -> tuple[SourceApiPaginationRequirement, ...]:
    requirements: list[SourceApiPaginationRequirement] = []
    for requirement_type in _TYPE_ORDER:
        candidates = grouped.get(requirement_type, [])
        if not candidates:
            continue
        evidence = tuple(_dedupe_evidence(item.evidence for item in candidates))[:5]
        source_field_paths = tuple(
            sorted(_dedupe(item.source_field_path for item in candidates), key=str.casefold)
        )
        matched_terms = tuple(
            sorted(
                _dedupe(term for item in candidates for term in item.matched_terms),
                key=str.casefold,
            )
        )
        questions = _follow_up_questions(requirement_type, " ".join(evidence))
        requirements.append(
            SourceApiPaginationRequirement(
                requirement_type=requirement_type,
                evidence=evidence,
                source_field_paths=source_field_paths,
                matched_terms=matched_terms,
                follow_up_questions=questions,
            )
        )
    return tuple(requirements)


def _candidate_texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in _SCANNED_FIELDS:
        value = payload.get(field_name)
        if field_name == "metadata":
            texts.extend(_nested_texts(value, field_name))
            continue
        for index, text in enumerate(_strings(value)):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))

    if isinstance(payload.get("source_payload"), Mapping):
        for field_name in _SCANNED_FIELDS:
            if field_name in payload["source_payload"]:
                texts.extend(_nested_texts(payload["source_payload"][field_name], f"source_payload.{field_name}"))
    return texts


def _nested_texts(value: Any, prefix: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_nested_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_nested_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
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
    text = _optional_text(value)
    return [text] if text else []


def _segments(text: str) -> list[str]:
    segments: list[str] = []
    for raw_segment in _SENTENCE_SPLIT_RE.split(text):
        segment = _clean_text(raw_segment)
        if segment:
            segments.append(segment)
    return segments


def _matched_requirement_types(text: str) -> tuple[PaginationRequirementType, ...]:
    return tuple(
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(text)
    )


def _matched_terms(
    requirement_type: PaginationRequirementType,
    text: str,
) -> tuple[str, ...]:
    return tuple(
        _dedupe(
            _clean_text(match.group(0)) for match in _TYPE_PATTERNS[requirement_type].finditer(text)
        )
    )


def _follow_up_questions(
    requirement_type: PaginationRequirementType,
    evidence_text: str,
) -> tuple[str, ...]:
    questions = list(_BASE_QUESTIONS[requirement_type])
    # Reduce questions if evidence already provides specific answers
    if requirement_type == "cursor_pagination" and re.search(
        r"\b(?:base64|jwt|opaque|encrypted)\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Encoding format mentioned
    if requirement_type == "page_size_limits" and re.search(
        r"\b(?:\d+\s+(?:items?|results?)|max(?:imum)?\s+\d+)\b", evidence_text, re.I
    ):
        questions = []  # Specific limits provided
    if requirement_type == "backwards_compatibility" and re.search(
        r"\b(?:\d+\s+(?:days?|weeks?|months?)|migration timeline)\b", evidence_text, re.I
    ):
        questions = questions[:1]  # Timeline provided
    return tuple(_dedupe(questions))


def _summary(requirements: tuple[SourceApiPaginationRequirement, ...]) -> dict[str, Any]:
    # Calculate coverage metrics
    pagination_strategies = {"cursor_pagination", "offset_pagination"}
    performance = {"large_result_performance", "page_size_limits"}
    compatibility = {"backwards_compatibility", "filtering_compatibility"}

    req_types = {req.requirement_type for req in requirements}
    pagination_strategy_coverage = int(100 * len(req_types & pagination_strategies) / len(pagination_strategies)) if pagination_strategies else 0
    performance_coverage = int(100 * len(req_types & performance) / len(performance)) if performance else 0
    compatibility_coverage = int(100 * len(req_types & compatibility) / len(compatibility)) if compatibility else 0

    return {
        "requirement_count": len(requirements),
        "source_count": 1,
        "type_counts": {
            req_type: sum(1 for req in requirements if req.requirement_type == req_type)
            for req_type in _TYPE_ORDER
        },
        "requirement_types": [req.requirement_type for req in requirements],
        "follow_up_question_count": sum(
            len(req.follow_up_questions) for req in requirements
        ),
        "pagination_strategy_coverage": pagination_strategy_coverage,
        "performance_coverage": performance_coverage,
        "compatibility_coverage": compatibility_coverage,
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
        "requirements",
        "acceptance_criteria",
        "acceptance",
        "constraints",
        "integration_points",
        "integrations",
        "metadata",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _BULLET_RE.sub("", text.strip())
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
    "PaginationRequirementType",
    "SourceApiPaginationRequirement",
    "SourceApiPaginationRequirementsReport",
    "build_source_api_pagination_requirements",
    "extract_source_api_pagination_requirements",
    "source_api_pagination_requirements_to_dict",
    "source_api_pagination_requirements_to_dicts",
    "source_api_pagination_requirements_to_markdown",
    "summarize_source_api_pagination_requirements",
]
