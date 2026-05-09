"""Extract search functionality requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


SearchRequirementType = Literal[
    "search_scope",
    "query_syntax",
    "ranking_algorithm",
    "filtering_options",
    "faceted_search",
    "relevance_tuning",
    "performance_optimization",
    "index_freshness",
    "typo_tolerance",
    "multi_language_support",
    "fuzzy_search",
    "autocomplete",
    "search_suggestions",
]

_TYPE_ORDER: tuple[SearchRequirementType, ...] = (
    "search_scope",
    "query_syntax",
    "ranking_algorithm",
    "filtering_options",
    "faceted_search",
    "relevance_tuning",
    "performance_optimization",
    "index_freshness",
    "typo_tolerance",
    "multi_language_support",
    "fuzzy_search",
    "autocomplete",
    "search_suggestions",
)

_SPACE_RE = re.compile(r"\s+")
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

_TYPE_PATTERNS: dict[SearchRequirementType, re.Pattern[str]] = {
    "search_scope": re.compile(
        r"\b(?:search[_\s-]+scope|search[_\s-]+(?:across|within|range)|"
        r"(?:full[_\s-]+text|global|local|scoped)[_\s-]+search|"
        r"search[_\s-]+(?:domain|boundary|coverage)|multi[_\s-]+(?:field|entity)[_\s-]+search|"
        r"search[_\s-]+(?:all|specific)[_\s-]+(?:field[s]?|attribute[s]?))\b",
        re.I,
    ),
    "query_syntax": re.compile(
        r"\b(?:query[_\s-]+syntax|search[_\s-]+syntax|query[_\s-]+(?:language|parser)|"
        r"(?:boolean|advanced)[_\s-]+(?:search|query)|"
        r"(?:AND|OR|NOT)[_\s-]+operator[s]?|wildcard[_\s-]+(?:search|query)|"
        r"phrase[_\s-]+(?:search|query)|proximity[_\s-]+search|"
        r"quoted[_\s-]+(?:search|query)|regex[_\s-]+(?:search|query))\b",
        re.I,
    ),
    "ranking_algorithm": re.compile(
        r"\b(?:ranking[_\s-]+algorithm|search[_\s-]+ranking|relevance[_\s-]+ranking|"
        r"(?:TF[_\s-]*IDF|BM25|tf[_\s-]*idf|bm25)|scoring[_\s-]+(?:algorithm|model)|"
        r"result[s]?[_\s-]+ranking|rank[_\s-]+(?:by|using)|"
        r"relevance[_\s-]+score|search[_\s-]+score)\b",
        re.I,
    ),
    "filtering_options": re.compile(
        r"\b(?:filter(?:ing)?[_\s-]+option[s]?|search[_\s-]+filter[s]?|"
        r"(?:pre[_\s-]*filter|post[_\s-]*filter|refinement)[s]?|"
        r"filter[_\s-]+(?:by|on|criteria)|facet[_\s-]+filter[s]?|"
        r"narrow[_\s-]+(?:search|result[s]?)|refine[_\s-]+(?:search|result[s]?))\b",
        re.I,
    ),
    "faceted_search": re.compile(
        r"\b(?:facet(?:ed)?[_\s-]+(?:search|navigation)|facet[s]?|"
        r"drill[_\s-]*down|guided[_\s-]+search|aggregation[s]?|"
        r"search[_\s-]+facet[s]?|facet[_\s-]+(?:count[s]?|value[s]?))\b",
        re.I,
    ),
    "relevance_tuning": re.compile(
        r"\b(?:relevance[_\s-]+tuning|tune[_\s-]+relevance|boost(?:ing)?|"
        r"(?:field|attribute)[_\s-]+weight[s]?|weight[_\s-]+(?:field[s]?|factor[s]?)|"
        r"relevance[_\s-]+(?:factor[s]?|signal[s]?|adjustment)|"
        r"custom[_\s-]+scoring|tune[_\s-]+(?:ranking|search))\b",
        re.I,
    ),
    "performance_optimization": re.compile(
        r"\b(?:search[_\s-]+performance|(?:optimize|optimization)[_\s-]+search|"
        r"search[_\s-]+(?:speed|latency|throughput)|fast[_\s-]+search|"
        r"(?:cache|caching|index)[_\s-]+(?:search|query|result[s]?)|"
        r"search[_\s-]+(?:pagination|limit[s]?)|result[_\s-]+(?:cache|caching))\b",
        re.I,
    ),
    "index_freshness": re.compile(
        r"\b(?:index[_\s-]+freshness|(?:real[_\s-]*time|near[_\s-]*real[_\s-]*time)[_\s-]+(?:search|index(?:ing)?)|"
        r"index[_\s-]+(?:update[s]?|refresh|rebuild)|(?:incremental|delta)[_\s-]+index(?:ing)?|"
        r"index[_\s-]+latency|stale[_\s-]+(?:index|result[s]?))\b",
        re.I,
    ),
    "typo_tolerance": re.compile(
        r"\b(?:typo[_\s-]+tolerance|(?:spell[_\s-]*check|spell[_\s-]*correction)|"
        r"fuzzy[_\s-]+matching|(?:did[_\s-]+you[_\s-]+mean|spell[_\s-]+suggestion[s]?)|"
        r"(?:edit|levenshtein)[_\s-]+distance|approximate[_\s-]+match(?:ing)?|"
        r"misspelling[_\s-]+(?:tolerance|handling))\b",
        re.I,
    ),
    "multi_language_support": re.compile(
        r"\b(?:multi[_\s-]*language[_\s-]+(?:search|support)|(?:i18n|internationalization)[_\s-]+search|"
        r"language[_\s-]+(?:detection|analyzer[s]?)|(?:auto[_\s-]*)?detect[_\s-]+(?:query[_\s-]+)?language|"
        r"(?:multilingual|polyglot)[_\s-]+search|"
        r"(?:stemming|lemmatization)|language[_\s-]+specific[_\s-]+(?:search|index(?:ing)?))\b",
        re.I,
    ),
    "fuzzy_search": re.compile(
        r"\b(?:fuzzy[_\s-]+(?:search|matching|query)|approximate[_\s-]+(?:search|match(?:ing)?)|"
        r"similarity[_\s-]+search|phonetic[_\s-]+(?:search|matching)|"
        r"soundex|metaphone)\b",
        re.I,
    ),
    "autocomplete": re.compile(
        r"\b(?:autocomplete|auto[_\s-]*complete|type[_\s-]*ahead|"
        r"(?:search[_\s-]+)?(?:as[_\s-]+you[_\s-]+type|instant[_\s-]+search)|"
        r"query[_\s-]+completion|prefix[_\s-]+(?:search|matching))\b",
        re.I,
    ),
    "search_suggestions": re.compile(
        r"\b(?:(?:search|query)[_\s-]+suggestion[s]?|suggest(?:ed)?[_\s-]+(?:search|query|term[s]?)|"
        r"related[_\s-]+(?:search(?:es)?|quer(?:y|ies)|term[s]?)|"
        r"popular[_\s-]+(?:search(?:es)?|quer(?:y|ies))|trending[_\s-]+search(?:es)?)\b",
        re.I,
    ),
}

_BASE_QUESTIONS: dict[SearchRequirementType, tuple[str, ...]] = {
    "search_scope": (
        "Which fields or entities should be included in search scope?",
        "Should search be global or scoped to specific contexts?",
    ),
    "query_syntax": (
        "What query syntax features should be supported (boolean, wildcards, phrases)?",
        "How should complex queries be parsed and validated?",
    ),
    "ranking_algorithm": (
        "Which ranking algorithm should be used (TF-IDF, BM25, custom)?",
        "How should relevance scores be calculated and displayed?",
    ),
    "filtering_options": (
        "What filtering criteria should be available to users?",
        "How should filters combine (AND, OR) and be applied?",
    ),
    "faceted_search": (
        "Which facets should be available for drill-down navigation?",
        "How should facet counts be calculated and updated?",
    ),
    "relevance_tuning": (
        "Which fields should be boosted and by what weights?",
        "How should relevance be tuned for different use cases?",
    ),
    "performance_optimization": (
        "What are the target latency and throughput requirements?",
        "How should search results be cached and paginated?",
    ),
    "index_freshness": (
        "How quickly should new content be searchable?",
        "What indexing strategy (real-time, batch, incremental) is needed?",
    ),
    "typo_tolerance": (
        "How tolerant should search be to spelling errors?",
        "Should spell suggestions be provided to users?",
    ),
    "multi_language_support": (
        "Which languages need to be supported?",
        "How should language-specific analyzers and stemmers be configured?",
    ),
    "fuzzy_search": (
        "What fuzzy matching algorithm should be used?",
        "How should fuzzy match thresholds be configured?",
    ),
    "autocomplete": (
        "What autocomplete suggestions should be provided?",
        "How should autocomplete be triggered and ranked?",
    ),
    "search_suggestions": (
        "Should related or popular searches be suggested?",
        "How should search suggestions be generated and ranked?",
    ),
}


@dataclass(frozen=True, slots=True)
class SearchRequirement:
    """One source-backed search requirement."""

    requirement_type: SearchRequirementType
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
class SearchRequirementsReport:
    """Source-level search requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SearchRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SearchRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirements": [req.to_dict() for req in self.requirements],
            "summary": dict(self.summary),
            "records": [rec.to_dict() for rec in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return search requirement records as plain dictionaries."""
        return [req.to_dict() for req in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Search Requirements Report"
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
            f"- Feature coverage: {self.summary.get('feature_coverage', 0)}%",
            f"- UX considerations: {self.summary.get('ux_coverage', 0)}%",
            f"- Performance targets: {self.summary.get('performance_coverage', 0)}%",
            "- Requirement type counts: "
            + ", ".join(f"{req_type} {type_counts.get(req_type, 0)}" for req_type in _TYPE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No search requirements were inferred."])
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
        for req in self.requirements:
            lines.append(
                "| "
                f"{req.requirement_type} | "
                f"{_markdown_cell('; '.join(req.source_field_paths))} | "
                f"{_markdown_cell('; '.join(req.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(req.follow_up_questions) or 'none')} |"
            )
        return "\n".join(lines)


def extract_search_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SearchRequirement, ...]:
    """Extract search requirement records from brief-shaped input."""
    return build_search_requirements_report(source).requirements


def build_search_requirements_report(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SearchRequirementsReport:
    """Extract search requirement signals from a source brief."""
    source_brief_id, payload = _source_payload(source)
    grouped = _group_requirements(payload)
    requirements = _merge_requirements(grouped)
    return SearchRequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_compute_summary(requirements),
    )


# Compatibility aliases
generate_search_requirements = extract_search_requirements
analyze_search_requirements = extract_search_requirements
derive_search_requirements = extract_search_requirements
summarize_search_requirements = lambda source: build_search_requirements_report(source).summary


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: SearchRequirementType
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
            return _source_brief_id(value), dict(value)
    if isinstance(source, Mapping):
        try:
            payload = dict(SourceBrief.model_validate(source).model_dump(mode="python"))
            return _source_brief_id(payload), payload
        except (TypeError, ValueError, ValidationError):
            return _source_brief_id(source), dict(source)
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


def _object_payload(obj: object) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for attr in dir(obj):
        if attr.startswith("_"):
            continue
        try:
            value = getattr(obj, attr)
            if not callable(value):
                payload[attr] = value
        except AttributeError:
            pass
    return payload


def _group_requirements(payload: Mapping[str, Any]) -> dict[SearchRequirementType, list[_Candidate]]:
    grouped: dict[SearchRequirementType, list[_Candidate]] = {}
    for source_field, text in _candidate_texts(payload):
        for segment in _segments(text):
            for req_type in _matched_requirement_types(segment):
                candidate = _Candidate(
                    requirement_type=req_type,
                    evidence=_evidence_snippet(source_field, segment),
                    source_field_path=source_field,
                    matched_terms=_matched_terms(req_type, segment),
                )
                grouped.setdefault(req_type, []).append(candidate)
    return grouped


def _merge_requirements(
    grouped: dict[SearchRequirementType, list[_Candidate]],
) -> tuple[SearchRequirement, ...]:
    requirements: list[SearchRequirement] = []
    for req_type in _TYPE_ORDER:
        candidates = grouped.get(req_type, [])
        if not candidates:
            continue
        evidence = tuple(_dedupe_evidence(item.evidence for item in candidates))[:5]
        source_field_paths = tuple(sorted(_dedupe(item.source_field_path for item in candidates), key=str.casefold))
        matched_terms = tuple(
            sorted(_dedupe(term for item in candidates for term in item.matched_terms), key=str.casefold)
        )
        questions = tuple(_BASE_QUESTIONS[req_type])
        requirements.append(
            SearchRequirement(
                requirement_type=req_type,
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


def _matched_requirement_types(text: str) -> tuple[SearchRequirementType, ...]:
    return tuple(req_type for req_type in _TYPE_ORDER if _TYPE_PATTERNS[req_type].search(text))


def _matched_terms(req_type: SearchRequirementType, text: str) -> tuple[str, ...]:
    return tuple(_dedupe(_clean_text(match.group(0)) for match in _TYPE_PATTERNS[req_type].finditer(text)))


def _evidence_snippet(source_field: str, text: str, max_chars: int = 150) -> str:
    _ = source_field  # Reserved for future use in evidence formatting
    clean = _clean_text(text)
    if len(clean) <= max_chars:
        return clean
    return clean[:max_chars].rsplit(" ", 1)[0] + "..."


def _clean_text(text: str) -> str:
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        text = _clean_text(value)
        return text if text else None
    return _clean_text(str(value)) if value else None


def _dedupe(items: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        if item.lower() not in seen:
            seen.add(item.lower())
            result.append(item)
    return tuple(result)


def _dedupe_evidence(items: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        clean = _clean_text(item)
        normalized = clean.lower()
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(clean)
    return tuple(result)


def _compute_summary(requirements: tuple[SearchRequirement, ...]) -> dict[str, Any]:
    type_counts = {req_type: 0 for req_type in _TYPE_ORDER}
    for req in requirements:
        type_counts[req.requirement_type] += 1

    # Feature coverage
    feature_types = {"search_scope", "query_syntax", "ranking_algorithm", "filtering_options", "faceted_search"}
    feature_coverage = sum(1 for req_type in feature_types if type_counts[req_type] > 0)
    feature_coverage_pct = int((feature_coverage / len(feature_types)) * 100)

    # UX considerations
    ux_types = {"typo_tolerance", "autocomplete", "search_suggestions", "multi_language_support"}
    ux_coverage = sum(1 for req_type in ux_types if type_counts[req_type] > 0)
    ux_coverage_pct = int((ux_coverage / len(ux_types)) * 100)

    # Performance targets
    performance_types = {"performance_optimization", "index_freshness", "relevance_tuning"}
    performance_coverage = sum(1 for req_type in performance_types if type_counts[req_type] > 0)
    performance_coverage_pct = int((performance_coverage / len(performance_types)) * 100)

    return {
        "requirement_count": len(requirements),
        "source_count": 1,
        "type_counts": type_counts,
        "feature_coverage": feature_coverage_pct,
        "ux_coverage": ux_coverage_pct,
        "performance_coverage": performance_coverage_pct,
    }


def _markdown_cell(text: str) -> str:
    return text.replace("|", "\\|").replace("\n", " ")


__all__ = [
    "SearchRequirement",
    "SearchRequirementsReport",
    "SearchRequirementType",
    "extract_search_requirements",
    "build_search_requirements_report",
    "generate_search_requirements",
    "analyze_search_requirements",
    "derive_search_requirements",
    "summarize_search_requirements",
]
