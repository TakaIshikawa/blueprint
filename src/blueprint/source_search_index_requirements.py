"""Extract source-level search index handling requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint._source_requirement_utils import (
    dedupe,
    evidence_snippet,
    markdown_cell,
    optional_text,
    segments,
    source_id,
    source_payloads,
)


SearchIndexCategory = Literal[
    "indexed_entity",
    "field_mapping",
    "analyzer_tokenizer",
    "reindex_backfill",
    "freshness_lag",
    "ranking_sorting",
    "access_filtering",
    "observability",
]
SearchIndexConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[SearchIndexCategory, ...] = (
    "indexed_entity",
    "field_mapping",
    "analyzer_tokenizer",
    "reindex_backfill",
    "freshness_lag",
    "ranking_sorting",
    "access_filtering",
    "observability",
)
_CONFIDENCE_ORDER: dict[SearchIndexConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SCANNED_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "context",
    "workflow_context",
    "requirements",
    "constraints",
    "scope",
    "non_goals",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "search",
    "index",
    "indexing",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CONTEXT_RE = re.compile(
    r"\b(?:search index|search indexing|indexing|indexed|reindex|backfill|"
    r"analyzer|tokenizer|stemming|synonym|freshness|stale results|ranking|sorting|"
    r"access filter|permission filter|tenant filter|search observability|index lag)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"index|map|analyze|tokenize|reindex|backfill|refresh|rank|sort|filter|alert|monitor|"
    r"metric|support|provide|acceptance|done when)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,120}"
    r"\b(?:search index|search indexing|reindex|field mapping|index freshness|ranking)\b|"
    r"\b(?:search index|search indexing|reindex|field mapping|index freshness|ranking)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no support|unsupported|excluded|no changes?)\b",
    re.I,
)
_UNRELATED_RE = re.compile(r"\b(?:browser search|search and replace|web search|search the docs|generic search box copy)\b", re.I)
_CATEGORY_PATTERNS: dict[SearchIndexCategory, re.Pattern[str]] = {
    "indexed_entity": re.compile(r"\b(?:index(?:ed|ing)? (?:products?|orders?|tickets?|documents?|users?|entities|records)|(?:product|order|ticket|document|user|entity|record) search)\b", re.I),
    "field_mapping": re.compile(r"\b(?:field mapping|mapped fields?|searchable fields?|indexed fields?|map (?:title|name|description|body|sku|status|owner|tags?))\b", re.I),
    "analyzer_tokenizer": re.compile(r"\b(?:analyzer|tokenizer|tokeniser|stemming|synonyms?|stop words?|ngram|language analysis)\b", re.I),
    "reindex_backfill": re.compile(r"\b(?:reindex|re-index|backfill|rebuild index|bulk indexing|historical records|existing records)\b", re.I),
    "freshness_lag": re.compile(r"\b(?:freshness|index lag|stale results?|updated within|refresh within|near real[- ]time|eventual consistency)\b", re.I),
    "ranking_sorting": re.compile(r"\b(?:ranking|rank|sorting|sort by|relevance|boost|pinned results?|recency sort)\b", re.I),
    "access_filtering": re.compile(r"\b(?:access filter|permission filter|tenant filter|authorization filter|acl|visibility filter|security trimming)\b", re.I),
    "observability": re.compile(r"\b(?:observability|monitoring|metric|metrics|dashboard|alert|index health|indexing failures?)\b", re.I),
}
_OWNER_SUGGESTIONS = {
    "indexed_entity": ("search_platform", "backend"),
    "field_mapping": ("search_platform", "backend"),
    "analyzer_tokenizer": ("search_platform", "product"),
    "reindex_backfill": ("search_platform", "data"),
    "freshness_lag": ("search_platform", "backend"),
    "ranking_sorting": ("search_platform", "product"),
    "access_filtering": ("search_platform", "security"),
    "observability": ("search_platform", "observability"),
}
_PLANNING_NOTES = {
    "indexed_entity": ("Define indexed entities, source of truth, and lifecycle triggers.",),
    "field_mapping": ("Specify searchable fields, types, normalization, and schema ownership.",),
    "analyzer_tokenizer": ("Document analyzers, tokenizers, synonyms, locale handling, and relevance examples.",),
    "reindex_backfill": ("Plan reindex and backfill strategy, throttling, validation, and rollback.",),
    "freshness_lag": ("Set freshness target, acceptable lag, stale-result behavior, and breach response.",),
    "ranking_sorting": ("Define ranking, sorting, boosts, tie-breakers, and acceptance examples.",),
    "access_filtering": ("Specify tenant, permission, and visibility filters enforced at query time.",),
    "observability": ("Add metrics, logs, dashboards, and alerts for index lag, failures, and quality.",),
}
_MISSING_DETAIL_MESSAGES = {
    "missing_mapping": "Specify field mapping and searchable attributes for the search index.",
    "missing_freshness": "Define index freshness target or acceptable lag.",
    "missing_access_filtering": "Define access filtering or tenant visibility behavior for search results.",
}


@dataclass(frozen=True, slots=True)
class SourceSearchIndexRequirement:
    category: SearchIndexCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: SearchIndexConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> SearchIndexCategory:
        return self.category

    @property
    def concern(self) -> SearchIndexCategory:
        return self.category

    @property
    def suggested_plan_impacts(self) -> tuple[str, ...]:
        return self.planning_notes

    def to_dict(self) -> dict[str, Any]:
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
class SourceSearchIndexRequirementsReport:
    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceSearchIndexRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSearchIndexRequirement, ...]:
        return self.requirements

    @property
    def findings(self) -> tuple[SourceSearchIndexRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        title = "# Source Search Index Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        category_counts = self.summary.get("category_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Category counts: " + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Missing detail flags: " + ", ".join(self.summary.get("missing_detail_flags", [])),
        ]
        if not self.requirements:
            lines.extend(["", "No source search index requirements were inferred."])
            return "\n".join(lines)
        lines.extend(["", "## Requirements", "", "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes | Gap Messages |", "| --- | --- | --- | --- | --- | --- | --- | --- |"])
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | {markdown_cell(requirement.value or '')} | {requirement.confidence} | "
                f"{markdown_cell(requirement.source_field)} | {markdown_cell(', '.join(requirement.suggested_owners))} | "
                f"{markdown_cell('; '.join(requirement.evidence))} | {markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{markdown_cell('; '.join(requirement.gap_messages))} |"
            )
        return "\n".join(lines)


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: SearchIndexCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: SearchIndexConfidence


def build_source_search_index_requirements(source: Mapping[str, Any] | SourceBrief | ImplementationBrief | Iterable[Any] | str | object) -> SourceSearchIndexRequirementsReport:
    payloads = source_payloads(source)
    candidates: list[_Candidate] = []
    for _, payload in payloads:
        if not _has_no_scope(payload):
            candidates.extend(_candidates(payload))
    requirements = tuple(_merge(candidates, _gap_messages(candidates)))
    ids = dedupe(source_id(payload) for _, payload in payloads)
    return SourceSearchIndexRequirementsReport(
        brief_id=ids[0] if len(ids) == 1 else None,
        title=optional_text(payloads[0][1].get("title")) if payloads else None,
        requirements=requirements,
        summary=_summary(requirements, len(payloads)),
    )


def extract_source_search_index_requirements(source: Any) -> tuple[SourceSearchIndexRequirement, ...]:
    return build_source_search_index_requirements(source).requirements


def derive_source_search_index_requirements(source: Any) -> SourceSearchIndexRequirementsReport:
    return build_source_search_index_requirements(source)


def generate_source_search_index_requirements(source: Any) -> SourceSearchIndexRequirementsReport:
    return build_source_search_index_requirements(source)


def summarize_source_search_index_requirements(source: Any) -> dict[str, Any]:
    if isinstance(source, SourceSearchIndexRequirementsReport):
        return dict(source.summary)
    return build_source_search_index_requirements(source).summary


def source_search_index_requirements_to_dict(report: SourceSearchIndexRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_search_index_requirements_to_dict.__test__ = False


def source_search_index_requirements_to_dicts(requirements: tuple[SourceSearchIndexRequirement, ...] | list[SourceSearchIndexRequirement] | SourceSearchIndexRequirementsReport) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceSearchIndexRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_search_index_requirements_to_dicts.__test__ = False


def source_search_index_requirements_to_markdown(report: SourceSearchIndexRequirementsReport) -> str:
    return report.to_markdown()


source_search_index_requirements_to_markdown.__test__ = False


def _candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    found: list[_Candidate] = []
    for source_field, text in segments(payload, _SCANNED_FIELDS):
        if source_field == "title" and re.search(r"\brequirements?\b\.?$", text, re.I):
            continue
        searchable = f"{source_field.replace('_', ' ')} {text}"
        if not (_CONTEXT_RE.search(searchable) and _REQUIREMENT_RE.search(searchable)) or _NEGATED_RE.search(searchable) or _UNRELATED_RE.search(searchable):
            continue
        for category in _CATEGORY_ORDER:
            if _CATEGORY_PATTERNS[category].search(searchable):
                found.append(_Candidate(category, _value(category, text), source_field, evidence_snippet(source_field, text), _confidence(searchable)))
    return found


def _has_no_scope(payload: Mapping[str, Any]) -> bool:
    return any(_NEGATED_RE.search(text) for _, text in segments(payload, _SCANNED_FIELDS))


def _merge(candidates: Iterable[_Candidate], gap_messages: tuple[str, ...]) -> list[SourceSearchIndexRequirement]:
    grouped: dict[SearchIndexCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)
    result: list[SourceSearchIndexRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        result.append(
            SourceSearchIndexRequirement(
                category=category,
                source_field=sorted({item.source_field for item in items}, key=str.casefold)[0],
                evidence=tuple(sorted(dedupe(item.evidence for item in items), key=str.casefold))[:5],
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                value=dedupe(item.value for item in items)[0],
                suggested_owners=_OWNER_SUGGESTIONS[category],
                planning_notes=_PLANNING_NOTES[category],
                gap_messages=gap_messages,
            )
        )
    return result


def _gap_messages(candidates: list[_Candidate]) -> tuple[str, ...]:
    if not candidates:
        return ()
    categories = {candidate.category for candidate in candidates}
    flags = []
    if "field_mapping" not in categories:
        flags.append("missing_mapping")
    if "freshness_lag" not in categories:
        flags.append("missing_freshness")
    if "access_filtering" not in categories:
        flags.append("missing_access_filtering")
    return tuple(_MISSING_DETAIL_MESSAGES[flag] for flag in flags)


def _summary(requirements: tuple[SourceSearchIndexRequirement, ...], source_count: int) -> dict[str, Any]:
    category_counts = {category: 0 for category in _CATEGORY_ORDER}
    confidence_counts = {confidence: 0 for confidence in _CONFIDENCE_ORDER}
    for requirement in requirements:
        category_counts[requirement.category] += 1
        confidence_counts[requirement.confidence] += 1
    gap_messages = dedupe(message for requirement in requirements for message in requirement.gap_messages)
    missing_flags = [flag for flag, message in _MISSING_DETAIL_MESSAGES.items() if message in gap_messages]
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "categories": [category for category in _CATEGORY_ORDER if category_counts[category]],
        "category_counts": category_counts,
        "confidence_counts": confidence_counts,
        "missing_detail_flags": missing_flags,
        "gap_messages": gap_messages,
        "owner_suggestions": dedupe(owner for requirement in requirements for owner in requirement.suggested_owners),
        "status": "no_search_index_language" if not requirements else ("needs_search_index_details" if missing_flags else "ready_for_planning"),
    }


def _confidence(text: str) -> SearchIndexConfidence:
    return "high" if re.search(r"\b(?:search index|field mapping|analyzer|tokenizer|reindex|freshness|access filter)\b", text, re.I) else "medium"


def _value(category: SearchIndexCategory, text: str) -> str | None:
    match = _CATEGORY_PATTERNS[category].search(text)
    return optional_text(match.group(0).casefold()) if match else None
