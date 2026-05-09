"""Analyze search indexing strategy for execution-plan tasks involving search functionality."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for search indexing concepts
_INDEX_SCHEMA_RE = re.compile(
    r"\b(?:index\s+schema|field\s+(?:type|mapping)|document\s+(?:schema|structure|mapping)|"
    r"(?:text|keyword|numeric|date|boolean|nested|object)\s+field|"
    r"(?:define|create|configure)\s+(?:index|mapping|schema)|"
    r"(?:elasticsearch|opensearch|solr|algolia|typesense|meilisearch)\s+(?:mapping|schema|index)|"
    r"field\s+definition|index\s+(?:structure|definition))\b",
    re.I,
)
_TOKENIZATION_STRATEGY_RE = re.compile(
    r"\b(?:tokeniz(?:ation|er)|analyzer|(?:standard|edge\s+ngram|ngram|whitespace|keyword)\s+analyzer|"
    r"(?:stem(?:ming)?|lemmatiz(?:ation|er))|stop\s+word(?:s)?|synonym(?:s)?|"
    r"(?:configure|define|set)\s+(?:analyzer|tokenizer)|"
    r"(?:language|custom)\s+analyzer|word\s+(?:boundary|delimiter)|"
    r"character\s+filter|token\s+filter)\b",
    re.I,
)
_RELEVANCE_TUNING_RE = re.compile(
    r"\b(?:relevance\s+(?:tuning|scoring|ranking)|(?:boost(?:ing)?|weight(?:ing)?)\s+field(?:s)?|"
    r"(?:bm25|tf[- ]?idf|vector\s+search|semantic\s+search)|"
    r"fuzzy\s+(?:matching|search|query)|edit\s+distance|"
    r"(?:query|field|document)\s+boost|relevance\s+score|"
    r"rank(?:ing)?\s+(?:function|algorithm|factor)|"
    r"search\s+(?:quality|relevance|precision|recall))\b",
    re.I,
)
_UPDATE_FREQUENCY_RE = re.compile(
    r"\b(?:(?:real[- ]?time|near[- ]?real[- ]?time|live)\s+(?:indexing|update(?:s)?)|"
    r"batch\s+(?:indexing|update(?:s)?|reindex)|"
    r"incremental\s+(?:indexing|update(?:s)?)|"
    r"(?:index|update)\s+(?:frequency|interval|schedule)|"
    r"(?:continuous|periodic|scheduled)\s+(?:indexing|update(?:s)?)|"
    r"(?:sync|refresh)\s+(?:interval|rate|frequency))\b",
    re.I,
)
_INDEX_REBUILD_RE = re.compile(
    r"\b(?:(?:reindex(?:ing)?|rebuild)\s+(?:time|duration|strategy|index)|"
    r"full\s+reindex|zero[- ]downtime\s+reindex|"
    r"(?:online|offline)\s+reindex|index\s+rebuild|"
    r"reindex\s+(?:without|with)\s+downtime|"
    r"(?:parallel|concurrent)\s+reindex(?:ing)?|"
    r"index\s+(?:migration|recreation))\b",
    re.I,
)
_CONSISTENCY_GUARANTEES_RE = re.compile(
    r"\b(?:(?:eventual|strong|immediate)\s+consistency|"
    r"consistency\s+(?:guarantee|model|level)|"
    r"(?:stale|fresh(?:ness)?|up[- ]to[- ]date)\s+(?:data|index|result(?:s)?|search)?|"
    r"search\s+results\s+are\s+fresh|"
    r"read\s+(?:after|before)\s+write|"
    r"search\s+(?:consistency|freshness|staleness)|"
    r"index\s+(?:latency|lag|delay))\b",
    re.I,
)
_QUERY_PERFORMANCE_RE = re.compile(
    r"\b(?:query\s+(?:performance|latency|speed|optimization)|"
    r"(?:cach(?:e|ing)|cached)\s+(?:query|result(?:s)?|search)|"
    r"(?:filter|query)\s+context|"
    r"search\s+(?:performance|speed|latency|optimization)|"
    r"(?:optimize|improve)\s+(?:query|search)\s+(?:performance|speed)|"
    r"query\s+(?:time|execution)|response\s+time|"
    r"(?:fast|slow)\s+(?:query|search))\b",
    re.I,
)
_STORAGE_COSTS_RE = re.compile(
    r"\b(?:storage\s+cost(?:s)?|index\s+size|disk\s+(?:space|usage)|"
    r"(?:shard(?:ing)?|partition(?:ing)?)\s+strategy|"
    r"(?:number\s+of|shard\s+count)\s+shard(?:s)?|"
    r"(?:replicat(?:ion|e)|replica)\s+(?:factor|count|strategy)|"
    r"(?:compress(?:ion)?|compact(?:ion)?)|"
    r"(?:optimize|reduce)\s+(?:storage|index\s+size)|"
    r"data\s+retention|index\s+lifecycle)\b",
    re.I,
)
_MULTI_LANGUAGE_RE = re.compile(
    r"\b(?:multi[- ]?language|(?:language|locale)[- ]specific|"
    r"(?:internationalization|i18n)|(?:localization|l10n)|"
    r"(?:english|spanish|french|german|chinese|japanese|arabic)\s+(?:analyzer|search|index)?|"
    r"(?:search|support)\s+in\s+(?:english|spanish|french|german)|"
    r"language\s+(?:detection|analysis|support)|"
    r"cross[- ]language\s+search)\b",
    re.I,
)
_FACETED_SEARCH_RE = re.compile(
    r"\b(?:facet(?:ed|s)?\s+(?:search|for)?|(?:filter|facet)\s+aggregation|"
    r"(?:category|tag|attribute)\s+filter(?:ing)?|"
    r"(?:add|implement)\s+facets(?:\s+for)?|"
    r"drill[- ]down|refinement\s+(?:filter(?:s)?|option(?:s)?)|"
    r"aggregation(?:s)?|bucketing|"
    r"search\s+(?:facet(?:s)?|filter(?:s)?|refinement(?:s)?))\b",
    re.I,
)
_MONITORING_OBSERVABILITY_RE = re.compile(
    r"\b(?:(?:monitor(?:ing)?|observability|metric(?:s)?|alert(?:ing|s)?)|"
    r"(?:index|search|query)\s+(?:metric(?:s)?|performance|health)|"
    r"(?:latency|throughput|error\s+rate)\s+(?:monitoring|metric(?:s)?)|"
    r"(?:log|trace|debug)\s+(?:search|index|query)|"
    r"search\s+(?:analytics|statistics)|"
    r"(?:dashboard|visualization)\s+for\s+(?:search|index))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class SearchIndexingStrategyAnalysis:
    """Search indexing strategy analysis for a change brief."""

    index_schema_defined: bool = False
    tokenization_strategy_defined: bool = False
    relevance_tuning_planned: bool = False
    update_frequency_specified: bool = False
    index_rebuild_considered: bool = False
    consistency_guarantees_defined: bool = False
    query_performance_optimized: bool = False
    storage_costs_considered: bool = False
    multi_language_support: bool = False
    faceted_search_planned: bool = False
    monitoring_observability_planned: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "index_schema_defined": self.index_schema_defined,
            "tokenization_strategy_defined": self.tokenization_strategy_defined,
            "relevance_tuning_planned": self.relevance_tuning_planned,
            "update_frequency_specified": self.update_frequency_specified,
            "index_rebuild_considered": self.index_rebuild_considered,
            "consistency_guarantees_defined": self.consistency_guarantees_defined,
            "query_performance_optimized": self.query_performance_optimized,
            "storage_costs_considered": self.storage_costs_considered,
            "multi_language_support": self.multi_language_support,
            "faceted_search_planned": self.faceted_search_planned,
            "monitoring_observability_planned": self.monitoring_observability_planned,
        }

    @property
    def readiness_score(self) -> float:
        """
        Calculate readiness score based on schema completeness, performance considerations, and monitoring.

        Returns:
            Float between 0.0 and 1.0 representing overall search indexing readiness.
        """
        # Core indexing elements (30% weight)
        core_score = sum([
            self.index_schema_defined,
            self.tokenization_strategy_defined,
            self.update_frequency_specified,
        ]) / 3.0 * 0.3

        # Performance and reliability (40% weight)
        performance_score = sum([
            self.query_performance_optimized,
            self.relevance_tuning_planned,
            self.consistency_guarantees_defined,
            self.index_rebuild_considered,
        ]) / 4.0 * 0.4

        # Operations and scalability (30% weight)
        operations_score = sum([
            self.storage_costs_considered,
            self.monitoring_observability_planned,
            self.multi_language_support or self.faceted_search_planned,
        ]) / 3.0 * 0.3

        return core_score + performance_score + operations_score

    @property
    def recommendations(self) -> list[str]:
        """
        Generate recommendations for production-ready search indexing.

        Returns:
            List of actionable recommendations for improving search indexing strategy.
        """
        recs = []

        if not self.index_schema_defined:
            recs.append("Define index schema with field types, analyzers, and mappings")

        if not self.tokenization_strategy_defined:
            recs.append("Specify tokenization strategy including analyzers, stemming, and stop words")

        if not self.relevance_tuning_planned:
            recs.append("Plan relevance tuning with field boosting, scoring algorithms, and fuzzy matching")

        if not self.update_frequency_specified:
            recs.append("Define index update frequency (real-time, near real-time, or batch)")

        if not self.consistency_guarantees_defined:
            recs.append("Document consistency guarantees and acceptable search staleness")

        if not self.query_performance_optimized:
            recs.append("Optimize query performance with caching and filter context strategies")

        if not self.storage_costs_considered and (
            self.index_schema_defined or self.tokenization_strategy_defined
        ):
            recs.append("Consider storage costs including sharding, replication, and compression")

        if not self.index_rebuild_considered and self.index_schema_defined:
            recs.append("Plan index rebuild strategy with zero-downtime migration approach")

        if not self.monitoring_observability_planned:
            recs.append("Add monitoring for search metrics, query latency, and index health")

        if self.multi_language_support and not self.tokenization_strategy_defined:
            recs.append("Define language-specific analyzers for multi-language search")

        if self.faceted_search_planned and not self.query_performance_optimized:
            recs.append("Optimize aggregation queries for faceted search performance")

        return recs


def analyze_search_indexing_strategy(change_brief: Mapping[str, Any]) -> SearchIndexingStrategyAnalysis:
    """
    Analyze search indexing strategy from a change brief.

    Args:
        change_brief: A mapping containing change information with fields like
                     'title', 'description', 'acceptance_criteria', etc.

    Returns:
        SearchIndexingStrategyAnalysis with boolean flags and readiness metrics.
    """
    if not isinstance(change_brief, Mapping):
        return SearchIndexingStrategyAnalysis()

    searchable_text = _extract_searchable_text(change_brief)

    return SearchIndexingStrategyAnalysis(
        index_schema_defined=bool(_INDEX_SCHEMA_RE.search(searchable_text)),
        tokenization_strategy_defined=bool(_TOKENIZATION_STRATEGY_RE.search(searchable_text)),
        relevance_tuning_planned=bool(_RELEVANCE_TUNING_RE.search(searchable_text)),
        update_frequency_specified=bool(_UPDATE_FREQUENCY_RE.search(searchable_text)),
        index_rebuild_considered=bool(_INDEX_REBUILD_RE.search(searchable_text)),
        consistency_guarantees_defined=bool(_CONSISTENCY_GUARANTEES_RE.search(searchable_text)),
        query_performance_optimized=bool(_QUERY_PERFORMANCE_RE.search(searchable_text)),
        storage_costs_considered=bool(_STORAGE_COSTS_RE.search(searchable_text)),
        multi_language_support=bool(_MULTI_LANGUAGE_RE.search(searchable_text)),
        faceted_search_planned=bool(_FACETED_SEARCH_RE.search(searchable_text)),
        monitoring_observability_planned=bool(_MONITORING_OBSERVABILITY_RE.search(searchable_text)),
    )


def _extract_searchable_text(payload: Mapping[str, Any]) -> str:
    """Extract and normalize text from common change brief fields."""
    field_names = (
        "title",
        "description",
        "summary",
        "body",
        "acceptance_criteria",
        "acceptance",
        "requirements",
        "constraints",
        "approach",
        "implementation",
        "notes",
        "risks",
        "testing_strategy",
        "rollback_plan",
    )
    parts: list[str] = []
    for field_name in field_names:
        value = payload.get(field_name)
        if value is not None:
            parts.extend(_strings(value))
    return _SPACE_RE.sub(" ", " ".join(parts))


def _strings(value: Any) -> list[str]:
    """Extract strings from various data structures."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=str):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        strings = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    return [str(value)]


__all__ = [
    "SearchIndexingStrategyAnalysis",
    "analyze_search_indexing_strategy",
]
