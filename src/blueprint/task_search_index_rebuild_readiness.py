"""Assess readiness for search index rebuild, reindex, and migration tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskSearchIndexRebuildReadinessPlan = SimpleReadinessPlan
TaskSearchIndexRebuildReadinessRecord = SimpleReadinessRecord
TaskSearchIndexRebuildReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "search_index_rebuild": re.compile(
        r"\b(?:search index|index rebuild|rebuild index|recreate index|index migration|"
        r"mapping migration|search migration|elasticsearch|opensearch|solr|algolia)\b",
        re.I,
    ),
    "reindex": re.compile(
        r"\b(?:reindex|re-index|full reindex|partial reindex|reindexing|bulk index|bulk indexing)\b",
        re.I,
    ),
    "search_backfill": re.compile(
        r"\b(?:search backfill|index backfill|backfill search|backfill index|populate index|"
        r"historical index)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "search_index_rebuild": re.compile(r"search.*index|index.*search|rebuild[_-]?index|elasticsearch|opensearch|solr|algolia", re.I),
    "reindex": re.compile(r"reindex|re[_-]?index|bulk[_-]?index", re.I),
    "search_backfill": re.compile(r"search[_-]?backfill|index[_-]?backfill|populate[_-]?index", re.I),
}
_CRITERIA = {
    "index_versioning": re.compile(
        r"\b(?:index version|versioned index|schema version|mapping version|alias version|"
        r"blue[- ]green index|new index name|index alias)\b",
        re.I,
    ),
    "dual_write_or_backfill_plan": re.compile(
        r"\b(?:dual write|dual-write|backfill plan|reindex plan|index backfill|bulk backfill|"
        r"change feed|cdc|incremental indexing|watermark|batch(?:ing)? plan)\b",
        re.I,
    ),
    "query_parity_checks": re.compile(
        r"\b(?:query parity|sample quer(?:y|ies)|result parity|hit count|document count|"
        r"relevance check|search regression|ranking regression|facet parity)\b",
        re.I,
    ),
    "cutover_rollback": re.compile(
        r"\b(?:cutover rollback|rollback|roll back|alias swap|swap back|restore alias|"
        r"previous index|old index|feature flag|kill switch|revert cutover)\b",
        re.I,
    ),
    "capacity_planning": re.compile(
        r"\b(?:capacity plan|capacity planning|cluster capacity|disk space|shard|replica|"
        r"throughput|queue depth|bulk size|concurrency|rate limit|throttle)\b",
        re.I,
    ),
    "stale_index_monitoring": re.compile(
        r"\b(?:stale index|stale results?|index freshness|freshness lag|index lag|indexing lag|"
        r"staleness|monitoring|dashboard|alert|metrics)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "index_versioning": "Define versioned index names, mappings, schemas, or aliases for the rebuild.",
    "dual_write_or_backfill_plan": "Document the dual-write, incremental indexing, or backfill plan.",
    "query_parity_checks": "Add query parity, result count, relevance, ranking, or facet checks.",
    "cutover_rollback": "Specify cutover rollback, alias swap-back, feature flag, or previous-index restore steps.",
    "capacity_planning": "Plan cluster capacity, shards, replicas, bulk size, throughput, and throttling.",
    "stale_index_monitoring": "Monitor stale results, freshness lag, indexing lag, dashboards, and alerts.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:search index|reindex|index rebuild|search backfill)\b"
    r".{0,80}\b(?:required|needed|planned|scope|impact|changes?)\b",
    re.I,
)


def build_task_search_index_rebuild_readiness_plan(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    """Build search index rebuild readiness records for task-shaped input."""
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Search Index Rebuild Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_search_index_rebuild_readiness(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    return build_task_search_index_rebuild_readiness_plan(source)


def extract_task_search_index_rebuild_readiness(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    return build_task_search_index_rebuild_readiness_plan(source)


def generate_task_search_index_rebuild_readiness(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    return build_task_search_index_rebuild_readiness_plan(source)


def derive_task_search_index_rebuild_readiness(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    return build_task_search_index_rebuild_readiness_plan(source)


def summarize_task_search_index_rebuild_readiness(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    return build_task_search_index_rebuild_readiness_plan(source)


def recommend_task_search_index_rebuild_readiness(source: Any) -> TaskSearchIndexRebuildReadinessPlan:
    return build_task_search_index_rebuild_readiness_plan(source)


def task_search_index_rebuild_readiness_plan_to_dict(
    result: TaskSearchIndexRebuildReadinessPlan,
) -> dict[str, Any]:
    return result.to_dict()


task_search_index_rebuild_readiness_plan_to_dict.__test__ = False


def task_search_index_rebuild_readiness_plan_to_dicts(
    result: TaskSearchIndexRebuildReadinessPlan | Iterable[TaskSearchIndexRebuildReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_search_index_rebuild_readiness_plan_to_dicts.__test__ = False
task_search_index_rebuild_readiness_to_dicts = task_search_index_rebuild_readiness_plan_to_dicts
task_search_index_rebuild_readiness_to_dicts.__test__ = False


def task_search_index_rebuild_readiness_plan_to_markdown(result: TaskSearchIndexRebuildReadinessPlan) -> str:
    return result.to_markdown()


task_search_index_rebuild_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskSearchIndexRebuildReadinessPlan",
    "TaskSearchIndexRebuildReadinessRecord",
    "TaskSearchIndexRebuildReadinessRecommendation",
    "analyze_task_search_index_rebuild_readiness",
    "build_task_search_index_rebuild_readiness_plan",
    "derive_task_search_index_rebuild_readiness",
    "extract_task_search_index_rebuild_readiness",
    "generate_task_search_index_rebuild_readiness",
    "recommend_task_search_index_rebuild_readiness",
    "summarize_task_search_index_rebuild_readiness",
    "task_search_index_rebuild_readiness_plan_to_dict",
    "task_search_index_rebuild_readiness_plan_to_dicts",
    "task_search_index_rebuild_readiness_plan_to_markdown",
    "task_search_index_rebuild_readiness_to_dicts",
]
