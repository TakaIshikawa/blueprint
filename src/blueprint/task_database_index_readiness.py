"""Plan database index rollout readiness for execution tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskDatabaseIndexReadinessPlan = SimpleReadinessPlan
TaskDatabaseIndexReadinessRecord = SimpleReadinessRecord

_SIGNALS = {
    "database_index": re.compile(
        r"\b(?:database index|db index|add index|create index|index rollout|index migration|"
        r"reindex|query index|postgres index|mysql index)\b",
        re.I,
    ),
    "query_performance": re.compile(
        r"\b(?:slow query|query performance|query latency|query plan|explain analyze|table scan|seq scan)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "database_index": re.compile(r"(?:index|indexes|indices|migration|schema)", re.I),
    "query_performance": re.compile(r"(?:query|explain|performance|slow)", re.I),
}
_CRITERIA = {
    "target_table_query": re.compile(
        r"\b(?:target table|table\s+\w+|on\s+\w+\s+table|query pattern|where clause|join|order by|filter)\b",
        re.I,
    ),
    "index_shape": re.compile(
        r"\b(?:index shape|composite index|covering index|partial index|unique index|btree|gin|gist|"
        r"columns?|column order|include columns?|create index)\b",
        re.I,
    ),
    "migration_strategy": re.compile(
        r"\b(?:migration strategy|schema migration|database migration|ddl|rollout plan|expand migrate|online migration)\b",
        re.I,
    ),
    "concurrent_backfill_safety": re.compile(
        r"\b(?:concurrently|concurrent index|online index|backfill safety|non[- ]blocking|background migration|"
        r"chunked backfill|batched backfill|avoid blocking)\b",
        re.I,
    ),
    "lock_downtime_risk": re.compile(
        r"\b(?:lock risk|table lock|downtime|blocking writes?|blocking reads?|lock timeout|statement timeout|"
        r"maintenance window|zero downtime)\b",
        re.I,
    ),
    "query_plan_validation": re.compile(
        r"\b(?:explain analyze|query plan|planner|index scan|no seq scan|before/after plan|performance validation|"
        r"load test|benchmark)\b",
        re.I,
    ),
    "rollback_removal_plan": re.compile(
        r"\b(?:rollback|drop index|remove index|revert migration|down migration|removal plan|cleanup index)\b",
        re.I,
    ),
    "ownership": re.compile(
        r"\b(?:owner|dri|database owner|data platform|backend owner|on[- ]call|owning team)\b",
        re.I,
    ),
    "monitoring_evidence": re.compile(
        r"\b(?:monitoring|metrics?|alerts?|dashboard|slow query log|pg_stat|query latency|error rate|observability)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "target_table_query": "Identify the target table, query pattern, filters, joins, and ordering the index supports.",
    "index_shape": "Specify index columns, order, uniqueness, partial predicate, covering columns, and index type.",
    "migration_strategy": "Document the schema migration and rollout strategy for adding the index.",
    "concurrent_backfill_safety": "Use concurrent, online, chunked, or otherwise non-blocking index/backfill behavior.",
    "lock_downtime_risk": "Assess lock, timeout, and downtime risk before applying the migration.",
    "query_plan_validation": "Validate before/after query plans and performance with EXPLAIN or benchmark evidence.",
    "rollback_removal_plan": "Define rollback, drop-index, down-migration, or removal steps.",
    "ownership": "Name the database, backend, data platform, or on-call owner.",
    "monitoring_evidence": "Add monitoring for slow queries, latency, lock waits, errors, and rollout alerts.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:database index|db index|index migration|query index)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


def build_task_database_index_readiness_plan(source: Any) -> TaskDatabaseIndexReadinessPlan:
    """Build database index rollout readiness findings for relevant tasks."""
    return build_simple_readiness_plan(
        source,
        title="Task Database Index Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_database_index_readiness(source: Any) -> TaskDatabaseIndexReadinessPlan:
    return build_task_database_index_readiness_plan(source)


def summarize_task_database_index_readiness(source: Any) -> TaskDatabaseIndexReadinessPlan:
    return build_task_database_index_readiness_plan(source)


def extract_task_database_index_readiness(source: Any) -> TaskDatabaseIndexReadinessPlan:
    return build_task_database_index_readiness_plan(source)


def generate_task_database_index_readiness(source: Any) -> TaskDatabaseIndexReadinessPlan:
    return build_task_database_index_readiness_plan(source)


def recommend_task_database_index_readiness(source: Any) -> TaskDatabaseIndexReadinessPlan:
    return build_task_database_index_readiness_plan(source)


def task_database_index_readiness_plan_to_dict(result: TaskDatabaseIndexReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_database_index_readiness_plan_to_dict.__test__ = False


def task_database_index_readiness_plan_to_dicts(result: Any) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [item.to_dict() for item in result]


task_database_index_readiness_plan_to_dicts.__test__ = False


def task_database_index_readiness_plan_to_markdown(result: TaskDatabaseIndexReadinessPlan) -> str:
    return result.to_markdown()


task_database_index_readiness_plan_to_markdown.__test__ = False
