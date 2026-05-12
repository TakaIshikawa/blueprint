"""Assess readiness for database connection pool change tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskDatabaseConnectionPoolReadinessPlan = SimpleReadinessPlan
TaskDatabaseConnectionPoolReadinessRecord = SimpleReadinessRecord
TaskDatabaseConnectionPoolReadinessFinding = SimpleReadinessRecord
TaskDatabaseConnectionPoolReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "db_pool": re.compile(
        r"\b(?:(?:db|database|postgres|postgresql|mysql|mariadb|sql).{0,60}(?:connection pool|pooling|pool size)|"
        r"(?:connection pool|pooling|pool size).{0,60}(?:db|database|postgres|postgresql|mysql|mariadb|sql))\b",
        re.I,
    ),
    "connection_limit": re.compile(
        r"\b(?:connection limits?|max(?:imum)? connections?|max_connections|connection cap|"
        r"pool max|pool limit|max pool size|min pool size|pool sizing)\b",
        re.I,
    ),
    "pool_timeout": re.compile(
        r"\b(?:pool timeout|connection timeout|acquire timeout|checkout timeout|idle timeout|"
        r"pool wait|wait_timeout|connect_timeout|statement timeout)\b",
        re.I,
    ),
    "database_client_config": re.compile(
        r"\b(?:database client|db client|datasource|data source|jdbc|hikari|sequelize|typeorm|"
        r"knex|sqlalchemy|psycopg|pgbouncer|rds proxy|connection string)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "db_pool": re.compile(r"(?:^|/)(?:db|database|databases|postgres|postgresql|mysql)(?:/|$)|pool", re.I),
    "connection_limit": re.compile(r"connections?|max[_-]?connections?|connection[_-]?limit|pool[_-]?size", re.I),
    "pool_timeout": re.compile(r"(?:pool|connection|checkout|acquire|idle)[_-]?timeout|wait[_-]?timeout", re.I),
    "database_client_config": re.compile(
        r"(?:database|db|postgres|postgresql|mysql|datasource|jdbc|hikari|sequelize|typeorm|knex|sqlalchemy|pgbouncer).*\.(?:ya?ml|json|toml|ini|conf|env|properties|py|ts|js)$",
        re.I,
    ),
}
_CRITERIA = {
    "pool_sizing": re.compile(
        r"\b(?:pool sizing|pool size|max pool size|min pool size|max(?:imum)? connections?|"
        r"pool max|pool min|connection limit|capacity model|concurrency budget|worker count)\b",
        re.I,
    ),
    "timeout_policy": re.compile(
        r"\b(?:pool timeout|connection timeout|checkout timeout|acquire timeout|idle timeout|"
        r"wait timeout|timeout policy|bounded wait)\b",
        re.I,
    ),
    "retry_behavior": re.compile(
        r"\b(?:retry|retries|backoff|jitter|retry budget|bounded retry|fail fast|circuit breaker)\b",
        re.I,
    ),
    "saturation_protection": re.compile(
        r"\b(?:saturation|pool exhaustion|exhausted pool|queue depth|backpressure|shed load|"
        r"load shedding|throttle|throttling|connection storm)\b",
        re.I,
    ),
    "migration_rollout": re.compile(
        r"\b(?:migration rollout|rollout|phased rollout|canary|dark launch|feature flag|"
        r"gradual ramp|staged deploy|deployment plan)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|monitoring|metrics?|alerts?|alerting|dashboard|logs?|traces?|"
        r"pool utilization|active connections|idle connections|wait time|timeout rate)\b",
        re.I,
    ),
    "failover": re.compile(
        r"\b(?:failover|fail over|replica promotion|primary failover|database outage|db outage|"
        r"rds failover|multi-az|read replica|standby)\b",
        re.I,
    ),
    "rollback": re.compile(
        r"\b(?:rollback|roll back|revert|restore|previous config|config rollback|kill switch|"
        r"disable flag|abort plan)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "pool_sizing": "Define max and min pool sizing against database connection limits and application concurrency.",
    "timeout_policy": "Specify acquire, checkout, idle, and connection timeout behavior for the pool.",
    "retry_behavior": "Document retry, backoff, fail-fast, or circuit-breaker behavior for pool acquisition failures.",
    "saturation_protection": "Add saturation safeguards such as backpressure, queue limits, throttling, or load shedding.",
    "migration_rollout": "Plan a canary, phased rollout, feature flag, or staged migration for the pool change.",
    "observability": "Add metrics, dashboards, logs, and alerts for pool utilization, wait time, and timeout rates.",
    "failover": "Describe behavior during database failover, replica promotion, or database outage scenarios.",
    "rollback": "Document rollback or kill-switch steps that restore the previous pool configuration.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:connection pool|pool sizing|database client|db client|connection limit)\b"
    r".{0,100}\b(?:scope|impact|changes?|required|needed|planned)\b",
    re.I,
)


def build_task_database_connection_pool_readiness_plan(source: Any) -> TaskDatabaseConnectionPoolReadinessPlan:
    """Build database connection pool readiness records for task-shaped input."""
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Database Connection Pool Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_database_connection_pool_readiness(source: Any) -> TaskDatabaseConnectionPoolReadinessPlan:
    return build_task_database_connection_pool_readiness_plan(source)


def extract_task_database_connection_pool_readiness(source: Any) -> TaskDatabaseConnectionPoolReadinessPlan:
    return build_task_database_connection_pool_readiness_plan(source)


def generate_task_database_connection_pool_readiness(source: Any) -> TaskDatabaseConnectionPoolReadinessPlan:
    return build_task_database_connection_pool_readiness_plan(source)


def derive_task_database_connection_pool_readiness(source: Any) -> TaskDatabaseConnectionPoolReadinessPlan:
    return build_task_database_connection_pool_readiness_plan(source)


def summarize_task_database_connection_pool_readiness(source: Any) -> TaskDatabaseConnectionPoolReadinessPlan:
    return build_task_database_connection_pool_readiness_plan(source)


def summarize_task_database_connection_pool_readiness_plan(source: Any) -> TaskDatabaseConnectionPoolReadinessPlan:
    return build_task_database_connection_pool_readiness_plan(source)


def recommend_task_database_connection_pool_readiness(source: Any) -> tuple[TaskDatabaseConnectionPoolReadinessRecord, ...]:
    return build_task_database_connection_pool_readiness_plan(source).records


def task_database_connection_pool_readiness_plan_to_dict(result: TaskDatabaseConnectionPoolReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_database_connection_pool_readiness_plan_to_dict.__test__ = False


def task_database_connection_pool_readiness_plan_to_dicts(
    result: TaskDatabaseConnectionPoolReadinessPlan | Iterable[TaskDatabaseConnectionPoolReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_database_connection_pool_readiness_plan_to_dicts.__test__ = False
task_database_connection_pool_readiness_to_dicts = task_database_connection_pool_readiness_plan_to_dicts
task_database_connection_pool_readiness_to_dicts.__test__ = False


def task_database_connection_pool_readiness_plan_to_markdown(result: TaskDatabaseConnectionPoolReadinessPlan) -> str:
    return result.to_markdown()


task_database_connection_pool_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskDatabaseConnectionPoolReadinessFinding",
    "TaskDatabaseConnectionPoolReadinessPlan",
    "TaskDatabaseConnectionPoolReadinessRecord",
    "TaskDatabaseConnectionPoolReadinessRecommendation",
    "analyze_task_database_connection_pool_readiness",
    "build_task_database_connection_pool_readiness_plan",
    "derive_task_database_connection_pool_readiness",
    "extract_task_database_connection_pool_readiness",
    "generate_task_database_connection_pool_readiness",
    "recommend_task_database_connection_pool_readiness",
    "summarize_task_database_connection_pool_readiness",
    "summarize_task_database_connection_pool_readiness_plan",
    "task_database_connection_pool_readiness_plan_to_dict",
    "task_database_connection_pool_readiness_plan_to_dicts",
    "task_database_connection_pool_readiness_plan_to_markdown",
    "task_database_connection_pool_readiness_to_dicts",
]
