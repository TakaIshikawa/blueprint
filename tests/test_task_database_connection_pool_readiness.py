import copy
import json

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord
from blueprint.domain.models import ExecutionPlan
from blueprint.task_database_connection_pool_readiness import (
    TaskDatabaseConnectionPoolReadinessPlan,
    analyze_task_database_connection_pool_readiness,
    build_task_database_connection_pool_readiness_plan,
    recommend_task_database_connection_pool_readiness,
    summarize_task_database_connection_pool_readiness,
    summarize_task_database_connection_pool_readiness_plan,
    task_database_connection_pool_readiness_plan_to_dict,
    task_database_connection_pool_readiness_plan_to_dicts,
    task_database_connection_pool_readiness_plan_to_markdown,
)


def test_complete_database_pool_task_is_ready():
    result = build_task_database_connection_pool_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Resize Postgres connection pool",
                    description="Change the database connection pool and max connections for checkout workers.",
                    acceptance_criteria=[
                        "Pool sizing defines min pool size, max pool size, and the database connection limit.",
                        "Timeout policy covers acquire timeout, checkout timeout, idle timeout, and connection timeout.",
                        "Retry behavior uses bounded retries with backoff, jitter, and fail-fast handling.",
                        "Saturation protection adds backpressure, queue depth limits, throttling, and load shedding.",
                        "Migration rollout uses a feature flag, canary, and phased rollout.",
                        "Observability adds pool utilization, active connection, wait time, timeout rate dashboards, and alerts.",
                        "Failover behavior covers RDS failover, replica promotion, and database outage cases.",
                        "Rollback restores the previous config through a kill switch.",
                    ],
                    files_or_modules=["config/postgres/pool.yaml"],
                )
            ]
        )
    )

    assert isinstance(result, TaskDatabaseConnectionPoolReadinessPlan)
    assert isinstance(result, SimpleReadinessPlan)
    record = result.records[0]
    assert isinstance(record, SimpleReadinessRecord)
    assert record.readiness == "ready"
    assert record.detected_signals == ("db_pool", "connection_limit", "pool_timeout", "database_client_config")
    assert record.present_criteria == (
        "pool_sizing",
        "timeout_policy",
        "retry_behavior",
        "saturation_protection",
        "migration_rollout",
        "observability",
        "failover",
        "rollback",
    )
    assert record.missing_criteria == ()
    assert result.summary["missing_criterion_count"] == 0


def test_detects_metadata_and_path_hints_with_partial_and_needs_planning_records():
    source = _plan(
        [
            _task(
                "task-partial",
                title="Tune database client config",
                description="Update DB client connection timeout settings.",
                metadata={
                    "driver": "Hikari datasource pool max tuning",
                    "safeguards": "Rollout uses a canary and metrics dashboard with timeout rate alerts.",
                },
                files_or_modules=["services/billing/database/hikari.properties"],
            ),
            _task(
                "task-path-only",
                title="Move connection config",
                description="Refactor service config.",
                files_or_modules=["config/mysql/connection_pool.json"],
            ),
            _task(
                "task-docs",
                title="Update runbook copy",
                description="No database connection pool changes are planned for this docs update.",
            ),
        ]
    )
    original = copy.deepcopy(source)

    result = analyze_task_database_connection_pool_readiness(ExecutionPlan.model_validate(source))

    assert source == original
    by_id = {record.task_id: record for record in result.records}
    assert result.impacted_task_ids == ("task-path-only", "task-partial")
    assert result.ignored_task_ids == ("task-docs",)
    assert by_id["task-partial"].readiness == "partial"
    assert by_id["task-partial"].present_criteria == ("pool_sizing", "timeout_policy", "migration_rollout", "observability")
    assert by_id["task-path-only"].readiness == "needs_planning"
    assert by_id["task-path-only"].detected_signals == ("db_pool", "connection_limit", "database_client_config")
    assert any("metadata.driver" in item for item in by_id["task-partial"].evidence)
    assert any("files_or_modules" in item for item in by_id["task-path-only"].evidence)


def test_aliases_serialization_and_markdown_are_stable():
    source = _plan(
        [
            _task(
                "task-alias",
                title="Update PgBouncer pool timeout",
                description="PgBouncer pool timeout change with rollback.",
                files_or_modules=["db/pgbouncer.ini"],
            )
        ],
        plan_id="plan-db-pool-alias",
    )

    result = summarize_task_database_connection_pool_readiness(source)
    payload = task_database_connection_pool_readiness_plan_to_dict(result)
    markdown = task_database_connection_pool_readiness_plan_to_markdown(result)

    assert summarize_task_database_connection_pool_readiness_plan(result) is result
    assert recommend_task_database_connection_pool_readiness(source) == result.records
    assert build_task_database_connection_pool_readiness_plan(result) is result
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-db-pool-alias"
    assert task_database_connection_pool_readiness_plan_to_dicts(result) == payload["records"]
    assert markdown.startswith("# Task Database Connection Pool Readiness: plan-db-pool-alias")
    assert "| Task | Title | Readiness |" in markdown


def test_invalid_inputs_return_empty_simple_plan():
    assert build_task_database_connection_pool_readiness_plan(42).records == ()
    assert build_task_database_connection_pool_readiness_plan({"id": "bad", "tasks": "not a list"}).records == ()
    assert build_task_database_connection_pool_readiness_plan({"tasks": []}).summary["task_count"] == 0


def _plan(tasks, *, plan_id="plan-db-pool"):
    return {"id": plan_id, "implementation_brief_id": "brief-db-pool", "milestones": [], "tasks": tasks}


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    return task
