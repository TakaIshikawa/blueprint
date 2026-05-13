import json

from blueprint.task_database_index_readiness import (
    TaskDatabaseIndexReadinessPlan,
    TaskDatabaseIndexReadinessRecord,
    analyze_task_database_index_readiness,
    build_task_database_index_readiness_plan,
    extract_task_database_index_readiness,
    generate_task_database_index_readiness,
    recommend_task_database_index_readiness,
    task_database_index_readiness_plan_to_dict,
    task_database_index_readiness_plan_to_dicts,
    task_database_index_readiness_plan_to_markdown,
)


def test_ready_database_index_rollout_task_has_all_criteria():
    result = build_task_database_index_readiness_plan(
        _plan(
            [
                _task(
                    "idx-ready",
                    "Add orders lookup database index",
                    (
                        "Create index concurrently on orders table for customer_id and created_at query pattern. "
                        "Migration strategy uses online schema migration with zero downtime and lock timeout. "
                        "EXPLAIN ANALYZE must show index scan and query latency improvement. "
                        "Rollback drops the index with a down migration. "
                        "Owner is backend data platform and monitoring uses dashboards, metrics, and alerts."
                    ),
                    files_or_modules=["migrations/20260513_add_orders_index.py"],
                )
            ]
        )
    )

    record = result.records[0]

    assert isinstance(result, TaskDatabaseIndexReadinessPlan)
    assert isinstance(record, TaskDatabaseIndexReadinessRecord)
    assert record.readiness == "ready"
    assert record.present_criteria == (
        "target_table_query",
        "index_shape",
        "migration_strategy",
        "concurrent_backfill_safety",
        "lock_downtime_risk",
        "query_plan_validation",
        "rollback_removal_plan",
        "ownership",
        "monitoring_evidence",
    )
    assert record.missing_criteria == ()
    assert result.impacted_task_ids == ("idx-ready",)


def test_partial_database_index_rollout_reports_distinct_gaps():
    result = analyze_task_database_index_readiness(
        _plan(
            [
                _task(
                    "idx-partial",
                    "Improve slow query with index",
                    "Add composite index on invoices table for account_id filter and due_at order by.",
                )
            ]
        )
    )

    record = result.records[0]

    assert record.readiness == "partial"
    assert record.present_criteria == ("target_table_query", "index_shape")
    assert "query_plan_validation" in record.missing_criteria
    assert "rollback_removal_plan" in record.missing_criteria
    assert "ownership" in record.missing_criteria
    assert any("EXPLAIN" in action for action in record.recommended_follow_up_actions)


def test_absent_no_impact_and_serialization_are_stable():
    plan = _plan(
        [
            _task("idx-absent", "Update copy", "No database index changes are in scope."),
            _task("copy", "Copy edit", "Adjust onboarding labels."),
        ]
    )

    result = recommend_task_database_index_readiness(plan)
    payload = task_database_index_readiness_plan_to_dict(result)
    markdown = task_database_index_readiness_plan_to_markdown(result)

    assert result.records == ()
    assert result.impacted_task_ids == ()
    assert result.ignored_task_ids == ("idx-absent", "copy")
    assert result.summary["impacted_task_count"] == 0
    assert json.loads(json.dumps(payload)) == payload
    assert task_database_index_readiness_plan_to_dicts(result) == []
    assert extract_task_database_index_readiness(plan).to_dict() == result.to_dict()
    assert generate_task_database_index_readiness(plan).to_dict() == result.to_dict()
    assert markdown.startswith("# Task Database Index Readiness")
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "impacted_task_ids",
        "ignored_task_ids",
        "summary",
    ]


def _plan(tasks):
    return {"id": "plan-db-index", "tasks": tasks}


def _task(task_id, title, description, **extra):
    return {"id": task_id, "title": title, "description": description, **extra}
