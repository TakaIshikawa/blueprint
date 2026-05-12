from blueprint.task_data_warehouse_sync_readiness import (
    analyze_task_data_warehouse_sync_readiness,
    task_data_warehouse_sync_readiness_plan_to_dict,
    task_data_warehouse_sync_readiness_plan_to_markdown,
)


def test_ready_data_warehouse_sync_task_covers_expected_criteria():
    plan = analyze_task_data_warehouse_sync_readiness(
        [{
            "id": "task-ready",
            "title": "Build data warehouse sync to BigQuery",
            "description": (
                "Source tables and source events come from orders CDC. Destination schema is analytics.orders_mart. "
                "Sync mode is incremental upsert batch. Freshness SLA is hourly with lag alerts. Backfill boundary "
                "uses a historical start date and watermark. Idempotency uses merge key deduplication. Privacy "
                "filter masks PII and checks consent. Reconciliation tests compare row count checksum and data quality."
            ),
            "files_or_modules": ["src/warehouse/bigquery_orders_sync.py"],
        }]
    )

    record = plan.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert "warehouse" in record.detected_signals


def test_partial_data_warehouse_sync_task_returns_followups():
    plan = analyze_task_data_warehouse_sync_readiness(
        [{"id": "task-partial", "title": "ETL customer data to warehouse", "description": "Source tables are customers and destination schema is bi.customer_dim."}]
    )

    record = plan.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("source_tables_events", "destination_schema")
    assert "Specify sync mode" in record.recommended_follow_up_actions[0]


def test_sparse_data_warehouse_sync_task_needs_planning():
    plan = analyze_task_data_warehouse_sync_readiness("Warehouse sync rollout")

    assert plan.records[0].readiness == "needs_planning"
    assert len(plan.records[0].missing_criteria) == 8


def test_generic_analytics_instrumentation_without_warehouse_sync_is_ignored():
    plan = analyze_task_data_warehouse_sync_readiness(
        [{"id": "task-analytics", "title": "Add analytics instrumentation", "description": "Track button clicks and page views."}]
    )

    assert plan.records == ()
    assert plan.ignored_task_ids == ("task-analytics",)


def test_data_warehouse_sync_serialization_and_markdown_are_deterministic():
    plan = analyze_task_data_warehouse_sync_readiness(
        [{"id": "task-wh", "title": "Warehouse sync", "description": "Destination schema is analytics.fact_orders."}]
    )

    payload = task_data_warehouse_sync_readiness_plan_to_dict(plan)
    assert payload["summary"]["missing_criterion_count"] == 7
    assert task_data_warehouse_sync_readiness_plan_to_markdown(plan) == plan.to_markdown()
