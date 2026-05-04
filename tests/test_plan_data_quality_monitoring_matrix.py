import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_data_quality_monitoring_matrix import (
    PlanDataQualityMonitoringMatrix,
    PlanDataQualityMonitoringRow,
    analyze_plan_data_quality_monitoring_matrix,
    build_plan_data_quality_monitoring_matrix,
    plan_data_quality_monitoring_matrix_to_dict,
    plan_data_quality_monitoring_matrix_to_dicts,
    plan_data_quality_monitoring_matrix_to_markdown,
    summarize_plan_data_quality_monitoring_matrix,
)


def test_etl_task_with_complete_monitoring_coverage():
    result = build_plan_data_quality_monitoring_matrix(
        _plan(
            [
                _task(
                    "task-etl-pipeline",
                    title="ETL pipeline for customer data",
                    description=(
                        "Extract, transform, and load customer data from source systems. "
                        "Includes row count checks, freshness monitoring, null constraint validation, "
                        "duplicate detection, reconciliation with source, alert owner on failures, "
                        "monitoring dashboard, sampling validation, rollback procedures, and audit trail."
                    ),
                    acceptance_criteria=[
                        "Row count matches source.",
                        "Freshness check passes.",
                        "No null values in required fields.",
                        "Duplicate records removed.",
                        "Reconciliation report generated.",
                        "Alert owner configured.",
                        "Dashboard created.",
                        "Sample validation complete.",
                        "Rollback tested.",
                        "Audit evidence recorded.",
                    ],
                ),
                _task("task-internal-refactor", title="Refactor internal API", description="Internal refactoring."),
            ]
        )
    )

    assert isinstance(result, PlanDataQualityMonitoringMatrix)
    assert isinstance(result.rows[0], PlanDataQualityMonitoringRow)
    assert result.data_change_task_ids == ("task-etl-pipeline",)
    assert result.no_data_change_task_ids == ("task-internal-refactor",)
    assert result.rows[0].data_change_signal == "etl"
    assert result.rows[0].row_count_check == "present"
    assert result.rows[0].freshness_check == "present"
    assert result.rows[0].null_constraint_check == "present"
    assert result.rows[0].duplicate_detection == "present"
    assert result.rows[0].reconciliation == "present"
    assert result.rows[0].alert_owner == "present"
    assert result.rows[0].dashboard == "present"
    assert result.rows[0].sampling == "present"
    assert result.rows[0].rollback_path == "present"
    assert result.rows[0].audit_evidence == "present"
    assert result.rows[0].missing_safeguards == ()
    assert result.summary["data_change_task_count"] == 1
    assert result.summary["tasks_with_complete_coverage"] == 1
    assert result.summary["tasks_with_missing_safeguards"] == 0


def test_multiple_data_change_tasks_with_missing_safeguards():
    result = build_plan_data_quality_monitoring_matrix(
        _plan(
            [
                _task(
                    "task-sync-job",
                    title="Sync job for external API",
                    description=(
                        "Synchronize data from external API to database. "
                        "Has row count validation and freshness monitoring."
                    ),
                    acceptance_criteria=[
                        "Row count validated.",
                        "Freshness check implemented.",
                    ],
                ),
                _task(
                    "task-migration",
                    title="Database migration for schema update",
                    description=(
                        "Migrate customer table schema. "
                        "Includes reconciliation and rollback procedures."
                    ),
                    acceptance_criteria=[
                        "Data reconciled with old schema.",
                        "Rollback tested.",
                    ],
                ),
                _task(
                    "task-backfill",
                    title="Backfill historical analytics events",
                    description=(
                        "Backfill missing analytics events. "
                        "No monitoring in place yet."
                    ),
                    acceptance_criteria=["Historical data loaded."],
                ),
            ]
        )
    )

    assert len(result.rows) == 3

    # Check sync job
    sync_row = next(row for row in result.rows if "sync" in row.task_id)
    assert sync_row.data_change_signal == "sync_job"
    assert sync_row.row_count_check == "present"
    assert sync_row.freshness_check == "present"
    assert len(sync_row.missing_safeguards) > 0

    # Check migration
    migration_row = next(row for row in result.rows if "migration" in row.task_id)
    assert migration_row.data_change_signal == "migration"
    assert migration_row.reconciliation == "present"
    assert migration_row.rollback_path == "present"
    assert len(migration_row.missing_safeguards) > 0

    # Check backfill
    backfill_row = next(row for row in result.rows if "backfill" in row.task_id)
    assert backfill_row.data_change_signal == "backfill"
    assert len(backfill_row.missing_safeguards) == len(result.rows[0].to_dict()) - 5  # All safeguards missing

    # Check summary
    assert result.summary["data_change_task_count"] == 3
    assert result.summary["tasks_with_missing_safeguards"] == 3
    assert result.summary["tasks_with_complete_coverage"] == 0


def test_unrelated_tasks_produce_empty_output():
    result = build_plan_data_quality_monitoring_matrix(
        _plan(
            [
                _task(
                    "task-ui-update",
                    title="Update UI components",
                    description="Frontend UI improvements, no data changes.",
                    acceptance_criteria=["UI updated.", "Tests pass."],
                ),
                _task(
                    "task-api-endpoint",
                    title="Add new API endpoint",
                    description="New read-only API endpoint.",
                    acceptance_criteria=["Endpoint deployed."],
                ),
            ]
        )
    )

    assert result.rows == ()
    assert result.data_change_task_ids == ()
    assert len(result.no_data_change_task_ids) == 2
    assert result.summary["data_change_task_count"] == 0
    assert result.summary["no_data_change_task_count"] == 2
    assert "No data quality monitoring rows were inferred." in result.to_markdown()


def test_object_input_serializes_and_renders_markdown():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-import",
                    title="Import customer data from CSV",
                    description=(
                        "Bulk import of customer records. "
                        "Includes row count check, duplicate detection, and alert notifications."
                    ),
                    acceptance_criteria=[
                        "Row count validated.",
                        "Duplicates removed.",
                        "Alerts configured.",
                    ],
                )
            ]
        )
    )

    result = analyze_plan_data_quality_monitoring_matrix(plan)
    payload = plan_data_quality_monitoring_matrix_to_dict(result)

    assert isinstance(result, PlanDataQualityMonitoringMatrix)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "data_change_task_ids",
        "no_data_change_task_ids",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "data_change_signal",
        "row_count_check",
        "freshness_check",
        "null_constraint_check",
        "duplicate_detection",
        "reconciliation",
        "alert_owner",
        "dashboard",
        "sampling",
        "rollback_path",
        "audit_evidence",
        "missing_safeguards",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    markdown = plan_data_quality_monitoring_matrix_to_markdown(result)
    assert "Plan Data Quality Monitoring Matrix" in markdown
    assert "task-import" in markdown
    assert markdown == result.to_markdown()


def test_dict_helpers_and_aliases_work():
    result = build_plan_data_quality_monitoring_matrix(
        _plan(
            [
                _task(
                    "task-export",
                    title="Export analytics data",
                    description=(
                        "Data export for external analytics platform. "
                        "Dashboard monitoring in place."
                    ),
                    acceptance_criteria=[
                        "Export completed.",
                        "Dashboard shows metrics.",
                    ],
                )
            ]
        )
    )

    assert summarize_plan_data_quality_monitoring_matrix(result) == result.summary
    assert analyze_plan_data_quality_monitoring_matrix(result) is result
    dicts = plan_data_quality_monitoring_matrix_to_dicts(result)
    assert dicts == result.to_dicts()
    assert len(dicts) == 1
    assert dicts[0]["task_id"] == "task-export"
    assert dicts[0]["data_change_signal"] == "export"


def test_mixed_data_change_signals():
    result = build_plan_data_quality_monitoring_matrix(
        _plan(
            [
                _task(
                    "task-derived-table",
                    title="Create derived analytics table",
                    description=(
                        "Materialized view for analytics. "
                        "Freshness check and row count validation."
                    ),
                    acceptance_criteria=["View created.", "Freshness monitored."],
                ),
                _task(
                    "task-analytics-event",
                    title="Track user analytics events",
                    description=(
                        "Event tracking for user actions. "
                        "Sampling validation and audit trail."
                    ),
                    acceptance_criteria=["Events tracked.", "Sampling validated."],
                ),
            ]
        )
    )

    assert len(result.rows) == 2

    # Check derived table
    derived_row = next(row for row in result.rows if "derived" in row.task_id)
    assert derived_row.data_change_signal == "derived_table"
    assert derived_row.freshness_check == "present"

    # Check analytics event
    analytics_row = next(row for row in result.rows if "analytics" in row.task_id)
    assert analytics_row.data_change_signal == "analytics_event"
    assert analytics_row.sampling == "present"
    assert analytics_row.audit_evidence == "present"

    # Check summary signal counts
    assert result.summary["signal_counts"]["derived_table"] == 1
    assert result.summary["signal_counts"]["analytics_event"] == 1


def test_task_mapping_input():
    task_dict = _task(
        "task-etl",
        title="ETL job",
        description="ETL pipeline with row count and freshness checks.",
    )

    result = build_plan_data_quality_monitoring_matrix(task_dict)

    assert len(result.rows) == 1
    assert result.rows[0].task_id == "task-etl"
    assert result.rows[0].data_change_signal == "etl"


def test_invalid_input_produces_empty_matrix():
    result = build_plan_data_quality_monitoring_matrix({})

    assert result.rows == ()
    assert result.data_change_task_ids == ()
    assert result.summary["data_change_task_count"] == 0


def _plan(tasks):
    return {
        "id": "plan-data-quality",
        "implementation_brief_id": "brief-dq-monitoring",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
):
    return {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
        "metadata": metadata or {},
    }
