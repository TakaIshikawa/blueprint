import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_data_quality_validation import (
    TaskDataQualityValidationPlan,
    TaskDataQualityValidationRecord,
    build_task_data_quality_validation_plan,
    derive_task_data_quality_validation_plan,
    summarize_task_data_quality_validation,
    task_data_quality_validation_plan_to_dict,
    task_data_quality_validation_plan_to_markdown,
)


def test_data_moving_tasks_infer_quality_categories_and_escalated_severity():
    result = build_task_data_quality_validation_plan(
        _plan(
            [
                _task(
                    "task-customer-migration",
                    title="Customer profile migration",
                    description=(
                        "Migrate customer data from legacy CRM to account profiles with schema mapping, "
                        "referential integrity checks, completeness row counts, uniqueness checks, and reconciliation."
                    ),
                    files_or_modules=["db/migrations/20260501_customer_profile.sql"],
                ),
                _task(
                    "task-etl",
                    title="Daily import/export ETL",
                    description=(
                        "Import partner CSV exports into the warehouse ETL pipeline with freshness SLA, "
                        "range validation, anomaly checks, and source-to-target checksum reconciliation."
                    ),
                    files_or_modules=["src/etl/imports/partner_csv_loader.py"],
                ),
                _task(
                    "task-analytics",
                    title="Revenue analytics report",
                    description="Build reporting dashboard analytics for billing metrics with freshness and anomaly detection.",
                    files_or_modules=["analytics/reports/revenue_dashboard.sql"],
                ),
                _task(
                    "task-normalize",
                    title="Normalize lead feed",
                    description="Deduplicate imported leads and normalize phone number values with column mapping.",
                    files_or_modules=["src/pipelines/lead_dedupe_normalize.py"],
                ),
                _task(
                    "task-destructive",
                    title="Destructive billing migration",
                    description="Run destructive migration to overwrite production billing ledger data and reconcile totals.",
                    files_or_modules=["db/migrations/20260501_billing_ledger.sql"],
                ),
                _task(
                    "task-ui",
                    title="Update empty state",
                    description="Adjust account settings copy and layout.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskDataQualityValidationPlan)
    assert result.plan_id == "plan-data-quality"
    assert result.validation_required_task_ids == (
        "task-analytics",
        "task-destructive",
        "task-customer-migration",
        "task-etl",
        "task-normalize",
    )
    assert result.no_validation_task_ids == ("task-ui",)
    assert result.summary == {
        "task_count": 6,
        "validation_required_task_count": 5,
        "no_validation_task_count": 1,
        "category_counts": {
            "completeness": 5,
            "uniqueness": 4,
            "referential_integrity": 4,
            "freshness": 3,
            "range_validation": 4,
            "reconciliation": 4,
            "anomaly_checks": 3,
        },
        "severity_counts": {"critical": 2, "high": 3, "medium": 0, "low": 0},
    }

    migration = _record(result, "task-customer-migration")
    assert isinstance(migration, TaskDataQualityValidationRecord)
    assert migration.severity == "high"
    assert migration.validation_categories == (
        "completeness",
        "uniqueness",
        "referential_integrity",
        "range_validation",
        "reconciliation",
    )
    assert any("Row-count" in item for item in migration.suggested_validation_artifacts)
    assert any("source datasets" in item.lower() for item in migration.follow_up_questions)

    etl = _record(result, "task-etl")
    assert etl.validation_categories == (
        "completeness",
        "uniqueness",
        "referential_integrity",
        "freshness",
        "range_validation",
        "reconciliation",
        "anomaly_checks",
    )
    assert any("Freshness SLA" in item for item in etl.suggested_validation_artifacts)
    assert any("Anomaly check" in item for item in etl.suggested_validation_artifacts)

    analytics = _record(result, "task-analytics")
    assert analytics.severity == "critical"
    assert analytics.validation_categories == (
        "completeness",
        "freshness",
        "range_validation",
        "anomaly_checks",
    )

    destructive = _record(result, "task-destructive")
    assert destructive.severity == "critical"
    assert "reconciliation" in destructive.validation_categories


def test_metadata_overrides_define_checks_datasets_freshness_and_thresholds():
    result = derive_task_data_quality_validation_plan(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Partner usage reconciliation",
                    description="Prepare partner usage validation records.",
                    files_or_modules={
                        "first": "src/reconciliation/usage_validation.py",
                        "duplicate": "src/reconciliation/usage_validation.py",
                    },
                    acceptance_criteria={
                        "quality": "Completeness and reconciliation evidence is attached before sign-off."
                    },
                    metadata={
                        "expected_checks": [
                            "completeness: row counts match",
                            "uniqueness: one usage row per event id",
                            "freshness: warehouse watermark under 15 minutes",
                            "reconciliation: amount variance <= 0.1%",
                        ],
                        "source_datasets": ["partner_usage.events"],
                        "destination_datasets": ["warehouse.fact_usage"],
                        "freshness_windows": "15 minutes after partner file arrival",
                        "reconciliation_thresholds": {"amount": "<= 0.1%", "rows": "exact match"},
                        "validation_commands": {
                            "quality": ["poetry run dq check usage --reconciliation --freshness"]
                        },
                    },
                )
            ]
        )
    )

    record = result.records[0]

    assert record.task_id == "task-metadata"
    assert record.severity == "low"
    assert record.validation_categories == (
        "completeness",
        "uniqueness",
        "freshness",
        "range_validation",
        "reconciliation",
    )
    assert "completeness: row counts match" in record.expected_checks
    assert record.source_datasets == ("partner_usage.events",)
    assert record.destination_datasets == ("warehouse.fact_usage",)
    assert record.freshness_windows == ("15 minutes after partner file arrival",)
    assert record.reconciliation_thresholds == ("<= 0.1%", "exact match")
    assert record.evidence.count("files_or_modules: src/reconciliation/usage_validation.py") == 1
    assert "metadata.source_datasets[0]: partner_usage.events" in record.evidence
    assert "metadata.destination_datasets[0]: warehouse.fact_usage" in record.evidence
    assert any("poetry run dq check usage --reconciliation --freshness" in item for item in record.evidence)
    assert not record.follow_up_questions


def test_empty_invalid_no_signal_serialization_markdown_and_escaping_are_stable():
    task_dict = _task(
        "task-import | pipe",
        title="Import orders | warehouse",
        description="Import order CSV with completeness checks and reconciliation.",
        files_or_modules=["src/imports/orders_csv.py"],
    )
    original = copy.deepcopy(task_dict)

    result = build_task_data_quality_validation_plan(_plan([task_dict]))
    payload = task_data_quality_validation_plan_to_dict(result)
    markdown = task_data_quality_validation_plan_to_markdown(result)
    empty = build_task_data_quality_validation_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_data_quality_validation_plan(13)
    no_signal = build_task_data_quality_validation_plan(
        _plan([_task("task-cache", title="Tune cache", description="Adjust backend cache TTL.")])
    )

    assert task_dict == original
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert result.findings == result.records
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "records",
        "validation_required_task_ids",
        "no_validation_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "severity",
        "validation_categories",
        "expected_checks",
        "source_datasets",
        "destination_datasets",
        "freshness_windows",
        "reconciliation_thresholds",
        "suggested_validation_artifacts",
        "evidence",
        "follow_up_questions",
    ]
    assert markdown.startswith("# Task Data Quality Validation Plan: plan-data-quality")
    assert "Summary: 1 validation-required tasks" in markdown
    assert "Import orders \\| warehouse" in markdown
    assert empty.to_dict() == {
        "plan_id": "empty-plan",
        "records": [],
        "validation_required_task_ids": [],
        "no_validation_task_ids": [],
        "summary": {
            "task_count": 0,
            "validation_required_task_count": 0,
            "no_validation_task_count": 0,
            "category_counts": {
                "completeness": 0,
                "uniqueness": 0,
                "referential_integrity": 0,
                "freshness": 0,
                "range_validation": 0,
                "reconciliation": 0,
                "anomaly_checks": 0,
            },
            "severity_counts": {"critical": 0, "high": 0, "medium": 0, "low": 0},
        },
    }
    assert "No data quality validation records were inferred." in empty.to_markdown()
    assert invalid.plan_id is None
    assert invalid.records == ()
    assert invalid.summary["task_count"] == 0
    assert no_signal.records == ()
    assert no_signal.no_validation_task_ids == ("task-cache",)
    assert "No-validation tasks: task-cache" in no_signal.to_markdown()


def test_execution_plan_execution_task_iterable_and_object_like_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Analytics freshness report",
        description="Create analytics dashboard with freshness and anomaly validation.",
        files_or_modules=["analytics/reports/freshness.sql"],
        acceptance_criteria=["Freshness watermark is visible."],
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Backfill invoice ledger",
            description="Backfill billing invoice ledger rows and reconcile source-to-target totals.",
            files_or_modules=["src/backfills/invoice_ledger.py"],
        )
    )
    plan_model = ExecutionPlan.model_validate(_plan([task_model.model_dump(mode="python")], plan_id="plan-model"))

    iterable_result = build_task_data_quality_validation_plan([object_task])
    task_result = summarize_task_data_quality_validation(task_model)
    plan_result = build_task_data_quality_validation_plan(plan_model)

    assert iterable_result.records[0].task_id == "task-object"
    assert "freshness" in iterable_result.records[0].validation_categories
    assert task_result.records[0].task_id == "task-model"
    assert task_result.records[0].severity == "critical"
    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].task_id == "task-model"


def _record(result, task_id):
    return next(record for record in result.records if record.task_id == task_id)


def _plan(tasks, plan_id="plan-data-quality"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-data-quality",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    depends_on=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    test_command=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [] if depends_on is None else depends_on,
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if test_command is not None:
        task["test_command"] = test_command
    return task
