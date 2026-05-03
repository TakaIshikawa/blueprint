import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_data_backfill_safety_matrix import (
    PlanDataBackfillSafetyMatrix,
    PlanDataBackfillSafetyRow,
    analyze_plan_data_backfill_safety_matrix,
    build_plan_data_backfill_safety_matrix,
    derive_plan_data_backfill_safety_matrix,
    extract_plan_data_backfill_safety_matrix,
    generate_plan_data_backfill_safety_matrix,
    plan_data_backfill_safety_matrix_to_dict,
    plan_data_backfill_safety_matrix_to_dicts,
    plan_data_backfill_safety_matrix_to_markdown,
    summarize_plan_data_backfill_safety_matrix,
)


def test_backfill_like_tasks_group_by_target_dataset_with_required_safety_signals():
    result = build_plan_data_backfill_safety_matrix(
        _plan(
            [
                _task(
                    "task-orders-backfill",
                    title="Backfill orders_events",
                    description="Backfill orders_events for orders missing lifecycle timestamps.",
                    acceptance_criteria=[
                        "Selection criteria: rows where shipped_at is null and status is shipped.",
                        "Dry-run previews row counts before writes.",
                        "Idempotency uses upsert by order_id and skips existing values.",
                    ],
                ),
                _task(
                    "task-orders-safety",
                    title="orders_events replay verification",
                    description="Replay orders_events with owner Data Platform.",
                    acceptance_criteria=[
                        "Batch size is 500 rows with a rate limit of 50 rps.",
                        "Verification query checks row count parity and checksum diffs.",
                        "Rollback compensation restores values from the snapshot.",
                    ],
                ),
                _task(
                    "task-profiles-resync",
                    title="Resync profile_index",
                    description="Resync profile_index for tenants updated since 2026-01-01.",
                    acceptance_criteria=[
                        "Dry run, idempotency checkpoint, batch limit, verification query, rollback plan, and owner are documented.",
                    ],
                ),
                _task("task-copy", title="Update labels", description="Refresh plain UI labels."),
            ]
        )
    )

    assert isinstance(result, PlanDataBackfillSafetyMatrix)
    assert all(isinstance(row, PlanDataBackfillSafetyRow) for row in result.rows)
    assert result.plan_id == "plan-backfill"
    assert result.backfill_task_ids == ("task-orders-backfill", "task-orders-safety", "task-profiles-resync")
    assert result.no_backfill_task_ids == ("task-copy",)
    assert [row.target_dataset for row in result.rows] == ["orders_events", "profile_index"]

    orders = _row(result, "orders_events")
    assert orders.task_ids == ("task-orders-backfill", "task-orders-safety")
    assert orders.readiness == "ready"
    assert orders.severity == "low"
    assert orders.missing_fields == ()
    assert orders.recommendation == "Backfill safety controls are documented; execute through the approved runbook."
    assert any("orders_events" in item for item in orders.evidence)


def test_detects_migration_repair_and_bulk_correction_with_actionable_missing_fields():
    result = build_plan_data_backfill_safety_matrix(
        _plan(
            [
                _task(
                    "task-ledger-repair",
                    title="Migration repair ledger_entries",
                    description="Run migration repair ledger_entries with a dry-run and batches.",
                    acceptance_criteria=["Selection criteria covers entries from the failed migration window."],
                ),
                _task(
                    "task-invoice-correction",
                    title="Bulk correction invoice_totals",
                    description=(
                        "Owner Billing runs bulk correction invoice_totals with selection criteria, dry-run, "
                        "idempotency, batch limit, verification query, and rollback compensation."
                    ),
                ),
            ]
        )
    )

    blocked = _row(result, "ledger_entries")
    assert blocked.readiness == "blocked"
    assert blocked.severity == "high"
    assert blocked.missing_fields == (
        "idempotency_strategy",
        "verification_query",
        "rollback_or_compensation",
        "owner",
    )
    assert blocked.recommendation == (
        "Document idempotency strategy, verification query, rollback or compensation, owner "
        "before running the backfill."
    )

    ready = _row(result, "invoice_totals")
    assert ready.readiness == "ready"
    assert ready.missing_fields == ()
    assert result.summary["readiness_counts"] == {"blocked": 1, "partial": 0, "ready": 1}
    assert result.summary["severity_counts"] == {"high": 1, "medium": 0, "low": 1}


def test_structured_metadata_satisfies_fields_when_prose_is_sparse():
    result = build_plan_data_backfill_safety_matrix(
        _plan(
            [
                _task(
                    "task-metadata-only",
                    title="Replay sparse task",
                    description="Replay historical import records.",
                    metadata={
                        "target_dataset": "imports.raw_events",
                        "selection_criteria": "tenant_id in approved replay list",
                        "dry_run_plan": "preview count query before writes",
                        "idempotency_strategy": "checkpoint by source_event_id and skip existing rows",
                        "batch_or_rate_limit": "batch size 100 and 20 rps throttle",
                        "verification_query": "row count parity query",
                        "rollback_or_compensation": "restore from snapshot",
                        "owner": "Data Imports DRI",
                    },
                )
            ]
        )
    )

    row = _row(result, "imports.raw_events")
    assert row.readiness == "ready"
    assert row.missing_fields == ()
    assert row.owner == "present"
    assert any("metadata.target_dataset" in item for item in row.evidence)


def test_no_backfill_plans_return_empty_matrix_with_not_applicable_ids():
    result = build_plan_data_backfill_safety_matrix(
        _plan(
            [
                _task("task-api", title="Build API endpoint", description="Implement normal CRUD behavior."),
                _task("task-docs", title="Document endpoint", description="Update docs."),
            ]
        )
    )

    assert result.rows == ()
    assert result.backfill_task_ids == ()
    assert result.no_backfill_task_ids == ("task-api", "task-docs")
    assert result.summary == {
        "task_count": 2,
        "row_count": 0,
        "backfill_task_count": 0,
        "no_backfill_task_count": 2,
        "readiness_counts": {"blocked": 0, "partial": 0, "ready": 0},
        "severity_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_field_counts": {},
        "dataset_counts": {},
    }
    assert "No data backfill safety rows were inferred." in result.to_markdown()
    assert "No backfill signals: task-api, task-docs" in result.to_markdown()


def test_serialization_aliases_markdown_model_object_input_and_file_path_hints():
    plan = _plan(
        [
            _task(
                "task-backfill | plan",
                title="Backfill account | ledger",
                description="Backfill account_ledger with selection criteria and dry-run.",
                files_or_modules=["runbooks/backfills/account_ledger.md"],
                acceptance_criteria=[
                    "Idempotency, batch limit, verification query, rollback compensation, and owner are ready.",
                ],
            )
        ]
    )
    original = copy.deepcopy(plan)
    model_plan = ExecutionPlan.model_validate(plan)

    result = build_plan_data_backfill_safety_matrix(model_plan)
    payload = plan_data_backfill_safety_matrix_to_dict(result)
    markdown = plan_data_backfill_safety_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_data_backfill_safety_matrix(plan).to_dict() == result.to_dict()
    assert analyze_plan_data_backfill_safety_matrix(plan).to_dict() == result.to_dict()
    assert derive_plan_data_backfill_safety_matrix(plan).to_dict() == result.to_dict()
    assert extract_plan_data_backfill_safety_matrix(plan).to_dict() == result.to_dict()
    assert summarize_plan_data_backfill_safety_matrix(result) == result.summary
    assert plan_data_backfill_safety_matrix_to_dicts(result) == payload["rows"]
    assert plan_data_backfill_safety_matrix_to_dicts(result.records) == payload["records"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "backfill_task_ids",
        "no_backfill_task_ids",
        "summary",
    ]
    assert "account \\| ledger" in markdown
    assert "task-backfill \\| plan" in markdown

    object_result = build_plan_data_backfill_safety_matrix(
        SimpleNamespace(
            id="object-backfill",
            title="Backfill object_store",
            description=(
                "Owner runs object_store backfill with selection criteria, dry-run, idempotency, "
                "batch limit, verification query, and rollback compensation."
            ),
            acceptance_criteria=["Ready"],
        )
    )
    invalid = build_plan_data_backfill_safety_matrix(23)

    assert object_result.rows[0].task_ids == ("object-backfill",)
    assert invalid.rows == ()
    assert invalid.summary["task_count"] == 0


def _row(result, target_dataset):
    return next(row for row in result.rows if row.target_dataset == target_dataset)


def _plan(tasks, *, plan_id="plan-backfill"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-backfill",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    depends_on=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "depends_on": [] if depends_on is None else depends_on,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
