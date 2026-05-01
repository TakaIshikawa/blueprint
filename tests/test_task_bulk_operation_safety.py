import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_bulk_operation_safety import (
    TaskBulkOperationSafetyPlan,
    TaskBulkOperationSafetyRecord,
    build_task_bulk_operation_safety_plan,
    summarize_task_bulk_operation_safety,
    task_bulk_operation_safety_plan_to_dict,
    task_bulk_operation_safety_plan_to_markdown,
)


def test_detects_bulk_edit_from_single_task_and_reports_missing_safeguards():
    result = build_task_bulk_operation_safety_plan(
        _task(
            "task-bulk-edit",
            title="Bulk update account permissions",
            description="Write changes to all tenants and update many production records.",
            files_or_modules=["src/jobs/bulk_update_permissions.py"],
            acceptance_criteria=[
                "Dry-run mode previews affected accounts.",
                "Operator approval gates full execution.",
            ],
            risks=["Destructive permission changes can affect all users."],
        )
    )

    assert isinstance(result, TaskBulkOperationSafetyPlan)
    assert result.plan_id is None
    assert result.bulk_operation_task_ids == ("task-bulk-edit",)
    record = result.records[0]
    assert isinstance(record, TaskBulkOperationSafetyRecord)
    assert record.operation_type == "bulk_edit"
    assert record.risk_level == "high"
    assert record.safeguards == (
        "dry_run",
        "batching",
        "sampling",
        "rate_limiting",
        "rollback",
        "operator_approval",
        "progress_monitoring",
    )
    assert record.missing_safeguards == (
        "batching",
        "sampling",
        "rate_limiting",
        "rollback",
        "progress_monitoring",
    )
    assert record.recommended_acceptance_criteria == (
        "Execution runs in bounded, resumable batches with explicit batch-size controls.",
        "A sampled or canary run validates representative records before full rollout.",
        "Rate limits or throttling protect downstream services and write paths during execution.",
        "Rollback or compensating steps are documented and tested for partial completion.",
        "Progress, failures, and completion metrics are monitored with audit evidence.",
    )
    assert "title: Bulk update account permissions" in record.evidence
    assert "files_or_modules: src/jobs/bulk_update_permissions.py" in record.evidence


def test_iterable_input_detects_import_notification_backfill_and_sorts_deterministically():
    result = summarize_task_bulk_operation_safety(
        [
            _task(
                "task-z",
                title="Backfill customer health scores",
                description="Backfill existing records with a dry-run, batches, sample, throttling, rollback, and progress metrics.",
                files_or_modules=["src/backfills/customer_health.py"],
                acceptance_criteria=[
                    "Dry-run previews changes.",
                    "Batches are resumable.",
                    "Sample validates representative records.",
                    "Throttling protects write paths.",
                    "Rollback is documented.",
                    "Progress metrics are monitored.",
                ],
            ),
            _task(
                "task-a",
                title="Send bulk email campaign",
                description="Mass notification to all users about billing changes.",
                acceptance_criteria=["Sample cohort is reviewed before full send."],
            ),
            _task(
                "task-m",
                title="CSV import for legacy accounts",
                description="Batch import existing customer rows.",
                metadata={
                    "safeguards": {
                        "dry_run": "Dry run validates mapping.",
                        "batching": "Chunked import can resume.",
                    }
                },
            ),
        ]
    )

    assert result.bulk_operation_task_ids == ("task-a", "task-m", "task-z")
    assert [record.operation_type for record in result.records] == [
        "mass_notification",
        "batch_import",
        "backfill",
    ]
    assert [record.risk_level for record in result.records] == ["high", "high", "medium"]
    assert result.summary["operation_counts"] == {
        "bulk_edit": 0,
        "batch_import": 1,
        "mass_notification": 1,
        "backfill": 1,
        "migration": 0,
        "wide_fanout_write": 0,
    }


def test_plan_dict_input_detects_metadata_risks_and_file_paths_without_mutation():
    plan = _plan(
        [
            _task(
                "task-fanout",
                title="Refresh customer denormalized rows",
                description="Recompute account summary cache.",
                files_or_modules=["src/workers/fanout/customer_summary.py"],
                metadata={"operation": "wide fan-out write", "scope": "all accounts"},
                risks=["Large dataset write may overload downstream services."],
                acceptance_criteria=[
                    "Dry run previews the fan-out impact.",
                    "Batched chunks use rate limiting.",
                    "Rollback path restores the old values.",
                    "Operator approval is required.",
                    "Progress monitoring reports completion and failures.",
                ],
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_bulk_operation_safety_plan(plan)

    assert plan == original
    assert result.plan_id == "plan-bulk"
    assert result.bulk_operation_task_ids == ("task-fanout",)
    record = result.records[0]
    assert record.operation_type == "wide_fanout_write"
    assert record.missing_safeguards == ("sampling",)
    assert record.risk_level == "high"
    assert any(item.startswith("metadata.operation:") for item in record.evidence)
    assert "risks[0]: Large dataset write may overload downstream services." in record.evidence


def test_execution_plan_model_and_task_model_inputs_are_supported():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Run data migration over existing orders",
                    description="Migrate existing order records with dry-run, batches, sample, rate limit, rollback, and monitoring.",
                    files_or_modules=["db/migrations/20260502_orders.py"],
                    acceptance_criteria=[
                        "Dry-run previews the migration.",
                        "Batched chunks can resume.",
                        "Sample validates representative records.",
                        "Rate limiting protects write paths.",
                        "Rollback path is tested.",
                        "Progress monitoring reports completion.",
                    ],
                )
            ],
            plan_id="plan-model",
        )
    )
    task = ExecutionTask.model_validate(
        _task(
            "task-object",
            title="Backfill invoices",
            description="Backfill production invoices.",
            acceptance_criteria=["Dry run covers invoice count."],
        )
    )

    plan_result = build_task_bulk_operation_safety_plan(plan)
    task_result = build_task_bulk_operation_safety_plan(task)

    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].risk_level == "low"
    assert plan_result.records[0].missing_safeguards == ()
    assert task_result.bulk_operation_task_ids == ("task-object",)
    assert task_result.records[0].operation_type == "backfill"


def test_no_signal_returns_empty_deterministic_plan_and_markdown():
    result = build_task_bulk_operation_safety_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update settings empty state",
                    description="Clarify onboarding copy.",
                    files_or_modules=["src/ui/settings_copy.py"],
                    metadata={"surface": "settings"},
                )
            ]
        )
    )

    assert result.records == ()
    assert result.bulk_operation_task_ids == ()
    assert result.summary == {
        "task_count": 1,
        "bulk_operation_task_count": 0,
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "operation_counts": {
            "bulk_edit": 0,
            "batch_import": 0,
            "mass_notification": 0,
            "backfill": 0,
            "migration": 0,
            "wide_fanout_write": 0,
        },
    }
    assert result.to_markdown() == (
        "# Task Bulk Operation Safety Plan: plan-bulk\n"
        "\n"
        "## Summary\n"
        "\n"
        "- Task count: 1\n"
        "- Bulk operation task count: 0\n"
        "- Missing safeguard count: 0\n"
        "- Risk counts: high 0, medium 0, low 0\n"
        "- Operation counts: bulk_edit 0, batch_import 0, mass_notification 0, backfill 0, migration 0, wide_fanout_write 0\n"
        "\n"
        "No bulk-operation tasks were detected."
    )


def test_serialization_and_markdown_paths_are_stable():
    result = build_task_bulk_operation_safety_plan(
        _plan(
            [
                _task(
                    "task-pipe",
                    title="Bulk import customer | tenant rows",
                    description="Batch import customer rows from CSV.",
                    acceptance_criteria=["Dry run previews row count."],
                )
            ]
        )
    )

    payload = task_bulk_operation_safety_plan_to_dict(result)
    markdown = task_bulk_operation_safety_plan_to_markdown(result)

    assert payload == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "records", "bulk_operation_task_ids", "summary"]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "risk_level",
        "operation_type",
        "safeguards",
        "missing_safeguards",
        "recommended_acceptance_criteria",
        "evidence",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Bulk Operation Safety Plan: plan-bulk")
    assert "Bulk import customer \\| tenant rows" in markdown
    assert "| `task-pipe` |" in markdown


def _plan(tasks, plan_id="plan-bulk"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-bulk",
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
    tags=None,
    risks=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    if risks is not None:
        task["risks"] = risks
    return task
