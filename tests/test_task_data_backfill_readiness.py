import copy
import json

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord
from blueprint.domain.models import ExecutionPlan
from blueprint.task_data_backfill_readiness import (
    TaskDataBackfillReadinessPlan,
    analyze_task_data_backfill_readiness,
    build_task_data_backfill_readiness_plan,
    recommend_task_data_backfill_readiness,
    summarize_task_data_backfill_readiness,
    summarize_task_data_backfill_readiness_plan,
    task_data_backfill_readiness_plan_to_dict,
    task_data_backfill_readiness_plan_to_dicts,
    task_data_backfill_readiness_plan_to_markdown,
)


def test_mapping_input_reports_present_and_missing_backfill_criteria():
    result = build_task_data_backfill_readiness_plan(
        _plan(
            [
                _task(
                    "task-ready",
                    title="Backfill customer invoice totals",
                    description="Run a data backfill for tenant invoices in monthly batches.",
                    acceptance_criteria=[
                        "Scope boundaries limit the job to allowlisted tenants and a date range.",
                        "Batching uses cursors, throttling, and checkpoint windows.",
                        "Idempotent upserts make the job safe to rerun.",
                        "Rollback restores from a snapshot and can abort through a kill switch.",
                        "Monitoring dashboards and alerts track progress and failures.",
                        "Reconciliation validates row count, checksums, and sampled records.",
                    ],
                    files_or_modules=["scripts/backfill_invoice_totals.py"],
                ),
                _task("task-docs", title="Update docs", description="Clarify release notes."),
            ]
        )
    )

    assert isinstance(result, TaskDataBackfillReadinessPlan)
    assert isinstance(result, SimpleReadinessPlan)
    assert result.impacted_task_ids == ("task-ready",)
    assert result.ignored_task_ids == ("task-docs",)
    record = result.records[0]
    assert isinstance(record, SimpleReadinessRecord)
    assert record.detected_signals == ("data_backfill",)
    assert record.present_criteria == (
        "scope_boundaries",
        "batching",
        "idempotency",
        "rollback",
        "monitoring",
        "reconciliation",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"


def test_execution_plan_input_detects_metadata_dependencies_and_path_hints_without_mutation():
    source = _plan(
        [
            _task(
                "task-replay",
                title="Refresh account aggregates",
                description="Refresh derived balances.",
                depends_on=["event replay worker"],
                files_or_modules=["jobs/recompute_account_aggregates.py"],
                metadata={
                    "operation": "recompute aggregate balances from replayed events",
                    "runbook": {
                        "scope": "customer cohort and watermark boundaries",
                        "monitoring": "progress metrics dashboard",
                    },
                },
            )
        ]
    )
    original = copy.deepcopy(source)

    result = build_task_data_backfill_readiness_plan(ExecutionPlan.model_validate(source))

    assert source == original
    record = result.records[0]
    assert record.detected_signals == ("replay", "recompute")
    assert record.present_criteria == ("scope_boundaries", "monitoring")
    assert record.missing_criteria == ("batching", "idempotency", "rollback", "reconciliation")
    assert any("depends_on" in item for item in record.evidence)
    assert any("metadata.operation" in item for item in record.evidence)
    assert any("files_or_modules" in item for item in record.evidence)


def test_ignored_tasks_markdown_dict_serialization_and_aliases_are_stable():
    source = _plan(
        [
            _task(
                "task-partial",
                title="Replay webhook events",
                description="Replay webhook events with batch windows and idempotent writes.",
            ),
            _task(
                "task-missing",
                title="Recompute reporting snapshots",
                description="Recompute derived reporting data.",
            ),
            _task(
                "task-copy",
                title="Copy edit",
                description="No backfill or replay changes are required for this copy update.",
            ),
        ],
        plan_id="plan-data-backfill-sort",
    )

    result = summarize_task_data_backfill_readiness(source)
    payload = task_data_backfill_readiness_plan_to_dict(result)
    markdown = task_data_backfill_readiness_plan_to_markdown(result)

    assert [record.task_id for record in result.records] == ["task-missing", "task-partial"]
    assert result.ignored_task_ids == ("task-copy",)
    assert analyze_task_data_backfill_readiness(source).to_dict() == result.to_dict()
    assert summarize_task_data_backfill_readiness_plan(result) is result
    assert recommend_task_data_backfill_readiness(source) == result.records
    assert result.to_dicts() == payload["records"]
    assert task_data_backfill_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-data-backfill-sort"
    assert markdown.startswith("# Task Data Backfill Readiness: plan-data-backfill-sort")
    assert "| Task | Title | Readiness |" in markdown


def test_invalid_inputs_return_empty_simple_plan():
    assert build_task_data_backfill_readiness_plan(42).records == ()
    assert build_task_data_backfill_readiness_plan({"id": "bad", "tasks": "not a list"}).records == ()
    assert build_task_data_backfill_readiness_plan({"tasks": []}).summary["task_count"] == 0


def _plan(tasks, *, plan_id="plan-data-backfill"):
    return {"id": plan_id, "implementation_brief_id": "brief-data-backfill", "milestones": [], "tasks": tasks}


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    files_or_modules=None,
    depends_on=None,
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
    if depends_on is not None:
        task["depends_on"] = depends_on
    if metadata is not None:
        task["metadata"] = metadata
    return task
