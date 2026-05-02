import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_data_backfill_readiness import (
    TaskDataBackfillReadinessFinding,
    TaskDataBackfillReadinessPlan,
    analyze_task_data_backfill_readiness,
    build_task_data_backfill_readiness_plan,
    summarize_task_data_backfill_readiness,
    summarize_task_data_backfill_readiness_plan,
    task_data_backfill_readiness_plan_to_dict,
)


def test_high_risk_production_backfill_recommends_all_readiness_checks():
    result = build_task_data_backfill_readiness_plan(
        _plan(
            [
                _task(
                    "task-prod",
                    title="Backfill customer invoice totals",
                    description=(
                        "Run a production backfill across all customer data after the billing schema change. "
                        "Use batches, checkpoints for resumability, idempotent upserts, monitoring dashboards, "
                        "backup restore steps, validation counts, and throttling."
                    ),
                    files_or_modules=["scripts/backfill_invoice_totals.py"],
                    acceptance_criteria=[
                        "Batch size and checkpoint resume behavior are documented.",
                        "The job is idempotent and safe to rerun.",
                        "Monitoring and alerts show progress.",
                        "Restore plan and validation counts are reviewed.",
                        "Production throttling keeps load below agreed limits.",
                    ],
                ),
                _task("task-ui", title="Polish dashboard copy", description="Update settings labels."),
            ]
        )
    )

    assert isinstance(result, TaskDataBackfillReadinessPlan)
    assert result.impacted_task_ids == ("task-prod",)
    assert result.ignored_task_ids == ("task-ui",)
    finding = result.findings[0]
    assert finding.risk_level == "high"
    assert "backfill" in finding.work_types
    assert finding.readiness_checks == (
        "batching",
        "resumability",
        "idempotency",
        "monitoring",
        "rollback_or_restore",
        "data_validation",
        "production_throttling",
    )
    assert finding.missing_acceptance_criteria == ()
    assert any("production backfill" in item for item in finding.evidence)
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}


def test_low_risk_local_repair_with_acceptance_criteria_is_low_risk():
    result = build_task_data_backfill_readiness_plan(
        [
            _task(
                "task-local",
                title="Repair local fixture records",
                description="Run a one-time script to repair local sample data.",
                files_or_modules=["tools/one_time_repair_fixture_data.py"],
                acceptance_criteria=[
                    "Process records in batches and resume from a checkpoint.",
                    "Script is idempotent, logs progress metrics, and validates row counts.",
                ],
            )
        ]
    )

    finding = result.records[0]
    assert finding.risk_level == "low"
    assert {"repair_job", "one_time_script"} <= set(finding.work_types)
    assert finding.missing_acceptance_criteria == ("rollback_or_restore", "production_throttling")


def test_missing_acceptance_criteria_are_reported_even_when_description_mentions_checks():
    result = build_task_data_backfill_readiness_plan(
        _plan(
            [
                _task(
                    "task-reprocess",
                    title="Reprocess historical webhook events",
                    description=(
                        "Reprocess historical imports with batches, checkpoints, idempotent writes, "
                        "monitoring, restore plan, validation, and throttling."
                    ),
                    acceptance_criteria=["Historical events are reprocessed for the affected accounts."],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.risk_level == "high"
    assert {"reprocessing", "historical_import"} <= set(finding.work_types)
    assert finding.missing_acceptance_criteria == (
        "batching",
        "resumability",
        "idempotency",
        "monitoring",
        "rollback_or_restore",
        "data_validation",
        "production_throttling",
    )


def test_metadata_tags_and_execution_plan_inputs_detect_backfill_without_mutation():
    tag_result = build_task_data_backfill_readiness_plan(
        [_task("task-tag", title="Data maintenance", description="Prepare data job.", tags=["data-backfill"])]
    )
    assert tag_result.findings[0].work_types == ("backfill",)

    source = _plan(
        [
            _task(
                    "task-metadata",
                    title="Refresh account aggregates",
                    description="Refresh account data.",
                    files_or_modules=["jobs/data_backfill_account_aggregates.py"],
                    metadata={
                        "operation": {"type": "recalculate derived balances"},
                        "dataset": {"scope": "tenant customer data"},
                        "readiness": {"validation": "dry run row count validation"},
                    },
            )
        ]
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)

    mapping_result = build_task_data_backfill_readiness_plan(source)
    model_result = build_task_data_backfill_readiness_plan(model)
    alias_result = summarize_task_data_backfill_readiness(source)
    plan_alias_result = summarize_task_data_backfill_readiness_plan(model)
    findings = analyze_task_data_backfill_readiness(model)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert alias_result.to_dict() == mapping_result.to_dict()
    assert plan_alias_result.to_dict() == model_result.to_dict()
    assert findings == model_result.findings
    finding = model_result.findings[0]
    assert finding.risk_level == "high"
    assert {"backfill", "recalculation"} <= set(finding.work_types)
    assert any("files_or_modules" in item for item in finding.evidence)
    assert any("metadata.operation.type" in item for item in finding.evidence)
    assert any("metadata.dataset.scope" in item for item in finding.evidence)


def test_stable_sorting_and_json_serialization_shape():
    result = build_task_data_backfill_readiness_plan(
        _plan(
            [
                _task(
                    "task-low",
                    title="Repair sandbox records",
                    description="Repair job for local sandbox data.",
                    acceptance_criteria=[
                        "Batching, resumability, idempotency, monitoring, restore, validation, and throttling are covered."
                    ],
                ),
                _task(
                    "task-high",
                    title="Historical import for production orders",
                    description="Historical import for production customer orders.",
                    acceptance_criteria=["Orders are imported."],
                ),
                _task(
                    "task-medium",
                    title="Recalculate invoice aggregates",
                    description="Recalculate invoice aggregates.",
                    acceptance_criteria=[
                        "Batches, checkpoint resume, idempotent writes, monitoring, and validation are covered."
                    ],
                ),
            ]
        )
    )
    payload = task_data_backfill_readiness_plan_to_dict(result)

    assert [finding.task_id for finding in result.findings] == ["task-high", "task-medium", "task-low"]
    assert isinstance(result.findings[0], TaskDataBackfillReadinessFinding)
    assert result.records == result.findings
    assert result.to_dicts() == payload["findings"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "findings",
        "impacted_task_ids",
        "ignored_task_ids",
        "summary",
    ]
    assert list(payload["findings"][0]) == [
        "task_id",
        "title",
        "work_types",
        "readiness_checks",
        "missing_acceptance_criteria",
        "risk_level",
        "evidence",
    ]
    assert payload["summary"]["work_type_counts"]["historical_import"] == 1


def _plan(tasks):
    return {
        "id": "plan-data-backfill",
        "implementation_brief_id": "brief-data-backfill",
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
    tags=None,
    metadata=None,
):
    task = {
        "title": title or task_id or "Untitled task",
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if task_id is not None:
        task["id"] = task_id
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if tags is not None:
        task["tags"] = tags
    if metadata is not None:
        task["metadata"] = metadata
    return task
