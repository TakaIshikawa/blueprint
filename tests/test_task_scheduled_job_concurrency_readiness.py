import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_scheduled_job_concurrency_readiness import (
    TaskScheduledJobConcurrencyReadinessPlan,
    TaskScheduledJobConcurrencyReadinessRecord,
    build_task_scheduled_job_concurrency_readiness,
    build_task_scheduled_job_concurrency_readiness_plan,
    derive_task_scheduled_job_concurrency_readiness_plan,
    extract_task_scheduled_job_concurrency_readiness_records,
    generate_task_scheduled_job_concurrency_readiness_plan,
    summarize_task_scheduled_job_concurrency_readiness,
    task_scheduled_job_concurrency_readiness_to_dict,
    task_scheduled_job_concurrency_readiness_to_dicts,
    task_scheduled_job_concurrency_readiness_to_markdown,
)


def test_detects_scheduled_job_signals_and_high_risk_mutating_cron():
    result = build_task_scheduled_job_concurrency_readiness_plan(
        _plan(
            [
                _task(
                    "task-cron",
                    title="Add cron job for invoice sync",
                    description="Cron job runs every 15 minutes to sync and update invoice records.",
                    acceptance_criteria=["Invoice rows are updated after each run."],
                ),
                _task(
                    "task-worker",
                    title="Scheduled worker for warehouse import",
                    description="Scheduled worker runs a recurring sync for partner warehouse inventory.",
                    acceptance_criteria=[
                        "Distributed lock prevents overlapping runs.",
                        "Idempotency key uses partner sku and warehouse id.",
                        "Missed-run recovery uses last successful run watermark.",
                        "Backfill controls limit backfill windows and chunk size.",
                        "Timeout is capped at 20 minutes with stale lock cleanup.",
                        "Retry policy uses exponential backoff.",
                        "Observability emits metrics, logs, alerts, and run history.",
                        "Manual replay command is documented for operators.",
                    ],
                ),
            ]
        )
    )

    assert isinstance(result, TaskScheduledJobConcurrencyReadinessPlan)
    assert all(isinstance(record, TaskScheduledJobConcurrencyReadinessRecord) for record in result.records)
    by_id = {record.task_id: record for record in result.readiness_records}
    assert by_id["task-cron"].matched_scheduling_signals == ("cron_job", "interval_polling")
    assert by_id["task-cron"].risk_level == "high"
    assert "distributed_locking" in by_id["task-cron"].missing_safeguards
    assert "idempotency" in by_id["task-cron"].missing_safeguards
    assert any("prevent overlapping executions" in check for check in by_id["task-cron"].recommended_checks)
    assert by_id["task-worker"].matched_scheduling_signals == ("scheduled_worker", "recurring_sync")
    assert by_id["task-worker"].present_safeguards == (
        "distributed_locking",
        "idempotency",
        "missed_run_recovery",
        "backfill_controls",
        "timeout_limits",
        "retry_policy",
        "observability",
        "manual_replay",
    )
    assert by_id["task-worker"].missing_safeguards == ()
    assert by_id["task-worker"].risk_level == "low"
    assert result.summary["scheduled_task_count"] == 2
    assert result.summary["high_risk_count"] == 1
    assert result.summary["signal_counts"]["cron_job"] == 1
    assert result.summary["present_safeguard_counts"]["distributed_locking"] == 1


def test_plan_context_and_path_based_detection_with_missed_run_guidance():
    result = build_task_scheduled_job_concurrency_readiness_plan(
        _plan(
            [
                _task(
                    "task-path",
                    title="Implement retention cleanup",
                    description="Purge expired uploads in a maintenance task.",
                    files_or_modules=["src/workers/cron/nightly_retention_cleanup.py"],
                    acceptance_criteria=["Cleanup deletes expired rows safely."],
                )
            ],
            risks=["Nightly jobs can miss runs during deploy freezes and need recovery guidance."],
            acceptance_criteria=["Job dashboard alerts on failures."],
        )
    )

    record = result.readiness_records[0]
    assert record.task_id == "task-path"
    assert record.matched_scheduling_signals == (
        "cron_job",
        "scheduled_worker",
        "nightly_job",
        "maintenance_task",
    )
    assert record.risk_level == "high"
    assert "missed_run_recovery" in record.missing_safeguards
    assert "backfill_controls" in record.required_safeguards
    assert "observability" in record.present_safeguards
    assert any("missed schedules" in check for check in record.recommended_checks)
    assert "files_or_modules: src/workers/cron/nightly_retention_cleanup.py" in record.evidence
    assert any(evidence.startswith("risks[0]: Nightly jobs can miss runs") for evidence in record.evidence)


def test_model_inputs_aliases_markdown_and_serialization_are_stable():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Batch job | poll partner API",
                    description="Batch job performs interval polling and is read-only.",
                    acceptance_criteria=[
                        "Timeout limit is 5 minutes.",
                        "Retry policy uses retries with backoff.",
                        "Observability emits duration metrics.",
                    ],
                )
            ],
            plan_id="plan-scheduled-model",
        )
    )
    task = ExecutionTask.model_validate(
        _task(
            "task-single",
            title="Nightly compaction job",
            description="Nightly maintenance job compacts records.",
        )
    )

    result = build_task_scheduled_job_concurrency_readiness(plan)
    generated = generate_task_scheduled_job_concurrency_readiness_plan(plan)
    derived = derive_task_scheduled_job_concurrency_readiness_plan(result)
    extracted = extract_task_scheduled_job_concurrency_readiness_records(plan)
    summarized = summarize_task_scheduled_job_concurrency_readiness(task)
    payload = task_scheduled_job_concurrency_readiness_to_dict(result)
    markdown = task_scheduled_job_concurrency_readiness_to_markdown(result)

    assert result.plan_id == "plan-scheduled-model"
    assert generated.to_dict() == result.to_dict()
    assert derived is result
    assert extracted == result.readiness_records
    assert summarized["scheduled_task_count"] == 1
    assert result.records == result.readiness_records
    assert result.recommendations == result.readiness_records
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["readiness_records"]
    assert task_scheduled_job_concurrency_readiness_to_dicts(result) == payload["readiness_records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "readiness_records", "records", "scheduled_task_ids", "summary"]
    assert list(payload["readiness_records"][0]) == [
        "task_id",
        "title",
        "matched_scheduling_signals",
        "required_safeguards",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "recommended_checks",
        "evidence",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Scheduled Job Concurrency Readiness: plan-scheduled-model")
    assert "Batch job \\| poll partner API" in markdown


def test_task_level_input_detects_all_scheduling_signal_families():
    result = build_task_scheduled_job_concurrency_readiness_plan(
        _task(
            "task-all",
            title="Daily batch polling and recurring sync maintenance task",
            description=(
                "Scheduled task runs every 2 hours as a cron batch job, recurring sync, "
                "interval polling worker, nightly cleanup, and maintenance task."
            ),
            acceptance_criteria=["Manual replay is available for backfill support."],
        )
    )

    assert result.plan_id is None
    assert result.scheduled_task_ids == ("task-all",)
    assert result.readiness_records[0].matched_scheduling_signals == (
        "cron_job",
        "scheduled_worker",
        "recurring_sync",
        "batch_job",
        "nightly_job",
        "interval_polling",
        "maintenance_task",
    )


def test_no_match_behavior_invalid_input_and_no_mutation_are_stable():
    plan = _plan(
        [
            _task(
                "task-copy",
                title="Update onboarding copy",
                description="Polish dashboard labels and helper text.",
                files_or_modules=["src/ui/onboarding.py"],
                acceptance_criteria=["Copy review is complete."],
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_scheduled_job_concurrency_readiness_plan(plan)
    invalid = build_task_scheduled_job_concurrency_readiness_plan({"id": "bad", "tasks": "not a list"})

    assert plan == original
    assert result.readiness_records == ()
    assert result.records == ()
    assert result.scheduled_task_ids == ()
    assert result.summary == {
        "task_count": 1,
        "scheduled_task_count": 0,
        "record_count": 0,
        "high_risk_count": 0,
        "medium_risk_count": 0,
        "low_risk_count": 0,
        "missing_safeguard_count": 0,
        "signal_counts": {
            "cron_job": 0,
            "scheduled_worker": 0,
            "recurring_sync": 0,
            "batch_job": 0,
            "nightly_job": 0,
            "interval_polling": 0,
            "maintenance_task": 0,
        },
        "present_safeguard_counts": {
            "distributed_locking": 0,
            "idempotency": 0,
            "missed_run_recovery": 0,
            "backfill_controls": 0,
            "timeout_limits": 0,
            "retry_policy": 0,
            "observability": 0,
            "manual_replay": 0,
        },
        "scheduled_task_ids": [],
    }
    assert "No scheduled job concurrency readiness records were inferred." in result.to_markdown()
    assert invalid.readiness_records == ()
    assert invalid.summary["task_count"] == 0


def _plan(tasks, *, plan_id="plan-scheduled", risks=None, acceptance_criteria=None):
    plan = {
        "id": plan_id,
        "implementation_brief_id": "brief-scheduled",
        "milestones": [],
        "tasks": tasks,
    }
    if risks is not None:
        plan["risks"] = risks
    if acceptance_criteria is not None:
        plan["acceptance_criteria"] = acceptance_criteria
    return plan


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
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
    return task
