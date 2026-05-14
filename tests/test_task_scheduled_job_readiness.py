import copy

from blueprint.domain.models import ExecutionPlan
from blueprint.task_scheduled_job_readiness import (
    build_task_scheduled_job_readiness_plan,
    task_scheduled_job_readiness_plan_to_dict,
    task_scheduled_job_readiness_plan_to_dicts,
    task_scheduled_job_readiness_plan_to_markdown,
)


def test_complete_scheduled_job_task_is_ready():
    result = build_task_scheduled_job_readiness_plan(
        _plan([
            _task(
                "job-ready",
                title="Add nightly scheduled job",
                description="Create a cron scheduled job.",
                acceptance_criteria=[
                    "Schedule definition uses cron expression 0 2 * * * in UTC timezone.",
                    "Job is idempotent and safe to rerun with dedupe keys.",
                    "Concurrency and overlap use a lease lock and skip if running.",
                    "Retry failure behavior uses backoff, timeout, and DLQ routing.",
                    "Observability includes metrics, logs, dashboard, and alerts.",
                    "Data platform owner and runbook are documented.",
                    "Validation coverage includes pytest scheduler tests and integration tests.",
                ],
            )
        ])
    )
    record = result.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert record.present_criteria == (
        "schedule_definition",
        "idempotency",
        "concurrency_overlap_handling",
        "retry_failure_behavior",
        "observability",
        "owner_runbook",
        "validation_coverage",
    )


def test_partial_scheduled_job_reports_ordered_gaps_and_actions():
    result = build_task_scheduled_job_readiness_plan([
        _task("job-partial", title="Recurring report job", description="Add recurring job with daily interval.")
    ])
    record = result.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("schedule_definition",)
    assert record.missing_criteria == (
        "idempotency",
        "concurrency_overlap_handling",
        "retry_failure_behavior",
        "observability",
        "owner_runbook",
        "validation_coverage",
    )
    assert record.recommended_follow_up_actions[0].startswith("Make the job idempotent")


def test_cron_scheduler_path_hints_nested_metadata_no_mutation_and_conversion():
    source = _plan(
        [
            _task(
                "job-paths",
                title="Calendar trigger worker",
                description="Run timer worker.",
                files_or_modules=["cron/reconcile.py", "scheduler/recurring_jobs.py"],
                metadata={"ops": {"owner": "On-call owner has runbook.", "safety": "Advisory lock prevents overlap."}},
            ),
            _task("noop", title="Docs", description="No scheduled job or cron changes are required."),
        ],
        plan_id="plan-jobs",
    )
    original = copy.deepcopy(source)
    result = build_task_scheduled_job_readiness_plan(ExecutionPlan.model_validate(source))
    payload = task_scheduled_job_readiness_plan_to_dict(result)
    record = result.records[0]
    assert source == original
    assert result.impacted_task_ids == ("job-paths",)
    assert result.ignored_task_ids == ("noop",)
    assert record.detected_signals == ("scheduled_job", "recurrence")
    assert record.present_criteria == ("schedule_definition", "concurrency_overlap_handling", "owner_runbook")
    assert any("files_or_modules: cron/reconcile.py" in item for item in record.evidence)
    assert task_scheduled_job_readiness_plan_to_dicts(result) == payload["records"]
    assert task_scheduled_job_readiness_plan_to_markdown(result).startswith("# Task Scheduled Job Readiness: plan-jobs")


def _plan(tasks, *, plan_id="plan-jobs"):
    return {"id": plan_id, "implementation_brief_id": "brief-jobs", "milestones": [], "tasks": tasks}


def _task(task_id, *, title=None, description=None, acceptance_criteria=None, files_or_modules=None, metadata=None):
    task = {"id": task_id, "title": title or task_id, "description": description or "", "acceptance_criteria": acceptance_criteria or []}
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    return task
