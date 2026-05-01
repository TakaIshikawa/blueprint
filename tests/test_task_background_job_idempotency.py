import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_background_job_idempotency import (
    TaskBackgroundJobIdempotencyPlan,
    TaskBackgroundJobIdempotencyRecord,
    analyze_task_background_job_idempotency,
    build_task_background_job_idempotency_plan,
    summarize_task_background_job_idempotency,
    summarize_task_background_job_idempotency_plans,
    task_background_job_idempotency_plan_to_dict,
    task_background_job_idempotency_plan_to_markdown,
)


def test_queue_worker_webhook_retry_batch_etl_and_scheduler_tasks_are_classified():
    result = build_task_background_job_idempotency_plan(
        _plan(
            [
                _task(
                    "task-queue-strong",
                    title="Add queue worker retry idempotency",
                    description=(
                        "Process SQS queue messages in a worker with an idempotency key, "
                        "dedupe key, retry-safe side effects, DLQ handling, replay tests, "
                        "and at-least-once delivery semantics."
                    ),
                    files_or_modules=["src/jobs/email_worker.py", "tests/jobs/test_retry_replay.py"],
                    acceptance_criteria=[
                        "Duplicate delivery does not send the email twice.",
                    ],
                    test_command="poetry run pytest tests/jobs/test_retry_replay.py",
                ),
                _task(
                    "task-cron-partial",
                    title="Add cron scheduler for nightly ETL batch",
                    description="Run a scheduled ETL batch with checkpoint cursor and advisory lock ownership.",
                    files_or_modules=["src/scheduler/nightly_etl.py"],
                ),
                _task(
                    "task-webhook-missing",
                    title="Handle Stripe webhook retries",
                    description="Consume webhook event delivery retries from Stripe.",
                    files_or_modules=["src/webhooks/stripe.py"],
                ),
                _task(
                    "task-copy",
                    title="Update settings copy",
                    description="Clarify labels on account settings.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskBackgroundJobIdempotencyPlan)
    assert result.plan_id == "plan-jobs"
    assert result.background_task_ids == (
        "task-webhook-missing",
        "task-cron-partial",
        "task-queue-strong",
    )
    assert result.not_applicable_task_ids == ("task-copy",)
    assert result.summary["readiness_counts"] == {
        "missing": 1,
        "partial": 1,
        "strong": 1,
        "not_applicable": 1,
    }
    assert result.summary["signal_counts"]["queue"] == 1
    assert result.summary["signal_counts"]["worker"] == 1
    assert result.summary["signal_counts"]["cron"] == 1
    assert result.summary["signal_counts"]["webhook"] == 1
    assert result.summary["signal_counts"]["retry"] == 2
    assert result.summary["signal_counts"]["batch"] == 1
    assert result.summary["signal_counts"]["etl"] == 1
    assert result.summary["signal_counts"]["scheduler"] == 1

    by_id = {record.task_id: record for record in result.records}
    strong = by_id["task-queue-strong"]
    assert isinstance(strong, TaskBackgroundJobIdempotencyRecord)
    assert strong.readiness == "strong"
    assert "idempotency" in strong.safeguard_evidence
    assert "dedupe" in strong.safeguard_evidence
    assert "replay" in strong.safeguard_evidence
    assert "dead_letter" in strong.safeguard_evidence
    assert any("validation_commands" in item for item in strong.evidence)
    assert any("detected validation commands" in check for check in strong.recommended_checks)

    partial = by_id["task-cron-partial"]
    assert partial.readiness == "partial"
    assert partial.detected_signals == ("cron", "batch", "etl", "scheduler")
    assert partial.safeguard_evidence == ("checkpoint", "lock_ownership")
    assert any("Document exactly-once" in check for check in partial.recommended_checks)

    missing = by_id["task-webhook-missing"]
    assert missing.readiness == "missing"
    assert missing.safeguard_evidence == ()
    assert any("Define an idempotency or dedupe key" in check for check in missing.recommended_checks)
    assert any("provider event id" in question for question in missing.open_questions)


def test_metadata_tags_dependencies_paths_and_validation_commands_contribute_evidence():
    result = analyze_task_background_job_idempotency(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Implement background job processor",
                    description="Run async job execution.",
                    depends_on=["task-webhook-receiver"],
                    files_or_modules={
                        "main": "src/workers/payment_retry_worker.py",
                        "duplicate": "src/workers/payment_retry_worker.py",
                    },
                    acceptance_criteria={"qa": "Replay duplicate events without double charging."},
                    metadata={
                        "tags": ["queue", "retry"],
                        "idempotency": {"key": "provider event id"},
                        "delivery_semantics": "at-least-once documented",
                        "validation_commands": {"test": ["poetry run pytest tests/jobs/test_idempotency.py"]},
                    },
                )
            ]
        )
    )

    record = result.records[0]

    assert record.task_id == "task-metadata"
    assert record.readiness == "strong"
    assert record.detected_signals == ("queue", "worker", "webhook", "retry", "background_job")
    assert "idempotency" in record.safeguard_evidence
    assert "replay" in record.safeguard_evidence
    assert "delivery_semantics" in record.safeguard_evidence
    assert record.evidence.count("files_or_modules: src/workers/payment_retry_worker.py") == 1
    assert "metadata.delivery_semantics: at-least-once documented" in record.evidence
    assert "validation_commands: poetry run pytest tests/jobs/test_idempotency.py" in record.evidence
    assert result.summary["safeguard_counts"]["idempotency"] == 1
    assert result.summary["safeguard_counts"]["delivery_semantics"] == 1


def test_empty_invalid_no_signal_serialization_markdown_and_aliases_are_stable():
    task_dict = _task(
        "task-batch | pipe",
        title="Backfill batch | replay",
        description="Run backfill batch with checkpoint watermark and replay tests.",
        files_or_modules=["src/batches/backfill_accounts.py"],
    )
    original = copy.deepcopy(task_dict)

    result = summarize_task_background_job_idempotency(_plan([task_dict]))
    alias = summarize_task_background_job_idempotency_plans(_plan([task_dict]))
    payload = task_background_job_idempotency_plan_to_dict(result)
    markdown = task_background_job_idempotency_plan_to_markdown(result)
    empty = build_task_background_job_idempotency_plan({"id": "empty-plan", "tasks": []})
    invalid = build_task_background_job_idempotency_plan(13)
    no_signal = build_task_background_job_idempotency_plan(
        _plan([_task("task-ui", title="Add profile UI", description="Render profile settings.")])
    )

    assert task_dict == original
    assert alias.to_dict() == result.to_dict()
    assert result.to_dicts() == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "records",
        "background_task_ids",
        "not_applicable_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "readiness",
        "detected_signals",
        "safeguard_evidence",
        "recommended_checks",
        "open_questions",
        "evidence",
    ]
    assert markdown.startswith("# Task Background Job Idempotency Plan: plan-jobs")
    assert "Summary: 1 background tasks" in markdown
    assert "Backfill batch \\| replay" in markdown
    assert empty.plan_id == "empty-plan"
    assert empty.records == ()
    assert empty.summary["task_count"] == 0
    assert invalid.plan_id is None
    assert invalid.records == ()
    assert no_signal.records == ()
    assert no_signal.not_applicable_task_ids == ("task-ui",)
    assert "No background job idempotency records were inferred." in no_signal.to_markdown()
    assert "Not-applicable tasks: task-ui" in no_signal.to_markdown()


def test_execution_plan_execution_task_iterable_and_object_like_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="Create scheduled webhook replay worker",
        description="Scheduler replays webhook failures with a dedupe key and DLQ recovery.",
        files_or_modules=["src/workers/webhook_replay.py"],
        acceptance_criteria=["Duplicate events are deduped."],
        metadata={"validation_commands": {"test": ["poetry run pytest tests/jobs/test_webhook_replay.py"]}},
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Queue retry processor",
            description="Build queue retry processing for account events.",
            files_or_modules=["src/queues/account_retry.py"],
        )
    )
    plan_model = ExecutionPlan.model_validate(_plan([task_model.model_dump(mode="python")], plan_id="plan-model"))

    iterable_result = build_task_background_job_idempotency_plan([object_task])
    task_result = build_task_background_job_idempotency_plan(task_model)
    plan_result = build_task_background_job_idempotency_plan(plan_model)

    assert iterable_result.records[0].task_id == "task-object"
    assert iterable_result.records[0].readiness == "strong"
    assert "webhook" in iterable_result.records[0].detected_signals
    assert "dedupe" in iterable_result.records[0].safeguard_evidence
    assert task_result.records[0].task_id == "task-model"
    assert task_result.records[0].readiness == "missing"
    assert plan_result.plan_id == "plan-model"
    assert plan_result.records[0].task_id == "task-model"


def _plan(tasks, plan_id="plan-jobs"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-jobs",
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
