import json

from blueprint.plan_background_job_readiness_matrix import (
    PlanBackgroundJobReadinessMatrix,
    build_plan_background_job_readiness_matrix,
    generate_plan_background_job_readiness_matrix,
    plan_background_job_readiness_matrix_to_dict,
    plan_background_job_readiness_matrix_to_dicts,
    plan_background_job_readiness_matrix_to_markdown,
)


def test_ready_background_job_plan_scores_all_rows():
    result = build_plan_background_job_readiness_matrix(
        _plan(
            [
                _task(
                    "task-worker",
                    title="Add queue worker for batch processing",
                    description=(
                        "Background job worker has retry policy with exponential backoff, idempotency job keys, "
                        "runtime timeout handling, poison message dead-letter queue handling, progress tracking "
                        "with heartbeat job status, cancellation support, and operational monitoring metrics."
                    ),
                    metadata={"owner": "platform-jobs"},
                )
            ]
        )
    )

    assert isinstance(result, PlanBackgroundJobReadinessMatrix)
    assert result.background_job_task_ids == ("task-worker",)
    assert [row.area for row in result.rows] == [
        "retry_backoff",
        "idempotency",
        "timeout_handling",
        "poison_message_handling",
        "progress_tracking",
        "cancellation",
        "operational_monitoring",
    ]
    assert all(row.readiness == "ready" for row in result.rows)
    assert all(row.score == 100 for row in result.rows)
    assert all(row.owner == "platform-jobs" for row in result.rows)


def test_partial_and_blocked_background_job_gaps_are_classified():
    partial = build_plan_background_job_readiness_matrix(
        _plan([_task("task-cron", title="Add scheduler cron job", description="Add retry metrics for the queue worker.")])
    )
    blocked = build_plan_background_job_readiness_matrix(
        _plan([_task("task-blocked", title="Add batch job worker", description="Blocked by missing queue infrastructure.")])
    )

    assert _row(partial, "idempotency").readiness == "partial"
    assert _row(partial, "idempotency").risk == "high"
    assert _row(partial, "retry_backoff").readiness == "ready"
    assert _row(blocked, "idempotency").readiness == "blocked"
    assert _row(blocked, "operational_monitoring").risk == "high"


def test_background_job_serialization_markdown_and_unrelated_plan():
    result = generate_plan_background_job_readiness_matrix(
        _plan(
            [
                _task(
                    "task-job | nightly",
                    title="Nightly worker | readiness",
                    description=(
                        "Retry backoff, idempotency, timeout, poison message dead-letter handling, "
                        "progress tracking, cancellation, and monitoring."
                    ),
                )
            ]
        )
    )
    payload = plan_background_job_readiness_matrix_to_dict(result)
    markdown = plan_background_job_readiness_matrix_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert plan_background_job_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert markdown.startswith("# Plan Background Job Readiness Matrix: plan-background-job")
    assert "task-job \\| nightly" in markdown
    assert build_plan_background_job_readiness_matrix({"id": "empty", "tasks": []}).rows == ()
    assert build_plan_background_job_readiness_matrix({"id": "none", "tasks": [_task("copy", title="Update copy")]}).rows == ()


def _row(result, area):
    return next(row for row in result.rows if row.area == area)


def _plan(tasks):
    return {"id": "plan-background-job", "implementation_brief_id": "brief", "milestones": [], "tasks": tasks}


def _task(task_id, *, title=None, description=None, acceptance_criteria=None, metadata=None):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [],
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    return task
