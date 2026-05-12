import json

from blueprint.plan_background_job_readiness_matrix import (
    PlanBackgroundJobReadinessMatrix,
    analyze_plan_background_job_readiness_matrix,
    build_plan_background_job_readiness_matrix,
    plan_background_job_readiness_matrix_to_dict,
    plan_background_job_readiness_matrix_to_dicts,
    plan_background_job_readiness_matrix_to_markdown,
)


def test_ready_background_job_row():
    result = build_plan_background_job_readiness_matrix(_plan([
        _task("job", "Queue worker", "Background job worker queue with retry backoff, idempotency key, timeout deadline, poison dead-letter queue, progress tracking, cancellation, metrics logs and alerts."),
        _task("ui", "UI cleanup", "Internal polish."),
    ]))

    row = result.rows[0]
    assert isinstance(result, PlanBackgroundJobReadinessMatrix)
    assert result.background_job_task_ids == ("job",)
    assert result.no_background_job_task_ids == ("ui",)
    assert row.readiness == "ready"
    assert row.readiness_score == 1.0


def test_partial_and_blocked_job_readiness():
    result = build_plan_background_job_readiness_matrix(_plan([
        _task("partial", "Cron scheduler", "Cron scheduler uses retry backoff and idempotent processing."),
        _task("blocked", "Batch processing", "Batch processing has progress tracking only."),
    ]))

    assert [row.task_id for row in result.rows] == ["blocked", "partial"]
    assert result.rows[0].readiness == "blocked"
    assert result.rows[1].readiness == "partial"
    assert "Missing timeout handling." in result.rows[1].gaps


def test_helpers_are_stable():
    matrix = analyze_plan_background_job_readiness_matrix(_plan([
        _task("worker", "Task queue worker", "Worker queue retry backoff and idempotent operation.")
    ]))
    payload = plan_background_job_readiness_matrix_to_dict(matrix)

    assert json.loads(json.dumps(payload)) == payload
    assert plan_background_job_readiness_matrix_to_dicts(matrix) == payload["rows"]
    markdown = plan_background_job_readiness_matrix_to_markdown(matrix)
    assert "Plan Background Job Readiness Matrix" in markdown
    assert "worker" in markdown


def _plan(tasks):
    return {"id": "plan-jobs", "tasks": tasks, "milestones": [], "implementation_brief_id": "brief"}


def _task(task_id, title, description):
    return {
        "id": task_id,
        "title": title,
        "description": description,
        "depends_on": [],
        "files_or_modules": [],
        "acceptance_criteria": ["Done"],
        "status": "pending",
        "metadata": {},
    }
