import json

from blueprint.plan_background_job_readiness_matrix import (
    analyze_plan_background_job_readiness_matrix,
    build_plan_background_job_readiness_matrix,
    plan_background_job_readiness_matrix_to_dict,
    plan_background_job_readiness_matrix_to_dicts,
    plan_background_job_readiness_matrix_to_markdown,
    summarize_plan_background_job_readiness_matrix,
)


def test_finds_background_job_tasks_from_task_text_and_metadata():
    result = build_plan_background_job_readiness_matrix(
        {
            "id": "plan-jobs",
            "tasks": [
                {
                    "id": "invoice-worker",
                    "title": "Invoice queue worker",
                    "description": "Process queued invoice background jobs.",
                    "acceptance_criteria": [
                        "Retry policy uses exponential backoff.",
                        "Idempotency deduplicates duplicate job operation keys.",
                        "Timeout deadline and visibility timeout are configured.",
                        "Poison messages move to a dead-letter queue.",
                        "Progress tracking checkpoints status for resume.",
                        "Cancellation can abort a running job.",
                        "Operational monitoring dashboard tracks queue depth, lag, logs, and alerts.",
                    ],
                    "metadata": {"runtime": "worker"},
                },
                {"id": "copy", "title": "Update labels", "description": "Copy-only change."},
            ],
        }
    )

    assert result.plan_id == "plan-jobs"
    assert result.background_job_task_ids == ("invoice-worker",)
    assert result.no_background_job_task_ids == ("copy",)
    row = result.rows[0]
    assert row.readiness == "ready"
    assert row.readiness_score == 1.0
    assert row.gaps == ()
    assert any("queue worker" in item for item in row.evidence)


def test_classifies_ready_partial_and_blocked_job_scenarios():
    result = build_plan_background_job_readiness_matrix(
        {
            "tasks": [
                {"id": "blocked", "title": "Cron scheduler", "description": "Cron batch runner has timeout and monitoring only."},
                {
                    "id": "partial",
                    "title": "Batch-processing worker",
                    "description": "Queue worker retry backoff, idempotency, timeout, poison DLQ, progress checkpoints, and monitoring are ready.",
                },
            ]
        }
    )

    assert [row.task_id for row in result.rows] == ["blocked", "partial"]
    assert result.rows[0].readiness == "blocked"
    assert "Missing retry and backoff policy." in result.rows[0].gaps
    assert result.rows[1].readiness == "partial"
    assert result.rows[1].gaps == ("Missing cancellation controls.",)
    assert result.summary["readiness_counts"] == {"blocked": 1, "partial": 1, "ready": 0}


def test_markdown_and_json_compatible_helpers_are_deterministic():
    result = build_plan_background_job_readiness_matrix(
        {"id": "plan|jobs", "tasks": [{"id": "task|worker", "title": "Queue|worker", "description": "Worker retry backoff idempotency timeout poison dead-letter progress cancellation monitoring."}]}
    )
    payload = plan_background_job_readiness_matrix_to_dict(result)

    assert analyze_plan_background_job_readiness_matrix(result).to_dict() == payload
    assert summarize_plan_background_job_readiness_matrix(result) == result.summary
    assert plan_background_job_readiness_matrix_to_dicts(result) == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    markdown = plan_background_job_readiness_matrix_to_markdown(result)
    assert "task\\|worker" in markdown
    assert "Queue\\|worker" in markdown
