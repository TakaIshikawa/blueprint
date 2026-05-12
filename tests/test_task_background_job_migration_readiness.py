from blueprint.task_background_job_migration_readiness import (
    TaskBackgroundJobMigrationReadiness,
    analyze_task_background_job_migration_readiness,
    summarize_task_background_job_migration_readiness,
    task_background_job_migration_readiness_to_dict,
)


def test_ready_background_job_migration_plan_scores_full_readiness():
    result = analyze_task_background_job_migration_readiness(
        {
            "title": "Migrate invoice background jobs to new worker topics",
            "description": (
                "Queue mapping routes legacy invoice_queue to billing.invoice.v2 topic. "
                "Jobs are idempotent using a unique job id and safe to replay. "
                "Retry policy uses exponential backoff with jitter and DLQ handling. "
                "Drain strategy pauses producers, drains in-flight jobs, and clears backlog. "
                "Scheduling changes move the cron cadence to the new scheduler. "
                "Rollback switches back to the legacy worker through a kill switch. "
                "Monitoring covers queue depth, consumer lag, job latency, and alerts. "
                "Owner is billing-platform on-call."
            ),
            "acceptance_criteria": [
                "Dashboard tracks success rate and failure rate during migration.",
            ],
        }
    )

    assert isinstance(result, TaskBackgroundJobMigrationReadiness)
    assert result.queue_mapping_defined is True
    assert result.idempotency_addressed is True
    assert result.retry_backoff_defined is True
    assert result.drain_strategy_defined is True
    assert result.scheduling_changes_defined is True
    assert result.rollback_path_defined is True
    assert result.monitoring_defined is True
    assert result.ownership_defined is True
    assert result.missing_requirements == ()
    assert result.actionable_gaps == ()
    assert result.readiness_score == 1.0
    assert result.is_ready is True


def test_partial_background_job_migration_plan_returns_actionable_gaps():
    result = summarize_task_background_job_migration_readiness(
        {
            "title": "Move email jobs to the notifications worker",
            "description": (
                "Queue mapping sends legacy email_jobs to notifications.email. "
                "Retry budget uses exponential backoff. "
                "Monitoring includes queue depth and DLQ alerts."
            ),
            "metadata": {"owner": "notifications team"},
        }
    )

    assert result.queue_mapping_defined is True
    assert result.retry_backoff_defined is True
    assert result.monitoring_defined is True
    assert result.ownership_defined is True
    assert result.idempotency_addressed is False
    assert result.drain_strategy_defined is False
    assert result.scheduling_changes_defined is False
    assert result.rollback_path_defined is False
    assert result.missing_requirements == (
        "idempotency",
        "drain_strategy",
        "scheduling",
        "rollback",
    )
    assert result.actionable_gaps == (
        "Document replay safety, deduplication keys, or idempotent job handling.",
        "Define how producers pause and existing in-flight or backlog jobs drain.",
        "Call out cron, cadence, scheduler, or run-window changes during migration.",
        "Provide a rollback or fallback path to the previous job pipeline.",
    )
    assert result.readiness_score == 0.5
    assert result.is_ready is False


def test_absent_background_job_migration_plan_reports_every_gap():
    result = analyze_task_background_job_migration_readiness(
        {
            "title": "Update copy on account settings",
            "description": "Tighten labels and adjust spacing.",
        }
    )

    assert result.to_dict() == {
        "queue_mapping_defined": False,
        "idempotency_addressed": False,
        "retry_backoff_defined": False,
        "drain_strategy_defined": False,
        "scheduling_changes_defined": False,
        "rollback_path_defined": False,
        "monitoring_defined": False,
        "ownership_defined": False,
        "missing_requirements": [
            "queue_mapping",
            "idempotency",
            "retry_backoff",
            "drain_strategy",
            "scheduling",
            "rollback",
            "monitoring",
            "ownership",
        ],
        "actionable_gaps": [
            "Map every legacy queue/topic and worker to its migration target.",
            "Document replay safety, deduplication keys, or idempotent job handling.",
            "Specify retry limits, backoff behavior, and failure queue handling.",
            "Define how producers pause and existing in-flight or backlog jobs drain.",
            "Call out cron, cadence, scheduler, or run-window changes during migration.",
            "Provide a rollback or fallback path to the previous job pipeline.",
            "Add metrics, alerts, dashboards, or logs for migration health.",
            "Name the responsible owner, team, DRI, or on-call group.",
        ],
        "readiness_score": 0.0,
        "is_ready": False,
    }


def test_serialization_helper_preserves_stable_shape():
    result = analyze_task_background_job_migration_readiness(
        {"description": "Worker routing map exists. Owner is platform."}
    )

    payload = task_background_job_migration_readiness_to_dict(result)

    assert list(payload) == [
        "queue_mapping_defined",
        "idempotency_addressed",
        "retry_backoff_defined",
        "drain_strategy_defined",
        "scheduling_changes_defined",
        "rollback_path_defined",
        "monitoring_defined",
        "ownership_defined",
        "missing_requirements",
        "actionable_gaps",
        "readiness_score",
        "is_ready",
    ]
    assert payload["queue_mapping_defined"] is True
    assert payload["ownership_defined"] is True
