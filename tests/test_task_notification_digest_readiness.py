from blueprint.task_notification_digest_readiness import (
    analyze_task_notification_digest_readiness,
    task_notification_digest_readiness_plan_to_dict,
    task_notification_digest_readiness_plan_to_markdown,
)


def test_ready_notification_digest_task_covers_expected_criteria():
    plan = analyze_task_notification_digest_readiness(
        [{
            "id": "task-ready",
            "title": "Build weekly notification digest delivery",
            "description": (
                "Recipient selection uses eligible subscribers by segment. Grouping rules roll up events by thread "
                "with dedupe. Cadence is weekly in each user timezone and send window. Email channel uses a template "
                "and subject line. Preferences, opt-out, unsubscribe, and quiet hours are honored. Delivery failure "
                "handling retries bounces with backoff and DLQ. Monitoring metrics dashboard alert on delivery rate "
                "and failure rate. Integration tests cover rendering."
            ),
            "files_or_modules": ["src/notifications/weekly_digest_batcher.py"],
        }]
    )

    record = plan.records[0]
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert "digest" in record.detected_signals


def test_partial_notification_digest_task_returns_followups():
    plan = analyze_task_notification_digest_readiness(
        [{"id": "task-partial", "title": "Daily digest email", "description": "Daily digest cadence uses local timezone and an email template."}]
    )

    record = plan.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("cadence_timezone", "channel_template")
    assert "Define eligible recipients" in record.recommended_follow_up_actions[0]


def test_sparse_notification_digest_task_needs_planning():
    plan = analyze_task_notification_digest_readiness("Batch notification summaries into a digest")

    assert plan.records[0].readiness == "needs_planning"
    assert len(plan.records[0].missing_criteria) == 8


def test_generic_notification_preferences_without_digest_are_ignored():
    plan = analyze_task_notification_digest_readiness(
        [{"id": "task-pref", "title": "Notification preferences", "description": "Add opt-out toggles for push and email notifications."}]
    )

    assert plan.records == ()
    assert plan.ignored_task_ids == ("task-pref",)


def test_notification_digest_serialization_and_markdown_are_deterministic():
    plan = analyze_task_notification_digest_readiness(
        [{"id": "task-digest", "title": "Digest rollout", "description": "Digest cadence is weekly."}]
    )

    payload = task_notification_digest_readiness_plan_to_dict(plan)
    assert payload["summary"]["missing_criterion_count"] == 7
    assert task_notification_digest_readiness_plan_to_markdown(plan) == plan.to_markdown()
