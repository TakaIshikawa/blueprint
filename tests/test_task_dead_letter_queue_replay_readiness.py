import json

from blueprint.task_dead_letter_queue_replay_readiness import (
    analyze_task_dead_letter_queue_replay_readiness,
    build_task_dead_letter_queue_replay_readiness_plan,
    task_dead_letter_queue_replay_readiness_plan_to_dict,
    task_dead_letter_queue_replay_readiness_plan_to_dicts,
    task_dead_letter_queue_replay_readiness_plan_to_markdown,
)


def test_complete_dlq_replay_task_is_ready():
    result = build_task_dead_letter_queue_replay_readiness_plan(
        _plan(
            [
                _task(
                    "dlq-ready",
                    "Replay checkout DLQ",
                    (
                        "DLQ replay redrives failed messages. Replay scope filters by tenant, message id, "
                        "event type, and time window. Idempotency uses dedupe keys for duplicate protection. "
                        "Poison messages are quarantined after terminal failure. Throttling and rate limits "
                        "pace batches. Audit trail records approvals and replay logs. Dry run, abort, pause, "
                        "and rollback stop controls are available. Monitoring dashboards alert on DLQ depth."
                    ),
                    ["src/queues/dlq_replay/redrive.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert record.present_criteria == (
        "replay_scope_filtering",
        "idempotency",
        "poison_message_handling",
        "rate_limiting",
        "audit_trail",
        "rollback_stop_control",
        "monitoring",
    )
    assert record.missing_criteria == ()


def test_partial_dlq_replay_plan_reports_actionable_missing_safeguards():
    result = analyze_task_dead_letter_queue_replay_readiness(
        _plan(
            [
                _task(
                    "dlq-partial",
                    "Redrive dead letter queue",
                    "Reprocess messages from the dead-letter queue with tenant filters and idempotency.",
                    ["workers/dead_letter_queue_reprocess.py"],
                ),
                _task("copy", "Docs", "No DLQ replay changes are required.", []),
            ]
        )
    )

    record = result.records[0]
    assert result.impacted_task_ids == ("dlq-partial",)
    assert result.ignored_task_ids == ("copy",)
    assert {"dead_letter_queue", "queue_reprocessing"} <= set(record.detected_signals)
    assert record.present_criteria == ("replay_scope_filtering", "idempotency")
    assert "poison_message_handling" in record.missing_criteria
    assert "rate_limiting" in record.missing_criteria
    assert any(action.startswith("Describe how poison messages") for action in record.recommended_follow_up_actions)


def test_detects_failed_message_replay_and_queue_reprocessing_language():
    result = build_task_dead_letter_queue_replay_readiness_plan(
        _plan(
            [
                _task("failed", "Failed message replay", "Replay failed messages from queue backlog.", []),
                _task("rerun", "Queue reprocessing", "Rerun messages with monitoring.", ["queues/message_replay.py"]),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.records}
    assert "failed_message_replay" in by_id["failed"].detected_signals
    assert "queue_reprocessing" in by_id["rerun"].detected_signals
    assert "message_replay" in by_id["rerun"].detected_signals


def test_serialization_and_markdown_are_stable():
    result = build_task_dead_letter_queue_replay_readiness_plan(_plan([_task("alias", "DLQ", "DLQ replay with audit trail.", [])]))
    payload = task_dead_letter_queue_replay_readiness_plan_to_dict(result)

    assert task_dead_letter_queue_replay_readiness_plan_to_dicts(result) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "# Task Dead Letter Queue Replay Readiness: plan-dlq-replay" in task_dead_letter_queue_replay_readiness_plan_to_markdown(result)


def _plan(tasks):
    return {"id": "plan-dlq-replay", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
