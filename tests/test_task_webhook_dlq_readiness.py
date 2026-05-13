import json

from blueprint.task_webhook_dlq_readiness import (
    analyze_task_webhook_dlq_readiness,
    build_task_webhook_dlq_readiness_plan,
    task_webhook_dlq_readiness_plan_to_dict,
    task_webhook_dlq_readiness_plan_to_markdown,
)


def test_complete_webhook_dlq_task_is_ready():
    result = build_task_webhook_dlq_readiness_plan(
        _plan(
            [
                _task(
                    "dlq-ready",
                    "Webhook DLQ drain",
                    (
                        "Webhook dead-letter queue handles failed callbacks after retry exhaustion. "
                        "DLQ routing sends terminal failures to the failed delivery queue. "
                        "Retry exhaustion policy defines max attempts. Poison payload isolation quarantines malformed payloads. "
                        "Replay tooling and redelivery tool support operators. Idempotent drain uses dedupe. "
                        "Retention policy sets TTL. Alerting tracks DLQ depth and failure rate. Runbook documents the drain procedure."
                    ),
                    ["src/webhooks/dlq/drain.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "ready"
    assert {"webhook_dlq", "failed_callback", "retry_exhaustion", "poison_payload", "replay", "drain", "retention"} <= set(
        record.detected_signals
    )
    assert record.missing_safeguards == ()


def test_partial_dlq_task_recommends_replay_alerting_retention_idempotency_and_runbook():
    result = analyze_task_webhook_dlq_readiness(
        _plan(
            [
                _task(
                    "dlq-partial",
                    "Route failed webhook callbacks to DLQ",
                    "Failed callback retry limit routes terminal delivery failures to a dead-letter queue.",
                    ["src/webhooks/failed_delivery_queue.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "partial"
    assert record.impact == "high"
    assert record.present_safeguards == ("dlq_routing", "retry_exhaustion_policy")
    assert {"replay_tooling", "idempotent_drain", "retention_policy", "alerting", "runbook"} <= set(record.missing_safeguards)
    assert any("operator replay" in action.lower() for action in record.recommendations)
    assert any("idempotent" in action.lower() for action in record.recommendations)
    assert result.summary["webhook_dlq_task_count"] == 1


def test_weak_dlq_task_and_serialization_are_stable():
    result = build_task_webhook_dlq_readiness_plan(
        _plan(
            [
                _task("dlq-weak", "Drain webhook DLQ", "Drain poison events from webhook dead-letter queue.", []),
                _task("copy", "Docs", "Update unrelated docs.", []),
            ]
        )
    )
    payload = task_webhook_dlq_readiness_plan_to_dict(result)

    assert result.records[0].readiness == "missing"
    assert result.ignored_task_ids == ("copy",)
    assert json.loads(json.dumps(payload)) == payload
    assert "| `dlq-weak` | Drain webhook DLQ |" in task_webhook_dlq_readiness_plan_to_markdown(result)


def _plan(tasks):
    return {"id": "plan-webhook-dlq", "tasks": tasks}


def _task(task_id, title, description, files):
    return {"id": task_id, "title": title, "description": description, "files_or_modules": files}
