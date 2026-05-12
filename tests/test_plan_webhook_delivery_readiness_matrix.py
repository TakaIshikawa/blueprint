import json

from blueprint.plan_webhook_delivery_readiness_matrix import (
    PlanWebhookDeliveryReadinessMatrix,
    analyze_plan_webhook_delivery_readiness_matrix,
    build_plan_webhook_delivery_readiness_matrix,
    plan_webhook_delivery_readiness_matrix_to_dict,
    plan_webhook_delivery_readiness_matrix_to_dicts,
    plan_webhook_delivery_readiness_matrix_to_markdown,
)


def test_ready_webhook_delivery_row():
    result = build_plan_webhook_delivery_readiness_matrix(_plan([
        _task("hook", "Webhook producer", "Outbound webhook event delivery with retry backoff, HMAC signature verification, delivery logs, replay tooling, timeout handling, DLQ dead-letter queue, and consumer notification email."),
        _task("ui", "UI polish", "Internal page cleanup."),
    ]))

    row = result.rows[0]
    assert isinstance(result, PlanWebhookDeliveryReadinessMatrix)
    assert result.webhook_task_ids == ("hook",)
    assert result.no_webhook_task_ids == ("ui",)
    assert row.readiness == "ready"
    assert row.readiness_score == 1.0


def test_partial_and_blocked_webhooks_are_classified():
    result = build_plan_webhook_delivery_readiness_matrix(_plan([
        _task("partial", "Webhook consumer", "Inbound webhook validates signature and uses retry policy."),
        _task("blocked", "Webhook producer", "Emit outbound webhook with delivery logs only."),
    ]))

    assert [row.task_id for row in result.rows] == ["blocked", "partial"]
    assert result.rows[0].readiness == "blocked"
    assert result.rows[1].readiness == "partial"
    assert "Missing replay tooling." in result.rows[1].gaps


def test_helpers_return_json_and_markdown():
    matrix = analyze_plan_webhook_delivery_readiness_matrix(_plan([
        _task("meta", "Partner callback", "Webhook delivery", metadata={"delivery": "retry backoff hmac signature replay"})
    ]))
    payload = plan_webhook_delivery_readiness_matrix_to_dict(matrix)

    assert json.loads(json.dumps(payload)) == payload
    assert plan_webhook_delivery_readiness_matrix_to_dicts(matrix) == payload["rows"]
    markdown = plan_webhook_delivery_readiness_matrix_to_markdown(matrix)
    assert "Plan Webhook Delivery Readiness Matrix" in markdown
    assert "meta" in markdown


def _plan(tasks):
    return {"id": "plan-webhook", "tasks": tasks, "milestones": [], "implementation_brief_id": "brief"}


def _task(task_id, title, description, metadata=None):
    return {
        "id": task_id,
        "title": title,
        "description": description,
        "depends_on": [],
        "files_or_modules": [],
        "acceptance_criteria": ["Done"],
        "status": "pending",
        "metadata": metadata or {},
    }
