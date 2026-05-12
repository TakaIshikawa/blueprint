import json

from blueprint.plan_webhook_delivery_readiness_matrix import (
    analyze_plan_webhook_delivery_readiness_matrix,
    build_plan_webhook_delivery_readiness_matrix,
    plan_webhook_delivery_readiness_matrix_to_dict,
    plan_webhook_delivery_readiness_matrix_to_dicts,
    plan_webhook_delivery_readiness_matrix_to_markdown,
    summarize_plan_webhook_delivery_readiness_matrix,
)


def test_webhook_tasks_are_detected_from_fields_and_metadata():
    result = build_plan_webhook_delivery_readiness_matrix(
        {
            "id": "plan-webhooks",
            "tasks": [
                {
                    "id": "deliver-invoices",
                    "title": "Invoice webhook producer",
                    "description": "Deliver invoice webhooks to subscribers.",
                    "acceptance_criteria": [
                        "Retry policy uses exponential backoff.",
                        "HMAC signature verification is documented.",
                        "Delivery logging stores status history.",
                        "Replay tooling can redeliver events.",
                        "Timeout handling caps slow consumer response windows.",
                        "Dead-letter queue captures poison deliveries.",
                        "Consumer notification emails cover failures.",
                    ],
                    "metadata": {"component": "webhook_delivery"},
                },
                {"id": "docs", "title": "Update copy", "description": "No delivery work."},
            ],
        }
    )

    assert result.webhook_task_ids == ("deliver-invoices",)
    assert result.no_webhook_task_ids == ("docs",)
    row = result.rows[0]
    assert row.readiness == "ready"
    assert row.readiness_score == 1.0
    assert row.gaps == ()
    assert any("webhook producer" in item for item in row.evidence)


def test_partial_and_blocked_readiness_are_classified():
    result = build_plan_webhook_delivery_readiness_matrix(
        {
            "tasks": [
                {
                    "id": "blocked",
                    "title": "Webhook consumer callback",
                    "description": "Consumer notification and delivery logging exist.",
                },
                {
                    "id": "partial",
                    "title": "Webhook producer",
                    "description": "Retry backoff, HMAC signature, logging, replay, and timeout handling are ready.",
                    "acceptance_criteria": ["Consumer notification is documented."],
                },
            ]
        }
    )

    assert [row.task_id for row in result.rows] == ["blocked", "partial"]
    assert result.rows[0].readiness == "blocked"
    assert "Missing retry policy." in result.rows[0].gaps
    assert result.rows[1].readiness == "partial"
    assert result.rows[1].gaps == ("Missing dead-letter handling.",)
    assert result.summary["readiness_counts"] == {"blocked": 1, "partial": 1, "ready": 0}


def test_markdown_and_dict_helpers_are_json_compatible():
    result = build_plan_webhook_delivery_readiness_matrix(
        {"id": "plan|webhook", "tasks": [{"id": "task|webhook", "title": "Webhook|producer", "description": "Webhook retry signature logging replay timeout dead-letter notification."}]}
    )
    payload = plan_webhook_delivery_readiness_matrix_to_dict(result)

    assert analyze_plan_webhook_delivery_readiness_matrix(result).to_dict() == payload
    assert summarize_plan_webhook_delivery_readiness_matrix(result) == result.summary
    assert plan_webhook_delivery_readiness_matrix_to_dicts(result) == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    markdown = plan_webhook_delivery_readiness_matrix_to_markdown(result)
    assert "task\\|webhook" in markdown
    assert "Webhook\\|producer" in markdown
