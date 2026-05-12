import json

from blueprint.plan_webhook_delivery_readiness_matrix import (
    PlanWebhookDeliveryReadinessMatrix,
    build_plan_webhook_delivery_readiness_matrix,
    generate_plan_webhook_delivery_readiness_matrix,
    plan_webhook_delivery_readiness_matrix_to_dict,
    plan_webhook_delivery_readiness_matrix_to_dicts,
    plan_webhook_delivery_readiness_matrix_to_markdown,
)


def test_ready_webhook_plan_scores_all_rows():
    result = build_plan_webhook_delivery_readiness_matrix(
        _plan(
            [
                _task(
                    "task-payment-webhook",
                    title="Add outbound payment webhook delivery",
                    description=(
                        "Webhook producer delivery uses a retry policy with exponential backoff, "
                        "HMAC signature verification for consumers, delivery logging with metrics, "
                        "manual replay tooling, timeout handling for slow consumer endpoints, "
                        "dead-letter queue handling, and consumer notification emails for repeated failures."
                    ),
                    metadata={"owner": "webhook-platform"},
                )
            ]
        )
    )

    assert isinstance(result, PlanWebhookDeliveryReadinessMatrix)
    assert result.webhook_task_ids == ("task-payment-webhook",)
    assert [row.area for row in result.rows] == [
        "retry_policy",
        "signature_verification",
        "delivery_logging",
        "replay_tooling",
        "timeout_handling",
        "dead_letter_handling",
        "consumer_notifications",
    ]
    assert all(row.readiness == "ready" for row in result.rows)
    assert all(row.score == 100 for row in result.rows)
    assert all(row.owner == "webhook-platform" for row in result.rows)
    assert result.summary["score"] == 100


def test_partial_and_blocked_webhook_gaps_are_classified():
    partial = build_plan_webhook_delivery_readiness_matrix(
        _plan([_task("task-order", title="Create order webhook producer", description="Add delivery retry policy and metrics.")])
    )
    blocked = build_plan_webhook_delivery_readiness_matrix(
        _plan([_task("task-blocked", title="Add webhook consumer endpoint", description="Blocked by missing storage dependency.")])
    )

    assert _row(partial, "signature_verification").readiness == "partial"
    assert _row(partial, "signature_verification").risk == "high"
    assert _row(partial, "retry_policy").readiness == "ready"
    assert _row(blocked, "signature_verification").readiness == "blocked"
    assert _row(blocked, "dead_letter_handling").readiness == "blocked"


def test_webhook_serialization_markdown_and_unrelated_plan():
    result = generate_plan_webhook_delivery_readiness_matrix(
        _plan(
            [
                _task(
                    "task-order | create",
                    title="Create order webhook | delivery",
                    description=(
                        "Retry policy, signature verification, delivery logging, replay tooling, timeout handling, "
                        "dead-letter queue, and consumer notification coverage."
                    ),
                )
            ]
        )
    )
    payload = plan_webhook_delivery_readiness_matrix_to_dict(result)
    markdown = plan_webhook_delivery_readiness_matrix_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert plan_webhook_delivery_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert markdown.startswith("# Plan Webhook Delivery Readiness Matrix: plan-webhook")
    assert "task-order \\| create" in markdown
    assert build_plan_webhook_delivery_readiness_matrix({"id": "empty", "tasks": []}).rows == ()
    assert build_plan_webhook_delivery_readiness_matrix({"id": "none", "tasks": [_task("copy", title="Update copy")]}).rows == ()


def _row(result, area):
    return next(row for row in result.rows if row.area == area)


def _plan(tasks):
    return {"id": "plan-webhook", "implementation_brief_id": "brief", "milestones": [], "tasks": tasks}


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
