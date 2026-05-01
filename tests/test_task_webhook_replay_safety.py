import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_webhook_replay_safety import (
    build_task_webhook_replay_safety_plan,
    recommend_task_webhook_replay_safety,
    task_webhook_replay_safety_plan_to_dict,
)


def test_detects_webhook_text_and_reports_missing_safeguards_with_evidence():
    result = build_task_webhook_replay_safety_plan(
        _plan(
            [
                _task(
                    "task-webhook",
                    title="Handle Stripe webhook events",
                    description="Persist incoming webhook event payloads from Stripe checkout.",
                    files_or_modules=["src/integrations/stripe/webhooks.py"],
                    acceptance_criteria=[
                        "Signature validation rejects invalid HMAC payloads.",
                        "Audit log records accepted delivery ids.",
                    ],
                )
            ]
        )
    )

    assert result.webhook_task_ids == ("task-webhook",)
    recommendation = result.recommendations[0]
    assert recommendation.ingestion_surfaces == (
        "Webhook receiver: Handle Stripe webhook events",
        "Event ingestion: Handle Stripe webhook events",
    )
    assert recommendation.missing_safeguards == (
        "idempotency_key_handling",
        "duplicate_event_tests",
        "dead_letter_handling",
        "replay_window_limits",
    )
    assert recommendation.risk_level == "high"
    assert recommendation.external_service_signals == ("Stripe", "incoming webhook")
    assert "title: Handle Stripe webhook events" in recommendation.evidence
    assert "files_or_modules: src/integrations/stripe/webhooks.py" in recommendation.evidence


def test_detects_event_ingestion_from_metadata_and_tags():
    result = recommend_task_webhook_replay_safety(
        _plan(
            [
                _task(
                    "task-consumer",
                    title="Store fulfillment messages",
                    description="Persist received fulfillment messages.",
                    metadata={
                        "surface": "order stream",
                        "ingestion": {"kind": "queue consumer", "source": "partner"},
                    },
                    tags=["event payload", "shopify"],
                    acceptance_criteria=[
                        "Idempotency key handling uses event_id.",
                        "Duplicate event tests cover redelivery.",
                        "Dead-letter queue captures poison messages.",
                        "Replay window rejects stale events older than 5 minutes.",
                        "Audit evidence links receipts to processed records.",
                    ],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]
    assert recommendation.ingestion_surfaces == (
        "Queue consumer: order stream",
        "Event ingestion: order stream",
    )
    assert recommendation.missing_safeguards == ("signature_timestamp_validation",)
    assert recommendation.external_service_signals == ("Shopify", "partner")
    assert recommendation.risk_level == "high"
    assert any(item.startswith("metadata.ingestion.kind: queue consumer") for item in recommendation.evidence)
    assert "tags[0]: event payload" in recommendation.evidence


def test_non_webhook_tasks_are_suppressed():
    result = build_task_webhook_replay_safety_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update dashboard empty state",
                    description="Clarify onboarding copy in account settings.",
                    files_or_modules=["src/ui/settings.py"],
                )
            ]
        )
    )

    assert result.recommendations == ()
    assert result.webhook_task_ids == ()
    assert result.summary == {
        "task_count": 1,
        "webhook_task_count": 0,
        "high_risk_count": 0,
        "medium_risk_count": 0,
        "low_risk_count": 0,
        "missing_safeguard_count": 0,
    }


def test_deterministic_sorting_no_mutation_and_json_serialization():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Internal queue consumer replay checks",
                description="Consume internal billing queue messages.",
                acceptance_criteria=[
                    "Idempotency key handling stores event_id.",
                    "Duplicate event tests cover redelivery.",
                    "Signature timestamp validation covers HMAC clock skew.",
                    "Dead-letter handling moves poison messages to DLQ.",
                    "Replay window limits reject stale events.",
                    "Audit evidence logs receipt and result.",
                ],
            ),
            _task(
                "task-a",
                title="GitHub callback receiver",
                description="Handle external callback deliveries from GitHub.",
                acceptance_criteria=["Duplicate event tests cover callback redelivery."],
            ),
            _task(
                "task-m",
                title="Internal event payload consumer",
                description="Process internal event payloads from the queue.",
                acceptance_criteria=["Idempotency key handling stores the event id."],
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_webhook_replay_safety_plan(plan)
    payload = task_webhook_replay_safety_plan_to_dict(result)

    assert plan == original
    assert result.webhook_task_ids == ("task-a", "task-m", "task-z")
    assert [item.risk_level for item in result.recommendations] == ["high", "medium", "low"]
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["recommendations"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "recommendations", "webhook_task_ids", "summary"]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "ingestion_surfaces",
        "missing_safeguards",
        "risk_level",
        "external_service_signals",
        "evidence",
    ]
    assert payload["summary"] == {
        "task_count": 3,
        "webhook_task_count": 3,
        "high_risk_count": 1,
        "medium_risk_count": 1,
        "low_risk_count": 1,
        "missing_safeguard_count": 10,
    }


def test_execution_plan_model_input_is_supported():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Receive partner webhook",
                    description="Build incoming webhook receiver for partner status events.",
                    acceptance_criteria=[
                        "Idempotency key handling stores event ids.",
                        "Signature timestamp validation rejects stale signed payloads.",
                    ],
                )
            ],
            plan_id="plan-model",
        )
    )

    result = build_task_webhook_replay_safety_plan(plan)

    assert result.plan_id == "plan-model"
    assert result.webhook_task_ids == ("task-model",)
    assert result.recommendations[0].risk_level == "medium"
    assert result.recommendations[0].missing_safeguards == (
        "duplicate_event_tests",
        "dead_letter_handling",
        "replay_window_limits",
        "audit_evidence",
    )


def _plan(tasks, plan_id="plan-webhook-replay"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-webhook-replay",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    tags=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
