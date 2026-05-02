import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_webhook_ordering_impact import (
    build_task_webhook_ordering_impact_plan,
    recommend_task_webhook_ordering_impact,
    task_webhook_ordering_impact_plan_to_dict,
)


def test_provider_webhook_reports_missing_ordering_safeguards_with_evidence():
    result = build_task_webhook_ordering_impact_plan(
        _plan(
            [
                _task(
                    "task-stripe",
                    title="Handle Stripe subscription webhooks",
                    description="Persist incoming webhook updates for subscription status.",
                    files_or_modules=["src/integrations/stripe/webhooks.py"],
                    acceptance_criteria=[
                        "Signature validation and idempotency are covered by replay tests."
                    ],
                )
            ]
        )
    )

    assert result.ordering_task_ids == ("task-stripe",)
    impact = result.impacts[0]
    assert impact.ingestion_surfaces == (
        "Webhook receiver: Handle Stripe subscription webhooks",
    )
    assert impact.missing_safeguards == (
        "sequence_number_checks",
        "monotonic_timestamp_checks",
        "version_conflict_checks",
        "stale_update_rejection",
        "ordering_buffering",
        "reordered_event_tests",
    )
    assert impact.risk_level == "high"
    assert impact.external_service_signals == ("Stripe", "incoming webhook")
    assert "title: Handle Stripe subscription webhooks" in impact.evidence
    assert "files_or_modules: src/integrations/stripe/webhooks.py" in impact.evidence


def test_queue_consumer_detected_from_files_tags_and_metadata():
    result = recommend_task_webhook_ordering_impact(
        _plan(
            [
                _task(
                    "task-queue",
                    title="Apply fulfillment updates",
                    description="Consume fulfillment status changes from workers.",
                    files_or_modules=["src/consumers/fulfillment_queue.py"],
                    metadata={
                        "surface": "fulfillment status stream",
                        "ingestion": {"kind": "queue consumer", "source": "partner"},
                    },
                    tags=["queue messages", "shopify"],
                    acceptance_criteria=[
                        "Reject stale updates when the stored version is newer."
                    ],
                )
            ]
        )
    )

    impact = result.impacts[0]
    assert impact.ingestion_surfaces == (
        "Queue consumer: fulfillment status stream",
        "Event stream: fulfillment status stream",
    )
    assert impact.missing_safeguards == (
        "sequence_number_checks",
        "monotonic_timestamp_checks",
        "ordering_buffering",
        "reordered_event_tests",
    )
    assert impact.external_service_signals == ("Shopify", "partner")
    assert impact.risk_level == "medium"
    assert any(
        item.startswith("metadata.ingestion.kind: queue consumer") for item in impact.evidence
    )
    assert "tags[0]: queue messages" in impact.evidence


def test_already_covered_ordering_safeguards_show_lower_risk():
    result = build_task_webhook_ordering_impact_plan(
        _plan(
            [
                _task(
                    "task-covered",
                    title="Process account event stream",
                    description="Handle account event stream deliveries.",
                    acceptance_criteria=[
                        "Sequence number checks ignore gaps until missing events arrive.",
                        "Monotonic event timestamps prevent older state from winning.",
                        "Version checks reject stale updates.",
                        "Buffer out-of-order events for short gaps.",
                        "Tests replay reordered events before duplicate delivery cases.",
                    ],
                )
            ]
        )
    )

    impact = result.impacts[0]
    assert impact.missing_safeguards == ()
    assert impact.risk_level == "low"
    assert result.summary["missing_safeguard_count"] == 0


def test_non_event_tasks_are_suppressed():
    result = build_task_webhook_ordering_impact_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update profile settings copy",
                    description="Clarify labels in account settings.",
                    files_or_modules=["src/ui/settings.py"],
                )
            ]
        )
    )

    assert result.impacts == ()
    assert result.ordering_task_ids == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "task_count": 1,
        "ordering_task_count": 0,
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
                title="Internal queue consumer ordering covered",
                description="Consume internal billing queue messages.",
                acceptance_criteria=[
                    "Sequence number checks enforce event order.",
                    "Monotonic timestamp checks compare event time.",
                    "Version conflict checks use aggregate version.",
                    "Reject stale updates before writes.",
                    "Ordering buffer waits for missing sequence gaps.",
                    "Reordered event tests cover out-of-order delivery.",
                ],
            ),
            _task(
                "task-a",
                title="GitHub callback receiver",
                description="Handle external callback deliveries from GitHub.",
                acceptance_criteria=["Replay safety handles duplicate callbacks."],
            ),
            _task(
                "task-m",
                title="Internal event stream consumer",
                description="Process internal event stream updates.",
                acceptance_criteria=["Version check rejects stale updates."],
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_webhook_ordering_impact_plan(plan)
    payload = task_webhook_ordering_impact_plan_to_dict(result)

    assert plan == original
    assert result.ordering_task_ids == ("task-a", "task-m", "task-z")
    assert [item.risk_level for item in result.impacts] == ["high", "medium", "low"]
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["impacts"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "impacts", "ordering_task_ids", "summary"]
    assert list(payload["impacts"][0]) == [
        "task_id",
        "title",
        "ingestion_surfaces",
        "missing_safeguards",
        "risk_level",
        "external_service_signals",
        "evidence",
    ]


def test_execution_plan_model_input_is_supported_without_mutation():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-model",
                    title="Receive partner webhook status updates",
                    description="Build incoming webhook receiver for partner status events.",
                    acceptance_criteria=[
                        "Sequence numbers and aggregate version checks reject stale updates."
                    ],
                )
            ],
            plan_id="plan-model",
        )
    )
    before = copy.deepcopy(plan.model_dump(mode="python"))

    result = build_task_webhook_ordering_impact_plan(plan)

    assert plan.model_dump(mode="python") == before
    assert result.plan_id == "plan-model"
    assert result.ordering_task_ids == ("task-model",)
    assert result.impacts[0].risk_level == "medium"
    assert result.impacts[0].missing_safeguards == (
        "monotonic_timestamp_checks",
        "ordering_buffering",
        "reordered_event_tests",
    )


def _plan(tasks, plan_id="plan-webhook-ordering"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-webhook-ordering",
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
