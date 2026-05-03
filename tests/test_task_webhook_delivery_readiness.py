import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_webhook_delivery_readiness import (
    TaskWebhookDeliveryReadinessPlan,
    TaskWebhookDeliveryReadinessRecord,
    analyze_task_webhook_delivery_readiness,
    build_task_webhook_delivery_readiness_plan,
    derive_task_webhook_delivery_readiness,
    generate_task_webhook_delivery_readiness,
    recommend_task_webhook_delivery_readiness,
    summarize_task_webhook_delivery_readiness,
    task_webhook_delivery_readiness_plan_to_dict,
    task_webhook_delivery_readiness_plan_to_dicts,
    task_webhook_delivery_readiness_plan_to_markdown,
    task_webhook_delivery_readiness_to_dicts,
)


def test_weak_webhook_delivery_task_sorts_first_and_separates_no_impact_ids():
    result = build_task_webhook_delivery_readiness_plan(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Publish account integration events",
                    description="Publish events to the event bus for partner webhook subscribers.",
                    acceptance_criteria=[
                        "Retry tests cover exponential backoff.",
                        "Observability emits delivery failure metrics and alerts.",
                        "Rate limiting bounds subscriber delivery throughput.",
                    ],
                ),
                _task(
                    "task-copy",
                    title="Polish settings copy",
                    description="Update labels in the admin settings page.",
                ),
                _task(
                    "task-weak",
                    title="Deliver partner webhooks",
                    description="Send outbound webhook delivery requests with HMAC signing and retries.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskWebhookDeliveryReadinessPlan)
    assert result.webhook_task_ids == ("task-weak", "task-partial")
    assert result.impacted_task_ids == result.webhook_task_ids
    assert result.no_impact_task_ids == ("task-copy",)
    assert [record.task_id for record in result.records] == ["task-weak", "task-partial"]
    weak = result.records[0]
    assert isinstance(weak, TaskWebhookDeliveryReadinessRecord)
    assert weak.readiness == "weak"
    assert weak.impact == "high"
    assert {"webhook_delivery", "signing", "retries"} <= set(weak.detected_signals)
    assert "retry_tests" in weak.missing_safeguards
    assert "signature_verification_tests" in weak.missing_safeguards


def test_strong_readiness_reflects_all_safeguards_and_summary_counts():
    result = analyze_task_webhook_delivery_readiness(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Add signed webhook delivery worker",
                    description=(
                        "Webhook delivery publishes event payloads, verifies tenant scoping, keeps delivery logs, "
                        "supports schema versioning, and sends failures to a dead-letter queue."
                    ),
                    acceptance_criteria=[
                        "Retry tests cover attempts, backoff, and terminal failures.",
                        "Signature verification tests cover valid, invalid, and rotated HMAC secrets.",
                        "Replay tooling lets operators redeliver from delivery history.",
                        "Observability includes metrics, dashboard, logs, queue depth, and alerts.",
                        "Rate limiting uses concurrency limits and a circuit breaker.",
                        "Runbook covers DLQ drain, replay, and partner escalation.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "strong"
    assert record.impact == "medium"
    assert record.present_safeguards == (
        "retry_tests",
        "signature_verification_tests",
        "replay_tooling",
        "observability",
        "rate_limiting",
        "runbook",
    )
    assert record.missing_safeguards == ()
    assert record.recommended_checks == ()
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "strong": 1}
    assert result.summary["impact_counts"] == {"high": 0, "medium": 1, "low": 0}
    assert result.summary["signal_counts"]["dead_letter_queue"] == 1
    assert result.summary["present_safeguard_counts"]["runbook"] == 1


def test_mapping_input_collects_title_description_paths_and_validation_command_evidence():
    result = build_task_webhook_delivery_readiness_plan(
        {
            "id": "task-mapping",
            "title": "Implement event publishing outbox",
            "description": "Publish integration events for outbound webhooks with idempotency keys.",
            "files_or_modules": [
                "src/integrations/webhooks/delivery_retries.py",
                "src/integrations/webhooks/signature_verification_tests.py",
                "ops/webhook_replay_runbook.md",
            ],
            "validation_commands": {
                "tests": [
                    "poetry run pytest tests/test_webhook_retry_delivery.py",
                    "poetry run pytest tests/test_webhook_signature_verification.py",
                ]
            },
        }
    )

    record = result.records[0]
    assert {"webhook_delivery", "event_publishing", "idempotency", "retries", "signing", "replay"} <= set(
        record.detected_signals
    )
    assert {"signature_verification_tests", "replay_tooling", "runbook"} <= set(record.present_safeguards)
    assert any(item == "title: Implement event publishing outbox" for item in record.evidence)
    assert any("description: Publish integration events" in item for item in record.evidence)
    assert any(item == "files_or_modules: src/integrations/webhooks/delivery_retries.py" for item in record.evidence)
    assert any("validation_commands: poetry run pytest tests/test_webhook_retry_delivery.py" in item for item in record.evidence)


def test_execution_plan_execution_task_and_object_inputs_are_supported_without_mutation():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add webhook delivery logs",
        description="Persist delivery logs and delivery attempts for webhook replay.",
        acceptance_criteria=["Observability dashboard shows delivery attempts and failure rate."],
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Version webhook payload schema",
            description="Add schema versioning for webhook payload contracts.",
            acceptance_criteria=["Runbook explains versioned payload rollout."],
        )
    )
    source = _plan(
        [
            model_task.model_dump(mode="python"),
            _task(
                "task-covered",
                title="Build tenant scoped webhook dispatcher",
                description="Webhook dispatcher enforces tenant scoping before delivery.",
                acceptance_criteria=[
                    "Retry tests cover backoff.",
                    "Signature verification tests cover invalid signatures.",
                    "Replay tooling is available.",
                    "Observability tracks delivery failures.",
                    "Rate limiting protects subscribers.",
                    "Runbook covers incidents.",
                ],
            ),
        ],
        plan_id="plan-webhooks-objects",
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)

    result = summarize_task_webhook_delivery_readiness(source)

    assert source == original
    assert build_task_webhook_delivery_readiness_plan(object_task).records[0].task_id == "task-object"
    assert generate_task_webhook_delivery_readiness(model).plan_id == "plan-webhooks-objects"
    assert derive_task_webhook_delivery_readiness(source).to_dict() == result.to_dict()
    assert recommend_task_webhook_delivery_readiness(source).to_dict() == result.to_dict()
    assert result.records == result.findings
    assert result.records == result.recommendations


def test_empty_state_markdown_and_summary_are_stable():
    result = build_task_webhook_delivery_readiness_plan(
        _plan([_task("task-ui", title="Polish dashboard", description="Adjust table spacing.")])
    )

    assert result.records == ()
    assert result.webhook_task_ids == ()
    assert result.no_impact_task_ids == ("task-ui",)
    assert result.summary["webhook_task_count"] == 0
    assert result.to_markdown() == "\n".join(
        [
            "# Task Webhook Delivery Readiness: plan-webhook-delivery",
            "",
            "## Summary",
            "",
            "- Task count: 1",
            "- Webhook task count: 0",
            "- Missing safeguard count: 0",
            "- Readiness counts: weak 0, partial 0, strong 0",
            "- Impact counts: high 0, medium 0, low 0",
            "",
            "No task webhook delivery readiness records were inferred.",
            "",
            "No-impact tasks: task-ui",
        ]
    )


def test_serialization_aliases_to_dict_output_and_markdown_are_json_safe():
    result = build_task_webhook_delivery_readiness_plan(
        _plan(
            [
                _task(
                    "task-webhook",
                    title="Add webhook DLQ replay",
                    description="Dead-letter queue stores failed webhook delivery events for replay.",
                    acceptance_criteria=[
                        "Retry tests cover failed delivery attempts.",
                        "Replay tooling can redeliver from history.",
                        "Monitoring metrics and alerts show queue depth.",
                    ],
                )
            ],
            plan_id="plan-serialization",
        )
    )
    payload = task_webhook_delivery_readiness_plan_to_dict(result)
    markdown = task_webhook_delivery_readiness_plan_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_webhook_delivery_readiness_plan_to_dicts(result) == payload["records"]
    assert task_webhook_delivery_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_webhook_delivery_readiness_to_dicts(result) == payload["records"]
    assert task_webhook_delivery_readiness_to_dicts(result.records) == payload["records"]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Webhook Delivery Readiness: plan-serialization")
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "webhook_task_ids",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "detected_signals",
        "present_safeguards",
        "missing_safeguards",
        "readiness",
        "impact",
        "recommended_checks",
        "evidence",
    ]


def _plan(tasks, *, plan_id="plan-webhook-delivery"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-webhook-delivery",
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
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    return task
