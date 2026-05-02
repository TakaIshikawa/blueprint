import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_queue_poison_pill_readiness import (
    TaskQueuePoisonPillReadinessPlan,
    TaskQueuePoisonPillReadinessRecord,
    analyze_task_queue_poison_pill_readiness,
    build_task_queue_poison_pill_readiness_plan,
    extract_task_queue_poison_pill_readiness,
    generate_task_queue_poison_pill_readiness,
    recommend_task_queue_poison_pill_readiness,
    summarize_task_queue_poison_pill_readiness,
    task_queue_poison_pill_readiness_plan_to_dict,
    task_queue_poison_pill_readiness_plan_to_dicts,
    task_queue_poison_pill_readiness_plan_to_markdown,
)


def test_detects_queue_stream_retry_dlq_idempotency_and_malformed_payload_sources():
    result = build_task_queue_poison_pill_readiness_plan(
        _plan(
            [
                _task(
                    "task-worker",
                    title="Harden order queue consumer poison pill handling",
                    description=(
                        "Kafka stream consumer processes order events with retry loop handling, "
                        "idempotent consumer deduplication, and malformed payload handling."
                    ),
                    files_or_modules=[
                        "src/workers/order_queue_consumer.py",
                        "src/events/kafka_dead_letter_queue.py",
                    ],
                    acceptance_criteria=[
                        "Dead-letter queue behavior is documented for poison messages.",
                        "Payload validation rejects invalid payloads before side effects.",
                    ],
                    validation_commands=[
                        "pytest tests/workers/test_order_consumer_retry_loop.py",
                        "pytest tests/events/test_malformed_payload_handling.py",
                    ],
                )
            ]
        )
    )

    assert isinstance(result, TaskQueuePoisonPillReadinessPlan)
    assert result.impacted_task_ids == ("task-worker",)
    record = result.records[0]
    assert isinstance(record, TaskQueuePoisonPillReadinessRecord)
    assert record.matched_signals == (
        "queue_consumer",
        "event_stream",
        "retry_loop",
        "dead_letter_queue",
        "idempotent_consumer",
        "malformed_payload_handling",
    )
    assert record.present_safeguards == ("payload_validation",)
    assert record.missing_safeguards == (
        "dlq_routing",
        "max_retry_limits",
        "alerting",
        "replay_tooling",
        "manual_quarantine_ownership",
    )
    assert record.risk_level == "high"
    assert record.recommended_checks[0].startswith("Verify unrecoverable messages route to a DLQ")
    assert any("description:" in item and "Kafka stream consumer" in item for item in record.evidence)
    assert "files_or_modules: src/events/kafka_dead_letter_queue.py" in record.evidence
    assert any("validation_commands[0]:" in item and "retry_loop" in item for item in record.evidence)
    assert result.summary["signal_counts"]["dead_letter_queue"] == 1
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 0}


def test_metadata_acceptance_criteria_and_validation_plan_detect_ready_safeguards():
    result = analyze_task_queue_poison_pill_readiness(
        _plan(
            [
                _task(
                    "task-dlq",
                    title="Add SQS worker DLQ redrive",
                    description="SQS worker consumes messages and handles poison pills.",
                    metadata={
                        "poison_pill": {
                            "dlq_routing": "Route unrecoverable messages to the DLQ after terminal failure.",
                            "max_retry_limits": "Maximum retry attempts are capped at 5 with backoff.",
                            "alerting": "Alert on DLQ depth, retry spikes, and consumer stalls.",
                            "replay_tooling": "Redrive and replay runbook covers idempotency expectations.",
                            "quarantine_owner": "Ops owner handles manual quarantine triage.",
                        }
                    },
                    acceptance_criteria=[
                        "Payload validation rejects malformed payloads.",
                        "Idempotent consumer behavior handles duplicate messages.",
                    ],
                    validation_plan="Run replay tooling tests and alerting checks for DLQ routing.",
                )
            ]
        )
    )

    record = result.records[0]
    assert {"queue_consumer", "dead_letter_queue", "idempotent_consumer", "malformed_payload_handling"} <= set(
        record.matched_signals
    )
    assert record.present_safeguards == (
        "dlq_routing",
        "max_retry_limits",
        "alerting",
        "replay_tooling",
        "payload_validation",
        "manual_quarantine_ownership",
    )
    assert record.missing_safeguards == ()
    assert record.risk_level == "low"
    assert record.recommended_checks == ()
    assert any("metadata.poison_pill.dlq_routing" in item for item in record.evidence)
    assert any("validation_plan:" in item for item in record.evidence)
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 1}


def test_execution_plan_execution_task_single_task_and_no_impact_handling():
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Bound worker retries",
            description="Set max retry limit for a queue worker and alert on retry spikes.",
            acceptance_criteria=[
                "Maximum retry attempts stop retrying after 3 attempts.",
                "Alerting covers retry spike alarms.",
            ],
        )
    )
    single_task = build_task_queue_poison_pill_readiness_plan(model_task)
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-stream",
                    title="Validate stream payloads",
                    description="Kinesis event stream consumer validates malformed payloads.",
                ),
                _task("task-copy", title="Update copy", description="Adjust empty state wording."),
            ]
        )
    )
    plan_result = generate_task_queue_poison_pill_readiness(plan)
    empty = build_task_queue_poison_pill_readiness_plan([])
    noop = build_task_queue_poison_pill_readiness_plan(
        _plan([_task("task-copy", title="Update copy", description="Static text.")])
    )

    assert single_task.plan_id is None
    assert single_task.impacted_task_ids == ("task-model",)
    assert single_task.records[0].risk_level == "high"
    assert plan_result.plan_id == "plan-poison-pill"
    assert plan_result.impacted_task_ids == ("task-stream",)
    assert plan_result.no_impact_task_ids == ("task-copy",)
    assert empty.records == ()
    assert empty.no_impact_task_ids == ()
    assert noop.records == ()
    assert noop.impacted_task_ids == ()
    assert noop.no_impact_task_ids == ("task-copy",)
    assert noop.summary == {
        "task_count": 1,
        "impacted_task_count": 0,
        "impacted_task_ids": [],
        "no_impact_task_ids": ["task-copy"],
        "missing_safeguard_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "signal_counts": {
            "queue_consumer": 0,
            "event_stream": 0,
            "retry_loop": 0,
            "dead_letter_queue": 0,
            "idempotent_consumer": 0,
            "malformed_payload_handling": 0,
        },
        "missing_safeguard_counts": {
            "dlq_routing": 0,
            "max_retry_limits": 0,
            "alerting": 0,
            "replay_tooling": 0,
            "payload_validation": 0,
            "manual_quarantine_ownership": 0,
        },
        "present_safeguard_counts": {
            "dlq_routing": 0,
            "max_retry_limits": 0,
            "alerting": 0,
            "replay_tooling": 0,
            "payload_validation": 0,
            "manual_quarantine_ownership": 0,
        },
    }
    assert "No task queue poison-pill readiness records" in noop.to_markdown()
    assert "No-impact tasks: task-copy" in noop.to_markdown()


def test_deterministic_serialization_markdown_aliases_sorting_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Kafka consumer ready | low",
                description="Kafka event stream consumer handles poison messages.",
                acceptance_criteria=[
                    "Route poison messages to DLQ.",
                    "Maximum retry attempts are capped.",
                    "Alert on DLQ depth and consumer stalls.",
                    "Replay tooling supports safe redrive.",
                    "Payload validation rejects invalid payloads.",
                    "Manual quarantine ownership is assigned.",
                ],
            ),
            _task(
                "task-a",
                title="Retry worker loop",
                description="Add retry loop handling to the task queue worker.",
                acceptance_criteria=["Maximum retry attempts are capped."],
            ),
            _task(
                "task-m",
                title="Queue consumer alerting",
                description="Add queue consumer monitoring and alerting.",
                acceptance_criteria=["Alerting covers queue depth."],
            ),
            _task("task-copy", title="Copy update", description="Update helper text."),
        ]
    )
    original = copy.deepcopy(plan)

    result = summarize_task_queue_poison_pill_readiness(plan)
    payload = task_queue_poison_pill_readiness_plan_to_dict(result)
    markdown = task_queue_poison_pill_readiness_plan_to_markdown(result)

    assert plan == original
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.findings
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["records"]
    assert task_queue_poison_pill_readiness_plan_to_dicts(result) == payload["records"]
    assert task_queue_poison_pill_readiness_plan_to_dicts(result.records) == payload["records"]
    assert extract_task_queue_poison_pill_readiness(plan).to_dict() == result.to_dict()
    assert recommend_task_queue_poison_pill_readiness(plan).to_dict() == result.to_dict()
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "matched_signals",
        "present_safeguards",
        "missing_safeguards",
        "risk_level",
        "recommended_checks",
        "evidence",
    ]
    assert result.impacted_task_ids == ("task-a", "task-m", "task-z")
    assert [record.risk_level for record in result.records] == ["high", "high", "low"]
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary["risk_counts"] == {"high": 2, "medium": 0, "low": 1}
    assert markdown.startswith("# Task Queue Poison Pill Readiness: plan-poison-pill")
    assert "Kafka consumer ready \\| low" in markdown
    assert (
        "| Task | Title | Risk | Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |"
        in markdown
    )


def _plan(tasks, plan_id="plan-poison-pill"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-poison-pill",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "service",
        "milestones": [],
        "test_strategy": "pytest",
        "status": "draft",
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title,
    description,
    files_or_modules=None,
    acceptance_criteria=None,
    validation_plan=None,
    validation_commands=None,
    metadata=None,
    tags=None,
    risks=None,
):
    payload = {
        "id": task_id,
        "execution_plan_id": "plan-poison-pill",
        "title": title,
        "description": description,
        "milestone": "implementation",
        "owner_type": "agent",
        "suggested_engine": "codex",
        "depends_on": [],
        "files_or_modules": files_or_modules,
        "acceptance_criteria": acceptance_criteria or ["Implemented."],
        "estimated_complexity": "small",
        "estimated_hours": 1.0,
        "risk_level": "medium",
        "test_command": "poetry run pytest",
        "status": "pending",
        "metadata": metadata or {},
    }
    if validation_plan is not None:
        payload["validation_plan"] = validation_plan
    if validation_commands is not None:
        payload["validation_commands"] = validation_commands
    if tags is not None:
        payload["tags"] = tags
    if risks is not None:
        payload["risks"] = risks
    return payload
