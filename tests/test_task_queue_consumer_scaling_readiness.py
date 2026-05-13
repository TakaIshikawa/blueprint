from blueprint.domain.models import ExecutionPlan
from blueprint.task_queue_consumer_scaling_readiness import (
    analyze_task_queue_consumer_scaling_readiness,
    generate_task_queue_consumer_scaling_readiness,
    task_queue_consumer_scaling_readiness_plan_to_dict,
    task_queue_consumer_scaling_readiness_plan_to_markdown,
)


def test_ready_queue_scaling_detects_all_signals_and_guidance_inputs():
    plan = analyze_task_queue_consumer_scaling_readiness(
        {
            "id": "plan-queue-scale",
            "tasks": [
                {
                    "id": "task-ready",
                    "title": "Queue consumer scaling for invoice workers",
                    "description": (
                        "Scale queue consumer workers with worker concurrency, autoscaling, partition "
                        "rebalancing, throughput increase, lag reduction, and backpressure throttling. "
                        "Idempotency uses dedupe message keys. Ordering preserves per-key order. Retry "
                        "behavior uses exponential backoff and jitter. Dead letter monitoring alerts on DLQ. "
                        "Capacity limits cap max workers and broker capacity. Rollback can scale down and "
                        "reduce concurrency. Metrics cover consumer lag, queue depth, throughput, latency, "
                        "and error rate."
                    ),
                    "files_or_modules": ["src/queues/invoice_consumer_autoscaling.py"],
                    "validation_commands": ["poetry run queue-load-test --canary --validate-lag"],
                }
            ],
        }
    )

    record = plan.records[0]
    assert record.detected_signals == (
        "queue_consumer_scaling",
        "worker_concurrency",
        "autoscaling",
        "partition_rebalancing",
        "throughput_increase",
        "lag_reduction",
        "backpressure",
    )
    assert record.present_criteria == (
        "idempotency",
        "ordering",
        "retry_behavior",
        "dead_letter_monitoring",
        "capacity_limits",
        "rollback_concurrency_reduction",
        "metrics",
        "validation",
    )
    assert record.missing_criteria == ()
    assert record.readiness == "ready"
    assert any("files_or_modules: src/queues/invoice_consumer_autoscaling.py" in item for item in record.evidence)
    assert any("validation_commands[0]:" in item for item in record.evidence)


def test_partial_queue_scaling_returns_ordered_followups():
    plan = analyze_task_queue_consumer_scaling_readiness(
        [
            {
                "id": "task-partial",
                "title": "Increase worker concurrency for email queue",
                "description": "Increase throughput and reduce consumer lag. Idempotency uses job keys. Metrics dashboard tracks queue depth.",
            }
        ]
    )

    record = plan.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("idempotency", "metrics")
    assert record.missing_criteria == (
        "ordering",
        "retry_behavior",
        "dead_letter_monitoring",
        "capacity_limits",
        "rollback_concurrency_reduction",
        "validation",
    )
    assert record.recommended_follow_up_actions[0].startswith("State ordering")


def test_sparse_queue_scaling_needs_planning():
    plan = analyze_task_queue_consumer_scaling_readiness(
        [{"id": "task-sparse", "title": "Autoscale queue workers", "description": "Autoscale consumers for backlog."}]
    )

    assert plan.records[0].readiness == "needs_planning"
    assert len(plan.records[0].missing_criteria) == 8


def test_unrelated_task_is_ignored():
    plan = analyze_task_queue_consumer_scaling_readiness(
        [{"id": "task-ui", "title": "Update empty state", "description": "Refresh dashboard copy."}]
    )

    assert plan.records == ()
    assert plan.ignored_task_ids == ("task-ui",)


def test_execution_plan_and_serialization_are_stable():
    source = {
        "id": "plan-model",
        "implementation_brief_id": "brief-queue",
        "milestones": [],
        "tasks": [
            {
                "id": "task-model",
                "title": "Queue worker scaling",
                "description": "Queue consumer scaling has retry policy, DLQ alerts, capacity limits, rollback, and validation command.",
                "acceptance_criteria": [],
            }
        ],
    }
    model = ExecutionPlan.model_validate(source)

    plan = generate_task_queue_consumer_scaling_readiness(model)
    payload = task_queue_consumer_scaling_readiness_plan_to_dict(plan)
    markdown = task_queue_consumer_scaling_readiness_plan_to_markdown(plan)

    assert plan.plan_id == "plan-model"
    assert list(payload) == ["plan_id", "records", "findings", "recommendations", "impacted_task_ids", "ignored_task_ids", "summary"]
    assert payload["summary"]["missing_criterion_count"] == 2
    assert markdown == plan.to_markdown()
    assert "# Task Queue Consumer Scaling Readiness: plan-model" in markdown
