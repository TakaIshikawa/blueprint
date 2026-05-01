import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_queue_backpressure_readiness import (
    TaskQueueBackpressureReadinessPlan,
    TaskQueueBackpressureReadinessRecommendation,
    build_task_queue_backpressure_readiness_plan,
    generate_task_queue_backpressure_readiness,
    summarize_task_queue_backpressure_readiness,
    task_queue_backpressure_readiness_plan_to_dict,
    task_queue_backpressure_readiness_to_dicts,
)


def test_queue_detection_from_title_description_paths_tags_and_metadata():
    result = build_task_queue_backpressure_readiness_plan(
        _plan(
            [
                _task(
                    "task-title",
                    title="Add task queue for email delivery",
                    description="Move notifications into async jobs.",
                ),
                _task(
                    "task-description",
                    title="Process billing events",
                    description="Add worker and consumer handling for billing messages.",
                ),
                _task(
                    "task-path",
                    title="Refactor dispatch module",
                    description="Move implementation files.",
                    files_or_modules=["src/jobs/workers/payment_consumer.py"],
                ),
                _task(
                    "task-tags",
                    title="Import reports",
                    description="Improve report imports.",
                    tags=["batch processor", "backfill"],
                ),
                _task(
                    "task-metadata",
                    title="Route event bus data",
                    description="Update routing.",
                    metadata={"surface": "stream", "throughput": "high volume customer-facing realtime work"},
                ),
            ]
        )
    )

    assert isinstance(result, TaskQueueBackpressureReadinessPlan)
    assert result.plan_id == "plan-queue-backpressure-readiness"
    by_id = {record.task_id: record for record in result.recommendations}
    assert set(by_id) == {
        "task-metadata",
        "task-title",
        "task-description",
        "task-path",
        "task-tags",
    }
    assert by_id["task-title"].queue_surfaces == ("queue",)
    assert {"worker", "consumer"} <= set(by_id["task-description"].queue_surfaces)
    assert {"worker", "consumer"} <= set(by_id["task-path"].queue_surfaces)
    assert by_id["task-tags"].queue_surfaces == ("batch processor",)
    assert "stream" in by_id["task-metadata"].queue_surfaces
    assert {"high_volume", "realtime", "customer_facing"} <= set(by_id["task-metadata"].throughput_signals)
    assert any("metadata.throughput" in item for item in by_id["task-metadata"].evidence)


def test_high_volume_realtime_customer_facing_queues_escalate_and_sort_first():
    result = build_task_queue_backpressure_readiness_plan(
        _plan(
            [
                _task(
                    "task-medium",
                    title="Add nightly batch processor",
                    description="Run scheduled batch job for admin exports.",
                ),
                _task(
                    "task-high",
                    title="Add realtime checkout queue",
                    description="Customer-facing high-volume worker queue for live checkout events.",
                ),
            ]
        )
    )

    assert result.flagged_task_ids == ("task-high", "task-medium")
    by_id = {record.task_id: record for record in result.recommendations}
    assert by_id["task-high"].risk_level == "high"
    assert {"high_volume", "realtime", "customer_facing"} <= set(by_id["task-high"].throughput_signals)
    assert by_id["task-medium"].risk_level == "medium"
    assert by_id["task-high"].missing_backpressure_controls == (
        "concurrency_limit",
        "retry_backoff",
        "dead_letter_or_quarantine",
        "rate_limit_or_throttle",
        "queue_depth_monitoring",
        "saturation_alerts",
        "load_test_evidence",
    )


def test_complete_backpressure_controls_reduce_recommendation_risk():
    result = build_task_queue_backpressure_readiness_plan(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Add high-volume worker queue",
                    description="Customer-facing realtime queue for payment events.",
                    acceptance_criteria=[
                        "Set concurrency limit and rate limit throttling.",
                        "Use retry backoff with jitter and dead-letter queue quarantine.",
                        "Track queue depth monitoring and saturation alerts.",
                        "Attach load test evidence before launch.",
                    ],
                ),
                _task(
                    "task-incomplete",
                    title="Add high-volume worker queue without safeguards",
                    description="Customer-facing realtime queue for payment events.",
                    acceptance_criteria=["Worker code is merged."],
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.recommendations}
    assert by_id["task-complete"].missing_backpressure_controls == ()
    assert by_id["task-complete"].risk_level == "low"
    assert by_id["task-incomplete"].risk_level == "high"
    assert by_id["task-complete"].throughput_signals == by_id["task-incomplete"].throughput_signals


def test_non_queue_tasks_are_suppressed_and_summary_counts_unrelated_tasks():
    result = build_task_queue_backpressure_readiness_plan(
        _plan(
            [
                _task(
                    "task-docs",
                    title="Update settings docs",
                    description="Document UI settings only.",
                    files_or_modules=["docs/settings.md"],
                ),
                _task(
                    "task-worker",
                    title="Add image worker",
                    description="Background worker for thumbnails.",
                ),
            ]
        )
    )

    assert result.flagged_task_ids == ("task-worker",)
    assert result.summary["total_task_count"] == 2
    assert result.summary["flagged_task_count"] == 1
    assert result.summary["unrelated_task_count"] == 1
    assert result.summary["risk_counts"] == {"low": 0, "medium": 1, "high": 0}
    assert result.summary["missing_backpressure_control_counts"]["queue_depth_monitoring"] == 1
    assert result.summary["queue_surface_counts"]["worker"] == 1


def test_model_input_serializes_stably_without_mutation_and_aliases_match():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Add worker saturation alerts",
                description="Create worker pool for invoices and alert on saturation.",
            ),
            _task(
                "task-a",
                title="Add stream consumer",
                description="Realtime customer-facing stream consumer for account events.",
                metadata={
                    "controls": {
                        "concurrency_limit": "max concurrency is capped",
                        "retry_backoff": "exponential backoff with jitter",
                        "dead_letter_or_quarantine": "dead-letter queue captures poison messages",
                        "rate_limit_or_throttle": "rate limiting protects downstream APIs",
                        "queue_depth_monitoring": "consumer lag monitoring is charted",
                        "saturation_alerts": "lag alert and worker saturation alert configured",
                        "load_test_evidence": "load test evidence attached",
                    }
                },
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = build_task_queue_backpressure_readiness_plan(model)
    alias_result = summarize_task_queue_backpressure_readiness(plan)
    records = generate_task_queue_backpressure_readiness(model)
    payload = task_queue_backpressure_readiness_plan_to_dict(result)

    assert plan == original
    assert result.flagged_task_ids == ("task-z", "task-a")
    assert isinstance(result.recommendations[0], TaskQueueBackpressureReadinessRecommendation)
    assert records == result.recommendations
    assert alias_result.to_dict() == result.to_dict()
    assert result.to_dicts() == payload["recommendations"]
    assert task_queue_backpressure_readiness_to_dicts(records) == payload["recommendations"]
    assert task_queue_backpressure_readiness_to_dicts(result) == payload["recommendations"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["plan_id", "recommendations", "flagged_task_ids", "summary"]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "queue_surfaces",
        "missing_backpressure_controls",
        "risk_level",
        "throughput_signals",
        "evidence",
    ]


def test_empty_partial_or_non_model_sources_do_not_raise():
    empty = build_task_queue_backpressure_readiness_plan(_plan([_task("task-general", title="General work")]))
    assert empty.recommendations == ()
    assert empty.flagged_task_ids == ()
    assert empty.summary["flagged_task_count"] == 0
    assert empty.summary["unrelated_task_count"] == 1
    assert generate_task_queue_backpressure_readiness({"tasks": "not a list"}) == ()
    assert generate_task_queue_backpressure_readiness("not a plan") == ()
    assert generate_task_queue_backpressure_readiness(None) == ()
    assert build_task_queue_backpressure_readiness_plan({"tasks": "not a list"}).summary == {
        "total_task_count": 0,
        "flagged_task_count": 0,
        "unrelated_task_count": 0,
        "risk_counts": {"low": 0, "medium": 0, "high": 0},
        "missing_backpressure_control_counts": {
            "concurrency_limit": 0,
            "retry_backoff": 0,
            "dead_letter_or_quarantine": 0,
            "rate_limit_or_throttle": 0,
            "queue_depth_monitoring": 0,
            "saturation_alerts": 0,
            "load_test_evidence": 0,
        },
        "queue_surface_counts": {
            "queue": 0,
            "worker": 0,
            "consumer": 0,
            "scheduler": 0,
            "stream": 0,
            "batch processor": 0,
        },
        "throughput_signal_counts": {
            "high_volume": 0,
            "realtime": 0,
            "customer_facing": 0,
            "bursty": 0,
            "long_running": 0,
        },
    }


def _plan(tasks):
    return {
        "id": "plan-queue-backpressure-readiness",
        "implementation_brief_id": "brief-queue-backpressure-readiness",
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
    tags=None,
    metadata=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {title or task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "metadata": metadata or {},
    }
    if tags is not None:
        task["tags"] = tags
    return task
