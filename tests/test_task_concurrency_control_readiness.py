import copy
import json

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_concurrency_control_readiness import (
    TaskConcurrencyControlReadinessPlan,
    TaskConcurrencyControlReadinessRecommendation,
    build_task_concurrency_control_readiness_plan,
    generate_task_concurrency_control_readiness,
    summarize_task_concurrency_control_readiness,
    task_concurrency_control_readiness_plan_to_dict,
    task_concurrency_control_readiness_plan_to_markdown,
    task_concurrency_control_readiness_to_dicts,
)


def test_concurrent_worker_surfaces_are_detected_from_text_paths_and_metadata():
    result = build_task_concurrency_control_readiness_plan(
        _plan(
            [
                _task(
                    "task-worker",
                    title="Add parallel worker writes",
                    description=(
                        "Parallel workers consume a shared queue and perform concurrent writes "
                        "that can cause a race condition or duplicate jobs."
                    ),
                    files_or_modules=[
                        "src/workers/payment_queue_consumer.py",
                        "src/locks/payment_advisory_lock.py",
                        "src/idempotency/duplicate_submission_guard.py",
                    ],
                    metadata={
                        "optimistic_concurrency": "use a version check for stale writes",
                        "owner": "payments",
                    },
                ),
                _task(
                    "task-docs",
                    title="Document worker dashboard",
                    description="Docs-only formatting update.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskConcurrencyControlReadinessPlan)
    assert result.plan_id == "plan-concurrency-control-readiness"
    assert result.concurrency_task_ids == ("task-worker",)
    assert result.suppressed_task_ids == ("task-docs",)
    record = result.recommendations[0]
    assert isinstance(record, TaskConcurrencyControlReadinessRecommendation)
    assert record.concurrency_surfaces == (
        "concurrent_write",
        "parallel_worker",
        "shared_queue",
        "lock_contention",
        "optimistic_concurrency",
        "race_condition",
        "duplicate_submission",
    )
    assert record.risk_level == "high"
    assert "retry_or_backoff" in record.missing_controls
    assert "rollback_or_repair_path" in record.missing_controls
    assert any("files_or_modules: src/workers/payment_queue_consumer.py" == item for item in record.evidence)
    assert any("metadata.optimistic_concurrency" in item for item in record.evidence)


def test_explicit_controls_reduce_missing_controls_and_risk_level():
    result = build_task_concurrency_control_readiness_plan(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Harden duplicate checkout submissions",
                    description=(
                        "Concurrent writes from duplicate requests use an idempotency key, "
                        "lock strategy, conflict detection with an etag version check, retries "
                        "with exponential backoff and jitter, and a transaction boundary."
                    ),
                    acceptance_criteria=[
                        "Observability metrics include conflict rate, duplicate rate, retry count, and alerts.",
                        "Rollback or repair path includes reconciliation and manual repair for bad writes.",
                    ],
                ),
                _task(
                    "task-missing",
                    title="Handle duplicate checkout submissions",
                    description="Concurrent writes from duplicate requests may race under parallel workers.",
                ),
            ]
        )
    )

    by_id = {record.task_id: record for record in result.recommendations}
    assert by_id["task-missing"].risk_level == "high"
    assert by_id["task-complete"].risk_level == "low"
    assert by_id["task-complete"].missing_controls == ()
    assert result.summary["risk_counts"] == {"high": 1, "medium": 0, "low": 1}
    assert result.summary["missing_control_counts"]["lock_strategy"] == 1
    assert result.summary["missing_control_counts"]["duplicate_request_guard"] == 1


def test_unrelated_tasks_are_suppressed_and_no_op_inputs_are_stable():
    unrelated = build_task_concurrency_control_readiness_plan(
        _plan(
            [
                _task(
                    "task-ui",
                    title="Polish dashboard spacing",
                    description="Adjust layout spacing.",
                    files_or_modules=["src/ui/dashboard.css"],
                )
            ]
        )
    )

    assert unrelated.recommendations == ()
    assert unrelated.concurrency_task_ids == ()
    assert unrelated.suppressed_task_ids == ("task-ui",)
    assert unrelated.summary["concurrency_task_count"] == 0
    assert unrelated.to_markdown().endswith("No concurrency-control readiness recommendations were inferred.")
    assert generate_task_concurrency_control_readiness(_plan([])) == ()
    assert generate_task_concurrency_control_readiness({"tasks": "not a list"}) == ()
    assert generate_task_concurrency_control_readiness("not a plan") == ()
    assert generate_task_concurrency_control_readiness(None) == ()


def test_model_inputs_aliases_and_serialization_are_deterministic_and_json_compatible():
    source = _plan(
        [
            _task(
                "task-z",
                title="Add worker retry handling",
                description="Background worker pool needs retry backoff for lock contention on shared queue writes.",
                metadata={"controls": {"observability_metric": "lock wait metric and alert"}},
            ),
            _task(
                "task-a",
                title="Add duplicate guard",
                description="Duplicate submissions use idempotency key conflict detection.",
            ),
        ]
    )
    original = copy.deepcopy(source)
    model = ExecutionPlan.model_validate(source)
    task_model = ExecutionTask.model_validate(source["tasks"][0])

    result = build_task_concurrency_control_readiness_plan(model)
    alias_result = summarize_task_concurrency_control_readiness(source)
    direct_records = generate_task_concurrency_control_readiness(task_model)
    payload = task_concurrency_control_readiness_plan_to_dict(result)
    markdown = task_concurrency_control_readiness_plan_to_markdown(result)

    assert source == original
    assert result.to_dict() == build_task_concurrency_control_readiness_plan(model).to_dict()
    assert alias_result.to_dict() == build_task_concurrency_control_readiness_plan(source).to_dict()
    assert result.records == result.recommendations
    assert result.to_dicts() == payload["recommendations"]
    assert task_concurrency_control_readiness_to_dicts(result) == payload["recommendations"]
    assert task_concurrency_control_readiness_to_dicts(result.recommendations) == payload["recommendations"]
    assert direct_records[0].task_id == "task-z"
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "recommendations",
        "concurrency_task_ids",
        "suppressed_task_ids",
        "summary",
    ]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "concurrency_surfaces",
        "missing_controls",
        "risk_level",
        "evidence",
    ]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task Concurrency Control Readiness: plan-concurrency-control-readiness")


def _plan(tasks):
    return {
        "id": "plan-concurrency-control-readiness",
        "implementation_brief_id": "brief-concurrency-control-readiness",
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
        "description": description or "",
        "acceptance_criteria": acceptance_criteria or [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if tags is not None:
        task["tags"] = tags
    if metadata is not None:
        task["metadata"] = metadata
    return task
