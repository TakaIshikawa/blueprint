import json

from blueprint.plan_api_idempotency_readiness_matrix import (
    PlanApiIdempotencyReadinessMatrix,
    build_plan_api_idempotency_readiness_matrix,
    generate_plan_api_idempotency_readiness_matrix,
    plan_api_idempotency_readiness_matrix_to_dict,
    plan_api_idempotency_readiness_matrix_to_dicts,
    plan_api_idempotency_readiness_matrix_to_markdown,
)


def test_ready_idempotency_plan_scores_all_rows():
    result = build_plan_api_idempotency_readiness_matrix(
        _plan(
            [
                _task(
                    "task-payment-idempotency",
                    title="Add idempotency keys to POST /payments",
                    description=(
                        "Create payment flow accepts an Idempotency-Key, defines retry semantics with backoff, "
                        "duplicate suppression through a request cache, 409 conflict responses for mismatched "
                        "payloads, observability metrics and logs, and rollback criteria behind a feature flag."
                    ),
                    metadata={"owner": "payments-api"},
                )
            ]
        )
    )

    assert isinstance(result, PlanApiIdempotencyReadinessMatrix)
    assert result.idempotency_task_ids == ("task-payment-idempotency",)
    assert [row.area for row in result.rows] == [
        "idempotency_keys",
        "retry_semantics",
        "duplicate_suppression",
        "conflict_responses",
        "observability",
        "rollback_criteria",
    ]
    assert all(row.readiness == "ready" for row in result.rows)
    assert all(row.score == 100 for row in result.rows)
    assert all(row.owner == "payments-api" for row in result.rows)
    assert result.summary["score"] == 100


def test_partial_and_blocked_idempotency_gaps_are_classified():
    partial = build_plan_api_idempotency_readiness_matrix(
        _plan([_task("task-order", title="Create order POST endpoint", description="Add retry metrics for checkout.")])
    )
    blocked = build_plan_api_idempotency_readiness_matrix(
        _plan([_task("task-blocked", title="Add idempotency to POST /orders", description="Blocked by missing storage dependency.")])
    )

    assert _row(partial, "idempotency_keys").readiness == "partial"
    assert _row(partial, "idempotency_keys").risk == "high"
    assert _row(partial, "retry_semantics").readiness == "ready"
    assert _row(blocked, "idempotency_keys").readiness == "blocked"
    assert _row(blocked, "rollback_criteria").readiness == "blocked"


def test_idempotency_serialization_markdown_and_unrelated_plan():
    result = generate_plan_api_idempotency_readiness_matrix(
        _plan(
            [
                _task(
                    "task-order | create",
                    title="Create order idempotency | API",
                    description="Idempotency-Key, retry semantics, dedupe, 409 conflict, metrics, rollback criteria.",
                )
            ]
        )
    )
    payload = plan_api_idempotency_readiness_matrix_to_dict(result)
    markdown = plan_api_idempotency_readiness_matrix_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert plan_api_idempotency_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert markdown.startswith("# Plan API Idempotency Readiness Matrix: plan-idempotency")
    assert "task-order \\| create" in markdown
    assert build_plan_api_idempotency_readiness_matrix({"id": "empty", "tasks": []}).rows == ()
    assert build_plan_api_idempotency_readiness_matrix({"id": "none", "tasks": [_task("copy", title="Update copy")]}).rows == ()


def _row(result, area):
    return next(row for row in result.rows if row.area == area)


def _plan(tasks):
    return {"id": "plan-idempotency", "implementation_brief_id": "brief", "milestones": [], "tasks": tasks}


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
