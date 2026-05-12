import json

from blueprint.plan_api_idempotency_readiness_matrix import (
    PlanApiIdempotencyReadinessMatrix,
    analyze_plan_api_idempotency_readiness_matrix,
    build_plan_api_idempotency_readiness_matrix,
    plan_api_idempotency_readiness_matrix_to_dict,
    plan_api_idempotency_readiness_matrix_to_dicts,
    plan_api_idempotency_readiness_matrix_to_markdown,
)


def test_detects_ready_payment_create_endpoint():
    result = build_plan_api_idempotency_readiness_matrix(
        _plan([
            _task(
                "pay-create",
                title="POST payment endpoint",
                description="Create payment API with Idempotency-Key, retry/backoff semantics, duplicate suppression, 409 conflict behavior, metrics, logs, and refund rollback criteria.",
            ),
            _task("cache", title="Tune cache", description="Internal cleanup."),
        ])
    )

    row = result.rows[0]
    assert isinstance(result, PlanApiIdempotencyReadinessMatrix)
    assert result.idempotency_task_ids == ("pay-create",)
    assert result.no_idempotency_task_ids == ("cache",)
    assert row.readiness == "ready"
    assert row.readiness_score == 1.0
    assert row.idempotency_key == "present"
    assert row.duplicate_suppression == "present"


def test_classifies_partial_and_blocked_rows():
    result = build_plan_api_idempotency_readiness_matrix(
        _plan([
            _task("partial", title="PATCH order endpoint", description="Order API uses idempotency key and duplicate suppression."),
            _task("blocked", title="POST order API", description="Create order endpoint has retry guidance only."),
        ])
    )

    assert [row.task_id for row in result.rows] == ["blocked", "partial"]
    assert result.rows[0].readiness == "blocked"
    assert result.rows[1].readiness == "partial"
    assert "Missing retry semantics." in result.rows[1].gaps
    assert result.summary["readiness_counts"]["blocked"] == 1
    assert result.summary["readiness_counts"]["partial"] == 1


def test_helpers_serialize_and_render_markdown():
    matrix = analyze_plan_api_idempotency_readiness_matrix(
        _plan([_task("create", title="Create checkout API", description="POST checkout with idempotency key and dedupe duplicate request handling.")])
    )
    payload = plan_api_idempotency_readiness_matrix_to_dict(matrix)

    assert json.loads(json.dumps(payload)) == payload
    assert plan_api_idempotency_readiness_matrix_to_dicts(matrix) == payload["rows"]
    markdown = plan_api_idempotency_readiness_matrix_to_markdown(matrix)
    assert "Plan API Idempotency Readiness Matrix" in markdown
    assert "create" in markdown
    assert markdown == matrix.to_markdown()


def _plan(tasks):
    return {"id": "plan-idem", "tasks": tasks, "milestones": [], "implementation_brief_id": "brief"}


def _task(task_id, *, title, description, acceptance_criteria=None, metadata=None):
    return {
        "id": task_id,
        "title": title,
        "description": description,
        "depends_on": [],
        "files_or_modules": [],
        "acceptance_criteria": acceptance_criteria or ["Done"],
        "status": "pending",
        "metadata": metadata or {},
    }
