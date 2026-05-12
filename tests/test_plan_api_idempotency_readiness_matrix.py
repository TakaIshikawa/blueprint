import json

from blueprint.plan_api_idempotency_readiness_matrix import (
    analyze_plan_api_idempotency_readiness_matrix,
    build_plan_api_idempotency_readiness_matrix,
    derive_plan_api_idempotency_readiness_matrix,
    extract_plan_api_idempotency_readiness_matrix,
    generate_plan_api_idempotency_readiness_matrix,
    plan_api_idempotency_readiness_matrix_to_dict,
    plan_api_idempotency_readiness_matrix_to_dicts,
    plan_api_idempotency_readiness_matrix_to_markdown,
    summarize_plan_api_idempotency_readiness_matrix,
)


def test_detects_api_idempotency_work_and_ready_row():
    result = build_plan_api_idempotency_readiness_matrix(
        _plan(
            [
                _task(
                    "create-order",
                    "POST order creation endpoint",
                    "Create orders through a POST /orders API endpoint.",
                    [
                        "Require an Idempotency-Key header and client token.",
                        "Retry semantics use exponential backoff for transient failures.",
                        "Duplicate request suppression returns the same cached response.",
                        "409 conflict response covers mismatched payloads.",
                        "Monitoring dashboard, logs, and alerts track idempotency collisions.",
                        "Rollback criteria disable the new write path with a kill switch.",
                    ],
                ),
                _task("copy", "Update help copy", "Refresh labels only.", []),
            ]
        )
    )

    assert result.plan_id == "plan-idempotency"
    assert result.idempotency_task_ids == ("create-order",)
    assert result.no_idempotency_task_ids == ("copy",)
    row = result.rows[0]
    assert row.readiness == "ready"
    assert row.readiness_score == 1.0
    assert row.gaps == ()
    assert any("Idempotency-Key" in item for item in row.evidence)


def test_classifies_partial_and_blocked_rows_with_gaps():
    result = build_plan_api_idempotency_readiness_matrix(
        _plan(
            [
                _task(
                    "partial-payment",
                    "PUT payment retry endpoint",
                    "PUT payment API supports idempotency key, retry backoff, duplicate suppression, and 409 conflict handling.",
                    ["Telemetry logs duplicate requests."],
                ),
                _task(
                    "blocked-checkout",
                    "POST checkout creation flow",
                    "POST checkout endpoint retries after transient API failures.",
                    ["Observability dashboard exists but duplicate handling is TBD."],
                ),
            ]
        )
    )

    assert [row.task_id for row in result.rows] == ["blocked-checkout", "partial-payment"]
    assert result.rows[0].readiness == "blocked"
    assert "Missing idempotency key contract." in result.rows[0].gaps
    assert result.rows[1].readiness == "partial"
    assert result.rows[1].gaps == ("Missing rollback criteria.",)
    assert result.summary["readiness_counts"] == {"blocked": 1, "partial": 1, "ready": 0}


def test_serialization_markdown_and_helper_aliases_are_stable():
    result = build_plan_api_idempotency_readiness_matrix(
        _plan([_task("task|id", "POST order|payment API", "Idempotency key, duplicate suppression, retries, conflict response, monitoring, and rollback.", [])])
    )
    payload = plan_api_idempotency_readiness_matrix_to_dict(result)

    assert generate_plan_api_idempotency_readiness_matrix(result).to_dict() == payload
    assert analyze_plan_api_idempotency_readiness_matrix(result).to_dict() == payload
    assert derive_plan_api_idempotency_readiness_matrix(result).to_dict() == payload
    assert extract_plan_api_idempotency_readiness_matrix(result).to_dict() == payload
    assert summarize_plan_api_idempotency_readiness_matrix(result) == result.summary
    assert plan_api_idempotency_readiness_matrix_to_dicts(result) == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    markdown = plan_api_idempotency_readiness_matrix_to_markdown(result)
    assert "task\\|id" in markdown
    assert "order\\|payment" in markdown


def _plan(tasks):
    return {"id": "plan-idempotency", "tasks": tasks}


def _task(task_id, title, description, acceptance_criteria):
    return {
        "id": task_id,
        "title": title,
        "description": description,
        "acceptance_criteria": acceptance_criteria,
    }
