import json

from blueprint.plan_api_contract_compatibility_matrix import (
    PlanApiContractCompatibilityMatrix,
    analyze_plan_api_contract_compatibility_matrix,
    build_plan_api_contract_compatibility_matrix,
    plan_api_contract_compatibility_matrix_to_dict,
    plan_api_contract_compatibility_matrix_to_dicts,
    plan_api_contract_compatibility_matrix_to_markdown,
)


def test_detects_ready_api_sdk_graphql_webhook_contract_change():
    result = build_plan_api_contract_compatibility_matrix(_plan([
        _task(
            "contract-ready",
            "Update public API contract",
            "API endpoint, SDK, GraphQL schema, and webhook payload change uses v2 versioning, backward compatible optional fields, schema examples, consumer contract tests, changelog release notes, and canary rollout guardrails with rollback monitor.",
        ),
        _task("internal", "Refactor cache", "Internal implementation cleanup."),
    ]))

    row = result.rows[0]
    assert isinstance(result, PlanApiContractCompatibilityMatrix)
    assert result.compatibility_task_ids == ("contract-ready",)
    assert result.no_compatibility_task_ids == ("internal",)
    assert row.readiness == "ready"
    assert row.readiness_score == 1.0
    assert {"endpoint", "sdk", "graphql", "webhook", "schema"}.issubset(set(row.surfaces))


def test_reports_gaps_and_classifies_blocked_rows():
    result = build_plan_api_contract_compatibility_matrix(_plan([
        _task("partial", "Add API response field", "Endpoint response change is backward compatible with optional field and consumer tests."),
        _task("blocked", "Rename SDK field", "Breaking SDK contract change with changelog only."),
    ]))

    assert [row.task_id for row in result.rows] == ["blocked", "partial"]
    assert result.rows[0].readiness == "blocked"
    assert "Missing backward compatibility plan." in result.rows[0].gaps
    assert result.rows[1].readiness == "partial"
    assert "Missing schema examples." in result.rows[1].gaps
    assert result.summary["readiness_counts"]["blocked"] == 1


def test_helpers_return_stable_dicts_and_markdown():
    matrix = analyze_plan_api_contract_compatibility_matrix(_plan([
        _task("graphql", "GraphQL mutation change", "GraphQL schema change includes backward compatible optional field and Pact consumer contract test.")
    ]))
    payload = plan_api_contract_compatibility_matrix_to_dict(matrix)

    assert list(payload) == ["plan_id", "rows", "records", "compatibility_task_ids", "no_compatibility_task_ids", "summary"]
    assert json.loads(json.dumps(payload)) == payload
    assert plan_api_contract_compatibility_matrix_to_dicts(matrix) == payload["rows"]
    markdown = plan_api_contract_compatibility_matrix_to_markdown(matrix)
    assert "Plan API Contract Compatibility Matrix" in markdown
    assert "graphql" in markdown


def _plan(tasks):
    return {"id": "plan-contract", "tasks": tasks, "milestones": [], "implementation_brief_id": "brief"}


def _task(task_id, title, description):
    return {
        "id": task_id,
        "title": title,
        "description": description,
        "depends_on": [],
        "files_or_modules": [],
        "acceptance_criteria": ["Done"],
        "status": "pending",
        "metadata": {},
    }
