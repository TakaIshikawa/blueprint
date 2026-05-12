import json

from blueprint.plan_api_contract_compatibility_matrix import (
    PlanApiContractCompatibilityMatrix,
    analyze_plan_api_contract_compatibility_matrix,
    build_plan_api_contract_compatibility_matrix,
    plan_api_contract_compatibility_matrix_to_dict,
    plan_api_contract_compatibility_matrix_to_dicts,
    plan_api_contract_compatibility_matrix_to_markdown,
    summarize_plan_api_contract_compatibility_matrix,
)


def test_detects_compatibility_sensitive_api_sdk_graphql_and_webhook_tasks():
    result = build_plan_api_contract_compatibility_matrix(
        {
            "id": "plan-contract",
            "tasks": [
                {
                    "id": "sdk-contract",
                    "title": "SDK endpoint contract update",
                    "description": "Additive v2 API version keeps backward compatibility.",
                    "acceptance_criteria": [
                        "OpenAPI schema examples and sample payloads are included.",
                        "Consumer contract tests and SDK integration tests pass.",
                        "Changelog notes and migration guide are published.",
                        "Canary rollout guardrails monitor errors with rollback.",
                    ],
                },
                {"id": "ui-copy", "title": "Refresh labels", "description": "Copy-only UI change."},
            ],
        }
    )

    row = result.rows[0]
    assert isinstance(result, PlanApiContractCompatibilityMatrix)
    assert result.compatibility_task_ids == ("sdk-contract",)
    assert result.no_compatibility_task_ids == ("ui-copy",)
    assert row.readiness == "ready"
    assert row.readiness_score == 1.0
    assert row.gaps == ()
    assert {"endpoint", "sdk", "schema"}.issubset(set(row.surfaces))


def test_detects_ready_api_sdk_graphql_webhook_contract_change():
    result = build_plan_api_contract_compatibility_matrix(
        _plan(
            [
                _task(
                    "contract-ready",
                    "Update public API contract",
                    "API endpoint, SDK, GraphQL schema, and webhook payload change uses v2 versioning, backward compatible optional fields, schema examples, consumer contract tests, changelog release notes, and canary rollout guardrails with rollback monitor.",
                ),
                _task("internal", "Refactor cache", "Internal implementation cleanup."),
            ]
        )
    )

    row = result.rows[0]
    assert result.compatibility_task_ids == ("contract-ready",)
    assert result.no_compatibility_task_ids == ("internal",)
    assert row.readiness == "ready"
    assert {"endpoint", "sdk", "graphql", "webhook", "schema"}.issubset(set(row.surfaces))


def test_reports_blocked_and_partial_compatibility_gaps():
    result = build_plan_api_contract_compatibility_matrix(
        {
            "tasks": [
                {"id": "blocked", "title": "GraphQL schema breaking change", "description": "Update clients and docs."},
                {
                    "id": "partial",
                    "title": "Webhook contract version",
                    "description": "Versioned webhook contract is backward compatible with OpenAPI schema examples.",
                    "acceptance_criteria": ["Consumer tests and changelog notes are complete."],
                },
            ]
        }
    )

    assert [row.task_id for row in result.rows] == ["blocked", "partial"]
    assert result.rows[0].readiness == "blocked"
    assert "Missing backward compatibility plan." in result.rows[0].gaps
    assert result.rows[1].readiness == "partial"
    assert result.rows[1].gaps == ("Missing rollout guardrails.",)
    assert result.summary["readiness_counts"]["blocked"] == 1


def test_helpers_return_stable_dicts_and_markdown():
    result = build_plan_api_contract_compatibility_matrix(
        {
            "id": "plan|contract",
            "tasks": [
                {
                    "id": "task|contract",
                    "title": "API|SDK contract",
                    "description": "Versioned backward compatible OpenAPI schema examples consumer tests changelog rollout guardrails.",
                }
            ],
        }
    )
    payload = plan_api_contract_compatibility_matrix_to_dict(result)

    assert analyze_plan_api_contract_compatibility_matrix(result).to_dict() == payload
    assert summarize_plan_api_contract_compatibility_matrix(result) == result.summary
    assert list(payload) == ["plan_id", "rows", "records", "compatibility_task_ids", "no_compatibility_task_ids", "summary"]
    assert json.loads(json.dumps(payload)) == payload
    assert plan_api_contract_compatibility_matrix_to_dicts(result) == payload["rows"]
    markdown = plan_api_contract_compatibility_matrix_to_markdown(result)
    assert "Plan API Contract Compatibility Matrix" in markdown
    assert "task\\|contract" in markdown
    assert "API\\|SDK" in markdown


def test_analyze_accepts_plan_like_payloads():
    matrix = analyze_plan_api_contract_compatibility_matrix(
        _plan(
            [
                _task(
                    "graphql",
                    "GraphQL mutation change",
                    "GraphQL schema change includes backward compatible optional field and Pact consumer contract test.",
                )
            ]
        )
    )

    assert matrix.plan_id == "plan-contract"
    assert matrix.compatibility_task_ids == ("graphql",)


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
