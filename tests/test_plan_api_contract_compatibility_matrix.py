import json

from blueprint.plan_api_contract_compatibility_matrix import (
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

    assert result.compatibility_task_ids == ("sdk-contract",)
    assert result.no_compatibility_task_ids == ("ui-copy",)
    assert result.rows[0].readiness == "ready"
    assert result.rows[0].gaps == ()


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


def test_dict_markdown_and_helper_contracts_are_stable():
    result = build_plan_api_contract_compatibility_matrix(
        {"id": "plan|contract", "tasks": [{"id": "task|contract", "title": "API|SDK contract", "description": "Versioned backward compatible OpenAPI schema examples consumer tests changelog rollout guardrails."}]}
    )
    payload = plan_api_contract_compatibility_matrix_to_dict(result)

    assert analyze_plan_api_contract_compatibility_matrix(result).to_dict() == payload
    assert summarize_plan_api_contract_compatibility_matrix(result) == result.summary
    assert plan_api_contract_compatibility_matrix_to_dicts(result) == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    markdown = plan_api_contract_compatibility_matrix_to_markdown(result)
    assert "task\\|contract" in markdown
    assert "API\\|SDK" in markdown
