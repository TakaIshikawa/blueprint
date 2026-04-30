import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_api_contract_tests import (
    build_task_api_contract_test_plan,
    task_api_contract_test_plan_to_dict,
)


def test_detects_rest_endpoint_and_missing_contract_acceptance_criteria():
    result = build_task_api_contract_test_plan(
        _plan(
            [
                _task(
                    "task-rest",
                    title="Add REST endpoint for order status",
                    description="Implement GET /api/orders/{id}/status for mobile clients.",
                    files_or_modules=["src/api/orders.py"],
                    acceptance_criteria=[
                        "Request validation rejects malformed order ids.",
                        "Response shape includes status and updated_at fields.",
                    ],
                )
            ]
        )
    )

    assert result.api_task_ids == ("task-rest",)
    recommendation = result.recommendations[0]
    assert recommendation.contract_surfaces == ("REST endpoint /api/orders/{id}/status",)
    assert recommendation.suggested_test_types == (
        "schema",
        "request_validation",
        "response_shape",
        "error_case",
        "backward_compatibility",
    )
    assert recommendation.missing_acceptance_criteria == (
        "schema",
        "error_case",
        "backward_compatibility",
    )
    assert recommendation.risk_level == "high"


def test_detects_graphql_schema_with_schema_evidence_from_acceptance_criteria():
    result = build_task_api_contract_test_plan(
        _plan(
            [
                _task(
                    "task-graphql",
                    title="Extend GraphQL invoice schema",
                    description="Add invoice query resolver and response fields for finance.",
                    files_or_modules=["src/graphql/invoices/schema.py"],
                    acceptance_criteria=[
                        "Schema snapshot covers invoice fields.",
                        "Response shape is compatible with existing clients.",
                        "Error behavior is covered for missing invoices.",
                    ],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]
    assert recommendation.contract_surfaces == ("GraphQL schema: extend invoice",)
    assert recommendation.suggested_test_types == (
        "schema",
        "request_validation",
        "response_shape",
        "error_case",
        "backward_compatibility",
    )
    assert recommendation.missing_acceptance_criteria == ("request_validation",)
    assert recommendation.risk_level == "medium"


def test_detects_webhook_contracts_from_metadata_and_tags():
    result = build_task_api_contract_test_plan(
        _plan(
            [
                _task(
                    "task-webhook",
                    title="Handle fulfillment callback",
                    description="Persist received event payloads.",
                    metadata={"integration": {"surface": "fulfillment webhook"}},
                    tags=["webhook", "shopify"],
                    acceptance_criteria=["Schema validates the event payload."],
                )
            ]
        )
    )

    recommendation = result.recommendations[0]
    assert recommendation.contract_surfaces == (
        "Webhook payload: handle fulfillment callback",
        "External service contract: Shopify",
    )
    assert recommendation.suggested_test_types == (
        "schema",
        "request_validation",
        "response_shape",
        "error_case",
        "backward_compatibility",
    )
    assert recommendation.missing_acceptance_criteria == (
        "error_case",
        "backward_compatibility",
    )
    assert recommendation.risk_level == "high"


def test_non_api_tasks_are_ignored_without_noisy_recommendations():
    result = build_task_api_contract_test_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update onboarding copy",
                    description="Clarify the empty state in the admin dashboard.",
                    files_or_modules=["src/ui/onboarding.py"],
                    acceptance_criteria=["Copy appears in the empty state."],
                )
            ]
        )
    )

    assert result.recommendations == ()
    assert result.api_task_ids == ()
    assert result.summary == {
        "task_count": 1,
        "api_task_count": 0,
        "high_risk_count": 0,
        "medium_risk_count": 0,
        "low_risk_count": 0,
        "missing_acceptance_criteria_count": 0,
    }


def test_model_inputs_do_not_mutate_and_sort_by_risk_then_task_id_and_title():
    plan = _plan(
        [
            _task(
                "task-m",
                title="Add internal REST endpoint",
                description="POST /api/internal/rebuild accepts rebuild requests.",
                acceptance_criteria=[
                    "Request validation covers invalid payloads.",
                    "Response shape covers accepted jobs.",
                    "Error behavior is covered.",
                ],
            ),
            _task(
                "task-z",
                title="Update GraphQL account schema",
                description="Add account query fields.",
                acceptance_criteria=[
                    "Schema snapshot covers account fields.",
                    "Request variables are validated.",
                    "Response shape remains compatible.",
                    "Error behavior is covered.",
                ],
            ),
            _task(
                "task-a",
                title="Sync Stripe SDK client",
                description="Update external service client response parsing.",
                acceptance_criteria=["Request and response examples are covered."],
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_task_api_contract_test_plan(ExecutionPlan.model_validate(plan))
    payload = task_api_contract_test_plan_to_dict(result)

    assert plan == original
    assert result.api_task_ids == ("task-a", "task-m", "task-z")
    assert [item.risk_level for item in result.recommendations] == [
        "high",
        "medium",
        "low",
    ]
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["recommendations"]
    assert list(payload) == ["plan_id", "recommendations", "api_task_ids", "summary"]
    assert list(payload["recommendations"][0]) == [
        "task_id",
        "title",
        "contract_surfaces",
        "suggested_test_types",
        "missing_acceptance_criteria",
        "risk_level",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    assert payload["summary"] == {
        "task_count": 3,
        "api_task_count": 3,
        "high_risk_count": 1,
        "medium_risk_count": 1,
        "low_risk_count": 1,
        "missing_acceptance_criteria_count": 5,
    }


def _plan(tasks):
    return {
        "id": "plan-api-contracts",
        "implementation_brief_id": "brief-api-contracts",
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
    metadata=None,
    tags=None,
    depends_on=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": depends_on or [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
    }
    if metadata is not None:
        task["metadata"] = metadata
    if tags is not None:
        task["tags"] = tags
    return task
