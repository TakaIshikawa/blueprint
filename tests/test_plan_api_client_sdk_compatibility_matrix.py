import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_api_client_sdk_compatibility_matrix import (
    PlanApiClientSdkCompatibilityMatrix,
    PlanApiClientSdkCompatibilityRow,
    analyze_plan_api_client_sdk_compatibility_matrix,
    build_plan_api_client_sdk_compatibility_matrix,
    plan_api_client_sdk_compatibility_matrix_to_dict,
    plan_api_client_sdk_compatibility_matrix_to_dicts,
    plan_api_client_sdk_compatibility_matrix_to_markdown,
    summarize_plan_api_client_sdk_compatibility_matrix,
)


def test_sdk_generation_and_breaking_changes_capture_high_impact():
    result = build_plan_api_client_sdk_compatibility_matrix(
        _plan(
            [
                _task(
                    "task-sdk-breaking",
                    title="Remove deprecated user fields from API",
                    description=(
                        "Breaking change: remove legacy user fields from REST API. "
                        "Mobile clients and web clients will be affected."
                    ),
                    files_or_modules=["src/api/v2/users_endpoint.py"],
                    acceptance_criteria=[
                        "OpenAPI spec is updated.",
                        "Generated client SDK is validated.",
                    ],
                ),
                _task("task-cache", title="Tune cache", description="Adjust cache TTL."),
            ]
        )
    )

    assert isinstance(result, PlanApiClientSdkCompatibilityMatrix)
    assert isinstance(result.rows[0], PlanApiClientSdkCompatibilityRow)
    assert result.impacted_task_ids == ("task-sdk-breaking",)
    assert result.no_impact_task_ids == ("task-cache",)
    assert result.rows[0].impact == "high"
    assert "mobile" in result.rows[0].affected_clients
    assert "web" in result.rows[0].affected_clients
    assert "sdk_generation" in result.rows[0].present_safeguards
    assert len(result.rows[0].missing_safeguards) > 0
    assert any("contract test" in rec for rec in result.rows[0].recommended_validation)
    assert result.summary["impact_counts"]["high"] == 1


def test_strong_safeguards_produce_low_impact_rows():
    result = build_plan_api_client_sdk_compatibility_matrix(
        _plan(
            [
                _task(
                    "task-sdk-safe",
                    title="Add optional field to API response",
                    description=(
                        "Non-breaking change: add optional field to SDK response. "
                        "Consumer contract tests and typed client validation included."
                    ),
                    files_or_modules=["src/api/client.ts", "src/api/schema.json"],
                    acceptance_criteria=[
                        "OpenAPI spec is updated with new field.",
                        "SDK generation passes.",
                        "Consumer contract tests verify behavior.",
                        "Sample requests updated.",
                        "Typed client annotations added.",
                        "Version negotiation supports old and new clients.",
                    ],
                )
            ]
        )
    )

    row = result.rows[0]

    assert row.impact == "low"
    assert "sdk_generation" in row.present_safeguards
    assert "contract_tests" in row.present_safeguards
    assert "sample_requests" in row.present_safeguards
    assert "typed_client" in row.present_safeguards
    assert "version_negotiation" in row.present_safeguards
    assert len(row.missing_safeguards) <= 1
    assert result.summary["safeguard_coverage"]["contract_tests"] == 1
    assert result.summary["client_surface_counts"]["sdk"] >= 1


def test_mobile_and_openapi_clients_detected_from_files_and_metadata():
    result = build_plan_api_client_sdk_compatibility_matrix(
        _plan(
            [
                _task(
                    "task-mobile-sdk",
                    title="Update GraphQL schema for mobile app",
                    description="Change GraphQL schema for iOS and Android clients.",
                    files_or_modules=["schema/user.graphql", "clients/mobile/ios/api.swift"],
                    acceptance_criteria=["Deprecation window announced."],
                    metadata={"clients": ["ios", "android", "react-native"]},
                )
            ]
        )
    )

    row = result.rows[0]

    assert "mobile" in row.affected_clients
    assert "graphql_consumers" in row.affected_clients
    assert "deprecation_window" in row.present_safeguards
    assert row.impact in ("high", "medium")


def test_no_sdk_signals_returns_empty_matrix():
    result = build_plan_api_client_sdk_compatibility_matrix(
        _plan(
            [
                _task(
                    "task-internal",
                    title="Refactor internal cache logic",
                    description="Optimize internal cache implementation.",
                    files_or_modules=["src/cache/internal.py"],
                )
            ]
        )
    )

    assert result.rows == ()
    assert result.impacted_task_ids == ()
    assert result.no_impact_task_ids == ("task-internal",)
    assert result.summary["impacted_task_count"] == 0
    assert result.summary["no_impact_task_count"] == 1
    assert "No API client SDK compatibility rows were inferred." in result.to_markdown()


def test_object_input_serializes_stably_and_renders_markdown():
    plan = ExecutionPlan.model_validate(
        _plan(
            [
                _task(
                    "task-openapi",
                    title="Update OpenAPI spec for payment endpoint",
                    description=(
                        "Breaking change to payment endpoint. "
                        "SDK consumers and web clients need migration."
                    ),
                    files_or_modules=["spec/openapi.yaml"],
                    acceptance_criteria=[
                        "Consumer contract tests added.",
                        "Sample requests updated.",
                        "Deprecation window is 90 days.",
                    ],
                )
            ]
        )
    )

    result = analyze_plan_api_client_sdk_compatibility_matrix(plan)
    payload = plan_api_client_sdk_compatibility_matrix_to_dict(result)

    assert isinstance(result, PlanApiClientSdkCompatibilityMatrix)
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert list(payload) == ["plan_id", "rows", "records", "impacted_task_ids", "no_impact_task_ids", "summary"]
    assert list(payload["rows"][0]) == [
        "task_id",
        "title",
        "affected_clients",
        "present_safeguards",
        "missing_safeguards",
        "recommended_validation",
        "impact",
        "evidence",
    ]
    assert json.loads(json.dumps(payload)) == payload
    markdown = plan_api_client_sdk_compatibility_matrix_to_markdown(result)
    assert "Plan API Client SDK Compatibility Matrix" in markdown
    assert "task-openapi" in markdown
    assert markdown == result.to_markdown()


def test_dict_helpers_and_aliases_work():
    result = build_plan_api_client_sdk_compatibility_matrix(
        _plan(
            [
                _task(
                    "task-proto",
                    title="Update protobuf schema",
                    description="Add new field to gRPC protobuf schema for typed clients.",
                    files_or_modules=["proto/service.proto"],
                    acceptance_criteria=["SDK generation validated."],
                )
            ]
        )
    )

    assert summarize_plan_api_client_sdk_compatibility_matrix(result) == result.summary
    assert analyze_plan_api_client_sdk_compatibility_matrix(result) is result
    dicts = plan_api_client_sdk_compatibility_matrix_to_dicts(result)
    assert dicts == result.to_dicts()
    assert len(dicts) == 1
    assert dicts[0]["task_id"] == "task-proto"
    assert "grpc_consumers" in dicts[0]["affected_clients"]
    assert "sdk_generation" in dicts[0]["present_safeguards"]


def test_stable_sorting_high_impact_weak_rows_first():
    result = build_plan_api_client_sdk_compatibility_matrix(
        _plan(
            [
                _task(
                    "task-low",
                    title="Add optional field with all safeguards",
                    description="SDK generation, contract tests, samples, deprecation window, typed client, version negotiation.",
                    files_or_modules=["src/api/endpoint.py"],
                ),
                _task(
                    "task-high",
                    title="Breaking API change for mobile",
                    description="Remove field affecting mobile clients.",
                    files_or_modules=["src/api/mobile.py"],
                ),
                _task(
                    "task-medium",
                    title="OpenAPI schema update",
                    description="Update OpenAPI spec with SDK generation and samples.",
                    files_or_modules=["spec/openapi.yaml"],
                ),
            ]
        )
    )

    # High impact rows should come first, then sorted by task_id
    assert result.rows[0].task_id == "task-high"
    assert result.rows[0].impact == "high"
    assert result.rows[1].impact in ("medium", "low")
    assert result.rows[2].impact == "low"


def _plan(tasks):
    return {
        "id": "plan-sdk-compat",
        "implementation_brief_id": "brief-sdk",
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
):
    return {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [],
        "files_or_modules": files_or_modules or [],
        "acceptance_criteria": acceptance_criteria if acceptance_criteria is not None else ["Done"],
        "status": "pending",
        "metadata": metadata or {},
    }
