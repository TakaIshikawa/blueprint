import json

from blueprint.plan_api_contract_compatibility_matrix import (
    PlanApiContractCompatibilityReadinessMatrix,
    build_plan_api_contract_compatibility_matrix,
    generate_plan_api_contract_compatibility_matrix,
    plan_api_contract_compatibility_matrix_to_dict,
    plan_api_contract_compatibility_matrix_to_dicts,
    plan_api_contract_compatibility_matrix_to_markdown,
)


def test_ready_contract_compatibility_plan_scores_all_rows():
    result = build_plan_api_contract_compatibility_matrix(
        _plan(
            [
                _task(
                    "task-sdk-contract",
                    title="Update SDK and GraphQL API contract compatibility",
                    description=(
                        "Ship v2 versioning with backward compatible additive fields, OpenAPI and GraphQL "
                        "schema examples, SDK consumer tests, changelog notes, and rollout guardrails with "
                        "feature flag canary monitoring and rollback criteria."
                    ),
                    metadata={"owner": "api-platform"},
                )
            ]
        )
    )

    assert isinstance(result, PlanApiContractCompatibilityReadinessMatrix)
    assert result.compatibility_task_ids == ("task-sdk-contract",)
    assert [row.area for row in result.rows] == [
        "versioning",
        "backward_compatibility",
        "schema_examples",
        "consumer_testing",
        "changelog_notes",
        "rollout_guardrails",
    ]
    assert all(row.readiness == "ready" for row in result.rows)
    assert all(row.score == 100 for row in result.rows)
    assert all(row.owner == "api-platform" for row in result.rows)


def test_partial_and_blocked_contract_compatibility_gaps_are_classified():
    partial = build_plan_api_contract_compatibility_matrix(
        _plan([_task("task-webhook", title="Webhook payload contract update", description="Add OpenAPI examples and changelog notes.")])
    )
    blocked = build_plan_api_contract_compatibility_matrix(
        _plan([_task("task-blocked", title="GraphQL schema compatibility change", description="Blocked by missing consumer inventory.")])
    )

    assert _row(partial, "versioning").readiness == "partial"
    assert _row(partial, "versioning").risk == "high"
    assert _row(partial, "schema_examples").readiness == "ready"
    assert _row(blocked, "consumer_testing").readiness == "blocked"
    assert _row(blocked, "backward_compatibility").risk == "high"


def test_contract_compatibility_serialization_markdown_and_unrelated_plan():
    result = generate_plan_api_contract_compatibility_matrix(
        _plan(
            [
                _task(
                    "task-contract | sdk",
                    title="SDK API contract | compatibility",
                    description=(
                        "Versioning, backward compatible schema examples, consumer tests, changelog notes, "
                        "and rollout guardrails."
                    ),
                )
            ]
        )
    )
    payload = plan_api_contract_compatibility_matrix_to_dict(result)
    markdown = plan_api_contract_compatibility_matrix_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert plan_api_contract_compatibility_matrix_to_dicts(result.records) == payload["records"]
    assert markdown.startswith("# Plan API Contract Compatibility Readiness Matrix: plan-compatibility")
    assert "task-contract \\| sdk" in markdown
    assert build_plan_api_contract_compatibility_matrix({"id": "empty", "tasks": []}).rows == ()
    assert build_plan_api_contract_compatibility_matrix({"id": "none", "tasks": [_task("copy", title="Update copy")]}).rows == ()


def _row(result, area):
    return next(row for row in result.rows if row.area == area)


def _plan(tasks):
    return {"id": "plan-compatibility", "implementation_brief_id": "brief", "milestones": [], "tasks": tasks}


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
