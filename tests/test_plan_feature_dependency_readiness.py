import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.plan_feature_dependency_readiness import (
    PlanFeatureDependencyReadinessMatrix,
    PlanFeatureDependencyReadinessRow,
    build_plan_feature_dependency_readiness_matrix,
    derive_plan_feature_dependency_readiness_matrix,
    extract_plan_feature_dependency_readiness_matrix,
    generate_plan_feature_dependency_readiness_matrix,
    plan_feature_dependency_readiness_matrix_to_dict,
    plan_feature_dependency_readiness_matrix_to_dicts,
    plan_feature_dependency_readiness_matrix_to_markdown,
    summarize_plan_feature_dependency_readiness_matrix,
)


def test_explicit_task_dependencies_and_dependency_language_group_readiness_rows():
    result = build_plan_feature_dependency_readiness_matrix(
        _plan(
            [
                _task("task-schema", title="Define account schema"),
                _task(
                    "task-api",
                    title="Build account API",
                    description="Depends on task-schema and shares an API contract with task-client.",
                    depends_on=["task-schema"],
                    acceptance_criteria=["The endpoint contract is frozen before task-client starts."],
                ),
                _task(
                    "task-client",
                    title="Build account client",
                    description="Blocked by task-api before the client feature branch starts.",
                    dependencies=["Build account API"],
                ),
                _task("task-copy", title="Refresh help copy", description="Update labels."),
            ]
        )
    )

    assert isinstance(result, PlanFeatureDependencyReadinessMatrix)
    assert all(isinstance(row, PlanFeatureDependencyReadinessRow) for row in result.rows)
    assert result.plan_id == "plan-feature-dependencies"
    assert result.no_dependency_task_ids == ("task-copy",)

    prereq = _row(result, "prerequisite_feature")
    assert prereq.upstream_task_ids == ("task-schema", "task-api")
    assert prereq.downstream_task_ids == ("task-api", "task-client")
    assert prereq.readiness_status == "ready"
    assert any("depends_on[0]: task-schema" == item for item in prereq.evidence)
    assert any("description: Blocked by task-api" in item for item in prereq.evidence)

    api_contract = _row(result, "shared_api_contract")
    assert api_contract.upstream_task_ids == ("task-schema", "task-api")
    assert api_contract.downstream_task_ids == ("task-api", "task-client")
    assert api_contract.readiness_status == "ready"
    assert api_contract.recommended_owner == "api_contract_owner"


def test_dependency_types_are_categorized_separately():
    result = build_plan_feature_dependency_readiness_matrix(
        _plan(
            [
                _task("task-schema", title="Shared schema foundation"),
                _task(
                    "task-api",
                    title="API contract",
                    description="API contract depends on task-schema.",
                    metadata={"shared_schema": "Payload schema follows task-schema before task-api."},
                ),
                _task(
                    "task-flag",
                    title="Feature flag",
                    description="Feature flag gate must precede task-rollout.",
                    blocked_by=["task-api"],
                ),
                _task(
                    "task-migration",
                    title="Migration",
                    description="Migration order: task-schema before task-migration.",
                ),
                _task(
                    "task-rollout",
                    title="Rollout",
                    description="Rollout order deploy after task-flag and launch before task-validate.",
                ),
                _task(
                    "task-validate",
                    title="Validation",
                    validation_plan="Validation order: run tests after task-rollout.",
                ),
            ]
        )
    )

    assert [row.dependency_type for row in result.rows] == [
        "prerequisite_feature",
        "shared_schema",
        "shared_api_contract",
        "shared_flag",
        "migration_order",
        "rollout_order",
        "validation_order",
    ]
    assert _row(result, "shared_schema").recommended_owner == "schema_owner"
    assert _row(result, "shared_api_contract").recommended_owner == "api_contract_owner"
    assert _row(result, "shared_flag").recommended_owner == "release_owner"
    assert _row(result, "migration_order").recommended_owner == "data_migration_owner"
    assert _row(result, "rollout_order").recommended_owner == "release_owner"
    assert _row(result, "validation_order").recommended_owner == "qa_owner"
    assert result.summary["dependency_type_counts"] == {
        "prerequisite_feature": 1,
        "shared_schema": 1,
        "shared_api_contract": 1,
        "shared_flag": 1,
        "migration_order": 1,
        "rollout_order": 1,
        "validation_order": 1,
    }


def test_missing_upstream_downstream_clarity_needs_clarification():
    result = build_plan_feature_dependency_readiness_matrix(
        _plan(
            [
                _task(
                    "task-dashboard",
                    title="Dashboard",
                    description="Depends on the upstream reporting feature before parallel branches begin.",
                ),
                _task(
                    "task-export",
                    title="Export",
                    dependencies=["unknown-reporting-task"],
                ),
            ]
        )
    )

    assert [row.readiness_status for row in result.rows] == ["needs_clarification"]
    row = result.rows[0]
    assert row.dependency_type == "prerequisite_feature"
    assert row.upstream_task_ids == ()
    assert row.downstream_task_ids == ("task-dashboard", "task-export")
    assert row.recommended_owner == "plan_owner"
    assert row.recommended_next_step == (
        "Clarify the upstream and downstream task ids before parallel execution starts."
    )
    assert result.summary["readiness_status_counts"] == {
        "needs_clarification": 1,
        "ready": 0,
    }


def test_empty_invalid_and_no_dependency_inputs_are_deterministic():
    empty = build_plan_feature_dependency_readiness_matrix({"id": "empty-plan", "tasks": []})
    invalid = build_plan_feature_dependency_readiness_matrix(17)
    no_dependency = build_plan_feature_dependency_readiness_matrix(
        _plan(
            [
                _task("task-api", title="Optimize API pagination"),
                _task("task-ui", title="Polish UI labels"),
            ],
            plan_id="no-deps",
        )
    )

    assert empty.to_dict() == {
        "plan_id": "empty-plan",
        "rows": [],
        "records": [],
        "no_dependency_task_ids": [],
        "summary": {
            "task_count": 0,
            "dependency_row_count": 0,
            "no_dependency_task_count": 0,
            "readiness_status_counts": {"needs_clarification": 0, "ready": 0},
            "dependency_type_counts": {
                "prerequisite_feature": 0,
                "shared_schema": 0,
                "shared_api_contract": 0,
                "shared_flag": 0,
                "migration_order": 0,
                "rollout_order": 0,
                "validation_order": 0,
            },
            "upstream_task_count": 0,
            "downstream_task_count": 0,
            "no_dependency_task_ids": [],
        },
    }
    assert empty.to_markdown() == "\n".join(
        [
            "# Plan Feature Dependency Readiness Matrix: empty-plan",
            "",
            "Summary: 0 dependency rows across 0 tasks (ready: 0, needs clarification: 0, no dependency: 0).",
            "",
            "No feature dependency readiness rows were inferred.",
        ]
    )
    assert invalid.plan_id is None
    assert invalid.rows == ()
    assert invalid.summary["dependency_row_count"] == 0
    assert no_dependency.rows == ()
    assert no_dependency.no_dependency_task_ids == ("task-api", "task-ui")
    assert "No-dependency tasks: task-api, task-ui" in no_dependency.to_markdown()


def test_serialization_aliases_model_object_inputs_markdown_escaping_and_no_mutation():
    plan = _plan(
        [
            _task("task-a | schema", title="Schema | model"),
            _task(
                "task-b",
                title="API | handler",
                description="API contract depends on task-a | schema.",
                depends_on=["Schema | model"],
            ),
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_feature_dependency_readiness_matrix(ExecutionPlan.model_validate(plan))
    payload = plan_feature_dependency_readiness_matrix_to_dict(result)
    markdown = plan_feature_dependency_readiness_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_feature_dependency_readiness_matrix(plan).to_dict() == result.to_dict()
    assert derive_plan_feature_dependency_readiness_matrix(plan).to_dict() == result.to_dict()
    assert extract_plan_feature_dependency_readiness_matrix(plan).to_dict() == result.to_dict()
    assert summarize_plan_feature_dependency_readiness_matrix(result) == result.summary
    assert plan_feature_dependency_readiness_matrix_to_dicts(result) == payload["rows"]
    assert plan_feature_dependency_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert result.to_dicts() == payload["rows"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "no_dependency_task_ids",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "dependency_type",
        "upstream_task_ids",
        "downstream_task_ids",
        "evidence",
        "readiness_status",
        "recommended_owner",
        "recommended_next_step",
    ]
    assert "task-a \\| schema" in markdown

    object_task = SimpleNamespace(
        id="task-object",
        title="Object task",
        description="Validation order: run tests after task-model.",
        acceptance_criteria=["Done"],
        status="pending",
    )
    task_model = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Model task",
            description="Feature flag must precede task-object.",
        )
    )
    object_result = build_plan_feature_dependency_readiness_matrix([task_model, object_task])

    assert _row(object_result, "shared_flag").upstream_task_ids == ("task-model",)
    assert _row(object_result, "shared_flag").downstream_task_ids == ("task-object",)
    assert _row(object_result, "validation_order").upstream_task_ids == ("task-model",)
    assert _row(object_result, "validation_order").readiness_status == "ready"


def _row(result, dependency_type):
    return next(row for row in result.rows if row.dependency_type == dependency_type)


def _plan(tasks, *, plan_id="plan-feature-dependencies"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-feature-dependencies",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    depends_on=None,
    dependencies=None,
    blocked_by=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    validation_plan=None,
):
    task = {
        "id": task_id,
        "title": title or task_id,
        "description": description or f"Implement {task_id}.",
        "depends_on": [] if depends_on is None else depends_on,
        "files_or_modules": [] if files_or_modules is None else files_or_modules,
        "acceptance_criteria": ["Done"] if acceptance_criteria is None else acceptance_criteria,
        "status": "pending",
    }
    if dependencies is not None:
        task["dependencies"] = dependencies
    if blocked_by is not None:
        task["blocked_by"] = blocked_by
    if metadata is not None:
        task["metadata"] = metadata
    if validation_plan is not None:
        task["validation_plan"] = validation_plan
    return task
