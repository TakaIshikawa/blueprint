import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_api_error_taxonomy_readiness_matrix import (
    PlanApiErrorTaxonomyReadinessMatrix,
    PlanApiErrorTaxonomyReadinessRow,
    build_plan_api_error_taxonomy_readiness_matrix,
    generate_plan_api_error_taxonomy_readiness_matrix,
    plan_api_error_taxonomy_readiness_matrix_to_dict,
    plan_api_error_taxonomy_readiness_matrix_to_dicts,
    plan_api_error_taxonomy_readiness_matrix_to_markdown,
)


def test_complete_error_taxonomy_plan_emits_ready_rows_with_owner_evidence_risk_and_next_action():
    result = build_plan_api_error_taxonomy_readiness_matrix(
        _plan(
            [
                _task(
                    "task-error-taxonomy",
                    title="Add standardized API error taxonomy with stable error codes",
                    description=(
                        "Define stable error codes and error enums for all payment error conditions, "
                        "map error codes to appropriate HTTP status codes (400, 401, 403, 404, 422, 500), "
                        "provide field-level validation errors with field paths and parameter error details, "
                        "indicate retryability for each error type with retry-after guidance and backoff strategy, "
                        "implement RFC 7807 problem details with correlation IDs and structured error metadata."
                    ),
                    acceptance_criteria=[
                        "Error documentation and API docs cover all error codes, HTTP status mappings, and client error handling with SDK error examples.",
                    ],
                    metadata={"owner": "payments-api"},
                )
            ]
        )
    )

    assert isinstance(result, PlanApiErrorTaxonomyReadinessMatrix)
    assert result.plan_id == "plan-error-taxonomy"
    assert result.error_taxonomy_task_ids == ("task-error-taxonomy",)
    assert [row.area for row in result.rows] == [
        "error_codes",
        "http_status_mapping",
        "validation_errors",
        "retryable_errors",
        "machine_readable_details",
        "documentation",
    ]
    assert all(isinstance(row, PlanApiErrorTaxonomyReadinessRow) for row in result.rows)
    assert all(row.readiness == "ready" for row in result.rows)
    assert all(row.risk == "low" for row in result.rows)
    assert all(row.evidence for row in result.rows)
    assert all(row.owner == "payments-api" for row in result.rows)
    assert all(row.next_action == "Ready for API error taxonomy handoff." for row in result.rows)
    assert result.gap_areas == ()
    assert result.summary["readiness_counts"] == {"blocked": 0, "partial": 0, "ready": 6}
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 6}


def test_incomplete_error_taxonomy_plan_marks_codes_status_validation_retryable_details_docs_gaps():
    result = build_plan_api_error_taxonomy_readiness_matrix(
        _plan(
            [
                _task(
                    "task-http-status",
                    title="Add HTTP status codes to error responses",
                    description="Return appropriate HTTP status codes for API errors.",
                    acceptance_criteria=["Endpoints return 400 for bad requests and 500 for internal errors."],
                    metadata={"owner": "api-platform"},
                )
            ]
        )
    )

    error_codes = _row(result, "error_codes")
    validation = _row(result, "validation_errors")
    retryable = _row(result, "retryable_errors")
    details = _row(result, "machine_readable_details")
    docs = _row(result, "documentation")

    assert error_codes.gaps == ("Missing stable error codes.",)
    assert validation.gaps == ("Missing validation error structure.",)
    assert retryable.gaps == ("Missing retryability indicators.",)
    assert details.gaps == ("Missing machine-readable error details.",)
    assert docs.gaps == ("Missing error documentation.",)
    assert error_codes.risk == details.risk == "high"
    assert validation.risk == retryable.risk == docs.risk == "medium"
    assert all(row.readiness == "partial" for row in (error_codes, validation, retryable, details, docs))
    assert result.gap_areas == (
        "error_codes",
        "validation_errors",
        "retryable_errors",
        "machine_readable_details",
        "documentation",
    )
    assert result.summary["risk_counts"] == {"high": 2, "medium": 3, "low": 1}
    assert "stable, machine-readable error codes" in error_codes.next_action


def test_serialization_model_input_markdown_empty_invalid_iterable_object_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-error | taxonomy",
                title="API error codes | taxonomy and status",
                description=(
                    "Define stable error codes, map to HTTP status codes (400, 401, 403, 404, 422, 500), "
                    "provide field-level validation errors, indicate retryability with retry-after, "
                    "implement RFC 7807 problem details with correlation IDs, and document error handling."
                ),
                metadata={"owner": "error-api"},
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_api_error_taxonomy_readiness_matrix(ExecutionPlan.model_validate(plan))
    payload = plan_api_error_taxonomy_readiness_matrix_to_dict(result)
    markdown = plan_api_error_taxonomy_readiness_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_api_error_taxonomy_readiness_matrix(plan).to_dict() == result.to_dict()
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert plan_api_error_taxonomy_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "error_taxonomy_task_ids",
        "gap_areas",
        "summary",
    ]
    assert list(payload["rows"][0]) == [
        "area",
        "owner",
        "evidence",
        "gaps",
        "readiness",
        "risk",
        "next_action",
        "task_ids",
    ]
    assert markdown.startswith("# Plan API Error Taxonomy Readiness Matrix: plan-error-taxonomy")
    assert "task-error \\| taxonomy" in markdown
    assert "API error codes \\| taxonomy and status" in markdown

    empty = build_plan_api_error_taxonomy_readiness_matrix({"id": "empty-plan", "tasks": []})
    unrelated = build_plan_api_error_taxonomy_readiness_matrix(
        {"id": "unrelated-plan", "tasks": [_task("task-unrelated", title="Update billing copy")]}
    )
    invalid = build_plan_api_error_taxonomy_readiness_matrix(23)
    iterable = build_plan_api_error_taxonomy_readiness_matrix(plan["tasks"])
    object_plan = build_plan_api_error_taxonomy_readiness_matrix(_ObjectPlan("object-plan", plan["tasks"]))

    assert empty.plan_id == "empty-plan"
    assert empty.rows == ()
    assert empty.summary["ready_area_count"] == 0
    assert empty.summary["area_count"] == 0
    assert unrelated.rows == ()
    assert unrelated.error_taxonomy_task_ids == ()
    assert invalid.plan_id is None
    assert invalid.summary["task_count"] == 0
    assert iterable.plan_id is None
    assert iterable.error_taxonomy_task_ids == ("task-error | taxonomy",)
    assert object_plan.plan_id == "object-plan"
    assert object_plan.to_dict()["rows"] == result.to_dict()["rows"]


def _row(result, area):
    return next(row for row in result.rows if row.area == area)


def _plan(tasks, *, plan_id="plan-error-taxonomy"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-error-taxonomy",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id,
    *,
    title=None,
    description=None,
    acceptance_criteria=None,
    metadata=None,
):
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


class _ObjectPlan:
    def __init__(self, plan_id, tasks):
        self.id = plan_id
        self.tasks = tasks
