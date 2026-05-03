import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.plan_api_pagination_readiness_matrix import (
    PlanApiPaginationReadinessMatrix,
    PlanApiPaginationReadinessRow,
    build_plan_api_pagination_readiness_matrix,
    generate_plan_api_pagination_readiness_matrix,
    plan_api_pagination_readiness_matrix_to_dict,
    plan_api_pagination_readiness_matrix_to_dicts,
    plan_api_pagination_readiness_matrix_to_markdown,
)


def test_complete_pagination_plan_emits_ready_rows_with_owner_evidence_risk_and_next_action():
    result = build_plan_api_pagination_readiness_matrix(
        _plan(
            [
                _task(
                    "task-pagination-contract",
                    title="Add cursor pagination to customer list API",
                    description=(
                        "Paginate the customer API with stable ordering by created_at and id tie-breaker, "
                        "page size defaults and max page-size limits, opaque cursor token handling, "
                        "and a backwards-compatible response shape with next_cursor."
                    ),
                    acceptance_criteria=[
                        "Boundary tests cover empty, first, last, invalid token, and oversized page requests.",
                        "OpenAPI client documentation and SDK migration notes describe the new pagination flow.",
                    ],
                    metadata={"owner": "api-platform"},
                )
            ]
        )
    )

    assert isinstance(result, PlanApiPaginationReadinessMatrix)
    assert result.plan_id == "plan-pagination"
    assert result.pagination_task_ids == ("task-pagination-contract",)
    assert [row.area for row in result.rows] == [
        "stable_ordering",
        "boundary_tests",
        "page_size_limits",
        "cursor_token_handling",
        "backwards_compatible_response_shape",
        "client_documentation",
    ]
    assert all(isinstance(row, PlanApiPaginationReadinessRow) for row in result.rows)
    assert all(row.readiness == "ready" for row in result.rows)
    assert all(row.risk == "low" for row in result.rows)
    assert all(row.evidence for row in result.rows)
    assert all(row.owner == "api-platform" for row in result.rows)
    assert all(row.next_action == "Ready for API pagination handoff." for row in result.rows)
    assert result.gap_areas == ()
    assert result.summary["readiness_counts"] == {"blocked": 0, "partial": 0, "ready": 6}
    assert result.summary["risk_counts"] == {"high": 0, "medium": 0, "low": 6}


def test_incomplete_pagination_plan_marks_ordering_token_compatibility_tests_and_docs_gaps():
    result = build_plan_api_pagination_readiness_matrix(
        _plan(
            [
                _task(
                    "task-page-size",
                    title="Add pagination page size limits to orders API",
                    description="Limit orders API responses with default page size and max page_size caps.",
                    acceptance_criteria=["Endpoint accepts limit and offset parameters."],
                    metadata={"owner": "orders-api"},
                )
            ]
        )
    )

    ordering = _row(result, "stable_ordering")
    boundary = _row(result, "boundary_tests")
    token = _row(result, "cursor_token_handling")
    compatibility = _row(result, "backwards_compatible_response_shape")
    docs = _row(result, "client_documentation")

    assert ordering.gaps == ("Missing stable ordering.",)
    assert boundary.gaps == ("Missing pagination boundary tests.",)
    assert token.gaps == ("Missing cursor or token handling.",)
    assert compatibility.gaps == ("Missing backwards-compatible response shape.",)
    assert docs.gaps == ("Missing client documentation.",)
    assert ordering.risk == token.risk == compatibility.risk == "high"
    assert boundary.risk == docs.risk == "medium"
    assert all(row.readiness == "partial" for row in (ordering, boundary, token, compatibility, docs))
    assert result.gap_areas == (
        "stable_ordering",
        "boundary_tests",
        "cursor_token_handling",
        "backwards_compatible_response_shape",
        "client_documentation",
    )
    assert result.summary["risk_counts"] == {"high": 3, "medium": 2, "low": 1}
    assert "deterministic ordering" in ordering.next_action


def test_serialization_model_input_markdown_empty_invalid_iterable_object_and_no_mutation():
    plan = _plan(
        [
            _task(
                "task-page | customers",
                title="Customer API pagination | response shape",
                description=(
                    "Paginate customers with stable sort order, boundary tests, max page size, "
                    "next token cursor handling, backwards compatible response schema, and OpenAPI docs."
                ),
                metadata={"owner": "customer-api"},
            )
        ]
    )
    original = copy.deepcopy(plan)

    result = build_plan_api_pagination_readiness_matrix(ExecutionPlan.model_validate(plan))
    payload = plan_api_pagination_readiness_matrix_to_dict(result)
    markdown = plan_api_pagination_readiness_matrix_to_markdown(result)

    assert plan == original
    assert generate_plan_api_pagination_readiness_matrix(plan).to_dict() == result.to_dict()
    assert payload == result.to_dict()
    assert result.to_dicts() == payload["rows"]
    assert plan_api_pagination_readiness_matrix_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "plan_id",
        "rows",
        "records",
        "pagination_task_ids",
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
    assert markdown.startswith("# Plan API Pagination Readiness Matrix: plan-pagination")
    assert "task-page \\| customers" in markdown
    assert "Customer API pagination \\| response shape" in markdown

    empty = build_plan_api_pagination_readiness_matrix({"id": "empty-plan", "tasks": []})
    unrelated = build_plan_api_pagination_readiness_matrix(
        {"id": "unrelated-plan", "tasks": [_task("task-unrelated", title="Update billing copy")]}
    )
    invalid = build_plan_api_pagination_readiness_matrix(23)
    iterable = build_plan_api_pagination_readiness_matrix(plan["tasks"])
    object_plan = build_plan_api_pagination_readiness_matrix(_ObjectPlan("object-plan", plan["tasks"]))

    assert empty.plan_id == "empty-plan"
    assert empty.rows == ()
    assert empty.summary["ready_area_count"] == 0
    assert empty.summary["area_count"] == 0
    assert unrelated.rows == ()
    assert unrelated.pagination_task_ids == ()
    assert invalid.plan_id is None
    assert invalid.summary["task_count"] == 0
    assert iterable.plan_id is None
    assert iterable.pagination_task_ids == ("task-page | customers",)
    assert object_plan.plan_id == "object-plan"
    assert object_plan.to_dict()["rows"] == result.to_dict()["rows"]


def _row(result, area):
    return next(row for row in result.rows if row.area == area)


def _plan(tasks, *, plan_id="plan-pagination"):
    return {
        "id": plan_id,
        "implementation_brief_id": "brief-pagination",
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

