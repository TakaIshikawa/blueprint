import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_api_pagination_impact import (
    TaskApiPaginationImpactPlan,
    TaskApiPaginationImpactRecord,
    build_task_api_pagination_impact_plan,
    derive_task_api_pagination_impact_plan,
    summarize_task_api_pagination_impact,
    summarize_task_api_pagination_impacts,
    task_api_pagination_impact_plan_to_dict,
    task_api_pagination_impact_plan_to_markdown,
)


def test_detects_pagination_signals_from_text_metadata_and_expected_paths():
    result = build_task_api_pagination_impact_plan(
        _plan(
            [
                _task(
                    "task-cursor",
                    title="Cursor pagination for public orders API",
                    description=(
                        "Add cursor pagination to the public API list endpoint for existing clients "
                        "without breaking deployed cursors."
                    ),
                    files_or_modules=["src/api/routes/orders_list.py"],
                    acceptance_criteria=[
                        "Use stable sort order by created_at and id tie-breaker.",
                        "Keep backwards compatibility for old cursor values.",
                    ],
                ),
                _task(
                    "task-offset",
                    title="Admin offset listing",
                    description="Add limit and offset pagination to the admin list users endpoint.",
                    metadata={
                        "pagination_signals": ["offset_pagination", "page_size_limit"],
                        "pagination_safeguards": ["max_page_size_handling"],
                    },
                ),
                _task(
                    "task-token",
                    title="Feed backend next_token",
                    description="Support next page token for infinite scroll load more backend.",
                    files_or_modules=["src/feed/infinite_scroll_backend.py"],
                    acceptance_criteria=["Empty results return no next token."],
                ),
                _task(
                    "task-bulk",
                    title="Bulk listing API export",
                    description="Build a bulk listing API for data export of all records.",
                    files_or_modules=["src/api/routes/bulk_export.py"],
                ),
                _task(
                    "task-internal",
                    title="Internal pagination helper",
                    description="Refactor a pagination utility for report generation.",
                    files_or_modules=["src/internal/pagination_helper.py"],
                ),
                _task("task-copy", title="Update API copy", description="Clarify settings text."),
            ]
        )
    )

    assert isinstance(result, TaskApiPaginationImpactPlan)
    assert result.plan_id == "plan-pagination"
    assert result.impacted_task_ids == (
        "task-bulk",
        "task-cursor",
        "task-offset",
        "task-token",
        "task-internal",
    )
    assert result.no_impact_task_ids == ("task-copy",)
    assert result.summary["impact_level_counts"] == {"high": 2, "medium": 2, "low": 1}
    assert result.summary["signal_counts"]["cursor_pagination"] >= 1
    assert result.summary["signal_counts"]["offset_pagination"] == 1
    assert result.summary["signal_counts"]["page_size_limit"] == 1
    assert result.summary["signal_counts"]["next_token"] == 1
    assert result.summary["signal_counts"]["infinite_scroll"] == 1
    assert result.summary["signal_counts"]["bulk_listing"] == 1

    cursor = _record(result, "task-cursor")
    assert cursor.impact_level == "high"
    assert {"list_endpoint", "cursor_pagination"} <= set(cursor.matched_signals)
    assert "stable_sort_order" not in cursor.missing_safeguards
    assert "backwards_compatibility" not in cursor.missing_safeguards
    assert "cursor_compatibility" not in cursor.missing_safeguards

    offset = _record(result, "task-offset")
    assert offset.impact_level == "medium"
    assert {"list_endpoint", "offset_pagination", "page_size_limit"} <= set(offset.matched_signals)
    assert "max_page_size_handling" not in offset.missing_safeguards

    token = _record(result, "task-token")
    assert {"next_token", "infinite_scroll"} <= set(token.matched_signals)
    assert "empty_page_behavior" not in token.missing_safeguards

    assert _record(result, "task-bulk").impact_level == "high"
    assert _record(result, "task-internal").impact_level == "low"


def test_recommended_checks_cover_required_pagination_safeguards():
    result = build_task_api_pagination_impact_plan(
        _task(
            "task-checks",
            title="List endpoint page size boundaries",
            description="Add page size limits for a REST API collection endpoint.",
        )
    )
    record = result.records[0]
    checks = " ".join(record.recommended_checks)

    assert "stable, deterministic ordering" in checks
    assert "cursor or next-token compatibility" in checks
    assert "page-size boundary behavior" in checks
    assert "empty result sets" in checks
    assert "compatible for existing clients" in checks
    assert "first page, middle page, last page" in checks
    assert record.missing_safeguards == (
        "stable_sort_order",
        "cursor_compatibility",
        "max_page_size_handling",
        "empty_page_behavior",
        "backwards_compatibility",
        "boundary_page_tests",
    )


def test_metadata_model_aliases_serialization_and_no_source_mutation():
    task = _task(
        "task-model",
        title="Partner API next token",
        description="Add page token support to a partner API endpoint.",
        metadata={
            "pagination_signals": ["next_token", "cursor"],
            "validation_commands": {
                "test": ["poetry run pytest tests/api/test_partner_next_page_token.py"]
            },
        },
    )
    original = copy.deepcopy(task)
    model = ExecutionTask.model_validate(task)
    plan_model = ExecutionPlan.model_validate(_plan([model.model_dump(mode="python")], plan_id="plan-model"))

    mapping_result = build_task_api_pagination_impact_plan(task)
    model_result = summarize_task_api_pagination_impact(model)
    plural_result = summarize_task_api_pagination_impacts(model)
    derived = derive_task_api_pagination_impact_plan(plan_model)
    payload = task_api_pagination_impact_plan_to_dict(model_result)

    assert task == original
    assert mapping_result.records[0].matched_signals == model_result.records[0].matched_signals
    assert plural_result.to_dict() == model_result.to_dict()
    assert derived.plan_id == "plan-model"
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.to_dicts() == payload["records"]
    assert model_result.findings == model_result.records
    assert list(payload) == [
        "plan_id",
        "records",
        "impacted_task_ids",
        "no_impact_task_ids",
        "summary",
    ]
    assert list(payload["records"][0]) == [
        "task_id",
        "title",
        "matched_signals",
        "impact_level",
        "missing_safeguards",
        "recommended_checks",
        "evidence",
    ]
    assert any("validation_commands:" in item for item in model_result.records[0].evidence)


def test_markdown_escapes_pipes_and_output_order_is_stable():
    result = build_task_api_pagination_impact_plan(
        _plan(
            [
                _task(
                    "task-z-low",
                    title="Internal cursor helper | reports",
                    description="Refactor internal cursor pagination helper.",
                ),
                _task(
                    "task-a-high",
                    title="Public API cursor | migration",
                    description="Migrate public API cursor pagination for existing clients.",
                ),
                _task(
                    "task-m-medium",
                    title="REST page size",
                    description="Add page size limit to REST API list endpoint.",
                ),
            ]
        )
    )

    payload = task_api_pagination_impact_plan_to_dict(result)
    markdown = task_api_pagination_impact_plan_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task API Pagination Impact Plan: plan-pagination")
    assert (
        "| Task | Impact | Matched Signals | Missing Safeguards | Recommended Checks | Evidence |"
        in markdown
    )
    assert "Public API cursor \\| migration" in markdown
    assert [record.impact_level for record in result.records] == ["high", "medium", "low"]


def test_invalid_empty_object_like_and_no_impact_inputs_are_supported():
    object_task = SimpleNamespace(
        id="task-object",
        title="GraphQL connection cursor",
        description="Add cursor pagination to a GraphQL connection endpoint.",
        files_or_modules=["src/api/graphql/connections.py"],
        acceptance_criteria=["Boundary page tests cover first page and last page."],
        status="pending",
    )

    empty = build_task_api_pagination_impact_plan({"id": "plan-empty", "tasks": []})
    invalid = build_task_api_pagination_impact_plan(13)
    no_impact = build_task_api_pagination_impact_plan(
        _plan([_task("task-ui", title="Profile UI", description="Render profile settings.")])
    )
    object_result = build_task_api_pagination_impact_plan([object_task])

    assert empty.plan_id == "plan-empty"
    assert empty.records == ()
    assert invalid.records == ()
    assert no_impact.records == ()
    assert no_impact.no_impact_task_ids == ("task-ui",)
    assert "No API pagination impact records were inferred." in no_impact.to_markdown()
    assert isinstance(object_result.records[0], TaskApiPaginationImpactRecord)
    assert object_result.records[0].task_id == "task-object"
    assert "boundary_page_tests" not in object_result.records[0].missing_safeguards


def _record(result, task_id):
    return next(record for record in result.records if record.task_id == task_id)


def _plan(tasks, plan_id="plan-pagination"):
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
    depends_on=None,
    files_or_modules=None,
    acceptance_criteria=None,
    metadata=None,
    test_command=None,
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
    if metadata is not None:
        task["metadata"] = metadata
    if test_command is not None:
        task["test_command"] = test_command
    return task
