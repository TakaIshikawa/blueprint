import copy
import json
from types import SimpleNamespace
from typing import Any

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_api_cursor_pagination_readiness import (
    TaskApiCursorPaginationReadinessFinding,
    TaskApiCursorPaginationReadinessPlan,
    analyze_task_api_cursor_pagination_readiness,
    build_task_api_cursor_pagination_readiness_plan,
    extract_task_api_cursor_pagination_readiness,
    generate_task_api_cursor_pagination_readiness,
    summarize_task_api_cursor_pagination_readiness,
    task_api_cursor_pagination_readiness_plan_to_dict,
    task_api_cursor_pagination_readiness_plan_to_dicts,
    task_api_cursor_pagination_readiness_plan_to_markdown,
)


def test_weak_sorting_first_deterministic_ordering():
    result = build_task_api_cursor_pagination_readiness_plan(
        _plan(
            [
                _task(
                    "task-weak",
                    title="Add cursor pagination",
                    description="Implement cursor-based pagination with cursor tokens and next links.",
                    files_or_modules=["src/api/cursor_pagination.py"],
                ),
                _task(
                    "task-partial",
                    title="Improve cursor pagination",
                    description=(
                        "Add cursor pagination with stable ordering, deterministic sort tests, "
                        "cursor expiry handling, and cursor documentation."
                    ),
                ),
                _task(
                    "task-strong",
                    title="Complete cursor pagination",
                    description=(
                        "Implement cursor-based pagination with cursor tokens, next/previous links, "
                        "stable ordering, page size limits, filtering interaction, deleted row handling, "
                        "and backward pagination support. Add deterministic sort tests with tie-breakers, "
                        "cursor expiry handling with TTL, malformed cursor tests with validation, "
                        "max page size enforcement with bounded limits, and comprehensive cursor documentation."
                    ),
                ),
            ]
        )
    )

    assert isinstance(result, TaskApiCursorPaginationReadinessPlan)
    assert result.plan_id == "plan-cursor-pagination"
    assert result.cursor_pagination_task_ids == ("task-weak", "task-partial", "task-strong")
    assert len(result.findings) == 3
    assert result.findings[0].task_id == "task-weak"
    assert result.findings[0].readiness == "weak"
    assert result.findings[1].task_id == "task-partial"
    assert result.findings[1].readiness == "partial"
    assert result.findings[2].task_id == "task-strong"
    assert result.findings[2].readiness == "strong"


def test_strong_coverage_has_no_actionable_remediations():
    result = analyze_task_api_cursor_pagination_readiness(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Add cursor-based pagination API",
                    description=(
                        "Implement cursor-based pagination for list endpoints. Use opaque cursor tokens "
                        "with continuation token encoding. Return next and previous links in response. "
                        "Define stable ordering with deterministic sort using created_at and tie-breaker on id. "
                        "Enforce page size limits with default, minimum, and maximum page size caps. "
                        "Support filtering interaction with cursor and filter parameters. "
                        "Handle deleted row scenarios with tombstones. Support backward pagination with "
                        "before cursor. Add deterministic sort tests verifying tie-breakers prevent duplicates. "
                        "Define cursor expiry handling with cursor TTL and expired token error responses. "
                        "Add malformed cursor tests for invalid cursor validation. "
                        "Enforce max page size with bounded limit validation. "
                        "Document cursor pagination usage, encoding, expiry, and client integration examples."
                    ),
                    files_or_modules=["src/api/pagination/cursor_pagination.py"],
                )
            ]
        )
    )

    assert isinstance(result, TaskApiCursorPaginationReadinessPlan)
    assert result.plan_id == "plan-cursor-pagination"
    assert result.cursor_pagination_task_ids == ("task-complete",)
    finding = result.findings[0]
    assert isinstance(finding, TaskApiCursorPaginationReadinessFinding)
    assert finding.detected_signals == (
        "cursor_token",
        "next_previous_links",
        "stable_ordering",
        "page_size_limits",
        "filtering_interaction",
        "deleted_row_handling",
        "backward_pagination",
    )
    assert finding.present_safeguards == (
        "deterministic_sort_tests",
        "cursor_expiry_handling",
        "malformed_cursor_tests",
        "max_page_size_enforcement",
        "cursor_documentation",
    )
    assert finding.missing_safeguards == ()
    assert finding.actionable_remediations == ()
    assert finding.actionable_gaps == ()
    assert finding.readiness == "strong"
    assert "files_or_modules: src/api/pagination/cursor_pagination.py" in finding.evidence
    assert result.summary["cursor_pagination_task_count"] == 1
    assert result.summary["missing_safeguard_count"] == 0
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "strong": 1}


def test_partial_cursor_pagination_task_reports_specific_remediations():
    result = build_task_api_cursor_pagination_readiness_plan(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Add cursor pagination to products API",
                    description=(
                        "Add cursor-based pagination with cursor tokens and next links. "
                        "Define stable ordering using created_at. Add deterministic sort tests. "
                        "Document cursor usage."
                    ),
                    files_or_modules=["src/api/products/cursor.py"],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert finding.task_id == "task-partial"
    assert finding.detected_signals == (
        "cursor_token",
        "next_previous_links",
        "stable_ordering",
    )
    assert finding.present_safeguards == ("deterministic_sort_tests", "cursor_documentation")
    assert finding.missing_safeguards == (
        "cursor_expiry_handling",
        "malformed_cursor_tests",
        "max_page_size_enforcement",
    )
    assert finding.readiness == "partial"
    assert finding.actionable_remediations == (
        "Define cursor or token expiry handling, TTL behavior, and expired-cursor error responses.",
        "Add tests for malformed, invalid, corrupt, or tampered cursor tokens with proper validation and error responses.",
        "Enforce maximum page size limits, validate page size parameters, and return bounded limit errors for oversized requests.",
    )
    assert result.summary["missing_safeguard_counts"]["cursor_expiry_handling"] == 1
    assert result.summary["present_safeguard_counts"]["deterministic_sort_tests"] == 1


def test_path_hints_contribute_to_detection():
    result = build_task_api_cursor_pagination_readiness_plan(
        _plan(
            [
                _task(
                    "task-paths",
                    title="Wire pagination middleware",
                    description="Add middleware and tests.",
                    files_or_modules=[
                        "src/api/cursor/routes.py",
                        "src/pagination/cursor_token_encoder.py",
                        "src/api/next_link_builder.py",
                        "src/sorting/deterministic_order.py",
                        "src/api/page_size_limits.py",
                        "src/filters/query_param_parser.py",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert {
        "cursor_token",
        "next_previous_links",
        "stable_ordering",
        "page_size_limits",
        "filtering_interaction",
    } <= set(finding.detected_signals)
    assert "files_or_modules: src/api/cursor/routes.py" in finding.evidence
    assert "files_or_modules: src/pagination/cursor_token_encoder.py" in finding.evidence


def test_unrelated_and_explicit_no_impact_tasks_are_not_applicable():
    result = build_task_api_cursor_pagination_readiness_plan(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update API docs copy",
                    description="Adjust helper text for endpoint descriptions.",
                    files_or_modules=["src/api/docs.py"],
                ),
                _task(
                    "task-no-cursor",
                    title="Refactor user model",
                    description="No cursor pagination, pagination, or cursor changes are required.",
                ),
            ]
        )
    )

    assert result.findings == ()
    assert result.records == ()
    assert result.cursor_pagination_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy", "task-no-cursor")
    assert result.to_dicts() == []
    assert result.summary == {
        "total_task_count": 2,
        "cursor_pagination_task_count": 0,
        "not_applicable_task_ids": ["task-copy", "task-no-cursor"],
        "missing_safeguard_count": 0,
        "readiness_counts": {"weak": 0, "partial": 0, "strong": 0},
        "signal_counts": {
            "cursor_token": 0,
            "next_previous_links": 0,
            "stable_ordering": 0,
            "page_size_limits": 0,
            "filtering_interaction": 0,
            "deleted_row_handling": 0,
            "backward_pagination": 0,
        },
        "present_safeguard_counts": {
            "deterministic_sort_tests": 0,
            "cursor_expiry_handling": 0,
            "malformed_cursor_tests": 0,
            "max_page_size_enforcement": 0,
            "cursor_documentation": 0,
        },
        "missing_safeguard_counts": {
            "deterministic_sort_tests": 0,
            "cursor_expiry_handling": 0,
            "malformed_cursor_tests": 0,
            "max_page_size_enforcement": 0,
            "cursor_documentation": 0,
        },
    }


def test_mapping_and_object_inputs():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Cursor pagination observability",
                description="Add cursor pagination metrics, logs, and tests.",
            ),
            _task(
                "task-a",
                title="Cursor token handling",
                description=(
                    "Cursor-based pagination uses opaque cursor tokens with deterministic sort tests, "
                    "cursor expiry handling, malformed cursor tests, max page size enforcement, "
                    "and cursor documentation."
                ),
                metadata={"cursor_token": "Base64-encoded opaque tokens"},
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = summarize_task_api_cursor_pagination_readiness(model)
    payload = task_api_cursor_pagination_readiness_plan_to_dict(result)
    task_result = build_task_api_cursor_pagination_readiness_plan(ExecutionTask.model_validate(plan["tasks"][1]))
    object_result = build_task_api_cursor_pagination_readiness_plan(
        SimpleNamespace(
            id="task-object",
            title="Add cursor pagination",
            description="Cursor pagination with cursor tokens and next links.",
            files_or_modules=["src/api/cursor.py"],
        )
    )

    assert plan == original
    assert isinstance(result, TaskApiCursorPaginationReadinessPlan)
    assert result.plan_id == "plan-cursor-pagination"
    assert len(result.findings) == 2
    # task-z is weak/partial, task-a is strong, so weak comes first
    assert result.findings[0].task_id == "task-z"
    assert result.findings[1].task_id == "task-a"
    assert result.findings[1].readiness == "strong"
    assert isinstance(payload, dict)
    assert payload["plan_id"] == "plan-cursor-pagination"
    assert len(payload["findings"]) == 2
    assert isinstance(task_result, TaskApiCursorPaginationReadinessPlan)
    assert len(task_result.findings) == 1
    assert task_result.findings[0].task_id == "task-a"
    assert isinstance(object_result, TaskApiCursorPaginationReadinessPlan)
    assert len(object_result.findings) == 1
    assert object_result.findings[0].task_id == "task-object"


def test_empty_state_markdown_output():
    result = build_task_api_cursor_pagination_readiness_plan(_plan([]))
    markdown = task_api_cursor_pagination_readiness_plan_to_markdown(result)

    assert "# Task API Cursor Pagination Readiness: plan-cursor-pagination" in markdown
    assert "## Summary" in markdown
    assert "Task count: 0" in markdown
    assert "Cursor pagination task count: 0" in markdown
    assert "No cursor pagination readiness findings were inferred." in markdown


def test_json_safe_serialization():
    result = build_task_api_cursor_pagination_readiness_plan(
        _plan(
            [
                _task(
                    "task-1",
                    title="Add cursor pagination",
                    description="Cursor pagination with cursor tokens, stable ordering, and deterministic sort tests.",
                )
            ]
        )
    )

    dict_output = result.to_dict()
    json_str = json.dumps(dict_output)
    parsed = json.loads(json_str)

    assert parsed["plan_id"] == "plan-cursor-pagination"
    assert len(parsed["findings"]) == 1
    assert parsed["findings"][0]["task_id"] == "task-1"
    assert isinstance(parsed["findings"][0]["detected_signals"], list)
    assert isinstance(parsed["findings"][0]["present_safeguards"], list)
    assert isinstance(parsed["summary"]["readiness_counts"], dict)

    dicts_output = task_api_cursor_pagination_readiness_plan_to_dicts(result)
    json_str = json.dumps(dicts_output)
    parsed_dicts = json.loads(json_str)

    assert len(parsed_dicts) == 1
    assert parsed_dicts[0]["task_id"] == "task-1"


def test_compatibility_aliases():
    plan = _plan([_task("task-1", title="Add cursor pagination", description="Cursor tokens and next links.")])

    build_result = build_task_api_cursor_pagination_readiness_plan(plan)
    analyze_result = analyze_task_api_cursor_pagination_readiness(plan)
    summarize_result = summarize_task_api_cursor_pagination_readiness(plan)
    extract_result = extract_task_api_cursor_pagination_readiness(plan)
    generate_result = generate_task_api_cursor_pagination_readiness(plan)

    assert build_result.plan_id == analyze_result.plan_id == summarize_result.plan_id
    assert build_result.plan_id == extract_result.plan_id == generate_result.plan_id
    assert len(build_result.findings) == len(analyze_result.findings)


def test_markdown_output_structure():
    result = build_task_api_cursor_pagination_readiness_plan(
        _plan(
            [
                _task(
                    "task-weak",
                    title="Add cursor pagination",
                    description="Cursor tokens and next links.",
                ),
                _task(
                    "task-strong",
                    title="Complete cursor pagination",
                    description=(
                        "Cursor pagination with cursor tokens, stable ordering, "
                        "deterministic sort tests, cursor expiry handling, malformed cursor tests, "
                        "max page size enforcement, and cursor documentation."
                    ),
                ),
            ]
        )
    )

    markdown = result.to_markdown()

    assert "# Task API Cursor Pagination Readiness: plan-cursor-pagination" in markdown
    assert "## Summary" in markdown
    assert "Task count: 2" in markdown
    assert "Cursor pagination task count: 2" in markdown
    assert "weak 1" in markdown
    assert "strong 1" in markdown
    assert "| Task | Title | Readiness | Signals | Present Safeguards | Missing Safeguards | Remediation | Evidence |" in markdown
    assert "| --- | --- | --- | --- | --- | --- | --- | --- |" in markdown
    assert "`task-weak`" in markdown
    assert "`task-strong`" in markdown
    assert "weak |" in markdown
    assert "strong |" in markdown


def test_metadata_signal_detection():
    result = build_task_api_cursor_pagination_readiness_plan(
        _plan(
            [
                _task(
                    "task-meta",
                    title="Update pagination",
                    description="Improve API endpoints.",
                    metadata={
                        "cursor_token": "Base64-encoded opaque cursor tokens",
                        "stable_ordering": "created_at with id tie-breaker",
                        "deterministic_sort_tests": "Tests for pagination consistency",
                    },
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "cursor_token" in finding.detected_signals
    assert "stable_ordering" in finding.detected_signals
    assert "deterministic_sort_tests" in finding.present_safeguards
    assert any("metadata.cursor_token" in ev for ev in finding.evidence)


def test_validation_commands_contribute_to_detection():
    result = build_task_api_cursor_pagination_readiness_plan(
        _plan(
            [
                _task(
                    "task-validation",
                    title="Pagination refactor",
                    description="Update endpoints.",
                    validation_commands=[
                        "pytest tests/test_cursor_pagination_deterministic_sort.py",
                        "pytest tests/test_malformed_cursor_validation.py",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "deterministic_sort_tests" in finding.present_safeguards
    assert "malformed_cursor_tests" in finding.present_safeguards
    assert any("validation_commands" in ev for ev in finding.evidence)


def _plan(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": "plan-cursor-pagination",
        "implementation_brief_id": "brief-cursor-pagination",
        "milestones": [],
        "tasks": tasks,
    }


def _task(
    task_id: str,
    *,
    title: str = "",
    description: str = "",
    files_or_modules: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    validation_commands: list[str] | None = None,
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "id": task_id,
        "title": title,
        "description": description,
        "acceptance_criteria": [],
    }
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    if validation_commands is not None:
        task["validation_commands"] = validation_commands
    return task
