import copy
import json
from types import SimpleNamespace
from typing import Any

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_api_request_validation_readiness import (
    TaskApiRequestValidationReadinessFinding,
    TaskApiRequestValidationReadinessPlan,
    analyze_task_api_request_validation_readiness,
    build_task_api_request_validation_readiness_plan,
    extract_task_api_request_validation_readiness,
    generate_task_api_request_validation_readiness,
    recommend_task_api_request_validation_readiness,
    summarize_task_api_request_validation_readiness,
    task_api_request_validation_readiness_plan_to_dict,
    task_api_request_validation_readiness_plan_to_dicts,
    task_api_request_validation_readiness_plan_to_markdown,
)


def test_high_risk_first_deterministic_ordering():
    result = build_task_api_request_validation_readiness_plan(
        _plan(
            [
                _task(
                    "task-high-1",
                    title="Add request validation",
                    description="Implement request schema validation with required fields and validation errors.",
                    files_or_modules=["src/api/validation.py"],
                ),
                _task(
                    "task-high-2",
                    title="Improve request validation",
                    description=(
                        "Add request schema validation with required field validation, "
                        "enum range validation, and validation test coverage."
                    ),
                ),
                _task(
                    "task-low",
                    title="Complete request validation",
                    description=(
                        "Implement comprehensive request validation with schema definition, "
                        "required field validation, enum and range validation, malformed payload handling, "
                        "validation error responses with 400 and 422, field constraint checks, "
                        "and complete validation test coverage."
                    ),
                ),
                _task(
                    "task-medium",
                    title="Better request validation",
                    description=(
                        "Implement request validation with schema definition, "
                        "required field validation, enum validation, malformed payload handling, "
                        "and validation test coverage."
                    ),
                ),
            ]
        )
    )

    assert isinstance(result, TaskApiRequestValidationReadinessPlan)
    assert result.plan_id == "plan-request-validation"
    assert len(result.findings) == 4
    # High risk tasks come first (sorted by task_id within same risk level)
    assert result.findings[0].risk_level == "high"
    assert result.findings[1].risk_level == "high"
    assert result.findings[2].risk_level == "medium"
    assert result.findings[3].risk_level == "medium"


def test_complete_coverage_has_no_actionable_gaps():
    result = analyze_task_api_request_validation_readiness(
        _plan(
            [
                _task(
                    "task-complete",
                    title="Add API request validation",
                    description=(
                        "Implement comprehensive API request validation for all endpoints. "
                        "Define request schema using JSON Schema for expected payload structure. "
                        "Validate required fields and return missing-field errors with field paths. "
                        "Check enum allowed values and numeric range constraints with clear error messages. "
                        "Handle malformed JSON and syntax errors with parse-error responses before schema validation. "
                        "Return structured 400 bad request or 422 unprocessable entity validation error responses "
                        "with field paths, error codes, and actionable messages. "
                        "Validate field constraints including length, pattern, format, email, and URL validation. "
                        "Add validation test coverage for required fields, enums, ranges, malformed payloads, "
                        "and validation error response formats."
                    ),
                    files_or_modules=["src/api/validation/request_validator.py"],
                )
            ]
        )
    )

    assert isinstance(result, TaskApiRequestValidationReadinessPlan)
    assert result.plan_id == "plan-request-validation"
    assert result.request_validation_task_ids == ("task-complete",)
    finding = result.findings[0]
    assert isinstance(finding, TaskApiRequestValidationReadinessFinding)
    assert finding.detected_signals == (
        "request_schema",
        "required_fields",
        "enum_range_checks",
        "malformed_json",
        "validation_errors",
        "field_constraints",
    )
    assert finding.present_requirements == (
        "schema_definition",
        "required_field_validation",
        "enum_range_validation",
        "malformed_payload_handling",
        "validation_error_responses",
        "field_constraint_checks",
        "validation_test_coverage",
    )
    assert finding.missing_requirements == ()
    assert finding.risk_level == "low"
    assert finding.actionable_gaps == ()
    assert any("description:" in ev for ev in finding.evidence)


def test_empty_plan_has_no_findings():
    result = build_task_api_request_validation_readiness_plan(_plan([]))

    assert result.plan_id == "plan-request-validation"
    assert result.findings == ()
    assert result.records == ()
    assert result.request_validation_task_ids == ()
    assert result.not_applicable_task_ids == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "total_task_count": 0,
        "request_validation_task_count": 0,
        "not_applicable_task_ids": [],
        "missing_requirement_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "signal_counts": {
            "request_schema": 0,
            "required_fields": 0,
            "enum_range_checks": 0,
            "malformed_json": 0,
            "validation_errors": 0,
            "field_constraints": 0,
        },
        "present_requirement_counts": {
            "schema_definition": 0,
            "required_field_validation": 0,
            "enum_range_validation": 0,
            "malformed_payload_handling": 0,
            "validation_error_responses": 0,
            "field_constraint_checks": 0,
            "validation_test_coverage": 0,
        },
        "missing_requirement_counts": {
            "schema_definition": 0,
            "required_field_validation": 0,
            "enum_range_validation": 0,
            "malformed_payload_handling": 0,
            "validation_error_responses": 0,
            "field_constraint_checks": 0,
            "validation_test_coverage": 0,
        },
    }


def test_no_impact_tasks_excluded():
    result = extract_task_api_request_validation_readiness(
        _plan(
            [
                _task(
                    "task-copy",
                    title="Update API docs copy",
                    description="Adjust helper text for endpoint descriptions.",
                    files_or_modules=["src/api/docs.py"],
                ),
                _task(
                    "task-no-validation",
                    title="Refactor user model",
                    description="No request validation, validation, or schema changes are required.",
                ),
            ]
        )
    )

    assert result.findings == ()
    assert result.records == ()
    assert result.request_validation_task_ids == ()
    assert result.not_applicable_task_ids == ("task-copy", "task-no-validation")
    assert result.to_dicts() == []
    assert result.summary == {
        "total_task_count": 2,
        "request_validation_task_count": 0,
        "not_applicable_task_ids": ["task-copy", "task-no-validation"],
        "missing_requirement_count": 0,
        "risk_counts": {"high": 0, "medium": 0, "low": 0},
        "signal_counts": {
            "request_schema": 0,
            "required_fields": 0,
            "enum_range_checks": 0,
            "malformed_json": 0,
            "validation_errors": 0,
            "field_constraints": 0,
        },
        "present_requirement_counts": {
            "schema_definition": 0,
            "required_field_validation": 0,
            "enum_range_validation": 0,
            "malformed_payload_handling": 0,
            "validation_error_responses": 0,
            "field_constraint_checks": 0,
            "validation_test_coverage": 0,
        },
        "missing_requirement_counts": {
            "schema_definition": 0,
            "required_field_validation": 0,
            "enum_range_validation": 0,
            "malformed_payload_handling": 0,
            "validation_error_responses": 0,
            "field_constraint_checks": 0,
            "validation_test_coverage": 0,
        },
    }


def test_mapping_and_object_inputs():
    plan = _plan(
        [
            _task(
                "task-z",
                title="Add API validation",
                description="Implement request schema validation for API endpoints with required fields.",
            ),
            _task(
                "task-a",
                title="Request schema validation",
                description=(
                    "Implement request schema validation using JSON Schema with schema definition, "
                    "required field validation, enum range validation, malformed payload handling, "
                    "validation error responses, field constraint checks, and validation test coverage."
                ),
                metadata={"request_schema": "JSON Schema for request validation"},
            ),
        ]
    )
    original = copy.deepcopy(plan)
    model = ExecutionPlan.model_validate(plan)

    result = summarize_task_api_request_validation_readiness(model)
    payload = task_api_request_validation_readiness_plan_to_dict(result)
    task_result = build_task_api_request_validation_readiness_plan(ExecutionTask.model_validate(plan["tasks"][1]))
    object_result = build_task_api_request_validation_readiness_plan(
        SimpleNamespace(
            id="task-object",
            title="Add request validation",
            description="Request validation with request schema and required fields.",
            files_or_modules=["src/api/validation.py"],
        )
    )

    assert plan == original
    assert isinstance(result, TaskApiRequestValidationReadinessPlan)
    assert result.plan_id == "plan-request-validation"
    assert len(result.findings) == 2
    # Both tasks have findings; task-z is likely high, task-a is likely lower risk
    assert result.findings[0].task_id == "task-z"
    assert result.findings[1].task_id == "task-a"
    assert isinstance(payload, dict)
    assert payload["plan_id"] == "plan-request-validation"
    assert len(payload["findings"]) == 2
    assert isinstance(task_result, TaskApiRequestValidationReadinessPlan)
    assert len(task_result.findings) == 1
    assert task_result.findings[0].task_id == "task-a"
    assert isinstance(object_result, TaskApiRequestValidationReadinessPlan)
    assert len(object_result.findings) == 1
    assert object_result.findings[0].task_id == "task-object"


def test_stable_serialization_to_dicts_and_dict():
    result = generate_task_api_request_validation_readiness(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Add validation",
                    description="Request schema validation with required fields and malformed JSON handling.",
                )
            ]
        )
    )

    dicts = task_api_request_validation_readiness_plan_to_dicts(result)
    dict_payload = task_api_request_validation_readiness_plan_to_dict(result)

    assert isinstance(dicts, list)
    assert len(dicts) == 1
    assert isinstance(dicts[0], dict)
    assert dicts[0]["task_id"] == "task-partial"
    assert "detected_signals" in dicts[0]
    assert "present_requirements" in dicts[0]
    assert "missing_requirements" in dicts[0]
    assert isinstance(dict_payload, dict)
    assert dict_payload["plan_id"] == "plan-request-validation"
    assert len(dict_payload["findings"]) == 1
    assert dict_payload["request_validation_task_ids"] == ["task-partial"]

    # Stable JSON serialization
    json_str = json.dumps(dict_payload, sort_keys=True)
    assert isinstance(json_str, str)
    assert "task-partial" in json_str


def test_markdown_rendering_stable_format():
    result = recommend_task_api_request_validation_readiness(
        _plan(
            [
                _task(
                    "task-weak",
                    title="Add validation",
                    description="Request schema validation with required fields.",
                )
            ]
        )
    )

    markdown = task_api_request_validation_readiness_plan_to_markdown(result)

    assert isinstance(markdown, str)
    assert "# Task API Request Validation Readiness: plan-request-validation" in markdown
    assert "## Summary" in markdown
    assert "- Task count: 1" in markdown
    assert "- Request validation task count: 1" in markdown
    assert "| Task | Title | Risk |" in markdown
    assert "task-weak" in markdown
    assert "Add validation" in markdown


def test_markdown_empty_state_text():
    result = build_task_api_request_validation_readiness_plan(_plan([]))

    markdown = result.to_markdown()

    assert isinstance(markdown, str)
    assert "# Task API Request Validation Readiness: plan-request-validation" in markdown
    assert "## Summary" in markdown
    assert "- Task count: 0" in markdown
    assert "- Request validation task count: 0" in markdown
    assert "No request validation readiness findings were inferred." in markdown
    # No table headers when empty
    assert "| Task | Title | Risk |" not in markdown


def test_compatibility_aliases():
    plan = _plan(
        [
            _task(
                "task-compat",
                title="Validation compatibility test",
                description="Request schema validation with required fields and validation errors.",
            )
        ]
    )

    build_result = build_task_api_request_validation_readiness_plan(plan)
    analyze_result = analyze_task_api_request_validation_readiness(plan)
    summarize_result = summarize_task_api_request_validation_readiness(plan)
    extract_result = extract_task_api_request_validation_readiness(plan)
    generate_result = generate_task_api_request_validation_readiness(plan)
    recommend_result = recommend_task_api_request_validation_readiness(plan)

    assert isinstance(build_result, TaskApiRequestValidationReadinessPlan)
    assert isinstance(analyze_result, TaskApiRequestValidationReadinessPlan)
    assert isinstance(summarize_result, TaskApiRequestValidationReadinessPlan)
    assert isinstance(extract_result, TaskApiRequestValidationReadinessPlan)
    assert isinstance(generate_result, TaskApiRequestValidationReadinessPlan)
    assert isinstance(recommend_result, TaskApiRequestValidationReadinessPlan)
    assert build_result.plan_id == analyze_result.plan_id == "plan-request-validation"
    assert len(build_result.findings) == len(analyze_result.findings) == 1


def test_metadata_signals_detected():
    result = build_task_api_request_validation_readiness_plan(
        _plan(
            [
                _task(
                    "task-metadata",
                    title="Validation refactor",
                    description="Update endpoints.",
                    metadata={
                        "request_schema": "JSON Schema",
                        "validation_errors": "400 and 422 responses",
                        "required_fields": ["id", "name", "email"],
                    },
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "request_schema" in finding.detected_signals
    assert "validation_errors" in finding.detected_signals
    assert "required_fields" in finding.detected_signals
    assert any("metadata" in ev for ev in finding.evidence)


def test_validation_commands_detected():
    result = build_task_api_request_validation_readiness_plan(
        _plan(
            [
                _task(
                    "task-validation",
                    title="Validation refactor",
                    description="Update endpoints.",
                    validation_commands=[
                        "pytest tests/test_required_field_validation.py",
                        "pytest tests/test_malformed_json_handling.py",
                    ],
                )
            ]
        )
    )

    finding = result.findings[0]
    assert "validation_test_coverage" in finding.present_requirements
    assert any("validation_commands" in ev for ev in finding.evidence)


def _plan(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": "plan-request-validation",
        "implementation_brief_id": "brief-request-validation",
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
