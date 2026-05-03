import copy
import json
from types import SimpleNamespace
from typing import Any

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.task_api_error_redaction_readiness import (
    TaskAPIErrorRedactionReadinessPlan,
    TaskAPIErrorRedactionReadinessRecord,
    analyze_task_api_error_redaction_readiness,
    build_task_api_error_redaction_readiness_plan,
    derive_task_api_error_redaction_readiness,
    extract_task_api_error_redaction_readiness,
    generate_task_api_error_redaction_readiness,
    recommend_task_api_error_redaction_readiness,
    summarize_task_api_error_redaction_readiness,
    task_api_error_redaction_readiness_plan_to_dict,
    task_api_error_redaction_readiness_plan_to_dicts,
    task_api_error_redaction_readiness_plan_to_markdown,
    task_api_error_redaction_readiness_to_dicts,
)


def test_weak_error_redaction_task_sorts_first_and_separates_no_impact_ids():
    result = build_task_api_error_redaction_readiness_plan(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Add error handling for API",
                    description="Implement error responses for API endpoints with PII exposure risk.",
                    acceptance_criteria=[
                        "Redaction tests verify sensitive data is not exposed.",
                        "Safe error mapper sanitizes error responses.",
                        "Structured logging redaction configured for sensitive fields.",
                    ],
                ),
                _task(
                    "task-copy",
                    title="Polish settings copy",
                    description="Update labels in the admin settings page.",
                ),
                _task(
                    "task-weak",
                    title="Implement error responses",
                    description="Add error responses that may expose secrets, PII, and stack traces to clients.",
                ),
            ]
        )
    )

    assert isinstance(result, TaskAPIErrorRedactionReadinessPlan)
    assert result.impacted_task_ids == ("task-weak", "task-partial")
    assert result.error_redaction_task_ids == result.impacted_task_ids
    assert result.no_impact_task_ids == ("task-copy",)
    assert [record.task_id for record in result.records] == ["task-weak", "task-partial"]
    weak = result.records[0]
    assert isinstance(weak, TaskAPIErrorRedactionReadinessRecord)
    assert weak.readiness == "weak"
    assert weak.impact == "high"
    assert {"secret_exposure", "pii_exposure", "stack_trace_exposure"} <= set(weak.detected_signals)
    assert "redaction_tests" in weak.missing_safeguards
    assert "safe_error_mapper" in weak.missing_safeguards


def test_strong_readiness_reflects_all_safeguards_and_summary_counts():
    result = analyze_task_api_error_redaction_readiness(
        _plan(
            [
                _task(
                    "task-strong",
                    title="Add error redaction for API endpoints",
                    description=(
                        "API endpoints may expose secrets, PII, stack traces, internal IDs, SQL details, "
                        "and provider tokens in error responses. Implement comprehensive error redaction."
                    ),
                    acceptance_criteria=[
                        "Redaction tests verify secrets, PII, and stack traces are redacted.",
                        "Safe error mapper sanitizes all error responses before returning to clients.",
                        "Structured logging redaction configured to mask sensitive fields in logs.",
                        "Tenant-safe correlation IDs used that don't leak tenant information.",
                        "Production stack trace blocking prevents stack traces in production mode.",
                        "Error schema validation ensures only allowed fields are exposed.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "strong"
    assert record.impact == "medium"
    assert record.present_safeguards == (
        "redaction_tests",
        "safe_error_mapper",
        "structured_logging_redaction",
        "tenant_safe_correlation_ids",
        "production_stack_trace_blocking",
        "error_schema_validation",
    )
    assert record.missing_safeguards == ()
    assert record.recommended_checks == ()
    assert result.summary["readiness_counts"] == {"weak": 0, "partial": 0, "strong": 1}
    assert result.summary["impact_counts"] == {"high": 0, "medium": 1, "low": 0}
    assert result.summary["signal_counts"]["secret_exposure"] >= 1
    assert result.summary["present_safeguard_counts"]["redaction_tests"] == 1


def test_mapping_input_collects_title_description_paths_and_validation_command_evidence():
    result = build_task_api_error_redaction_readiness_plan(
        {
            "id": "task-mapping",
            "title": "Add error redaction for user API",
            "description": "Implement error redaction to prevent PII exposure and tenant data leaks in error responses.",
            "files_or_modules": [
                "src/api/error_mapper.py",
                "src/api/error_redaction.py",
                "tests/test_error_redaction.py",
            ],
            "validation_commands": {
                "tests": [
                    "poetry run pytest tests/test_error_redaction.py",
                    "poetry run pytest tests/test_safe_error_mapper.py",
                ]
            },
        }
    )

    record = result.records[0]
    assert {"pii_exposure", "tenant_data_exposure"} <= set(record.detected_signals)
    assert {"redaction_tests", "safe_error_mapper"} <= set(record.present_safeguards)
    assert any(item == "title: Add error redaction for user API" for item in record.evidence)
    assert any("description: Implement error redaction to prevent PII exposure" in item for item in record.evidence)
    assert any(item == "files_or_modules: src/api/error_redaction.py" for item in record.evidence)
    assert any("validation_commands: poetry run pytest tests/test_error_redaction.py" in item for item in record.evidence)


def test_execution_plan_execution_task_and_object_inputs_are_supported_without_mutation():
    object_task = SimpleNamespace(
        id="task-object",
        title="Add API error handling",
        description="Implement error handling that may expose secrets and stack traces.",
        acceptance_criteria=["Redaction tests verify sensitive data is not exposed."],
    )
    model_task = ExecutionTask.model_validate(
        _task(
            "task-model",
            title="Implement safe error mapper",
            description="Create error mapper that redacts PII and provider tokens.",
            acceptance_criteria=["Safe error mapper sanitizes all error responses."],
        )
    )
    source = _plan(
        [
            model_task.model_dump(mode="python"),
            _task(
                "task-covered",
                title="Add comprehensive error redaction",
                description="API error responses may expose secrets, PII, stack traces, and tenant data.",
                acceptance_criteria=[
                    "Redaction tests verify all sensitive data is redacted.",
                    "Safe error mapper implemented and tested.",
                    "Structured logging redaction configured.",
                    "Tenant-safe correlation IDs implemented.",
                    "Production stack trace blocking enabled.",
                    "Error schema validation implemented.",
                ],
            ),
        ],
        plan_id="plan-error-redaction-objects",
    )
    original = copy.deepcopy(source)

    result = summarize_task_api_error_redaction_readiness(source)

    assert source == original
    assert build_task_api_error_redaction_readiness_plan(object_task).records[0].task_id == "task-object"
    assert derive_task_api_error_redaction_readiness(source).to_dict() == result.to_dict()
    assert recommend_task_api_error_redaction_readiness(source).to_dict() == result.to_dict()
    assert result.records == result.findings
    assert result.records == result.recommendations


def test_empty_state_markdown_and_summary_are_stable():
    result = build_task_api_error_redaction_readiness_plan(
        _plan([_task("task-ui", title="Polish dashboard", description="Adjust table spacing.")])
    )

    assert result.records == ()
    assert result.impacted_task_ids == ()
    assert result.no_impact_task_ids == ("task-ui",)
    assert result.summary["error_redaction_task_count"] == 0
    assert result.to_markdown() == "\n".join(
        [
            "# Task API Error Redaction Readiness: plan-error-redaction",
            "",
            "## Summary",
            "",
            "- Task count: 1",
            "- Error redaction task count: 0",
            "- Missing safeguard count: 0",
            "- Readiness counts: weak 0, partial 0, strong 0",
            "- Impact counts: high 0, medium 0, low 0",
            "",
            "No task API error redaction readiness records were inferred.",
            "",
            "No-impact tasks: task-ui",
        ]
    )


def test_serialization_aliases_to_dict_output_and_markdown_are_json_safe():
    result = build_task_api_error_redaction_readiness_plan(
        _plan(
            [
                _task(
                    "task-redaction",
                    title="Add error redaction for API",
                    description="API endpoints may expose secrets and PII in error responses.",
                    acceptance_criteria=[
                        "Redaction tests verify sensitive data is not exposed.",
                        "Safe error mapper sanitizes error responses.",
                        "Structured logging redaction configured.",
                    ],
                )
            ],
            plan_id="plan-serialization",
        )
    )
    payload = task_api_error_redaction_readiness_plan_to_dict(result)
    markdown = task_api_error_redaction_readiness_plan_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == payload["records"]
    assert task_api_error_redaction_readiness_plan_to_dicts(result) == payload["records"]
    assert task_api_error_redaction_readiness_plan_to_dicts(result.records) == payload["records"]
    assert task_api_error_redaction_readiness_to_dicts(result) == payload["records"]
    assert task_api_error_redaction_readiness_to_dicts(result.records) == payload["records"]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Task API Error Redaction Readiness: plan-serialization")
    assert list(payload) == [
        "plan_id",
        "records",
        "findings",
        "recommendations",
        "impacted_task_ids",
        "error_redaction_task_ids",
        "no_impact_task_ids",
        "summary",
    ]


def test_high_impact_weak_tasks_highlighted_in_markdown():
    result = build_task_api_error_redaction_readiness_plan(
        _plan(
            [
                _task(
                    "task-high-weak",
                    title="Add error handling",
                    description="Implement error responses that may expose secrets, PII, and tenant data.",
                ),
                _task(
                    "task-low-weak",
                    title="Log SQL errors",
                    description="Log SQL details and internal IDs in error logs.",
                ),
            ]
        )
    )

    markdown = result.to_markdown()
    assert "## High-Impact Weak Tasks" in markdown
    assert "task-high-weak" in markdown
    assert "Add error handling" in markdown
    assert "**Readiness**: weak" in markdown
    assert "**Impact**: high" in markdown


def test_secret_and_pii_signals_detected_from_various_sources():
    result = build_task_api_error_redaction_readiness_plan(
        _plan(
            [
                _task(
                    "task-signals",
                    title="API error responses",
                    description="Error responses may contain secrets, API keys, tokens, passwords, and personal data.",
                    files_or_modules=[
                        "src/api/error_handler.py",
                        "src/api/exception_mapper.py",
                    ],
                    metadata={
                        "concerns": ["PII exposure", "secret exposure", "stack trace exposure"],
                        "risk": "Error responses leak internal IDs and SQL details",
                    },
                )
            ]
        )
    )

    record = result.records[0]
    assert "secret_exposure" in record.detected_signals
    assert "pii_exposure" in record.detected_signals
    assert "stack_trace_exposure" in record.detected_signals
    assert "internal_id_exposure" in record.detected_signals
    assert "sql_detail_exposure" in record.detected_signals


def test_safeguard_detection_from_acceptance_criteria_and_paths():
    result = build_task_api_error_redaction_readiness_plan(
        _plan(
            [
                _task(
                    "task-safeguards",
                    title="Implement error redaction",
                    description="Error responses need redaction to prevent data exposure.",
                    files_or_modules=[
                        "src/api/safe_error_mapper.py",
                        "tests/test_redaction.py",
                        "src/logging/structured_logging_redaction.py",
                    ],
                    acceptance_criteria=[
                        "Redaction tests verify no secrets are exposed.",
                        "Tenant-safe correlation IDs implemented.",
                        "Production stack trace blocking prevents stack traces in prod mode.",
                        "Error schema validation ensures only allowed fields are exposed.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert "redaction_tests" in record.present_safeguards
    assert "safe_error_mapper" in record.present_safeguards
    assert "structured_logging_redaction" in record.present_safeguards
    assert "tenant_safe_correlation_ids" in record.present_safeguards
    assert "production_stack_trace_blocking" in record.present_safeguards
    assert "error_schema_validation" in record.present_safeguards


def test_provider_token_and_tenant_data_signals_trigger_high_impact():
    result = build_task_api_error_redaction_readiness_plan(
        _plan(
            [
                _task(
                    "task-provider",
                    title="Handle external API errors",
                    description="Error responses may expose provider tokens from Stripe and AWS in exception messages.",
                )
            ]
        )
    )

    record = result.records[0]
    assert "provider_token_exposure" in record.detected_signals
    assert record.impact == "high"
    assert record.readiness == "weak"


def test_recommended_checks_guidance_matches_missing_safeguards():
    result = build_task_api_error_redaction_readiness_plan(
        _plan(
            [
                _task(
                    "task-guidance",
                    title="Add error handling",
                    description="Error responses may expose secrets and PII.",
                )
            ]
        )
    )

    record = result.records[0]
    assert len(record.recommended_checks) == len(record.missing_safeguards)
    assert all(
        "redact" in check.lower()
        or "error" in check.lower()
        or "production" in check.lower()
        or "log" in check.lower()
        or "correlation" in check.lower()
        or "schema" in check.lower()
        for check in record.recommended_checks
    )


def test_object_payload_extraction_handles_arbitrary_objects():
    task_object = SimpleNamespace(
        id="task-obj",
        title="Error handling",
        description="Error responses with stack traces and secrets.",
        files_or_modules=["src/api/errors.py"],
        acceptance_criteria=["Redaction tests verify sensitive data is not exposed."],
    )

    result = build_task_api_error_redaction_readiness_plan(task_object)

    assert len(result.records) == 1
    assert result.records[0].task_id == "task-obj"
    assert "stack_trace_exposure" in result.records[0].detected_signals
    assert "secret_exposure" in result.records[0].detected_signals
    assert "redaction_tests" in result.records[0].present_safeguards


def test_validation_command_evidence_collected():
    result = build_task_api_error_redaction_readiness_plan(
        {
            "id": "task-validation",
            "title": "Error redaction implementation",
            "description": "Implement error redaction for API error responses that may expose secrets and PII.",
            "validation_commands": {
                "tests": [
                    "poetry run pytest tests/test_error_redaction.py",
                    "poetry run pytest tests/test_safe_error_mapper.py",
                ]
            },
        }
    )

    assert len(result.records) > 0, "Expected at least one record for error redaction task"
    record = result.records[0]
    assert any("validation_commands" in item for item in record.evidence)
    assert "redaction_tests" in record.present_safeguards
    assert "safe_error_mapper" in record.present_safeguards


def test_partial_readiness_requires_at_least_three_safeguards():
    result = build_task_api_error_redaction_readiness_plan(
        _plan(
            [
                _task(
                    "task-partial",
                    title="Error handling",
                    description="Error responses may expose secrets.",
                    acceptance_criteria=[
                        "Redaction tests implemented.",
                        "Safe error mapper configured.",
                        "Structured logging redaction enabled.",
                    ],
                )
            ]
        )
    )

    record = result.records[0]
    assert record.readiness == "partial"
    assert len(record.present_safeguards) >= 3
    assert len(record.missing_safeguards) >= 1


def test_properties_provide_compatibility_aliases():
    result = build_task_api_error_redaction_readiness_plan(
        _plan(
            [
                _task(
                    "task-alias",
                    title="Error handling",
                    description="Error responses may expose secrets.",
                )
            ]
        )
    )

    record = result.records[0]
    assert record.signals == record.detected_signals
    assert record.safeguards == record.present_safeguards
    assert record.recommendations == record.recommended_checks
    assert record.recommended_actions == record.recommended_checks


def _plan(tasks: list[dict[str, Any]], *, plan_id: str = "plan-error-redaction") -> dict[str, Any]:
    return {"id": plan_id, "tasks": tasks}


def _task(task_id: str, *, title: str = "", description: str = "", **kwargs: Any) -> dict[str, Any]:
    return {"id": task_id, "title": title, "description": description, **kwargs}
