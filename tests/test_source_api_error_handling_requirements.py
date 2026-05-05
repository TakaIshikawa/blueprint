from blueprint.domain.models import SourceBrief
from blueprint.source_api_error_handling_requirements import (
    SourceApiErrorHandlingRequirement,
    SourceApiErrorHandlingRequirementsReport,
    build_source_api_error_handling_requirements,
    extract_source_api_error_handling_requirements,
    source_api_error_handling_requirements_to_dicts,
    source_api_error_handling_requirements_to_markdown,
    summarize_source_api_error_handling_requirements,
)


def test_extracts_multi_signal_error_handling_requirements_with_evidence():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary=(
                "Implement API with standardized error response format using proper HTTP status codes. "
                "Include retry logic with exponential backoff for transient failures."
            ),
            source_payload={
                "requirements": [
                    "Return 400 for invalid input, 401 for unauthorized, 404 for not found, 500 for server errors.",
                    "Error response must include error code, message, and details fields.",
                    "Provide human-readable error messages with actionable guidance.",
                    "Implement automatic retry with exponential backoff for 5xx errors.",
                ],
                "acceptance_criteria": [
                    "Log all errors with stack traces for debugging.",
                    "Support fallback to cached data when API is unavailable.",
                    "Return field-level validation errors for invalid input.",
                ],
            },
        )
    )

    assert isinstance(result, SourceApiErrorHandlingRequirementsReport)
    assert all(isinstance(record, SourceApiErrorHandlingRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "error_response_format",
        "status_code_mapping",
        "error_message_templates",
        "retry_strategies",
        "fallback_behaviors",
        "error_logging",
        "client_error_guidance",
        "validation_errors",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert any("error code" in item.lower() or "error response" in item.lower() for item in by_type["error_response_format"].evidence)
    assert any("400" in item or "401" in item or "404" in item or "500" in item for item in by_type["status_code_mapping"].evidence)
    assert any("exponential backoff" in item.lower() or "retry" in item.lower() for item in by_type["retry_strategies"].evidence)
    assert result.summary["requirement_count"] == 8
    assert result.summary["type_counts"]["error_response_format"] == 1
    assert result.summary["client_experience_coverage"] > 0
    assert result.summary["reliability_coverage"] > 0
    assert result.summary["observability_coverage"] > 0


def test_brief_without_error_handling_language_returns_stable_empty_report():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            title="Simple API",
            summary="Create a basic REST endpoint for users.",
            source_payload={
                "requirements": [
                    "Accept user data via POST.",
                    "Return user ID on success.",
                ],
            },
        )
    )
    repeat = build_source_api_error_handling_requirements(
        _source_brief(
            title="Simple API",
            summary="Create a basic REST endpoint for users.",
            source_payload={
                "requirements": [
                    "Accept user data via POST.",
                    "Return user ID on success.",
                ],
            },
        )
    )

    expected_summary = {
        "requirement_count": 0,
        "source_count": 1,
        "type_counts": {
            "error_response_format": 0,
            "status_code_mapping": 0,
            "error_message_templates": 0,
            "retry_strategies": 0,
            "fallback_behaviors": 0,
            "error_logging": 0,
            "client_error_guidance": 0,
            "validation_errors": 0,
        },
        "requirement_types": [],
        "follow_up_question_count": 0,
        "client_experience_coverage": 0,
        "reliability_coverage": 0,
        "observability_coverage": 0,
    }
    assert result.summary == expected_summary
    assert result.requirements == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.to_dict() == repeat.to_dict()


def test_error_response_format_requirements_detected():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Implement standardized error response format.",
            source_payload={
                "requirements": [
                    "Error response must include code, message, and detail fields.",
                    "Use JSON error format with standard error envelope.",
                    "Error object should contain timestamp and request_id.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "error_response_format" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    format_req = by_type["error_response_format"]
    assert any("error" in term.lower() and ("format" in term.lower() or "response" in term.lower() or "object" in term.lower()) for term in format_req.matched_terms)


def test_status_code_mapping_requirements_detected():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Use appropriate HTTP status codes for different error types.",
            source_payload={
                "requirements": [
                    "Return 400 Bad Request for invalid input.",
                    "Return 401 Unauthorized for authentication failures.",
                    "Return 404 Not Found for missing resources.",
                    "Return 422 Unprocessable Entity for validation errors.",
                    "Return 429 Too Many Requests for rate limit exceeded.",
                    "Return 500 Internal Server Error for unexpected failures.",
                    "Return 503 Service Unavailable when dependencies are down.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "status_code_mapping" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    status_req = by_type["status_code_mapping"]
    assert any(code in str(status_req.matched_terms) for code in ["400", "401", "404", "422", "429", "500", "503"])


def test_error_message_template_requirements_detected():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Provide clear, user-facing error messages.",
            source_payload={
                "requirements": [
                    "Error messages should be human-readable and actionable.",
                    "Support localized error messages for international users.",
                    "Use message templates for consistent error wording.",
                    "Error descriptions should guide users to resolution.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "error_message_templates" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    message_req = by_type["error_message_templates"]
    assert any("message" in term.lower() or "error" in term.lower() for term in message_req.matched_terms)


def test_retry_strategy_requirements_detected():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Implement intelligent retry logic for transient failures.",
            source_payload={
                "requirements": [
                    "Retry failed requests with exponential backoff.",
                    "Maximum of 3 retries for idempotent operations.",
                    "Only retry on transient errors (5xx, network timeouts).",
                    "Include Retry-After header in rate limit responses.",
                    "Use jittered backoff to prevent thundering herd.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "retry_strategies" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    retry_req = by_type["retry_strategies"]
    assert any("retry" in term.lower() or "backoff" in term.lower() for term in retry_req.matched_terms)


def test_fallback_behavior_requirements_detected():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Implement graceful degradation when services fail.",
            source_payload={
                "requirements": [
                    "Fall back to cached data when API is unavailable.",
                    "Return partial response with best-effort data.",
                    "Implement circuit breaker to prevent cascading failures.",
                    "Degrade to read-only mode when write operations fail.",
                    "Provide default values when optional services are down.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "fallback_behaviors" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    fallback_req = by_type["fallback_behaviors"]
    assert any("fallback" in term.lower() or "circuit breaker" in term.lower() or "degraded" in term.lower() or "default" in term.lower() for term in fallback_req.matched_terms)


def test_error_logging_requirements_detected():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Comprehensive error logging and monitoring.",
            source_payload={
                "requirements": [
                    "Log all errors with full stack traces.",
                    "Track error rates and alert on anomalies.",
                    "Include request context in error logs (user_id, request_id).",
                    "Send critical errors to monitoring dashboard.",
                    "Aggregate errors by type for analysis.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "error_logging" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    logging_req = by_type["error_logging"]
    assert any("log" in term.lower() or "track" in term.lower() or "monitor" in term.lower() for term in logging_req.matched_terms)


def test_client_error_guidance_requirements_detected():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Provide comprehensive API error documentation.",
            source_payload={
                "requirements": [
                    "Document all error codes with examples.",
                    "Include troubleshooting guide for common errors.",
                    "Provide remediation steps in error responses.",
                    "Create error reference documentation for developers.",
                    "Include debugging tips in API documentation.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "client_error_guidance" in types


def test_validation_error_requirements_detected():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Return detailed field-level validation errors.",
            source_payload={
                "requirements": [
                    "Return validation errors for each invalid field.",
                    "Include field path and constraint violation details.",
                    "Validate all fields and return all errors at once.",
                    "Provide per-field error messages with validation rules.",
                    "Support schema validation against input constraints.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "validation_errors" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    validation_req = by_type["validation_errors"]
    assert any("validation" in term.lower() or "field" in term.lower() for term in validation_req.matched_terms)


def test_rest_api_error_scenarios_detected():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="REST API with comprehensive error handling.",
            source_payload={
                "requirements": [
                    "Use 404 for resource not found.",
                    "Use 400 for malformed requests.",
                    "Return JSON error response with code and message.",
                    "Include retry-after header for 429 responses.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "status_code_mapping" in types
    assert "error_response_format" in types


def test_graphql_error_handling_detected():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="GraphQL API error handling with extensions.",
            source_payload={
                "requirements": [
                    "Return errors array in GraphQL response.",
                    "Include error code in extensions field.",
                    "Log GraphQL errors with query context.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "error_response_format" in types
    assert "error_logging" in types


def test_requirement_deduplication_merges_evidence():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Return 400 for invalid requests with error details.",
            source_payload={
                "requirements": [
                    "Use HTTP status code 400 for bad requests.",
                    "Status code 400 indicates client error.",
                ],
                "acceptance": "All 400 errors must include error message.",
            },
        )
    )

    by_type = {record.requirement_type: record for record in result.records}
    status_req = by_type["status_code_mapping"]
    assert len(status_req.source_field_paths) >= 2
    assert "summary" in status_req.source_field_paths
    assert any("requirements" in field or "acceptance" in field for field in status_req.source_field_paths)
    assert len(status_req.evidence) >= 2


def test_dict_serialization_round_trips():
    original = build_source_api_error_handling_requirements(
        _source_brief(
            summary="API with error formatting and status codes.",
            source_payload={
                "requirements": [
                    "Return JSON error format.",
                    "Use 404 for not found.",
                ],
            },
        )
    )

    serialized = original.to_dict()
    assert isinstance(serialized, dict)
    assert serialized["source_brief_id"] == "error-handling-source"
    assert len(serialized["requirements"]) == len(original.requirements)
    assert serialized["summary"]["requirement_count"] == len(original.requirements)

    repeat = original.to_dict()
    assert repeat == serialized


def test_markdown_output_renders_table():
    report = build_source_api_error_handling_requirements(
        _source_brief(
            source_id="error-markdown-test",
            summary="API with standardized error responses.",
            source_payload={
                "requirements": ["Return JSON error format with code field."],
            },
        )
    )

    markdown = source_api_error_handling_requirements_to_markdown(report)
    assert isinstance(markdown, str)
    assert "# Source API Error Handling Requirements Report: error-markdown-test" in markdown
    assert "## Summary" in markdown
    assert "## Requirements" in markdown
    assert "error_response_format" in markdown

    repeat_markdown = report.to_markdown()
    assert repeat_markdown == markdown


def test_empty_report_markdown_message():
    report = build_source_api_error_handling_requirements(
        _source_brief(summary="Simple endpoint with no error handling.")
    )

    markdown = report.to_markdown()
    assert "No source API error handling requirements were inferred." in markdown


def test_extracts_from_raw_text_input():
    result = build_source_api_error_handling_requirements(
        "Implement API with 400 status for bad requests, "
        "JSON error format with code and message fields, "
        "and automatic retry with exponential backoff."
    )

    assert len(result.requirements) >= 3
    types = {req.requirement_type for req in result.requirements}
    assert "status_code_mapping" in types
    assert "error_response_format" in types
    assert "retry_strategies" in types


def test_extracts_from_mapping_input():
    result = build_source_api_error_handling_requirements(
        {
            "id": "mapping-source",
            "title": "Error handling",
            "summary": "API with 500 status for server errors.",
            "source_payload": {
                "requirements": "Log all errors with stack traces.",
            },
        }
    )

    assert result.source_brief_id == "mapping-source"
    types = {req.requirement_type for req in result.requirements}
    assert "status_code_mapping" in types
    assert "error_logging" in types


def test_extract_helper_returns_tuple():
    requirements = extract_source_api_error_handling_requirements(
        _source_brief(summary="API with error response format.")
    )

    assert isinstance(requirements, tuple)
    assert all(isinstance(req, SourceApiErrorHandlingRequirement) for req in requirements)


def test_summarize_helper_returns_dict():
    summary = summarize_source_api_error_handling_requirements(
        _source_brief(summary="API with retry logic and fallback.")
    )

    assert isinstance(summary, dict)
    assert "requirement_count" in summary
    assert "type_counts" in summary


def test_coverage_metrics_calculated():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Comprehensive error handling with messages, guidance, validation, retry, fallback, and logging.",
            source_payload={
                "requirements": [
                    "Provide user-facing error messages.",
                    "Include troubleshooting documentation.",
                    "Return field-level validation errors.",
                    "Implement retry with backoff.",
                    "Fall back to cached data.",
                    "Log all errors with traces.",
                    "Use standard error format.",
                ],
            },
        )
    )

    summary = result.summary
    assert summary["client_experience_coverage"] == 100
    assert summary["reliability_coverage"] == 100
    assert summary["observability_coverage"] == 100


def test_follow_up_questions_reduced_with_specifics():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="API returns 400, 401, 404, 500 status codes.",
            source_payload={
                "requirements": [
                    "Error response includes code, message, and details fields.",
                    "Retry with exponential backoff and max 3 retries.",
                ],
            },
        )
    )

    by_type = {req.requirement_type: req for req in result.requirements}
    status_codes = by_type.get("status_code_mapping")
    if status_codes:
        assert len(status_codes.follow_up_questions) == 0
    error_format = by_type.get("error_response_format")
    if error_format:
        assert len(error_format.follow_up_questions) < 2
    retry = by_type.get("retry_strategies")
    if retry:
        assert len(retry.follow_up_questions) < 2


def test_extracts_from_pydantic_model():
    source_brief = SourceBrief(
        id="pydantic-test",
        title="Error handling",
        summary="API with 404 errors and retry logic.",
        source_project="test-project",
        source_entity_type="manual",
        source_id="pydantic-source",
        source_payload={
            "requirements": ["Return 404 for not found.", "Retry on transient errors."]
        },
        source_links={},
    )

    result = build_source_api_error_handling_requirements(source_brief)

    assert result.source_brief_id == "pydantic-test"
    types = {req.requirement_type for req in result.requirements}
    assert "status_code_mapping" in types
    assert "retry_strategies" in types


def test_extracts_from_object_with_attributes():
    class CustomSource:
        def __init__(self):
            self.id = "custom-obj"
            self.title = "Custom"
            self.summary = "API with validation errors and error logging."
            self.requirements = ["Return per-field validation errors.", "Log all errors."]

    result = build_source_api_error_handling_requirements(CustomSource())

    assert result.source_brief_id == "custom-obj"
    types = {req.requirement_type for req in result.requirements}
    assert "validation_errors" in types
    assert "error_logging" in types


def test_handles_invalid_input_gracefully():
    result = build_source_api_error_handling_requirements({})
    assert result.requirements == ()
    assert result.summary["requirement_count"] == 0

    result = build_source_api_error_handling_requirements("")
    assert result.requirements == ()

    result = build_source_api_error_handling_requirements(None)
    assert result.requirements == ()


def test_to_dicts_helper_from_report():
    report = build_source_api_error_handling_requirements(
        _source_brief(summary="API with 500 errors and error format.")
    )

    dicts = source_api_error_handling_requirements_to_dicts(report)
    assert isinstance(dicts, list)
    assert all(isinstance(d, dict) for d in dicts)
    assert len(dicts) == len(report.requirements)


def test_to_dicts_helper_from_tuple():
    requirements = extract_source_api_error_handling_requirements(
        _source_brief(summary="API with retry logic.")
    )

    dicts = source_api_error_handling_requirements_to_dicts(requirements)
    assert isinstance(dicts, list)
    assert all(isinstance(d, dict) for d in dicts)


def test_edge_case_multiple_status_codes_in_one_sentence():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Return 400, 401, 403, 404, 422, 429, 500, 502, 503, 504 based on error type."
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "status_code_mapping" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    status_req = by_type["status_code_mapping"]
    matched = str(status_req.matched_terms)
    assert "400" in matched or "401" in matched or "404" in matched or "500" in matched


def test_edge_case_retry_max_attempts():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Retry failed requests with maximum 5 retries and 2 second delay."
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "retry_strategies" in types


def test_edge_case_circuit_breaker_fallback():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Implement circuit breaker pattern with fallback to default values."
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "fallback_behaviors" in types


def test_no_mutation_of_input_dict():
    input_dict = {
        "id": "mutation-test",
        "title": "Test",
        "summary": "API with error format.",
        "source_payload": {"requirements": ["Error response format."]},
    }
    original = dict(input_dict)

    build_source_api_error_handling_requirements(input_dict)

    assert input_dict == original


def test_no_mutation_of_input_object():
    class CustomSource:
        def __init__(self):
            self.id = "obj-mutation-test"
            self.summary = "API with 404 errors."
            self.requirements = ["Return 404 for not found."]

    obj = CustomSource()
    original_summary = obj.summary
    original_requirements = obj.requirements

    build_source_api_error_handling_requirements(obj)

    assert obj.summary == original_summary
    assert obj.requirements == original_requirements


def test_matched_terms_extraction():
    result = build_source_api_error_handling_requirements(
        _source_brief(
            summary="Use exponential backoff retry strategy with circuit breaker.",
            source_payload={
                "requirements": [
                    "Return 400 Bad Request and 500 Internal Server Error.",
                    "Error format includes error code field.",
                ],
            },
        )
    )

    by_type = {req.requirement_type: req for req in result.requirements}

    if "retry_strategies" in by_type:
        retry_req = by_type["retry_strategies"]
        assert len(retry_req.matched_terms) > 0
        assert any("backoff" in term.lower() or "retry" in term.lower() for term in retry_req.matched_terms)

    if "status_code_mapping" in by_type:
        status_req = by_type["status_code_mapping"]
        assert len(status_req.matched_terms) > 0


def _source_brief(
    *,
    source_id="error-handling-source",
    title="API error handling requirements",
    domain="platform",
    summary="General error handling requirements.",
    source_payload=None,
    source_links=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }
