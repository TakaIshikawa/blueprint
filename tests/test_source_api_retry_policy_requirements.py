import json

from blueprint.domain.models import SourceBrief
from blueprint.source_api_retry_policy_requirements import (
    SourceApiRetryPolicyRequirement,
    SourceApiRetryPolicyRequirementsReport,
    build_source_api_retry_policy_requirements,
    extract_source_api_retry_policy_requirements,
    source_api_retry_policy_requirements_to_dict,
    source_api_retry_policy_requirements_to_dicts,
    source_api_retry_policy_requirements_to_markdown,
    summarize_source_api_retry_policy_requirements,
)


def test_extracts_multi_signal_retry_policy_requirements_with_evidence():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary=(
                "Implement API retry logic with exponential backoff and jitter. "
                "Limit retries to 3 attempts with idempotency validation."
            ),
            source_payload={
                "requirements": [
                    "Use exponential backoff with base delay of 100ms and max delay of 10 seconds.",
                    "Add full jitter to prevent thundering herd issues.",
                    "Validate idempotency keys before retrying non-GET requests.",
                    "Integrate with circuit breaker to fail fast when service is down.",
                ],
                "acceptance_criteria": [
                    "Retry on 5xx errors, network timeouts, and 429 rate limit responses.",
                    "Total retry timeout must not exceed 30 seconds.",
                    "Use backoff multiplier of 2x between attempts.",
                ],
            },
        )
    )

    assert isinstance(result, SourceApiRetryPolicyRequirementsReport)
    assert all(isinstance(record, SourceApiRetryPolicyRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "exponential_backoff",
        "jitter_strategy",
        "max_retry_attempts",
        "retry_timeout",
        "idempotency_validation",
        "circuit_breaker_integration",
        "retry_condition",
        "backoff_multiplier",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert any("exponential" in item.lower() or "backoff" in item.lower() for item in by_type["exponential_backoff"].evidence)
    assert any("jitter" in item.lower() for item in by_type["jitter_strategy"].evidence)
    assert any("3 attempts" in item or "limit" in item.lower() for item in by_type["max_retry_attempts"].evidence)
    assert any("circuit breaker" in item.lower() for item in by_type["circuit_breaker_integration"].evidence)
    assert result.summary["requirement_count"] == 8
    assert result.summary["type_counts"]["exponential_backoff"] == 1
    assert result.summary["backoff_coverage"] > 0
    assert result.summary["safety_coverage"] > 0
    assert result.summary["control_coverage"] > 0


def test_brief_without_retry_language_returns_stable_empty_report():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            title="Database migration",
            summary="Add new column to users table with default value.",
            source_payload={
                "requirements": [
                    "Create migration script for schema change.",
                    "Ensure backward compatibility with existing code.",
                ],
            },
        )
    )
    repeat = build_source_api_retry_policy_requirements(
        _source_brief(
            title="Database migration",
            summary="Add new column to users table with default value.",
            source_payload={
                "requirements": [
                    "Create migration script for schema change.",
                    "Ensure backward compatibility with existing code.",
                ],
            },
        )
    )

    expected_summary = {
        "requirement_count": 0,
        "source_count": 1,
        "type_counts": {
            "exponential_backoff": 0,
            "jitter_strategy": 0,
            "max_retry_attempts": 0,
            "retry_timeout": 0,
            "idempotency_validation": 0,
            "circuit_breaker_integration": 0,
            "retry_condition": 0,
            "backoff_multiplier": 0,
        },
        "requirement_types": [],
        "follow_up_question_count": 0,
        "backoff_coverage": 0,
        "safety_coverage": 0,
        "control_coverage": 0,
    }
    assert result.summary == expected_summary
    assert result.requirements == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.to_dict() == repeat.to_dict()


def test_exponential_backoff_requirements_detected():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Implement exponential backoff strategy for API retries.",
            source_payload={
                "requirements": [
                    "Use exponential backoff starting with 100ms base delay.",
                    "Double the retry interval after each failed attempt.",
                    "Cap maximum backoff delay at 30 seconds.",
                    "Use progressive delay to reduce server load during outages.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "exponential_backoff" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    backoff_req = by_type["exponential_backoff"]
    assert any("exponential" in term.lower() or "backoff" in term.lower() for term in backoff_req.matched_terms)
    # Should have reduced questions since base delay is mentioned
    assert len(backoff_req.follow_up_questions) < 2


def test_jitter_strategy_requirements_detected():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Add jitter to retry backoff to prevent thundering herd.",
            source_payload={
                "requirements": [
                    "Use full jitter strategy for retry delays.",
                    "Add random offset to backoff intervals to decorrelate retry attempts.",
                    "Randomize delay between 0 and calculated backoff value.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "jitter_strategy" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    jitter_req = by_type["jitter_strategy"]
    assert any("jitter" in term.lower() or "random" in term.lower() for term in jitter_req.matched_terms)
    # Should have reduced questions since full jitter is specified
    assert len(jitter_req.follow_up_questions) <= 1


def test_max_retry_attempts_requirements_detected():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Limit API retry attempts to prevent infinite loops.",
            source_payload={
                "requirements": [
                    "Maximum of 3 retry attempts before giving up.",
                    "Stop retrying after 5 failed attempts for non-critical requests.",
                    "Set retry limit to prevent resource exhaustion.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "max_retry_attempts" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    max_req = by_type["max_retry_attempts"]
    assert any("3 retry" in item or "5 failed" in item or "limit" in item.lower() for item in max_req.evidence)
    # Should have no questions since specific numbers are mentioned
    assert len(max_req.follow_up_questions) == 0


def test_retry_timeout_requirements_detected():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Configure retry timeout limits for API operations.",
            source_payload={
                "requirements": [
                    "Total retry timeout must not exceed 60 seconds.",
                    "Set overall timeout of 30 seconds across all retry attempts.",
                    "Individual request timeout of 10 seconds per attempt.",
                    "Use time budget to limit total retry duration.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "retry_timeout" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    timeout_req = by_type["retry_timeout"]
    assert any("timeout" in item.lower() or "60 seconds" in item or "30 seconds" in item for item in timeout_req.evidence)


def test_idempotency_validation_requirements_detected():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Ensure safe retries with idempotency validation.",
            source_payload={
                "requirements": [
                    "Validate idempotency keys before retrying POST, PUT, DELETE requests.",
                    "Use idempotency tokens to prevent duplicate request processing.",
                    "Only retry idempotent operations automatically.",
                    "Implement replay protection for non-idempotent mutations.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "idempotency_validation" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    idempotent_req = by_type["idempotency_validation"]
    assert any("idempoten" in term.lower() for term in idempotent_req.matched_terms)


def test_circuit_breaker_integration_requirements_detected():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Integrate retry logic with circuit breaker pattern.",
            source_payload={
                "requirements": [
                    "Fail fast when circuit breaker is open instead of retrying.",
                    "Trip circuit breaker after 5 consecutive failures.",
                    "Coordinate retry policy with circuit breaker state.",
                    "Enter half-open state to test service recovery before full retry resumption.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "circuit_breaker_integration" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    breaker_req = by_type["circuit_breaker_integration"]
    assert any("circuit" in term.lower() or "breaker" in term.lower() for term in breaker_req.matched_terms)


def test_retry_condition_requirements_detected():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Define which errors should trigger automatic retries.",
            source_payload={
                "requirements": [
                    "Retry on 5xx server errors (500, 502, 503, 504).",
                    "Retry on network timeouts and connection reset errors.",
                    "Retry when rate limited (429 status code).",
                    "Only retry transient failures, not client errors (4xx).",
                    "Handle connection errors and temporary failures with retry.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "retry_condition" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    condition_req = by_type["retry_condition"]
    assert any("5xx" in item or "500" in item or "503" in item or "429" in item for item in condition_req.evidence)


def test_backoff_multiplier_requirements_detected():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Configure backoff multiplication factor for retries.",
            source_payload={
                "requirements": [
                    "Use backoff multiplier of 2x between retry attempts.",
                    "Double the delay after each failed retry.",
                    "Base delay of 100ms with multiplication factor of 2.",
                    "Initial retry interval of 1 second, increasing exponentially.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "backoff_multiplier" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    multiplier_req = by_type["backoff_multiplier"]
    assert any("2x" in item or "double" in item.lower() or "factor" in item.lower() for item in multiplier_req.evidence)
    # Should have reduced questions since specific multiplier is mentioned
    assert len(multiplier_req.follow_up_questions) < 2


def test_infinite_retry_risk_edge_case():
    """Test detection of potentially infinite retry configurations."""
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Retry failed API requests indefinitely until success.",
            source_payload={
                "requirements": [
                    "Keep retrying failed requests with exponential backoff.",
                    "Never give up on failed requests, retry until successful.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    # Should detect backoff but NOT max attempts (infinite retry risk)
    assert "exponential_backoff" in types
    assert "max_retry_attempts" not in types
    # Should have follow-up questions about max attempts
    assert result.summary["follow_up_question_count"] > 0


def test_various_retry_scenarios_with_mixed_configs():
    """Test edge cases with various retry scenario combinations."""
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Complex retry strategy with multiple configurations.",
            source_payload={
                "requirements": [
                    "Use exponential backoff with equal jitter for API retries.",
                    "Retry up to 5 times for read operations, 2 times for writes.",
                    "Set per-request timeout of 5 seconds, total timeout of 30 seconds.",
                    "Only retry GET requests automatically; require manual retry for mutations.",
                ],
                "constraints": [
                    "Circuit breaker must prevent retries when service is degraded.",
                    "Idempotency keys required for all POST/PUT/DELETE retry attempts.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    # Should detect multiple retry policy aspects
    assert "exponential_backoff" in types
    assert "jitter_strategy" in types
    assert "max_retry_attempts" in types
    assert "retry_timeout" in types
    assert "idempotency_validation" in types
    assert "circuit_breaker_integration" in types
    assert result.summary["requirement_count"] >= 6


def test_empty_config_returns_no_requirements():
    """Test edge case with empty or minimal configuration."""
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Simple API endpoint with no retry logic.",
            source_payload={
                "requirements": ["Return data directly without any error handling."],
            },
        )
    )

    assert result.summary["requirement_count"] == 0
    assert result.requirements == ()
    assert result.summary["backoff_coverage"] == 0
    assert result.summary["safety_coverage"] == 0
    assert result.summary["control_coverage"] == 0


def test_requirement_deduplication_merges_evidence_without_losing_source_fields():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Use exponential backoff for API retries with jitter.",
            source_payload={
                "requirements": [
                    "Implement exponential backoff with base delay of 100ms.",
                    "Use exponential delay doubling after each retry attempt.",
                ],
                "acceptance": "All retries must use exponential backoff strategy.",
            },
        )
    )

    by_type = {record.requirement_type: record for record in result.records}
    backoff_req = by_type["exponential_backoff"]
    # Multiple source fields should be captured
    assert len(backoff_req.source_field_paths) >= 2
    assert "summary" in backoff_req.source_field_paths
    assert any("requirements" in field for field in backoff_req.source_field_paths)
    assert len(backoff_req.evidence) >= 2
    assert any("exponential" in evidence.lower() or "backoff" in evidence.lower() for evidence in backoff_req.evidence)


def test_dict_serialization_round_trips_without_mutation():
    original = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Retry with exponential backoff and 3 max attempts.",
            source_payload={
                "requirements": [
                    "Use exponential backoff starting at 100ms.",
                    "Maximum of 3 retry attempts before failure.",
                ],
            },
        )
    )

    serialized = original.to_dict()
    assert isinstance(serialized, dict)
    assert serialized["source_brief_id"] == "retry-source"
    assert len(serialized["requirements"]) == len(original.requirements)
    assert serialized["summary"]["requirement_count"] == len(original.requirements)

    # Repeat to verify no mutation
    repeat = original.to_dict()
    assert repeat == serialized


def test_to_dicts_helper_serializes_requirements_list():
    report = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Retry policy with backoff and timeout.",
            source_payload={
                "requirements": [
                    "Exponential backoff with 2x multiplier.",
                    "Total timeout of 60 seconds.",
                ],
            },
        )
    )

    dicts = source_api_retry_policy_requirements_to_dicts(report)
    assert isinstance(dicts, list)
    assert all(isinstance(item, dict) for item in dicts)
    assert len(dicts) == report.summary["requirement_count"]

    # Also test tuple input
    tuple_dicts = source_api_retry_policy_requirements_to_dicts(report.requirements)
    assert tuple_dicts == dicts


def test_markdown_output_renders_deterministic_table():
    report = build_source_api_retry_policy_requirements(
        _source_brief(
            source_id="retry-markdown-test",
            summary="API retry policy with exponential backoff and jitter.",
            source_payload={
                "requirements": [
                    "Use exponential backoff with full jitter.",
                    "Maximum 5 retry attempts.",
                ],
            },
        )
    )

    markdown = source_api_retry_policy_requirements_to_markdown(report)
    assert isinstance(markdown, str)
    assert "# Source API Retry Policy Requirements Report: retry-markdown-test" in markdown
    assert "## Summary" in markdown
    assert "## Requirements" in markdown
    assert "| Type | Source Field Paths | Evidence | Follow-up Questions |" in markdown
    assert "exponential_backoff" in markdown

    # Repeat to verify deterministic output
    repeat_markdown = report.to_markdown()
    assert repeat_markdown == markdown


def test_empty_report_markdown_includes_no_requirements_message():
    report = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Simple data retrieval with no retry logic.",
        )
    )

    markdown = report.to_markdown()
    assert "No source API retry policy requirements were inferred." in markdown
    assert "## Requirements" not in markdown


def test_extracts_from_raw_text_input():
    result = build_source_api_retry_policy_requirements(
        "Implement API retries with exponential backoff, jitter strategy, and maximum 3 attempts. "
        "Validate idempotency before retrying mutations."
    )

    assert len(result.requirements) >= 3
    types = {req.requirement_type for req in result.requirements}
    assert "exponential_backoff" in types
    assert "jitter_strategy" in types
    assert "max_retry_attempts" in types


def test_extracts_from_mapping_input():
    result = build_source_api_retry_policy_requirements(
        {
            "id": "mapping-source",
            "title": "Retry policy requirements",
            "summary": "API retry with exponential backoff and circuit breaker integration.",
            "source_payload": {
                "requirements": "Use jitter to prevent thundering herd, max 5 retries.",
            },
        }
    )

    assert result.source_brief_id == "mapping-source"
    types = {req.requirement_type for req in result.requirements}
    assert "exponential_backoff" in types
    assert "jitter_strategy" in types
    assert "circuit_breaker_integration" in types


def test_extracts_from_pydantic_model():
    model = SourceBrief(
        id="pydantic-source",
        title="Retry policy requirements",
        domain="api",
        summary="Implement retry logic with exponential backoff, jitter, and idempotency validation.",
        source_project="test",
        source_entity_type="issue",
        source_id="pydantic-source",
        source_payload={
            "requirements": "Retry on 5xx errors with circuit breaker integration.",
        },
        source_links={},
    )

    result = build_source_api_retry_policy_requirements(model)
    assert result.source_brief_id == "pydantic-source"
    types = {req.requirement_type for req in result.requirements}
    assert "exponential_backoff" in types
    assert "jitter_strategy" in types
    assert "retry_condition" in types


def test_extract_helper_returns_tuple_of_requirements():
    requirements = extract_source_api_retry_policy_requirements(
        _source_brief(
            summary="Retry with exponential backoff and jitter.",
        )
    )

    assert isinstance(requirements, tuple)
    assert all(isinstance(req, SourceApiRetryPolicyRequirement) for req in requirements)
    assert len(requirements) >= 1


def test_summarize_helper_returns_summary_dict():
    summary = summarize_source_api_retry_policy_requirements(
        _source_brief(
            summary="Retry policy with backoff and max attempts.",
        )
    )

    assert isinstance(summary, dict)
    assert "requirement_count" in summary
    assert "type_counts" in summary
    assert summary["requirement_count"] >= 1


def test_summarize_accepts_report_object():
    report = build_source_api_retry_policy_requirements(
        _source_brief(summary="API retry configuration.")
    )
    summary = summarize_source_api_retry_policy_requirements(report)

    assert summary == report.summary


def test_coverage_metrics_calculated_correctly():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Comprehensive retry policy with all features.",
            source_payload={
                "requirements": [
                    "Use exponential backoff with full jitter and 2x multiplier.",
                    "Maximum 5 retry attempts with 30 second total timeout.",
                    "Validate idempotency for non-GET requests.",
                    "Integrate with circuit breaker for fail-fast behavior.",
                    "Retry on 5xx errors and network timeouts.",
                ],
            },
        )
    )

    summary = result.summary
    # Backoff requirements present (exponential_backoff, jitter_strategy, backoff_multiplier)
    assert summary["backoff_coverage"] == 100
    # Safety requirements present (idempotency_validation, circuit_breaker_integration)
    assert summary["safety_coverage"] == 100
    # Control requirements present (max_retry_attempts, retry_timeout, retry_condition)
    assert summary["control_coverage"] == 100


def test_follow_up_questions_reduced_when_evidence_is_specific():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Use exponential backoff with 100ms base delay and full jitter.",
            source_payload={
                "requirements": [
                    "Maximum 3 retry attempts before giving up.",
                    "Backoff multiplier of 2.5x between attempts.",
                ],
            },
        )
    )

    by_type = {req.requirement_type: req for req in result.requirements}
    # Exponential backoff mentions base delay, should have fewer questions
    backoff = by_type.get("exponential_backoff")
    if backoff:
        assert len(backoff.follow_up_questions) < 2
    # Jitter mentions specific type (full), should have fewer questions
    jitter = by_type.get("jitter_strategy")
    if jitter:
        assert len(jitter.follow_up_questions) <= 1
    # Max attempts mentions specific number, should have no questions
    max_attempts = by_type.get("max_retry_attempts")
    if max_attempts:
        assert len(max_attempts.follow_up_questions) == 0
    # Backoff multiplier mentions specific value, should have fewer questions
    multiplier = by_type.get("backoff_multiplier")
    if multiplier:
        assert len(multiplier.follow_up_questions) < 2


def test_matched_terms_captured_for_each_requirement():
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Retry with exponential backoff, jitter, and circuit breaker integration.",
        )
    )

    for req in result.requirements:
        assert len(req.matched_terms) > 0
        # Matched terms should be deduplicated
        assert len(req.matched_terms) == len(set(term.casefold() for term in req.matched_terms))


def test_transient_error_patterns_detected():
    """Test detection of various transient error patterns."""
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="Handle transient failures gracefully.",
            source_payload={
                "requirements": [
                    "Retry on transient network errors.",
                    "Handle temporary service unavailability with retry.",
                    "Retry throttled requests (429 status).",
                    "Retry on connection timeout and connection reset.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "retry_condition" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    condition_req = by_type["retry_condition"]
    assert any(
        "transient" in item.lower() or "429" in item or "timeout" in item.lower()
        for item in condition_req.evidence
    )


def test_nested_metadata_extraction():
    """Test extraction from nested metadata structures."""
    result = build_source_api_retry_policy_requirements(
        _source_brief(
            summary="API retry policy.",
            metadata={
                "retry_config": {
                    "strategy": "exponential backoff with jitter",
                    "max_attempts": "5 retry attempts",
                },
                "timeout": "30 seconds total timeout",
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "exponential_backoff" in types
    assert "jitter_strategy" in types
    assert "max_retry_attempts" in types
    assert "retry_timeout" in types


def test_nested_list_in_source_payload():
    """Test extraction from nested lists in source_payload."""
    result = build_source_api_retry_policy_requirements(
        {
            "id": "list-source",
            "summary": "Retry with exponential backoff",
            "source_payload": {
                "requirements": [
                    "Use exponential backoff strategy",
                    "Maximum 3 retry attempts before giving up",
                ],
            },
        }
    )

    types = {req.requirement_type for req in result.requirements}
    assert "exponential_backoff" in types
    assert "max_retry_attempts" in types


def test_extraction_from_object_with_attributes():
    """Test extraction from an object with attributes."""

    class RetryConfig:
        def __init__(self):
            self.id = "object-source"
            self.summary = "Retry with exponential backoff"
            self.requirements = "Use jitter and circuit breaker"

    result = build_source_api_retry_policy_requirements(RetryConfig())

    assert result.source_brief_id == "object-source"
    types = {req.requirement_type for req in result.requirements}
    assert "exponential_backoff" in types
    assert "jitter_strategy" in types
    assert "circuit_breaker_integration" in types


def test_source_with_set_values():
    """Test extraction from source with set values."""
    result = build_source_api_retry_policy_requirements(
        {
            "id": "set-source",
            "summary": "Retry policy",
            "requirements": {
                "Use exponential backoff",
                "Maximum 3 retry attempts",
                "Add jitter to prevent thundering herd",
            },
        }
    )

    types = {req.requirement_type for req in result.requirements}
    assert "exponential_backoff" in types
    assert "max_retry_attempts" in types
    assert "jitter_strategy" in types


def test_bytes_and_bytearray_inputs_return_empty():
    """Test that bytes and bytearray inputs return empty results."""
    result_bytes = build_source_api_retry_policy_requirements(b"retry policy")
    result_bytearray = build_source_api_retry_policy_requirements(bytearray(b"retry"))

    assert result_bytes.requirements == ()
    assert result_bytearray.requirements == ()


def test_source_brief_id_extraction_priority():
    """Test source_brief_id extraction from different field names."""
    # Test with id field
    result1 = build_source_api_retry_policy_requirements(
        {"id": "id-field", "summary": "retry policy"}
    )
    assert result1.source_brief_id == "id-field"

    # Test with source_brief_id field
    result2 = build_source_api_retry_policy_requirements(
        {"source_brief_id": "brief-id", "summary": "retry policy"}
    )
    assert result2.source_brief_id == "brief-id"

    # Test with source_id field
    result3 = build_source_api_retry_policy_requirements(
        {"source_id": "source-id", "summary": "retry policy"}
    )
    assert result3.source_brief_id == "source-id"


def _source_brief(
    *,
    source_id="retry-source",
    title="Retry policy requirements",
    domain="platform",
    summary="General retry policy requirements.",
    source_payload=None,
    source_links=None,
    metadata=None,
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
        "metadata": {} if metadata is None else metadata,
        "created_at": None,
        "updated_at": None,
    }
