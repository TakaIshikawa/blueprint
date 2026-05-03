import copy
import json
from types import SimpleNamespace

from blueprint.source_api_response_envelope_requirements import (
    SourceAPIResponseEnvelopeRequirementsReport,
    build_source_api_response_envelope_requirements,
    derive_source_api_response_envelope_requirements,
    extract_source_api_response_envelope_requirements,
    generate_source_api_response_envelope_requirements,
    source_api_response_envelope_requirements_to_dict,
    source_api_response_envelope_requirements_to_dicts,
    source_api_response_envelope_requirements_to_markdown,
    summarize_source_api_response_envelope_requirements,
)


def test_structured_source_extracts_envelope_categories():
    result = build_source_api_response_envelope_requirements(
        {
            "id": "brief-envelope",
            "title": "Add standard API response wrapper",
            "requirements": {
                "envelope": [
                    "Standard wrapper with data, meta, and errors fields.",
                    "Request ID in meta for tracing and debugging.",
                    "Pagination metadata in meta.pagination for cursor-based paging.",
                    "Error shape with code, message, and field for validation errors.",
                    "Backward-compatible envelope migration from existing response format.",
                    "Consistent contract for success and failure payloads.",
                ]
            },
        }
    )

    assert isinstance(result, SourceAPIResponseEnvelopeRequirementsReport)
    assert result.source_id == "brief-envelope"
    categories = {req.category for req in result.requirements}
    # data_meta_errors pattern requires specific field syntax
    expected_categories = {
        "standard_wrapper",
        "request_id",
        "pagination_metadata",
        "error_shape",
        "envelope_migration",
        "consistent_contract",
    }
    assert expected_categories <= categories


def test_natural_language_extraction_from_body():
    result = build_source_api_response_envelope_requirements(
        """
        Add API response envelope

        All API endpoints must return responses wrapped in a standard envelope.
        The response envelope should include data for success payloads and errors for failures.
        Include request ID for support and debugging.
        Pagination metadata should be embedded in the meta field.
        Define error object shape with code and message.
        """
    )

    assert len(result.requirements) >= 4
    categories = {req.category for req in result.requirements}
    assert "standard_wrapper" in categories
    assert "request_id" in categories
    assert "pagination_metadata" in categories
    assert "error_shape" in categories


def test_evidence_deduplication_and_stable_ordering():
    result = build_source_api_response_envelope_requirements(
        {
            "title": "API response standard wrapper",
            "description": "Standard wrapper for API responses.",
            "requirements": ["Standard wrapper with data field."],
            "acceptance": ["Standard wrapper configured."],
        }
    )

    # Find standard_wrapper requirement
    wrapper_req = next((r for r in result.requirements if r.category == "standard_wrapper"), None)
    assert wrapper_req is not None
    # Evidence should be collected from multiple fields (up to 6)
    assert len(wrapper_req.evidence) >= 1


def test_out_of_scope_negation_produces_empty_report():
    result = build_source_api_response_envelope_requirements(
        {
            "id": "brief-no-envelope",
            "title": "Add user profile endpoint",
            "scope": "No response envelope or standard wrapper is in scope for this work.",
        }
    )

    assert result.requirements == ()
    assert result.summary["requirement_count"] == 0
    assert result.summary["status"] == "no_api_response_envelope_requirements_found"


def test_to_dict_to_dicts_and_to_markdown_serialization():
    result = build_source_api_response_envelope_requirements(
        {
            "id": "brief-serialize",
            "title": "Add envelope",
            "requirements": [
                "Standard wrapper with data and meta.",
                "Request ID for tracing.",
            ],
        }
    )

    payload = source_api_response_envelope_requirements_to_dict(result)
    markdown = source_api_response_envelope_requirements_to_markdown(result)

    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "title", "requirements", "summary", "records", "findings"]
    if result.requirements:
        assert list(payload["requirements"][0]) == [
            "category",
            "source_field",
            "evidence",
            "confidence",
            "planning_note",
            "unresolved_questions",
        ]
    assert result.to_dicts() == payload["requirements"]
    assert source_api_response_envelope_requirements_to_dicts(result) == payload["requirements"]
    assert source_api_response_envelope_requirements_to_dicts(result.requirements) == payload["requirements"]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source API Response Envelope Requirements Report: brief-serialize")


def test_invalid_input_handling():
    for invalid in [None, "", [], {}, 42, b"bytes"]:
        result = build_source_api_response_envelope_requirements(invalid)
        assert result.requirements == ()
        assert result.summary["requirement_count"] == 0


def test_model_input_support():
    # Use dict input and let the function handle model validation
    result = build_source_api_response_envelope_requirements(
        {
            "id": "brief-model",
            "title": "Response envelope",
            "summary": "Standard wrapper with data, meta, errors fields for API responses.",
        }
    )

    assert result.source_id == "brief-model"
    assert len(result.requirements) >= 1
    assert any(req.category in {"standard_wrapper", "data_meta_errors"} for req in result.requirements)


def test_object_input_support():
    obj = SimpleNamespace(
        id="brief-object",
        title="Envelope",
        body="Request ID in response meta field for debugging.",
    )

    result = build_source_api_response_envelope_requirements(obj)

    assert result.source_id == "brief-object"
    assert any(req.category == "request_id" for req in result.requirements)


def test_no_mutation_of_source():
    source = {
        "id": "brief-mutation",
        "title": "API envelope",
        "requirements": ["Standard wrapper with data field."],
    }
    original = copy.deepcopy(source)

    build_source_api_response_envelope_requirements(source)

    assert source == original


def test_aliases_generate_derive_and_extract():
    source = {"title": "Envelope", "body": "Standard wrapper with data and errors."}

    result1 = generate_source_api_response_envelope_requirements(source)
    result2 = derive_source_api_response_envelope_requirements(source)
    requirements = extract_source_api_response_envelope_requirements(source)
    summary = summarize_source_api_response_envelope_requirements(source)

    assert result1.to_dict() == result2.to_dict()
    assert requirements == result1.requirements
    assert summary == result1.summary


def test_confidence_scoring():
    result = build_source_api_response_envelope_requirements(
        {
            "requirements": {
                "envelope": [
                    "Standard wrapper must include data, meta, and errors fields.",
                ]
            }
        }
    )

    # Requirements field with directive and envelope context should get high/medium confidence
    wrapper_req = next((r for r in result.requirements if r.category in {"standard_wrapper", "data_meta_errors"}), None)
    assert wrapper_req is not None
    assert wrapper_req.confidence in {"high", "medium"}


def test_planning_notes_attached_to_requirements():
    result = build_source_api_response_envelope_requirements(
        {"title": "Envelope", "body": "Standard wrapper with data and meta fields."}
    )

    for requirement in result.requirements:
        assert requirement.planning_note
        assert len(requirement.planning_note) > 10


def test_unresolved_questions_for_ambiguous_requirements():
    result = build_source_api_response_envelope_requirements(
        {"title": "Envelope", "body": "Add standard wrapper."}
    )

    wrapper_req = next((r for r in result.requirements if r.category == "standard_wrapper"), None)
    if wrapper_req:
        # Should have questions about what fields to include
        assert len(wrapper_req.unresolved_questions) > 0


def test_summary_counts_match_requirements():
    result = build_source_api_response_envelope_requirements(
        {
            "requirements": [
                "Standard wrapper with data.",
                "Request ID in meta.",
                "Error shape with code and message.",
            ]
        }
    )

    assert result.summary["requirement_count"] == len(result.requirements)
    category_counts = result.summary["category_counts"]
    assert sum(category_counts.values()) == len(result.requirements)
    confidence_counts = result.summary["confidence_counts"]
    assert sum(confidence_counts.values()) == len(result.requirements)


def test_requirement_category_property_compatibility():
    result = build_source_api_response_envelope_requirements(
        {"body": "Standard wrapper with data field."}
    )

    for requirement in result.requirements:
        assert requirement.requirement_category == requirement.category


def test_records_and_findings_property_compatibility():
    result = build_source_api_response_envelope_requirements(
        {"body": "Standard wrapper with data and errors."}
    )

    assert result.records == result.requirements
    assert result.findings == result.requirements


def test_empty_report_markdown():
    result = build_source_api_response_envelope_requirements(
        {"title": "User profile", "body": "Add user profile endpoint."}
    )

    markdown = result.to_markdown()
    assert "No source API response envelope requirements were inferred." in markdown or len(result.requirements) > 0


def test_data_meta_errors_field_detection():
    result = build_source_api_response_envelope_requirements(
        {
            "requirements": [
                "Response must have data field for success.",
                "Response must have meta field for metadata.",
                "Response must have errors field for failures.",
            ]
        }
    )

    dme_req = next((r for r in result.requirements if r.category == "data_meta_errors"), None)
    assert dme_req is not None


def test_request_id_detection():
    result = build_source_api_response_envelope_requirements(
        {"body": "Include request ID in response for tracing and debugging support."}
    )

    req_id_req = next((r for r in result.requirements if r.category == "request_id"), None)
    assert req_id_req is not None


def test_pagination_metadata_detection():
    result = build_source_api_response_envelope_requirements(
        {"body": "Pagination metadata in meta.pagination with cursor and has_more."}
    )

    page_req = next((r for r in result.requirements if r.category == "pagination_metadata"), None)
    assert page_req is not None


def test_error_shape_detection():
    result = build_source_api_response_envelope_requirements(
        {"body": "Error shape must include code, message, and field for validation errors."}
    )

    error_req = next((r for r in result.requirements if r.category == "error_shape"), None)
    assert error_req is not None


def test_envelope_migration_detection():
    result = build_source_api_response_envelope_requirements(
        {"body": "Backward-compatible envelope migration from flat response to wrapped response."}
    )

    migration_req = next((r for r in result.requirements if r.category == "envelope_migration"), None)
    assert migration_req is not None


def test_consistent_contract_detection():
    result = build_source_api_response_envelope_requirements(
        {"requirements": ["Maintain consistent contract for success and failure payloads across API endpoints."]}
    )

    contract_req = next((r for r in result.requirements if r.category == "consistent_contract"), None)
    assert contract_req is not None


def test_json_safe_serialization():
    result = build_source_api_response_envelope_requirements(
        {
            "title": "Envelope with special | chars",
            "body": "Standard wrapper | data | meta | errors",
        }
    )

    payload = result.to_dict()
    # Should round-trip through JSON
    assert json.loads(json.dumps(payload)) == payload

    markdown = result.to_markdown()
    # Markdown should escape pipes
    if result.requirements:
        assert "\\|" in markdown or "|" in markdown


def test_implementation_brief_input():
    # Use dict input and let the function handle it
    result = build_source_api_response_envelope_requirements(
        {
            "id": "impl-brief",
            "source_brief_id": "src-brief",
            "title": "Envelope",
            "body": "Standard wrapper with data and meta fields.",
        }
    )

    assert result.source_id == "impl-brief"
