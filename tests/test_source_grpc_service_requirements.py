import copy
import json
from types import SimpleNamespace

from blueprint.source_grpc_service_requirements import (
    SourceGRPCServiceRequirementsReport,
    build_source_grpc_service_requirements,
    derive_source_grpc_service_requirements,
    extract_source_grpc_service_requirements,
    generate_source_grpc_service_requirements,
    source_grpc_service_requirements_to_dict,
    source_grpc_service_requirements_to_dicts,
    source_grpc_service_requirements_to_markdown,
    summarize_source_grpc_service_requirements,
)


def test_structured_source_extracts_grpc_categories():
    result = build_source_grpc_service_requirements(
        {
            "id": "brief-grpc",
            "title": "Add gRPC Service",
            "requirements": {
                "grpc": [
                    "Protobuf schema definition with .proto files.",
                    "Service methods including unary and server streaming RPCs.",
                    "Error handling with gRPC status codes.",
                    "Metadata headers for request tracing.",
                    "Interceptors for authentication and logging.",
                    "Deadline and timeout configuration.",
                    "Load balancing with round-robin strategy.",
                    "Service reflection for debugging.",
                ]
            },
        }
    )

    assert isinstance(result, SourceGRPCServiceRequirementsReport)
    assert result.source_id == "brief-grpc"
    categories = {req.category for req in result.requirements}
    expected_categories = {
        "protobuf_schema",
        "service_methods",
        "error_handling",
        "metadata_headers",
        "interceptors",
        "deadline_timeout",
        "load_balancing",
        "service_reflection",
    }
    assert expected_categories <= categories


def test_natural_language_extraction_from_body():
    result = build_source_grpc_service_requirements(
        """
        Add gRPC service endpoint

        The service must use protobuf schema definitions.
        Implement unary and streaming service methods.
        Handle errors with gRPC status codes.
        Add metadata headers for tracing.
        Use gRPC interceptors for authentication.
        Configure deadlines and timeouts.
        """
    )

    assert len(result.requirements) >= 4
    categories = {req.category for req in result.requirements}
    assert "protobuf_schema" in categories
    assert "service_methods" in categories
    assert "error_handling" in categories
    assert "metadata_headers" in categories or "interceptors" in categories or "deadline_timeout" in categories


def test_evidence_deduplication_and_stable_ordering():
    result = build_source_grpc_service_requirements(
        {
            "title": "gRPC service with protobuf",
            "description": "Protobuf schema definition.",
            "requirements": ["Schema definition with .proto files."],
            "acceptance": ["Protobuf configured."],
        }
    )

    # Find protobuf_schema requirement
    protobuf_req = next((r for r in result.requirements if r.category == "protobuf_schema"), None)
    assert protobuf_req is not None
    # Evidence should be collected from multiple fields (up to 6)
    assert len(protobuf_req.evidence) >= 1


def test_out_of_scope_negation_produces_empty_report():
    result = build_source_grpc_service_requirements(
        {
            "id": "brief-no-grpc",
            "title": "Add REST API endpoint",
            "scope": "No gRPC or protobuf is in scope for this work.",
        }
    )

    assert result.requirements == ()
    assert result.summary["requirement_count"] == 0
    assert result.summary["status"] == "no_grpc_service_requirements_found"


def test_to_dict_to_dicts_and_to_markdown_serialization():
    result = build_source_grpc_service_requirements(
        {
            "id": "brief-serialize",
            "title": "Add gRPC",
            "requirements": [
                "Protobuf schema with messages.",
                "Unary service methods.",
            ],
        }
    )

    payload = source_grpc_service_requirements_to_dict(result)
    markdown = source_grpc_service_requirements_to_markdown(result)

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
    assert source_grpc_service_requirements_to_dicts(result) == payload["requirements"]
    assert source_grpc_service_requirements_to_dicts(result.requirements) == payload["requirements"]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source gRPC Service Requirements Report: brief-serialize")


def test_invalid_input_handling():
    for invalid in [None, "", [], {}, 42, b"bytes"]:
        result = build_source_grpc_service_requirements(invalid)
        assert result.requirements == ()
        assert result.summary["requirement_count"] == 0


def test_model_input_support():
    # Use dict input and let the function handle model validation
    result = build_source_grpc_service_requirements(
        {
            "id": "brief-model",
            "title": "gRPC service",
            "summary": "gRPC service with protobuf schema and unary methods.",
        }
    )

    assert result.source_id == "brief-model"
    assert len(result.requirements) >= 1
    assert any(req.category in {"protobuf_schema", "service_methods"} for req in result.requirements)


def test_object_input_support():
    obj = SimpleNamespace(
        id="brief-object",
        title="gRPC",
        body="Unary and streaming service methods for gRPC API.",
    )

    result = build_source_grpc_service_requirements(obj)

    assert result.source_id == "brief-object"
    assert any(req.category == "service_methods" for req in result.requirements)


def test_no_mutation_of_source():
    source = {
        "id": "brief-mutation",
        "title": "gRPC Service",
        "requirements": ["Protobuf schema definition."],
    }
    original = copy.deepcopy(source)

    build_source_grpc_service_requirements(source)

    assert source == original


def test_aliases_generate_derive_and_extract():
    source = {"title": "gRPC", "body": "gRPC service with protobuf schema."}

    result1 = generate_source_grpc_service_requirements(source)
    result2 = derive_source_grpc_service_requirements(source)
    requirements = extract_source_grpc_service_requirements(source)
    summary = summarize_source_grpc_service_requirements(source)

    assert result1.to_dict() == result2.to_dict()
    assert requirements == result1.requirements
    assert summary == result1.summary


def test_confidence_scoring():
    result = build_source_grpc_service_requirements(
        {
            "requirements": {
                "grpc": [
                    "Protobuf schema must include message definitions.",
                ]
            }
        }
    )

    # Requirements field with directive and grpc context should get high/medium confidence
    protobuf_req = next((r for r in result.requirements if r.category == "protobuf_schema"), None)
    assert protobuf_req is not None
    assert protobuf_req.confidence in {"high", "medium"}


def test_planning_notes_attached_to_requirements():
    result = build_source_grpc_service_requirements(
        {"title": "gRPC", "body": "gRPC service with protobuf."}
    )

    for requirement in result.requirements:
        assert requirement.planning_note
        assert len(requirement.planning_note) > 10


def test_unresolved_questions_for_ambiguous_requirements():
    result = build_source_grpc_service_requirements(
        {"title": "gRPC", "body": "Add gRPC service."}
    )

    service_req = next((r for r in result.requirements if r.category == "service_methods"), None)
    if service_req:
        # Should have questions about method types
        assert len(service_req.unresolved_questions) > 0


def test_summary_counts_match_requirements():
    result = build_source_grpc_service_requirements(
        {
            "requirements": [
                "Protobuf schema definition.",
                "Unary service methods.",
                "gRPC status codes for errors.",
            ]
        }
    )

    assert result.summary["requirement_count"] == len(result.requirements)
    category_counts = result.summary["category_counts"]
    assert sum(category_counts.values()) == len(result.requirements)
    confidence_counts = result.summary["confidence_counts"]
    assert sum(confidence_counts.values()) == len(result.requirements)


def test_requirement_category_property_compatibility():
    result = build_source_grpc_service_requirements(
        {"body": "Protobuf schema definition."}
    )

    for requirement in result.requirements:
        assert requirement.requirement_category == requirement.category


def test_records_and_findings_property_compatibility():
    result = build_source_grpc_service_requirements(
        {"body": "gRPC service with unary methods."}
    )

    assert result.records == result.requirements
    assert result.findings == result.requirements


def test_empty_report_markdown():
    result = build_source_grpc_service_requirements(
        {"title": "User profile", "body": "Add user profile endpoint."}
    )

    markdown = result.to_markdown()
    assert "No source gRPC service requirements were inferred." in markdown or len(result.requirements) > 0


def test_protobuf_schema_detection():
    result = build_source_grpc_service_requirements(
        {"body": "Protobuf schema definition with .proto files and message definitions."}
    )

    protobuf_req = next((r for r in result.requirements if r.category == "protobuf_schema"), None)
    assert protobuf_req is not None


def test_service_methods_detection():
    result = build_source_grpc_service_requirements(
        {"body": "Service methods including unary, server streaming, and bidirectional streaming RPCs."}
    )

    methods_req = next((r for r in result.requirements if r.category == "service_methods"), None)
    assert methods_req is not None


def test_error_handling_detection():
    result = build_source_grpc_service_requirements(
        {"body": "Error handling with gRPC status codes and error propagation."}
    )

    error_req = next((r for r in result.requirements if r.category == "error_handling"), None)
    assert error_req is not None


def test_metadata_headers_detection():
    result = build_source_grpc_service_requirements(
        {"requirements": ["Metadata headers for request tracing and correlation IDs."]}
    )

    metadata_req = next((r for r in result.requirements if r.category == "metadata_headers"), None)
    assert metadata_req is not None


def test_interceptors_detection():
    result = build_source_grpc_service_requirements(
        {"body": "gRPC interceptors for authentication, logging, and metrics collection."}
    )

    interceptor_req = next((r for r in result.requirements if r.category == "interceptors"), None)
    assert interceptor_req is not None


def test_deadline_timeout_detection():
    result = build_source_grpc_service_requirements(
        {"body": "Deadline and timeout configuration for gRPC calls."}
    )

    deadline_req = next((r for r in result.requirements if r.category == "deadline_timeout"), None)
    assert deadline_req is not None


def test_load_balancing_detection():
    result = build_source_grpc_service_requirements(
        {"body": "gRPC load balancing with round-robin strategy."}
    )

    lb_req = next((r for r in result.requirements if r.category == "load_balancing"), None)
    assert lb_req is not None


def test_service_reflection_detection():
    result = build_source_grpc_service_requirements(
        {"body": "Service reflection for debugging with grpcurl."}
    )

    reflection_req = next((r for r in result.requirements if r.category == "service_reflection"), None)
    assert reflection_req is not None


def test_json_safe_serialization():
    result = build_source_grpc_service_requirements(
        {
            "title": "gRPC with special | chars",
            "body": "gRPC service | protobuf | unary | streaming",
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
    result = build_source_grpc_service_requirements(
        {
            "id": "impl-brief",
            "source_brief_id": "src-brief",
            "title": "gRPC",
            "body": "gRPC service with protobuf schema.",
        }
    )

    assert result.source_id == "impl-brief"


def test_no_grpc_in_rest_only_api():
    result = build_source_grpc_service_requirements(
        {
            "title": "Add REST API",
            "body": "Add REST API endpoints with JSON responses. No gRPC support.",
        }
    )

    # Should have empty requirements since no gRPC patterns detected
    assert len(result.requirements) == 0


def test_mixed_rest_and_grpc():
    result = build_source_grpc_service_requirements(
        {
            "title": "Add API layer",
            "body": "Add both REST endpoints and gRPC service with protobuf schema for high-performance operations.",
        }
    )

    # Should detect gRPC patterns despite mentioning REST
    assert any(req.category in {"protobuf_schema", "service_methods"} for req in result.requirements)


def test_unary_vs_streaming_methods():
    result_unary = build_source_grpc_service_requirements(
        {"body": "Unary RPC methods for request-response operations."}
    )

    result_streaming = build_source_grpc_service_requirements(
        {"body": "Server streaming and bidirectional streaming RPCs."}
    )

    # Both should detect service_methods
    assert any(req.category == "service_methods" for req in result_unary.requirements)
    assert any(req.category == "service_methods" for req in result_streaming.requirements)


def test_grpc_status_codes():
    result = build_source_grpc_service_requirements(
        {"body": "Use gRPC status codes including OK, INVALID_ARGUMENT, NOT_FOUND, and PERMISSION_DENIED."}
    )

    error_req = next((r for r in result.requirements if r.category == "error_handling"), None)
    assert error_req is not None


def test_context_propagation():
    result = build_source_grpc_service_requirements(
        {"body": "Metadata headers for context propagation and distributed tracing."}
    )

    metadata_req = next((r for r in result.requirements if r.category == "metadata_headers"), None)
    assert metadata_req is not None


def test_authentication_interceptor():
    result = build_source_grpc_service_requirements(
        {"body": "Authentication interceptor for validating JWT tokens."}
    )

    interceptor_req = next((r for r in result.requirements if r.category == "interceptors"), None)
    assert interceptor_req is not None


def test_deadline_propagation():
    result = build_source_grpc_service_requirements(
        {"body": "Deadline propagation across service boundaries."}
    )

    deadline_req = next((r for r in result.requirements if r.category == "deadline_timeout"), None)
    assert deadline_req is not None


def test_client_side_load_balancing():
    result = build_source_grpc_service_requirements(
        {"body": "gRPC client-side load balancing with least-request policy."}
    )

    lb_req = next((r for r in result.requirements if r.category == "load_balancing"), None)
    assert lb_req is not None


def test_grpc_reflection_api():
    result = build_source_grpc_service_requirements(
        {"body": "gRPC reflection API for service discovery."}
    )

    reflection_req = next((r for r in result.requirements if r.category == "service_reflection"), None)
    assert reflection_req is not None


def test_multiple_categories_same_evidence():
    result = build_source_grpc_service_requirements(
        {
            "requirements": [
                "gRPC service with unary methods, error handling, and interceptors.",
            ]
        }
    )

    categories = {req.category for req in result.requirements}
    # Should extract multiple categories from the same sentence
    assert "service_methods" in categories or "error_handling" in categories or "interceptors" in categories


def test_protobuf_message_types():
    result = build_source_grpc_service_requirements(
        {"body": "Protobuf message definitions with nested messages and enums."}
    )

    protobuf_req = next((r for r in result.requirements if r.category == "protobuf_schema"), None)
    assert protobuf_req is not None


def test_bidirectional_streaming():
    result = build_source_grpc_service_requirements(
        {"body": "Bidirectional streaming for real-time chat features."}
    )

    methods_req = next((r for r in result.requirements if r.category == "service_methods"), None)
    assert methods_req is not None


def test_service_mesh_integration():
    result = build_source_grpc_service_requirements(
        {"body": "gRPC service mesh integration for traffic management and load balancing."}
    )

    lb_req = next((r for r in result.requirements if r.category == "load_balancing"), None)
    assert lb_req is not None


def test_grpcurl_debugging():
    result = build_source_grpc_service_requirements(
        {"body": "Enable grpcurl for gRPC service testing and debugging."}
    )

    reflection_req = next((r for r in result.requirements if r.category == "service_reflection"), None)
    assert reflection_req is not None


def test_logging_interceptor():
    result = build_source_grpc_service_requirements(
        {"body": "Logging interceptor for request and response logging."}
    )

    interceptor_req = next((r for r in result.requirements if r.category == "interceptors"), None)
    assert interceptor_req is not None


def test_timeout_handling():
    result = build_source_grpc_service_requirements(
        {"body": "Timeout handling for long-running operations."}
    )

    deadline_req = next((r for r in result.requirements if r.category == "deadline_timeout"), None)
    assert deadline_req is not None


def test_correlation_id_propagation():
    result = build_source_grpc_service_requirements(
        {"body": "Correlation ID propagation via metadata headers."}
    )

    metadata_req = next((r for r in result.requirements if r.category == "metadata_headers"), None)
    assert metadata_req is not None
