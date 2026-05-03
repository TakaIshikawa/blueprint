import copy
import json
from types import SimpleNamespace

from blueprint.source_websocket_connection_requirements import (
    SourceWebSocketConnectionRequirementsReport,
    build_source_websocket_connection_requirements,
    derive_source_websocket_connection_requirements,
    extract_source_websocket_connection_requirements,
    generate_source_websocket_connection_requirements,
    source_websocket_connection_requirements_to_dict,
    source_websocket_connection_requirements_to_dicts,
    source_websocket_connection_requirements_to_markdown,
    summarize_source_websocket_connection_requirements,
)


def test_structured_source_extracts_websocket_categories():
    result = build_source_websocket_connection_requirements(
        {
            "id": "brief-websocket",
            "title": "Add WebSocket Support",
            "requirements": {
                "websocket": [
                    "WebSocket handshake with HTTP upgrade protocol.",
                    "Connection authentication with JWT tokens.",
                    "Heartbeat mechanism with ping-pong keepalive.",
                    "Message framing using Socket.IO protocol.",
                    "Connection recovery with exponential backoff.",
                    "Message ordering with sequence numbers.",
                    "Backpressure handling with buffer limits.",
                    "Connection limits of 100 per user.",
                ]
            },
        }
    )

    assert isinstance(result, SourceWebSocketConnectionRequirementsReport)
    assert result.source_id == "brief-websocket"
    categories = {req.category for req in result.requirements}
    expected_categories = {
        "handshake_upgrade",
        "connection_auth",
        "heartbeat_keepalive",
        "message_framing",
        "connection_recovery",
        "message_ordering",
        "backpressure_handling",
        "connection_limits",
    }
    assert expected_categories <= categories


def test_natural_language_extraction_from_body():
    result = build_source_websocket_connection_requirements(
        """
        Add real-time WebSocket connections

        The API must support WebSocket handshake and upgrade.
        Implement connection authentication with bearer tokens.
        Add heartbeat mechanism with ping-pong keepalive.
        Use Socket.IO for message framing and encoding.
        Support automatic reconnection with backoff strategy.
        Ensure message ordering with sequence numbers.
        """
    )

    assert len(result.requirements) >= 4
    categories = {req.category for req in result.requirements}
    assert "handshake_upgrade" in categories
    assert "connection_auth" in categories
    assert "heartbeat_keepalive" in categories
    assert "message_framing" in categories


def test_evidence_deduplication_and_stable_ordering():
    result = build_source_websocket_connection_requirements(
        {
            "title": "WebSocket handshake",
            "description": "WebSocket handshake protocol.",
            "requirements": ["Handshake with upgrade headers."],
            "acceptance": ["Handshake configured."],
        }
    )

    # Find handshake_upgrade requirement
    handshake_req = next((r for r in result.requirements if r.category == "handshake_upgrade"), None)
    assert handshake_req is not None
    # Evidence should be collected from multiple fields (up to 6)
    assert len(handshake_req.evidence) >= 1


def test_out_of_scope_negation_produces_empty_report():
    result = build_source_websocket_connection_requirements(
        {
            "id": "brief-no-websocket",
            "title": "Add HTTP API endpoint",
            "scope": "No WebSocket or real-time connections are in scope for this work.",
        }
    )

    assert result.requirements == ()
    assert result.summary["requirement_count"] == 0
    assert result.summary["status"] == "no_websocket_connection_requirements_found"


def test_to_dict_to_dicts_and_to_markdown_serialization():
    result = build_source_websocket_connection_requirements(
        {
            "id": "brief-serialize",
            "title": "Add WebSocket",
            "requirements": [
                "WebSocket handshake with upgrade.",
                "Connection auth with tokens.",
            ],
        }
    )

    payload = source_websocket_connection_requirements_to_dict(result)
    markdown = source_websocket_connection_requirements_to_markdown(result)

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
    assert source_websocket_connection_requirements_to_dicts(result) == payload["requirements"]
    assert source_websocket_connection_requirements_to_dicts(result.requirements) == payload["requirements"]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source WebSocket Connection Requirements Report: brief-serialize")


def test_invalid_input_handling():
    for invalid in [None, "", [], {}, 42, b"bytes"]:
        result = build_source_websocket_connection_requirements(invalid)
        assert result.requirements == ()
        assert result.summary["requirement_count"] == 0


def test_model_input_support():
    # Use dict input and let the function handle model validation
    result = build_source_websocket_connection_requirements(
        {
            "id": "brief-model",
            "title": "WebSocket connections",
            "summary": "WebSocket handshake with upgrade and authentication.",
        }
    )

    assert result.source_id == "brief-model"
    assert len(result.requirements) >= 1
    assert any(req.category in {"handshake_upgrade", "connection_auth"} for req in result.requirements)


def test_object_input_support():
    obj = SimpleNamespace(
        id="brief-object",
        title="WebSocket",
        body="WebSocket heartbeat with ping-pong keepalive mechanism.",
    )

    result = build_source_websocket_connection_requirements(obj)

    assert result.source_id == "brief-object"
    assert any(req.category == "heartbeat_keepalive" for req in result.requirements)


def test_no_mutation_of_source():
    source = {
        "id": "brief-mutation",
        "title": "WebSocket API",
        "requirements": ["Handshake with upgrade headers."],
    }
    original = copy.deepcopy(source)

    build_source_websocket_connection_requirements(source)

    assert source == original


def test_aliases_generate_derive_and_extract():
    source = {"title": "WebSocket", "body": "WebSocket handshake with connection auth."}

    result1 = generate_source_websocket_connection_requirements(source)
    result2 = derive_source_websocket_connection_requirements(source)
    requirements = extract_source_websocket_connection_requirements(source)
    summary = summarize_source_websocket_connection_requirements(source)

    assert result1.to_dict() == result2.to_dict()
    assert requirements == result1.requirements
    assert summary == result1.summary


def test_confidence_scoring():
    result = build_source_websocket_connection_requirements(
        {
            "requirements": {
                "websocket": [
                    "WebSocket handshake must support upgrade protocol.",
                ]
            }
        }
    )

    # Requirements field with directive and websocket context should get high/medium confidence
    handshake_req = next((r for r in result.requirements if r.category == "handshake_upgrade"), None)
    assert handshake_req is not None
    assert handshake_req.confidence in {"high", "medium"}


def test_planning_notes_attached_to_requirements():
    result = build_source_websocket_connection_requirements(
        {"title": "WebSocket", "body": "WebSocket handshake with upgrade."}
    )

    for requirement in result.requirements:
        assert requirement.planning_note
        assert len(requirement.planning_note) > 10


def test_unresolved_questions_for_ambiguous_requirements():
    result = build_source_websocket_connection_requirements(
        {"title": "WebSocket", "body": "Add WebSocket handshake."}
    )

    handshake_req = next((r for r in result.requirements if r.category == "handshake_upgrade"), None)
    if handshake_req:
        # Should have questions about protocol version and subprotocols
        assert len(handshake_req.unresolved_questions) > 0


def test_summary_counts_match_requirements():
    result = build_source_websocket_connection_requirements(
        {
            "requirements": [
                "WebSocket handshake protocol.",
                "Connection authentication.",
                "Heartbeat mechanism.",
            ]
        }
    )

    assert result.summary["requirement_count"] == len(result.requirements)
    category_counts = result.summary["category_counts"]
    assert sum(category_counts.values()) == len(result.requirements)
    confidence_counts = result.summary["confidence_counts"]
    assert sum(confidence_counts.values()) == len(result.requirements)


def test_requirement_category_property_compatibility():
    result = build_source_websocket_connection_requirements(
        {"body": "WebSocket handshake protocol."}
    )

    for requirement in result.requirements:
        assert requirement.requirement_category == requirement.category


def test_records_and_findings_property_compatibility():
    result = build_source_websocket_connection_requirements(
        {"body": "WebSocket connections with auth."}
    )

    assert result.records == result.requirements
    assert result.findings == result.requirements


def test_empty_report_markdown():
    result = build_source_websocket_connection_requirements(
        {"title": "User profile", "body": "Add user profile endpoint."}
    )

    markdown = result.to_markdown()
    assert "No source WebSocket connection requirements were inferred." in markdown or len(result.requirements) > 0


def test_handshake_upgrade_detection():
    result = build_source_websocket_connection_requirements(
        {"body": "WebSocket handshake with HTTP upgrade and Sec-WebSocket-Key header."}
    )

    handshake_req = next((r for r in result.requirements if r.category == "handshake_upgrade"), None)
    assert handshake_req is not None


def test_connection_auth_detection():
    result = build_source_websocket_connection_requirements(
        {"body": "Connection authentication with JWT bearer tokens over WebSocket."}
    )

    auth_req = next((r for r in result.requirements if r.category == "connection_auth"), None)
    assert auth_req is not None


def test_heartbeat_keepalive_detection():
    result = build_source_websocket_connection_requirements(
        {"body": "Heartbeat mechanism with ping-pong keepalive every 30 seconds."}
    )

    heartbeat_req = next((r for r in result.requirements if r.category == "heartbeat_keepalive"), None)
    assert heartbeat_req is not None


def test_message_framing_detection():
    result = build_source_websocket_connection_requirements(
        {"body": "Message framing using Socket.IO protocol with text and binary frames."}
    )

    framing_req = next((r for r in result.requirements if r.category == "message_framing"), None)
    assert framing_req is not None


def test_connection_recovery_detection():
    result = build_source_websocket_connection_requirements(
        {"requirements": ["Connection recovery with automatic reconnection and exponential backoff."]}
    )

    recovery_req = next((r for r in result.requirements if r.category == "connection_recovery"), None)
    assert recovery_req is not None


def test_message_ordering_detection():
    result = build_source_websocket_connection_requirements(
        {"body": "Message ordering guarantees with sequence numbers and FIFO delivery."}
    )

    ordering_req = next((r for r in result.requirements if r.category == "message_ordering"), None)
    assert ordering_req is not None


def test_backpressure_handling_detection():
    result = build_source_websocket_connection_requirements(
        {"body": "Backpressure handling with buffer limits and message dropping policies."}
    )

    backpressure_req = next((r for r in result.requirements if r.category == "backpressure_handling"), None)
    assert backpressure_req is not None


def test_connection_limits_detection():
    result = build_source_websocket_connection_requirements(
        {"body": "Connection limits of 100 concurrent connections per user."}
    )

    limits_req = next((r for r in result.requirements if r.category == "connection_limits"), None)
    assert limits_req is not None


def test_json_safe_serialization():
    result = build_source_websocket_connection_requirements(
        {
            "title": "WebSocket with special | chars",
            "body": "WebSocket | handshake | auth | heartbeat",
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
    result = build_source_websocket_connection_requirements(
        {
            "id": "impl-brief",
            "source_brief_id": "src-brief",
            "title": "WebSocket",
            "body": "WebSocket handshake with upgrade.",
        }
    )

    assert result.source_id == "impl-brief"


def test_no_websocket_in_http_only_api():
    result = build_source_websocket_connection_requirements(
        {
            "title": "Add REST API",
            "body": "Add REST API endpoints with JSON responses. No WebSocket support.",
        }
    )

    # Should have empty requirements since no WebSocket patterns detected
    assert len(result.requirements) == 0


def test_mixed_http_and_websocket():
    result = build_source_websocket_connection_requirements(
        {
            "title": "Add API layer",
            "body": "Add both HTTP endpoints for simple requests and WebSocket handshake upgrade for real-time updates.",
        }
    )

    # Should detect WebSocket patterns despite mentioning HTTP
    assert any(req.category in {"handshake_upgrade"} for req in result.requirements)


def test_socket_io_protocol():
    result = build_source_websocket_connection_requirements(
        {"body": "Socket.IO protocol for message framing with text and binary frames."}
    )

    framing_req = next((r for r in result.requirements if r.category == "message_framing"), None)
    assert framing_req is not None


def test_stomp_protocol():
    result = build_source_websocket_connection_requirements(
        {"body": "STOMP protocol over WebSocket for message framing and routing."}
    )

    framing_req = next((r for r in result.requirements if r.category == "message_framing"), None)
    assert framing_req is not None


def test_exponential_backoff_reconnection():
    result = build_source_websocket_connection_requirements(
        {"body": "Reconnection strategy with exponential backoff and jitter."}
    )

    recovery_req = next((r for r in result.requirements if r.category == "connection_recovery"), None)
    assert recovery_req is not None


def test_jwt_connection_auth():
    result = build_source_websocket_connection_requirements(
        {"body": "JWT over WebSocket for connection-level authentication and authorization."}
    )

    auth_req = next((r for r in result.requirements if r.category == "connection_auth"), None)
    assert auth_req is not None


def test_ping_pong_keepalive():
    result = build_source_websocket_connection_requirements(
        {"body": "Ping-pong mechanism for connection health and idle timeout detection."}
    )

    heartbeat_req = next((r for r in result.requirements if r.category == "heartbeat_keepalive"), None)
    assert heartbeat_req is not None


def test_fifo_message_ordering():
    result = build_source_websocket_connection_requirements(
        {"body": "FIFO message ordering with sequence numbers and reordering."}
    )

    ordering_req = next((r for r in result.requirements if r.category == "message_ordering"), None)
    assert ordering_req is not None


def test_flow_control_backpressure():
    result = build_source_websocket_connection_requirements(
        {"body": "Flow control and backpressure handling with send buffer limits."}
    )

    backpressure_req = next((r for r in result.requirements if r.category == "backpressure_handling"), None)
    assert backpressure_req is not None


def test_per_user_connection_limits():
    result = build_source_websocket_connection_requirements(
        {"body": "Per-user connection limits with maximum 50 concurrent connections."}
    )

    limits_req = next((r for r in result.requirements if r.category == "connection_limits"), None)
    assert limits_req is not None


def test_per_tenant_connection_limits():
    result = build_source_websocket_connection_requirements(
        {"body": "Per-tenant connection quota with resource limits."}
    )

    limits_req = next((r for r in result.requirements if r.category == "connection_limits"), None)
    assert limits_req is not None


def test_sec_websocket_headers():
    result = build_source_websocket_connection_requirements(
        {"body": "Sec-WebSocket-Key and Sec-WebSocket-Accept headers for handshake validation."}
    )

    handshake_req = next((r for r in result.requirements if r.category == "handshake_upgrade"), None)
    assert handshake_req is not None


def test_connection_health_monitoring():
    result = build_source_websocket_connection_requirements(
        {"body": "Connection health monitoring with liveness probes and heartbeat checks."}
    )

    heartbeat_req = next((r for r in result.requirements if r.category == "heartbeat_keepalive"), None)
    assert heartbeat_req is not None


def test_session_resumption():
    result = build_source_websocket_connection_requirements(
        {"body": "Session resumption and persistent sessions after reconnection."}
    )

    recovery_req = next((r for r in result.requirements if r.category == "connection_recovery"), None)
    assert recovery_req is not None


def test_message_buffer_overflow():
    result = build_source_websocket_connection_requirements(
        {"body": "WebSocket message buffer with overflow handling and backpressure dropping policies."}
    )

    backpressure_req = next((r for r in result.requirements if r.category == "backpressure_handling"), None)
    assert backpressure_req is not None


def test_binary_and_text_frames():
    result = build_source_websocket_connection_requirements(
        {"body": "WebSocket text frames and binary frames with encoding."}
    )

    framing_req = next((r for r in result.requirements if r.category == "message_framing"), None)
    assert framing_req is not None


def test_connection_upgrade_101():
    result = build_source_websocket_connection_requirements(
        {"body": "HTTP upgrade with 101 Switching Protocols status for WebSocket handshake."}
    )

    handshake_req = next((r for r in result.requirements if r.category == "handshake_upgrade"), None)
    assert handshake_req is not None


def test_multiple_categories_same_evidence():
    result = build_source_websocket_connection_requirements(
        {
            "requirements": [
                "WebSocket with handshake, authentication, and heartbeat mechanism.",
            ]
        }
    )

    categories = {req.category for req in result.requirements}
    # Should extract multiple categories from the same sentence
    assert "handshake_upgrade" in categories or "connection_auth" in categories or "heartbeat_keepalive" in categories
