import json

from blueprint.domain.models import SourceBrief
from blueprint.source_api_websocket_requirements import (
    SourceApiWebSocketRequirement,
    SourceApiWebSocketRequirementsReport,
    build_source_api_websocket_requirements,
    extract_source_api_websocket_requirements,
    source_api_websocket_requirements_to_dict,
    source_api_websocket_requirements_to_dicts,
    source_api_websocket_requirements_to_markdown,
    summarize_source_api_websocket_requirements,
)


def test_extracts_multi_signal_websocket_requirements_with_evidence():
    result = build_source_api_websocket_requirements(
        _source_brief(
            summary=(
                "Implement WebSocket connection for real-time notifications. "
                "Support text and binary message frames with JWT authentication."
            ),
            source_payload={
                "requirements": [
                    "Handle WebSocket handshake and connection lifecycle with proper close frames.",
                    "Implement ping-pong heartbeat mechanism with 30-second intervals.",
                    "Support automatic reconnection with exponential backoff strategy.",
                    "Enforce rate limiting of 100 messages per minute per connection.",
                ],
                "acceptance_criteria": [
                    "Messages must be delivered in order with FIFO guarantees.",
                    "Support Socket.io subprotocol negotiation for backwards compatibility.",
                ],
            },
        )
    )

    assert isinstance(result, SourceApiWebSocketRequirementsReport)
    assert all(isinstance(record, SourceApiWebSocketRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "connection_lifecycle",
        "message_framing",
        "authentication",
        "rate_limiting",
        "reconnection_strategy",
        "message_ordering",
        "ping_pong_heartbeat",
        "subprotocol_negotiation",
    ]
    by_type = {record.requirement_type: record for record in result.records}
    assert any("handshake" in item.lower() for item in by_type["connection_lifecycle"].evidence)
    assert any("text" in item.lower() or "binary" in item.lower() for item in by_type["message_framing"].evidence)
    assert any("jwt" in item.lower() for item in by_type["authentication"].evidence)
    assert any("100 messages" in item or "rate limit" in item.lower() for item in by_type["rate_limiting"].evidence)
    assert any("reconnect" in term.lower() or "backoff" in term.lower() for term in by_type["reconnection_strategy"].matched_terms)
    assert result.summary["requirement_count"] == 8
    assert result.summary["type_counts"]["connection_lifecycle"] == 1
    assert result.summary["connection_coverage"] > 0
    assert result.summary["security_coverage"] > 0
    assert result.summary["reliability_coverage"] > 0


def test_brief_without_websocket_language_returns_stable_empty_report():
    result = build_source_api_websocket_requirements(
        _source_brief(
            title="REST API pagination",
            summary="Add cursor-based pagination to the users endpoint.",
            source_payload={
                "requirements": [
                    "Support offset and limit parameters.",
                    "Return next and previous page tokens.",
                ],
            },
        )
    )
    repeat = build_source_api_websocket_requirements(
        _source_brief(
            title="REST API pagination",
            summary="Add cursor-based pagination to the users endpoint.",
            source_payload={
                "requirements": [
                    "Support offset and limit parameters.",
                    "Return next and previous page tokens.",
                ],
            },
        )
    )

    expected_summary = {
        "requirement_count": 0,
        "source_count": 1,
        "type_counts": {
            "connection_lifecycle": 0,
            "message_framing": 0,
            "authentication": 0,
            "rate_limiting": 0,
            "reconnection_strategy": 0,
            "message_ordering": 0,
            "ping_pong_heartbeat": 0,
            "subprotocol_negotiation": 0,
        },
        "requirement_types": [],
        "follow_up_question_count": 0,
        "connection_coverage": 0,
        "security_coverage": 0,
        "reliability_coverage": 0,
    }
    assert result.summary == expected_summary
    assert result.requirements == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.to_dict() == repeat.to_dict()


def test_native_websocket_requirements_detected():
    result = build_source_api_websocket_requirements(
        _source_brief(
            summary="Implement native WebSocket server for real-time data streaming.",
            source_payload={
                "requirements": [
                    "Support WSS (secure WebSocket) connections with TLS.",
                    "Handle WebSocket upgrade handshake from HTTP.",
                    "Send ping frames every 30 seconds to keep connections alive.",
                    "Support graceful connection closure with close frames.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "connection_lifecycle" in types
    assert "ping_pong_heartbeat" in types
    assert result.summary["requirement_count"] >= 2


def test_socketio_requirements_detected():
    result = build_source_api_websocket_requirements(
        _source_brief(
            summary="Integrate Socket.io for bidirectional real-time communication.",
            source_payload={
                "requirements": [
                    "Use Socket.io protocol with automatic reconnection.",
                    "Support both text and binary message events.",
                    "Implement room-based message broadcasting.",
                    "Handle connection authentication via JWT tokens.",
                ],
                "acceptance_criteria": [
                    "Messages must maintain delivery order.",
                    "Support Socket.io subprotocol negotiation.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "subprotocol_negotiation" in types
    assert "message_framing" in types
    assert "authentication" in types
    assert "reconnection_strategy" in types
    assert "message_ordering" in types


def test_websocket_rate_limiting_and_backpressure_detected():
    result = build_source_api_websocket_requirements(
        _source_brief(
            summary="Add WebSocket rate limiting and flow control.",
            source_payload={
                "requirements": [
                    "Limit WebSocket connections to 1000 concurrent connections per server.",
                    "Throttle message rate to 50 messages per second per connection.",
                    "Implement backpressure handling when client cannot keep up.",
                    "Reject new connections when at maximum capacity.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "rate_limiting" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    assert any("concurrent" in item.lower() or "50 messages" in item for item in by_type["rate_limiting"].evidence)


def test_websocket_authentication_patterns_detected():
    result = build_source_api_websocket_requirements(
        _source_brief(
            summary="Secure WebSocket connections with authentication.",
            source_payload={
                "requirements": [
                    "Authenticate WebSocket connections using bearer tokens in the upgrade request.",
                    "Support API key authentication for service-to-service WebSocket connections.",
                    "Validate JWT tokens during WebSocket handshake.",
                    "Automatically close connections with invalid or expired credentials.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "authentication" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    auth_req = by_type["authentication"]
    assert any("bearer" in term.lower() or "jwt" in term.lower() or "token" in term.lower() for term in auth_req.matched_terms)


def test_reconnection_strategy_requirements_detected():
    result = build_source_api_websocket_requirements(
        _source_brief(
            summary="Implement robust WebSocket reconnection logic.",
            source_payload={
                "requirements": [
                    "Automatically reconnect WebSocket when connection is lost.",
                    "Use exponential backoff with jitter for reconnection attempts.",
                    "Maximum reconnection delay of 30 seconds.",
                    "Restore connection state after successful reconnection.",
                    "Give up after 5 failed reconnection attempts.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "reconnection_strategy" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    reconnect_req = by_type["reconnection_strategy"]
    assert any("reconnect" in item.lower() or "backoff" in item.lower() for item in reconnect_req.evidence)


def test_message_ordering_guarantees_detected():
    result = build_source_api_websocket_requirements(
        _source_brief(
            summary="Ensure message ordering in WebSocket communication.",
            source_payload={
                "requirements": [
                    "Maintain FIFO message ordering for all WebSocket messages.",
                    "Guarantee sequential delivery of messages to clients.",
                    "Implement message queue for ordered processing.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "message_ordering" in types


def test_ping_pong_heartbeat_configuration_detected():
    result = build_source_api_websocket_requirements(
        _source_brief(
            summary="Configure WebSocket heartbeat mechanism.",
            source_payload={
                "requirements": [
                    "Send ping frames every 30 seconds.",
                    "Expect pong response within 5 seconds.",
                    "Close connection if no pong received after 3 ping attempts.",
                    "Implement automatic keep-alive using ping-pong frames.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "ping_pong_heartbeat" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    heartbeat_req = by_type["ping_pong_heartbeat"]
    # Should have reduced questions since interval is specified
    assert len(heartbeat_req.follow_up_questions) < 2


def test_subprotocol_negotiation_detected():
    result = build_source_api_websocket_requirements(
        _source_brief(
            summary="Support multiple WebSocket subprotocols.",
            source_payload={
                "requirements": [
                    "Support Socket.io and STOMP subprotocols.",
                    "Negotiate subprotocol during WebSocket handshake using Sec-WebSocket-Protocol header.",
                    "Fall back to default protocol if client doesn't specify supported subprotocol.",
                ],
            },
        )
    )

    types = {req.requirement_type for req in result.requirements}
    assert "subprotocol_negotiation" in types
    by_type = {req.requirement_type: req for req in result.requirements}
    subprotocol_req = by_type["subprotocol_negotiation"]
    assert any("socket.io" in term.lower() or "stomp" in term.lower() for term in subprotocol_req.matched_terms)


def test_requirement_deduplication_merges_evidence_without_losing_source_fields():
    result = build_source_api_websocket_requirements(
        _source_brief(
            summary="Use WebSocket connections with JWT authentication.",
            source_payload={
                "requirements": [
                    "Authenticate WebSocket during handshake using JWT tokens.",
                    "Validate bearer tokens for WebSocket authorization.",
                ],
                "acceptance": "All WebSocket connections must be authenticated with valid JWT.",
            },
        )
    )

    by_type = {record.requirement_type: record for record in result.records}
    auth_req = by_type["authentication"]
    # Multiple source fields should be captured
    assert len(auth_req.source_field_paths) >= 2
    assert "summary" in auth_req.source_field_paths
    assert any("requirements" in field for field in auth_req.source_field_paths)
    assert len(auth_req.evidence) >= 2
    assert any("jwt" in evidence.lower() or "token" in evidence.lower() for evidence in auth_req.evidence)


def test_dict_serialization_round_trips_without_mutation():
    original = build_source_api_websocket_requirements(
        _source_brief(
            summary="WebSocket with authentication and heartbeat.",
            source_payload={
                "requirements": [
                    "JWT authentication for WebSocket connections.",
                    "Ping-pong heartbeat every 30 seconds.",
                ],
            },
        )
    )

    serialized = original.to_dict()
    assert isinstance(serialized, dict)
    assert serialized["source_brief_id"] == "websocket-source"
    assert len(serialized["requirements"]) == len(original.requirements)
    assert serialized["summary"]["requirement_count"] == len(original.requirements)

    # Repeat to verify no mutation
    repeat = original.to_dict()
    assert repeat == serialized


def test_to_dicts_helper_serializes_requirements_list():
    report = build_source_api_websocket_requirements(
        _source_brief(
            summary="WebSocket with reconnection and message ordering.",
            source_payload={
                "requirements": [
                    "Automatic reconnection with exponential backoff.",
                    "FIFO message ordering guarantees.",
                ],
            },
        )
    )

    dicts = source_api_websocket_requirements_to_dicts(report)
    assert isinstance(dicts, list)
    assert all(isinstance(item, dict) for item in dicts)
    assert len(dicts) == report.summary["requirement_count"]

    # Also test tuple input
    tuple_dicts = source_api_websocket_requirements_to_dicts(report.requirements)
    assert tuple_dicts == dicts


def test_markdown_output_renders_deterministic_table():
    report = build_source_api_websocket_requirements(
        _source_brief(
            source_id="websocket-markdown-test",
            summary="WebSocket with connection lifecycle and heartbeat.",
            source_payload={
                "requirements": [
                    "Handle WebSocket handshake and close frames.",
                    "Send ping every 30 seconds.",
                ],
            },
        )
    )

    markdown = source_api_websocket_requirements_to_markdown(report)
    assert isinstance(markdown, str)
    assert "# Source API WebSocket Requirements Report: websocket-markdown-test" in markdown
    assert "## Summary" in markdown
    assert "## Requirements" in markdown
    assert "| Type | Source Field Paths | Evidence | Follow-up Questions |" in markdown
    assert "connection_lifecycle" in markdown

    # Repeat to verify deterministic output
    repeat_markdown = report.to_markdown()
    assert repeat_markdown == markdown


def test_empty_report_markdown_includes_no_requirements_message():
    report = build_source_api_websocket_requirements(
        _source_brief(
            summary="REST API refactoring with no WebSocket changes.",
        )
    )

    markdown = report.to_markdown()
    assert "No source API WebSocket requirements were inferred." in markdown
    assert "## Requirements" not in markdown


def test_extracts_from_raw_text_input():
    result = build_source_api_websocket_requirements(
        "Implement WebSocket connections with JWT authentication, automatic reconnection, "
        "and ping-pong heartbeat every 30 seconds."
    )

    assert len(result.requirements) >= 3
    types = {req.requirement_type for req in result.requirements}
    assert "authentication" in types
    assert "reconnection_strategy" in types
    assert "ping_pong_heartbeat" in types


def test_extracts_from_mapping_input():
    result = build_source_api_websocket_requirements(
        {
            "id": "mapping-source",
            "title": "WebSocket requirements",
            "summary": "WebSocket connection with message framing and rate limiting.",
            "source_payload": {
                "requirements": "Support text and binary frames with backpressure handling.",
            },
        }
    )

    assert result.source_brief_id == "mapping-source"
    types = {req.requirement_type for req in result.requirements}
    assert "message_framing" in types
    assert "rate_limiting" in types


def test_extracts_from_pydantic_model():
    model = SourceBrief(
        id="pydantic-source",
        title="WebSocket requirements",
        domain="api",
        summary="WebSocket with Socket.io subprotocol and connection lifecycle management.",
        source_project="test",
        source_entity_type="issue",
        source_id="pydantic-source",
        source_payload={
            "requirements": "Handle WebSocket handshake, upgrade, and graceful closure.",
        },
        source_links={},
    )

    result = build_source_api_websocket_requirements(model)
    assert result.source_brief_id == "pydantic-source"
    types = {req.requirement_type for req in result.requirements}
    assert "connection_lifecycle" in types
    assert "subprotocol_negotiation" in types


def test_extract_helper_returns_tuple_of_requirements():
    requirements = extract_source_api_websocket_requirements(
        _source_brief(
            summary="WebSocket with authentication and heartbeat.",
        )
    )

    assert isinstance(requirements, tuple)
    assert all(isinstance(req, SourceApiWebSocketRequirement) for req in requirements)
    assert len(requirements) >= 1


def test_summarize_helper_returns_summary_dict():
    summary = summarize_source_api_websocket_requirements(
        _source_brief(
            summary="WebSocket with reconnection and message ordering.",
        )
    )

    assert isinstance(summary, dict)
    assert "requirement_count" in summary
    assert "type_counts" in summary
    assert summary["requirement_count"] >= 1


def test_summarize_accepts_report_object():
    report = build_source_api_websocket_requirements(
        _source_brief(summary="WebSocket connections.")
    )
    summary = summarize_source_api_websocket_requirements(report)

    assert summary == report.summary


def test_coverage_metrics_calculated_correctly():
    result = build_source_api_websocket_requirements(
        _source_brief(
            summary="WebSocket with handshake, reconnection, authentication, rate limiting, and heartbeat.",
            source_payload={
                "requirements": [
                    "Handle connection lifecycle with close frames.",
                    "Automatic reconnection with backoff.",
                    "JWT authentication during handshake.",
                    "Rate limit to 100 messages per minute.",
                    "Ping-pong heartbeat every 30 seconds.",
                    "FIFO message ordering.",
                ],
            },
        )
    )

    summary = result.summary
    # Connection requirements present (lifecycle, reconnection)
    assert summary["connection_coverage"] == 100
    # Security requirements present (authentication, rate limiting)
    assert summary["security_coverage"] == 100
    # Reliability requirements present (heartbeat, ordering)
    assert summary["reliability_coverage"] == 100


def test_follow_up_questions_reduced_when_evidence_is_specific():
    result = build_source_api_websocket_requirements(
        _source_brief(
            summary="Use JWT bearer tokens for WebSocket authentication.",
            source_payload={
                "requirements": [
                    "Support text and binary message frames.",
                    "Ping interval of 30 seconds with 5 second timeout.",
                ],
            },
        )
    )

    by_type = {req.requirement_type: req for req in result.requirements}
    # Authentication mentions specific method (JWT), so should have fewer questions
    auth = by_type.get("authentication")
    if auth:
        assert len(auth.follow_up_questions) < 2
    # Message framing mentions specific types, so should have fewer questions
    framing = by_type.get("message_framing")
    if framing:
        assert len(framing.follow_up_questions) < 2
    # Heartbeat mentions interval, so should have fewer questions
    heartbeat = by_type.get("ping_pong_heartbeat")
    if heartbeat:
        assert len(heartbeat.follow_up_questions) < 2


def test_matched_terms_captured_for_each_requirement():
    result = build_source_api_websocket_requirements(
        _source_brief(
            summary="WebSocket connection with handshake, ping-pong, and JWT authentication.",
        )
    )

    for req in result.requirements:
        assert len(req.matched_terms) > 0
        # Matched terms should be deduplicated
        assert len(req.matched_terms) == len(set(term.casefold() for term in req.matched_terms))


def _source_brief(
    *,
    source_id="websocket-source",
    title="WebSocket requirements",
    domain="platform",
    summary="General WebSocket requirements.",
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
