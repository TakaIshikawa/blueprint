import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_model_context_protocol_requirements import (
    SourceModelContextProtocolRequirement,
    SourceModelContextProtocolRequirementsReport,
    build_source_model_context_protocol_requirements,
    derive_source_model_context_protocol_requirements,
    extract_source_model_context_protocol_requirements,
    generate_source_model_context_protocol_requirements,
    source_model_context_protocol_requirements_to_dict,
    source_model_context_protocol_requirements_to_dicts,
    source_model_context_protocol_requirements_to_markdown,
    summarize_source_model_context_protocol_requirements,
)


def test_extracts_mcp_requirements_and_security_coverage_in_order():
    result = build_source_model_context_protocol_requirements(
        _source(
            source_payload={
                "mcp": {
                    "server": "MCP server role exposes an endpoint for tools.",
                    "client": "MCP client role connects to partner MCP servers.",
                    "tools": "Tool exposure must include tool definitions and side effects.",
                    "resources": "Resource access includes workspace resources by URI.",
                    "auth": "OAuth scopes and API key authentication are required.",
                    "consent": "User consent is required before sensitive tool invocation.",
                    "sandbox": "Sandboxing restricts filesystem and network egress.",
                    "limits": "Rate limit tool calls and concurrency.",
                    "audit": "Audit logging records tool invocation and resource access.",
                    "prompt": "Prompt/context boundaries prevent prompt injection and data leakage.",
                    "fallback": "Fallback behavior handles server unavailable and timeout cases.",
                }
            }
        )
    )

    assert isinstance(result, SourceModelContextProtocolRequirementsReport)
    assert all(isinstance(record, SourceModelContextProtocolRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "server_role",
        "client_role",
        "tool_exposure",
        "resource_access",
        "auth",
        "consent",
        "sandboxing",
        "rate_limits",
        "audit_logging",
        "prompt_context_boundaries",
        "fallback_behavior",
    ]
    assert result.summary["security_coverage"] == 100
    assert result.summary["security_sensitive_types"] == [
        "auth",
        "consent",
        "sandboxing",
        "audit_logging",
        "prompt_context_boundaries",
    ]
    assert result.summary["fallback_coverage"] == 100


def test_sourcebrief_mapping_equivalence_and_stable_to_dict_order():
    source = _source(
        summary="MCP server must expose tools with OAuth auth.",
        source_payload={
            "security": [
                "Consent is required before tool invocation.",
                "Audit log records MCP resource access.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping = build_source_model_context_protocol_requirements(source)
    model_result = derive_source_model_context_protocol_requirements(model)
    payload = source_model_context_protocol_requirements_to_dict(mapping)

    assert source == original
    assert mapping.to_dict() == model_result.to_dict()
    assert extract_source_model_context_protocol_requirements(model) == model_result.requirements
    assert generate_source_model_context_protocol_requirements(model).to_dict() == model_result.to_dict()
    assert summarize_source_model_context_protocol_requirements(mapping) == mapping.summary
    assert list(payload) == ["source_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "requirement_type",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "planning_notes",
    ]


def test_serialization_empty_negated_and_markdown_are_stable():
    result = build_source_model_context_protocol_requirements(
        _source(source_payload={"a": "MCP client connects to MCP server.", "b": "MCP client connects to MCP server."})
    )
    empty = build_source_model_context_protocol_requirements(_source(summary="Update dashboard copy."))
    negated = build_source_model_context_protocol_requirements(_source(summary="No MCP tool exposure changes are in scope."))
    payload = source_model_context_protocol_requirements_to_dict(result)

    assert len(result.records[0].evidence) == 1
    assert source_model_context_protocol_requirements_to_dicts(result) == payload["requirements"]
    assert source_model_context_protocol_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "| Type | Confidence |" in source_model_context_protocol_requirements_to_markdown(result)
    assert empty.records == ()
    assert negated.records == ()
    assert empty.summary["status"] == "no_mcp_language"


def _source(**overrides):
    payload = {
        "id": "source-mcp",
        "source_project": "blueprint",
        "source_entity_type": "brief",
        "source_id": "source-mcp",
        "source_links": {},
        "title": "MCP source",
        "summary": "Model Context Protocol requirements.",
        "source_payload": {},
    }
    payload.update(overrides)
    return payload
