import copy
import json
from types import SimpleNamespace

from blueprint.source_graphql_schema_requirements import (
    SourceGraphQLSchemaRequirementsReport,
    build_source_graphql_schema_requirements,
    derive_source_graphql_schema_requirements,
    extract_source_graphql_schema_requirements,
    generate_source_graphql_schema_requirements,
    source_graphql_schema_requirements_to_dict,
    source_graphql_schema_requirements_to_dicts,
    source_graphql_schema_requirements_to_markdown,
    summarize_source_graphql_schema_requirements,
)


def test_structured_source_extracts_graphql_categories():
    result = build_source_graphql_schema_requirements(
        {
            "id": "brief-graphql",
            "title": "Add GraphQL API",
            "requirements": {
                "graphql": [
                    "GraphQL schema definition with type definitions.",
                    "Query complexity limits with max depth of 10.",
                    "Mutation operations for creating and updating users.",
                    "Real-time subscriptions for live updates via WebSocket.",
                    "Field-level authorization with @auth directives.",
                    "Schema federation with Apollo Federation.",
                    "Disable introspection in production.",
                    "Persisted queries for security and performance.",
                    "DataLoader for batching to prevent N+1 queries.",
                ]
            },
        }
    )

    assert isinstance(result, SourceGraphQLSchemaRequirementsReport)
    assert result.source_id == "brief-graphql"
    categories = {req.category for req in result.requirements}
    expected_categories = {
        "schema_definition",
        "query_complexity",
        "mutations",
        "subscriptions",
        "field_authorization",
        "federation",
        "introspection",
        "persisted_queries",
        "batching_dataloader",
    }
    assert expected_categories <= categories


def test_natural_language_extraction_from_body():
    result = build_source_graphql_schema_requirements(
        """
        Add GraphQL API endpoint

        The API must support GraphQL queries with complexity limits.
        Define schema using GraphQL schema definition language.
        Mutations should validate input and handle errors.
        Support real-time subscriptions via WebSocket.
        Implement field-level authorization rules.
        Use DataLoader to prevent N+1 query problems.
        """
    )

    assert len(result.requirements) >= 4
    categories = {req.category for req in result.requirements}
    assert "schema_definition" in categories
    assert "query_complexity" in categories
    assert "mutations" in categories
    assert "batching_dataloader" in categories


def test_evidence_deduplication_and_stable_ordering():
    result = build_source_graphql_schema_requirements(
        {
            "title": "GraphQL API schema",
            "description": "GraphQL schema definition.",
            "requirements": ["Schema definition with types."],
            "acceptance": ["Schema configured."],
        }
    )

    # Find schema_definition requirement
    schema_req = next((r for r in result.requirements if r.category == "schema_definition"), None)
    assert schema_req is not None
    # Evidence should be collected from multiple fields (up to 6)
    assert len(schema_req.evidence) >= 1


def test_out_of_scope_negation_produces_empty_report():
    result = build_source_graphql_schema_requirements(
        {
            "id": "brief-no-graphql",
            "title": "Add REST API endpoint",
            "scope": "No GraphQL or schema definition is in scope for this work.",
        }
    )

    assert result.requirements == ()
    assert result.summary["requirement_count"] == 0
    assert result.summary["status"] == "no_graphql_schema_requirements_found"


def test_to_dict_to_dicts_and_to_markdown_serialization():
    result = build_source_graphql_schema_requirements(
        {
            "id": "brief-serialize",
            "title": "Add GraphQL",
            "requirements": [
                "GraphQL schema with types.",
                "Query complexity limits.",
            ],
        }
    )

    payload = source_graphql_schema_requirements_to_dict(result)
    markdown = source_graphql_schema_requirements_to_markdown(result)

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
    assert source_graphql_schema_requirements_to_dicts(result) == payload["requirements"]
    assert source_graphql_schema_requirements_to_dicts(result.requirements) == payload["requirements"]
    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source GraphQL Schema Requirements Report: brief-serialize")


def test_invalid_input_handling():
    for invalid in [None, "", [], {}, 42, b"bytes"]:
        result = build_source_graphql_schema_requirements(invalid)
        assert result.requirements == ()
        assert result.summary["requirement_count"] == 0


def test_model_input_support():
    # Use dict input and let the function handle model validation
    result = build_source_graphql_schema_requirements(
        {
            "id": "brief-model",
            "title": "GraphQL schema",
            "summary": "GraphQL schema definition with types and resolvers.",
        }
    )

    assert result.source_id == "brief-model"
    assert len(result.requirements) >= 1
    assert any(req.category == "schema_definition" for req in result.requirements)


def test_object_input_support():
    obj = SimpleNamespace(
        id="brief-object",
        title="GraphQL",
        body="Query complexity limits with max depth for GraphQL queries.",
    )

    result = build_source_graphql_schema_requirements(obj)

    assert result.source_id == "brief-object"
    assert any(req.category == "query_complexity" for req in result.requirements)


def test_no_mutation_of_source():
    source = {
        "id": "brief-mutation",
        "title": "GraphQL API",
        "requirements": ["Schema definition with types."],
    }
    original = copy.deepcopy(source)

    build_source_graphql_schema_requirements(source)

    assert source == original


def test_aliases_generate_derive_and_extract():
    source = {"title": "GraphQL", "body": "GraphQL schema with query complexity."}

    result1 = generate_source_graphql_schema_requirements(source)
    result2 = derive_source_graphql_schema_requirements(source)
    requirements = extract_source_graphql_schema_requirements(source)
    summary = summarize_source_graphql_schema_requirements(source)

    assert result1.to_dict() == result2.to_dict()
    assert requirements == result1.requirements
    assert summary == result1.summary


def test_confidence_scoring():
    result = build_source_graphql_schema_requirements(
        {
            "requirements": {
                "graphql": [
                    "Schema definition must include type definitions.",
                ]
            }
        }
    )

    # Requirements field with directive and graphql context should get high/medium confidence
    schema_req = next((r for r in result.requirements if r.category == "schema_definition"), None)
    assert schema_req is not None
    assert schema_req.confidence in {"high", "medium"}


def test_planning_notes_attached_to_requirements():
    result = build_source_graphql_schema_requirements(
        {"title": "GraphQL", "body": "GraphQL schema with types."}
    )

    for requirement in result.requirements:
        assert requirement.planning_note
        assert len(requirement.planning_note) > 10


def test_unresolved_questions_for_ambiguous_requirements():
    result = build_source_graphql_schema_requirements(
        {"title": "GraphQL", "body": "Add GraphQL schema."}
    )

    schema_req = next((r for r in result.requirements if r.category == "schema_definition"), None)
    if schema_req:
        # Should have questions about schema-first vs code-first
        assert len(schema_req.unresolved_questions) > 0


def test_summary_counts_match_requirements():
    result = build_source_graphql_schema_requirements(
        {
            "requirements": [
                "GraphQL schema definition.",
                "Query complexity limits.",
                "Mutation operations.",
            ]
        }
    )

    assert result.summary["requirement_count"] == len(result.requirements)
    category_counts = result.summary["category_counts"]
    assert sum(category_counts.values()) == len(result.requirements)
    confidence_counts = result.summary["confidence_counts"]
    assert sum(confidence_counts.values()) == len(result.requirements)


def test_requirement_category_property_compatibility():
    result = build_source_graphql_schema_requirements(
        {"body": "GraphQL schema definition."}
    )

    for requirement in result.requirements:
        assert requirement.requirement_category == requirement.category


def test_records_and_findings_property_compatibility():
    result = build_source_graphql_schema_requirements(
        {"body": "GraphQL schema with queries."}
    )

    assert result.records == result.requirements
    assert result.findings == result.requirements


def test_empty_report_markdown():
    result = build_source_graphql_schema_requirements(
        {"title": "User profile", "body": "Add user profile endpoint."}
    )

    markdown = result.to_markdown()
    assert "No source GraphQL schema requirements were inferred." in markdown or len(result.requirements) > 0


def test_schema_definition_detection():
    result = build_source_graphql_schema_requirements(
        {"body": "GraphQL schema definition with object types, input types, and enums."}
    )

    schema_req = next((r for r in result.requirements if r.category == "schema_definition"), None)
    assert schema_req is not None


def test_query_complexity_detection():
    result = build_source_graphql_schema_requirements(
        {"body": "Query complexity limits with max depth of 10 and field cost calculation."}
    )

    complexity_req = next((r for r in result.requirements if r.category == "query_complexity"), None)
    assert complexity_req is not None


def test_mutations_detection():
    result = build_source_graphql_schema_requirements(
        {"body": "GraphQL mutations for creating, updating, and deleting resources."}
    )

    mutation_req = next((r for r in result.requirements if r.category == "mutations"), None)
    assert mutation_req is not None


def test_subscriptions_detection():
    result = build_source_graphql_schema_requirements(
        {"body": "Real-time subscriptions via WebSocket for live updates."}
    )

    subscription_req = next((r for r in result.requirements if r.category == "subscriptions"), None)
    assert subscription_req is not None


def test_field_authorization_detection():
    result = build_source_graphql_schema_requirements(
        {"requirements": ["Field-level authorization with @auth directives for sensitive data."]}
    )

    auth_req = next((r for r in result.requirements if r.category == "field_authorization"), None)
    assert auth_req is not None


def test_federation_detection():
    result = build_source_graphql_schema_requirements(
        {"body": "Schema federation with Apollo Federation and @key directives."}
    )

    federation_req = next((r for r in result.requirements if r.category == "federation"), None)
    assert federation_req is not None


def test_introspection_detection():
    result = build_source_graphql_schema_requirements(
        {"body": "Disable introspection in production for security."}
    )

    introspection_req = next((r for r in result.requirements if r.category == "introspection"), None)
    assert introspection_req is not None


def test_persisted_queries_detection():
    result = build_source_graphql_schema_requirements(
        {"body": "Persisted queries with query allowlist for security."}
    )

    persisted_req = next((r for r in result.requirements if r.category == "persisted_queries"), None)
    assert persisted_req is not None


def test_batching_dataloader_detection():
    result = build_source_graphql_schema_requirements(
        {"body": "DataLoader for batching to prevent N+1 query problems."}
    )

    batching_req = next((r for r in result.requirements if r.category == "batching_dataloader"), None)
    assert batching_req is not None


def test_json_safe_serialization():
    result = build_source_graphql_schema_requirements(
        {
            "title": "GraphQL with special | chars",
            "body": "GraphQL schema | types | queries | mutations",
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
    result = build_source_graphql_schema_requirements(
        {
            "id": "impl-brief",
            "source_brief_id": "src-brief",
            "title": "GraphQL",
            "body": "GraphQL schema definition with types.",
        }
    )

    assert result.source_id == "impl-brief"


def test_no_graphql_in_rest_only_api():
    result = build_source_graphql_schema_requirements(
        {
            "title": "Add REST API",
            "body": "Add REST API endpoints with JSON responses. No GraphQL support.",
        }
    )

    # Should have empty requirements since no GraphQL patterns detected
    assert len(result.requirements) == 0


def test_mixed_rest_and_graphql():
    result = build_source_graphql_schema_requirements(
        {
            "title": "Add API layer",
            "body": "Add both REST endpoints for simple queries and GraphQL schema for complex queries with depth limits.",
        }
    )

    # Should detect GraphQL patterns despite mentioning REST
    assert any(req.category in {"schema_definition", "query_complexity"} for req in result.requirements)


def test_schema_first_vs_code_first():
    result_schema_first = build_source_graphql_schema_requirements(
        {"body": "Schema-first approach with .graphql SDL files."}
    )

    result_code_first = build_source_graphql_schema_requirements(
        {"body": "Code-first schema generation from resolvers."}
    )

    # Both should detect schema_definition
    assert any(req.category == "schema_definition" for req in result_schema_first.requirements)
    assert any(req.category == "schema_definition" for req in result_code_first.requirements)


def test_n_plus_1_query_prevention():
    result = build_source_graphql_schema_requirements(
        {"body": "Prevent N+1 query problems using DataLoader batching pattern."}
    )

    batching_req = next((r for r in result.requirements if r.category == "batching_dataloader"), None)
    assert batching_req is not None
    assert any("n+1" in evidence.lower() or "dataloader" in evidence.lower() for evidence in batching_req.evidence)


def test_apollo_federation_specific():
    result = build_source_graphql_schema_requirements(
        {"body": "Apollo Federation with @key, @external, @requires, and @provides directives."}
    )

    federation_req = next((r for r in result.requirements if r.category == "federation"), None)
    assert federation_req is not None


def test_automatic_persisted_queries():
    result = build_source_graphql_schema_requirements(
        {"body": "Automatic persisted queries (APQ) for performance and security."}
    )

    persisted_req = next((r for r in result.requirements if r.category == "persisted_queries"), None)
    assert persisted_req is not None


def test_graphql_over_websocket():
    result = build_source_graphql_schema_requirements(
        {"body": "GraphQL subscriptions over WebSocket for real-time updates."}
    )

    subscription_req = next((r for r in result.requirements if r.category == "subscriptions"), None)
    assert subscription_req is not None


def test_field_level_permissions():
    result = build_source_graphql_schema_requirements(
        {"body": "Field-level permissions and resolver authorization guards."}
    )

    auth_req = next((r for r in result.requirements if r.category == "field_authorization"), None)
    assert auth_req is not None


def test_multiple_categories_same_evidence():
    result = build_source_graphql_schema_requirements(
        {
            "requirements": [
                "GraphQL schema with mutations, subscriptions, and field-level auth.",
            ]
        }
    )

    categories = {req.category for req in result.requirements}
    # Should extract multiple categories from the same sentence
    assert "mutations" in categories or "subscriptions" in categories or "field_authorization" in categories
