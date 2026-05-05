"""Tests for GraphQL migration readiness analyzer."""

import pytest

from blueprint.task_graphql_migration_readiness import (
    GraphQLMigrationReadiness,
    analyze_graphql_migration_readiness,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_graphql_migration_readiness({})

    assert isinstance(result, GraphQLMigrationReadiness)
    assert result.schema_defined is False
    assert result.resolvers_planned is False
    assert result.n_plus_one_queries_addressed is False
    assert result.caching_strategy_specified is False
    assert result.backwards_compatibility_maintained is False
    assert result.readiness_score == 0.0


def test_schema_defined_detected():
    """Detect GraphQL schema definition in task data."""
    task = {
        "title": "Define GraphQL schema",
        "description": "Create GraphQL schema definition for user queries",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.schema_defined is True
    assert result.resolvers_planned is False
    assert result.readiness_score == 0.2


def test_resolvers_planned_detected():
    """Detect resolver planning in task data."""
    task = {
        "description": "Plan resolver implementation for user and post types",
        "acceptance_criteria": ["Resolver strategy documented", "Query resolvers defined"],
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.resolvers_planned is True
    assert result.schema_defined is False


def test_n_plus_one_queries_addressed():
    """Detect N+1 query prevention in task data."""
    task = {
        "description": "Implement DataLoader to prevent N+1 query problems",
        "acceptance_criteria": ["Batch loading configured", "Query optimization verified"],
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.n_plus_one_queries_addressed is True
    assert result.schema_defined is False


def test_caching_strategy_specified():
    """Detect caching strategy in task data."""
    task = {
        "title": "Configure Apollo cache",
        "description": "Set up GraphQL cache layer with cache invalidation strategy",
        "acceptance_criteria": ["Caching strategy documented", "Response caching enabled"],
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.caching_strategy_specified is True
    assert result.schema_defined is False


def test_backwards_compatibility_maintained():
    """Detect backwards compatibility considerations in task data."""
    task = {
        "description": "Ensure backward compatibility during GraphQL migration with API versioning",
        "acceptance_criteria": [
            "Non-breaking changes only",
            "Schema evolution strategy defined",
        ],
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.backwards_compatibility_maintained is True
    assert result.schema_defined is False


def test_comprehensive_graphql_migration_all_detected():
    """Test comprehensive GraphQL migration with all aspects present."""
    task = {
        "title": "Complete GraphQL migration implementation",
        "description": (
            "Implement GraphQL schema definition with resolver implementation plan. "
            "Use DataLoader to prevent N+1 queries. Configure Apollo cache strategy "
            "with query caching. Maintain backward compatibility with gradual migration."
        ),
        "acceptance_criteria": [
            "GraphQL schema defined and validated",
            "Resolver architecture planned and documented",
            "DataLoader configured for batch loading",
            "Caching strategy implemented with invalidation",
            "Backwards compatibility maintained with versioning",
        ],
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.schema_defined is True
    assert result.resolvers_planned is True
    assert result.n_plus_one_queries_addressed is True
    assert result.caching_strategy_specified is True
    assert result.backwards_compatibility_maintained is True
    assert result.readiness_score == 1.0


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_graphql_migration_readiness(None)  # type: ignore

    assert isinstance(result, GraphQLMigrationReadiness)
    assert result.schema_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_graphql_migration_readiness([{"key": "value"}])  # type: ignore

    assert isinstance(result, GraphQLMigrationReadiness)
    assert result.schema_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_string():
    """Test with string input instead of mapping."""
    result = analyze_graphql_migration_readiness("not a mapping")  # type: ignore

    assert isinstance(result, GraphQLMigrationReadiness)
    assert result.schema_defined is False


def test_invalid_task_data_tuple():
    """Test with tuple input instead of mapping."""
    result = analyze_graphql_migration_readiness(("tuple", "data"))  # type: ignore

    assert isinstance(result, GraphQLMigrationReadiness)
    assert result.schema_defined is False


def test_partial_data_missing_fields():
    """Test with partial task data missing some fields."""
    task = {
        "title": "GraphQL setup",
        # Missing description, acceptance_criteria, etc.
    }

    result = analyze_graphql_migration_readiness(task)

    assert isinstance(result, GraphQLMigrationReadiness)
    assert result.readiness_score == 0.0


def test_partial_migration_readiness():
    """Test partial migration readiness with some aspects covered."""
    task = {
        "title": "Partial GraphQL migration",
        "description": "Define GraphQL schema and plan resolvers",
        "acceptance_criteria": [
            "Schema definition complete",
            "Resolver implementation planned",
        ],
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.schema_defined is True
    assert result.resolvers_planned is True
    assert result.n_plus_one_queries_addressed is False
    assert result.caching_strategy_specified is False
    assert result.backwards_compatibility_maintained is False
    assert result.readiness_score == 0.4


def test_task_data_with_nested_acceptance_criteria():
    """Test extraction from nested acceptance criteria structure."""
    task = {
        "title": "GraphQL improvements",
        "acceptance_criteria": [
            "Define type definitions in schema",
            "Implement query resolver and mutation resolver",
            "Configure DataLoader for batching",
        ],
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.schema_defined is True
    assert result.resolvers_planned is True
    assert result.n_plus_one_queries_addressed is True


def test_validation_commands_checked():
    """Test that validation commands are included in analysis."""
    task = {
        "title": "Setup GraphQL",
        "validation_command": "pytest tests/test_graphql_schema.py tests/test_dataloader.py",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.schema_defined is True
    assert result.n_plus_one_queries_addressed is True


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "GRAPHQL SCHEMA with RESOLVER PLAN and DATALOADER",
        "acceptance_criteria": ["CACHING STRATEGY documented", "BACKWARD COMPATIBILITY maintained"],
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.schema_defined is True
    assert result.resolvers_planned is True
    assert result.n_plus_one_queries_addressed is True
    assert result.caching_strategy_specified is True
    assert result.backwards_compatibility_maintained is True


def test_alternative_terminology_schema():
    """Test alternative schema terminology is recognized."""
    task = {
        "description": "Design schema first approach with SDL schema document",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.schema_defined is True


def test_alternative_terminology_resolvers():
    """Test alternative resolver terminology is recognized."""
    task = {
        "description": "Implement field resolver logic with resolver mapping layer",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.resolvers_planned is True


def test_alternative_terminology_n_plus_one():
    """Test alternative N+1 terminology is recognized."""
    task = {
        "description": "Optimize queries with batch loader to prevent overfetching",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.n_plus_one_queries_addressed is True


def test_alternative_terminology_caching():
    """Test alternative caching terminology is recognized."""
    task = {
        "description": "Set up query caching with cache invalidation policy",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.caching_strategy_specified is True


def test_alternative_terminology_compatibility():
    """Test alternative compatibility terminology is recognized."""
    task = {
        "description": "Ensure schema evolution without breaking changes and phased rollout",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.backwards_compatibility_maintained is True


def test_to_dict_method():
    """Test GraphQLMigrationReadiness.to_dict() serialization."""
    readiness = GraphQLMigrationReadiness(
        schema_defined=True,
        resolvers_planned=True,
        n_plus_one_queries_addressed=False,
        caching_strategy_specified=True,
        backwards_compatibility_maintained=False,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["schema_defined"] is True
    assert result["resolvers_planned"] is True
    assert result["n_plus_one_queries_addressed"] is False
    assert result["caching_strategy_specified"] is True
    assert result["backwards_compatibility_maintained"] is False
    assert result["readiness_score"] == 0.6


def test_multiple_fields_in_different_sections():
    """Test detection across multiple task data sections."""
    task = {
        "title": "GraphQL setup",
        "description": "Define GraphQL schema",
        "acceptance_criteria": ["Resolver plan documented"],
        "requirements": ["DataLoader configured"],
        "notes": ["Caching strategy needed"],
        "risks": ["No backward compatibility plan"],
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.schema_defined is True
    assert result.resolvers_planned is True
    assert result.n_plus_one_queries_addressed is True
    assert result.caching_strategy_specified is True
    assert result.backwards_compatibility_maintained is True


def test_validation_commands_as_list():
    """Test validation_commands as list."""
    task = {
        "validation_commands": [
            "test_schema_definition.py",
            "test_resolver_implementation.py",
        ],
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.schema_defined is True
    assert result.resolvers_planned is True


def test_no_false_positives_similar_words():
    """Test that similar but different words don't trigger false positives."""
    task = {
        "description": "Create schema for database tables. Resolve conflicts.",
    }

    result = analyze_graphql_migration_readiness(task)

    # "schema" alone without "graphql" context shouldn't trigger
    assert result.schema_defined is False
    # "resolve" alone shouldn't trigger "resolver"
    assert result.resolvers_planned is False


def test_dataclass_immutability():
    """Test that GraphQLMigrationReadiness is frozen/immutable."""
    readiness = GraphQLMigrationReadiness(schema_defined=True)

    with pytest.raises(AttributeError):
        readiness.schema_defined = False  # type: ignore


def test_graphql_file_extension():
    """Test .graphql file extension detection."""
    task = {
        "description": "Create schema.graphql file with type definitions",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.schema_defined is True


def test_apollo_cache_terminology():
    """Test Apollo-specific caching terminology."""
    task = {
        "description": "Configure Apollo cache for client-side state",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.caching_strategy_specified is True


def test_deprecation_as_compatibility():
    """Test deprecation strategy as backwards compatibility."""
    task = {
        "description": "Mark old fields as deprecated for gradual migration",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.backwards_compatibility_maintained is True


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    task = {
        "acceptance_criteria": "Define GraphQL schema and plan resolver implementation",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.schema_defined is True
    assert result.resolvers_planned is True


def test_readiness_score_calculation():
    """Test readiness score calculation with different combinations."""
    # 0/5 = 0.0
    task1 = {"description": "Generic task"}
    result1 = analyze_graphql_migration_readiness(task1)
    assert result1.readiness_score == 0.0

    # 1/5 = 0.2
    task2 = {"description": "Define GraphQL schema"}
    result2 = analyze_graphql_migration_readiness(task2)
    assert result2.readiness_score == 0.2

    # 3/5 = 0.6
    task3 = {
        "description": "GraphQL schema with resolver plan and DataLoader"
    }
    result3 = analyze_graphql_migration_readiness(task3)
    assert result3.readiness_score == 0.6

    # 5/5 = 1.0
    task4 = {
        "description": (
            "GraphQL schema with resolver plan, DataLoader, "
            "caching strategy, and backward compatibility"
        )
    }
    result4 = analyze_graphql_migration_readiness(task4)
    assert result4.readiness_score == 1.0


def test_n_plus_one_alternative_patterns():
    """Test various N+1 query prevention patterns."""
    patterns = [
        "prevent N+1 queries",
        "n+1 problem addressed",
        "query batching enabled",
        "minimize database queries",
    ]

    for pattern in patterns:
        task = {"description": pattern}
        result = analyze_graphql_migration_readiness(task)
        assert result.n_plus_one_queries_addressed is True, f"Failed for pattern: {pattern}"


def test_cdn_caching():
    """Test CDN caching as a valid caching strategy."""
    task = {
        "description": "Implement CDN caching for GraphQL responses",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.caching_strategy_specified is True


def test_api_versioning_compatibility():
    """Test API versioning as backwards compatibility."""
    task = {
        "description": "Implement GraphQL API versioning for client support",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.backwards_compatibility_maintained is True


def test_empty_string_fields():
    """Test handling of empty string fields."""
    task = {
        "title": "",
        "description": "",
        "acceptance_criteria": [""],
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.schema_defined is False
    assert result.readiness_score == 0.0


def test_field_resolver_terminology():
    """Test field resolver as valid resolver terminology."""
    task = {
        "description": "Implement field resolvers for nested queries",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.resolvers_planned is True


def test_query_optimization_as_n_plus_one():
    """Test query optimization recognized as N+1 addressing."""
    task = {
        "description": "Apply query optimization to reduce database calls",
    }

    result = analyze_graphql_migration_readiness(task)

    assert result.n_plus_one_queries_addressed is True
