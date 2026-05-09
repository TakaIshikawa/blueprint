"""Tests for database sharding requirements extractor."""

import pytest

from blueprint.source_api_database_sharding_requirements import (
    DatabaseShardingRequirements,
    extract_database_sharding_requirements,
)


def test_empty_source_data_returns_all_false():
    """Empty source data should return all fields as False."""
    result = extract_database_sharding_requirements({})

    assert isinstance(result, DatabaseShardingRequirements)
    assert result.sharding_key_selection_specified is False
    assert result.shard_routing_logic_defined is False
    assert result.cross_shard_query_handling_specified is False
    assert result.shard_rebalancing_strategy_defined is False
    assert result.consistent_hashing_specified is False
    assert result.shard_migration_procedures_defined is False
    assert result.cross_shard_transaction_handling_specified is False
    assert result.shard_monitoring_included is False
    assert result.completeness_score == 0.0


def test_sharding_key_selection_detected():
    """Detect sharding key selection in source data."""
    source = {
        "title": "Database partitioning strategy",
        "description": "Select the sharding key based on tenant ID for horizontal scaling",
    }

    result = extract_database_sharding_requirements(source)

    assert result.sharding_key_selection_specified is True
    assert result.completeness_score == 0.125


def test_shard_routing_logic_detected():
    """Detect shard routing logic in source data."""
    source = {
        "description": "Implement shard routing to direct queries to the correct shard",
        "requirements": ["Shard lookup must be sub-millisecond", "Route to shard by tenant"],
    }

    result = extract_database_sharding_requirements(source)

    assert result.shard_routing_logic_defined is True


def test_cross_shard_query_handling_detected():
    """Detect cross-shard query handling in source data."""
    source = {
        "description": "Support cross-shard query for aggregation reports",
        "requirements": ["Scatter gather pattern for analytics queries"],
    }

    result = extract_database_sharding_requirements(source)

    assert result.cross_shard_query_handling_specified is True


def test_shard_rebalancing_strategy_detected():
    """Detect shard rebalancing strategy in source data."""
    source = {
        "description": "Implement shard rebalancing when load becomes uneven",
        "requirements": ["Split shard when size exceeds threshold"],
    }

    result = extract_database_sharding_requirements(source)

    assert result.shard_rebalancing_strategy_defined is True


def test_consistent_hashing_detected():
    """Detect consistent hashing in source data."""
    source = {
        "description": "Use consistent hashing with virtual nodes for even distribution",
    }

    result = extract_database_sharding_requirements(source)

    assert result.consistent_hashing_specified is True


def test_shard_migration_procedures_detected():
    """Detect shard migration procedures in source data."""
    source = {
        "description": "Define shard migration procedures for cluster expansion",
        "requirements": ["Online resharding with zero downtime"],
    }

    result = extract_database_sharding_requirements(source)

    assert result.shard_migration_procedures_defined is True


def test_cross_shard_transaction_handling_detected():
    """Detect cross-shard transaction handling in source data."""
    source = {
        "description": "Handle cross-shard transaction using two phase commit protocol",
        "requirements": ["Saga pattern for long-running distributed transactions"],
    }

    result = extract_database_sharding_requirements(source)

    assert result.cross_shard_transaction_handling_specified is True


def test_shard_monitoring_detected():
    """Detect shard monitoring in source data."""
    source = {
        "description": "Set up shard monitoring and health checks",
        "requirements": ["Shard metrics exposed via dashboard", "Shard alerting on high latency"],
    }

    result = extract_database_sharding_requirements(source)

    assert result.shard_monitoring_included is True


def test_comprehensive_all_fields_detected():
    """Test comprehensive source with all aspects present."""
    source = {
        "title": "Complete database sharding implementation",
        "description": (
            "Select the sharding key based on tenant ID. "
            "Implement shard routing logic with a shard directory. "
            "Support cross-shard query via scatter gather pattern. "
            "Define shard rebalancing strategy for uneven loads. "
            "Use consistent hashing with virtual nodes. "
            "Document shard migration procedures for cluster growth. "
            "Handle cross-shard transaction via saga pattern. "
            "Configure shard monitoring dashboards and alerts."
        ),
        "requirements": [
            "Partition key selection documented",
            "Route to shard in sub-millisecond",
            "Fan out query for analytics",
            "Dynamic sharding when load increases",
            "Hash ring with vnodes",
            "Online resharding procedure",
            "Two phase commit for cross-partition writes",
            "Shard health checks configured",
        ],
    }

    result = extract_database_sharding_requirements(source)

    assert result.sharding_key_selection_specified is True
    assert result.shard_routing_logic_defined is True
    assert result.cross_shard_query_handling_specified is True
    assert result.shard_rebalancing_strategy_defined is True
    assert result.consistent_hashing_specified is True
    assert result.shard_migration_procedures_defined is True
    assert result.cross_shard_transaction_handling_specified is True
    assert result.shard_monitoring_included is True
    assert result.completeness_score == 1.0


def test_partial_detection_completeness_score():
    """Test partial detection with some fields True and verify score."""
    source = {
        "description": "Basic sharding setup",
        "requirements": [
            "Choose the sharding key for partitioning",
            "Implement shard routing logic",
            "Monitor shards for performance issues",
        ],
    }

    result = extract_database_sharding_requirements(source)

    assert result.sharding_key_selection_specified is True
    assert result.shard_routing_logic_defined is True
    assert result.shard_monitoring_included is True
    assert result.cross_shard_query_handling_specified is False
    assert result.shard_rebalancing_strategy_defined is False
    assert result.consistent_hashing_specified is False
    assert result.shard_migration_procedures_defined is False
    assert result.cross_shard_transaction_handling_specified is False
    assert result.completeness_score == 3 / 8


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    source = {
        "description": "SHARDING KEY SELECTION with SHARD ROUTING LOGIC and CROSS SHARD QUERY handling",
        "requirements": ["CONSISTENT HASHING configured", "SHARD MONITORING planned"],
    }

    result = extract_database_sharding_requirements(source)

    assert result.sharding_key_selection_specified is True
    assert result.shard_routing_logic_defined is True
    assert result.cross_shard_query_handling_specified is True
    assert result.consistent_hashing_specified is True
    assert result.shard_monitoring_included is True


def test_alternative_terminology_partition_key():
    """Test partition key as alternative to sharding key."""
    source = {
        "description": "Choose the partition key selection for horizontal partitioning",
    }

    result = extract_database_sharding_requirements(source)

    assert result.sharding_key_selection_specified is True


def test_alternative_terminology_hash_ring():
    """Test hash ring as alternative to consistent hashing."""
    source = {
        "description": "Implement hash ring with virtual nodes for distribution",
    }

    result = extract_database_sharding_requirements(source)

    assert result.consistent_hashing_specified is True


def test_alternative_terminology_distributed_transaction():
    """Test distributed transaction as alternative to cross-shard transaction."""
    source = {
        "description": "Use distributed transaction coordination across partitions",
    }

    result = extract_database_sharding_requirements(source)

    assert result.cross_shard_transaction_handling_specified is True


def test_alternative_terminology_federated_query():
    """Test federated query as alternative to cross-shard query."""
    source = {
        "description": "Support federated query across all database partitions",
    }

    result = extract_database_sharding_requirements(source)

    assert result.cross_shard_query_handling_specified is True


def test_alternative_terminology_2pc():
    """Test 2PC abbreviation for two-phase commit."""
    source = {
        "description": "Use 2pc for atomic writes across shards",
    }

    result = extract_database_sharding_requirements(source)

    assert result.cross_shard_transaction_handling_specified is True


def test_list_based_field_detection_requirements():
    """Test detection from requirements list field."""
    source = {
        "requirements": [
            "Define shard key for user data",
            "Implement shard routing for reads",
        ],
    }

    result = extract_database_sharding_requirements(source)

    assert result.sharding_key_selection_specified is True
    assert result.shard_routing_logic_defined is True


def test_list_based_field_detection_acceptance_criteria():
    """Test detection from acceptance_criteria list field."""
    source = {
        "acceptance_criteria": [
            "Distributed query completes in under 500ms",
            "Shard rebalancing completes without downtime",
        ],
    }

    result = extract_database_sharding_requirements(source)

    assert result.cross_shard_query_handling_specified is True
    assert result.shard_rebalancing_strategy_defined is True


def test_list_based_field_detection_definition_of_done():
    """Test detection from definition_of_done list field."""
    source = {
        "definition_of_done": [
            "Shard migration procedures documented and tested",
            "Shard monitoring dashboards configured",
        ],
    }

    result = extract_database_sharding_requirements(source)

    assert result.shard_migration_procedures_defined is True
    assert result.shard_monitoring_included is True


def test_to_dict_method():
    """Test DatabaseShardingRequirements.to_dict() serialization."""
    requirements = DatabaseShardingRequirements(
        sharding_key_selection_specified=True,
        shard_routing_logic_defined=True,
        cross_shard_query_handling_specified=False,
        shard_rebalancing_strategy_defined=True,
        consistent_hashing_specified=False,
        shard_migration_procedures_defined=True,
        cross_shard_transaction_handling_specified=False,
        shard_monitoring_included=True,
    )

    result = requirements.to_dict()

    assert isinstance(result, dict)
    assert result["sharding_key_selection_specified"] is True
    assert result["shard_routing_logic_defined"] is True
    assert result["cross_shard_query_handling_specified"] is False
    assert result["shard_rebalancing_strategy_defined"] is True
    assert result["consistent_hashing_specified"] is False
    assert result["shard_migration_procedures_defined"] is True
    assert result["cross_shard_transaction_handling_specified"] is False
    assert result["shard_monitoring_included"] is True
    assert result["completeness_score"] == 5 / 8


def test_dataclass_immutability():
    """Test that DatabaseShardingRequirements is frozen/immutable."""
    requirements = DatabaseShardingRequirements(sharding_key_selection_specified=True)

    with pytest.raises(AttributeError):
        requirements.sharding_key_selection_specified = False  # type: ignore


def test_invalid_source_data_none():
    """Test with None input."""
    result = extract_database_sharding_requirements(None)  # type: ignore

    assert isinstance(result, DatabaseShardingRequirements)
    assert result.completeness_score == 0.0


def test_invalid_source_data_list():
    """Test with list input instead of mapping."""
    result = extract_database_sharding_requirements([{"key": "value"}])  # type: ignore

    assert isinstance(result, DatabaseShardingRequirements)
    assert result.completeness_score == 0.0


def test_cross_section_detection():
    """Test detection across multiple source data sections."""
    source = {
        "title": "Sharding setup",
        "description": "Choose the sharding key for partitioning",
        "requirements": ["Shard routing logic implemented"],
        "acceptance_criteria": ["Cross shard query handling verified"],
        "notes": ["Consistent hashing preferred"],
        "definition_of_done": ["Shard monitoring operational"],
    }

    result = extract_database_sharding_requirements(source)

    assert result.sharding_key_selection_specified is True
    assert result.shard_routing_logic_defined is True
    assert result.cross_shard_query_handling_specified is True
    assert result.consistent_hashing_specified is True
    assert result.shard_monitoring_included is True
