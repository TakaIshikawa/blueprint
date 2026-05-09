"""Tests for database sharding readiness analyzer."""

import pytest

from blueprint.task_api_database_sharding_readiness import (
    DatabaseShardingReadiness,
    analyze_database_sharding_readiness,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_database_sharding_readiness({})

    assert isinstance(result, DatabaseShardingReadiness)
    assert result.sharding_key_selection_appropriate is False
    assert result.shard_routing_implemented is False
    assert result.cross_shard_query_optimized is False
    assert result.rebalancing_automated is False
    assert result.consistent_hashing_configured is False
    assert result.shard_migration_procedures_tested is False
    assert result.cross_shard_transaction_handled is False
    assert result.shard_health_monitored is False
    assert result.readiness_score == 0.0


def test_sharding_key_selection_detected():
    """Detect sharding key selection in task data."""
    task = {
        "title": "Configure database sharding",
        "description": "Ensure sharding key selection is appropriate for the workload",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.sharding_key_selection_appropriate is True
    assert result.shard_routing_implemented is False
    assert result.readiness_score == 0.125


def test_shard_routing_detected():
    """Detect shard routing implementation in task data."""
    task = {
        "description": "Implement shard routing implementation for query distribution",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_routing_implemented is True
    assert result.sharding_key_selection_appropriate is False


def test_cross_shard_query_detected():
    """Detect cross-shard query optimization in task data."""
    task = {
        "description": "Ensure cross shard query optimization for analytics workloads",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.cross_shard_query_optimized is True
    assert result.sharding_key_selection_appropriate is False


def test_rebalancing_automated_detected():
    """Detect rebalancing automation in task data."""
    task = {
        "description": "Set up rebalancing automation for shard scaling",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.rebalancing_automated is True
    assert result.sharding_key_selection_appropriate is False


def test_consistent_hashing_detected():
    """Detect consistent hashing configuration in task data."""
    task = {
        "description": "Configure consistent hashing configuration for data distribution",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.consistent_hashing_configured is True
    assert result.sharding_key_selection_appropriate is False


def test_shard_migration_detected():
    """Detect shard migration procedures testing in task data."""
    task = {
        "description": "Complete shard migration testing for the production cluster",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_migration_procedures_tested is True
    assert result.sharding_key_selection_appropriate is False


def test_cross_shard_transaction_detected():
    """Detect cross-shard transaction handling in task data."""
    task = {
        "description": "Implement cross shard transaction handling for order processing",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.cross_shard_transaction_handled is True
    assert result.sharding_key_selection_appropriate is False


def test_shard_health_monitoring_detected():
    """Detect shard health monitoring in task data."""
    task = {
        "description": "Set up shard health monitoring across all database clusters",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_health_monitored is True
    assert result.sharding_key_selection_appropriate is False


def test_comprehensive_sharding_all_detected():
    """Test comprehensive sharding implementation with all aspects present."""
    task = {
        "title": "Complete database sharding implementation",
        "description": (
            "Implement database sharding with sharding key selection appropriate for the workload. "
            "Set up shard routing implementation and cross shard query optimization. "
            "Configure rebalancing automation and consistent hashing configuration. "
            "Complete shard migration testing for production readiness. "
            "Implement cross shard transaction handling and shard health monitoring."
        ),
        "acceptance_criteria": [
            "Sharding key validated",
            "Routing logic deployed",
            "Query performance verified",
            "Auto-rebalancing enabled",
            "Hash ring configured",
            "Migration procedures verified",
            "Distributed transactions working",
            "Health dashboards live",
        ],
    }

    result = analyze_database_sharding_readiness(task)

    assert result.sharding_key_selection_appropriate is True
    assert result.shard_routing_implemented is True
    assert result.cross_shard_query_optimized is True
    assert result.rebalancing_automated is True
    assert result.consistent_hashing_configured is True
    assert result.shard_migration_procedures_tested is True
    assert result.cross_shard_transaction_handled is True
    assert result.shard_health_monitored is True
    assert result.readiness_score == 1.0


def test_partial_sharding_readiness():
    """Test partial sharding readiness with some aspects covered."""
    task = {
        "title": "Initial sharding setup",
        "description": "Configure shard key definition and shard routing implementation",
        "acceptance_criteria": [
            "Shard key validated for workload",
            "Routing logic deployed",
        ],
    }

    result = analyze_database_sharding_readiness(task)

    assert result.sharding_key_selection_appropriate is True
    assert result.shard_routing_implemented is True
    assert result.cross_shard_query_optimized is False
    assert result.rebalancing_automated is False
    assert result.consistent_hashing_configured is False
    assert result.shard_migration_procedures_tested is False
    assert result.cross_shard_transaction_handled is False
    assert result.shard_health_monitored is False
    assert result.readiness_score == 0.25


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "SHARD ROUTING IMPLEMENTED with CONSISTENT HASHING CONFIGURED",
        "acceptance_criteria": [
            "CROSS SHARD QUERY OPTIMIZED",
            "SHARD HEALTH MONITORED",
        ],
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_routing_implemented is True
    assert result.consistent_hashing_configured is True
    assert result.cross_shard_query_optimized is True
    assert result.shard_health_monitored is True


def test_alternative_terminology_partition_key():
    """Test partition key choice as sharding key terminology."""
    task = {
        "description": "Ensure partition key chosen correctly for tenant distribution",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.sharding_key_selection_appropriate is True


def test_alternative_terminology_distribution_key():
    """Test distribution key configuration as sharding key terminology."""
    task = {
        "description": "Validate distribution key configuration across nodes",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.sharding_key_selection_appropriate is True


def test_alternative_terminology_shard_lookup():
    """Test shard lookup as routing terminology."""
    task = {
        "description": "Implement shard lookup implementation for request routing",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_routing_implemented is True


def test_alternative_terminology_shard_map():
    """Test shard map configuration as routing terminology."""
    task = {
        "description": "Deploy shard map configuration for query distribution",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_routing_implemented is True


def test_alternative_terminology_scatter_gather():
    """Test scatter-gather as cross-shard query terminology."""
    task = {
        "description": "Optimize scatter gather optimization for analytics queries",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.cross_shard_query_optimized is True


def test_alternative_terminology_fan_out_query():
    """Test fan-out query tuning as cross-shard query terminology."""
    task = {
        "description": "Implement fan out query tuning for distributed search",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.cross_shard_query_optimized is True


def test_alternative_terminology_auto_rebalance():
    """Test auto-rebalance as rebalancing terminology."""
    task = {
        "description": "Enable auto rebalancing for dynamic shard management",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.rebalancing_automated is True


def test_alternative_terminology_automated_resharding():
    """Test automated resharding as rebalancing terminology."""
    task = {
        "description": "Configure automated resharding when load thresholds exceeded",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.rebalancing_automated is True


def test_alternative_terminology_hash_ring():
    """Test hash ring setup as consistent hashing terminology."""
    task = {
        "description": "Deploy hash ring setup for data distribution",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.consistent_hashing_configured is True


def test_alternative_terminology_virtual_nodes():
    """Test virtual nodes configuration as consistent hashing terminology."""
    task = {
        "description": "Configure virtual nodes configuration for balanced distribution",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.consistent_hashing_configured is True


def test_alternative_terminology_migration_verification():
    """Test migration procedure verification as shard migration terminology."""
    task = {
        "description": "Complete migration procedure verification before cutover",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_migration_procedures_tested is True


def test_alternative_terminology_online_resharding():
    """Test online resharding testing as shard migration terminology."""
    task = {
        "description": "Validate online resharding testing for zero-downtime migration",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_migration_procedures_tested is True


def test_alternative_terminology_saga_pattern():
    """Test saga pattern as cross-shard transaction terminology."""
    task = {
        "description": "Implement saga pattern implementation for distributed workflows",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.cross_shard_transaction_handled is True


def test_alternative_terminology_two_phase_commit():
    """Test two-phase commit as cross-shard transaction terminology."""
    task = {
        "description": "Set up two phase commit configuration for atomic operations",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.cross_shard_transaction_handled is True


def test_alternative_terminology_shard_metrics():
    """Test shard metrics collection as health monitoring terminology."""
    task = {
        "description": "Enable shard metrics collection for observability",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_health_monitored is True


def test_alternative_terminology_shard_alerting():
    """Test shard alerting configuration as health monitoring terminology."""
    task = {
        "description": "Set up shard alerting configuration for proactive incident response",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_health_monitored is True


def test_list_based_field_detection():
    """Test detection across multiple task data sections."""
    task = {
        "title": "Sharding infrastructure",
        "description": "Configure sharding key selection appropriate for the workload",
        "acceptance_criteria": ["Shard routing implemented"],
        "requirements": ["Cross shard query optimized"],
        "notes": ["Rebalancing automated for scaling"],
        "risks": ["Shard health monitored for failures"],
    }

    result = analyze_database_sharding_readiness(task)

    assert result.sharding_key_selection_appropriate is True
    assert result.shard_routing_implemented is True
    assert result.cross_shard_query_optimized is True
    assert result.rebalancing_automated is True
    assert result.shard_health_monitored is True


def test_validation_command_field_detection():
    """Test that validation commands are included in analysis."""
    task = {
        "title": "Basic sharding setup",
        "validation_command": "echo 'shard routing implemented' && echo 'shard migration tested'",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_routing_implemented is True
    assert result.shard_migration_procedures_tested is True


def test_validation_commands_as_list():
    """Test validation_commands as list."""
    task = {
        "validation_commands": [
            "check shard routing implemented",
            "verify shard health monitored",
        ],
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_routing_implemented is True
    assert result.shard_health_monitored is True


def test_to_dict_method():
    """Test DatabaseShardingReadiness.to_dict() serialization."""
    readiness = DatabaseShardingReadiness(
        sharding_key_selection_appropriate=True,
        shard_routing_implemented=True,
        cross_shard_query_optimized=False,
        rebalancing_automated=True,
        consistent_hashing_configured=False,
        shard_migration_procedures_tested=True,
        cross_shard_transaction_handled=False,
        shard_health_monitored=True,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["sharding_key_selection_appropriate"] is True
    assert result["shard_routing_implemented"] is True
    assert result["cross_shard_query_optimized"] is False
    assert result["rebalancing_automated"] is True
    assert result["consistent_hashing_configured"] is False
    assert result["shard_migration_procedures_tested"] is True
    assert result["cross_shard_transaction_handled"] is False
    assert result["shard_health_monitored"] is True
    assert result["readiness_score"] == 0.625


def test_dataclass_immutability():
    """Test that DatabaseShardingReadiness is frozen/immutable."""
    readiness = DatabaseShardingReadiness(sharding_key_selection_appropriate=True)

    with pytest.raises(AttributeError):
        readiness.sharding_key_selection_appropriate = False  # type: ignore


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_database_sharding_readiness(None)  # type: ignore

    assert isinstance(result, DatabaseShardingReadiness)
    assert result.sharding_key_selection_appropriate is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_database_sharding_readiness([{"key": "value"}])  # type: ignore

    assert isinstance(result, DatabaseShardingReadiness)
    assert result.sharding_key_selection_appropriate is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_string():
    """Test with string input instead of mapping."""
    result = analyze_database_sharding_readiness("not a mapping")  # type: ignore

    assert isinstance(result, DatabaseShardingReadiness)
    assert result.sharding_key_selection_appropriate is False


def test_invalid_task_data_tuple():
    """Test with tuple input instead of mapping."""
    result = analyze_database_sharding_readiness(("tuple", "data"))  # type: ignore

    assert isinstance(result, DatabaseShardingReadiness)
    assert result.sharding_key_selection_appropriate is False


def test_partial_data_missing_fields():
    """Test with partial task data missing some fields."""
    task = {
        "title": "System configuration",
        # Missing description, acceptance_criteria, etc.
    }

    result = analyze_database_sharding_readiness(task)

    assert isinstance(result, DatabaseShardingReadiness)
    assert result.readiness_score == 0.0


def test_empty_string_fields():
    """Test handling of empty string fields."""
    task = {
        "title": "",
        "description": "",
        "acceptance_criteria": [""],
    }

    result = analyze_database_sharding_readiness(task)

    assert result.sharding_key_selection_appropriate is False
    assert result.readiness_score == 0.0


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    task = {
        "acceptance_criteria": "Sharding key selection appropriate and shard routing implemented",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.sharding_key_selection_appropriate is True
    assert result.shard_routing_implemented is True


def test_readiness_score_calculation():
    """Test readiness score calculation with different combinations."""
    # 0/8 = 0.0
    task1 = {"description": "Generic task"}
    result1 = analyze_database_sharding_readiness(task1)
    assert result1.readiness_score == 0.0

    # 1/8 = 0.125
    task2 = {"description": "Configure shard routing implementation"}
    result2 = analyze_database_sharding_readiness(task2)
    assert result2.readiness_score == 0.125

    # 4/8 = 0.5
    task3 = {
        "description": (
            "Shard routing implemented, cross shard query optimized, "
            "rebalancing automated, consistent hashing configured"
        )
    }
    result3 = analyze_database_sharding_readiness(task3)
    assert result3.readiness_score == 0.5


def test_shard_key_validation():
    """Test shard key validation as sharding key terminology."""
    task = {
        "description": "Complete shard key validation before production deployment",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.sharding_key_selection_appropriate is True


def test_shard_directory_setup():
    """Test shard directory setup as routing terminology."""
    task = {
        "description": "Configure shard directory setup for service discovery",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_routing_implemented is True


def test_distributed_query_optimization():
    """Test distributed query optimization as cross-shard query terminology."""
    task = {
        "description": "Implement distributed query optimization for multi-shard reads",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.cross_shard_query_optimized is True


def test_dynamic_shard_rebalancing():
    """Test dynamic shard rebalancing terminology."""
    task = {
        "description": "Enable dynamic shard rebalancing for elastic scaling",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.rebalancing_automated is True


def test_hash_partitioning_implementation():
    """Test hash partitioning as consistent hashing terminology."""
    task = {
        "description": "Deploy hash partitioning implementation for even data distribution",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.consistent_hashing_configured is True


def test_shard_cutover_testing():
    """Test shard cutover testing as migration terminology."""
    task = {
        "description": "Complete shard cutover testing for production migration",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_migration_procedures_tested is True


def test_distributed_transaction_implementation():
    """Test distributed transaction as cross-shard transaction terminology."""
    task = {
        "description": "Implement distributed transaction implementation for consistency",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.cross_shard_transaction_handled is True


def test_transaction_coordinator_deployment():
    """Test transaction coordinator as cross-shard transaction terminology."""
    task = {
        "description": "Deploy transaction coordinator deployment for saga orchestration",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.cross_shard_transaction_handled is True


def test_shard_performance_tracking():
    """Test shard performance tracking as health monitoring terminology."""
    task = {
        "description": "Enable shard performance tracking for capacity planning",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_health_monitored is True


def test_shard_utilization_monitoring():
    """Test shard utilization monitoring as health monitoring terminology."""
    task = {
        "description": "Configure shard utilization monitoring for resource optimization",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_health_monitored is True


def test_shard_dashboard_deployment():
    """Test shard dashboard deployment as health monitoring terminology."""
    task = {
        "description": "Deploy shard dashboard deployment for operational visibility",
    }

    result = analyze_database_sharding_readiness(task)

    assert result.shard_health_monitored is True
