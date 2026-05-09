"""Tests for horizontal scaling readiness analyzer."""

import pytest

from blueprint.task_horizontal_scaling_readiness import (
    HorizontalScalingReadiness,
    analyze_horizontal_scaling_readiness,
)


def test_empty_change_brief_returns_all_false():
    """Empty change brief should return all fields as False."""
    result = analyze_horizontal_scaling_readiness({})

    assert isinstance(result, HorizontalScalingReadiness)
    assert result.stateless_design_implemented is False
    assert result.session_handling_externalized is False
    assert result.shared_state_managed is False
    assert result.load_balancing_configured is False
    assert result.in_memory_state_avoided is False
    assert result.file_system_dependencies_removed is False
    assert result.singleton_patterns_eliminated is False
    assert result.distributed_locks_implemented is False
    assert result.statelessness_achieved is False
    assert result.external_state_storage_configured is False
    assert result.cache_coherence_addressed is False
    assert result.deployment_flexibility_enabled is False


def test_stateless_design_detected():
    """Detect stateless design in change brief."""
    brief = {
        "title": "Implement stateless architecture",
        "description": "Make service stateless for horizontal scaling",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.stateless_design_implemented is True
    assert result.session_handling_externalized is False


def test_stateless_components_detected():
    """Detect stateless components."""
    brief = {
        "description": "Design stateless components for better scalability",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.stateless_design_implemented is True


def test_stateless_api_detected():
    """Detect stateless API design."""
    brief = {
        "description": "Ensure stateless API endpoints for scaling",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.stateless_design_implemented is True


def test_session_handling_detected():
    """Detect session handling externalization."""
    brief = {
        "title": "Externalize session storage",
        "description": "Move sessions to Redis for distributed session management",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.session_handling_externalized is True


def test_sticky_sessions_detected():
    """Detect sticky sessions consideration."""
    brief = {
        "description": "Implement sticky sessions for session affinity",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.session_handling_externalized is True


def test_external_session_storage_detected():
    """Detect external session storage."""
    brief = {
        "description": "Use centralized session store for all instances",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.session_handling_externalized is True


def test_shared_state_detected():
    """Detect shared state management."""
    brief = {
        "title": "Implement shared state",
        "description": "Use Redis for distributed state management",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.shared_state_managed is True


def test_external_state_storage_detected():
    """Detect external state storage."""
    brief = {
        "description": "Externalize state to distributed cache",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.shared_state_managed is True


def test_centralized_state_detected():
    """Detect centralized state management."""
    brief = {
        "description": "Move to centralized state storage for scaling",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.shared_state_managed is True


def test_load_balancing_detected():
    """Detect load balancing configuration."""
    brief = {
        "title": "Configure load balancer",
        "description": "Set up load balancing with round-robin strategy",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.load_balancing_configured is True


def test_nginx_load_balancing_detected():
    """Detect Nginx load balancing."""
    brief = {
        "description": "Use nginx load balancing for traffic distribution",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.load_balancing_configured is True


def test_alb_detected():
    """Detect ALB (Application Load Balancer)."""
    brief = {
        "description": "Configure ALB load balancer for the service",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.load_balancing_configured is True


def test_least_connections_detected():
    """Detect least connections load balancing."""
    brief = {
        "description": "Use least-connections algorithm for load balancing",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.load_balancing_configured is True


def test_in_memory_state_avoided():
    """Detect in-memory state avoidance."""
    brief = {
        "title": "Remove in-memory state",
        "description": "Eliminate in-memory state for stateless design",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.in_memory_state_avoided is True


def test_local_state_elimination_detected():
    """Detect local state elimination."""
    brief = {
        "description": "Avoid local state to enable horizontal scaling",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.in_memory_state_avoided is True


def test_in_process_cache_avoided():
    """Detect in-process cache avoidance."""
    brief = {
        "description": "Remove in-process cache for scaling",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.in_memory_state_avoided is True


def test_file_system_dependencies_detected():
    """Detect file system dependencies removal."""
    brief = {
        "title": "Remove file system dependencies",
        "description": "Use S3 instead of local file system",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.file_system_dependencies_removed is True


def test_shared_file_system_detected():
    """Detect shared file system usage."""
    brief = {
        "description": "Migrate to network file system for shared access",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.file_system_dependencies_removed is True


def test_object_storage_migration_detected():
    """Detect migration to object storage."""
    brief = {
        "description": "Move files to blob storage instead of local disk",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.file_system_dependencies_removed is True


def test_singleton_patterns_eliminated():
    """Detect singleton pattern elimination."""
    brief = {
        "title": "Eliminate singleton pattern",
        "description": "Remove singleton instances to enable scaling",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.singleton_patterns_eliminated is True


def test_singleton_anti_pattern_detected():
    """Detect singleton as anti-pattern for scaling."""
    brief = {
        "description": "Address singleton anti-pattern blocker",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.singleton_patterns_eliminated is True


def test_distributed_locks_detected():
    """Detect distributed locks implementation."""
    brief = {
        "title": "Implement distributed locks",
        "description": "Use Redis locks for coordination across instances",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.distributed_locks_implemented is True


def test_redis_locks_detected():
    """Detect Redis distributed locks."""
    brief = {
        "description": "Add redis locks for distributed locking",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.distributed_locks_implemented is True


def test_optimistic_locking_detected():
    """Detect optimistic locking strategy."""
    brief = {
        "description": "Implement optimistic locking for concurrency control",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.distributed_locks_implemented is True


def test_distributed_mutex_detected():
    """Detect distributed mutex."""
    brief = {
        "description": "Use distributed mutex for lock coordination",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.distributed_locks_implemented is True


def test_statelessness_achieved():
    """Detect statelessness achievement."""
    brief = {
        "title": "Achieve statelessness",
        "description": "Ensure statelessness across all instances",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.statelessness_achieved is True


def test_stateless_servers_detected():
    """Detect stateless servers configuration."""
    brief = {
        "description": "Configure stateless nodes for horizontal scaling",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.statelessness_achieved is True


def test_external_state_storage_configuration_detected():
    """Detect external state storage configuration."""
    brief = {
        "title": "Configure external state storage",
        "description": "Store state externally in Redis",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.external_state_storage_configured is True


def test_externalize_state_detected():
    """Detect state externalization."""
    brief = {
        "description": "Externalize state for scaling readiness",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.external_state_storage_configured is True


def test_move_state_to_database_detected():
    """Detect moving state to database."""
    brief = {
        "description": "Move state to database from local memory",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.external_state_storage_configured is True


def test_cache_coherence_detected():
    """Detect cache coherence addressing."""
    brief = {
        "title": "Address cache coherence",
        "description": "Implement cache invalidation for consistency",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.cache_coherence_addressed is True


def test_cache_consistency_detected():
    """Detect cache consistency management."""
    brief = {
        "description": "Ensure cache consistency across instances",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.cache_coherence_addressed is True


def test_distributed_cache_detected():
    """Detect distributed cache usage."""
    brief = {
        "description": "Use distributed cache for scaling",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.cache_coherence_addressed is True


def test_deployment_flexibility_detected():
    """Detect deployment flexibility."""
    brief = {
        "title": "Enable deployment flexibility",
        "description": "Support horizontal deployment with auto-scaling",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.deployment_flexibility_enabled is True


def test_scale_horizontally_detected():
    """Detect horizontal scaling capability."""
    brief = {
        "description": "Enable ability to scale horizontally",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.deployment_flexibility_enabled is True


def test_autoscaling_detected():
    """Detect auto-scaling configuration."""
    brief = {
        "description": "Configure auto-scaling for dynamic capacity",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.deployment_flexibility_enabled is True


def test_add_instances_detected():
    """Detect adding instances for scaling."""
    brief = {
        "description": "Support adding instances dynamically",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.deployment_flexibility_enabled is True


def test_comprehensive_horizontal_scaling_all_aspects_detected():
    """Test comprehensive horizontal scaling with all aspects present."""
    brief = {
        "title": "Complete horizontal scaling implementation",
        "description": (
            "Implement stateless architecture with external session storage in Redis. "
            "Use distributed cache for shared state management. "
            "Configure load balancing with round-robin and least-connections. "
            "Eliminate in-memory state and remove local file system dependencies. "
            "Remove singleton patterns and implement distributed locks. "
            "Achieve statelessness with external state storage. "
            "Address cache coherence and enable deployment flexibility with auto-scaling."
        ),
        "acceptance_criteria": [
            "Stateless design implemented",
            "Session handling externalized",
            "Shared state managed",
            "Load balancing configured",
            "In-memory state avoided",
            "File system dependencies removed",
            "Singleton patterns eliminated",
            "Distributed locks implemented",
            "Statelessness achieved",
            "External state storage configured",
            "Cache coherence addressed",
            "Deployment flexibility enabled",
        ],
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.stateless_design_implemented is True
    assert result.session_handling_externalized is True
    assert result.shared_state_managed is True
    assert result.load_balancing_configured is True
    assert result.in_memory_state_avoided is True
    assert result.file_system_dependencies_removed is True
    assert result.singleton_patterns_eliminated is True
    assert result.distributed_locks_implemented is True
    assert result.statelessness_achieved is True
    assert result.external_state_storage_configured is True
    assert result.cache_coherence_addressed is True
    assert result.deployment_flexibility_enabled is True


def test_background_jobs_edge_case():
    """Test background jobs with stateless design (edge case)."""
    brief = {
        "description": "Make background jobs stateless for scaling",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.stateless_design_implemented is True


def test_scheduled_tasks_edge_case():
    """Test scheduled tasks with distributed locks (edge case)."""
    brief = {
        "description": "Use distributed locks for scheduled tasks coordination",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.distributed_locks_implemented is True


def test_distributed_transactions_edge_case():
    """Test distributed transactions with external state (edge case)."""
    brief = {
        "description": "Store transaction state externally for distributed processing",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.external_state_storage_configured is True


def test_invalid_change_brief_non_mapping():
    """Test with invalid input (non-mapping type)."""
    result = analyze_horizontal_scaling_readiness("not a mapping")

    assert isinstance(result, HorizontalScalingReadiness)
    assert result.stateless_design_implemented is False


def test_invalid_change_brief_none():
    """Test with None input."""
    result = analyze_horizontal_scaling_readiness(None)

    assert isinstance(result, HorizontalScalingReadiness)
    assert result.stateless_design_implemented is False


def test_invalid_change_brief_list():
    """Test with list input instead of mapping."""
    result = analyze_horizontal_scaling_readiness([{"key": "value"}])

    assert isinstance(result, HorizontalScalingReadiness)
    assert result.stateless_design_implemented is False


def test_change_brief_with_nested_acceptance_criteria():
    """Test extraction from nested acceptance criteria structure."""
    brief = {
        "title": "Horizontal scaling improvements",
        "acceptance_criteria": [
            "Implement stateless design",
            "Externalize session handling",
            "Configure load balancing",
            "Remove singleton patterns",
        ],
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.stateless_design_implemented is True
    assert result.session_handling_externalized is True
    assert result.load_balancing_configured is True
    assert result.singleton_patterns_eliminated is True


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    brief = {
        "description": "STATELESS DESIGN with LOAD BALANCING",
        "acceptance_criteria": ["DISTRIBUTED LOCKS", "EXTERNAL STATE STORAGE"],
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.stateless_design_implemented is True
    assert result.load_balancing_configured is True
    assert result.distributed_locks_implemented is True
    assert result.external_state_storage_configured is True


def test_to_dict_method():
    """Test HorizontalScalingReadiness.to_dict() serialization."""
    readiness = HorizontalScalingReadiness(
        stateless_design_implemented=True,
        session_handling_externalized=True,
        shared_state_managed=False,
        load_balancing_configured=True,
        in_memory_state_avoided=False,
        file_system_dependencies_removed=True,
        singleton_patterns_eliminated=False,
        distributed_locks_implemented=True,
        statelessness_achieved=False,
        external_state_storage_configured=True,
        cache_coherence_addressed=False,
        deployment_flexibility_enabled=True,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["stateless_design_implemented"] is True
    assert result["session_handling_externalized"] is True
    assert result["shared_state_managed"] is False
    assert result["load_balancing_configured"] is True
    assert result["in_memory_state_avoided"] is False
    assert result["file_system_dependencies_removed"] is True
    assert result["singleton_patterns_eliminated"] is False
    assert result["distributed_locks_implemented"] is True
    assert result["statelessness_achieved"] is False
    assert result["external_state_storage_configured"] is True
    assert result["cache_coherence_addressed"] is False
    assert result["deployment_flexibility_enabled"] is True


def test_dataclass_immutability():
    """Test that HorizontalScalingReadiness is frozen/immutable."""
    readiness = HorizontalScalingReadiness(stateless_design_implemented=True)

    with pytest.raises(AttributeError):
        readiness.stateless_design_implemented = False


def test_multiple_fields_in_different_sections():
    """Test detection across multiple brief sections."""
    brief = {
        "title": "Scaling improvements",
        "description": "Implement stateless design",
        "acceptance_criteria": ["Configure load balancing"],
        "requirements": ["Externalize sessions"],
        "notes": ["Use distributed locks"],
        "risks": ["Singleton pattern blocker"],
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.stateless_design_implemented is True
    assert result.load_balancing_configured is True
    assert result.session_handling_externalized is True
    assert result.distributed_locks_implemented is True
    assert result.singleton_patterns_eliminated is True


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    brief = {
        "acceptance_criteria": "Implement stateless architecture and configure load balancing",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.stateless_design_implemented is True
    assert result.load_balancing_configured is True


def test_haproxy_load_balancing_detected():
    """Test HAProxy load balancing detection."""
    brief = {
        "description": "Use haproxy load balancing for high availability",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.load_balancing_configured is True


def test_nfs_file_system_detected():
    """Test NFS as shared file system."""
    brief = {
        "description": "Migrate to NFS for shared file access",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.file_system_dependencies_removed is True


def test_zookeeper_locks_detected():
    """Test Zookeeper distributed locks."""
    brief = {
        "description": "Use zookeeper locks for coordination",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.distributed_locks_implemented is True


def test_etcd_locks_detected():
    """Test etcd distributed locks."""
    brief = {
        "description": "Implement etcd locks for distributed coordination",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.distributed_locks_implemented is True


def test_cache_warming_detected():
    """Test cache warming as cache coherence."""
    brief = {
        "description": "Implement cache warming for consistency",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.cache_coherence_addressed is True


def test_scale_out_deployment_detected():
    """Test scale-out deployment terminology."""
    brief = {
        "description": "Enable scale-out deployment strategy",
    }

    result = analyze_horizontal_scaling_readiness(brief)

    assert result.deployment_flexibility_enabled is True
