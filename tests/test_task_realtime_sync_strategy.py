"""Tests for realtime synchronization strategy readiness analyzer."""

import pytest

from blueprint.task_realtime_sync_strategy import (
    RealtimeSyncStrategyReadiness,
    analyze_realtime_sync_strategy,
)


def test_empty_change_brief_returns_all_false():
    """Empty change brief should return all fields as False."""
    result = analyze_realtime_sync_strategy({})

    assert isinstance(result, RealtimeSyncStrategyReadiness)
    assert result.sync_protocol_defined is False
    assert result.conflict_resolution_addressed is False
    assert result.offline_support_implemented is False
    assert result.delta_updates_configured is False
    assert result.connection_resilience_implemented is False
    assert result.state_reconciliation_planned is False
    assert result.bandwidth_optimization_included is False
    assert result.latency_requirements_specified is False
    assert result.scalability_considered is False
    assert result.monitoring_coverage_planned is False


def test_websocket_protocol_detected():
    """Detect WebSocket protocol in change brief."""
    brief = {
        "title": "Implement WebSocket synchronization",
        "description": "Add realtime sync using WebSocket protocol",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.sync_protocol_defined is True
    assert result.conflict_resolution_addressed is False


def test_server_sent_events_detected():
    """Detect Server-Sent Events (SSE) protocol."""
    brief = {
        "description": "Implement server-sent event streaming for realtime updates",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.sync_protocol_defined is True


def test_polling_protocol_detected():
    """Detect polling synchronization strategy."""
    brief = {
        "description": "Use long-polling for realtime communication",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.sync_protocol_defined is True


def test_operational_transform_detected():
    """Detect operational transform conflict resolution."""
    brief = {
        "title": "Add operational transformation",
        "description": "Implement OT algorithm for concurrent edits",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.conflict_resolution_addressed is True
    assert result.sync_protocol_defined is False


def test_crdt_conflict_resolution_detected():
    """Detect CRDT conflict resolution."""
    brief = {
        "description": "Use CRDT for conflict-free replicated data synchronization",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.conflict_resolution_addressed is True


def test_last_write_wins_detected():
    """Detect last-write-wins conflict resolution."""
    brief = {
        "description": "Implement last-write-wins merge strategy for conflicts",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.conflict_resolution_addressed is True


def test_offline_support_detected():
    """Detect offline support in change brief."""
    brief = {
        "title": "Add offline-first capability",
        "description": "Support offline mode with optimistic updates",
        "acceptance_criteria": ["Work offline", "Sync when online"],
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.offline_support_implemented is True


def test_delta_updates_detected():
    """Detect delta updates in change brief."""
    brief = {
        "description": "Implement delta sync with incremental updates",
        "acceptance_criteria": ["Send only changes", "Partial update support"],
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.delta_updates_configured is True


def test_connection_resilience_detected():
    """Detect connection resilience in change brief."""
    brief = {
        "title": "Add connection resilience",
        "description": "Implement auto-reconnect with exponential backoff",
        "acceptance_criteria": ["Handle disconnects", "Retry logic tested"],
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.connection_resilience_implemented is True


def test_state_reconciliation_detected():
    """Detect state reconciliation in change brief."""
    brief = {
        "description": "Implement state reconciliation after reconnection",
        "acceptance_criteria": ["Merge state correctly", "Consistency check passed"],
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.state_reconciliation_planned is True


def test_bandwidth_optimization_detected():
    """Detect bandwidth optimization in change brief."""
    brief = {
        "title": "Optimize bandwidth usage",
        "description": "Add compression and debouncing to minimize traffic",
        "acceptance_criteria": ["Batch updates", "Throttle sync requests"],
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.bandwidth_optimization_included is True


def test_latency_requirements_detected():
    """Detect latency requirements in change brief."""
    brief = {
        "description": "Meet low-latency requirements with real-time sync",
        "acceptance_criteria": ["Latency < 100ms", "Response time measured"],
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.latency_requirements_specified is True


def test_scalability_detected():
    """Detect scalability considerations in change brief."""
    brief = {
        "title": "Scale WebSocket connections",
        "description": "Add horizontal scaling with load balancing for distributed sync",
        "acceptance_criteria": ["Cluster support", "Shard connections"],
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.scalability_considered is True


def test_monitoring_coverage_detected():
    """Detect monitoring coverage in change brief."""
    brief = {
        "description": "Add monitoring for sync metrics and WebSocket telemetry",
        "acceptance_criteria": ["Track sync latency", "Log connection metrics"],
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.monitoring_coverage_planned is True


def test_comprehensive_realtime_sync_all_aspects_detected():
    """Test comprehensive realtime sync with all aspects present."""
    brief = {
        "title": "Complete realtime synchronization implementation",
        "description": (
            "Implement realtime sync using WebSocket protocol with CRDT conflict resolution. "
            "Support offline-first mode with delta updates and optimistic sync. "
            "Add auto-reconnect with connection resilience and state reconciliation. "
            "Optimize bandwidth with compression and batching. "
            "Meet low-latency requirements with horizontal scalability. "
            "Include monitoring for sync metrics and telemetry."
        ),
        "acceptance_criteria": [
            "WebSocket protocol implemented",
            "CRDT conflict resolution tested",
            "Offline support working",
            "Delta updates configured",
            "Connection resilience verified",
            "State reconciliation tested",
            "Bandwidth optimization enabled",
            "Latency requirements met",
            "Scalability demonstrated",
            "Monitoring coverage complete",
        ],
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.sync_protocol_defined is True
    assert result.conflict_resolution_addressed is True
    assert result.offline_support_implemented is True
    assert result.delta_updates_configured is True
    assert result.connection_resilience_implemented is True
    assert result.state_reconciliation_planned is True
    assert result.bandwidth_optimization_included is True
    assert result.latency_requirements_specified is True
    assert result.scalability_considered is True
    assert result.monitoring_coverage_planned is True


def test_network_partition_handling():
    """Test network partition handling (edge case)."""
    brief = {
        "description": "Handle network partitions with connection recovery",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.connection_resilience_implemented is True


def test_concurrent_edits_handling():
    """Test concurrent edits handling (edge case)."""
    brief = {
        "description": "Resolve concurrent edit conflicts with merge strategy",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.conflict_resolution_addressed is True


def test_sync_storm_prevention():
    """Test sync storm prevention (edge case)."""
    brief = {
        "description": "Prevent sync storm with throttling and batching",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.bandwidth_optimization_included is True


def test_invalid_change_brief_non_mapping():
    """Test with invalid input (non-mapping type)."""
    result = analyze_realtime_sync_strategy("not a mapping")

    assert isinstance(result, RealtimeSyncStrategyReadiness)
    assert result.sync_protocol_defined is False


def test_invalid_change_brief_none():
    """Test with None input."""
    result = analyze_realtime_sync_strategy(None)

    assert isinstance(result, RealtimeSyncStrategyReadiness)
    assert result.sync_protocol_defined is False


def test_invalid_change_brief_list():
    """Test with list input instead of mapping."""
    result = analyze_realtime_sync_strategy([{"key": "value"}])

    assert isinstance(result, RealtimeSyncStrategyReadiness)
    assert result.sync_protocol_defined is False


def test_change_brief_with_nested_acceptance_criteria():
    """Test extraction from nested acceptance criteria structure."""
    brief = {
        "title": "Realtime sync improvements",
        "acceptance_criteria": [
            "Implement WebSocket protocol",
            "Add conflict resolution with CRDT",
            "Support offline mode",
            "Optimize bandwidth",
        ],
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.sync_protocol_defined is True
    assert result.conflict_resolution_addressed is True
    assert result.offline_support_implemented is True
    assert result.bandwidth_optimization_included is True


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    brief = {
        "description": "WEBSOCKET with CONFLICT RESOLUTION and OFFLINE SUPPORT",
        "acceptance_criteria": ["DELTA UPDATES enabled", "CONNECTION RESILIENCE tested"],
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.sync_protocol_defined is True
    assert result.conflict_resolution_addressed is True
    assert result.offline_support_implemented is True
    assert result.delta_updates_configured is True
    assert result.connection_resilience_implemented is True


def test_to_dict_method():
    """Test RealtimeSyncStrategyReadiness.to_dict() serialization."""
    readiness = RealtimeSyncStrategyReadiness(
        sync_protocol_defined=True,
        conflict_resolution_addressed=True,
        offline_support_implemented=False,
        delta_updates_configured=True,
        connection_resilience_implemented=False,
        state_reconciliation_planned=True,
        bandwidth_optimization_included=False,
        latency_requirements_specified=False,
        scalability_considered=True,
        monitoring_coverage_planned=False,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["sync_protocol_defined"] is True
    assert result["conflict_resolution_addressed"] is True
    assert result["offline_support_implemented"] is False
    assert result["delta_updates_configured"] is True
    assert result["connection_resilience_implemented"] is False
    assert result["state_reconciliation_planned"] is True
    assert result["bandwidth_optimization_included"] is False
    assert result["latency_requirements_specified"] is False
    assert result["scalability_considered"] is True
    assert result["monitoring_coverage_planned"] is False


def test_dataclass_immutability():
    """Test that RealtimeSyncStrategyReadiness is frozen/immutable."""
    readiness = RealtimeSyncStrategyReadiness(sync_protocol_defined=True)

    with pytest.raises(AttributeError):
        readiness.sync_protocol_defined = False


def test_alternative_sync_protocol_terminology():
    """Test alternative sync protocol terminology."""
    brief = {
        "description": "Implement bidirectional communication for realtime sync",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.sync_protocol_defined is True


def test_local_first_offline_terminology():
    """Test local-first as offline support."""
    brief = {
        "description": "Build local-first application with background sync",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.offline_support_implemented is True


def test_changeset_as_delta_updates():
    """Test changeset propagation as delta updates."""
    brief = {
        "description": "Sync changesets incrementally",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.delta_updates_configured is True


def test_patch_as_delta_updates():
    """Test patch as delta updates."""
    brief = {
        "description": "Apply patches for partial update sync",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.delta_updates_configured is True


def test_reconnection_as_resilience():
    """Test reconnection logic as connection resilience."""
    brief = {
        "description": "Implement reconnection with retry strategy",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.connection_resilience_implemented is True


def test_sync_reconciliation_terminology():
    """Test sync reconciliation as state reconciliation."""
    brief = {
        "description": "Perform sync reconciliation after network recovery",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.state_reconciliation_planned is True


def test_multiple_fields_in_different_sections():
    """Test detection across multiple brief sections."""
    brief = {
        "title": "Realtime sync setup",
        "description": "Use WebSocket protocol",
        "acceptance_criteria": ["Add conflict resolution"],
        "requirements": ["Support offline mode"],
        "notes": ["Optimize bandwidth"],
        "risks": ["Latency requirements not met"],
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.sync_protocol_defined is True
    assert result.conflict_resolution_addressed is True
    assert result.offline_support_implemented is True
    assert result.bandwidth_optimization_included is True
    assert result.latency_requirements_specified is True


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    brief = {
        "acceptance_criteria": "Implement WebSocket and add conflict resolution with CRDT",
    }

    result = analyze_realtime_sync_strategy(brief)

    assert result.sync_protocol_defined is True
    assert result.conflict_resolution_addressed is True
