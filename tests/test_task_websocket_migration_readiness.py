import json
from types import SimpleNamespace

from blueprint.task_websocket_migration_readiness import (
    TaskWebSocketMigrationReadinessFinding,
    TaskWebSocketMigrationReadinessPlan,
    build_task_websocket_migration_readiness_plan,
    derive_task_websocket_migration_readiness_plan,
    extract_task_websocket_migration_readiness_findings,
    generate_task_websocket_migration_readiness_plan,
    summarize_task_websocket_migration_readiness,
)


def test_protocol_upgrade_detected_with_partial_readiness():
    """Test basic protocol upgrade detection with partial safeguards."""
    plan = {
        "id": "plan-ws-upgrade",
        "tasks": [
            {
                "id": "task-upgrade",
                "title": "Implement WebSocket protocol upgrade",
                "description": "Add WebSocket upgrade handshake with Sec-WebSocket-Key validation. Implement 101 Switching Protocols response.",
                "acceptance_criteria": [
                    "Protocol upgrade handshake is implemented",
                    "Sec-WebSocket-Accept header is validated",
                ],
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)
    finding = result.findings[0]

    assert isinstance(result, TaskWebSocketMigrationReadinessPlan)
    assert isinstance(finding, TaskWebSocketMigrationReadinessFinding)
    assert finding.task_id == "task-upgrade"
    assert "protocol_upgrade" in finding.detected_signals
    assert finding.readiness in {"weak", "partial"}
    assert len(finding.actionable_remediations) > 0


def test_fallback_strategy_with_weak_readiness():
    """Test fallback strategy detection without proper tests results in weak readiness."""
    plan = {
        "id": "plan-fallback",
        "tasks": [
            {
                "id": "task-fallback",
                "title": "Add graceful fallback to long-polling",
                "description": "Implement fallback to HTTP long-polling when WebSocket upgrade fails. Support progressive enhancement.",
                "acceptance_criteria": ["Fallback transport is configured when WebSocket is unavailable"],
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)
    finding = result.findings[0]

    assert "fallback_strategy" in finding.detected_signals
    assert "fallback_mechanism_tests" not in finding.present_safeguards
    assert "fallback_mechanism_tests" in finding.missing_safeguards
    assert finding.readiness == "weak"
    assert any("fallback" in r.lower() for r in finding.actionable_remediations)


def test_strong_readiness_with_comprehensive_safeguards():
    """Test comprehensive WebSocket migration with all safeguards results in strong readiness."""
    plan = {
        "id": "plan-complete-migration",
        "tasks": [
            {
                "id": "task-complete",
                "title": "Complete WebSocket migration with full test coverage",
                "description": (
                    "Implement WebSocket protocol upgrade with proper handshake validation. "
                    "Add fallback strategy to HTTP long-polling for graceful degradation. "
                    "Implement state synchronization between HTTP and WebSocket sessions. "
                    "Handle connection lifecycle including open, close, error, and message events. "
                    "Configure message routing with pub/sub pattern for event dispatch. "
                    "Maintain backward compatibility with legacy polling clients."
                ),
                "acceptance_criteria": [
                    "Upgrade detection tests verify protocol switching and handshake validation",
                    "Fallback mechanism tests ensure graceful degradation to polling without data loss",
                    "State sync tests validate reconciliation between HTTP and WebSocket sessions",
                    "Connection failure tests cover network drops, reconnection, and state recovery",
                    "Migration rollback plan documents feature flags and revert strategy",
                    "Client compatibility matrix lists WebSocket support across target browsers",
                ],
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)
    finding = result.findings[0]

    # All signals should be detected
    assert "protocol_upgrade" in finding.detected_signals
    assert "fallback_strategy" in finding.detected_signals
    assert "state_synchronization" in finding.detected_signals
    assert "connection_lifecycle" in finding.detected_signals
    assert "message_routing" in finding.detected_signals
    assert "backward_compatibility" in finding.detected_signals

    # All safeguards should be present
    assert "upgrade_detection_tests" in finding.present_safeguards
    assert "fallback_mechanism_tests" in finding.present_safeguards
    assert "state_sync_tests" in finding.present_safeguards
    assert "connection_failure_tests" in finding.present_safeguards
    assert "migration_rollback_plan" in finding.present_safeguards
    assert "client_compatibility_matrix" in finding.present_safeguards

    assert len(finding.missing_safeguards) == 0
    assert finding.readiness == "strong"
    assert len(finding.actionable_remediations) == 0


def test_connection_failure_and_state_sync_detection():
    """Test detection of connection failure handling and state synchronization."""
    plan = {
        "id": "plan-resilience",
        "tasks": [
            {
                "id": "task-resilience",
                "title": "Add WebSocket resilience and state management",
                "description": (
                    "Implement connection lifecycle management with proper cleanup. "
                    "Add state reconciliation for session consistency. "
                    "Handle connection failures with automatic reconnection."
                ),
                "acceptance_criteria": [
                    "Connection failure tests validate resilience to network drops",
                    "State sync tests ensure consistent state across reconnections",
                ],
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)
    finding = result.findings[0]

    assert "connection_lifecycle" in finding.detected_signals
    assert "state_synchronization" in finding.detected_signals
    assert "connection_failure_tests" in finding.present_safeguards
    assert "state_sync_tests" in finding.present_safeguards


def test_backward_compatibility_and_rollback_detection():
    """Test detection of backward compatibility and migration rollback planning."""
    plan = {
        "id": "plan-compat",
        "tasks": [
            {
                "id": "task-compat",
                "title": "Ensure backward compatibility during WebSocket migration",
                "description": (
                    "Maintain support for legacy polling clients during gradual migration. "
                    "Implement phased migration with version compatibility layer. "
                    "Document migration rollback strategy with feature flags."
                ),
                "acceptance_criteria": [
                    "Legacy client support is maintained throughout migration",
                    "Migration rollback plan includes revert procedure and contingency steps",
                ],
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)
    finding = result.findings[0]

    assert "backward_compatibility" in finding.detected_signals
    assert "migration_rollback_plan" in finding.present_safeguards


def test_no_websocket_scope_is_filtered_out():
    """Test that tasks explicitly stating no WebSocket impact are filtered out."""
    plan = {
        "id": "plan-no-ws",
        "tasks": [
            {
                "id": "task-no-ws",
                "title": "Update API endpoint",
                "description": "This change has no WebSocket migration impact. No WebSocket changes required.",
                "acceptance_criteria": ["Endpoint returns updated data"],
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)

    assert len(result.findings) == 0
    assert "task-no-ws" in result.not_applicable_task_ids


def test_partial_upgrade_scenario():
    """Test partial upgrade scenario with missing critical safeguards."""
    plan = {
        "id": "plan-partial",
        "tasks": [
            {
                "id": "task-partial",
                "title": "Add WebSocket support with basic upgrade",
                "description": (
                    "Implement WebSocket protocol upgrade and connection handling. "
                    "Add message routing for event dispatch."
                ),
                "acceptance_criteria": [
                    "WebSocket connections are established",
                    "Messages are routed correctly",
                ],
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)
    finding = result.findings[0]

    assert "protocol_upgrade" in finding.detected_signals
    assert "message_routing" in finding.detected_signals
    assert "fallback_strategy" not in finding.detected_signals
    assert finding.readiness in {"weak", "partial"}
    assert "fallback_mechanism_tests" in finding.missing_safeguards
    assert len(finding.actionable_remediations) > 0


def test_connection_failure_edge_cases():
    """Test edge cases for connection failure scenarios."""
    plan = {
        "id": "plan-failure-edge",
        "tasks": [
            {
                "id": "task-failure",
                "title": "Handle WebSocket connection edge cases",
                "description": (
                    "Test connection drops during active sessions. "
                    "Test network interruptions and server restarts. "
                    "Verify reconnection with state recovery after outages."
                ),
                "acceptance_criteria": [
                    "Connection failure tests cover network drops and server restarts",
                    "Reconnection tests verify state recovery",
                ],
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)
    finding = result.findings[0]

    assert "connection_lifecycle" in finding.detected_signals
    assert "connection_failure_tests" in finding.present_safeguards


def test_state_reconciliation_scenarios():
    """Test state reconciliation between different connection states."""
    plan = {
        "id": "plan-state-recon",
        "tasks": [
            {
                "id": "task-recon",
                "title": "Implement state reconciliation logic",
                "description": (
                    "Reconcile state between HTTP and WebSocket sessions. "
                    "Ensure shared state consistency across connection changes. "
                    "Validate client state synchronization during transport switching."
                ),
                "acceptance_criteria": [
                    "State sync tests validate reconciliation logic",
                    "State consistency tests ensure accurate shared state",
                ],
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)
    finding = result.findings[0]

    assert "state_synchronization" in finding.detected_signals
    assert "state_sync_tests" in finding.present_safeguards


def test_compatibility_functions():
    """Test that derive and generate functions work as expected."""
    plan = {
        "id": "plan-compat-func",
        "tasks": [
            {
                "id": "task-compat",
                "title": "Add WebSocket upgrade",
                "description": "Implement protocol upgrade to WebSocket",
            },
        ],
    }

    result1 = build_task_websocket_migration_readiness_plan(plan)
    result2 = derive_task_websocket_migration_readiness_plan(plan)
    result3 = generate_task_websocket_migration_readiness_plan(plan)

    assert len(result1.findings) == len(result2.findings) == len(result3.findings)
    assert result1.findings[0].task_id == result2.findings[0].task_id == result3.findings[0].task_id


def test_extract_findings():
    """Test extracting findings directly."""
    plan = {
        "id": "plan-extract",
        "tasks": [
            {
                "id": "task-extract",
                "title": "Implement WebSocket fallback",
                "description": "Add fallback to polling mechanism",
            },
        ],
    }

    findings = extract_task_websocket_migration_readiness_findings(plan)

    assert len(findings) == 1
    assert findings[0].task_id == "task-extract"


def test_summarize_readiness():
    """Test summarizing WebSocket migration readiness."""
    plan = {
        "id": "plan-summary",
        "tasks": [
            {
                "id": "task-summary-1",
                "title": "Add WebSocket with tests",
                "description": "Implement upgrade with fallback mechanism tests",
                "acceptance_criteria": ["Fallback mechanism tests verify graceful degradation"],
            },
            {
                "id": "task-summary-2",
                "title": "No WebSocket needed",
                "description": "This has no migration impact",
            },
        ],
    }

    summary = summarize_task_websocket_migration_readiness(plan)

    assert "migration_task_count" in summary
    assert "not_applicable_task_count" in summary
    assert "readiness_counts" in summary
    assert "overall_readiness" in summary
    assert summary["migration_task_count"] >= 1


def test_to_dict_serialization():
    """Test that findings can be serialized to dictionaries."""
    plan = {
        "id": "plan-serialize",
        "tasks": [
            {
                "id": "task-serialize",
                "title": "Add WebSocket migration",
                "description": "Implement protocol upgrade",
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)
    result_dict = result.to_dict()
    finding_dicts = result.to_dicts()

    assert isinstance(result_dict, dict)
    assert "plan_id" in result_dict
    assert "findings" in result_dict
    assert isinstance(finding_dicts, list)
    # Can be JSON serialized
    json.dumps(result_dict)
    json.dumps(finding_dicts)


def test_to_markdown_rendering():
    """Test that results can be rendered as Markdown."""
    plan = {
        "id": "plan-markdown",
        "tasks": [
            {
                "id": "task-md",
                "title": "Add WebSocket migration",
                "description": "Implement protocol upgrade",
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)
    markdown = result.to_markdown()

    assert isinstance(markdown, str)
    assert "# Task WebSocket Migration Readiness" in markdown
    assert "task-md" in markdown


def test_simple_namespace_plan():
    """Test that plans represented as SimpleNamespace objects work."""
    task = SimpleNamespace(
        id="task-ns",
        title="Add WebSocket migration",
        description="Implement WebSocket protocol upgrade with fallback strategy",
    )
    plan = SimpleNamespace(
        id="plan-ns",
        tasks=[task],
    )

    result = build_task_websocket_migration_readiness_plan(plan)

    assert len(result.findings) == 1
    assert result.findings[0].task_id == "task-ns"


def test_client_compatibility_matrix_detection():
    """Test detection of client compatibility matrix."""
    plan = {
        "id": "plan-compat-matrix",
        "tasks": [
            {
                "id": "task-matrix",
                "title": "Document WebSocket browser support",
                "description": (
                    "Create client compatibility matrix for WebSocket support. "
                    "List supported browsers, devices, and versions."
                ),
                "acceptance_criteria": [
                    "Client compatibility matrix documents browser support",
                    "Supported client versions are listed",
                ],
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)
    finding = result.findings[0]

    assert "client_compatibility_matrix" in finding.present_safeguards


def test_multiple_tasks_with_varying_readiness():
    """Test plan with multiple tasks having different readiness levels."""
    plan = {
        "id": "plan-multi",
        "tasks": [
            {
                "id": "task-strong",
                "title": "Full WebSocket migration",
                "description": (
                    "Protocol upgrade with fallback strategy. "
                    "State synchronization and connection lifecycle management."
                ),
                "acceptance_criteria": [
                    "Upgrade detection tests verify protocol switching",
                    "Fallback mechanism tests ensure graceful degradation",
                    "State sync tests validate reconciliation",
                    "Connection failure tests cover network drops",
                ],
            },
            {
                "id": "task-weak",
                "title": "Basic WebSocket upgrade",
                "description": "Add simple WebSocket upgrade without comprehensive testing",
                "acceptance_criteria": ["WebSocket connections work"],
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)

    assert len(result.findings) == 2
    strong_finding = next(f for f in result.findings if f.task_id == "task-strong")
    weak_finding = next(f for f in result.findings if f.task_id == "task-weak")

    assert strong_finding.readiness in {"strong", "partial"}
    assert weak_finding.readiness == "weak"
    assert len(strong_finding.missing_safeguards) < len(weak_finding.missing_safeguards)


def test_path_based_signal_detection():
    """Test that signals are detected from file paths."""
    plan = {
        "id": "plan-path",
        "tasks": [
            {
                "id": "task-path",
                "title": "Update files",
                "description": "Update configuration",
                "expected_files": [
                    "src/websocket/upgrade_handler.py",
                    "src/transport/fallback_polling.py",
                    "src/state/sync_manager.py",
                ],
            },
        ],
    }

    result = build_task_websocket_migration_readiness_plan(plan)
    finding = result.findings[0]

    assert "protocol_upgrade" in finding.detected_signals
    assert "fallback_strategy" in finding.detected_signals
    assert "state_synchronization" in finding.detected_signals
