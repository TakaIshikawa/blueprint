"""Tests for chaos engineering readiness analyzer."""

import pytest

from blueprint.task_chaos_engineering_readiness import (
    ChaosEngineeringReadiness,
    analyze_chaos_engineering_readiness,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_chaos_engineering_readiness({})

    assert isinstance(result, ChaosEngineeringReadiness)
    assert result.failure_injection_planned is False
    assert result.steady_state_defined is False
    assert result.blast_radius_controlled is False
    assert result.experiment_designed is False
    assert result.safety_controls_implemented is False
    assert result.observability_coverage is False
    assert result.rollback_mechanism_ready is False
    assert result.team_prepared is False
    assert result.readiness_score == 0.0


def test_failure_injection_detected():
    """Detect failure injection planning in task data."""
    task = {
        "title": "Implement chaos experiment",
        "description": "Setup failure injection for network partition testing",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True
    assert result.steady_state_defined is False
    assert result.readiness_score == 0.125


def test_steady_state_detected():
    """Detect steady-state definition in task data."""
    task = {
        "description": "Define steady-state metrics and baseline behavior for the system",
        "acceptance_criteria": ["Steady-state hypothesis documented", "Normal operation defined"],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.steady_state_defined is True
    assert result.failure_injection_planned is False


def test_blast_radius_detected():
    """Detect blast radius controls in task data."""
    task = {
        "description": "Implement blast radius controls with circuit breaker and rollback plan",
        "acceptance_criteria": ["Impact scope limited", "Canary deployment configured"],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.blast_radius_controlled is True
    assert result.failure_injection_planned is False


def test_experiment_design_detected():
    """Detect experiment design in task data."""
    task = {
        "title": "Design chaos experiment",
        "description": "Create hypothesis-driven experiment plan with controlled failure scenarios",
        "acceptance_criteria": ["Experiment design documented", "Test protocol defined"],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.experiment_designed is True
    # Note: "chaos experiment" in title also triggers failure_injection_planned
    assert result.failure_injection_planned is True


def test_safety_controls_detected():
    """Detect safety controls in task data."""
    task = {
        "description": "Implement production safety controls with kill switch and automatic rollback",
        "acceptance_criteria": [
            "Emergency stop configured",
            "Fail-safe mechanisms verified",
        ],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.safety_controls_implemented is True


def test_observability_detected():
    """Detect observability coverage in task data."""
    task = {
        "description": "Setup monitoring with metrics, logging, and alerting for chaos experiments",
        "acceptance_criteria": [
            "Dashboard created",
            "SLI/SLO defined",
        ],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.observability_coverage is True
    assert result.failure_injection_planned is False


def test_rollback_mechanism_detected():
    """Detect rollback mechanism in task data."""
    task = {
        "description": "Implement automatic rollback procedure with failover capability",
        "acceptance_criteria": ["Rollback mechanism tested", "Recovery time objective met"],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.rollback_mechanism_ready is True
    assert result.failure_injection_planned is False


def test_team_preparedness_detected():
    """Detect team preparedness in task data."""
    task = {
        "description": "Conduct chaos game day with team training and incident response runbooks",
        "acceptance_criteria": ["Team trained on procedures", "Playbooks documented"],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.team_prepared is True
    assert result.failure_injection_planned is False


def test_comprehensive_chaos_engineering_all_detected():
    """Test comprehensive chaos engineering implementation with all aspects present."""
    task = {
        "title": "Complete chaos engineering implementation",
        "description": (
            "Implement comprehensive chaos engineering with failure injection for network failures and latency. "
            "Define steady-state metrics and baseline behavior with success criteria. "
            "Control blast radius with circuit breaker and canary deployment. "
            "Design hypothesis-driven experiments with controlled failure scenarios. "
            "Implement production safety controls with kill switch and automatic rollback. "
            "Setup observability with monitoring, logging, alerting, and dashboards. "
            "Implement rollback mechanism with automatic recovery and failover. "
            "Conduct team training with game day exercises and incident response runbooks."
        ),
        "acceptance_criteria": [
            "Failure injection implemented",
            "Steady-state defined",
            "Blast radius controlled",
            "Experiment design documented",
            "Safety controls verified",
            "Observability configured",
            "Rollback mechanism tested",
            "Team prepared with training",
        ],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True
    assert result.steady_state_defined is True
    assert result.blast_radius_controlled is True
    assert result.experiment_designed is True
    assert result.safety_controls_implemented is True
    assert result.observability_coverage is True
    assert result.rollback_mechanism_ready is True
    assert result.team_prepared is True
    assert result.readiness_score == 1.0


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_chaos_engineering_readiness(None)  # type: ignore

    assert isinstance(result, ChaosEngineeringReadiness)
    assert result.failure_injection_planned is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_chaos_engineering_readiness([{"key": "value"}])  # type: ignore

    assert isinstance(result, ChaosEngineeringReadiness)
    assert result.failure_injection_planned is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_string():
    """Test with string input instead of mapping."""
    result = analyze_chaos_engineering_readiness("not a mapping")  # type: ignore

    assert isinstance(result, ChaosEngineeringReadiness)
    assert result.failure_injection_planned is False


def test_invalid_task_data_tuple():
    """Test with tuple input instead of mapping."""
    result = analyze_chaos_engineering_readiness(("tuple", "data"))  # type: ignore

    assert isinstance(result, ChaosEngineeringReadiness)
    assert result.failure_injection_planned is False


def test_partial_data_missing_fields():
    """Test with partial task data missing some fields."""
    task = {
        "title": "System setup",
        # Missing description, acceptance_criteria, etc.
    }

    result = analyze_chaos_engineering_readiness(task)

    assert isinstance(result, ChaosEngineeringReadiness)
    assert result.readiness_score == 0.0


def test_partial_chaos_engineering_readiness():
    """Test partial chaos engineering readiness with some aspects covered."""
    task = {
        "title": "Basic setup",
        "description": "Setup basic infrastructure",
        "acceptance_criteria": [
            "Failure injection configured",
            "Steady-state metrics defined",
        ],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True
    assert result.steady_state_defined is True
    assert result.blast_radius_controlled is False
    assert result.experiment_designed is False
    assert result.safety_controls_implemented is False
    # Note: "metrics" in acceptance_criteria triggers observability_coverage
    assert result.observability_coverage is True
    assert result.rollback_mechanism_ready is False
    assert result.team_prepared is False
    # 3/8 = 0.375
    assert result.readiness_score == 0.375


def test_task_data_with_nested_acceptance_criteria():
    """Test extraction from nested acceptance criteria structure."""
    task = {
        "title": "Chaos engineering improvements",
        "acceptance_criteria": [
            "Inject failure scenarios for testing",
            "Define baseline behavior for system health",
            "Control blast radius with canary deployment",
        ],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True
    assert result.steady_state_defined is True
    assert result.blast_radius_controlled is True


def test_validation_commands_checked():
    """Test that validation commands are included in analysis."""
    task = {
        "title": "Basic setup",
        "validation_command": "pytest tests/test_failure_injection.py tests/test_steady_state.py",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True
    assert result.steady_state_defined is True


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "FAILURE INJECTION with STEADY STATE and OBSERVABILITY",
        "acceptance_criteria": ["BLAST RADIUS controlled", "SAFETY CONTROL implemented"],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True
    assert result.steady_state_defined is True
    assert result.observability_coverage is True
    assert result.blast_radius_controlled is True
    assert result.safety_controls_implemented is True


def test_alternative_terminology_failure_fault_injection():
    """Test fault injection terminology is recognized."""
    task = {
        "description": "Implement fault injection for network partition testing",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True


def test_alternative_terminology_failure_latency_injection():
    """Test latency injection terminology is recognized."""
    task = {
        "description": "Inject latency to simulate network degradation",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True


def test_alternative_terminology_failure_service_outage():
    """Test service outage terminology is recognized."""
    task = {
        "description": "Simulate service outage for resilience testing",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True


def test_alternative_terminology_failure_resource_exhaustion():
    """Test resource exhaustion terminology is recognized."""
    task = {
        "description": "Test system behavior under memory pressure and CPU spike",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True


def test_alternative_terminology_steady_state_baseline():
    """Test baseline metric terminology is recognized."""
    task = {
        "description": "Establish baseline metrics for normal operation",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.steady_state_defined is True


def test_alternative_terminology_steady_state_hypothesis():
    """Test steady-state hypothesis terminology is recognized."""
    task = {
        "description": "Define steady-state hypothesis for system health validation",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.steady_state_defined is True


def test_alternative_terminology_blast_radius_impact_scope():
    """Test impact scope terminology is recognized."""
    task = {
        "description": "Limit impact scope of chaos experiments",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.blast_radius_controlled is True


def test_alternative_terminology_blast_radius_rollback():
    """Test rollback plan as blast radius control is recognized."""
    task = {
        "description": "Prepare rollback strategy to contain failure",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.blast_radius_controlled is True


def test_alternative_terminology_blast_radius_circuit_breaker():
    """Test circuit breaker terminology is recognized."""
    task = {
        "description": "Implement circuit breaker pattern for failure isolation",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.blast_radius_controlled is True


def test_alternative_terminology_experiment_hypothesis_driven():
    """Test hypothesis-driven experiment terminology is recognized."""
    task = {
        "description": "Design hypothesis-driven chaos testing methodology",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.experiment_designed is True


def test_alternative_terminology_experiment_controlled():
    """Test controlled experiment terminology is recognized."""
    task = {
        "description": "Execute controlled chaos experiment in production",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.experiment_designed is True


def test_alternative_terminology_safety_kill_switch():
    """Test kill switch terminology is recognized."""
    task = {
        "description": "Implement emergency kill switch for chaos experiments",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.safety_controls_implemented is True


def test_alternative_terminology_safety_fail_safe():
    """Test fail-safe terminology is recognized."""
    task = {
        "description": "Ensure fail-safe mechanisms for production protection",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.safety_controls_implemented is True


def test_alternative_terminology_observability_monitoring():
    """Test monitoring terminology is recognized."""
    task = {
        "description": "Setup monitoring and metrics for chaos experiments",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.observability_coverage is True


def test_alternative_terminology_observability_telemetry():
    """Test telemetry terminology is recognized."""
    task = {
        "description": "Implement telemetry and instrumentation for observability",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.observability_coverage is True


def test_alternative_terminology_observability_slo():
    """Test SLO/SLI terminology is recognized."""
    task = {
        "description": "Define SLI and SLO for service level monitoring",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.observability_coverage is True


def test_alternative_terminology_rollback_automatic_recovery():
    """Test automatic recovery terminology is recognized."""
    task = {
        "description": "Implement automatic recovery for failed experiments",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.rollback_mechanism_ready is True


def test_alternative_terminology_rollback_failover():
    """Test failover terminology is recognized."""
    task = {
        "description": "Configure failover and failback procedures",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.rollback_mechanism_ready is True


def test_alternative_terminology_rollback_rto():
    """Test RTO/RPO terminology is recognized."""
    task = {
        "description": "Define RTO and RPO for recovery procedures",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.rollback_mechanism_ready is True


def test_alternative_terminology_team_runbook():
    """Test runbook terminology is recognized."""
    task = {
        "description": "Create runbooks and playbooks for incident response",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.team_prepared is True


def test_alternative_terminology_team_game_day():
    """Test game day terminology is recognized."""
    task = {
        "description": "Conduct chaos game day with team training",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.team_prepared is True


def test_alternative_terminology_team_fire_drill():
    """Test fire drill terminology is recognized."""
    task = {
        "description": "Run disaster recovery fire drill with on-call team",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.team_prepared is True


def test_to_dict_method():
    """Test ChaosEngineeringReadiness.to_dict() serialization."""
    readiness = ChaosEngineeringReadiness(
        failure_injection_planned=True,
        steady_state_defined=True,
        blast_radius_controlled=False,
        experiment_designed=True,
        safety_controls_implemented=False,
        observability_coverage=True,
        rollback_mechanism_ready=False,
        team_prepared=True,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["failure_injection_planned"] is True
    assert result["steady_state_defined"] is True
    assert result["blast_radius_controlled"] is False
    assert result["experiment_designed"] is True
    assert result["safety_controls_implemented"] is False
    assert result["observability_coverage"] is True
    assert result["rollback_mechanism_ready"] is False
    assert result["team_prepared"] is True
    assert result["readiness_score"] == 0.625


def test_multiple_fields_in_different_sections():
    """Test detection across multiple task data sections."""
    task = {
        "title": "Chaos engineering setup",
        "description": "Implement failure injection",
        "acceptance_criteria": ["Steady-state defined"],
        "requirements": ["Blast radius controlled"],
        "notes": ["Observability needed"],
        "risks": ["No safety controls"],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True
    assert result.steady_state_defined is True
    assert result.blast_radius_controlled is True
    assert result.observability_coverage is True
    assert result.safety_controls_implemented is True


def test_validation_commands_as_list():
    """Test validation_commands as list."""
    task = {
        "validation_commands": [
            "test_failure_injection.py",
            "test_steady_state.py",
        ],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True
    assert result.steady_state_defined is True


def test_dataclass_immutability():
    """Test that ChaosEngineeringReadiness is frozen/immutable."""
    readiness = ChaosEngineeringReadiness(failure_injection_planned=True)

    with pytest.raises(AttributeError):
        readiness.failure_injection_planned = False  # type: ignore


def test_network_partition_edge_case():
    """Test network partition as failure injection."""
    task = {
        "description": "Test resilience under network partition scenarios",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True


def test_cascading_failures_edge_case():
    """Test cascading failure detection."""
    task = {
        "description": "Simulate cascading failures with service shutdown",
        "acceptance_criteria": [
            "Blast radius limited to prevent cascading impact",
        ],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True
    assert result.blast_radius_controlled is True


def test_distributed_system_edge_case():
    """Test distributed system chaos engineering."""
    task = {
        "description": "Test distributed system resilience with network delay injection",
        "acceptance_criteria": [
            "Monitor distributed system health",
            "Define steady-state for microservices",
        ],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True
    assert result.observability_coverage is True
    assert result.steady_state_defined is True


def test_stateful_services_edge_case():
    """Test stateful service chaos engineering."""
    task = {
        "description": "Test stateful service with disk failure simulation and rollback mechanism",
        "acceptance_criteria": [
            "Restore state after experiment",
            "Verify data integrity",
        ],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True
    assert result.rollback_mechanism_ready is True


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    task = {
        "acceptance_criteria": "Implement failure injection and define steady-state",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True
    assert result.steady_state_defined is True


def test_readiness_score_calculation():
    """Test readiness score calculation with different combinations."""
    # 0/8 = 0.0
    task1 = {"description": "Generic task"}
    result1 = analyze_chaos_engineering_readiness(task1)
    assert result1.readiness_score == 0.0

    # 1/8 = 0.125
    task2 = {"description": "Implement failure injection"}
    result2 = analyze_chaos_engineering_readiness(task2)
    assert result2.readiness_score == 0.125

    # 4/8 = 0.5
    task3 = {
        "description": "Fault injection with baseline metrics and rollback strategy using bulkhead pattern"
    }
    result3 = analyze_chaos_engineering_readiness(task3)
    assert result3.readiness_score == 0.5

    # 8/8 = 1.0
    task4 = {
        "description": (
            "Failure injection, steady-state, blast radius control, experiment design, "
            "safety control, observability, rollback mechanism, and team training"
        )
    }
    result4 = analyze_chaos_engineering_readiness(task4)
    assert result4.readiness_score == 1.0


def test_empty_string_fields():
    """Test handling of empty string fields."""
    task = {
        "title": "",
        "description": "",
        "acceptance_criteria": [""],
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is False
    assert result.readiness_score == 0.0


def test_health_check_observability():
    """Test health check as observability coverage."""
    task = {
        "description": "Implement health checks and liveness probes for monitoring",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.observability_coverage is True


def test_dashboard_observability():
    """Test dashboard as observability coverage."""
    task = {
        "description": "Create dashboard for chaos experiment visualization",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.observability_coverage is True


def test_alerting_observability():
    """Test alerting as observability coverage."""
    task = {
        "description": "Configure alerting and notifications for anomaly detection",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.observability_coverage is True


def test_canary_deployment_blast_radius():
    """Test canary deployment as blast radius control."""
    task = {
        "description": "Use canary deployment to limit impact of changes",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.blast_radius_controlled is True


def test_incremental_rollout_blast_radius():
    """Test incremental rollout as blast radius control."""
    task = {
        "description": "Implement incremental rollout strategy for controlled experiments",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.blast_radius_controlled is True


def test_bulkhead_pattern_blast_radius():
    """Test bulkhead pattern as blast radius control."""
    task = {
        "description": "Apply bulkhead pattern for failure isolation",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.blast_radius_controlled is True


def test_abort_mechanism_safety():
    """Test abort mechanism as safety control."""
    task = {
        "description": "Implement emergency abort procedure for experiments",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.safety_controls_implemented is True


def test_guardrail_safety():
    """Test guardrail as safety control."""
    task = {
        "description": "Configure guardrails and safety constraints",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.safety_controls_implemented is True


def test_pre_production_testing_safety():
    """Test pre-production testing as safety control."""
    task = {
        "description": "Validate chaos experiments in staging environment first",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.safety_controls_implemented is True


def test_incident_response_team_preparedness():
    """Test incident response as team preparedness."""
    task = {
        "description": "Train team on incident response and escalation procedures",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.team_prepared is True


def test_post_mortem_team_preparedness():
    """Test post-mortem as team preparedness."""
    task = {
        "description": "Conduct post-mortem and retrospective after chaos experiments",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.team_prepared is True


def test_on_call_rotation_team_preparedness():
    """Test on-call rotation as team preparedness."""
    task = {
        "description": "Ensure on-call team is trained for chaos incidents",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.team_prepared is True


def test_manual_rollback_mechanism():
    """Test manual rollback as rollback mechanism."""
    task = {
        "description": "Document manual rollback procedure for experiments",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.rollback_mechanism_ready is True


def test_restore_service_rollback():
    """Test service restoration as rollback mechanism."""
    task = {
        "description": "Implement procedure to restore service to baseline state",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.rollback_mechanism_ready is True


def test_experiment_procedure_design():
    """Test experiment procedure as design aspect."""
    task = {
        "description": "Define test procedure for chaos experiments",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.experiment_designed is True


def test_experiment_methodology_design():
    """Test experiment methodology as design aspect."""
    task = {
        "description": "Establish chaos testing methodology and protocol",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.experiment_designed is True


def test_scientific_method_experiment():
    """Test scientific method as experiment design."""
    task = {
        "description": "Apply scientific method to chaos experiments",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.experiment_designed is True


def test_expected_behavior_steady_state():
    """Test expected behavior as steady-state."""
    task = {
        "description": "Define expected behavior and success criteria",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.steady_state_defined is True


def test_system_health_steady_state():
    """Test system health as steady-state."""
    task = {
        "description": "Monitor system health to validate healthy state",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.steady_state_defined is True


def test_error_injection_failure():
    """Test error injection as failure injection."""
    task = {
        "description": "Inject errors to test system resilience",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True


def test_simulate_degradation_failure():
    """Test simulate degradation as failure injection."""
    task = {
        "description": "Simulate system degradation with chaos scenarios",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True


def test_cpu_throttle_failure():
    """Test CPU throttle as failure injection."""
    task = {
        "description": "Test system under CPU throttle and resource contention",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True


def test_disk_full_failure():
    """Test disk full as failure injection."""
    task = {
        "description": "Simulate disk full scenario for resilience testing",
    }

    result = analyze_chaos_engineering_readiness(task)

    assert result.failure_injection_planned is True
