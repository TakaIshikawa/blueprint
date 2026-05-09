"""Tests for workflow requirements extractor."""

import pytest

from blueprint.source_workflow_requirements import (
    WorkflowRequirements,
    extract_workflow_requirements,
)


def test_empty_source_data_returns_all_false():
    """Empty source data should return all fields as False."""
    result = extract_workflow_requirements({})

    assert isinstance(result, WorkflowRequirements)
    assert result.state_definitions_specified is False
    assert result.transition_rules_defined is False
    assert result.workflow_triggers_identified is False
    assert result.workflow_conditions_specified is False
    assert result.workflow_actors_defined is False
    assert result.state_persistence_addressed is False
    assert result.concurrent_transitions_handled is False
    assert result.rollback_scenarios_planned is False
    assert result.workflow_versioning_considered is False
    assert result.workflow_audit_included is False
    assert result.completeness_score == 0.0


def test_state_definitions_detected():
    """Detect state definitions in source data."""
    source = {
        "title": "Order workflow",
        "description": "Define workflow states: draft, pending, approved, rejected, completed",
    }

    result = extract_workflow_requirements(source)

    assert result.state_definitions_specified is True
    assert result.completeness_score == 0.1


def test_transition_rules_detected():
    """Detect transition rules in source data."""
    source = {
        "description": "Define transition rules for state changes and valid transitions",
        "requirements": ["Transition from draft to pending", "Allowed transitions documented"],
    }

    result = extract_workflow_requirements(source)

    assert result.transition_rules_defined is True


def test_workflow_triggers_detected():
    """Detect workflow triggers in source data."""
    source = {
        "description": "Configure workflow triggers on user actions",
        "requirements": ["Event-driven transitions", "Trigger on approval"],
    }

    result = extract_workflow_requirements(source)

    assert result.workflow_triggers_identified is True


def test_workflow_conditions_detected():
    """Detect workflow conditions in source data."""
    source = {
        "description": "Add transition guards and conditional workflow logic",
        "requirements": ["Guard clauses for transitions", "Validation rules before state change"],
    }

    result = extract_workflow_requirements(source)

    assert result.workflow_conditions_specified is True


def test_workflow_actors_detected():
    """Detect workflow actors and roles in source data."""
    source = {
        "description": "Define workflow roles and approval chain",
        "requirements": ["Role-based workflow permissions", "Who can approve transitions"],
    }

    result = extract_workflow_requirements(source)

    assert result.workflow_actors_defined is True


def test_state_persistence_detected():
    """Detect state persistence requirements in source data."""
    source = {
        "description": "Persist workflow state and track state history",
        "requirements": ["State persistence configured", "Workflow audit trail maintained"],
    }

    result = extract_workflow_requirements(source)

    assert result.state_persistence_addressed is True


def test_concurrent_transitions_detected():
    """Detect concurrent transition handling in source data."""
    source = {
        "description": "Handle concurrent transitions with optimistic locking",
        "requirements": ["Race condition handling", "Conflict resolution for parallel transitions"],
    }

    result = extract_workflow_requirements(source)

    assert result.concurrent_transitions_handled is True


def test_rollback_scenarios_detected():
    """Detect rollback scenarios in source data."""
    source = {
        "description": "Implement rollback logic and compensation for failed transitions",
        "requirements": ["Rollback state changes", "Compensating actions defined"],
    }

    result = extract_workflow_requirements(source)

    assert result.rollback_scenarios_planned is True


def test_workflow_versioning_detected():
    """Detect workflow versioning requirements in source data."""
    source = {
        "description": "Support workflow versioning and migration",
        "requirements": ["Workflow schema evolution", "Backward compatible state machine"],
    }

    result = extract_workflow_requirements(source)

    assert result.workflow_versioning_considered is True


def test_workflow_audit_detected():
    """Detect workflow audit requirements in source data."""
    source = {
        "description": "Implement workflow monitoring and audit trail",
        "requirements": ["Track workflow events", "State change audit log"],
    }

    result = extract_workflow_requirements(source)

    assert result.workflow_audit_included is True


def test_comprehensive_workflow_all_detected():
    """Test comprehensive workflow with all aspects present."""
    source = {
        "title": "Complete order approval workflow",
        "description": (
            "Define workflow states (draft, pending, approved, rejected). "
            "Configure transition rules with guards and validation. "
            "Set up event triggers and role-based workflow actors. "
            "Persist workflow state with audit trail. "
            "Handle concurrent transitions with optimistic locking. "
            "Implement rollback and compensation logic. "
            "Support workflow versioning and migration. "
            "Track all workflow events for monitoring."
        ),
        "requirements": [
            "State definitions specified",
            "Valid transitions defined",
            "Triggers configured",
            "Conditional transitions with guards",
            "Workflow roles and approval chain",
            "State persistence and history",
            "Race condition handling",
            "Compensation for failures",
            "Workflow schema evolution",
            "Audit trail maintained",
        ],
    }

    result = extract_workflow_requirements(source)

    assert result.state_definitions_specified is True
    assert result.transition_rules_defined is True
    assert result.workflow_triggers_identified is True
    assert result.workflow_conditions_specified is True
    assert result.workflow_actors_defined is True
    assert result.state_persistence_addressed is True
    assert result.concurrent_transitions_handled is True
    assert result.rollback_scenarios_planned is True
    assert result.workflow_versioning_considered is True
    assert result.workflow_audit_included is True
    assert result.completeness_score == 1.0


def test_invalid_source_data_none():
    """Test with None input."""
    result = extract_workflow_requirements(None)  # type: ignore

    assert isinstance(result, WorkflowRequirements)
    assert result.completeness_score == 0.0


def test_invalid_source_data_list():
    """Test with list input instead of mapping."""
    result = extract_workflow_requirements([{"key": "value"}])  # type: ignore

    assert isinstance(result, WorkflowRequirements)
    assert result.completeness_score == 0.0


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    source = {
        "description": "STATE DEFINITIONS with TRANSITION RULES and WORKFLOW TRIGGERS",
        "requirements": ["GUARDS configured", "ROLLBACK planned"],
    }

    result = extract_workflow_requirements(source)

    assert result.state_definitions_specified is True
    assert result.transition_rules_defined is True
    assert result.workflow_triggers_identified is True
    assert result.workflow_conditions_specified is True
    assert result.rollback_scenarios_planned is True


def test_fsm_terminology():
    """Test finite state machine terminology is recognized."""
    source = {
        "description": "Implement FSM with state machine model",
    }

    result = extract_workflow_requirements(source)

    assert result.state_definitions_specified is True


def test_status_enum_terminology():
    """Test status enum terminology is recognized."""
    source = {
        "description": "Define status values for workflow states",
    }

    result = extract_workflow_requirements(source)

    assert result.state_definitions_specified is True


def test_event_driven_workflow():
    """Test event-driven workflow terminology."""
    source = {
        "description": "Event-driven workflow with event handlers",
    }

    result = extract_workflow_requirements(source)

    assert result.workflow_triggers_identified is True


def test_approval_chain():
    """Test approval chain as workflow actors."""
    source = {
        "description": "Configure approval hierarchy for workflow",
    }

    result = extract_workflow_requirements(source)

    assert result.workflow_actors_defined is True


def test_state_tracking():
    """Test state tracking as persistence."""
    source = {
        "description": "Track workflow state changes with changelog",
    }

    result = extract_workflow_requirements(source)

    assert result.state_persistence_addressed is True


def test_pessimistic_locking():
    """Test pessimistic locking for concurrent transitions."""
    source = {
        "description": "Use pessimistic locking to prevent race conditions",
    }

    result = extract_workflow_requirements(source)

    assert result.concurrent_transitions_handled is True


def test_compensation_logic():
    """Test compensation terminology for rollback."""
    source = {
        "description": "Implement compensating transactions for error recovery",
    }

    result = extract_workflow_requirements(source)

    assert result.rollback_scenarios_planned is True


def test_state_machine_migration():
    """Test state machine migration as versioning."""
    source = {
        "description": "Migrate workflow schema with backward compatibility",
    }

    result = extract_workflow_requirements(source)

    assert result.workflow_versioning_considered is True


def test_transition_log():
    """Test transition log as audit."""
    source = {
        "description": "Log all transition history for audit purposes",
    }

    result = extract_workflow_requirements(source)

    assert result.workflow_audit_included is True


def test_to_dict_method():
    """Test WorkflowRequirements.to_dict() serialization."""
    requirements = WorkflowRequirements(
        state_definitions_specified=True,
        transition_rules_defined=True,
        workflow_triggers_identified=False,
        workflow_conditions_specified=True,
        workflow_actors_defined=False,
        state_persistence_addressed=True,
        concurrent_transitions_handled=False,
        rollback_scenarios_planned=True,
        workflow_versioning_considered=False,
        workflow_audit_included=True,
    )

    result = requirements.to_dict()

    assert isinstance(result, dict)
    assert result["state_definitions_specified"] is True
    assert result["transition_rules_defined"] is True
    assert result["workflow_triggers_identified"] is False
    assert result["workflow_conditions_specified"] is True
    assert result["workflow_actors_defined"] is False
    assert result["state_persistence_addressed"] is True
    assert result["concurrent_transitions_handled"] is False
    assert result["rollback_scenarios_planned"] is True
    assert result["workflow_versioning_considered"] is False
    assert result["workflow_audit_included"] is True
    assert result["completeness_score"] == 0.6


def test_dataclass_immutability():
    """Test that WorkflowRequirements is frozen/immutable."""
    requirements = WorkflowRequirements(state_definitions_specified=True)

    with pytest.raises(AttributeError):
        requirements.state_definitions_specified = False  # type: ignore


def test_multiple_fields_in_different_sections():
    """Test detection across multiple source data sections."""
    source = {
        "title": "Workflow setup",
        "description": "State definitions specified",
        "requirements": ["Transition rules defined"],
        "acceptance_criteria": ["Workflow triggers configured"],
        "notes": ["Conditions needed"],
        "definition_of_done": ["Audit trail implemented"],
    }

    result = extract_workflow_requirements(source)

    assert result.state_definitions_specified is True
    assert result.transition_rules_defined is True
    assert result.workflow_triggers_identified is True
    assert result.workflow_conditions_specified is True
    assert result.workflow_audit_included is True


def test_nested_workflows():
    """Test nested workflow terminology."""
    source = {
        "description": "Support nested sub-workflows with parent-child state relationships",
    }

    result = extract_workflow_requirements(source)

    # Nested workflows should match state definitions
    assert result.state_definitions_specified is True


def test_parallel_branches():
    """Test parallel workflow branches."""
    source = {
        "description": "Support parallel branches in workflow with concurrent transitions",
    }

    result = extract_workflow_requirements(source)

    assert result.concurrent_transitions_handled is True


def test_guard_expression():
    """Test guard expression terminology."""
    source = {
        "description": "Evaluate guard expressions before allowing transitions",
    }

    result = extract_workflow_requirements(source)

    assert result.workflow_conditions_specified is True


def test_partial_workflow_completeness():
    """Test partial workflow with some aspects covered."""
    source = {
        "description": "Basic workflow implementation",
        "requirements": ["Draft state configured", "Transition from draft to pending allowed"],
    }

    result = extract_workflow_requirements(source)

    assert result.state_definitions_specified is True
    assert result.transition_rules_defined is True
    assert result.workflow_triggers_identified is False
    assert result.completeness_score == 0.2
