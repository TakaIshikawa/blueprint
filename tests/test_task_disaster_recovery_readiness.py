"""Tests for disaster recovery readiness analyzer."""

import pytest

from blueprint.task_disaster_recovery_readiness import (
    DisasterRecoveryReadiness,
    analyze_disaster_recovery_readiness,
)


def test_empty_change_brief_returns_all_false():
    """Empty change brief should return all fields as False."""
    result = analyze_disaster_recovery_readiness({})

    assert isinstance(result, DisasterRecoveryReadiness)
    assert result.backup_strategy_defined is False
    assert result.rto_documented is False
    assert result.rpo_documented is False
    assert result.failover_tested is False
    assert result.data_replication_verified is False
    assert result.recovery_procedures_automated is False
    assert result.rollback_plan_exists is False


def test_backup_strategy_detected():
    """Detect backup strategy in change brief."""
    brief = {
        "title": "Implement backup strategy",
        "description": "Define backup strategy for customer data with daily snapshots",
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.backup_strategy_defined is True
    assert result.rto_documented is False
    assert result.rpo_documented is False


def test_rto_documented_detected():
    """Detect RTO documentation in change brief."""
    brief = {
        "description": "Set Recovery Time Objective (RTO) to 4 hours for critical services",
        "acceptance_criteria": ["RTO target documented", "Recovery time validated"],
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.rto_documented is True
    assert result.backup_strategy_defined is False


def test_rpo_documented_detected():
    """Detect RPO documentation in change brief."""
    brief = {
        "description": "Define Recovery Point Objective with acceptable data loss of 15 minutes",
        "acceptance_criteria": ["RPO documented in runbook"],
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.rpo_documented is True
    assert result.backup_strategy_defined is False


def test_failover_tested_detected():
    """Detect failover testing in change brief."""
    brief = {
        "title": "Conduct DR testing",
        "description": "Execute failover testing for production database",
        "acceptance_criteria": [
            "Failover drill completed successfully",
            "Test results documented",
        ],
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.failover_tested is True
    assert result.backup_strategy_defined is False


def test_data_replication_verified_detected():
    """Detect data replication configuration in change brief."""
    brief = {
        "description": "Configure cross-region replication for S3 buckets",
        "acceptance_criteria": ["Data replication verified", "Geo-replication enabled"],
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.data_replication_verified is True
    assert result.backup_strategy_defined is False


def test_recovery_procedures_automated_detected():
    """Detect automated recovery procedures in change brief."""
    brief = {
        "title": "Automate disaster recovery",
        "description": "Implement automated recovery procedures with runbook automation",
        "acceptance_criteria": ["Recovery automation tested", "Playbook documented"],
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.recovery_procedures_automated is True
    assert result.backup_strategy_defined is False


def test_rollback_plan_exists_detected():
    """Detect rollback plan in change brief."""
    brief = {
        "description": "Create rollback plan for database migration",
        "acceptance_criteria": [
            "Rollback strategy documented",
            "Contingency plan approved",
        ],
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.rollback_plan_exists is True
    assert result.backup_strategy_defined is False


def test_comprehensive_dr_readiness_all_detected():
    """Test comprehensive disaster recovery with all aspects present."""
    brief = {
        "title": "Complete disaster recovery implementation",
        "description": (
            "Implement comprehensive DR strategy with backup strategy for all services. "
            "Define RTO of 2 hours and RPO of 30 minutes. Configure data replication "
            "across regions with automated recovery procedures. Include rollback plan."
        ),
        "acceptance_criteria": [
            "Backup strategy defined and implemented",
            "Recovery Time Objective (RTO) documented",
            "Recovery Point Objective (RPO) documented",
            "Failover testing completed successfully",
            "Cross-region data replication verified",
            "Automated recovery procedures tested",
            "Rollback plan documented and approved",
        ],
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.backup_strategy_defined is True
    assert result.rto_documented is True
    assert result.rpo_documented is True
    assert result.failover_tested is True
    assert result.data_replication_verified is True
    assert result.recovery_procedures_automated is True
    assert result.rollback_plan_exists is True


def test_missing_rto_value():
    """Test when RTO is not mentioned at all."""
    brief = {
        "description": "Implement disaster recovery for production systems",
        "acceptance_criteria": ["DR plan created"],
    }

    result = analyze_disaster_recovery_readiness(brief)

    # Should not detect RTO since not mentioned
    assert result.rto_documented is False
    assert result.rpo_documented is False


def test_missing_rpo_value():
    """Test when RPO is not mentioned at all."""
    brief = {
        "description": "Set up database backups for production",
        "acceptance_criteria": ["Backups configured"],
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.rpo_documented is False
    assert result.backup_strategy_defined is False


def test_untested_failover():
    """Test when failover mechanism exists but not tested."""
    brief = {
        "title": "Configure failover",
        "description": "Set up failover configuration for database cluster",
        "acceptance_criteria": ["Failover configuration complete"],
    }

    result = analyze_disaster_recovery_readiness(brief)

    # Should not detect failover_tested since testing not mentioned
    assert result.failover_tested is False


def test_incomplete_backup_strategy():
    """Test incomplete backup strategy without full definition."""
    brief = {
        "description": "Perform daily backups",
        "acceptance_criteria": ["Backup job scheduled"],
    }

    result = analyze_disaster_recovery_readiness(brief)

    # Should not detect backup_strategy_defined since "strategy" not mentioned
    assert result.backup_strategy_defined is False


def test_invalid_change_brief_non_mapping():
    """Test with invalid input (non-mapping type)."""
    result = analyze_disaster_recovery_readiness("not a mapping")  # type: ignore

    assert isinstance(result, DisasterRecoveryReadiness)
    assert result.backup_strategy_defined is False
    assert result.rto_documented is False


def test_invalid_change_brief_none():
    """Test with None input."""
    result = analyze_disaster_recovery_readiness(None)  # type: ignore

    assert isinstance(result, DisasterRecoveryReadiness)
    assert result.backup_strategy_defined is False


def test_invalid_change_brief_list():
    """Test with list input instead of mapping."""
    result = analyze_disaster_recovery_readiness([{"key": "value"}])  # type: ignore

    assert isinstance(result, DisasterRecoveryReadiness)
    assert result.backup_strategy_defined is False


def test_change_brief_with_nested_acceptance_criteria():
    """Test extraction from nested acceptance criteria structure."""
    brief = {
        "title": "DR improvements",
        "acceptance_criteria": [
            "Define backup strategy for production databases",
            "Document RTO and RPO targets",
            "Execute failover drill",
        ],
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.backup_strategy_defined is True
    assert result.rto_documented is True
    assert result.rpo_documented is True
    assert result.failover_tested is True


def test_validation_commands_checked():
    """Test that validation commands are included in analysis."""
    brief = {
        "title": "Setup DR",
        "validation_command": "pytest tests/test_failover_testing.py tests/test_backup_strategy.py",
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.failover_tested is True
    assert result.backup_strategy_defined is True


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    brief = {
        "description": "BACKUP STRATEGY with RECOVERY TIME OBJECTIVE and ROLLBACK PLAN",
        "acceptance_criteria": ["FAILOVER TESTING completed"],
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.backup_strategy_defined is True
    assert result.rto_documented is True
    assert result.rollback_plan_exists is True
    assert result.failover_tested is True


def test_alternative_terminology_rto():
    """Test alternative RTO terminology is recognized."""
    brief = {
        "description": "System must recover within 1 hour (maximum outage tolerance)",
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.rto_documented is True


def test_alternative_terminology_rpo():
    """Test alternative RPO terminology is recognized."""
    brief = {
        "description": "Acceptable data loss limited to 5 minutes with point-in-time recovery",
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.rpo_documented is True


def test_alternative_terminology_replication():
    """Test alternative replication terminology is recognized."""
    brief = {
        "description": "Setup multi-region standby replicas for database",
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.data_replication_verified is True


def test_alternative_terminology_automation():
    """Test alternative automation terminology is recognized."""
    brief = {
        "description": "Create recovery playbook with self-healing capabilities",
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.recovery_procedures_automated is True


def test_alternative_terminology_rollback():
    """Test alternative rollback terminology is recognized."""
    brief = {
        "description": "Define back-out plan and contingency procedures",
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.rollback_plan_exists is True


def test_to_dict_method():
    """Test DisasterRecoveryReadiness.to_dict() serialization."""
    readiness = DisasterRecoveryReadiness(
        backup_strategy_defined=True,
        rto_documented=True,
        rpo_documented=False,
        failover_tested=True,
        data_replication_verified=False,
        recovery_procedures_automated=True,
        rollback_plan_exists=False,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["backup_strategy_defined"] is True
    assert result["rto_documented"] is True
    assert result["rpo_documented"] is False
    assert result["failover_tested"] is True
    assert result["data_replication_verified"] is False
    assert result["recovery_procedures_automated"] is True
    assert result["rollback_plan_exists"] is False


def test_multiple_fields_in_different_sections():
    """Test detection across multiple brief sections."""
    brief = {
        "title": "DR setup",
        "description": "Define backup strategy",
        "acceptance_criteria": ["RTO documented"],
        "requirements": ["RPO defined"],
        "notes": ["Failover testing required"],
        "risks": ["No rollback plan"],
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.backup_strategy_defined is True
    assert result.rto_documented is True
    assert result.rpo_documented is True
    assert result.failover_tested is True
    assert result.rollback_plan_exists is True


def test_validation_commands_as_list():
    """Test validation_commands as list."""
    brief = {
        "validation_commands": [
            "test_disaster_recovery_drill.py",
            "verify_data_replication.py",
        ],
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.failover_tested is True
    assert result.data_replication_verified is True


def test_no_false_positives_similar_words():
    """Test that similar but different words don't trigger false positives."""
    brief = {
        "description": "Backup files to external drive. Restore from archive.",
    }

    result = analyze_disaster_recovery_readiness(brief)

    # "backup" and "restore" alone shouldn't trigger "backup strategy"
    assert result.backup_strategy_defined is False
    # Should not detect RTO/RPO
    assert result.rto_documented is False
    assert result.rpo_documented is False


def test_dataclass_immutability():
    """Test that DisasterRecoveryReadiness is frozen/immutable."""
    readiness = DisasterRecoveryReadiness(backup_strategy_defined=True)

    with pytest.raises(AttributeError):
        readiness.backup_strategy_defined = False  # type: ignore


def test_dr_drill_terminology():
    """Test DR drill and test specific terminology."""
    brief = {
        "description": "Conduct disaster recovery test for production systems",
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.failover_tested is True


def test_snapshot_strategy_terminology():
    """Test snapshot strategy as backup strategy."""
    brief = {
        "description": "Define snapshot strategy for all databases",
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.backup_strategy_defined is True


def test_pitr_terminology():
    """Test point-in-time recovery terminology for RPO."""
    brief = {
        "description": "Enable PITR for transaction logs",
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.rpo_documented is True


def test_geo_replication_terminology():
    """Test geo-replication as data replication."""
    brief = {
        "description": "Configure geo-replication for global availability",
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.data_replication_verified is True


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    brief = {
        "acceptance_criteria": "Define backup strategy and document RTO",
    }

    result = analyze_disaster_recovery_readiness(brief)

    assert result.backup_strategy_defined is True
    assert result.rto_documented is True
