"""Tests for data migration readiness analyzer."""

import pytest

from blueprint.task_api_data_migration_readiness import (
    DataMigrationReadiness,
    analyze_data_migration_readiness,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_data_migration_readiness({})

    assert isinstance(result, DataMigrationReadiness)
    assert result.migration_script_quality_assessed is False
    assert result.schema_version_tracking_configured is False
    assert result.transformation_logic_verified is False
    assert result.rollback_procedure_tested is False
    assert result.zero_downtime_implemented is False
    assert result.post_migration_validation_configured is False
    assert result.progress_tracking_configured is False
    assert result.migration_testing_coverage_adequate is False
    assert result.readiness_score == 0.0


def test_migration_script_quality_detected():
    """Detect migration script quality assessment in task data."""
    task = {
        "title": "Review migration script quality",
        "description": "Ensure migration script quality is assessed before deployment",
    }

    result = analyze_data_migration_readiness(task)

    assert result.migration_script_quality_assessed is True
    assert result.schema_version_tracking_configured is False
    assert result.readiness_score == 0.125


def test_schema_version_tracking_detected():
    """Detect schema version tracking configuration in task data."""
    task = {
        "description": "Set up schema version tracking for all database changes",
        "acceptance_criteria": ["Version tracking enabled", "Schema registry configured"],
    }

    result = analyze_data_migration_readiness(task)

    assert result.schema_version_tracking_configured is True
    assert result.migration_script_quality_assessed is False


def test_transformation_logic_detected():
    """Detect transformation logic verification in task data."""
    task = {
        "description": "Verify transformation logic for all data mapping operations",
        "acceptance_criteria": ["Data mapping verified", "Field mapping tested"],
    }

    result = analyze_data_migration_readiness(task)

    assert result.transformation_logic_verified is True
    assert result.migration_script_quality_assessed is False


def test_rollback_procedure_detected():
    """Detect rollback procedure testing in task data."""
    task = {
        "title": "Validate rollback procedures",
        "description": "Ensure rollback tested for all migration steps",
        "acceptance_criteria": ["Rollback procedure verified"],
    }

    result = analyze_data_migration_readiness(task)

    assert result.rollback_procedure_tested is True
    assert result.migration_script_quality_assessed is False


def test_zero_downtime_detected():
    """Detect zero downtime implementation in task data."""
    task = {
        "description": "Ensure zero downtime implemented for the production migration",
        "acceptance_criteria": [
            "Online migration configured",
        ],
    }

    result = analyze_data_migration_readiness(task)

    assert result.zero_downtime_implemented is True


def test_post_migration_validation_detected():
    """Detect post-migration validation configuration in task data."""
    task = {
        "description": "Configure post migration validation for data integrity checks",
        "acceptance_criteria": [
            "Migration validation automated",
            "Data integrity verified",
        ],
    }

    result = analyze_data_migration_readiness(task)

    assert result.post_migration_validation_configured is True
    assert result.migration_script_quality_assessed is False


def test_progress_tracking_detected():
    """Detect progress tracking configuration in task data."""
    task = {
        "description": "Set up progress tracking configured for migration monitoring",
        "acceptance_criteria": ["Migration dashboard configured", "Status reporting enabled"],
    }

    result = analyze_data_migration_readiness(task)

    assert result.progress_tracking_configured is True
    assert result.migration_script_quality_assessed is False


def test_migration_testing_coverage_detected():
    """Detect migration testing coverage assessment in task data."""
    task = {
        "description": "Ensure migration test coverage meets the 90% target",
        "acceptance_criteria": ["Migration integration tests passing"],
    }

    result = analyze_data_migration_readiness(task)

    assert result.migration_testing_coverage_adequate is True
    assert result.migration_script_quality_assessed is False


def test_comprehensive_all_fields_detected():
    """Test comprehensive data migration readiness with all aspects present."""
    task = {
        "title": "Complete data migration readiness checklist",
        "description": (
            "Assess migration script quality and configure schema version tracking. "
            "Verify transformation logic for all field mappings. "
            "Ensure rollback tested for all critical paths. "
            "Implement zero downtime implemented for production cutover. "
            "Set up post migration validation configured with automated checks. "
            "Enable progress tracking configured for real-time monitoring. "
            "Achieve adequate migration test coverage across all scenarios."
        ),
        "acceptance_criteria": [
            "Migration script quality assessed",
            "Schema version tracking configured",
            "Transformation logic verified",
            "Rollback procedure verified",
            "Zero downtime implemented",
            "Post-migration validation configured",
            "Progress tracking configured",
            "Migration test coverage adequate",
        ],
    }

    result = analyze_data_migration_readiness(task)

    assert result.migration_script_quality_assessed is True
    assert result.schema_version_tracking_configured is True
    assert result.transformation_logic_verified is True
    assert result.rollback_procedure_tested is True
    assert result.zero_downtime_implemented is True
    assert result.post_migration_validation_configured is True
    assert result.progress_tracking_configured is True
    assert result.migration_testing_coverage_adequate is True
    assert result.readiness_score == 1.0


def test_partial_migration_readiness():
    """Test partial data migration readiness with some aspects covered."""
    task = {
        "title": "Partial migration setup",
        "description": "Assess migration script quality and set up schema version tracking",
        "acceptance_criteria": [
            "Validate migration script testing",
            "Version tracking configured",
        ],
    }

    result = analyze_data_migration_readiness(task)

    assert result.migration_script_quality_assessed is True
    assert result.schema_version_tracking_configured is True
    assert result.transformation_logic_verified is False
    assert result.rollback_procedure_tested is False
    assert result.zero_downtime_implemented is False
    assert result.post_migration_validation_configured is False
    assert result.progress_tracking_configured is False
    assert result.migration_testing_coverage_adequate is False
    assert result.readiness_score == 0.25


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "MIGRATION SCRIPT QUALITY review and SCHEMA VERSION TRACKING setup",
        "acceptance_criteria": ["ROLLBACK TESTED successfully", "ZERO DOWNTIME IMPLEMENTED"],
    }

    result = analyze_data_migration_readiness(task)

    assert result.migration_script_quality_assessed is True
    assert result.schema_version_tracking_configured is True
    assert result.rollback_procedure_tested is True
    assert result.zero_downtime_implemented is True


def test_alternative_terminology_script_validation():
    """Test script validation as migration script quality."""
    task = {
        "description": "Run script validation against production schema",
    }

    result = analyze_data_migration_readiness(task)

    assert result.migration_script_quality_assessed is True


def test_alternative_terminology_migration_code_review():
    """Test migration code review as migration script quality."""
    task = {
        "description": "Complete migration code review before deployment",
    }

    result = analyze_data_migration_readiness(task)

    assert result.migration_script_quality_assessed is True


def test_alternative_terminology_schema_registry():
    """Test schema registry as schema version tracking."""
    task = {
        "description": "Register all schemas in the schema registry",
    }

    result = analyze_data_migration_readiness(task)

    assert result.schema_version_tracking_configured is True


def test_alternative_terminology_schema_catalog():
    """Test schema catalog as schema version tracking."""
    task = {
        "description": "Maintain schema catalog for version history",
    }

    result = analyze_data_migration_readiness(task)

    assert result.schema_version_tracking_configured is True


def test_alternative_terminology_etl_logic():
    """Test ETL logic tested as transformation logic."""
    task = {
        "description": "Ensure ETL logic tested with production-like data",
    }

    result = analyze_data_migration_readiness(task)

    assert result.transformation_logic_verified is True


def test_alternative_terminology_conversion_logic():
    """Test conversion logic validated as transformation logic."""
    task = {
        "description": "Run conversion logic validated against sample datasets",
    }

    result = analyze_data_migration_readiness(task)

    assert result.transformation_logic_verified is True


def test_alternative_terminology_rollback_rehearsal():
    """Test rollback rehearsal as rollback procedure."""
    task = {
        "description": "Conduct rollback rehearsal in staging environment",
    }

    result = analyze_data_migration_readiness(task)

    assert result.rollback_procedure_tested is True


def test_alternative_terminology_rollback_drill():
    """Test rollback drill as rollback procedure."""
    task = {
        "description": "Schedule rollback drill for disaster recovery",
    }

    result = analyze_data_migration_readiness(task)

    assert result.rollback_procedure_tested is True


def test_alternative_terminology_rolling_migration():
    """Test rolling migration setup as zero downtime."""
    task = {
        "description": "Configure rolling migration setup for zero-impact deployment",
    }

    result = analyze_data_migration_readiness(task)

    assert result.zero_downtime_implemented is True


def test_alternative_terminology_dual_write():
    """Test dual write enabled as zero downtime."""
    task = {
        "description": "Enable dual write enabled for seamless migration",
    }

    result = analyze_data_migration_readiness(task)

    assert result.zero_downtime_implemented is True


def test_alternative_terminology_migration_smoke_test():
    """Test migration smoke test as post-migration validation."""
    task = {
        "description": "Run migration smoke test after each migration step",
    }

    result = analyze_data_migration_readiness(task)

    assert result.post_migration_validation_configured is True


def test_alternative_terminology_data_integrity_verified():
    """Test data integrity verified as post-migration validation."""
    task = {
        "description": "Ensure data integrity verified post-cutover",
    }

    result = analyze_data_migration_readiness(task)

    assert result.post_migration_validation_configured is True


def test_alternative_terminology_migration_monitoring():
    """Test migration monitoring setup as progress tracking."""
    task = {
        "description": "Configure migration monitoring setup with alerts",
    }

    result = analyze_data_migration_readiness(task)

    assert result.progress_tracking_configured is True


def test_alternative_terminology_migration_status_reporting():
    """Test migration status reporting as progress tracking."""
    task = {
        "description": "Enable migration status reporting for stakeholders",
    }

    result = analyze_data_migration_readiness(task)

    assert result.progress_tracking_configured is True


def test_alternative_terminology_migration_dry_run():
    """Test migration dry run completed as migration testing coverage."""
    task = {
        "description": "Ensure migration dry run completed in staging",
    }

    result = analyze_data_migration_readiness(task)

    assert result.migration_testing_coverage_adequate is True


def test_alternative_terminology_migration_staging_tested():
    """Test migration staging tested as migration testing coverage."""
    task = {
        "description": "Confirm migration staging tested with full dataset",
    }

    result = analyze_data_migration_readiness(task)

    assert result.migration_testing_coverage_adequate is True


def test_list_based_field_detection():
    """Test detection across multiple list-based task data sections."""
    task = {
        "title": "Migration preparation",
        "description": "Prepare for production migration",
        "acceptance_criteria": ["Migration script quality reviewed"],
        "requirements": ["Schema version tracking must be configured"],
        "notes": ["Transformation logic needs verification"],
        "risks": ["No rollback tested yet"],
    }

    result = analyze_data_migration_readiness(task)

    assert result.migration_script_quality_assessed is True
    assert result.schema_version_tracking_configured is True
    assert result.transformation_logic_verified is True
    assert result.rollback_procedure_tested is True


def test_validation_command_field_detection():
    """Test that validation commands are included in analysis."""
    task = {
        "title": "Basic migration setup",
        "validation_command": "check migration script quality && verify schema version tracking",
    }

    result = analyze_data_migration_readiness(task)

    assert result.migration_script_quality_assessed is True
    assert result.schema_version_tracking_configured is True


def test_validation_commands_as_list():
    """Test validation_commands as list."""
    task = {
        "validation_commands": [
            "verify rollback tested successfully",
            "check migration test coverage meets threshold",
        ],
    }

    result = analyze_data_migration_readiness(task)

    assert result.rollback_procedure_tested is True
    assert result.migration_testing_coverage_adequate is True


def test_to_dict_method():
    """Test DataMigrationReadiness.to_dict() serialization."""
    readiness = DataMigrationReadiness(
        migration_script_quality_assessed=True,
        schema_version_tracking_configured=True,
        transformation_logic_verified=False,
        rollback_procedure_tested=True,
        zero_downtime_implemented=False,
        post_migration_validation_configured=True,
        progress_tracking_configured=False,
        migration_testing_coverage_adequate=True,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["migration_script_quality_assessed"] is True
    assert result["schema_version_tracking_configured"] is True
    assert result["transformation_logic_verified"] is False
    assert result["rollback_procedure_tested"] is True
    assert result["zero_downtime_implemented"] is False
    assert result["post_migration_validation_configured"] is True
    assert result["progress_tracking_configured"] is False
    assert result["migration_testing_coverage_adequate"] is True
    assert result["readiness_score"] == 0.625


def test_dataclass_immutability():
    """Test that DataMigrationReadiness is frozen/immutable."""
    readiness = DataMigrationReadiness(migration_script_quality_assessed=True)

    with pytest.raises(AttributeError):
        readiness.migration_script_quality_assessed = False  # type: ignore


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_data_migration_readiness(None)  # type: ignore

    assert isinstance(result, DataMigrationReadiness)
    assert result.migration_script_quality_assessed is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_data_migration_readiness([{"key": "value"}])  # type: ignore

    assert isinstance(result, DataMigrationReadiness)
    assert result.migration_script_quality_assessed is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_string():
    """Test with string input instead of mapping."""
    result = analyze_data_migration_readiness("not a mapping")  # type: ignore

    assert isinstance(result, DataMigrationReadiness)
    assert result.migration_script_quality_assessed is False


def test_invalid_task_data_tuple():
    """Test with tuple input instead of mapping."""
    result = analyze_data_migration_readiness(("tuple", "data"))  # type: ignore

    assert isinstance(result, DataMigrationReadiness)
    assert result.migration_script_quality_assessed is False


def test_partial_data_missing_fields():
    """Test with partial task data missing some fields."""
    task = {
        "title": "System configuration",
        # Missing description, acceptance_criteria, etc.
    }

    result = analyze_data_migration_readiness(task)

    assert isinstance(result, DataMigrationReadiness)
    assert result.readiness_score == 0.0


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    task = {
        "acceptance_criteria": "Run migration script quality check and verify transformation logic",
    }

    result = analyze_data_migration_readiness(task)

    assert result.migration_script_quality_assessed is True
    assert result.transformation_logic_verified is True


def test_empty_string_fields():
    """Test handling of empty string fields."""
    task = {
        "title": "",
        "description": "",
        "acceptance_criteria": [""],
    }

    result = analyze_data_migration_readiness(task)

    assert result.migration_script_quality_assessed is False
    assert result.readiness_score == 0.0


def test_readiness_score_calculation():
    """Test readiness score calculation with different combinations."""
    # 0/8 = 0.0
    task1 = {"description": "Generic task"}
    result1 = analyze_data_migration_readiness(task1)
    assert result1.readiness_score == 0.0

    # 1/8 = 0.125
    task2 = {"description": "Assess migration script quality"}
    result2 = analyze_data_migration_readiness(task2)
    assert result2.readiness_score == 0.125

    # 4/8 = 0.5
    task3 = {
        "description": (
            "Assess migration script quality, configure schema version tracking, "
            "verify transformation logic, and ensure rollback tested"
        )
    }
    result3 = analyze_data_migration_readiness(task3)
    assert result3.readiness_score == 0.5
