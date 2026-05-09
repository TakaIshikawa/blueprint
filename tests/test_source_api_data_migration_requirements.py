"""Tests for API data migration requirements extractor."""

import pytest

from blueprint.source_api_data_migration_requirements import (
    DataMigrationRequirements,
    extract_data_migration_requirements,
)


def test_empty_source_data_returns_all_false():
    """Empty source data should return all fields as False."""
    result = extract_data_migration_requirements({})

    assert isinstance(result, DataMigrationRequirements)
    assert result.migration_scripts_specified is False
    assert result.schema_versioning_defined is False
    assert result.data_transformation_rules_specified is False
    assert result.rollback_procedures_defined is False
    assert result.zero_downtime_strategy_specified is False
    assert result.post_migration_validation_planned is False
    assert result.progress_tracking_included is False
    assert result.migration_testing_specified is False
    assert result.completeness_score == 0.0


def test_migration_scripts_detected():
    """Detect migration scripts in source data."""
    source = {
        "title": "Database update",
        "description": "Create migration scripts for the new user table schema",
    }

    result = extract_data_migration_requirements(source)

    assert result.migration_scripts_specified is True
    assert result.completeness_score == 0.125


def test_schema_versioning_detected():
    """Detect schema versioning in source data."""
    source = {
        "description": "Implement schema versioning for the API data models",
        "requirements": ["Schema evolution strategy defined"],
    }

    result = extract_data_migration_requirements(source)

    assert result.schema_versioning_defined is True


def test_data_transformation_detected():
    """Detect data transformation rules in source data."""
    source = {
        "description": "Define data transformation rules for field mapping",
        "requirements": ["Column mapping from legacy to new schema"],
    }

    result = extract_data_migration_requirements(source)

    assert result.data_transformation_rules_specified is True


def test_rollback_procedures_detected():
    """Detect rollback procedures in source data."""
    source = {
        "description": "Implement migration rollback procedures for safe deployments",
        "requirements": ["Downgrade migration support", "Reverse migration scripts ready"],
    }

    result = extract_data_migration_requirements(source)

    assert result.rollback_procedures_defined is True


def test_zero_downtime_detected():
    """Detect zero downtime strategy in source data."""
    source = {
        "description": "Perform zero downtime migration using dual write approach",
        "requirements": ["Online migration without service interruption"],
    }

    result = extract_data_migration_requirements(source)

    assert result.zero_downtime_strategy_specified is True


def test_post_migration_validation_detected():
    """Detect post migration validation in source data."""
    source = {
        "description": "Run post migration validation checks after deployment",
        "requirements": ["Data integrity check after migration"],
    }

    result = extract_data_migration_requirements(source)

    assert result.post_migration_validation_planned is True


def test_progress_tracking_detected():
    """Detect migration progress tracking in source data."""
    source = {
        "description": "Add migration progress monitoring and dashboard",
        "requirements": ["Track migration status in real time"],
    }

    result = extract_data_migration_requirements(source)

    assert result.progress_tracking_included is True


def test_migration_testing_detected():
    """Detect migration testing in source data."""
    source = {
        "description": "Set up migration dry run in staging environment",
        "requirements": ["Test data migration before production"],
    }

    result = extract_data_migration_requirements(source)

    assert result.migration_testing_specified is True


def test_comprehensive_all_detected():
    """Test comprehensive source with all aspects present."""
    source = {
        "title": "Complete data migration plan",
        "description": (
            "Create migration scripts for schema changes. "
            "Implement schema versioning with backward compatible schema. "
            "Define data transformation and field mapping rules. "
            "Prepare migration rollback procedures for each step. "
            "Use zero downtime strategy with dual write. "
            "Run post migration validation after deployment. "
            "Add migration progress tracking and monitoring. "
            "Execute migration dry run in staging first."
        ),
        "requirements": [
            "Migration scripts ready",
            "Schema evolution documented",
            "Data mapping rules defined",
            "Rollback migration scripts",
            "Online migration support",
            "Validate migration results",
            "Migration status dashboard",
            "Test migration in staging",
        ],
    }

    result = extract_data_migration_requirements(source)

    assert result.migration_scripts_specified is True
    assert result.schema_versioning_defined is True
    assert result.data_transformation_rules_specified is True
    assert result.rollback_procedures_defined is True
    assert result.zero_downtime_strategy_specified is True
    assert result.post_migration_validation_planned is True
    assert result.progress_tracking_included is True
    assert result.migration_testing_specified is True
    assert result.completeness_score == 1.0


def test_invalid_source_data_none():
    """Test with None input."""
    result = extract_data_migration_requirements(None)  # type: ignore

    assert isinstance(result, DataMigrationRequirements)
    assert result.completeness_score == 0.0


def test_invalid_source_data_list():
    """Test with list input instead of mapping."""
    result = extract_data_migration_requirements([{"key": "value"}])  # type: ignore

    assert isinstance(result, DataMigrationRequirements)
    assert result.completeness_score == 0.0


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    source = {
        "description": "MIGRATION SCRIPTS and SCHEMA VERSIONING with DATA TRANSFORMATION",
        "requirements": ["ROLLBACK MIGRATION defined", "MIGRATION DRY RUN planned"],
    }

    result = extract_data_migration_requirements(source)

    assert result.migration_scripts_specified is True
    assert result.schema_versioning_defined is True
    assert result.data_transformation_rules_specified is True
    assert result.rollback_procedures_defined is True
    assert result.migration_testing_specified is True


def test_alembic_terminology():
    """Test Alembic migration tool terminology is recognized."""
    source = {
        "description": "Use alembic for database schema migrations",
    }

    result = extract_data_migration_requirements(source)

    assert result.migration_scripts_specified is True


def test_flyway_terminology():
    """Test Flyway migration tool terminology is recognized."""
    source = {
        "description": "Set up flyway for versioned database migrations",
    }

    result = extract_data_migration_requirements(source)

    assert result.migration_scripts_specified is True


def test_etl_terminology():
    """Test ETL terminology for data transformation."""
    source = {
        "description": "Build ETL pipeline for data conversion",
    }

    result = extract_data_migration_requirements(source)

    assert result.data_transformation_rules_specified is True


def test_blue_green_migration():
    """Test blue green migration as zero downtime strategy."""
    source = {
        "description": "Deploy using blue green migration approach",
    }

    result = extract_data_migration_requirements(source)

    assert result.zero_downtime_strategy_specified is True


def test_dual_write_strategy():
    """Test dual write as zero downtime strategy."""
    source = {
        "description": "Implement dual write pattern during migration period",
    }

    result = extract_data_migration_requirements(source)

    assert result.zero_downtime_strategy_specified is True


def test_migration_smoke_test():
    """Test migration smoke test as post-migration validation."""
    source = {
        "description": "Execute migration smoke test after each batch completes",
    }

    result = extract_data_migration_requirements(source)

    assert result.post_migration_validation_planned is True


def test_migration_audit_log():
    """Test migration audit as progress tracking."""
    source = {
        "description": "Maintain migration audit log for compliance tracking",
    }

    result = extract_data_migration_requirements(source)

    assert result.progress_tracking_included is True


def test_migration_rehearsal():
    """Test migration rehearsal as migration testing."""
    source = {
        "description": "Conduct a migration rehearsal before the production cutover",
    }

    result = extract_data_migration_requirements(source)

    assert result.migration_testing_specified is True


def test_list_based_field_detection():
    """Test that list-based fields are searched for patterns."""
    source = {
        "requirements": ["Prepare migration scripts for all tables"],
        "acceptance_criteria": ["Schema versioning in place"],
        "constraints": ["Data transformation must be idempotent"],
        "notes": ["Migration rollback tested"],
        "definition_of_done": ["Post migration validation passes"],
    }

    result = extract_data_migration_requirements(source)

    assert result.migration_scripts_specified is True
    assert result.schema_versioning_defined is True
    assert result.data_transformation_rules_specified is True
    assert result.rollback_procedures_defined is True
    assert result.post_migration_validation_planned is True


def test_to_dict_method():
    """Test DataMigrationRequirements.to_dict() serialization."""
    requirements = DataMigrationRequirements(
        migration_scripts_specified=True,
        schema_versioning_defined=True,
        data_transformation_rules_specified=False,
        rollback_procedures_defined=True,
        zero_downtime_strategy_specified=False,
        post_migration_validation_planned=True,
        progress_tracking_included=False,
        migration_testing_specified=True,
    )

    result = requirements.to_dict()

    assert isinstance(result, dict)
    assert result["migration_scripts_specified"] is True
    assert result["schema_versioning_defined"] is True
    assert result["data_transformation_rules_specified"] is False
    assert result["rollback_procedures_defined"] is True
    assert result["zero_downtime_strategy_specified"] is False
    assert result["post_migration_validation_planned"] is True
    assert result["progress_tracking_included"] is False
    assert result["migration_testing_specified"] is True
    assert result["completeness_score"] == 0.625


def test_dataclass_immutability():
    """Test that DataMigrationRequirements is frozen/immutable."""
    requirements = DataMigrationRequirements(migration_scripts_specified=True)

    with pytest.raises(AttributeError):
        requirements.migration_scripts_specified = False  # type: ignore


def test_multiple_fields_in_different_sections():
    """Test detection across multiple source data sections."""
    source = {
        "title": "Data migration plan",
        "description": "Migration scripts ready",
        "requirements": ["Schema versioning defined"],
        "acceptance_criteria": ["Data transformation rules documented"],
        "notes": ["Migration rollback procedures tested"],
        "definition_of_done": ["Migration progress tracking active"],
    }

    result = extract_data_migration_requirements(source)

    assert result.migration_scripts_specified is True
    assert result.schema_versioning_defined is True
    assert result.data_transformation_rules_specified is True
    assert result.rollback_procedures_defined is True
    assert result.progress_tracking_included is True


def test_partial_migration_completeness():
    """Test partial migration with some aspects covered."""
    source = {
        "description": "Basic migration implementation",
        "requirements": [
            "Migration scripts configured",
            "Data transformation rules defined",
        ],
    }

    result = extract_data_migration_requirements(source)

    assert result.migration_scripts_specified is True
    assert result.data_transformation_rules_specified is True
    assert result.schema_versioning_defined is False
    assert result.rollback_procedures_defined is False
    assert result.completeness_score == 0.25
