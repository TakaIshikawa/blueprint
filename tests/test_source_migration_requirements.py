"""Tests for migration requirements extractor."""

import pytest

from blueprint.source_migration_requirements import (
    MigrationRequirements,
    extract_migration_requirements,
)


def test_empty_source_data_returns_all_false():
    result = extract_migration_requirements({})
    assert isinstance(result, MigrationRequirements)
    assert result.source_systems_identified is False
    assert result.data_volume_estimated is False
    assert result.transformation_rules_defined is False
    assert result.validation_criteria_specified is False
    assert result.cutover_strategy_planned is False
    assert result.data_quality_addressed is False
    assert result.deduplication_planned is False
    assert result.reconciliation_included is False
    assert result.rollback_procedures_defined is False
    assert result.dual_run_period_planned is False
    assert result.completeness_score == 0.0


def test_source_systems_detected():
    source = {"description": "Migrate from legacy system to new platform"}
    result = extract_migration_requirements(source)
    assert result.source_systems_identified is True


def test_data_volume_detected():
    source = {"description": "Migration of millions of records with 500GB of data"}
    result = extract_migration_requirements(source)
    assert result.data_volume_estimated is True


def test_transformation_rules_detected():
    source = {"requirements": ["Define transformation rules", "Field mapping required"]}
    result = extract_migration_requirements(source)
    assert result.transformation_rules_defined is True


def test_validation_criteria_detected():
    source = {"description": "Validate data integrity and data quality checks"}
    result = extract_migration_requirements(source)
    assert result.validation_criteria_specified is True
    assert result.data_quality_addressed is True


def test_cutover_strategy_detected():
    source = {"description": "Plan cutover strategy with go-live timeline"}
    result = extract_migration_requirements(source)
    assert result.cutover_strategy_planned is True


def test_deduplication_detected():
    source = {"requirements": ["Remove duplicates", "Deduplication required"]}
    result = extract_migration_requirements(source)
    assert result.deduplication_planned is True


def test_reconciliation_detected():
    source = {"description": "Reconcile data between source and target systems"}
    result = extract_migration_requirements(source)
    assert result.reconciliation_included is True


def test_rollback_procedures_detected():
    source = {"description": "Define rollback procedure if migration fails"}
    result = extract_migration_requirements(source)
    assert result.rollback_procedures_defined is True


def test_dual_run_period_detected():
    source = {"description": "Dual-run period with both systems running in parallel"}
    result = extract_migration_requirements(source)
    assert result.dual_run_period_planned is True


def test_comprehensive_migration_all_detected():
    source = {
        "title": "Complete migration specification",
        "description": (
            "Migrate from legacy system with millions of records estimated. "
            "Define transformation rules and field mapping. "
            "Validate data with quality checks and deduplication. "
            "Plan cutover strategy with rollback procedures. "
            "Reconcile data between systems during dual-run period."
        ),
    }
    result = extract_migration_requirements(source)
    assert result.completeness_score == 1.0


def test_invalid_source_data_none():
    result = extract_migration_requirements(None)  # type: ignore
    assert isinstance(result, MigrationRequirements)
    assert result.completeness_score == 0.0


def test_incremental_migrations_edge_case():
    source = {"description": "Incremental migration with phased cutover approach"}
    result = extract_migration_requirements(source)
    assert result.cutover_strategy_planned is True


def test_zero_downtime_migrations_edge_case():
    source = {"description": "Zero-downtime migration with parallel run"}
    result = extract_migration_requirements(source)
    assert result.dual_run_period_planned is True


def test_multi_phase_migrations_edge_case():
    source = {"description": "Multi-phase migration from legacy database"}
    result = extract_migration_requirements(source)
    assert result.source_systems_identified is True


def test_dataclass_immutability():
    reqs = MigrationRequirements(source_systems_identified=True)
    with pytest.raises(AttributeError):
        reqs.source_systems_identified = False  # type: ignore


def test_to_dict_method():
    reqs = MigrationRequirements(
        source_systems_identified=True,
        data_volume_estimated=True,
        transformation_rules_defined=False,
        validation_criteria_specified=True,
        cutover_strategy_planned=False,
        data_quality_addressed=True,
        deduplication_planned=False,
        reconciliation_included=True,
        rollback_procedures_defined=False,
        dual_run_period_planned=True,
    )
    result = reqs.to_dict()
    assert isinstance(result, dict)
    assert result["source_systems_identified"] is True
    assert result["completeness_score"] == 0.6
