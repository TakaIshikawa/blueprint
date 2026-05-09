"""Tests for multi-tenancy readiness analyzer."""

import pytest

from blueprint.task_multi_tenancy_readiness import (
    MultiTenancyReadiness,
    analyze_multi_tenancy_readiness,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_multi_tenancy_readiness({})

    assert isinstance(result, MultiTenancyReadiness)
    assert result.isolation_model_defined is False
    assert result.data_partitioning_specified is False
    assert result.tenant_identification_configured is False
    assert result.resource_quotas_planned is False
    assert result.cross_tenant_leakage_prevented is False
    assert result.tenant_customization_supported is False
    assert result.tenant_lifecycle_managed is False
    assert result.tenant_administration_included is False
    assert result.readiness_score == 0.0


def test_isolation_model_detected():
    """Detect isolation model definition in task data."""
    task = {
        "title": "Define tenant isolation model",
        "description": "Implement shared schema with tenant isolation",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True
    assert result.data_partitioning_specified is False
    assert result.readiness_score == 0.125


def test_data_partitioning_detected():
    """Detect data partitioning strategy in task data."""
    task = {
        "description": "Implement data partitioning by tenant_id column with RLS",
        "acceptance_criteria": ["Partition strategy documented", "Tenant key defined"],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.data_partitioning_specified is True
    assert result.isolation_model_defined is False


def test_tenant_identification_detected():
    """Detect tenant identification configuration in task data."""
    task = {
        "description": "Implement tenant identification from subdomain and JWT claims",
        "acceptance_criteria": ["Tenant context middleware configured", "Resolve tenant from header"],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_identification_configured is True
    assert result.isolation_model_defined is False


def test_resource_quotas_detected():
    """Detect resource quota planning in task data."""
    task = {
        "title": "Configure tenant quotas",
        "description": "Set per-tenant resource limits for storage and API rate limiting",
        "acceptance_criteria": ["Resource quotas configured", "Usage limits enforced"],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.resource_quotas_planned is True
    assert result.isolation_model_defined is False


def test_cross_tenant_leakage_prevention_detected():
    """Detect cross-tenant leakage prevention in task data."""
    task = {
        "description": "Prevent cross-tenant data access with tenant boundary enforcement",
        "acceptance_criteria": [
            "Data segregation implemented",
        ],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.cross_tenant_leakage_prevented is True


def test_tenant_customization_detected():
    """Detect tenant customization support in task data."""
    task = {
        "description": "Support tenant-specific branding with per-tenant configuration",
        "acceptance_criteria": [
            "Tenant customization enabled",
            "White-labeling supported",
        ],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_customization_supported is True
    assert result.isolation_model_defined is False


def test_tenant_lifecycle_detected():
    """Detect tenant lifecycle management in task data."""
    task = {
        "description": "Implement tenant onboarding and offboarding workflows with provisioning automation",
        "acceptance_criteria": ["Tenant provisioning automated", "Tenant deletion cleanup verified"],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_lifecycle_managed is True
    assert result.isolation_model_defined is False


def test_tenant_administration_detected():
    """Detect tenant administration features in task data."""
    task = {
        "description": "Build tenant admin portal with user management and audit logging",
        "acceptance_criteria": [
            "Tenant dashboard created",
            "Tenant-level permissions configured",
        ],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_administration_included is True
    assert result.isolation_model_defined is False


def test_comprehensive_multi_tenancy_all_detected():
    """Test comprehensive multi-tenancy implementation with all aspects present."""
    task = {
        "title": "Complete multi-tenant architecture implementation",
        "description": (
            "Implement multi-tenant architecture with separate schema isolation model and data partitioning by tenant_id. "
            "Configure tenant identification from subdomain with tenant context middleware. "
            "Set per-tenant resource quotas and API rate limiting. "
            "Prevent cross-tenant data leakage with tenant security boundaries. "
            "Support tenant-specific customization and white-labeling. "
            "Implement tenant onboarding and offboarding workflows. "
            "Build tenant administration portal with monitoring."
        ),
        "acceptance_criteria": [
            "Isolation model defined with separate schema",
            "Data partitioning strategy implemented",
            "Tenant identification configured",
            "Resource quotas enforced",
            "Cross-tenant access prevented",
            "Tenant customization supported",
            "Tenant lifecycle managed",
            "Admin portal deployed",
        ],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True
    assert result.data_partitioning_specified is True
    assert result.tenant_identification_configured is True
    assert result.resource_quotas_planned is True
    assert result.cross_tenant_leakage_prevented is True
    assert result.tenant_customization_supported is True
    assert result.tenant_lifecycle_managed is True
    assert result.tenant_administration_included is True
    assert result.readiness_score == 1.0


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_multi_tenancy_readiness(None)  # type: ignore

    assert isinstance(result, MultiTenancyReadiness)
    assert result.isolation_model_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_multi_tenancy_readiness([{"key": "value"}])  # type: ignore

    assert isinstance(result, MultiTenancyReadiness)
    assert result.isolation_model_defined is False
    assert result.readiness_score == 0.0


def test_invalid_task_data_string():
    """Test with string input instead of mapping."""
    result = analyze_multi_tenancy_readiness("not a mapping")  # type: ignore

    assert isinstance(result, MultiTenancyReadiness)
    assert result.isolation_model_defined is False


def test_invalid_task_data_tuple():
    """Test with tuple input instead of mapping."""
    result = analyze_multi_tenancy_readiness(("tuple", "data"))  # type: ignore

    assert isinstance(result, MultiTenancyReadiness)
    assert result.isolation_model_defined is False


def test_partial_data_missing_fields():
    """Test with partial task data missing some fields."""
    task = {
        "title": "System configuration",
        # Missing description, acceptance_criteria, etc.
    }

    result = analyze_multi_tenancy_readiness(task)

    assert isinstance(result, MultiTenancyReadiness)
    assert result.readiness_score == 0.0


def test_partial_multi_tenancy_readiness():
    """Test partial multi-tenancy readiness with some aspects covered."""
    task = {
        "title": "System infrastructure",
        "description": "Set up basic infrastructure",
        "acceptance_criteria": [
            "Shared database with separate schema per tenant",
            "Partition by tenant_id",
        ],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True
    assert result.data_partitioning_specified is True
    assert result.tenant_identification_configured is False
    assert result.resource_quotas_planned is False
    assert result.cross_tenant_leakage_prevented is False
    assert result.tenant_customization_supported is False
    assert result.tenant_lifecycle_managed is False
    assert result.tenant_administration_included is False
    assert result.readiness_score == 0.25


def test_task_data_with_nested_acceptance_criteria():
    """Test extraction from nested acceptance criteria structure."""
    task = {
        "title": "Multi-tenant improvements",
        "acceptance_criteria": [
            "Define tenant isolation model with shared instance",
            "Implement tenant identification from JWT token",
            "Configure per-tenant storage quota",
        ],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True
    assert result.tenant_identification_configured is True
    assert result.resource_quotas_planned is True


def test_validation_commands_checked():
    """Test that validation commands are included in analysis."""
    task = {
        "title": "Basic setup",
        "validation_command": "pytest tests/test_tenant_isolation.py tests/test_data_partitioning.py",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True
    assert result.data_partitioning_specified is True


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "TENANT ISOLATION with DATA PARTITIONING and USAGE LIMITS",
        "acceptance_criteria": ["TENANT IDENTIFICATION configured", "PREVENT data leakage"],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True
    assert result.data_partitioning_specified is True
    assert result.resource_quotas_planned is True
    assert result.tenant_identification_configured is True
    assert result.cross_tenant_leakage_prevented is True


def test_alternative_terminology_isolation_shared_schema():
    """Test shared schema isolation terminology is recognized."""
    task = {
        "description": "Implement shared schema multi-tenant architecture with database isolation",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True


def test_alternative_terminology_isolation_separate_database():
    """Test separate database isolation terminology is recognized."""
    task = {
        "description": "Use dedicated database per tenant with siloed tenancy",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True


def test_alternative_terminology_partitioning_rls():
    """Test RLS partitioning terminology is recognized."""
    task = {
        "description": "Implement row-level security for tenant data partition",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.data_partitioning_specified is True


def test_alternative_terminology_partitioning_sharding():
    """Test sharding terminology is recognized."""
    task = {
        "description": "Shard by tenant for horizontal partitioning strategy",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.data_partitioning_specified is True


def test_alternative_terminology_identification_subdomain():
    """Test subdomain-based tenant identification is recognized."""
    task = {
        "description": "Extract tenant from subdomain in tenant middleware",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_identification_configured is True


def test_alternative_terminology_identification_jwt():
    """Test JWT claim-based tenant identification is recognized."""
    task = {
        "description": "Resolve tenant from JWT claim in authentication token",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_identification_configured is True


def test_alternative_terminology_quotas_rate_limiting():
    """Test rate limiting as resource quota is recognized."""
    task = {
        "description": "Throttle API requests per tenant with rate limiting",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.resource_quotas_planned is True


def test_alternative_terminology_quotas_storage():
    """Test storage quota terminology is recognized."""
    task = {
        "description": "Enforce storage quota limits per tenant",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.resource_quotas_planned is True


def test_alternative_terminology_leakage_prevention():
    """Test data segregation as leakage prevention is recognized."""
    task = {
        "description": "Implement data segregation to prevent unauthorized cross-tenant access",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.cross_tenant_leakage_prevented is True


def test_alternative_terminology_leakage_boundary():
    """Test tenant boundary terminology is recognized."""
    task = {
        "description": "Enforce tenant security boundary to isolate tenant data",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.cross_tenant_leakage_prevented is True


def test_alternative_terminology_customization_branding():
    """Test tenant branding terminology is recognized."""
    task = {
        "description": "Support per-tenant branding and theme customization",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_customization_supported is True


def test_alternative_terminology_customization_white_label():
    """Test white-labeling terminology is recognized."""
    task = {
        "description": "Enable white-labeling for tenant-specific features",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_customization_supported is True


def test_alternative_terminology_lifecycle_provisioning():
    """Test tenant provisioning terminology is recognized."""
    task = {
        "description": "Automate tenant provisioning during onboarding",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_lifecycle_managed is True


def test_alternative_terminology_lifecycle_deprovisioning():
    """Test tenant deprovisioning terminology is recognized."""
    task = {
        "description": "Handle tenant offboarding with data cleanup and deletion",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_lifecycle_managed is True


def test_alternative_terminology_administration_portal():
    """Test tenant admin portal terminology is recognized."""
    task = {
        "description": "Build tenant management console with administration dashboard",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_administration_included is True


def test_alternative_terminology_administration_monitoring():
    """Test tenant monitoring terminology is recognized."""
    task = {
        "description": "Configure tenant audit logging and analytics",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_administration_included is True


def test_to_dict_method():
    """Test MultiTenancyReadiness.to_dict() serialization."""
    readiness = MultiTenancyReadiness(
        isolation_model_defined=True,
        data_partitioning_specified=True,
        tenant_identification_configured=False,
        resource_quotas_planned=True,
        cross_tenant_leakage_prevented=False,
        tenant_customization_supported=True,
        tenant_lifecycle_managed=False,
        tenant_administration_included=True,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["isolation_model_defined"] is True
    assert result["data_partitioning_specified"] is True
    assert result["tenant_identification_configured"] is False
    assert result["resource_quotas_planned"] is True
    assert result["cross_tenant_leakage_prevented"] is False
    assert result["tenant_customization_supported"] is True
    assert result["tenant_lifecycle_managed"] is False
    assert result["tenant_administration_included"] is True
    assert result["readiness_score"] == 0.625


def test_multiple_fields_in_different_sections():
    """Test detection across multiple task data sections."""
    task = {
        "title": "System setup",
        "description": "Define isolation model",
        "acceptance_criteria": ["Data partitioning implemented"],
        "requirements": ["Tenant identification configured"],
        "notes": ["Usage limits needed"],
        "risks": ["No data leakage prevention"],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True
    assert result.data_partitioning_specified is True
    assert result.tenant_identification_configured is True
    assert result.resource_quotas_planned is True
    assert result.cross_tenant_leakage_prevented is True


def test_validation_commands_as_list():
    """Test validation_commands as list."""
    task = {
        "validation_commands": [
            "test_tenant_isolation.py",
            "test_tenant_provisioning.py",
        ],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True
    assert result.tenant_lifecycle_managed is True


def test_dataclass_immutability():
    """Test that MultiTenancyReadiness is frozen/immutable."""
    readiness = MultiTenancyReadiness(isolation_model_defined=True)

    with pytest.raises(AttributeError):
        readiness.isolation_model_defined = False  # type: ignore


def test_shared_schema_pattern():
    """Test shared schema multi-tenant pattern."""
    task = {
        "description": "Implement shared schema with tenant_id column for data isolation",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True
    assert result.data_partitioning_specified is True


def test_separate_schema_pattern():
    """Test separate schema per tenant pattern."""
    task = {
        "description": "Use separate schema per tenant with schema isolation",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True


def test_separate_database_pattern():
    """Test separate database per tenant pattern."""
    task = {
        "description": "Dedicated database per tenant for complete data isolation",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True


def test_hierarchical_tenants_edge_case():
    """Test hierarchical tenant structure detection."""
    task = {
        "description": "Support hierarchical tenant structure with parent-child tenant relationships",
        "acceptance_criteria": [
            "Tenant hierarchy model defined",
            "Parent tenant isolation maintained",
        ],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True


def test_tenant_migration_edge_case():
    """Test tenant data migration detection."""
    task = {
        "description": "Implement tenant migration workflow for moving tenant data between isolation tiers",
        "acceptance_criteria": [
            "Migration process documented",
            "Tenant lifecycle managed during migration",
        ],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_lifecycle_managed is True


def test_cross_tenant_operations_edge_case():
    """Test controlled cross-tenant operations detection."""
    task = {
        "description": "Enable controlled cross-tenant reporting while preventing unauthorized cross-tenant access",
        "acceptance_criteria": [
            "Cross-tenant security boundary enforced",
            "Authorized cross-tenant operations audited",
        ],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.cross_tenant_leakage_prevented is True


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    task = {
        "acceptance_criteria": "Define tenant isolation model and implement data partitioning",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True
    assert result.data_partitioning_specified is True


def test_readiness_score_calculation():
    """Test readiness score calculation with different combinations."""
    # 0/8 = 0.0
    task1 = {"description": "Generic task"}
    result1 = analyze_multi_tenancy_readiness(task1)
    assert result1.readiness_score == 0.0

    # 1/8 = 0.125
    task2 = {"description": "Define database isolation"}
    result2 = analyze_multi_tenancy_readiness(task2)
    assert result2.readiness_score == 0.125

    # 4/8 = 0.5
    task3 = {
        "description": "Database isolation, partition by tenant_id, identify tenant from subdomain, and set usage limits"
    }
    result3 = analyze_multi_tenancy_readiness(task3)
    assert result3.readiness_score == 0.5

    # 8/8 = 1.0
    task4 = {
        "description": (
            "Tenant isolation model with data partitioning, tenant identification, usage limits, "
            "prevent data leakage, tenant customization, tenant provisioning, and admin portal"
        )
    }
    result4 = analyze_multi_tenancy_readiness(task4)
    assert result4.readiness_score == 1.0


def test_pooled_tenancy_pattern():
    """Test pooled tenancy pattern detection."""
    task = {
        "description": "Implement pooled tenancy with shared instance and data isolation",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True


def test_siloed_tenancy_pattern():
    """Test siloed tenancy pattern detection."""
    task = {
        "description": "Use siloed tenant architecture with dedicated resources",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True


def test_bridge_model_pattern():
    """Test bridge model pattern detection."""
    task = {
        "description": "Implement bridge model for hybrid tenant isolation",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is True


def test_tenant_context_middleware():
    """Test tenant context middleware detection."""
    task = {
        "description": "Add tenant context middleware to identify current tenant",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_identification_configured is True


def test_tenant_from_path():
    """Test tenant identification from URL path."""
    task = {
        "description": "Extract tenant identifier from request path",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_identification_configured is True


def test_fair_use_policy():
    """Test fair use policy as resource quota."""
    task = {
        "description": "Enforce fair use policy for per-tenant resource allocation",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.resource_quotas_planned is True


def test_api_quota():
    """Test API quota detection."""
    task = {
        "description": "Configure API quota limits for each tenant",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.resource_quotas_planned is True


def test_tenant_suspension():
    """Test tenant suspension as lifecycle management."""
    task = {
        "description": "Implement tenant suspension for policy violations",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_lifecycle_managed is True


def test_tenant_activation():
    """Test tenant activation as lifecycle management."""
    task = {
        "description": "Handle tenant activation after onboarding",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_lifecycle_managed is True


def test_tenant_role_management():
    """Test tenant role management as administration."""
    task = {
        "description": "Configure tenant-level role and permission management",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_administration_included is True


def test_tenant_analytics():
    """Test tenant analytics as administration feature."""
    task = {
        "description": "Provide tenant analytics dashboard for usage monitoring",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_administration_included is True


def test_empty_string_fields():
    """Test handling of empty string fields."""
    task = {
        "title": "",
        "description": "",
        "acceptance_criteria": [""],
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.isolation_model_defined is False
    assert result.readiness_score == 0.0


def test_tenant_override():
    """Test tenant override as customization feature."""
    task = {
        "description": "Support tenant-specific configuration overrides",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_customization_supported is True


def test_tenant_extension():
    """Test tenant extension as customization feature."""
    task = {
        "description": "Enable tenant extensions for custom features",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_customization_supported is True


def test_tenant_preference():
    """Test tenant preference as customization feature."""
    task = {
        "description": "Store tenant preferences for per-tenant settings",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_customization_supported is True


def test_row_level_security():
    """Test row-level security detection."""
    task = {
        "description": "Implement RLS policies for tenant data partitioning",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.data_partitioning_specified is True


def test_partition_key():
    """Test partition key detection."""
    task = {
        "description": "Use partition key for tenant data separation",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.data_partitioning_specified is True


def test_tenant_specific_tables():
    """Test tenant-specific table detection."""
    task = {
        "description": "Create tenant-specific tables for data isolation",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.data_partitioning_specified is True


def test_tenant_registration():
    """Test tenant registration as lifecycle management."""
    task = {
        "description": "Implement tenant registration workflow during onboarding",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_lifecycle_managed is True


def test_tenant_cleanup():
    """Test tenant cleanup as lifecycle management."""
    task = {
        "description": "Perform tenant data cleanup during offboarding",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_lifecycle_managed is True


def test_usage_limit():
    """Test usage limit as resource quota."""
    task = {
        "description": "Set usage limits for tenant resource consumption",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.resource_quotas_planned is True


def test_tenant_data_breach_prevention():
    """Test tenant data breach prevention."""
    task = {
        "description": "Prevent tenant data leakage and unauthorized access",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.cross_tenant_leakage_prevented is True


def test_tenant_separation():
    """Test tenant separation as isolation concern."""
    task = {
        "description": "Ensure tenant separation with secure tenant boundaries",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.cross_tenant_leakage_prevented is True


def test_tenant_console():
    """Test tenant console as administration feature."""
    task = {
        "description": "Build tenant console for self-service administration",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_administration_included is True


def test_tenant_user_management():
    """Test tenant user management as administration feature."""
    task = {
        "description": "Implement tenant user management for admin access",
    }

    result = analyze_multi_tenancy_readiness(task)

    assert result.tenant_administration_included is True
