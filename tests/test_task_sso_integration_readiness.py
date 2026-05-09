"""Tests for SSO integration readiness analyzer."""

import pytest

from blueprint.task_sso_integration_readiness import (
    SsoIntegrationReadiness,
    analyze_sso_integration_readiness,
)


def test_empty_change_brief_returns_all_false():
    """Empty change brief should return all fields as False."""
    result = analyze_sso_integration_readiness({})

    assert isinstance(result, SsoIntegrationReadiness)
    assert result.protocol_support_defined is False
    assert result.idp_configuration_specified is False
    assert result.user_provisioning_addressed is False
    assert result.role_mapping_configured is False
    assert result.session_management_implemented is False
    assert result.logout_handling_implemented is False
    assert result.group_sync_configured is False
    assert result.multi_tenant_isolation_considered is False
    assert result.security_measures_included is False
    assert result.error_handling_implemented is False
    assert result.sso_testing_planned is False


def test_saml_protocol_support_detected():
    """Detect SAML 2.0 protocol support in change brief."""
    brief = {
        "title": "Implement SAML 2.0 authentication",
        "description": "Add SAML support for enterprise customers",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.protocol_support_defined is True
    assert result.idp_configuration_specified is False


def test_oauth_protocol_support_detected():
    """Detect OAuth 2.0 protocol support in change brief."""
    brief = {
        "description": "Implement OAuth 2.0 integration for SSO",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.protocol_support_defined is True
    assert result.idp_configuration_specified is False


def test_oidc_protocol_support_detected():
    """Detect OpenID Connect (OIDC) protocol support in change brief."""
    brief = {
        "description": "Add OpenID Connect support for federated authentication",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.protocol_support_defined is True
    assert result.idp_configuration_specified is False


def test_identity_provider_configuration_detected():
    """Detect identity provider configuration in change brief."""
    brief = {
        "title": "Configure Okta as identity provider",
        "description": "Set up IDP configuration for Azure AD integration",
        "acceptance_criteria": ["Identity provider metadata configured"],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.idp_configuration_specified is True
    assert result.protocol_support_defined is False


def test_user_provisioning_detected():
    """Detect user provisioning in change brief."""
    brief = {
        "description": "Implement SCIM 2.0 endpoint for automatic user provisioning",
        "acceptance_criteria": ["User provisioning configured", "Deprovisioning tested"],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.user_provisioning_addressed is True
    assert result.protocol_support_defined is False


def test_jit_provisioning_detected():
    """Detect just-in-time provisioning in change brief."""
    brief = {
        "description": "Enable just-in-time provisioning for new users",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.user_provisioning_addressed is True


def test_role_mapping_detected():
    """Detect role and attribute mapping in change brief."""
    brief = {
        "title": "Configure role mapping for SSO",
        "description": "Map SAML attributes to application roles with claim mapping",
        "acceptance_criteria": ["Attribute mapping configured", "Profile mapping tested"],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.role_mapping_configured is True
    assert result.protocol_support_defined is False


def test_session_management_detected():
    """Detect SSO session management in change brief."""
    brief = {
        "description": "Implement SSO session management with session timeout handling",
        "acceptance_criteria": ["Session expiration configured", "Session revocation tested"],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.session_management_implemented is True
    assert result.protocol_support_defined is False


def test_logout_handling_detected():
    """Detect single logout (SLO) handling in change brief."""
    brief = {
        "title": "Implement single logout",
        "description": "Add SLO support for federated logout across all services",
        "acceptance_criteria": ["Logout endpoint implemented", "Global logout tested"],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.logout_handling_implemented is True
    assert result.protocol_support_defined is False


def test_group_sync_detected():
    """Detect group synchronization in change brief."""
    brief = {
        "description": "Implement group sync to synchronize team memberships from IDP",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.group_sync_configured is True


def test_multi_tenant_isolation_detected():
    """Detect multi-tenant SSO isolation in change brief."""
    brief = {
        "description": "Implement multi-tenancy with tenant-specific IDP configuration",
        "acceptance_criteria": ["Tenant isolation verified", "Per-tenant SSO configured"],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.multi_tenant_isolation_considered is True


def test_security_measures_detected():
    """Detect security measures in change brief."""
    brief = {
        "description": "Validate SAML assertion signatures with X509 certificate verification",
        "acceptance_criteria": ["Signature verification implemented", "Certificate validation tested"],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.security_measures_included is True


def test_error_handling_detected():
    """Detect SSO error handling in change brief."""
    brief = {
        "description": "Implement SSO error handling with authentication fallback",
        "acceptance_criteria": ["Handle SAML errors gracefully", "Fallback mechanism tested"],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.error_handling_implemented is True


def test_sso_testing_detected():
    """Detect SSO testing in change brief."""
    brief = {
        "title": "Add SSO integration tests",
        "description": "Create integration tests with mock IDP for SAML testing",
        "acceptance_criteria": ["SSO tests passing", "Mock identity provider configured"],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.sso_testing_planned is True


def test_comprehensive_sso_all_aspects_detected():
    """Test comprehensive SSO integration with all aspects present."""
    brief = {
        "title": "Complete SSO integration implementation",
        "description": (
            "Implement comprehensive SSO with SAML 2.0 protocol support. "
            "Configure Okta as identity provider with SCIM provisioning. "
            "Implement role mapping and attribute mapping for user profiles. "
            "Add SSO session management with single logout (SLO) support. "
            "Enable group synchronization and multi-tenant isolation. "
            "Include signature verification for security. "
            "Implement SSO error handling with fallback mechanism."
        ),
        "acceptance_criteria": [
            "SAML 2.0 integration complete",
            "Identity provider configuration verified",
            "User provisioning working with SCIM",
            "Role and attribute mapping configured",
            "Session management implemented",
            "Logout handling tested",
            "Group sync operational",
            "Tenant isolation verified",
            "Certificate validation implemented",
            "Error handling tested",
            "SSO integration tests passing",
        ],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.protocol_support_defined is True
    assert result.idp_configuration_specified is True
    assert result.user_provisioning_addressed is True
    assert result.role_mapping_configured is True
    assert result.session_management_implemented is True
    assert result.logout_handling_implemented is True
    assert result.group_sync_configured is True
    assert result.multi_tenant_isolation_considered is True
    assert result.security_measures_included is True
    assert result.error_handling_implemented is True
    assert result.sso_testing_planned is True


def test_sp_initiated_flow():
    """Test SP-initiated SSO flow detection."""
    brief = {
        "description": "Implement SP-initiated SAML flow with authentication request",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.protocol_support_defined is True


def test_idp_initiated_flow():
    """Test IdP-initiated SSO flow detection."""
    brief = {
        "description": "Support IdP-initiated login flow for SAML integration",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.protocol_support_defined is True
    assert result.idp_configuration_specified is True


def test_force_reauthentication():
    """Test force re-authentication detection."""
    brief = {
        "description": "Implement force authentication for sensitive operations in SSO",
    }

    result = analyze_sso_integration_readiness(brief)

    # Should detect session management due to authentication handling
    assert result.session_management_implemented is False  # Not explicitly mentioned as session mgmt


def test_invalid_change_brief_non_mapping():
    """Test with invalid input (non-mapping type)."""
    result = analyze_sso_integration_readiness("not a mapping")

    assert isinstance(result, SsoIntegrationReadiness)
    assert result.protocol_support_defined is False
    assert result.idp_configuration_specified is False


def test_invalid_change_brief_none():
    """Test with None input."""
    result = analyze_sso_integration_readiness(None)

    assert isinstance(result, SsoIntegrationReadiness)
    assert result.protocol_support_defined is False


def test_invalid_change_brief_list():
    """Test with list input instead of mapping."""
    result = analyze_sso_integration_readiness([{"key": "value"}])

    assert isinstance(result, SsoIntegrationReadiness)
    assert result.protocol_support_defined is False


def test_change_brief_with_nested_acceptance_criteria():
    """Test extraction from nested acceptance criteria structure."""
    brief = {
        "title": "SSO improvements",
        "acceptance_criteria": [
            "Implement SAML support",
            "Configure identity provider metadata",
            "Add user provisioning with JIT",
            "Implement single logout",
        ],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.protocol_support_defined is True
    assert result.idp_configuration_specified is True
    assert result.user_provisioning_addressed is True
    assert result.logout_handling_implemented is True


def test_validation_commands_checked():
    """Test that validation commands are included in analysis."""
    brief = {
        "title": "Setup SSO",
        "validation_command": "pytest tests/test_sso_integration.py tests/test_saml_authentication.py",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.sso_testing_planned is True
    assert result.protocol_support_defined is True


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    brief = {
        "description": "SAML 2.0 with IDP CONFIGURATION and SESSION MANAGEMENT",
        "acceptance_criteria": ["SSO TESTING completed", "ROLE MAPPING configured"],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.protocol_support_defined is True
    assert result.idp_configuration_specified is True
    assert result.session_management_implemented is True
    assert result.sso_testing_planned is True
    assert result.role_mapping_configured is True


def test_alternative_idp_providers():
    """Test alternative IDP provider names are recognized."""
    brief = {
        "description": "Configure Auth0 for authentication and Google Workspace for SSO",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.idp_configuration_specified is True


def test_alternative_provisioning_terminology():
    """Test alternative provisioning terminology is recognized."""
    brief = {
        "description": "Auto-provision users on first login with automatic user creation",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.user_provisioning_addressed is True


def test_alternative_logout_terminology():
    """Test alternative logout terminology is recognized."""
    brief = {
        "description": "Implement federated logout for global sign-out",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.logout_handling_implemented is True


def test_to_dict_method():
    """Test SsoIntegrationReadiness.to_dict() serialization."""
    readiness = SsoIntegrationReadiness(
        protocol_support_defined=True,
        idp_configuration_specified=True,
        user_provisioning_addressed=False,
        role_mapping_configured=True,
        session_management_implemented=False,
        logout_handling_implemented=True,
        group_sync_configured=False,
        multi_tenant_isolation_considered=False,
        security_measures_included=True,
        error_handling_implemented=False,
        sso_testing_planned=True,
    )

    result = readiness.to_dict()

    assert isinstance(result, dict)
    assert result["protocol_support_defined"] is True
    assert result["idp_configuration_specified"] is True
    assert result["user_provisioning_addressed"] is False
    assert result["role_mapping_configured"] is True
    assert result["session_management_implemented"] is False
    assert result["logout_handling_implemented"] is True
    assert result["group_sync_configured"] is False
    assert result["multi_tenant_isolation_considered"] is False
    assert result["security_measures_included"] is True
    assert result["error_handling_implemented"] is False
    assert result["sso_testing_planned"] is True


def test_multiple_fields_in_different_sections():
    """Test detection across multiple brief sections."""
    brief = {
        "title": "SSO setup",
        "description": "Implement SAML protocol",
        "acceptance_criteria": ["Configure Okta"],
        "requirements": ["User provisioning via SCIM"],
        "notes": ["Add role mapping"],
        "risks": ["No session timeout handling"],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.protocol_support_defined is True
    assert result.idp_configuration_specified is True
    assert result.user_provisioning_addressed is True
    assert result.role_mapping_configured is True
    assert result.session_management_implemented is True


def test_validation_commands_as_list():
    """Test validation_commands as list."""
    brief = {
        "validation_commands": [
            "test_sso_integration.py",
            "verify_saml_authentication.py",
        ],
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.sso_testing_planned is True
    assert result.protocol_support_defined is True


def test_no_false_positives_similar_words():
    """Test that similar but different words don't trigger false positives."""
    brief = {
        "description": "Add authentication to the API. Configure session cookies.",
    }

    result = analyze_sso_integration_readiness(brief)

    # "authentication" and "session" alone shouldn't trigger SSO-specific patterns
    assert result.protocol_support_defined is False
    assert result.session_management_implemented is False


def test_dataclass_immutability():
    """Test that SsoIntegrationReadiness is frozen/immutable."""
    readiness = SsoIntegrationReadiness(protocol_support_defined=True)

    with pytest.raises(AttributeError):
        readiness.protocol_support_defined = False


def test_scim_endpoint_terminology():
    """Test SCIM endpoint specific terminology."""
    brief = {
        "description": "Implement SCIM endpoint for user lifecycle management",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.user_provisioning_addressed is True


def test_claim_mapping_terminology():
    """Test claim mapping as role mapping."""
    brief = {
        "description": "Configure OIDC claim mapping for user attributes",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.role_mapping_configured is True


def test_session_timeout_terminology():
    """Test session timeout as session management."""
    brief = {
        "description": "Configure SSO session timeout and expiration policies",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.session_management_implemented is True


def test_tenant_specific_sso():
    """Test tenant-specific SSO configuration."""
    brief = {
        "description": "Enable per-tenant IDP configuration for multi-tenant system",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.multi_tenant_isolation_considered is True
    assert result.idp_configuration_specified is True


def test_metadata_validation_as_security():
    """Test metadata validation as security measure."""
    brief = {
        "description": "Validate IDP metadata and verify SSL certificates",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.security_measures_included is True
    assert result.idp_configuration_specified is True


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    brief = {
        "acceptance_criteria": "Implement SAML integration and configure IDP metadata",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.protocol_support_defined is True
    assert result.idp_configuration_specified is True


def test_oauth_integration_terminology():
    """Test OAuth 2.0 integration terminology."""
    brief = {
        "description": "Implement OAuth 2.0 integration for SSO protocol support",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.protocol_support_defined is True


def test_azure_ad_provider():
    """Test Azure AD as identity provider."""
    brief = {
        "description": "Configure Azure AD integration for enterprise SSO",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.idp_configuration_specified is True


def test_group_membership_sync():
    """Test group membership synchronization."""
    brief = {
        "description": "Synchronize group membership from identity provider",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.group_sync_configured is True


def test_authentication_fallback():
    """Test authentication fallback as error handling."""
    brief = {
        "description": "Implement authentication fallback when SSO fails",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.error_handling_implemented is True


def test_mock_idp_testing():
    """Test mock IDP for testing."""
    brief = {
        "description": "Set up mock identity provider for testing SSO flows",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.sso_testing_planned is True
    assert result.idp_configuration_specified is True


def test_team_sync_as_group_sync():
    """Test team sync as group synchronization."""
    brief = {
        "description": "Implement team sync from IDP to application",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.group_sync_configured is True


def test_session_revocation():
    """Test session revocation as session management."""
    brief = {
        "description": "Add session revocation for SSO sessions",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.session_management_implemented is True


def test_profile_mapping():
    """Test profile mapping as role mapping."""
    brief = {
        "description": "Configure profile mapping from SAML assertions",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.role_mapping_configured is True


def test_pingidentity_provider():
    """Test PingIdentity as identity provider."""
    brief = {
        "description": "Integrate with PingFederate for SSO",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.idp_configuration_specified is True


def test_onelogin_provider():
    """Test OneLogin as identity provider."""
    brief = {
        "description": "Configure OneLogin integration",
    }

    result = analyze_sso_integration_readiness(brief)

    assert result.idp_configuration_specified is True
