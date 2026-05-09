"""Tests for plan data governance matrix generator."""

import pytest

from blueprint.plan_data_governance import (
    DataGovernanceMatrix,
    generate_data_governance_matrix,
)


def test_empty_plan_data_returns_all_false():
    """Empty plan data should return all fields as False."""
    result = generate_data_governance_matrix({})

    assert isinstance(result, DataGovernanceMatrix)
    assert result.data_classification_defined is False
    assert result.access_controls_established is False
    assert result.data_lineage_tracked is False
    assert result.quality_rules_defined is False
    assert result.retention_policies_set is False
    assert result.policy_coverage_adequate is False
    assert result.automation_level_planned is False
    assert result.audit_trail_maintained is False
    assert result.compliance_alignment_verified is False
    assert result.data_ownership_assigned is False
    assert result.governance_maturity_score == 0.0


def test_data_classification_detected():
    """Detect data classification in plan data."""
    plan = {
        "title": "Data governance implementation",
        "description": "Define data classification with sensitive data and PII data",
    }

    result = generate_data_governance_matrix(plan)

    assert result.data_classification_defined is True
    assert result.governance_maturity_score == 0.1


def test_access_controls_detected():
    """Detect access controls in plan data."""
    plan = {
        "description": "Establish access controls with RBAC policies and permission management",
        "requirements": ["Role-based access", "Access restrictions"],
    }

    result = generate_data_governance_matrix(plan)

    assert result.access_controls_established is True


def test_data_lineage_detected():
    """Detect data lineage in plan data."""
    plan = {
        "description": "Track data lineage and document data provenance",
        "requirements": ["Lineage tracking", "Data transformation tracking"],
    }

    result = generate_data_governance_matrix(plan)

    assert result.data_lineage_tracked is True


def test_quality_rules_detected():
    """Detect quality rules in plan data."""
    plan = {
        "description": "Define quality rules and establish data quality framework",
        "requirements": ["Quality standards", "Validation rules"],
    }

    result = generate_data_governance_matrix(plan)

    assert result.quality_rules_defined is True


def test_retention_policies_detected():
    """Detect retention policies in plan data."""
    plan = {
        "description": "Set retention policies with data lifecycle management",
        "requirements": ["Retention period", "Archive old data"],
    }

    result = generate_data_governance_matrix(plan)

    assert result.retention_policies_set is True


def test_policy_coverage_detected():
    """Detect policy coverage in plan data."""
    plan = {
        "description": "Ensure policy coverage with comprehensive governance policies",
        "requirements": ["Governance framework", "Complete policies"],
    }

    result = generate_data_governance_matrix(plan)

    assert result.policy_coverage_adequate is True


def test_automation_level_detected():
    """Detect automation level in plan data."""
    plan = {
        "description": "Implement governance automation with automated policy enforcement",
        "requirements": ["Automate governance", "Automated compliance"],
    }

    result = generate_data_governance_matrix(plan)

    assert result.automation_level_planned is True


def test_audit_trail_detected():
    """Detect audit trail in plan data."""
    plan = {
        "description": "Maintain audit trail with access logging and change tracking",
        "requirements": ["Audit logging", "Track data access"],
    }

    result = generate_data_governance_matrix(plan)

    assert result.audit_trail_maintained is True


def test_compliance_alignment_detected():
    """Detect compliance alignment in plan data."""
    plan = {
        "description": "Verify compliance alignment with GDPR compliance and regulatory requirements",
        "requirements": ["HIPAA compliance", "Meet compliance requirements"],
    }

    result = generate_data_governance_matrix(plan)

    assert result.compliance_alignment_verified is True


def test_data_ownership_detected():
    """Detect data ownership in plan data."""
    plan = {
        "description": "Assign data ownership with data stewardship and accountability",
        "requirements": ["Define data owners", "Establish ownership"],
    }

    result = generate_data_governance_matrix(plan)

    assert result.data_ownership_assigned is True


def test_comprehensive_governance_all_detected():
    """Test comprehensive data governance with all aspects present."""
    plan = {
        "title": "Complete data governance program",
        "description": (
            "Implement data classification for sensitive data. "
            "Establish access controls with RBAC policies. "
            "Track data lineage across transformations. "
            "Define quality rules for data validation. "
            "Set retention policies for data lifecycle. "
            "Ensure policy coverage with comprehensive framework. "
            "Plan governance automation with automated enforcement. "
            "Maintain audit trail for all data access. "
            "Verify GDPR compliance alignment. "
            "Assign data ownership to stewards."
        ),
        "requirements": [
            "Data classification",
            "Access controls",
            "Data lineage",
            "Quality rules",
            "Retention policies",
            "Policy coverage",
            "Automation",
            "Audit trail",
            "Compliance",
            "Ownership",
        ],
    }

    result = generate_data_governance_matrix(plan)

    assert result.data_classification_defined is True
    assert result.access_controls_established is True
    assert result.data_lineage_tracked is True
    assert result.quality_rules_defined is True
    assert result.retention_policies_set is True
    assert result.policy_coverage_adequate is True
    assert result.automation_level_planned is True
    assert result.audit_trail_maintained is True
    assert result.compliance_alignment_verified is True
    assert result.data_ownership_assigned is True
    assert result.governance_maturity_score == 1.0


def test_invalid_plan_data_none():
    """Test with None input."""
    result = generate_data_governance_matrix(None)  # type: ignore

    assert isinstance(result, DataGovernanceMatrix)
    assert result.governance_maturity_score == 0.0


def test_invalid_plan_data_list():
    """Test with list input instead of mapping."""
    result = generate_data_governance_matrix([{"key": "value"}])  # type: ignore

    assert isinstance(result, DataGovernanceMatrix)
    assert result.governance_maturity_score == 0.0


def test_dataclass_immutability():
    """Test that DataGovernanceMatrix is frozen/immutable."""
    matrix = DataGovernanceMatrix(data_classification_defined=True)

    with pytest.raises(AttributeError):
        matrix.data_classification_defined = False  # type: ignore


def test_to_dict_method():
    """Test DataGovernanceMatrix.to_dict() serialization."""
    matrix = DataGovernanceMatrix(
        data_classification_defined=True,
        access_controls_established=True,
        data_lineage_tracked=False,
        quality_rules_defined=True,
        retention_policies_set=False,
        policy_coverage_adequate=True,
        automation_level_planned=False,
        audit_trail_maintained=True,
        compliance_alignment_verified=False,
        data_ownership_assigned=True,
    )

    result = matrix.to_dict()

    assert isinstance(result, dict)
    assert result["data_classification_defined"] is True
    assert result["access_controls_established"] is True
    assert result["data_lineage_tracked"] is False
    assert result["quality_rules_defined"] is True
    assert result["retention_policies_set"] is False
    assert result["policy_coverage_adequate"] is True
    assert result["automation_level_planned"] is False
    assert result["audit_trail_maintained"] is True
    assert result["compliance_alignment_verified"] is False
    assert result["data_ownership_assigned"] is True
    assert result["governance_maturity_score"] == 0.6


def test_pii_data_pattern():
    """Test PII data classification pattern detection."""
    plan = {
        "description": "Classify PII data appropriately",
    }

    result = generate_data_governance_matrix(plan)

    assert result.data_classification_defined is True


def test_phi_data_pattern():
    """Test PHI data classification pattern detection."""
    plan = {
        "description": "Handle PHI data with proper controls",
    }

    result = generate_data_governance_matrix(plan)

    assert result.data_classification_defined is True


def test_rbac_pattern():
    """Test RBAC pattern detection."""
    plan = {
        "description": "Implement RBAC for access control",
    }

    result = generate_data_governance_matrix(plan)

    assert result.access_controls_established is True


def test_gdpr_compliance_pattern():
    """Test GDPR compliance pattern detection."""
    plan = {
        "description": "Ensure GDPR compliance",
    }

    result = generate_data_governance_matrix(plan)

    assert result.compliance_alignment_verified is True


def test_hipaa_compliance_pattern():
    """Test HIPAA compliance pattern detection."""
    plan = {
        "description": "Maintain HIPAA compliance standards",
    }

    result = generate_data_governance_matrix(plan)

    assert result.compliance_alignment_verified is True


def test_data_stewardship_pattern():
    """Test data stewardship pattern detection."""
    plan = {
        "description": "Establish data stewardship program",
    }

    result = generate_data_governance_matrix(plan)

    assert result.data_ownership_assigned is True


def test_data_anonymization_edge_case():
    """Test data anonymization mentioned."""
    plan = {
        "description": "Classify sensitive data and implement anonymization",
        "requirements": [
            "Data classification scheme",
            "Access controls",
        ],
    }

    result = generate_data_governance_matrix(plan)

    assert result.data_classification_defined is True
    assert result.access_controls_established is True


def test_cross_border_data_edge_case():
    """Test cross-border data governance."""
    plan = {
        "description": "Handle cross-border data with compliance alignment and retention policies",
        "requirements": [
            "Data classification",
            "Regulatory compliance",
        ],
    }

    result = generate_data_governance_matrix(plan)

    assert result.data_classification_defined is True
    assert result.retention_policies_set is True
    assert result.compliance_alignment_verified is True


def test_derived_data_edge_case():
    """Test derived data governance."""
    plan = {
        "description": "Track data lineage for derived data with quality rules",
        "requirements": [
            "Data provenance",
            "Quality validation",
        ],
    }

    result = generate_data_governance_matrix(plan)

    assert result.data_lineage_tracked is True
    assert result.quality_rules_defined is True


def test_partial_maturity():
    """Test partial governance maturity with some aspects covered."""
    plan = {
        "title": "Basic governance",
        "description": "Classify data as confidential",
        "requirements": [
            "Access controls",
            "Audit logging",
        ],
    }

    result = generate_data_governance_matrix(plan)

    assert result.data_classification_defined is True
    assert result.access_controls_established is True
    assert result.audit_trail_maintained is True
    assert result.data_lineage_tracked is False
    assert result.quality_rules_defined is False
    assert result.retention_policies_set is False
    assert result.policy_coverage_adequate is False
    assert result.automation_level_planned is False
    assert result.compliance_alignment_verified is False
    assert result.data_ownership_assigned is False
    assert result.governance_maturity_score == 0.3


def test_multiple_fields_in_different_sections():
    """Test detection across multiple plan data sections."""
    plan = {
        "title": "Governance program",
        "description": "PII data classification",
        "requirements": ["RBAC controls"],
        "notes": ["Track lineage"],
        "objectives": ["GDPR compliance"],
    }

    result = generate_data_governance_matrix(plan)

    assert result.data_classification_defined is True
    assert result.access_controls_established is True
    assert result.data_lineage_tracked is True
    assert result.compliance_alignment_verified is True
