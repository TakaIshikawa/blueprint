"""Tests for data privacy impact analyzer."""

import pytest

from blueprint.task_data_privacy_impact import (
    DataPrivacyImpact,
    analyze_data_privacy_impact,
)


def test_empty_task_data_returns_all_false():
    """Empty task data should return all fields as False."""
    result = analyze_data_privacy_impact({})

    assert isinstance(result, DataPrivacyImpact)
    assert result.pii_collection_identified is False
    assert result.data_sharing_detected is False
    assert result.consent_requirements_present is False
    assert result.retention_policy_defined is False
    assert result.cross_border_transfers_flagged is False
    assert result.gdpr_compliance_addressed is False
    assert result.ccpa_compliance_addressed is False
    assert result.data_minimization_practiced is False
    assert result.right_to_deletion_implemented is False
    assert result.breach_notification_planned is False
    assert result.children_data_handled is False
    assert result.sensitive_categories_processed is False
    assert result.anonymization_applied is False
    assert result.pseudonymization_applied is False


def test_pii_collection_detected():
    """Detect PII collection in task data."""
    task = {
        "title": "Implement user registration",
        "description": "Collect PII including name, email, and phone number from users",
    }

    result = analyze_data_privacy_impact(task)

    assert result.pii_collection_identified is True
    assert result.data_sharing_detected is False


def test_data_sharing_detected():
    """Detect data sharing in task data."""
    task = {
        "description": "Share user data with third party analytics provider",
        "acceptance_criteria": ["Third-party integration enabled"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.data_sharing_detected is True
    assert result.pii_collection_identified is False


def test_consent_requirements_detected():
    """Detect consent requirements in task data."""
    task = {
        "title": "Add consent mechanism",
        "description": "Implement user consent flow with opt-in for marketing emails",
        "acceptance_criteria": ["Consent banner displayed", "User preferences saved"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.consent_requirements_present is True
    assert result.pii_collection_identified is False


def test_retention_policy_detected():
    """Detect retention policy in task data."""
    task = {
        "description": "Define data retention policy with automatic deletion after 90 days",
        "acceptance_criteria": ["Retention schedule documented"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.retention_policy_defined is True
    assert result.pii_collection_identified is False


def test_cross_border_transfers_detected():
    """Detect cross-border transfers in task data."""
    task = {
        "description": "Enable international data transfer with standard contractual clauses",
        "acceptance_criteria": ["Cross-border transfer compliance verified"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.cross_border_transfers_flagged is True
    assert result.pii_collection_identified is False


def test_gdpr_compliance_detected():
    """Detect GDPR compliance in task data."""
    task = {
        "title": "GDPR compliance implementation",
        "description": "Ensure GDPR compliance with privacy by design principles",
        "acceptance_criteria": ["GDPR requirements met", "Data protection impact assessment completed"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.gdpr_compliance_addressed is True
    assert result.pii_collection_identified is False


def test_ccpa_compliance_detected():
    """Detect CCPA compliance in task data."""
    task = {
        "description": "Implement CCPA compliance with do not sell option",
        "acceptance_criteria": ["CCPA compliant", "Consumer privacy rights honored"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.ccpa_compliance_addressed is True
    assert result.pii_collection_identified is False


def test_data_minimization_detected():
    """Detect data minimization in task data."""
    task = {
        "description": "Apply data minimization principles to collect PII with minimal data collection",
        "acceptance_criteria": ["Only necessary data collected"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.data_minimization_practiced is True
    assert result.pii_collection_identified is True


def test_right_to_deletion_detected():
    """Detect right to deletion in task data."""
    task = {
        "title": "Implement right to deletion",
        "description": "Allow users to request data deletion and erasure",
        "acceptance_criteria": ["User data removal implemented", "Deletion request flow tested"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.right_to_deletion_implemented is True
    assert result.pii_collection_identified is False


def test_breach_notification_detected():
    """Detect breach notification in task data."""
    task = {
        "description": "Establish breach notification protocol for security incidents",
        "acceptance_criteria": ["Incident response plan documented"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.breach_notification_planned is True
    assert result.pii_collection_identified is False


def test_children_data_detected():
    """Detect children's data handling in task data."""
    task = {
        "title": "Add parental consent",
        "description": "Implement COPPA compliance for children's data with age verification",
        "acceptance_criteria": ["Parental consent obtained for users under 13"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.children_data_handled is True
    assert result.pii_collection_identified is False


def test_sensitive_categories_detected():
    """Detect sensitive data categories in task data."""
    task = {
        "description": "Process health data and medical records for patient portal",
        "acceptance_criteria": ["Sensitive personal data protected"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.sensitive_categories_processed is True
    assert result.pii_collection_identified is False


def test_anonymization_detected():
    """Detect anonymization in task data."""
    task = {
        "description": "Anonymize user data by removing personal identifiers",
        "acceptance_criteria": ["Data anonymization applied", "De-identification tested"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.anonymization_applied is True
    assert result.pii_collection_identified is False


def test_pseudonymization_detected():
    """Detect pseudonymization in task data."""
    task = {
        "description": "Apply pseudonymization to tokenize user identifiers",
        "acceptance_criteria": ["Hashed identifiers used"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.pseudonymization_applied is True
    assert result.pii_collection_identified is False


def test_comprehensive_privacy_all_detected():
    """Test comprehensive privacy with all aspects present."""
    task = {
        "title": "Complete privacy-compliant user management system",
        "description": (
            "Build user management with PII collection and data sharing to third parties. "
            "Implement user consent mechanism with opt-in and data retention policy. "
            "Handle cross-border data transfers with GDPR and CCPA compliance. "
            "Apply data minimization and support right to deletion. "
            "Establish breach notification protocol. Handle children's data with parental consent. "
            "Process sensitive health data with anonymization and hashed identifiers for pseudonymization."
        ),
        "acceptance_criteria": [
            "PII collection documented",
            "Third-party data sharing compliant",
            "User consent obtained",
            "Retention policy defined",
            "Cross-border transfers compliant",
            "GDPR requirements met",
            "CCPA compliance verified",
            "Data minimization applied",
            "Right to deletion implemented",
            "Breach notification protocol established",
            "Parental consent for children under 13",
            "Sensitive categories protected",
            "Anonymization tested",
            "Pseudonymization applied",
        ],
    }

    result = analyze_data_privacy_impact(task)

    assert result.pii_collection_identified is True
    assert result.data_sharing_detected is True
    assert result.consent_requirements_present is True
    assert result.retention_policy_defined is True
    assert result.cross_border_transfers_flagged is True
    assert result.gdpr_compliance_addressed is True
    assert result.ccpa_compliance_addressed is True
    assert result.data_minimization_practiced is True
    assert result.right_to_deletion_implemented is True
    assert result.breach_notification_planned is True
    assert result.children_data_handled is True
    assert result.sensitive_categories_processed is True
    assert result.anonymization_applied is True
    assert result.pseudonymization_applied is True


def test_privacy_risk_score_no_sensitive_data():
    """Test privacy risk score with no sensitive data processing."""
    task = {
        "description": "Implement public API documentation generator",
    }

    result = analyze_data_privacy_impact(task)

    # No sensitive data = high baseline score
    assert result.privacy_risk_score == 0.95


def test_privacy_risk_score_sensitive_no_protection():
    """Test privacy risk score with sensitive data but no protections."""
    task = {
        "description": "Collect PII and share with third parties",
    }

    result = analyze_data_privacy_impact(task)

    # Sensitive data without protection = low score
    assert result.privacy_risk_score < 0.4


def test_privacy_risk_score_sensitive_with_protection():
    """Test privacy risk score with sensitive data and comprehensive protections."""
    task = {
        "description": (
            "Collect PII with user consent, data minimization, and right to deletion. "
            "GDPR and CCPA compliant with retention policy and breach notification."
        ),
    }

    result = analyze_data_privacy_impact(task)

    # Sensitive data with comprehensive protection = high score
    assert result.privacy_risk_score > 0.8


def test_privacy_risk_score_high_sensitivity_partial_protection():
    """Test privacy risk score with high sensitivity and partial protections."""
    task = {
        "description": (
            "Collect PII, share with third parties, process children's data, "
            "and handle sensitive health data. Implement user consent."
        ),
    }

    result = analyze_data_privacy_impact(task)

    # High sensitivity with only partial protection = low to moderate score
    assert 0.15 < result.privacy_risk_score < 0.4


def test_invalid_task_data_non_mapping():
    """Test with invalid input (non-mapping type)."""
    result = analyze_data_privacy_impact("not a mapping")

    assert isinstance(result, DataPrivacyImpact)
    assert result.pii_collection_identified is False
    assert result.privacy_risk_score == 0.95


def test_invalid_task_data_none():
    """Test with None input."""
    result = analyze_data_privacy_impact(None)

    assert isinstance(result, DataPrivacyImpact)
    assert result.pii_collection_identified is False


def test_invalid_task_data_list():
    """Test with list input instead of mapping."""
    result = analyze_data_privacy_impact([{"key": "value"}])

    assert isinstance(result, DataPrivacyImpact)
    assert result.pii_collection_identified is False


def test_task_data_with_nested_acceptance_criteria():
    """Test extraction from nested acceptance criteria structure."""
    task = {
        "title": "Privacy improvements",
        "acceptance_criteria": [
            "Collect PII with user consent",
            "Implement GDPR compliance",
            "Apply data minimization",
        ],
    }

    result = analyze_data_privacy_impact(task)

    assert result.pii_collection_identified is True
    assert result.consent_requirements_present is True
    assert result.gdpr_compliance_addressed is True
    assert result.data_minimization_practiced is True


def test_validation_commands_checked():
    """Test that validation commands are included in analysis."""
    task = {
        "title": "Privacy testing",
        "validation_command": "pytest tests/test_pii_collection.py tests/test_consent.py",
    }

    result = analyze_data_privacy_impact(task)

    assert result.pii_collection_identified is True
    assert result.consent_requirements_present is True


def test_case_insensitive_matching():
    """Test that pattern matching is case-insensitive."""
    task = {
        "description": "COLLECT PII WITH USER CONSENT AND GDPR COMPLIANCE",
        "acceptance_criteria": ["DATA MINIMIZATION and RIGHT TO DELETION"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.pii_collection_identified is True
    assert result.consent_requirements_present is True
    assert result.gdpr_compliance_addressed is True
    assert result.data_minimization_practiced is True
    assert result.right_to_deletion_implemented is True


def test_alternative_terminology_pii():
    """Test alternative PII terminology is recognized."""
    task = {
        "description": "Store personal information including user email addresses",
    }

    result = analyze_data_privacy_impact(task)

    assert result.pii_collection_identified is True


def test_alternative_terminology_consent():
    """Test alternative consent terminology is recognized."""
    task = {
        "description": "Add opt-in mechanism for marketing preferences",
    }

    result = analyze_data_privacy_impact(task)

    assert result.consent_requirements_present is True


def test_alternative_terminology_deletion():
    """Test alternative deletion terminology is recognized."""
    task = {
        "description": "Implement right to be forgotten for user accounts",
    }

    result = analyze_data_privacy_impact(task)

    assert result.right_to_deletion_implemented is True


def test_alternative_terminology_anonymization():
    """Test alternative anonymization terminology is recognized."""
    task = {
        "description": "Apply data masking to redact personal identifiers",
    }

    result = analyze_data_privacy_impact(task)

    assert result.anonymization_applied is True


def test_alternative_terminology_pseudonymization():
    """Test alternative pseudonymization terminology is recognized."""
    task = {
        "description": "Use hashed identifiers for user tracking",
    }

    result = analyze_data_privacy_impact(task)

    assert result.pseudonymization_applied is True


def test_to_dict_method():
    """Test DataPrivacyImpact.to_dict() serialization."""
    impact = DataPrivacyImpact(
        pii_collection_identified=True,
        data_sharing_detected=True,
        consent_requirements_present=True,
        retention_policy_defined=False,
        cross_border_transfers_flagged=False,
        gdpr_compliance_addressed=True,
        ccpa_compliance_addressed=False,
        data_minimization_practiced=True,
        right_to_deletion_implemented=True,
        breach_notification_planned=False,
        children_data_handled=False,
        sensitive_categories_processed=False,
        anonymization_applied=False,
        pseudonymization_applied=False,
    )

    result = impact.to_dict()

    assert isinstance(result, dict)
    assert result["pii_collection_identified"] is True
    assert result["data_sharing_detected"] is True
    assert result["consent_requirements_present"] is True
    assert result["retention_policy_defined"] is False
    assert result["cross_border_transfers_flagged"] is False
    assert result["gdpr_compliance_addressed"] is True
    assert result["ccpa_compliance_addressed"] is False
    assert result["data_minimization_practiced"] is True
    assert result["right_to_deletion_implemented"] is True
    assert result["breach_notification_planned"] is False
    assert result["children_data_handled"] is False
    assert result["sensitive_categories_processed"] is False
    assert result["anonymization_applied"] is False
    assert result["pseudonymization_applied"] is False
    assert "privacy_risk_score" in result
    assert isinstance(result["privacy_risk_score"], float)


def test_multiple_fields_in_different_sections():
    """Test detection across multiple task sections."""
    task = {
        "title": "Privacy compliance",
        "description": "Collect PII",
        "acceptance_criteria": ["User consent required"],
        "requirements": ["GDPR compliance"],
        "notes": ["Data minimization applied"],
        "risks": ["No retention policy"],
    }

    result = analyze_data_privacy_impact(task)

    assert result.pii_collection_identified is True
    assert result.consent_requirements_present is True
    assert result.gdpr_compliance_addressed is True
    assert result.data_minimization_practiced is True
    assert result.retention_policy_defined is True


def test_validation_commands_as_list():
    """Test validation_commands as list."""
    task = {
        "validation_commands": [
            "pytest tests/test_pii_collection.py",
            "pytest tests/test_gdpr.py",
        ],
    }

    result = analyze_data_privacy_impact(task)

    assert result.pii_collection_identified is True
    assert result.gdpr_compliance_addressed is True


def test_no_false_positives_similar_words():
    """Test that similar but different words don't trigger false positives."""
    task = {
        "description": "Collect logs for debugging. Delete old files.",
    }

    result = analyze_data_privacy_impact(task)

    # "collect" and "delete" alone shouldn't trigger PII or deletion rights
    assert result.pii_collection_identified is False
    assert result.right_to_deletion_implemented is False


def test_dataclass_immutability():
    """Test that DataPrivacyImpact is frozen/immutable."""
    impact = DataPrivacyImpact(pii_collection_identified=True)

    with pytest.raises(AttributeError):
        impact.pii_collection_identified = False


def test_gdpr_article_references():
    """Test GDPR article references are recognized."""
    task = {
        "description": "Implement Article 17 right to erasure",
    }

    result = analyze_data_privacy_impact(task)

    assert result.gdpr_compliance_addressed is True


def test_ccpa_terminology_variations():
    """Test CCPA terminology variations are recognized."""
    task = {
        "description": "Implement California Consumer Privacy Act requirements",
    }

    result = analyze_data_privacy_impact(task)

    assert result.ccpa_compliance_addressed is True


def test_children_age_variations():
    """Test various age thresholds for children's data."""
    task_under_13 = {
        "description": "Verify users are not under 13",
    }
    task_under_16 = {
        "description": "Age verification for users under 16",
    }
    task_under_18 = {
        "description": "Minors under 18 require parental consent",
    }

    assert analyze_data_privacy_impact(task_under_13).children_data_handled is True
    assert analyze_data_privacy_impact(task_under_16).children_data_handled is True
    assert analyze_data_privacy_impact(task_under_18).children_data_handled is True


def test_sensitive_health_data():
    """Test health data detection."""
    task = {
        "description": "Store medical records for patient care",
    }

    result = analyze_data_privacy_impact(task)

    assert result.sensitive_categories_processed is True


def test_sensitive_biometric_data():
    """Test biometric data detection."""
    task = {
        "description": "Collect biometric data for authentication",
    }

    result = analyze_data_privacy_impact(task)

    assert result.sensitive_categories_processed is True


def test_sensitive_financial_data():
    """Test financial data detection."""
    task = {
        "description": "Process financial data for payment transactions",
    }

    result = analyze_data_privacy_impact(task)

    assert result.sensitive_categories_processed is True


def test_cross_border_eu_specific():
    """Test EU-specific cross-border transfer terminology."""
    task = {
        "description": "Handle EU data transfer with adequacy decision",
    }

    result = analyze_data_privacy_impact(task)

    assert result.cross_border_transfers_flagged is True


def test_retention_alternative_terminology():
    """Test alternative retention terminology."""
    task = {
        "description": "Define data lifecycle with purge schedule",
    }

    result = analyze_data_privacy_impact(task)

    assert result.retention_policy_defined is True


def test_breach_incident_response():
    """Test incident response terminology for breach notification."""
    task = {
        "description": "Create security incident notification procedure",
    }

    result = analyze_data_privacy_impact(task)

    assert result.breach_notification_planned is True


def test_string_field_instead_of_list():
    """Test that string fields in list-based positions are handled."""
    task = {
        "acceptance_criteria": "Collect PII with user consent and GDPR compliance",
    }

    result = analyze_data_privacy_impact(task)

    assert result.pii_collection_identified is True
    assert result.consent_requirements_present is True
    assert result.gdpr_compliance_addressed is True


def test_privacy_risk_score_with_critical_protections():
    """Test that critical protections (consent, minimization, deletion) boost score."""
    task = {
        "description": (
            "Collect PII with user consent, data minimization, and right to deletion"
        ),
    }

    result = analyze_data_privacy_impact(task)

    # Should have high score due to all critical protections
    assert result.privacy_risk_score > 0.9


def test_privacy_risk_score_missing_critical_protections():
    """Test score when sensitive data lacks critical protections."""
    task = {
        "description": "Collect PII with GDPR compliance",
    }

    result = analyze_data_privacy_impact(task)

    # Missing critical protections = lower score
    assert 0.2 < result.privacy_risk_score < 0.5


def test_edge_case_children_apostrophe_variations():
    """Test children's data with apostrophe variations."""
    task_with_apostrophe = {
        "description": "Handle children's privacy data",
    }
    task_without_apostrophe = {
        "description": "Handle childrens privacy data",
    }

    assert analyze_data_privacy_impact(task_with_apostrophe).children_data_handled is True
    assert analyze_data_privacy_impact(task_without_apostrophe).children_data_handled is True


def test_edge_case_anonymization_vs_pseudonymization():
    """Test that anonymization and pseudonymization are detected separately."""
    task_anon = {
        "description": "Anonymize data by de-identification",
    }
    task_pseudo = {
        "description": "Pseudonymize data through tokenization",
    }

    result_anon = analyze_data_privacy_impact(task_anon)
    result_pseudo = analyze_data_privacy_impact(task_pseudo)

    assert result_anon.anonymization_applied is True
    assert result_anon.pseudonymization_applied is False

    assert result_pseudo.pseudonymization_applied is True
    assert result_pseudo.anonymization_applied is False
