import json

from blueprint.source_privacy_consent_requirements import (
    SourcePrivacyConsentRequirementsReport,
    build_source_privacy_consent_requirements,
    derive_source_privacy_consent_requirements,
    extract_source_privacy_consent_requirements,
    source_privacy_consent_requirements_to_dict,
    source_privacy_consent_requirements_to_dicts,
)


def test_extracts_full_privacy_consent_requirements_in_stable_order():
    result = build_source_privacy_consent_requirements(
        {
            "id": "privacy-consent",
            "source_payload": {
                "body": """
- Signup must collect explicit consent with an unchecked checkbox.
- Users must withdraw consent and revoke processing from settings.
- Consent must be purpose-specific for analytics, marketing, and data sharing.
- GDPR, CPRA, and UK regional policy copy must vary by jurisdiction.
- Store proof of consent with consent receipt, timestamp, policy version, and audit evidence.
"""
            },
        }
    )

    assert isinstance(result, SourcePrivacyConsentRequirementsReport)
    assert [record.requirement_type for record in result.requirements] == [
        "consent_collection",
        "withdrawal",
        "purpose_binding",
        "regional_policy",
        "audit_evidence",
    ]
    assert result.missing_details == ()
    assert result.summary["status"] == "ready_for_privacy_consent_planning"
    assert result.records == result.requirements
    assert result.findings == result.requirements


def test_partial_brief_reports_stable_missing_detail_identifiers_and_labels():
    result = build_source_privacy_consent_requirements(
        {
            "id": "partial-consent",
            "requirements": [
                "Marketing opt-in permission must be captured before promotional email.",
                "Users should opt out and unsubscribe from future processing.",
            ],
        }
    )

    assert [record.requirement_type for record in result.requirements] == [
        "consent_collection",
        "withdrawal",
        "purpose_binding",
    ]
    assert [detail.identifier for detail in result.missing_details] == [
        "regional_policy",
        "audit_evidence",
    ]
    assert [detail.label for detail in result.missing_details] == [
        "Regional policy",
        "Audit evidence",
    ]
    assert result.summary["missing_detail_identifiers"] == ["regional_policy", "audit_evidence"]


def test_case_insensitive_synonyms_unrelated_and_serialization_are_stable():
    consent = "OBTAIN authorization for processing purpose research under GDPR and export consent receipt."
    no_match = build_source_privacy_consent_requirements(
        {"title": "Privacy copy update", "summary": "No consent changes are required for this release."}
    )
    result = extract_source_privacy_consent_requirements(consent)
    payload = source_privacy_consent_requirements_to_dict(result)

    assert [record.requirement_type for record in result.requirements] == [
        "consent_collection",
        "purpose_binding",
        "regional_policy",
        "audit_evidence",
    ]
    assert no_match.requirements == ()
    assert no_match.summary["status"] == "no_privacy_consent_language"
    assert json.loads(json.dumps(payload)) == payload
    assert source_privacy_consent_requirements_to_dicts(result) == payload["requirements"]
    assert derive_source_privacy_consent_requirements(consent).to_dict() == result.to_dict()

