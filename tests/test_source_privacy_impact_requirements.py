import json

from blueprint.domain.models import SourceBrief
from blueprint.source_privacy_impact_requirements import (
    SourcePrivacyImpactRequirement,
    SourcePrivacyImpactRequirementsReport,
    build_source_privacy_impact_requirements,
    derive_source_privacy_impact_requirements,
    extract_source_privacy_impact_requirements,
    generate_source_privacy_impact_requirements,
    source_privacy_impact_requirements_to_dict,
    source_privacy_impact_requirements_to_dicts,
    source_privacy_impact_requirements_to_markdown,
    summarize_source_privacy_impact_requirements,
)


def test_extracts_complete_privacy_impact_requirements():
    result = build_source_privacy_impact_requirements(
        _source_brief(
            summary=(
                "Collect personal data categories including name and email. "
                "Legal basis is consent with opt-in and withdrawal. "
                "Data subject rights include DSAR, erasure, and portability. "
                "Use data minimization and collect only minimum necessary fields."
            ),
            source_payload={
                "acceptance_criteria": [
                    "Retention reference is the customer PII retention schedule with delete after 30 days.",
                    "Third-party sharing with SendGrid requires processor and subprocessor review.",
                    "DPA and DPIA evidence must be attached before launch.",
                    "Privacy owner is Privacy Platform team.",
                ]
            },
        )
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert isinstance(result, SourcePrivacyImpactRequirementsReport)
    assert all(isinstance(record, SourcePrivacyImpactRequirement) for record in result.records)
    assert {
        "data_categories",
        "legal_basis_consent",
        "data_subject_rights",
        "minimization",
        "retention_reference",
        "third_party_sharing",
        "dpa_dpia_evidence",
        "owner",
    } <= set(by_type)
    assert by_type["owner"].owner == "Privacy Platform team"
    assert result.summary["owners"] == ["Privacy Platform team"]
    assert result.summary["unresolved_gaps"] == []


def test_plain_text_and_sourcebrief_inputs_are_equivalent():
    text = (
        "Personal data categories are name and email. Legal basis is consent. "
        "Data subject rights include DSAR and erasure. Data minimization collects only necessary fields. "
        "Retention follows the PII retention schedule. Third-party sharing uses a processor DPA. "
        "DPIA evidence is required. Privacy owner is Privacy Platform team."
    )
    text_result = build_source_privacy_impact_requirements(text)
    model_result = summarize_source_privacy_impact_requirements(SourceBrief.model_validate(_source_brief(summary=text)))

    strip_fields = {"source_field", "evidence"}
    assert [
        {key: value for key, value in item.to_dict().items() if key not in strip_fields}
        for item in text_result.records
    ] == [
        {key: value for key, value in item.to_dict().items() if key not in strip_fields}
        for item in model_result.records
    ]
    assert text_result.summary["requirement_types"] == model_result.summary["requirement_types"]


def test_partial_privacy_impact_requirements_report_gaps():
    result = build_source_privacy_impact_requirements(
        {
            "id": "privacy-partial",
            "title": "Profile enrichment",
            "requirements": [
                "Collect personal data categories for phone number and location data.",
                "Consent is required, but retention is TBD.",
            ],
        }
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert {"data_categories", "legal_basis_consent", "retention_reference", "unresolved_gap"} <= set(by_type)
    assert result.source_brief_id == "privacy-partial"
    assert by_type["unresolved_gap"].unresolved_gaps
    assert any("DSAR" in gap or "Confirm DSAR" in gap for gap in result.summary["unresolved_gaps"])


def test_absent_privacy_impact_requirements_and_serialization_are_stable():
    result = build_source_privacy_impact_requirements(
        {"id": "copy-only", "title": "Copy edit", "summary": "Update onboarding labels."}
    )
    payload = source_privacy_impact_requirements_to_dict(result)
    markdown = source_privacy_impact_requirements_to_markdown(result)

    assert result.records == ()
    assert result.summary["requirement_count"] == 0
    assert result.summary["unresolved_gaps"] == []
    assert json.loads(json.dumps(payload)) == payload
    assert result.to_dicts() == []
    assert source_privacy_impact_requirements_to_dicts(result) == []
    assert extract_source_privacy_impact_requirements(result) == ()
    assert derive_source_privacy_impact_requirements(result).to_dict() == result.to_dict()
    assert generate_source_privacy_impact_requirements(result).to_dict() == result.to_dict()
    assert markdown.startswith("# Source Privacy Impact Requirements Report: copy-only")
    assert list(payload) == ["source_brief_id", "title", "summary", "requirements", "records"]


def _source_brief(source_id="privacy-source", summary="Privacy impact requirements.", source_payload=None):
    return {
        "id": source_id,
        "title": "Privacy impact",
        "domain": "platform",
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }
