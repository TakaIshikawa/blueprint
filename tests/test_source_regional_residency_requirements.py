from blueprint.source_regional_residency_requirements import (
    build_source_regional_residency_requirements,
    extract_source_regional_residency_requirements,
    source_regional_residency_requirements_to_dict,
)


def test_complete_residency_text_extracts_all_requirement_signals():
    report = build_source_regional_residency_requirements(
        {
            "id": "source-residency",
            "summary": (
                "Allowed regions are EU and eu-west-1. Data must not transfer to the US. "
                "Cross-border transfers require privacy approval. Backups and replicas stay in-region. "
                "Processing jobs run only within the EU. GDPR is the compliance driver. "
                "Responsible team is privacy platform. Verification evidence includes audit reports."
            ),
        }
    )

    by_signal = {requirement.signal: requirement for requirement in report.requirements}

    assert set(by_signal) == {
        "allowed_regions",
        "prohibited_regions",
        "transfer_rules",
        "backup_placement",
        "processing_location",
        "compliance_driver",
        "owner",
        "verification_evidence",
    }
    assert "EU" in by_signal["allowed_regions"].regions
    assert "US" in by_signal["prohibited_regions"].regions
    assert report.missing_signals == ()
    assert report.weak_signals == ()


def test_partial_residency_text_reports_missing_and_weak_requirements():
    report = build_source_regional_residency_requirements(
        "Customer data has regional needs. Store records only in Canada."
    )

    assert [requirement.signal for requirement in report.requirements] == ["allowed_regions"]
    assert "transfer_rules" in report.missing_signals
    assert "backup_placement" in report.missing_signals
    assert "processing_location" in report.missing_signals
    assert "owner" in report.missing_signals
    assert "verification_evidence" in report.missing_signals
    assert report.weak_signals


def test_absent_residency_text_returns_empty_findings_with_all_gaps():
    report = build_source_regional_residency_requirements(
        {"summary": "Add dashboard sorting and persist the selected column order."}
    )

    assert extract_source_regional_residency_requirements("No regional rules here") == ()
    assert report.requirements == ()
    assert report.records == ()
    assert report.missing_signals == (
        "allowed_regions",
        "prohibited_regions",
        "transfer_rules",
        "backup_placement",
        "processing_location",
        "compliance_driver",
        "owner",
        "verification_evidence",
    )
    assert source_regional_residency_requirements_to_dict(report)["requirements"] == []
