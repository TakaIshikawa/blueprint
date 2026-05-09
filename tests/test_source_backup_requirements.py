import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_backup_requirements import (
    BackupRequirement,
    BackupRequirementsReport,
    build_backup_requirements_report,
    derive_backup_requirements,
    extract_backup_requirements,
    backup_requirements_report_to_dict,
    backup_requirements_to_dicts,
)


def test_extracts_backup_frequency_and_retention_periods_with_evidence():
    result = build_backup_requirements_report(
        _source_brief(
            summary=(
                "Database backups must run hourly with retention period of 30 days. "
                "Critical data requires daily full backups."
            ),
            source_payload={
                "success_criteria": [
                    "Backup frequency is every 1 hour for transaction logs.",
                    "Retain backups for 90 days to meet compliance requirements.",
                ],
            },
        )
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert isinstance(result, BackupRequirementsReport)
    assert all(isinstance(record, BackupRequirement) for record in result.records)
    assert "frequency" in by_type
    assert "retention" in by_type
    assert by_type["frequency"].value in ("hourly", "1 hour", "every 1 hour")
    assert any("hourly" in evidence.lower() for evidence in by_type["frequency"].evidence)
    assert any("retention" in evidence.lower() for evidence in by_type["retention"].evidence)
    assert by_type["frequency"].recommended_follow_up


def test_identifies_encryption_and_geographic_redundancy_requirements():
    result = build_backup_requirements_report(
        {
            "id": "backup-security",
            "title": "Secure backup strategy",
            "summary": "All backups must be encrypted at rest using AES-256.",
            "context": "Cross-region backup replication to secondary geographic location is required.",
            "constraints": [
                "Backup encryption is mandatory for PCI compliance.",
                "Geographic redundancy protects against regional outages.",
                "Remote backup storage in at least two regions.",
            ],
        }
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert "encryption" in by_type
    assert "geo_redundancy" in by_type
    assert by_type["encryption"].confidence >= 0.74
    assert by_type["geo_redundancy"].confidence >= 0.74
    assert any("encrypt" in evidence.lower() for evidence in by_type["encryption"].evidence)
    assert any(
        "region" in evidence.lower() or "geographic" in evidence.lower()
        for evidence in by_type["geo_redundancy"].evidence
    )


def test_completeness_score_reflects_coverage_and_testing():
    comprehensive = build_backup_requirements_report(
        {
            "id": "backup-comprehensive",
            "title": "Comprehensive backup plan",
            "summary": "Hourly incremental backups with daily full backups.",
            "constraints": [
                "Retain backups for 30 days, archives for 7 years.",
                "All backups encrypted at rest.",
                "Cross-region replication to secondary datacenter.",
                "Verify backup integrity after each run.",
                "Quarterly restore testing drills are required.",
                "Point-in-time recovery must be available.",
                "HIPAA compliance for backup retention.",
            ],
        }
    )

    minimal = build_backup_requirements_report(
        {
            "id": "backup-minimal",
            "title": "Basic backup",
            "summary": "Daily backups are needed.",
        }
    )

    assert comprehensive.summary["completeness_score"] > minimal.summary["completeness_score"]
    assert comprehensive.summary["has_rpo_rto_alignment"] is True
    assert comprehensive.summary["has_testing_coverage"] is True
    assert comprehensive.summary["has_automation"] is True
    assert comprehensive.summary["has_security_measures"] is True
    assert minimal.summary["has_testing_coverage"] is False
    assert minimal.summary["has_security_measures"] is False


def test_extracts_all_backup_requirement_types():
    result = build_backup_requirements_report(
        {
            "id": "backup-complete",
            "title": "Complete backup requirements",
            "summary": "Hourly incremental backups with daily full backups.",
            "context": "Point-in-time recovery capability is essential.",
            "constraints": [
                "Backup frequency: every hour for incremental, daily for full.",
                "Retention period of 30 days for regular backups, 7 years for archives.",
                "Full, incremental, and differential backup types supported.",
                "Verify backup integrity with checksums after each backup.",
                "Encryption at rest using AES-256 for all backup data.",
                "Geographic redundancy with cross-region replication.",
                "Quarterly restore testing and disaster recovery drills.",
                "Point-in-time recovery for database transactions.",
                "GDPR and HIPAA compliance for backup retention.",
            ],
        }
    )

    requirement_types = {record.requirement_type for record in result.records}

    assert requirement_types == {
        "frequency",
        "retention",
        "backup_type",
        "verification",
        "encryption",
        "geo_redundancy",
        "restore_testing",
        "point_in_time",
        "compliance",
    }
    assert result.summary["requirement_count"] == 9
    assert result.summary["completeness_score"] > 0.75


def test_edge_case_continuous_backup_and_backup_chains():
    result = build_backup_requirements_report(
        {
            "id": "backup-advanced",
            "title": "Advanced backup scenarios",
            "summary": "Continuous backup with real-time replication.",
            "constraints": [
                "Continuous data protection for zero data loss.",
                "Backup chain management for incremental backup sequences.",
                "Cross-region backup with multi-region geographic distribution.",
            ],
            "success_criteria": [
                "Real-time backup replication maintains RPO of zero.",
                "Incremental backup chains validated for restore integrity.",
            ],
        }
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert "frequency" in by_type
    assert "geo_redundancy" in by_type
    assert any(
        "continuous" in evidence.lower() or "real-time" in evidence.lower()
        for evidence in by_type["frequency"].evidence
    )
    assert any(
        "cross-region" in evidence.lower() or "multi-region" in evidence.lower()
        for evidence in by_type["geo_redundancy"].evidence
    )


def test_duplicate_evidence_is_deduped_and_deterministic():
    result = build_backup_requirements_report(
        {
            "id": "backup-dupes",
            "title": "Backup with duplicates",
            "summary": "Daily backups are required.",
            "constraints": [
                "Daily backups are required.",
                "daily backups are required.",
                "Backups must run every 24 hours.",
                "Encrypted backups are mandatory.",
                "Backup encryption is required.",
            ],
            "metadata": {"backup": "Daily backups are required."},
        }
    )

    frequency = next(
        (record for record in result.records if record.requirement_type == "frequency"),
        None,
    )
    encryption = next(
        (record for record in result.records if record.requirement_type == "encryption"),
        None,
    )

    assert frequency is not None
    assert len(frequency.evidence) <= 4
    assert len(frequency.evidence) == len(
        {_statement(evidence).casefold() for evidence in frequency.evidence}
    )
    assert frequency.evidence == tuple(sorted(frequency.evidence, key=lambda item: item.casefold()))

    assert encryption is not None
    assert len(encryption.evidence) <= 4


def test_mapping_and_sourcebrief_inputs_match_without_mutation():
    source = _source_brief(
        source_id="source-backup",
        summary="Hourly backups with 30-day retention are required.",
        source_payload={
            "constraints": [
                "Backup encryption at rest is mandatory.",
                "Geographic redundancy with cross-region replication.",
            ],
            "success_criteria": ["Restore testing quarterly to validate backup integrity."],
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_backup_requirements_report(source)
    model_result = build_backup_requirements_report(model)
    derived_records = derive_backup_requirements(model)
    extracted_records = extract_backup_requirements(model)
    payload = backup_requirements_report_to_dict(model_result)

    assert source == original
    assert payload == backup_requirements_report_to_dict(mapping_result)
    assert derived_records == model_result.requirements
    assert extracted_records == model_result.requirements
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert backup_requirements_to_dicts(model_result) == payload["requirements"]
    assert backup_requirements_to_dicts(model_result.records) == payload["records"]
    assert list(payload) == ["source_brief_id", "title", "summary", "requirements", "records"]
    assert list(payload["requirements"][0]) == [
        "requirement_type",
        "value",
        "source_field",
        "evidence",
        "confidence",
        "recommended_follow_up",
    ]


def test_empty_and_invalid_inputs_return_no_records():
    empty = build_backup_requirements_report(
        {"id": "empty", "title": "UI update", "summary": "Update button color to blue."}
    )
    invalid = build_backup_requirements_report(object())

    assert empty.source_brief_id == "empty"
    assert empty.requirements == ()
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary["requirement_count"] == 0
    assert empty.summary["completeness_score"] == 0.0
    assert invalid.source_brief_id is None
    assert invalid.requirements == ()


def test_confidence_scoring_prioritizes_explicit_values_and_key_fields():
    result = build_backup_requirements_report(
        {
            "id": "backup-confidence",
            "title": "Backup confidence test",
            "summary": "Backups are nice to have.",
            "constraints": [
                "Backup frequency must be hourly for critical databases.",
            ],
            "success_criteria": [
                "Backups shall be encrypted at rest using AES-256.",
            ],
        }
    )

    by_type = {record.requirement_type: record for record in result.records}

    # Explicit value in constraints with "must" should have high confidence
    assert by_type["frequency"].confidence >= 0.84

    # "Shall" in success_criteria with encryption keyword should have high confidence
    assert by_type["encryption"].confidence >= 0.74

    # Records should be sorted by confidence (descending)
    confidences = [record.confidence for record in result.records]
    assert confidences == sorted(confidences, reverse=True)


def test_restore_testing_and_compliance_extraction():
    result = build_backup_requirements_report(
        {
            "id": "backup-testing",
            "title": "Backup testing and compliance",
            "summary": "Regular disaster recovery drills are essential.",
            "constraints": [
                "Test restores monthly to verify backup integrity.",
                "Backup retention must comply with GDPR requirements.",
                "SOX compliance requires 7-year archive retention.",
            ],
            "success_criteria": [
                "Quarterly DR drills demonstrate successful restore capability.",
            ],
        }
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert "restore_testing" in by_type
    assert "compliance" in by_type
    assert any(
        "drill" in evidence.lower() or "test restore" in evidence.lower()
        for evidence in by_type["restore_testing"].evidence
    )
    assert any(
        "gdpr" in evidence.lower() or "sox" in evidence.lower()
        for evidence in by_type["compliance"].evidence
    )


def test_point_in_time_recovery_extraction():
    result = build_backup_requirements_report(
        {
            "id": "backup-pitr",
            "title": "Point-in-time recovery",
            "summary": "Database must support point-in-time recovery.",
            "constraints": [
                "PITR capability for transaction log replay.",
                "Point-in-time restore to any timestamp within retention window.",
            ],
        }
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert "point_in_time" in by_type
    assert any(
        "point-in-time" in evidence.lower() or "pitr" in evidence.lower()
        for evidence in by_type["point_in_time"].evidence
    )


def test_backup_type_and_verification_extraction():
    result = build_backup_requirements_report(
        {
            "id": "backup-types",
            "title": "Backup types and verification",
            "summary": "Full backups weekly, incremental backups daily.",
            "constraints": [
                "Differential backups for faster restore times.",
                "Verify backup integrity with checksum validation.",
                "Backup health monitoring and alerting.",
            ],
        }
    )

    by_type = {record.requirement_type: record for record in result.records}

    assert "backup_type" in by_type
    assert "verification" in by_type
    assert any(
        "full" in evidence.lower() or "incremental" in evidence.lower() or "differential" in evidence.lower()
        for evidence in by_type["backup_type"].evidence
    )
    assert any(
        "verify" in evidence.lower() or "checksum" in evidence.lower() or "monitoring" in evidence.lower()
        for evidence in by_type["verification"].evidence
    )


def _statement(evidence):
    return evidence.partition(": ")[2] or evidence


def _source_brief(
    *,
    source_id="source-backup-requirements",
    title="Backup requirements",
    domain="platform",
    summary="General backup and recovery requirements.",
    source_payload=None,
    source_links=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {} if source_links is None else source_links,
        "created_at": None,
        "updated_at": None,
    }
