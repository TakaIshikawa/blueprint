import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_disaster_recovery_objectives import (
    SourceDisasterRecoveryObjective,
    SourceDisasterRecoveryObjectivesReport,
    build_source_disaster_recovery_objectives_report,
    derive_source_disaster_recovery_objectives,
    extract_source_disaster_recovery_objectives,
    source_disaster_recovery_objectives_report_to_dict,
    source_disaster_recovery_objectives_to_dicts,
)


def test_extracts_explicit_rto_and_rpo_values_with_evidence_preserved():
    result = build_source_disaster_recovery_objectives_report(
        _source_brief(
            summary=(
                "Disaster recovery target: RTO <= 30 minutes for checkout. "
                "RPO: 5 minutes for order and payment data."
            ),
            source_payload={
                "success_criteria": [
                    "Restore within the RTO <= 30 minutes during a regional outage.",
                ],
            },
        )
    )

    by_type = {record.objective_type: record for record in result.records}

    assert isinstance(result, SourceDisasterRecoveryObjectivesReport)
    assert all(isinstance(record, SourceDisasterRecoveryObjective) for record in result.records)
    assert by_type["rto"].value == "<= 30 minutes"
    assert by_type["rpo"].value == "5 minutes"
    assert any("RTO <= 30 minutes" in evidence for evidence in by_type["rto"].evidence)
    assert any("RPO: 5 minutes" in evidence for evidence in by_type["rpo"].evidence)
    assert by_type["rto"].confidence >= by_type["restore"].confidence
    assert by_type["rpo"].recommended_follow_up


def test_infers_backup_restore_failover_region_incident_and_continuity_objectives():
    result = build_source_disaster_recovery_objectives_report(
        {
            "id": "dr-inferred",
            "title": "Checkout resilience",
            "summary": "Business continuity is required for checkout during major incidents.",
            "problem_statement": "A regional outage must keep critical operations running.",
            "context": "Use multi-region standby and failover for the order service.",
            "constraints": [
                "Nightly backups and hourly snapshots are required.",
                "Test restores from backup before launch.",
            ],
            "success_criteria": [
                "Incident runbooks cover disaster recovery ownership.",
            ],
        }
    )

    assert {record.objective_type for record in result.records} == {
        "backup",
        "restore",
        "failover",
        "region",
        "incident",
        "continuity",
    }
    assert result.summary["objective_count"] == 6
    assert result.summary["objective_type_counts"]["backup"] == 1
    assert [record.confidence for record in result.records] == sorted(
        (record.confidence for record in result.records),
        reverse=True,
    )


def test_duplicate_evidence_is_deduped_and_output_order_is_deterministic():
    result = build_source_disaster_recovery_objectives_report(
        {
            "id": "dr-dupes",
            "title": "Backup requirements",
            "summary": "Backups must run every hour.",
            "constraints": [
                "Backups must run every hour.",
                "backups must run every hour.",
                "Snapshots must be retained for 30 days.",
                "Archive backups monthly.",
                "Replication must protect critical data.",
                "Point-in-time recovery should be available.",
            ],
            "metadata": {"backup": "Backups must run every hour."},
        }
    )

    backup = next(record for record in result.records if record.objective_type == "backup")

    assert len(backup.evidence) == 4
    assert len(backup.evidence) == len(
        {_statement(evidence).casefold() for evidence in backup.evidence}
    )
    assert backup.evidence == tuple(sorted(backup.evidence, key=lambda item: item.casefold()))
    assert [record.objective_type for record in result.records] == [
        record.objective_type for record in build_source_disaster_recovery_objectives_report(
            copy.deepcopy(
                {
                    "id": "dr-dupes",
                    "title": "Backup requirements",
                    "summary": "Backups must run every hour.",
                    "constraints": [
                        "Backups must run every hour.",
                        "backups must run every hour.",
                        "Snapshots must be retained for 30 days.",
                        "Archive backups monthly.",
                        "Replication must protect critical data.",
                        "Point-in-time recovery should be available.",
                    ],
                    "metadata": {"backup": "Backups must run every hour."},
                }
            )
        ).records
    ]


def test_mapping_and_sourcebrief_inputs_match_without_mutating_source_data():
    source = _source_brief(
        source_id="source-dr",
        summary="DR readiness requires failover to a secondary region.",
        source_payload={
            "constraints": [
                "RTO of 1 hour is required for the reporting API.",
                "RPO under 15 minutes is required for customer records.",
            ],
            "success_criteria": ["Restore runbook passes a quarterly DR drill."],
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_disaster_recovery_objectives_report(source)
    model_result = build_source_disaster_recovery_objectives_report(model)
    derived_records = derive_source_disaster_recovery_objectives(model)
    extracted_records = extract_source_disaster_recovery_objectives(model)
    payload = source_disaster_recovery_objectives_report_to_dict(model_result)

    assert source == original
    assert payload == source_disaster_recovery_objectives_report_to_dict(mapping_result)
    assert derived_records == model_result.objectives
    assert extracted_records == model_result.objectives
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.objectives
    assert model_result.to_dicts() == payload["objectives"]
    assert source_disaster_recovery_objectives_to_dicts(model_result) == payload["objectives"]
    assert source_disaster_recovery_objectives_to_dicts(model_result.records) == payload["records"]
    assert list(payload) == ["source_brief_id", "title", "summary", "objectives", "records"]
    assert list(payload["objectives"][0]) == [
        "objective_type",
        "value",
        "source_field",
        "evidence",
        "confidence",
        "recommended_follow_up",
    ]


def test_empty_and_invalid_inputs_return_no_records():
    empty = build_source_disaster_recovery_objectives_report(
        {"id": "empty", "title": "Copy update", "summary": "Update onboarding copy."}
    )
    invalid = build_source_disaster_recovery_objectives_report(object())

    assert empty.source_brief_id == "empty"
    assert empty.objectives == ()
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary["objective_count"] == 0
    assert invalid.source_brief_id is None
    assert invalid.objectives == ()


def _statement(evidence):
    return evidence.partition(": ")[2] or evidence


def _source_brief(
    *,
    source_id="source-disaster-recovery",
    title="Disaster recovery objectives",
    domain="platform",
    summary="General disaster recovery requirements.",
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
