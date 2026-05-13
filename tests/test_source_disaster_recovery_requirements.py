import json

from blueprint.source_disaster_recovery_requirements import (
    SourceDisasterRecoveryRequirement,
    SourceDisasterRecoveryRequirementsReport,
    build_source_disaster_recovery_requirements,
    derive_source_disaster_recovery_requirements,
    extract_source_disaster_recovery_requirements,
    generate_source_disaster_recovery_requirements,
    source_disaster_recovery_requirements_to_dict,
    source_disaster_recovery_requirements_to_dicts,
    source_disaster_recovery_requirements_to_markdown,
    summarize_source_disaster_recovery_requirements,
)


def test_structured_reliability_operations_and_infrastructure_sections_extract_records():
    report = build_source_disaster_recovery_requirements(
        _source(
            {
                "reliability": [
                    "RTO must recover API service within 2 hours and measurement is reported against the objective.",
                    "RPO must lose no more than 5 minutes of transaction data and measurement is tracked.",
                    "Automatic failover must trigger on region outage and support failback rollback.",
                ],
                "infrastructure": [
                    "Backup dependency requires snapshot backup, restore runbook path, and 35 days retention.",
                    "Regional constraint requires EU primary, EU secondary region failover target, and data residency rule.",
                ],
                "operations": [
                    "Recovery owner must be SRE on-call with secondary backup owner and PagerDuty escalation.",
                    "Restore drill must run quarterly for region outage scenario with ticket evidence.",
                    "Customer communication must notify customers through status page within 30 minutes.",
                ],
            }
        )
    )

    assert isinstance(report, SourceDisasterRecoveryRequirementsReport)
    assert all(isinstance(record, SourceDisasterRecoveryRequirement) for record in report.records)
    assert [record.requirement_type for record in report.records] == [
        "rto",
        "rpo",
        "failover_mode",
        "backup_dependency",
        "recovery_ownership",
        "validation_drill",
        "customer_communication",
        "regional_constraint",
    ]
    assert all(record.readiness == "ready" for record in report.records)
    assert report.summary["category_counts"]["rpo"] == 1
    assert report.summary["readiness_counts"] == {"ready": 8, "needs_detail": 0}


def test_vague_disaster_recovery_mentions_need_detail():
    report = build_source_disaster_recovery_requirements("Disaster recovery requires RTO, RPO, failover, backup, and restore drill.")
    by_type = {record.requirement_type: record for record in report.records}

    assert [record.requirement_type for record in report.records] == ["rto", "rpo", "failover_mode", "backup_dependency", "validation_drill"]
    assert by_type["rto"].missing_details == ("target_time", "service_scope", "measurement")
    assert by_type["rto"].readiness == "needs_detail"
    assert report.summary["missing_detail_count"] > 0


def test_helpers_and_empty_inputs_are_stable():
    report = build_source_disaster_recovery_requirements(_source({"operations": ["RTO must recover service within 1 hour and metric measurement | note."]}))
    payload = source_disaster_recovery_requirements_to_dict(report)
    empty = build_source_disaster_recovery_requirements(_source({"operations": ["No disaster recovery, RTO, RPO, failover, backup, or restore drill work is required."]}))

    source = _source({"operations": ["RTO must recover service within 1 hour and metric measurement | note."]})
    assert extract_source_disaster_recovery_requirements(source).to_dict() == payload
    assert generate_source_disaster_recovery_requirements(source).to_dict() == payload
    assert derive_source_disaster_recovery_requirements(source).to_dict() == payload
    assert json.loads(json.dumps(payload)) == payload
    assert report.records == report.findings
    assert source_disaster_recovery_requirements_to_dicts(report) == payload["requirements"]
    assert source_disaster_recovery_requirements_to_dicts(report.records) == payload["records"]
    assert summarize_source_disaster_recovery_requirements(report) == report.summary
    assert source_disaster_recovery_requirements_to_markdown(report) == report.to_markdown()
    assert "metric measurement \\| note" in report.to_markdown()
    assert empty.records == ()
    assert build_source_disaster_recovery_requirements({"source_payload": {"notes": object()}}).records == ()
    assert build_source_disaster_recovery_requirements(42).records == ()


def _source(source_payload):
    return {
        "id": "sb-dr",
        "title": "Disaster recovery requirements",
        "domain": "reliability",
        "summary": "DR planning.",
        "source_project": "project",
        "source_entity_type": "issue",
        "source_id": "issue-dr",
        "source_payload": source_payload,
        "source_links": {},
    }
