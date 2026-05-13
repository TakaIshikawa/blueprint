import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import SourceBrief
from blueprint.source_service_decommission_requirements import (
    SourceServiceDecommissionRequirement,
    SourceServiceDecommissionRequirementsReport,
    build_source_service_decommission_requirements,
    derive_source_service_decommission_requirements,
    extract_source_service_decommission_requirements,
    generate_source_service_decommission_requirements,
    source_service_decommission_requirements_to_dict,
    source_service_decommission_requirements_to_dicts,
    summarize_source_service_decommission_requirements,
)


def test_complete_decommission_plan_extracts_all_requirement_signals():
    report = build_source_service_decommission_requirements(
        _source_brief(
            source_payload={
                "decommission": {
                    "consumers": "Service decommission requires a consumer inventory of API clients and downstream integrations.",
                    "traffic": "Drain traffic to zero traffic before shutdown.",
                    "data": "Archive customer data and purge expired records after retention is met.",
                    "dependencies": "Remove dependencies, feature flags, cron jobs, DNS, secrets, and dashboards.",
                    "comms": "Notify customers, support, and stakeholders with a migration notice.",
                    "monitoring": "Monitor logs and alerts for unexpected traffic for 14 days after shutdown.",
                    "rollback": "Rollback window is 7 days to re-enable the service.",
                    "owner": "Owner is platform lifecycle; escalation goes to PagerDuty on-call.",
                }
            }
        )
    )

    by_signal = {requirement.signal: requirement for requirement in report.records}

    assert isinstance(report, SourceServiceDecommissionRequirementsReport)
    assert all(isinstance(record, SourceServiceDecommissionRequirement) for record in report.records)
    assert list(by_signal) == [
        "consumer_inventory",
        "traffic_drain",
        "data_archival_deletion",
        "dependency_removal",
        "communication",
        "monitoring_after_shutdown",
        "rollback_window",
        "ownership",
    ]
    assert by_signal["traffic_drain"].value == "zero traffic"
    assert by_signal["rollback_window"].value == "7 days"
    assert report.missing_signals == ()
    assert report.weak_signals == ()


def test_partial_decommission_text_reports_missing_and_weak_signals():
    report = build_source_service_decommission_requirements(
        "We should decommission the legacy service. Notify support."
    )

    assert [requirement.signal for requirement in report.records] == ["communication"]
    assert "consumer_inventory" in report.missing_signals
    assert "traffic_drain" in report.missing_signals
    assert "rollback_window" in report.missing_signals
    assert report.weak_signals
    assert "clarify concrete decommission rule" in report.weak_signals[0]


def test_model_object_serialization_helpers_are_stable_without_mutation():
    source = _source_brief(
        summary="Service shutdown needs owner and traffic drain planning.",
        source_payload={
            "shutdown": [
                "Known consumers must be inventoried before sunset.",
                "Rollback window lasts 48 hours after shutdown.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    obj = SimpleNamespace(
        id="object-decommission",
        summary="Retire the old feed.",
        source_payload={"monitoring": "Monitor alerts after shutdown for unexpected traffic."},
    )

    mapping_report = build_source_service_decommission_requirements(source)
    model_report = derive_source_service_decommission_requirements(model)
    generated = generate_source_service_decommission_requirements(model)
    object_report = build_source_service_decommission_requirements(obj)
    payload = source_service_decommission_requirements_to_dict(model_report)

    assert source == original
    assert mapping_report.to_dict() == model_report.to_dict()
    assert generated.to_dict() == model_report.to_dict()
    assert extract_source_service_decommission_requirements(model) == model_report.requirements
    assert source_service_decommission_requirements_to_dicts(model_report) == payload["requirements"]
    assert source_service_decommission_requirements_to_dicts(model_report.records) == payload["records"]
    assert summarize_source_service_decommission_requirements(model_report) == model_report.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "source_id",
        "requirements",
        "records",
        "missing_signals",
        "weak_signals",
        "summary",
    ]
    assert object_report.source_id == "object-decommission"
    assert [record.signal for record in object_report.records] == ["monitoring_after_shutdown"]


def test_absent_and_negated_decommission_language_returns_stable_empty_report():
    empty = build_source_service_decommission_requirements({"summary": "Update billing copy only."})
    negated = build_source_service_decommission_requirements(
        {"summary": "No service decommission or shutdown work is in scope."}
    )

    assert empty.records == ()
    assert negated.records == ()
    assert empty.missing_signals == (
        "consumer_inventory",
        "traffic_drain",
        "data_archival_deletion",
        "dependency_removal",
        "communication",
        "monitoring_after_shutdown",
        "rollback_window",
        "ownership",
    )
    assert empty.summary["requirement_count"] == 0
    assert source_service_decommission_requirements_to_dict(empty)["requirements"] == []


def _source_brief(**overrides):
    payload = {
        "id": "source-service-decommission",
        "source_project": "blueprint",
        "source_entity_type": "brief",
        "source_id": "source-service-decommission",
        "source_links": {},
        "title": "Service decommission source",
        "summary": "Service decommission constraints.",
        "source_payload": {},
    }
    payload.update(overrides)
    return payload
