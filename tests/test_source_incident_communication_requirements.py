import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_incident_communication_requirements import (
    SourceIncidentCommunicationRequirement,
    SourceIncidentCommunicationRequirementsReport,
    build_source_incident_communication_requirements,
    derive_source_incident_communication_requirements,
    extract_source_incident_communication_requirements,
    generate_source_incident_communication_requirements,
    source_incident_communication_requirements_to_dict,
    source_incident_communication_requirements_to_dicts,
    source_incident_communication_requirements_to_markdown,
    summarize_source_incident_communication_requirements,
)


def test_extracts_incident_communication_requirements_in_order():
    result = build_source_incident_communication_requirements(
        _source(
            source_payload={
                "incident_comms": {
                    "audience": "Incident communication audience includes customers and partners.",
                    "channel": "Notify via email, SMS, Slack, and in-app channels.",
                    "severity": "Severity threshold is SEV-1 and customer-impacting SEV-2.",
                    "timing": "Initial notice must be sent within 30 minutes and updates every 60 minutes.",
                    "status": "Publish customer-impacting outages to the status page.",
                    "support": "Customer support handoff uses support macros and ticket routing.",
                    "regulatory": "Regulatory notice covers GDPR breach notification.",
                    "owner": "Incident commander is the communications owner and approver.",
                    "template": "Use pre-approved notification templates.",
                    "post": "Post-incident communication includes postmortem and RCA.",
                }
            }
        )
    )

    assert isinstance(result, SourceIncidentCommunicationRequirementsReport)
    assert all(isinstance(record, SourceIncidentCommunicationRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "audience",
        "channel",
        "severity_threshold",
        "notification_timing",
        "status_page",
        "customer_support_handoff",
        "regulatory_notice",
        "owner",
        "template",
        "post_incident_communication",
    ]
    assert result.summary["timing_coverage"] == 100
    assert result.summary["audience_coverage"] == 100
    assert result.summary["channel_coverage"] == 100
    assert result.summary["ownership_coverage"] == 100
    assert result.summary["regulatory_notice_coverage"] == 100
    assert result.records[6].recommended_follow_up


def test_scans_summary_risks_success_criteria_and_metadata_for_sourcebrief_equivalence():
    source = _source(
        summary="Incident communication must notify customers via email.",
        source_payload={
            "risks": ["Regulatory notice may be required for GDPR incidents."],
            "success_criteria": ["Status page updates posted within 15 minutes."],
            "metadata": {"owner": "Comms owner is the incident commander."},
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping = build_source_incident_communication_requirements(source)
    model_result = derive_source_incident_communication_requirements(model)

    assert source == original
    assert mapping.to_dict() == model_result.to_dict()
    assert extract_source_incident_communication_requirements(model) == model_result.requirements
    assert generate_source_incident_communication_requirements(model).to_dict() == model_result.to_dict()
    assert summarize_source_incident_communication_requirements(mapping) == mapping.summary


def test_serialization_empty_and_markdown_are_stable():
    result = build_source_incident_communication_requirements(
        _source(source_payload={"a": "Incident communication audience includes customers.", "b": "Incident communication audience includes customers."})
    )
    empty = build_source_incident_communication_requirements(_source(summary="Update dashboard copy."))
    negated = build_source_incident_communication_requirements(_source(summary="No incident notification changes are in scope."))
    payload = source_incident_communication_requirements_to_dict(result)

    assert len(result.records[0].evidence) == 1
    assert source_incident_communication_requirements_to_dicts(result) == payload["requirements"]
    assert source_incident_communication_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "| Type | Confidence |" in source_incident_communication_requirements_to_markdown(result)
    assert empty.records == ()
    assert negated.records == ()
    assert empty.summary["status"] == "no_incident_communication_language"


def _source(**overrides):
    payload = {
        "id": "source-incident-comms",
        "source_project": "blueprint",
        "source_entity_type": "brief",
        "source_id": "source-incident-comms",
        "source_links": {},
        "title": "Incident communication source",
        "summary": "Incident communication requirements.",
        "source_payload": {},
    }
    payload.update(overrides)
    return payload
