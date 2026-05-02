import json

from blueprint.domain.models import SourceBrief
from blueprint.source_sla_uptime_requirements import (
    SourceSlaUptimeRequirement,
    SourceSlaUptimeRequirementsReport,
    build_source_sla_uptime_requirements,
    derive_source_sla_uptime_requirements,
    extract_source_sla_uptime_requirements,
    generate_source_sla_uptime_requirements,
    source_sla_uptime_requirements_to_dict,
    source_sla_uptime_requirements_to_dicts,
    source_sla_uptime_requirements_to_markdown,
    summarize_source_sla_uptime_requirements,
)


def test_extracts_explicit_sla_uptime_credits_response_and_maintenance_requirements():
    result = build_source_sla_uptime_requirements(
        _source_brief(
            summary=(
                "Checkout must meet 99.95% uptime under the SLA. "
                "P1 incidents require an initial response within 15 minutes."
            ),
            source_payload={
                "sla": [
                    "Service credits apply when monthly availability drops below the target.",
                    "Scheduled maintenance window is Sunday 01:00-03:00 UTC and is excluded from uptime.",
                ]
            },
        )
    )

    by_label = {record.requirement_label: record for record in result.records}

    assert isinstance(result, SourceSlaUptimeRequirementsReport)
    assert all(isinstance(record, SourceSlaUptimeRequirement) for record in result.records)
    assert {
        "uptime_percentage",
        "availability_expectation",
        "sla_credit",
        "response_time_commitment",
        "maintenance_window",
    } <= set(by_label)
    assert by_label["uptime_percentage"].value == "99.95%"
    assert by_label["response_time_commitment"].value == "within 15 minutes"
    assert "Sunday 01:00-03:00 UTC" in by_label["maintenance_window"].value
    assert any("99.95% uptime" in item for item in by_label["uptime_percentage"].evidence)
    assert any("Service credits apply" in item for item in by_label["sla_credit"].evidence)
    assert by_label["sla_credit"].planning_note.startswith("Define credit triggers")
    assert result.summary["requirement_label_counts"]["maintenance_window"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_extracts_mixed_implied_availability_incident_credit_and_serialization_helpers():
    source = _source_brief(
        source_id="mixed-sla",
        title="Availability and support commitments",
        summary="The admin console should be available 24/7 for enterprise tenants.",
        source_payload={
            "requirements": [
                "Critical incidents must be acknowledged under 30 minutes.",
                "Outage credits are owed when a regional incident breaches availability commitments.",
            ],
            "operations": [
                "Planned maintenance is monthly maintenance window with 7 days notice.",
            ],
        },
    )
    model = SourceBrief.model_validate(source)

    result = build_source_sla_uptime_requirements(model)
    generated = generate_source_sla_uptime_requirements(model)
    derived = derive_source_sla_uptime_requirements(model)
    extracted = extract_source_sla_uptime_requirements(model)
    summarized = summarize_source_sla_uptime_requirements(model)
    payload = source_sla_uptime_requirements_to_dict(result)
    markdown = source_sla_uptime_requirements_to_markdown(result)

    labels = {record.requirement_label for record in result.records}
    assert {
        "availability_expectation",
        "incident_credit",
        "response_time_commitment",
        "maintenance_window",
    } <= labels
    assert generated.to_dict() == result.to_dict()
    assert derived.to_dict() == result.to_dict()
    assert extracted == result.requirements
    assert summarized.to_dict() == result.to_dict()
    assert source_sla_uptime_requirements_to_dicts(result) == payload["requirements"]
    assert source_sla_uptime_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_brief_id", "title", "summary", "requirements", "records"]
    assert list(payload["requirements"][0]) == [
        "requirement_label",
        "value",
        "source_field",
        "evidence",
        "matched_terms",
        "confidence",
        "planning_note",
    ]
    assert markdown.startswith("# Source SLA Uptime Requirements Report: mixed-sla")


def test_plain_text_maintenance_window_and_response_commitment_are_extracted():
    result = build_source_sla_uptime_requirements(
        "Maintenance windows must be published for Friday 22:00-23:00 UTC. "
        "Severity 1 response time must be under 10 minutes."
    )

    by_label = {record.requirement_label: record for record in result.records}

    assert "maintenance_window" in by_label
    assert "response_time_commitment" in by_label
    assert by_label["response_time_commitment"].value == "under 10 minutes"
    assert any("Friday 22:00-23:00 UTC" in item for item in by_label["maintenance_window"].evidence)


def test_unrelated_and_malformed_inputs_return_stable_empty_reports():
    unrelated = build_source_sla_uptime_requirements(
        _source_brief(
            title="Profile settings",
            summary="Improve onboarding copy and button labels.",
            source_payload={"requirements": ["Keep form submission behavior unchanged."]},
        )
    )
    malformed = build_source_sla_uptime_requirements({"source_payload": {"notes": object()}})
    blank_text = build_source_sla_uptime_requirements("")

    assert unrelated.records == ()
    assert unrelated.to_dicts() == []
    assert unrelated.summary == {
        "requirement_count": 0,
        "requirement_labels": [],
        "requirement_label_counts": {
            "uptime_percentage": 0,
            "availability_expectation": 0,
            "sla_credit": 0,
            "incident_credit": 0,
            "response_time_commitment": 0,
            "maintenance_window": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_sla_uptime_language",
    }
    assert malformed.records == ()
    assert blank_text.records == ()
    assert "No source SLA uptime requirements were inferred" in unrelated.to_markdown()


def _source_brief(
    *,
    source_id="source-sla-uptime",
    title="SLA uptime requirements",
    domain="platform",
    summary="General SLA uptime requirements.",
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
