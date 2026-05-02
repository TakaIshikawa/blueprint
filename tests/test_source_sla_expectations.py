import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_sla_expectations import (
    SourceSLAExpectation,
    SourceSLAExpectationsReport,
    build_source_sla_expectation_report,
    build_source_sla_expectations,
    extract_source_sla_expectations,
    source_sla_expectations_to_dict,
    source_sla_expectations_to_dicts,
)


def test_markdown_like_brief_body_extracts_sla_categories():
    result = build_source_sla_expectations(
        _source(
            source_payload={
                "body": """
# Service Commitments

- Availability target is 99.95% uptime for the reporting API.
- p95 latency must stay under 250ms for dashboard reads.
- Support response commitment: P1 incidents receive first response within 15 minutes.
- Scheduled maintenance window is Sunday 02:00-04:00 UTC.
- Error budget burn over 10% in a day requires SRE review.
- Contractual SLA includes service credits if monthly uptime is missed.
"""
            }
        )
    )

    assert isinstance(result, SourceSLAExpectationsReport)
    assert all(isinstance(record, SourceSLAExpectation) for record in result.records)
    assert [record.category for record in result.expectations] == [
        "availability",
        "latency",
        "support_response",
        "maintenance_window",
        "error_budget",
        "contractual_sla",
    ]
    assert result.summary["expectation_count"] == 6
    assert result.summary["high_confidence_count"] >= 4
    assert result.expectations[0].suggested_owner == "engineering_oncall"
    assert "availability commitment" in result.expectations[0].suggested_planning_note


def test_structured_fields_and_implementation_brief_contribute_evidence():
    model = ImplementationBrief.model_validate(
        _implementation(
            architecture_notes="Read API SLO: p99 response time below 1s for regional dashboards.",
            risks=[
                "Customer SLA penalties apply if availability drops below 99.9%.",
                "Escalation policy requires support to respond within 30 minutes for Sev1.",
            ],
            definition_of_done=[
                "Maintenance window must avoid weekdays for planned downtime.",
                "Error budget policy is reviewed before launch.",
            ],
        )
    )

    result = build_source_sla_expectation_report(model)

    assert result.brief_id == "impl-sla"
    assert [record.category for record in result.records] == [
        "availability",
        "latency",
        "support_response",
        "maintenance_window",
        "error_budget",
        "contractual_sla",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["latency"].evidence == (
        "architecture_notes: Read API SLO: p99 response time below 1s for regional dashboards.",
    )
    assert by_category["support_response"].suggested_owner == "support_lead"
    assert by_category["error_budget"].suggested_owner == "sre_owner"
    assert by_category["contractual_sla"].confidence >= 0.85


def test_duplicate_categories_merge_deterministically_with_stable_confidence():
    result = build_source_sla_expectations(
        {
            "id": "dupes",
            "source_payload": {
                "sla": {
                    "availability": "Availability target is 99.9% uptime.",
                    "same_availability": "Availability target is 99.9% uptime.",
                    "latency": "Latency must stay under 400ms.",
                },
                "acceptance_criteria": [
                    "Availability target is 99.9% uptime.",
                    "Latency must stay under 400ms.",
                ],
            },
        }
    )

    assert [record.category for record in result.records] == ["availability", "latency"]
    assert result.records[0].evidence == ("source_payload.acceptance_criteria[0]: Availability target is 99.9% uptime.",)
    assert result.records[0].confidence == 0.95
    assert result.records[1].confidence == 0.95


def test_sourcebrief_object_serialization_and_summary_counts_are_stable_without_mutation():
    source = _source(
        source_payload={
            "service_levels": {
                "availability_target": "99.99% uptime target for enterprise tenants.",
                "maintenance_window": "Planned downtime window must be announced 7 days in advance.",
            },
            "requirements": ["Customer support first response within 2 hours during business hours."],
        }
    )
    original = copy.deepcopy(source)
    model_result = build_source_sla_expectations(SourceBrief.model_validate(source))
    mapping_result = build_source_sla_expectations(source)
    object_result = build_source_sla_expectations(
        SimpleNamespace(id="object-sla", body="Vendor SLA service credits apply when support response exceeds one day.")
    )
    extracted = extract_source_sla_expectations(SourceBrief.model_validate(source))
    payload = source_sla_expectations_to_dict(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert extracted == model_result.expectations
    assert model_result.records == model_result.expectations
    assert model_result.to_dicts() == payload["expectations"]
    assert source_sla_expectations_to_dicts(model_result) == payload["expectations"]
    assert source_sla_expectations_to_dicts(model_result.records) == payload["expectations"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "expectations", "summary"]
    assert list(payload["expectations"][0]) == [
        "category",
        "confidence",
        "evidence",
        "suggested_owner",
        "suggested_planning_note",
    ]
    assert payload["summary"]["category_counts"]["availability"] == 1
    assert payload["summary"]["category_counts"]["maintenance_window"] == 1
    assert payload["summary"]["suggested_owner_counts"]["engineering_oncall"] == 1
    assert object_result.brief_id == "object-sla"
    assert [record.category for record in object_result.records] == ["support_response", "contractual_sla"]


def test_no_expectations_invalid_input_and_stable_category_counts():
    empty = build_source_sla_expectations(_source(summary="Polish onboarding copy.", source_payload={"body": "No timing or support changes."}))
    repeat = build_source_sla_expectations(_source(summary="Polish onboarding copy.", source_payload={"body": "No timing or support changes."}))
    invalid = build_source_sla_expectations(17)

    assert empty.to_dict() == repeat.to_dict()
    assert empty.brief_id == "source-sla"
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "expectation_count": 0,
        "category_counts": {
            "availability": 0,
            "latency": 0,
            "support_response": 0,
            "maintenance_window": 0,
            "error_budget": 0,
            "contractual_sla": 0,
        },
        "high_confidence_count": 0,
        "categories": [],
        "suggested_owner_counts": {},
    }
    assert invalid.brief_id is None
    assert invalid.records == ()


def _source(*, summary="Enterprise reporting service levels.", source_payload=None):
    return {
        "id": "source-sla",
        "title": "Reporting SLA",
        "domain": "analytics",
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "issue",
        "source_id": "SLA-1",
        "source_payload": source_payload or {},
        "source_links": {},
    }


def _implementation(*, architecture_notes=None, risks=None, definition_of_done=None):
    return {
        "id": "impl-sla",
        "source_brief_id": "source-sla",
        "title": "Enterprise reporting",
        "domain": "analytics",
        "target_user": "ops",
        "buyer": "enterprise",
        "workflow_context": "Plan service-level commitments before task generation.",
        "problem_statement": "Enterprise customers need committed reliability.",
        "mvp_goal": "Capture SLA expectations in the plan.",
        "product_surface": "reporting",
        "scope": ["Reporting API"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": architecture_notes,
        "data_requirements": None,
        "integration_points": [],
        "risks": risks or [],
        "validation_plan": "Review SLO dashboards.",
        "definition_of_done": definition_of_done or [],
    }
