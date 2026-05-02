import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_usage_metering_requirements import (
    SourceUsageMeteringRequirement,
    SourceUsageMeteringRequirementsReport,
    build_source_usage_metering_requirements,
    derive_source_usage_metering_requirements,
    extract_source_usage_metering_requirements,
    generate_source_usage_metering_requirements,
    source_usage_metering_requirements_to_dict,
    source_usage_metering_requirements_to_dicts,
    source_usage_metering_requirements_to_markdown,
    summarize_source_usage_metering_requirements,
)


def test_structured_source_payload_extracts_all_usage_metering_dimensions():
    result = build_source_usage_metering_requirements(
        _source_brief(
            source_payload={
                "usage_metering": {
                    "events": "Metered events: api_call and export_run must emit usage with idempotency keys.",
                    "counters": "Counter must increment billable API calls and support reconciliation corrections.",
                    "period": "Billing period resets monthly on the invoice period boundary.",
                    "aggregation": "Aggregation window must roll up usage hourly and handle late-arriving events.",
                    "quota": "Quota unit: API calls with an included allowance of 10,000 per month.",
                    "overage": "Overage behavior must charge extra usage after the allowance and notify admins.",
                    "audit": "Usage ledger audit trail must persist evidence for usage disputes.",
                    "reporting": "Usage dashboard must show customer-visible usage breakdowns and CSV export.",
                }
            }
        )
    )

    assert isinstance(result, SourceUsageMeteringRequirementsReport)
    assert result.source_id == "source-usage-metering"
    assert all(isinstance(record, SourceUsageMeteringRequirement) for record in result.records)
    assert [record.dimension for record in result.records] == [
        "metered_event",
        "counter",
        "billing_period",
        "aggregation_window",
        "quota_unit",
        "overage_behavior",
        "auditability",
        "customer_visible_reporting",
    ]
    by_dimension = {record.dimension: record for record in result.records}
    assert by_dimension["metered_event"].metered_event == "api_call and export_run must emit usage with idempotency keys"
    assert by_dimension["counter"].counter.startswith("counter must increment billable api calls")
    assert by_dimension["billing_period"].billing_period.endswith("resets monthly on the invoice period boundary")
    assert by_dimension["aggregation_window"].aggregation_window.startswith("must roll up usage hourly")
    assert by_dimension["quota_unit"].quota_unit == "api calls with an included allowance of 10,000 per month"
    assert "charge extra usage" in by_dimension["overage_behavior"].overage_behavior
    assert "persist evidence" in by_dimension["auditability"].auditability
    assert "customer-visible usage" in by_dimension["customer_visible_reporting"].customer_visible_reporting
    assert by_dimension["metered_event"].source_field == "source_payload.usage_metering"
    assert any("source_payload.usage_metering" in item for item in by_dimension["metered_event"].evidence)
    assert result.summary["requirement_count"] == 8
    assert result.summary["dimension_counts"]["overage_behavior"] == 1
    assert result.summary["confidence_counts"] == {"high": 8, "medium": 0, "low": 0}
    assert result.summary["status"] == "ready_for_usage_metering_planning"


def test_prose_and_implementation_brief_inputs_extract_metering_work():
    text_result = build_source_usage_metering_requirements(
        """
# Usage metering

- Usage-based billing must meter workspace exports as billable events.
- Customer usage reporting should display monthly quota units and overage status.
"""
    )
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Metered events must track tokens consumed and aggregate usage in daily windows.",
                "Over limit usage should throttle after a grace period.",
            ],
            definition_of_done=[
                "Audit trail records every usage adjustment.",
                "Billing period resets monthly on renewal.",
            ],
        )
    )
    object_result = build_source_usage_metering_requirements(
        SimpleNamespace(
            id="object-usage",
            summary="Quota units must be messages with included allowance by plan.",
            usage={"reporting": "Usage dashboard should show account | workspace breakdowns."},
        )
    )

    assert {
        "metered_event",
        "quota_unit",
        "overage_behavior",
        "customer_visible_reporting",
    } <= {record.dimension for record in text_result.records}
    implementation_result = generate_source_usage_metering_requirements(implementation)
    assert implementation_result.source_id == "implementation-usage-metering"
    assert {
        "metered_event",
        "billing_period",
        "aggregation_window",
        "overage_behavior",
        "auditability",
    } <= {record.dimension for record in implementation_result.records}
    assert [record.dimension for record in object_result.records] == [
        "quota_unit",
        "customer_visible_reporting",
    ]


def test_no_metering_impact_malformed_and_invalid_inputs_are_stable_empty_reports():
    class BriefLike:
        id = "object-no-usage"
        summary = "No usage metering, quota, or overage work is required for this release."

    empty = build_source_usage_metering_requirements(
        _source_brief(summary="Admin copy update has no usage-based billing impact.")
    )
    negated = build_source_usage_metering_requirements(BriefLike())
    malformed = build_source_usage_metering_requirements({"source_payload": {"notes": object()}})
    invalid = build_source_usage_metering_requirements(42)
    blank = build_source_usage_metering_requirements("")

    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "dimension_counts": {
            "metered_event": 0,
            "counter": 0,
            "billing_period": 0,
            "aggregation_window": 0,
            "quota_unit": 0,
            "overage_behavior": 0,
            "auditability": 0,
            "customer_visible_reporting": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "dimensions": [],
        "requires_metered_events": False,
        "requires_counters": False,
        "requires_billing_periods": False,
        "requires_aggregation_windows": False,
        "requires_quota_units": False,
        "requires_overage_behavior": False,
        "requires_auditability": False,
        "requires_customer_visible_reporting": False,
        "status": "no_usage_metering_language",
    }
    assert "No usage metering requirements were found" in empty.to_markdown()
    assert negated.records == ()
    assert malformed.records == ()
    assert invalid.records == ()
    assert blank.records == ()


def test_aliases_json_serialization_ordering_markdown_escaping_and_no_mutation():
    source = _source_brief(
        source_id="usage-model",
        summary="Usage-based product must meter API calls and expose usage reporting.",
        source_payload={
            "requirements": [
                "Metered events must capture api_call usage with event idempotency keys.",
                "Metered events must capture api_call usage with event idempotency keys.",
                "Overage behavior must charge overage after 10k calls | finance note.",
                "Usage dashboard must export usage as CSV for customers.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_usage_metering_requirements(source)
    model_result = extract_source_usage_metering_requirements(model)
    generated = generate_source_usage_metering_requirements(model)
    derived = derive_source_usage_metering_requirements(model)
    payload = source_usage_metering_requirements_to_dict(model_result)
    markdown = source_usage_metering_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert generated.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_usage_metering_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_usage_metering_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_usage_metering_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "dimension",
        "requirement_text",
        "metered_event",
        "counter",
        "billing_period",
        "aggregation_window",
        "quota_unit",
        "overage_behavior",
        "auditability",
        "customer_visible_reporting",
        "source_field",
        "evidence",
        "matched_terms",
        "confidence",
        "planning_note",
    ]
    assert {
        "metered_event",
        "quota_unit",
        "overage_behavior",
        "customer_visible_reporting",
    } <= {record.dimension for record in model_result.records}
    metered_event = model_result.records[0]
    assert metered_event.evidence == (
        "source_payload.requirements[0]: Metered events must capture api_call usage with event idempotency keys.",
    )
    assert metered_event.requirement_category == "metered_event"
    assert metered_event.planning_notes == (metered_event.planning_note,)
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Dimension | Requirement | Event | Counter |" in markdown
    assert "10k calls \\| finance note" in markdown


def _source_brief(
    *,
    source_id="source-usage-metering",
    title="Usage metering requirements",
    domain="billing",
    summary="General usage metering requirements.",
    source_payload=None,
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
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "implementation-usage-metering",
        "source_brief_id": "source-usage-metering",
        "title": "Usage metering rollout",
        "domain": "billing",
        "target_user": "customers",
        "buyer": None,
        "workflow_context": "Usage-based products need explicit usage metering requirements.",
        "problem_statement": "Metered product planning needs source-backed tasks.",
        "mvp_goal": "Plan usage metering behavior from source briefs.",
        "product_surface": "billing usage",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review generated plan for usage metering coverage.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
