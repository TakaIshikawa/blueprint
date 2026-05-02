import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_reporting_metric_definitions import (
    SourceReportingMetricDefinition,
    SourceReportingMetricDefinitionsReport,
    build_source_reporting_metric_definitions,
    derive_source_reporting_metric_definitions,
    extract_source_reporting_metric_definitions,
    generate_source_reporting_metric_definitions,
    source_reporting_metric_definitions_to_dict,
    source_reporting_metric_definitions_to_dicts,
    source_reporting_metric_definitions_to_markdown,
    summarize_source_reporting_metric_definitions,
)


def test_extracts_reporting_metrics_from_nested_payloads_and_metadata():
    result = build_source_reporting_metric_definitions(
        _source_brief(
            source_payload={
                "reporting": {
                    "dashboard": "Executive dashboard must show KPI activation rate by plan and region, filtered to paid accounts, weekly, for growth leadership.",
                    "funnel": "Signup funnel conversion rate from visit to verified workspace over 14 days, broken down by acquisition channel.",
                    "revenue": "Revenue report needs monthly ARR and net revenue retention by segment for finance.",
                },
                "metadata": {
                    "analytics_owner": "Owner: analytics team",
                    "freshness": "Dashboard data refreshes within 2 hours.",
                },
            }
        )
    )

    assert isinstance(result, SourceReportingMetricDefinitionsReport)
    assert result.source_id == "sb-reporting"
    assert all(isinstance(record, SourceReportingMetricDefinition) for record in result.records)
    assert [record.metric_surface for record in result.records] == [
        "Executive dashboard",
        "Revenue report",
        "Signup funnel conversion rate",
    ]
    by_surface = {record.metric_surface: record for record in result.records}
    assert by_surface["Executive dashboard"].aggregation_or_window == "weekly"
    assert by_surface["Executive dashboard"].dimensions == ("plan", "region")
    assert by_surface["Executive dashboard"].filters == ("paid accounts",)
    assert by_surface["Executive dashboard"].audience_hint == "growth leadership"
    assert by_surface["Revenue report"].aggregation_or_window == "monthly"
    assert "segment" in by_surface["Revenue report"].dimensions
    assert by_surface["Revenue report"].missing_owner is False
    assert result.summary["metric_definition_count"] == 3
    assert result.summary["surface_counts"] == {
        "Executive dashboard": 1,
        "Revenue report": 1,
        "Signup funnel conversion rate": 1,
    }
    assert result.summary["missing_detail_counts"] == {
        "missing_numerator_denominator": 2,
        "missing_time_window": 0,
        "missing_dimension_filter": 0,
        "missing_owner": 0,
    }


def test_flags_missing_details_for_underdefined_conversion_metric():
    result = build_source_reporting_metric_definitions(
        _source_brief(
            source_payload={
                "acceptance_criteria": [
                    "Dashboard should include conversion rate for onboarding.",
                    "Cohort retention drilldown must be available to customer success.",
                ]
            }
        )
    )

    by_surface = {record.metric_surface: record for record in result.records}

    assert by_surface["conversion rate"].missing_numerator_denominator is True
    assert by_surface["conversion rate"].missing_time_window is True
    assert by_surface["conversion rate"].missing_dimension_filter is True
    assert by_surface["conversion rate"].missing_owner is True
    assert by_surface["conversion rate"].confidence == "low"
    assert by_surface["Cohort retention drilldown"].missing_owner is False
    assert by_surface["Cohort retention drilldown"].audience_hint == "customer success"
    assert result.summary["missing_detail_counts"]["missing_numerator_denominator"] == 2
    assert result.summary["missing_detail_counts"]["missing_time_window"] == 2


def test_structured_metric_definition_and_implementation_brief_inputs_are_supported():
    structured = build_source_reporting_metric_definitions(
        _source_brief(
            source_payload={
                "metrics": [
                    {
                        "metric": "Trial-to-paid conversion KPI",
                        "numerator": "paid conversions",
                        "denominator": "trial starts",
                        "window": "30 days",
                        "dimensions": ["plan", "sales assisted"],
                        "filter": "self-serve trials",
                        "audience": "growth team",
                        "freshness": "daily",
                    }
                ]
            }
        )
    )
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Operations report must show daily SLA breaches by queue for support managers.",
                "Retention cohort dashboard tracks 90-day retention by signup month.",
            ]
        )
    )
    object_result = build_source_reporting_metric_definitions(
        SimpleNamespace(
            id="object-reporting",
            summary="KPI dashboard for revenue metrics, monthly, by country, owner finance.",
        )
    )

    record = structured.records[0]
    assert record.metric_surface == "Trial-to-paid conversion KPI"
    assert record.numerator_hint == "paid conversions"
    assert record.denominator_hint == "trial starts"
    assert record.dimensions == ("plan", "sales assisted")
    assert record.filters == ("self-serve trials",)
    assert record.freshness_or_latency == "daily"
    assert record.confidence == "high"

    model_result = extract_source_reporting_metric_definitions(brief)
    assert model_result.source_id == "impl-reporting"
    assert [record.metric_surface for record in model_result.records] == [
        "Operations report",
        "Retention cohort dashboard",
    ]
    assert object_result.records[0].audience_hint == "finance"


def test_empty_no_signal_and_malformed_inputs_return_deterministic_empty_reports():
    empty = build_source_reporting_metric_definitions(
        _source_brief(summary="Update billing copy.", source_payload={})
    )
    no_signal = build_source_reporting_metric_definitions(
        {"id": "brief-empty", "source_payload": {"notes": object()}}
    )
    invalid = build_source_reporting_metric_definitions(42)

    assert empty.records == ()
    assert empty.metric_definitions == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "metric_definition_count": 0,
        "surface_counts": {},
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_counts": {
            "missing_numerator_denominator": 0,
            "missing_time_window": 0,
            "missing_dimension_filter": 0,
            "missing_owner": 0,
        },
        "metric_surfaces": [],
    }
    assert "No reporting metric definitions were found" in empty.to_markdown()
    assert no_signal.records == ()
    assert invalid.records == ()


def test_aliases_serialization_markdown_ordering_and_no_mutation():
    source = _source_brief(
        source_id="reporting-model",
        summary="Analytics dashboard must show activation KPI weekly by role for product leadership.",
        source_payload={
            "acceptance_criteria": [
                "Revenue dashboard must show monthly MRR by country, filtered to active subscriptions, owner finance.",
                "Funnel report defines conversion rate numerator completed checkout and denominator checkout starts over 7 days by channel.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_reporting_metric_definitions(source)
    model_result = generate_source_reporting_metric_definitions(model)
    derived = derive_source_reporting_metric_definitions(model)
    payload = source_reporting_metric_definitions_to_dict(model_result)
    markdown = source_reporting_metric_definitions_to_markdown(model_result)

    assert source == original
    assert payload == source_reporting_metric_definitions_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.metric_definitions
    assert source_reporting_metric_definitions_to_dicts(model_result) == payload["metric_definitions"]
    assert source_reporting_metric_definitions_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_reporting_metric_definitions(model_result) == model_result.summary
    assert list(payload) == ["source_id", "metric_definitions", "summary", "records"]
    assert list(payload["metric_definitions"][0]) == [
        "source_brief_id",
        "metric_surface",
        "aggregation_or_window",
        "dimensions",
        "filters",
        "freshness_or_latency",
        "audience_hint",
        "numerator_hint",
        "denominator_hint",
        "missing_numerator_denominator",
        "missing_time_window",
        "missing_dimension_filter",
        "missing_owner",
        "confidence",
        "evidence",
    ]
    assert [record.metric_surface for record in model_result.records] == [
        "Analytics dashboard",
        "Funnel report",
        "Revenue dashboard",
    ]
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Metric Surface | Window | Dimensions | Filters | Audience | Flags | Evidence |" in markdown


def _source_brief(
    *,
    source_id="sb-reporting",
    title="Reporting requirements",
    domain="analytics",
    summary="General reporting metric requirements.",
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


def _implementation_brief(*, scope=None):
    return {
        "id": "impl-reporting",
        "source_brief_id": "source-reporting",
        "title": "Reporting rollout",
        "domain": "analytics",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need operational reporting.",
        "problem_statement": "Current reports are insufficient.",
        "mvp_goal": "Improve metric visibility.",
        "product_surface": "reporting",
        "scope": scope or [],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Validate reporting metrics.",
        "definition_of_done": [],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
