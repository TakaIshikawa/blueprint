import json

from blueprint.domain.models import SourceBrief
from blueprint.source_measurable_outcomes import (
    SourceMeasurableOutcome,
    SourceMeasurableOutcomeReport,
    build_source_measurable_outcomes,
    extract_source_measurable_outcomes,
    source_measurable_outcomes_report_to_dict,
    source_measurable_outcomes_to_dicts,
    source_measurable_outcomes_to_markdown,
    summarize_source_measurable_outcomes,
)


def test_structured_dict_input_extracts_measurement_details_and_owner_hints():
    result = build_source_measurable_outcomes(
        _source_brief(
            source_payload={
                "measurable_outcomes": [
                    {
                        "metric": "checkout conversion rate",
                        "baseline": "42%",
                        "target": "50%",
                        "measurement_window": "30 days after launch",
                        "owner": "Growth Analytics",
                    },
                    {
                        "kpi": "p95 API latency",
                        "current": "420ms",
                        "threshold": "under 250ms",
                        "timeframe": "weekly for the first month",
                        "dri": "@platform",
                    },
                ]
            }
        )
    )

    assert isinstance(result, SourceMeasurableOutcomeReport)
    assert all(isinstance(outcome, SourceMeasurableOutcome) for outcome in result.outcomes)
    assert [
        (
            outcome.metric_name,
            outcome.baseline,
            outcome.target,
            outcome.threshold,
            outcome.measurement_window,
            outcome.owner_hint,
        )
        for outcome in result.outcomes
    ] == [
        (
            "checkout conversion rate",
            "42%",
            "50%",
            None,
            "30 days after launch",
            "Growth Analytics",
        ),
        (
            "p95 API latency",
            "420ms",
            None,
            "under 250ms",
            "weekly for the first month",
            "@platform",
        ),
    ]
    assert [outcome.confidence for outcome in result.outcomes] == ["high", "high"]
    assert result.summary["missing_baseline_count"] == 0
    assert result.summary["missing_target_count"] == 0
    assert result.summary["missing_measurement_window_count"] == 0
    assert result.summary["owner_hint_count"] == 2


def test_markdown_text_extracts_metrics_and_normalizes_duplicate_signals():
    text = """
    ## Success measures
    - Reduce checkout abandonment from 42% to below 30% within 60 days, owned by Growth.
    - Reduce checkout abandonment from 42% to below 30% within 60 days, owned by Growth.
    - Keep p95 latency under 250ms within 2 weeks after release. DRI: Platform.
    """

    outcomes = extract_source_measurable_outcomes(text)

    assert [(outcome.metric_name, outcome.source_field) for outcome in outcomes] == [
        ("checkout abandonment", "body"),
        ("p95 latency", "body"),
    ]
    assert outcomes[0].baseline == "42%"
    assert outcomes[0].target == "below 30%"
    assert outcomes[0].measurement_window == "60 days"
    assert outcomes[0].owner_hint == "Growth"
    assert outcomes[1].threshold == "under 250ms"
    assert outcomes[1].measurement_window == "2 weeks after release"


def test_ambiguous_metrics_and_missing_details_are_flagged():
    outcomes = extract_source_measurable_outcomes(
        _source_brief(
            summary="Improve engagement and make the experience better.",
            source_payload={
                "success_metrics": [
                    "Increase activation rate to 35%.",
                    "Reduce churn from 7% by Q3 2026.",
                ]
            },
        )
    )

    engagement = next(outcome for outcome in outcomes if outcome.metric_name == "engagement")
    activation = next(outcome for outcome in outcomes if outcome.metric_name == "activation rate")
    churn = next(outcome for outcome in outcomes if outcome.metric_name == "churn")

    assert engagement.ambiguous_metric_definition is True
    assert engagement.missing_baseline is True
    assert engagement.missing_target is True
    assert engagement.missing_measurement_window is True
    assert engagement.confidence == "low"
    assert activation.target == "35%"
    assert activation.missing_baseline is True
    assert activation.missing_measurement_window is True
    assert churn.baseline == "7%"
    assert churn.missing_target is True
    assert churn.measurement_window == "Q3 2026"


def test_complete_metrics_from_source_brief_model_do_not_mutate_input():
    source = SourceBrief.model_validate(
        _source_brief(
            summary="Activation improvements.",
            source_payload={
                "goals": [
                    "Increase activation rate from 24% to 32% within 45 days, owner: Lifecycle."
                ]
            },
        )
    )
    before = source.model_dump(mode="python")

    result = summarize_source_measurable_outcomes(source)

    assert source.model_dump(mode="python") == before
    assert result.brief_id == "sb-outcomes"
    assert result.outcomes[0].metric_name == "activation rate"
    assert result.outcomes[0].baseline == "24%"
    assert result.outcomes[0].target == "32%"
    assert result.outcomes[0].measurement_window == "45 days"
    assert result.outcomes[0].owner_hint == "Lifecycle"
    assert result.outcomes[0].confidence == "high"


def test_empty_input_returns_stable_empty_report():
    assert extract_source_measurable_outcomes("") == ()
    assert extract_source_measurable_outcomes(_source_brief(summary="Update checkout copy.")) == ()

    result = build_source_measurable_outcomes({"id": "empty", "body": "No measurable goals yet."})

    assert result.brief_id == "empty"
    assert result.outcomes == ()
    assert result.summary["confidence_counts"] == {"high": 0, "medium": 0, "low": 0}
    assert "No measurable outcomes were found" in result.to_markdown()


def test_dict_serialization_and_json_key_order_are_stable():
    result = build_source_measurable_outcomes(
        _source_brief(
            source_payload={
                "success_metrics": [
                    {
                        "metric": "signup conversion rate",
                        "baseline": "10%",
                        "target": "12%",
                        "window": "monthly",
                    }
                ]
            }
        )
    )
    payload = source_measurable_outcomes_report_to_dict(result)
    outcome_payload = source_measurable_outcomes_to_dicts(result.outcomes)

    assert result.to_dicts() == outcome_payload
    assert payload["outcomes"] == outcome_payload
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "outcomes", "summary"]
    assert list(payload["outcomes"][0]) == [
        "metric_name",
        "source_field",
        "evidence",
        "baseline",
        "target",
        "threshold",
        "measurement_window",
        "owner_hint",
        "missing_baseline",
        "missing_target",
        "missing_measurement_window",
        "ambiguous_metric_definition",
        "confidence",
    ]


def test_markdown_output_escapes_cells_and_matches_helper():
    result = build_source_measurable_outcomes(
        {
            "id": "pipes",
            "body": "Increase activation | referral rate from 20% to 30% within 30 days.",
        }
    )
    markdown = source_measurable_outcomes_to_markdown(result)

    assert markdown == result.to_markdown()
    assert markdown.startswith("# Source Measurable Outcomes Report: pipes")
    assert "## Summary" in markdown
    assert "| Metric | Source | Baseline | Target | Threshold | Window | Owner | Flags | Evidence |" in markdown
    assert "activation \\| referral rate" in markdown


def _source_brief(
    *,
    source_id="sb-outcomes",
    title="Checkout outcomes",
    domain="payments",
    summary="General checkout outcomes.",
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
