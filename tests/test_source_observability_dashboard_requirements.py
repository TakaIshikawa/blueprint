import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_observability_dashboard_requirements import (
    build_source_observability_dashboard_requirements,
    derive_source_observability_dashboard_requirements,
    extract_source_observability_dashboard_requirements,
    generate_source_observability_dashboard_requirements,
    source_observability_dashboard_requirements_to_dict,
    source_observability_dashboard_requirements_to_dicts,
    source_observability_dashboard_requirements_to_markdown,
    summarize_source_observability_dashboard_requirements,
)


def test_extracts_all_observability_dashboard_categories():
    result = build_source_observability_dashboard_requirements(
        _source(
            [
                "Observability dashboard audience must include SRE operators and executive stakeholders.",
                "Observability dashboard key metrics must show latency, error rate, traffic, saturation, and SLO burn rate.",
                "Observability dashboard data source must use Prometheus metrics and Datadog traces.",
                "Observability dashboard filters and dimensions must include environment, region, service, and tenant.",
                "Observability dashboard alert links must connect PagerDuty alert rules and incidents.",
                "Observability dashboard refresh cadence must auto-refresh every 60 seconds.",
                "Observability dashboard ownership must list the SRE owning team and maintainer.",
                "Observability dashboard runbook links must point to remediation playbooks and wiki docs.",
            ]
        )
    )

    assert [record.requirement_type for record in result.records] == [
        "dashboard_audience",
        "key_metrics",
        "data_source",
        "filters_dimensions",
        "alert_links",
        "refresh_cadence",
        "ownership",
        "runbook_links",
    ]
    assert result.summary["missing_detail_flags"] == []


def test_partial_brief_flags_missing_metric_data_source_and_ownership():
    result = derive_source_observability_dashboard_requirements(
        ImplementationBrief.model_validate(
            _implementation_brief(
                [
                    "Observability dashboard key metrics are required.",
                    "Observability dashboard data source is required.",
                    "Observability dashboard ownership is required.",
                ]
            )
        )
    )

    assert result.summary["missing_detail_flags"] == [
        "missing_metric_detail",
        "missing_data_source",
        "missing_ownership",
    ]


def test_model_mapping_string_blank_malformed_serializers_and_aliases():
    model = SourceBrief.model_validate(
        _source(["Observability dashboard data source must use CloudWatch metrics."], source_id="obs-model")
    )
    mapping = _source(["Observability dashboard refresh cadence must update every 5 minutes."], source_id="obs-map")
    free_text = "Observability dashboard runbook links must include remediation docs."
    negated = build_source_observability_dashboard_requirements("No observability dashboard changes are required.")

    model_result = extract_source_observability_dashboard_requirements(model)
    payload = source_observability_dashboard_requirements_to_dict(model_result)

    assert model_result.source_id == "obs-model"
    assert generate_source_observability_dashboard_requirements(mapping).summary["requirement_count"] == 1
    assert summarize_source_observability_dashboard_requirements(free_text)["requirement_count"] == 1
    assert summarize_source_observability_dashboard_requirements(model_result)["requirement_count"] == 1
    assert build_source_observability_dashboard_requirements("").records == ()
    assert build_source_observability_dashboard_requirements(42).records == ()
    assert negated.records == ()
    assert json.loads(json.dumps(payload, sort_keys=True))["source_id"] == "obs-model"
    assert source_observability_dashboard_requirements_to_dicts(model_result) == payload["records"]
    assert source_observability_dashboard_requirements_to_dicts(model_result.records) == payload["records"]
    assert "# Source Observability Dashboard Requirements Report: obs-model" in source_observability_dashboard_requirements_to_markdown(model_result)


def _source(lines, source_id="obs-source"):
    return {
        "id": source_id,
        "source_project": "requirements",
        "source_entity_type": "brief",
        "source_id": f"{source_id}-upstream",
        "title": "Observability dashboard",
        "summary": "Observability dashboard planning",
        "source_payload": {"requirements": lines},
        "source_links": {},
    }


def _implementation_brief(scope):
    return {
        "id": "impl-obs",
        "source_brief_id": "obs-source",
        "title": "Observability dashboard implementation",
        "problem_statement": "Plan observability dashboard.",
        "mvp_goal": "Build a monitoring dashboard.",
        "scope": scope,
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Run tests.",
        "definition_of_done": ["Requirements detected."],
    }
