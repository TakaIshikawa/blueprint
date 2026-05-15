import json

from blueprint.source_customer_health_score_requirements import (
    build_source_customer_health_score_requirements,
    derive_source_customer_health_score_requirements,
    extract_source_customer_health_score_requirements,
    generate_source_customer_health_score_requirements,
    source_customer_health_score_requirements_to_dict,
    source_customer_health_score_requirements_to_dicts,
    source_customer_health_score_requirements_to_markdown,
    summarize_source_customer_health_score_requirements,
)


def test_extracts_all_customer_health_score_categories_with_evidence():
    result = build_source_customer_health_score_requirements(_source([
        "Customer health score score inputs must include usage, support tickets, billing, and NPS signals.",
        "Customer health score weighting model must weight product usage at 40 percent in the formula.",
        "Customer health score risk thresholds must define red, yellow, and green bands below 60.",
        "Customer health score account segments must separate enterprise, SMB, region, and ARR tiers.",
        "Customer health score refresh cadence must recompute scores daily on a scheduled refresh.",
        "Customer health score owner workflow must create CSM tasks and playbook follow-up actions.",
        "Customer health score alert routing must send Slack and email notifications to account owners.",
        "Customer health score historical trend must retain score history, deltas, and timeline snapshots.",
        "Customer health score reporting surface must expose a dashboard and executive report export.",
    ]))

    assert [record.requirement_type for record in result.records] == ["score_inputs", "weighting_model", "risk_thresholds", "account_segments", "refresh_cadence", "owner_workflow", "alert_routing", "historical_trend", "reporting_surface"]
    assert all(record.evidence for record in result.records)
    assert result.summary["missing_detail_flags"] == []


def test_partial_brief_flags_score_inputs_risk_thresholds_and_owner_workflow_details():
    result = derive_source_customer_health_score_requirements("Customer health score score inputs are required. Customer health score risk thresholds are required. Customer health score owner workflow is required.")

    assert result.summary["missing_detail_flags"] == ["missing_score_inputs", "missing_risk_thresholds", "missing_owner_workflow"]


def test_serializers_aliases_and_negated_scope_are_deterministic():
    result = extract_source_customer_health_score_requirements(_source(["Customer health score score inputs must include usage and support signals."], "health-1"))
    payload = source_customer_health_score_requirements_to_dict(result)

    assert generate_source_customer_health_score_requirements("Customer health score reporting surface must expose a dashboard.").summary["requirement_count"] == 1
    assert summarize_source_customer_health_score_requirements(result)["requirement_count"] == 1
    assert json.loads(json.dumps(payload))["source_id"] == "health-1"
    assert source_customer_health_score_requirements_to_dicts(result) == payload["records"]
    assert source_customer_health_score_requirements_to_dicts(result.records) == payload["records"]
    assert "# Source Customer Health Score Requirements Report: health-1" in source_customer_health_score_requirements_to_markdown(result)
    assert build_source_customer_health_score_requirements("No customer health score planning changes are required.").records == ()


def _source(lines, source_id="health-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "title": "Customer health score planning", "summary": "Customer health score planning", "source_payload": {"requirements": lines}, "source_links": {}}
