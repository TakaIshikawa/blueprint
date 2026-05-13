import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_data_quality_monitoring_requirements import (
    build_source_data_quality_monitoring_requirements,
    derive_source_data_quality_monitoring_requirements,
    extract_source_data_quality_monitoring_requirements,
    source_data_quality_monitoring_requirements_to_dict,
    source_data_quality_monitoring_requirements_to_dicts,
    source_data_quality_monitoring_requirements_to_markdown,
)


def test_extracts_all_data_quality_monitoring_categories():
    result = build_source_data_quality_monitoring_requirements(
        _source(
            [
                "Data quality dimensions must track accuracy, validity, consistency, uniqueness, timeliness, and completeness.",
                "Data quality validation rules must include schema validation, range constraints, and referential integrity.",
                "Data freshness checks must alert when warehouse data is stale after 2 hours.",
                "Data quality completeness thresholds must fail when required field coverage is below 99 percent.",
                "Data quality anomaly detection must compare row volume against a seasonal baseline for spikes and drops.",
                "Data quality quarantine and remediation must send bad records to a repair workflow ticket.",
                "Data quality owner escalation must page the data steward team on-call.",
                "Data quality dashboards and alerts must publish metrics to dashboard and Slack alert channels.",
                "Data quality audit history must retain check history and run logs for 13 months.",
            ]
        )
    )

    assert [record.requirement_type for record in result.records] == [
        "quality_dimensions",
        "validation_rules",
        "freshness_checks",
        "completeness_thresholds",
        "anomaly_detection",
        "quarantine_remediation",
        "owner_escalation",
        "dashboards_alerts",
        "audit_history",
    ]
    assert result.summary["missing_detail_flags"] == []


def test_partial_brief_flags_thresholds_remediation_and_ownership():
    result = derive_source_data_quality_monitoring_requirements(
        ImplementationBrief.model_validate(
            _implementation_brief(
                [
                    "Data quality freshness checks are required.",
                    "Data quality completeness thresholds are required.",
                    "Data quality remediation is required.",
                    "Data quality owner escalation is required.",
                ]
            )
        )
    )

    assert result.summary["missing_detail_flags"] == [
        "missing_thresholds",
        "missing_remediation_path",
        "missing_ownership_escalation",
    ]


def test_model_raw_object_serializers_markdown_and_suppression():
    model = SourceBrief.model_validate(
        _source(["Data quality validation rules must include schema constraints."], source_id="dq-model")
    )
    obj = type("Obj", (), {"summary": "Data quality anomaly detection should use baseline volume checks."})()
    observability = build_source_data_quality_monitoring_requirements(
        "Service observability dashboard tracks latency alerts and uptime logs."
    )
    negated = build_source_data_quality_monitoring_requirements(
        _source(["No data quality monitoring changes are required."])
    )

    assert extract_source_data_quality_monitoring_requirements(model).source_id == "dq-model"
    assert build_source_data_quality_monitoring_requirements(obj).summary["requirement_count"] == 1
    assert build_source_data_quality_monitoring_requirements("Data quality audit history must retain run history.").summary["requirement_count"] == 1
    assert observability.records == ()
    assert negated.records == ()

    payload = source_data_quality_monitoring_requirements_to_dict(extract_source_data_quality_monitoring_requirements(model))
    assert json.loads(json.dumps(payload, sort_keys=True))["source_id"] == "dq-model"
    assert source_data_quality_monitoring_requirements_to_dicts(extract_source_data_quality_monitoring_requirements(model)) == payload["records"]
    assert "# Source Data Quality Monitoring Requirements Report: dq-model" in source_data_quality_monitoring_requirements_to_markdown(extract_source_data_quality_monitoring_requirements(model))


def _source(lines, source_id="dq-source"):
    return {
        "id": source_id,
        "source_project": "requirements",
        "source_entity_type": "brief",
        "source_id": f"{source_id}-upstream",
        "title": "Data quality monitoring",
        "summary": "Data quality monitoring planning",
        "source_payload": {"requirements": lines},
        "source_links": {},
    }


def _implementation_brief(scope):
    return {
        "id": "impl-dq",
        "source_brief_id": "dq-source",
        "title": "Data quality monitoring implementation",
        "problem_statement": "Plan data quality monitoring.",
        "mvp_goal": "Monitor source data quality.",
        "scope": scope,
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Run tests.",
        "definition_of_done": ["Requirements detected."],
    }
