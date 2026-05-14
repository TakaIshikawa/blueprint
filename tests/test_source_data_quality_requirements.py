import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_data_quality_requirements import (
    build_source_data_quality_requirements,
    derive_source_data_quality_requirements,
    source_data_quality_requirements_to_dict,
    source_data_quality_requirements_to_dicts,
    source_data_quality_requirements_to_markdown,
)


def test_extracts_data_quality_categories_and_summary_details():
    result = build_source_data_quality_requirements(
        {
            "id": "dq-source",
            "source_payload": {
                "requirements": [
                    "Dataset orders freshness SLA must alert when stale after 2 hours.",
                    "Completeness threshold for table orders must stay above 99 percent field coverage.",
                    "Duplicate handling must dedupe records by unique key and quarantine conflicts.",
                    "Schema drift must alert and block unexpected column changes.",
                    "Reconciliation checks compare source totals, target totals, and checksum.",
                    "Anomaly alerts must page Slack for volume spike or volume drop.",
                    "Backfill correction must reprocess and repair historical partitions.",
                ]
            },
        }
    )

    assert [record.requirement_type for record in result.records] == [
        "freshness_sla",
        "completeness_thresholds",
        "duplicate_handling",
        "schema_drift",
        "reconciliation_checks",
        "anomaly_alerts",
        "backfill_correction",
    ]
    assert result.summary["status"] == "ready_for_data_quality_planning"
    assert result.summary["category_counts"]["freshness_sla"] == 1
    assert result.summary["confidence_counts"]["high"] == 6
    assert result.summary["threshold_values"] == ["2 hours", "99 percent"]
    assert "orders" in result.summary["affected_datasets"]


def test_partial_and_model_inputs_report_stable_missing_flags():
    impl = ImplementationBrief.model_validate(
        {
            "id": "impl-dq",
            "source_brief_id": "dq-source",
            "title": "Data quality",
            "problem_statement": "Plan data quality.",
            "mvp_goal": "Quality checks.",
            "scope": ["Data quality freshness checks are required.", "Data quality anomaly alerts are required."],
            "non_goals": [],
            "assumptions": [],
            "risks": [],
            "validation_plan": "Run tests.",
            "definition_of_done": ["Requirements extracted."],
        }
    )

    result = derive_source_data_quality_requirements(impl)

    assert result.source_id == "impl-dq"
    assert result.summary["missing_detail_flags"] == ["missing_threshold_values", "missing_alert_channel"]
    assert result.summary["status"] == "needs_detail"


def test_negated_malformed_and_serialization_are_stable():
    model = SourceBrief.model_validate(
        {
            "id": "dq-model",
            "source_project": "requirements",
            "source_entity_type": "brief",
            "source_id": "upstream",
            "title": "Data quality",
            "summary": "No data quality changes are required.",
            "source_payload": {},
            "source_links": {},
        }
    )
    empty = build_source_data_quality_requirements(model)
    result = build_source_data_quality_requirements("Data quality reconciliation checks must compare source totals.")
    payload = source_data_quality_requirements_to_dict(result)

    assert empty.records == ()
    assert empty.summary["status"] == "no_data_quality_requirements"
    assert build_source_data_quality_requirements(42).records == ()
    assert json.loads(json.dumps(payload, sort_keys=True))["summary"]["requirement_count"] == 1
    assert source_data_quality_requirements_to_dicts(result) == payload["records"]
    assert source_data_quality_requirements_to_markdown(result).startswith("# Source Data Quality Requirements Report")
