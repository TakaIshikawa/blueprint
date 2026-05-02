import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_observability_expectations import (
    SourceObservabilityExpectation,
    SourceObservabilityExpectationReport,
    build_source_observability_expectation_report,
    extract_source_observability_expectations,
    source_observability_expectation_report_to_dict,
    source_observability_expectations_to_dicts,
    summarize_source_observability_expectations,
)


def test_source_brief_fields_extract_observability_expectations_with_notes():
    result = build_source_observability_expectation_report(
        _source(
            summary="Checkout must emit metrics for retry success rate.",
            source_payload={
                "acceptance_criteria": [
                    "Done when dashboards show retry error rate and alerts fire within 5 minutes.",
                    "Structured logs must include checkout_id and retry_attempt.",
                ],
                "risks": ["Audit events are required for admin retry overrides."],
                "constraints": ["Trace propagation must work across payment provider callbacks."],
                "metadata": {"observability": "SLO is 99.9% successful checkout retries."},
            },
        )
    )

    assert isinstance(result, SourceObservabilityExpectationReport)
    assert [record.category for record in result.expectations] == [
        "logs",
        "metrics",
        "traces",
        "dashboards",
        "alerts",
        "audit_events",
        "slos",
    ]
    assert all(isinstance(record, SourceObservabilityExpectation) for record in result.records)
    assert result.source_id == "source-observability"
    assert result.summary["expectation_count"] == 7
    assert result.expectations[0].source_id == "source-observability"
    assert "structured logs" in result.expectations[0].suggested_planning_note.lower()
    assert result.expectations[3].evidence == (
        "source_payload.acceptance_criteria[0]: Done when dashboards show retry error rate and alerts fire within 5 minutes.",
    )


def test_structured_fields_receive_higher_confidence_than_generic_notes():
    result = build_source_observability_expectation_report(
        {
            "id": "brief-confidence",
            "notes": "We should consider logs for support debugging.",
            "metadata": {
                "observability": {"logs": "Structured logs are required for support debugging."}
            },
        }
    )

    record = result.expectations[0]

    assert record.category == "logs"
    assert record.confidence >= 0.85
    assert record.evidence == (
        "metadata.observability.logs: Structured logs are required for support debugging.",
        "notes: We should consider logs for support debugging.",
    )

    generic = build_source_observability_expectation_report(
        {"id": "brief-generic", "notes": "We should consider logs for support debugging."}
    )
    assert generic.expectations[0].confidence < record.confidence


def test_implementation_brief_extracts_anomaly_detection_and_debug_tooling():
    brief = ImplementationBrief.model_validate(
        _implementation(
            risks=[
                "Anomaly detection should flag unusual import failure spikes.",
            ],
            definition_of_done=[
                "Debug tooling must let support diagnose failed partner syncs.",
            ],
            validation_plan="Validate metrics and traces during canary.",
        )
    )

    result = build_source_observability_expectation_report(brief)

    assert result.source_id == "impl-observability"
    assert [record.category for record in result.expectations] == [
        "metrics",
        "traces",
        "anomaly_detection",
        "debug_tooling",
    ]
    assert "anomaly detection rules" in result.expectations[2].suggested_planning_note
    assert result.expectations[3].evidence == (
        "definition_of_done[0]: Debug tooling must let support diagnose failed partner syncs.",
    )


def test_markdown_sections_mapping_payloads_and_deduplication_are_stable():
    result = build_source_observability_expectation_report(
        {
            "id": "brief-markdown",
            "source_payload": {
                "body": """
# Observability

- Logs must include request_id and account_id.
- Logs must include request_id and account_id.
- Alerts should page on-call when the SLO burns too quickly.
- Add an anomaly dashboard for import drift.
""",
            },
        }
    )

    assert [record.category for record in result.expectations] == [
        "logs",
        "dashboards",
        "alerts",
        "slos",
        "anomaly_detection",
    ]
    assert result.expectations[0].evidence == (
        "source_payload.body: Logs must include request_id and account_id.",
    )
    assert result.to_dicts() == source_observability_expectations_to_dicts(result.records)


def test_empty_briefs_return_no_records_and_deterministic_summary():
    result = build_source_observability_expectation_report(
        _source(summary="Adjust onboarding copy.", source_payload={})
    )

    assert result.expectations == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary == {
        "expectation_count": 0,
        "high_confidence_count": 0,
        "category_counts": {
            "logs": 0,
            "metrics": 0,
            "traces": 0,
            "dashboards": 0,
            "alerts": 0,
            "audit_events": 0,
            "slos": 0,
            "anomaly_detection": 0,
            "debug_tooling": 0,
        },
        "categories": [],
    }
    assert build_source_observability_expectation_report({}).summary == result.summary


def test_model_mapping_and_serialization_are_stable_without_mutation():
    source = _source(
        source_payload={
            "observability": {
                "metrics": "Metrics must include import success and failure counters.",
                "audit": "Audit logs must capture role changes.",
            }
        }
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_observability_expectation_report(source)
    model_result = build_source_observability_expectation_report(model)
    extracted = extract_source_observability_expectations(model)
    summarized = summarize_source_observability_expectations(extracted)
    payload = source_observability_expectation_report_to_dict(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert extracted == model_result.expectations
    assert summarized.summary == model_result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "expectations", "summary"]
    assert list(payload["expectations"][0]) == [
        "source_id",
        "category",
        "confidence",
        "evidence",
        "suggested_planning_note",
    ]


def _source(*, summary="Checkout observability.", source_payload=None):
    return {
        "id": "source-observability",
        "title": "Checkout rollout",
        "domain": "commerce",
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "issue",
        "source_id": "ISSUE-OBS",
        "source_payload": source_payload or {},
        "source_links": {},
    }


def _implementation(*, risks=None, definition_of_done=None, validation_plan="Validate release."):
    return {
        "id": "impl-observability",
        "source_brief_id": "source-observability",
        "title": "Partner import rollout",
        "domain": "integrations",
        "target_user": "ops",
        "buyer": "operations",
        "workflow_context": "Partner imports run as a scheduled workflow.",
        "problem_statement": "Import failures are difficult to diagnose.",
        "mvp_goal": "Improve import reliability.",
        "product_surface": "imports",
        "scope": ["Partner import"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": risks or [],
        "validation_plan": validation_plan,
        "definition_of_done": definition_of_done or [],
    }
