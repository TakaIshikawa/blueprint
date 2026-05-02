import json

from blueprint.domain.models import SourceBrief
from blueprint.source_observability_requirements import (
    SourceObservabilityRequirement,
    SourceObservabilityRequirementsReport,
    build_source_observability_requirements,
    derive_source_observability_requirements,
    extract_source_observability_requirements,
    generate_source_observability_requirements,
    source_observability_requirements_to_dict,
    source_observability_requirements_to_dicts,
    source_observability_requirements_to_markdown,
    summarize_source_observability_requirements,
)


def test_extracts_logs_metrics_traces_alerts_and_dashboards_in_deterministic_order():
    result = build_source_observability_requirements(
        _source_brief(
            summary=(
                "Checkout must emit structured logs with request_id and order_id. "
                "Metrics should include retry success rate and payment latency."
            ),
            source_payload={
                "observability_requirements": [
                    "Trace propagation must work across checkout, payment, and fulfillment services.",
                    "Alerts must page on-call when payment error rate exceeds threshold.",
                    "Dashboards need Grafana panels for latency, errors, and retry volume.",
                ]
            },
        )
    )

    assert isinstance(result, SourceObservabilityRequirementsReport)
    assert all(isinstance(record, SourceObservabilityRequirement) for record in result.records)
    assert [record.concern for record in result.records] == [
        "logging",
        "metrics",
        "tracing",
        "alerting",
        "dashboards",
    ]
    by_concern = {record.concern: record for record in result.records}
    assert by_concern["logging"].confidence == "high"
    assert "structured logs" in by_concern["logging"].matched_terms
    assert "metric names" in by_concern["metrics"].implementation_note
    assert "span boundaries" in by_concern["tracing"].implementation_note
    assert "routing" in by_concern["alerting"].implementation_note
    assert "dashboard panels" in by_concern["dashboards"].implementation_note
    assert (
        "source_payload.observability_requirements[0]" in by_concern["tracing"].source_field_paths
    )
    assert any("payment error rate" in evidence for evidence in by_concern["alerting"].evidence)
    assert result.summary["requirement_count"] == 5
    assert result.summary["concern_counts"]["dashboards"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_extracts_slo_monitoring_audit_evidence_and_incident_diagnostics_without_uptime_duplication():
    result = build_source_observability_requirements(
        _source_brief(
            summary=(
                "SLO monitoring must include burn-rate alerts and an error budget dashboard. "
                "Audit evidence should record operator action context for production overrides. "
                "Incident diagnostics need trace ids, logs, and runbook evidence for triage."
            ),
            source_payload={
                "requirements": [
                    "Availability target is 99.95% for checkout.",
                    "Uptime target: 99.9%.",
                ]
            },
        )
    )

    assert [record.concern for record in result.records] == [
        "logging",
        "tracing",
        "alerting",
        "dashboards",
        "slo_monitoring",
        "audit_evidence",
        "incident_diagnostics",
    ]
    by_concern = {record.concern: record for record in result.records}
    assert "error-budget review" in by_concern["slo_monitoring"].implementation_note
    assert "operator action" in by_concern["audit_evidence"].matched_terms
    assert any(
        "Incident diagnostics" in evidence
        for evidence in by_concern["incident_diagnostics"].evidence
    )
    assert not any(
        "Availability target is 99.95%" in evidence
        for record in result.records
        for evidence in record.evidence
    )
    assert not any(
        "Uptime target: 99.9%" in evidence
        for record in result.records
        for evidence in record.evidence
    )


def test_duplicate_evidence_and_serialization_helpers_are_stable():
    source = _source_brief(
        source_id="observability-duplicates",
        summary="Logs must include request_id for failed imports.",
        source_payload={
            "requirements": [
                "Logs must include request_id for failed imports.",
                "Logs must include request_id for failed imports.",
                "Metrics must include import failure counters.",
            ],
            "acceptance_criteria": [
                "Metrics must include import failure counters.",
            ],
        },
    )
    model = SourceBrief.model_validate(source)

    result = build_source_observability_requirements(model)
    generated = generate_source_observability_requirements(model)
    extracted = extract_source_observability_requirements(model)
    derived = derive_source_observability_requirements(model)
    payload = source_observability_requirements_to_dict(result)
    markdown = source_observability_requirements_to_markdown(result)

    assert generated.to_dict() == result.to_dict()
    assert extracted.to_dict() == result.to_dict()
    assert derived.to_dict() == result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert source_observability_requirements_to_dicts(result) == payload["requirements"]
    assert source_observability_requirements_to_dicts(result.records) == payload["records"]
    assert summarize_source_observability_requirements(result) == result.summary
    assert [record.concern for record in result.records] == ["logging", "metrics"]
    by_concern = {record.concern: record for record in result.records}
    assert len(by_concern["logging"].evidence) == 1
    assert len(by_concern["metrics"].evidence) == 1
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "concern",
        "evidence",
        "source_field_paths",
        "matched_terms",
        "confidence",
        "implementation_note",
    ]
    assert markdown.startswith(
        "# Source Observability Requirements Report: observability-duplicates"
    )


def test_empty_invalid_no_match_and_pure_slo_target_inputs_return_stable_empty_reports():
    empty = build_source_observability_requirements(
        _source_brief(
            summary="No observability or monitoring changes are in scope.",
            source_payload={"requirements": []},
        )
    )
    malformed = build_source_observability_requirements({"source_payload": {"notes": object()}})
    blank_text = build_source_observability_requirements("")
    unrelated = build_source_observability_requirements(
        _source_brief(
            title="Profile settings",
            summary="Improve onboarding copy and button labels.",
            source_payload={"requirements": ["Keep form submission behavior unchanged."]},
        )
    )
    pure_slo = build_source_observability_requirements(
        _source_brief(
            title="Availability objective",
            summary="Checkout must meet an availability target of 99.95% and uptime target of 99.9%.",
        )
    )

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "concern_counts": {
            "logging": 0,
            "metrics": 0,
            "tracing": 0,
            "alerting": 0,
            "dashboards": 0,
            "slo_monitoring": 0,
            "audit_evidence": 0,
            "incident_diagnostics": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "concerns": [],
        "status": "no_observability_language",
    }
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert malformed.summary == expected_summary
    assert blank_text.summary == expected_summary
    assert unrelated.summary == expected_summary
    assert pure_slo.summary == expected_summary
    assert "No source observability requirements were inferred" in empty.to_markdown()


def _source_brief(
    *,
    source_id="observability-source",
    title="Observability requirements",
    domain="platform",
    summary="General observability requirements.",
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
