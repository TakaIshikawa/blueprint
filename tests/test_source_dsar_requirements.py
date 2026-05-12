import json

from blueprint.source_dsar_requirements import (
    SourceDsarRequirement,
    SourceDsarRequirementsReport,
    build_source_dsar_requirements,
    extract_source_dsar_requirements,
    source_dsar_requirements_to_dict,
    source_dsar_requirements_to_dicts,
    source_dsar_requirements_to_markdown,
)


def test_extracts_complete_dsar_access_request_requirements():
    result = build_source_dsar_requirements(
        _source(
            source_payload={
                "privacy": [
                    "DSAR access requests must verify requester identity and authorized agents.",
                    "Fulfillment SLA is 30 days from intake unless an extension is approved.",
                    "Data scope includes profile data, billing data, support tickets, and activity logs.",
                    "Delivery format is a secure download link with JSON and CSV in an encrypted zip.",
                    "Audit evidence records the case log, timestamps, reviewer, and proof of fulfillment.",
                    "Exceptions require legal review with redaction for third-party or security-sensitive data.",
                ]
            }
        )
    )

    assert isinstance(result, SourceDsarRequirementsReport)
    assert all(isinstance(record, SourceDsarRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "requester_verification",
        "fulfillment_sla",
        "data_scope",
        "delivery_format",
        "audit_evidence",
        "exception_redaction",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["requester_verification"].suggested_owner == "privacy_operations"
    assert any("source_payload.privacy[0]" in item for item in by_category["requester_verification"].evidence)
    assert result.summary["requirement_count"] == 6
    assert result.summary["category_counts"]["audit_evidence"] == 1


def test_common_support_export_wording_extracts_partial_requirements():
    result = build_source_dsar_requirements(
        _source(
            summary="Support needs a privacy request workflow for customers asking to download my data.",
            requirements=[
                "Verify account ownership before releasing the data export request.",
                "Email a secure link to a machine-readable CSV export.",
            ],
        )
    )

    assert [record.category for record in result.records] == [
        "requester_verification",
        "delivery_format",
    ]
    assert extract_source_dsar_requirements(_source(summary="DSAR must keep audit evidence."))[0].category == "audit_evidence"


def test_non_dsar_and_negated_briefs_return_empty_reports():
    empty = build_source_dsar_requirements(_source(summary="Refresh the billing dashboard export button copy."))
    negated = build_source_dsar_requirements(_source(summary="No DSAR access request changes are in scope."))

    assert empty.records == ()
    assert negated.records == ()
    assert empty.summary["requirement_count"] == 0
    assert empty.summary["categories"] == []
    assert "No DSAR requirements" in empty.to_markdown()


def test_serialization_helpers_are_stable():
    result = build_source_dsar_requirements(_source(summary="DSAR request must verify identity and respond within 30 days."))
    payload = source_dsar_requirements_to_dict(result)

    assert source_dsar_requirements_to_dicts(result) == payload["requirements"]
    assert source_dsar_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert "# Source DSAR Requirements Report: source-dsar" in source_dsar_requirements_to_markdown(result)


def _source(**overrides):
    payload = {
        "id": "source-dsar",
        "title": "Privacy operations brief",
        "summary": "",
        "source_payload": {},
    }
    payload.update(overrides)
    return payload
