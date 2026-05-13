import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_data_loss_prevention_requirements import (
    SourceDataLossPreventionRequirement,
    SourceDataLossPreventionRequirementsReport,
    build_source_data_loss_prevention_requirements,
    derive_source_data_loss_prevention_requirements,
    extract_source_data_loss_prevention_requirements,
    generate_source_data_loss_prevention_requirements,
    source_data_loss_prevention_requirements_to_dict,
    source_data_loss_prevention_requirements_to_dicts,
    source_data_loss_prevention_requirements_to_markdown,
    summarize_source_data_loss_prevention_requirements,
)


def test_structured_brief_extracts_ordered_dlp_records_with_evidence():
    result = build_source_data_loss_prevention_requirements(
        _source_brief(
            source_payload={
                "security": {
                    "requirements": [
                        "Sensitive data discovery must scan uploads for PII and secrets; security owns review.",
                        "Outbound sharing controls must block public links and external recipients unless domains are approved.",
                        "Clipboard and download restrictions must disable copy, paste, export, print, and screenshot for support roles in the browser.",
                        "Watermarking must stamp PDF export and screen preview with user id, email, timestamp, and tenant id.",
                        "DLP alerting must send high severity events to SIEM and PagerDuty for security triage owner.",
                        "Exception approval must require compliance approver, audit ticket evidence, and expiry after 14 days.",
                    ]
                }
            }
        )
    )

    assert isinstance(result, SourceDataLossPreventionRequirementsReport)
    assert result.source_id == "sb-dlp"
    assert all(isinstance(record, SourceDataLossPreventionRequirement) for record in result.records)
    assert [record.requirement_type for record in result.records] == [
        "sensitive_data_discovery",
        "outbound_sharing_controls",
        "clipboard_download_restrictions",
        "watermarking",
        "alerting",
        "exception_approval",
    ]
    assert all(record.confidence == "high" for record in result.records)
    assert all(record.evidence and record.source_field in record.evidence[0] for record in result.records)
    assert result.records[0].source_field == "source_payload.security.requirements[0]"
    assert result.summary["requirement_count"] == 6


def test_free_text_and_implementation_brief_inputs_are_supported():
    text_result = build_source_data_loss_prevention_requirements(
        """
        Data loss prevention requirements:
        - Discover sensitive data and classify PII in workspace documents.
        - Outbound sharing controls should restrict external sharing by approved domains.
        - Watermark downloads with user id.
        """
    )
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "DLP alerting must notify Slack and SIEM with high severity.",
                "Exception approval requires security approver and audit evidence before an override.",
            ],
            definition_of_done=[
                "Clipboard restrictions block copy, paste, download, and export for contractor roles.",
            ],
        )
    )

    assert [record.requirement_type for record in text_result.records] == [
        "sensitive_data_discovery",
        "outbound_sharing_controls",
        "watermarking",
    ]
    model_result = generate_source_data_loss_prevention_requirements(implementation)
    assert model_result.source_id == "impl-dlp"
    assert [record.requirement_type for record in model_result.records] == [
        "clipboard_download_restrictions",
        "alerting",
        "exception_approval",
    ]


def test_partial_briefs_return_stable_missing_detail_guidance():
    result = build_source_data_loss_prevention_requirements(
        _source_brief(source_payload={"requirements": ["DLP alerting is required.", "Watermarking is needed."]})
    )

    assert [record.requirement_type for record in result.records] == ["watermarking", "alerting"]
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["watermarking"].missing_details == (
        "watermark_content",
        "surfaces",
        "exception_behavior",
    )
    assert by_type["alerting"].missing_details == ("alert_destination", "severity", "triage_owner")
    assert by_type["alerting"].missing_detail_guidance == "alert_destination; severity; triage_owner"
    assert result.summary["missing_detail_count"] == 6
    assert result.records[0].readiness == "needs_detail"


def test_noisy_negated_and_malformed_inputs_return_empty_reports():
    empty = build_source_data_loss_prevention_requirements(
        _source_brief(
            summary="Dashboard copy update.",
            source_payload={"body": "No DLP, data loss prevention, outbound sharing, clipboard, download, or watermark work is required."},
        )
    )
    repeat = build_source_data_loss_prevention_requirements(
        _source_brief(
            summary="Dashboard copy update.",
            source_payload={"body": "No DLP, data loss prevention, outbound sharing, clipboard, download, or watermark work is required."},
        )
    )
    malformed = build_source_data_loss_prevention_requirements({"id": "bad", "source_payload": {"notes": object()}})
    invalid = build_source_data_loss_prevention_requirements(42)

    assert empty.to_dict() == repeat.to_dict()
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary["requirement_count"] == 0
    assert empty.summary["requirement_type_counts"]["alerting"] == 0
    assert "No data loss prevention requirements were inferred." in empty.to_markdown()
    assert malformed.records == ()
    assert invalid.records == ()


def test_serialization_aliases_json_ordering_and_no_input_mutation_are_stable():
    source = _source_brief(
        source_payload={
            "requirements": [
                "Outbound sharing controls must block external recipients | audit note.",
                "DLP alerting must notify SIEM.",
            ]
        }
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_data_loss_prevention_requirements(source)
    model_result = extract_source_data_loss_prevention_requirements(model)
    derived = derive_source_data_loss_prevention_requirements(model)
    payload = source_data_loss_prevention_requirements_to_dict(model_result)
    markdown = source_data_loss_prevention_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_data_loss_prevention_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert source_data_loss_prevention_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_data_loss_prevention_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_data_loss_prevention_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert model_result.records[0].category == "outbound_sharing_controls"
    assert model_result.records[0].data_loss_prevention_category == "outbound_sharing_controls"
    assert markdown == model_result.to_markdown()
    assert "external recipients \\| audit note" in markdown


def _source_brief(*, source_id="sb-dlp", title="DLP requirements", summary="DLP planning.", source_payload=None):
    return {
        "id": source_id,
        "title": title,
        "domain": "security",
        "summary": summary,
        "source_project": "project",
        "source_entity_type": "issue",
        "source_id": "issue-1",
        "source_payload": source_payload or {},
        "source_links": {},
    }


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "impl-dlp",
        "source_brief_id": "sb-dlp",
        "title": "Implementation DLP",
        "domain": "security",
        "problem_statement": "Prevent accidental sensitive data loss.",
        "mvp_goal": "Add DLP safeguards.",
        "scope": scope or [],
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "validation_plan": "Run security tests.",
        "definition_of_done": definition_of_done or [],
    }
