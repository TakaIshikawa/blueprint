import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import SourceBrief
from blueprint.source_webhook_payload_validation_requirements import (
    SourceWebhookPayloadValidationRequirement,
    SourceWebhookPayloadValidationRequirementsReport,
    build_source_webhook_payload_validation_requirements,
    derive_source_webhook_payload_validation_requirements,
    extract_source_webhook_payload_validation_requirements,
    generate_source_webhook_payload_validation_requirements,
    source_webhook_payload_validation_requirements_to_dict,
    source_webhook_payload_validation_requirements_to_dicts,
    summarize_source_webhook_payload_validation_requirements,
)


def test_complete_webhook_payload_validation_extracts_all_requirement_signals():
    report = build_source_webhook_payload_validation_requirements(
        _source_brief(
            source_payload={
                "webhook_payload_validation": {
                    "schema": "Webhook payload schema validation must use JSON Schema.",
                    "required": "Payloads must include required fields event id, event type, and account id.",
                    "unknown": "Unknown fields are rejected unless additional properties are allowlisted.",
                    "version": "Versioned payloads use schema version v2.",
                    "signature": "Validate payload using the raw body before signature and timestamp checks.",
                    "malformed": "Malformed payloads return 422 validation error and do not process the event.",
                    "observability": "Validation logs, invalid payload count metrics, and dashboard alerts are required.",
                }
            }
        )
    )

    by_signal = {requirement.signal: requirement for requirement in report.records}

    assert isinstance(report, SourceWebhookPayloadValidationRequirementsReport)
    assert all(isinstance(record, SourceWebhookPayloadValidationRequirement) for record in report.records)
    assert list(by_signal) == [
        "schema_validation",
        "required_fields",
        "unknown_field_handling",
        "versioned_payloads",
        "signature_timestamp_coupling",
        "malformed_payload_response",
        "validation_observability",
    ]
    assert by_signal["versioned_payloads"].value == "v2"
    assert by_signal["malformed_payload_response"].value == "422"
    assert report.missing_signals == ()
    assert report.weak_signals == ()


def test_partial_webhook_validation_reports_missing_and_weak_signals():
    report = build_source_webhook_payload_validation_requirements(
        "We need webhook validation. Malformed payloads should return 400."
    )

    assert [requirement.signal for requirement in report.records] == ["malformed_payload_response"]
    assert "schema_validation" in report.missing_signals
    assert "required_fields" in report.missing_signals
    assert "unknown_field_handling" in report.missing_signals
    assert report.weak_signals
    assert "clarify concrete webhook payload validation rule" in report.weak_signals[0]


def test_retry_signing_and_ordering_language_is_not_payload_validation_by_itself():
    report = build_source_webhook_payload_validation_requirements(
        {
            "summary": (
                "Webhook retry uses exponential backoff. Signature verification uses HMAC. "
                "Ordering is not guaranteed."
            )
        }
    )

    assert report.records == ()
    assert report.summary["requirement_count"] == 0


def test_model_object_serialization_helpers_are_stable_without_mutation():
    source = _source_brief(
        summary="Webhook payload validation must handle schema version v1.",
        source_payload={
            "validation": [
                "Required fields include event id and event type.",
                "Validation metrics log malformed payload counts.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)
    obj = SimpleNamespace(
        id="object-webhook-validation",
        summary="Validate payloads.",
        source_payload={"schema": "JSON Schema validation rejects unknown fields."},
    )

    mapping_report = build_source_webhook_payload_validation_requirements(source)
    model_report = derive_source_webhook_payload_validation_requirements(model)
    generated = generate_source_webhook_payload_validation_requirements(model)
    object_report = build_source_webhook_payload_validation_requirements(obj)
    payload = source_webhook_payload_validation_requirements_to_dict(model_report)

    assert source == original
    assert mapping_report.to_dict() == model_report.to_dict()
    assert generated.to_dict() == model_report.to_dict()
    assert extract_source_webhook_payload_validation_requirements(model) == model_report.requirements
    assert source_webhook_payload_validation_requirements_to_dicts(model_report) == payload["requirements"]
    assert source_webhook_payload_validation_requirements_to_dicts(model_report.records) == payload["records"]
    assert summarize_source_webhook_payload_validation_requirements(model_report) == model_report.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == [
        "source_id",
        "requirements",
        "records",
        "missing_signals",
        "weak_signals",
        "summary",
    ]
    assert object_report.source_id == "object-webhook-validation"
    assert [record.signal for record in object_report.records] == [
        "schema_validation",
        "unknown_field_handling",
    ]


def test_absent_and_negated_webhook_validation_returns_stable_empty_report():
    empty = build_source_webhook_payload_validation_requirements({"summary": "Update dashboard copy only."})
    negated = build_source_webhook_payload_validation_requirements(
        {"summary": "No webhook payload validation or schema validation work is in scope."}
    )

    assert empty.records == ()
    assert negated.records == ()
    assert empty.missing_signals == (
        "schema_validation",
        "required_fields",
        "unknown_field_handling",
        "versioned_payloads",
        "signature_timestamp_coupling",
        "malformed_payload_response",
        "validation_observability",
    )
    assert empty.summary["requirement_count"] == 0
    assert source_webhook_payload_validation_requirements_to_dict(empty)["requirements"] == []


def _source_brief(**overrides):
    payload = {
        "id": "source-webhook-payload-validation",
        "source_project": "blueprint",
        "source_entity_type": "brief",
        "source_id": "source-webhook-payload-validation",
        "source_links": {},
        "title": "Webhook payload validation source",
        "summary": "Webhook payload validation constraints.",
        "source_payload": {},
    }
    payload.update(overrides)
    return payload
