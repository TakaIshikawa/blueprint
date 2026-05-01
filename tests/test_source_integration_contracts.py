import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_integration_contracts import (
    SourceIntegrationContract,
    extract_source_integration_contracts,
    source_integration_contracts_to_dicts,
)


def test_api_rest_graphql_webhook_and_schema_contracts_are_detected():
    records = extract_source_integration_contracts(
        _source_brief(
            summary="Checkout must call Stripe REST API endpoint POST /v1/payment_intents.",
            source_payload={
                "requirements": [
                    (
                        "Receive webhooks from Stripe for events `payment_intent.succeeded` and "
                        "`payment_intent.payment_failed`; required fields include id, amount, currency."
                    ),
                    "Admin app queries Shopify GraphQL API with fields: order.id, customer.email.",
                    "Use the OpenAPI schema contract for provider: Stripe.",
                ]
            },
        )
    )

    assert all(isinstance(record, SourceIntegrationContract) for record in records)
    by_type = {record.contract_type: record for record in records}
    assert {"api", "rest", "graphql", "webhook", "schema"} <= set(by_type)
    assert by_type["api"].provider_or_system == "Stripe"
    assert by_type["api"].direction == "outbound"
    assert by_type["rest"].direction == "outbound"
    assert by_type["webhook"].provider_or_system == "Stripe"
    assert by_type["webhook"].direction == "inbound"
    assert "payment_intent.succeeded" in by_type["webhook"].required_fields_or_events
    assert "payment_intent.payment_failed" in by_type["webhook"].required_fields_or_events
    assert "id" in by_type["webhook"].required_fields_or_events
    assert by_type["graphql"].provider_or_system == "Shopify"
    assert "order.id" in by_type["graphql"].required_fields_or_events
    assert by_type["schema"].provider_or_system == "Stripe"
    assert any("source_payload.requirements[0]" in item for item in by_type["webhook"].evidence)


def test_oauth_sso_file_event_and_queue_contracts_infer_direction_and_fields():
    records = extract_source_integration_contracts(
        _source_brief(
            source_payload={
                "body": (
                    "- OAuth with GitHub must request scopes repo, read:user, and user:email.\n"
                    "- SSO from Okta uses SAML claims: email, groups, name_id.\n"
                    "- Two-way import and export with NetSuite CSV over SFTP; columns include account_id, balance.\n"
                    "- Publish events to Kafka topic `invoice.paid` and consume queue messages from SQS."
                )
            }
        )
    )

    by_type = {record.contract_type: record for record in records}
    assert by_type["oauth"].provider_or_system == "GitHub"
    assert by_type["oauth"].direction == "unknown"
    assert "repo" in by_type["oauth"].required_fields_or_events
    assert "user:email" in by_type["oauth"].required_fields_or_events
    assert by_type["sso"].provider_or_system == "Okta"
    assert by_type["sso"].direction == "inbound"
    assert "email" in by_type["sso"].required_fields_or_events
    assert by_type["file_import_export"].provider_or_system == "NetSuite"
    assert by_type["file_import_export"].direction == "bidirectional"
    assert "account_id" in by_type["file_import_export"].required_fields_or_events
    assert by_type["event"].provider_or_system == "Kafka"
    assert by_type["event"].direction == "bidirectional"
    assert "invoice.paid" in by_type["event"].required_fields_or_events
    assert by_type["message_queue"].direction == "bidirectional"
    assert any("dead-letter" in question for question in by_type["message_queue"].open_questions)


def test_metadata_and_links_can_supply_contract_signals_without_mutation():
    source = _source_brief(
        summary="Partner sync should support a bidirectional contract.",
        source_payload={
            "metadata": {
                "provider": "Workday",
                "integration": "Workday API must read and write employee records.",
            },
            "nested": {"queue": "Consume messages from PayrollEvents queue."},
        },
        source_links={"contract": "https://example.test/contracts/workday-openapi"},
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    records = extract_source_integration_contracts(model)
    payload = source_integration_contracts_to_dicts(records)

    assert source == original
    assert json.loads(json.dumps(payload)) == payload
    assert any(
        record.contract_type == "api"
        and record.provider_or_system == "Workday"
        and record.direction == "bidirectional"
        for record in records
    )
    assert any(record.contract_type == "message_queue" for record in records)
    assert list(payload[0]) == [
        "provider_or_system",
        "contract_type",
        "required_fields_or_events",
        "direction",
        "evidence",
        "open_questions",
    ]


def test_open_questions_capture_missing_contract_details():
    records = extract_source_integration_contracts(
        _source_brief(summary="Add a webhook integration for order updates.")
    )

    assert len(records) == 1
    record = records[0]
    assert record.contract_type == "webhook"
    assert record.provider_or_system == ""
    assert record.direction == "unknown"
    assert record.required_fields_or_events == ()
    assert "Confirm the provider or internal system that owns this contract." in record.open_questions
    assert "Confirm whether the integration is inbound, outbound, or bidirectional." in record.open_questions
    assert "List required fields, events, claims, topics, or file columns for this contract." in record.open_questions


def test_empty_partial_or_non_model_sources_do_not_raise():
    assert extract_source_integration_contracts(_source_brief(summary="General background only.")) == ()
    assert extract_source_integration_contracts({"source_payload": "not a mapping"}) == ()
    assert extract_source_integration_contracts("not a source brief") == ()
    assert extract_source_integration_contracts(None) == ()


def _source_brief(
    *,
    source_id="sb-integrations",
    title="Integration requirements",
    domain="platform",
    summary="General integration requirements.",
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
