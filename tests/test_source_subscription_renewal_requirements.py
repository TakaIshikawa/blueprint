import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_subscription_renewal_requirements import (
    SourceSubscriptionRenewalRequirement,
    SourceSubscriptionRenewalRequirementsReport,
    build_source_subscription_renewal_requirements,
    derive_source_subscription_renewal_requirements,
    extract_source_subscription_renewal_requirements,
    generate_source_subscription_renewal_requirements,
    source_subscription_renewal_requirements_to_dict,
    source_subscription_renewal_requirements_to_dicts,
    source_subscription_renewal_requirements_to_markdown,
    summarize_source_subscription_renewal_requirements,
)


def test_structured_payload_extracts_subscription_renewal_and_dunning_requirements():
    result = build_source_subscription_renewal_requirements(
        _source_brief(
            source_payload={
                "renewal_rules": {
                    "auto_renewal": "Subscriptions must auto-renew at term end unless the customer cancels.",
                    "notice": "Renewal notice emails must be sent 30 days before renewal.",
                    "grace": "A 7 day grace period keeps subscriptions active after a failed payment.",
                    "retry": "Payment retry cadence should attempt the card 3 times every 2 days.",
                    "dunning": "Dunning messages notify customers about failed payment recovery.",
                    "cancel": "Cancellation window requires customers to cancel 2 days before renewal.",
                    "price": "Renewal price change notice is required 45 days before a price increase.",
                    "access": "Failed payment access should become read-only after the grace period.",
                }
            }
        )
    )

    assert isinstance(result, SourceSubscriptionRenewalRequirementsReport)
    assert result.source_id == "source-renewal"
    assert all(
        isinstance(record, SourceSubscriptionRenewalRequirement) for record in result.records
    )
    assert [record.requirement_type for record in result.records] == [
        "auto_renewal",
        "renewal_notice",
        "grace_period",
        "payment_retry",
        "dunning_message",
        "cancellation_window",
        "renewal_price_change",
        "failed_payment_access",
    ]
    assert result.summary["requirement_count"] == 8
    assert result.summary["type_counts"] == {
        "auto_renewal": 1,
        "renewal_notice": 1,
        "grace_period": 1,
        "payment_retry": 1,
        "dunning_message": 1,
        "cancellation_window": 1,
        "renewal_price_change": 1,
        "failed_payment_access": 1,
    }
    assert result.summary["confidence_counts"] == {"high": 8, "medium": 0, "low": 0}
    assert any(
        "source_payload.renewal_rules.notice" in evidence
        for record in result.records
        for evidence in record.evidence
    )
    by_type = {record.requirement_type: record for record in result.records}
    assert by_type["auto_renewal"].value == "enabled"
    assert by_type["renewal_notice"].billing_surface == "notification"
    assert by_type["payment_retry"].billing_surface == "payment_method"
    assert by_type["failed_payment_access"].billing_surface == "account_access"


def test_markdown_plain_text_implementation_brief_and_object_inputs_are_supported():
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Renewal notice must notify account owners 60 days before renewal.",
                "Payment retry schedule retries failed invoices weekly for 4 attempts.",
            ],
            definition_of_done=[
                "Cancellation window is documented as 5 days before renewal.",
                "Failed payment access suspends account access after 14 days.",
            ],
        )
    )
    text = build_source_subscription_renewal_requirements(
        """
# Subscription Renewal

- Auto-renewal must be enabled for annual subscriptions.
- Dunning notification emails are required for past-due accounts.
"""
    )
    object_result = build_source_subscription_renewal_requirements(
        SimpleNamespace(
            id="object-renewal",
            summary="Grace period should last 10 days after payment failure.",
            metadata={"price": "Renewal price change notice must be sent 30 days before renewal."},
        )
    )

    model_records = extract_source_subscription_renewal_requirements(brief)

    assert {
        "renewal_notice",
        "payment_retry",
        "cancellation_window",
        "failed_payment_access",
    } == {record.requirement_type for record in model_records}
    assert text.records[0].requirement_type == "auto_renewal"
    assert text.records[1].requirement_type == "dunning_message"
    assert [record.requirement_type for record in object_result.records] == [
        "grace_period",
        "renewal_price_change",
    ]


def test_duplicate_semantic_requirements_collapse_with_strongest_evidence():
    result = build_source_subscription_renewal_requirements(
        {
            "id": "dupe-renewal",
            "source_payload": {
                "renewal": {
                    "notice": "Renewal notice must be sent 30 days before renewal.",
                    "same_notice": "Renewal notice must be sent 30 days before renewal.",
                    "soft_notice": "Renewal notice is part of the subscription lifecycle.",
                },
                "acceptance_criteria": [
                    "Renewal notice must be sent 30 days before renewal.",
                    "Payment retry cadence should retry failed payments every 3 days.",
                ],
            },
        }
    )

    assert [record.requirement_type for record in result.records] == [
        "renewal_notice",
        "payment_retry",
    ]
    assert result.records[0].evidence == (
        "source_payload.acceptance_criteria[0]: Renewal notice must be sent 30 days before renewal.",
        "source_payload.renewal.soft_notice: Renewal notice is part of the subscription lifecycle.",
    )
    assert result.records[0].value == "30 days"
    assert result.records[0].confidence == "high"
    assert result.summary["requirement_types"] == ["renewal_notice", "payment_retry"]


def test_sourcebrief_aliases_json_serialization_ordering_markdown_and_no_mutation():
    source = _source_brief(
        source_id="renewal-model",
        summary="Subscription renewal must include auto-renewal and cancellation windows.",
        source_payload={
            "requirements": [
                "Auto-renewal must stay enabled for monthly plans.",
                "Cancellation window must allow cancellation 1 day before renewal.",
                "Dunning message must escape customer | account notes.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_subscription_renewal_requirements(source)
    model_result = generate_source_subscription_renewal_requirements(model)
    derived = derive_source_subscription_renewal_requirements(model)
    payload = source_subscription_renewal_requirements_to_dict(model_result)
    markdown = source_subscription_renewal_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_subscription_renewal_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert (
        source_subscription_renewal_requirements_to_dicts(model_result) == payload["requirements"]
    )
    assert (
        source_subscription_renewal_requirements_to_dicts(model_result.records)
        == payload["records"]
    )
    assert summarize_source_subscription_renewal_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "requirement_type",
        "billing_surface",
        "value",
        "evidence",
        "confidence",
        "source_id",
    ]
    assert [(record.source_id, record.requirement_type) for record in model_result.records] == [
        ("renewal-model", "auto_renewal"),
        ("renewal-model", "dunning_message"),
        ("renewal-model", "cancellation_window"),
    ]
    assert markdown == model_result.to_markdown()
    assert "| Type | Billing Surface | Value | Confidence | Source | Evidence |" in markdown
    assert "customer \\| account notes" in markdown


def test_empty_invalid_negated_and_malformed_inputs_return_stable_empty_reports():
    class BriefLike:
        id = "object-empty"
        summary = "No renewal notice or dunning changes are required for this copy update."

    empty = build_source_subscription_renewal_requirements(
        _source_brief(source_id="empty-renewal", summary="Update billing copy only.")
    )
    repeat = build_source_subscription_renewal_requirements(
        _source_brief(source_id="empty-renewal", summary="Update billing copy only.")
    )
    negated = build_source_subscription_renewal_requirements(BriefLike())
    malformed = build_source_subscription_renewal_requirements(
        {"source_payload": {"notes": object()}}
    )
    invalid = build_source_subscription_renewal_requirements(42)

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "type_counts": {
            "auto_renewal": 0,
            "renewal_notice": 0,
            "grace_period": 0,
            "payment_retry": 0,
            "dunning_message": 0,
            "cancellation_window": 0,
            "renewal_price_change": 0,
            "failed_payment_access": 0,
        },
        "billing_surface_counts": {
            "subscription": 0,
            "invoice": 0,
            "payment_method": 0,
            "account_access": 0,
            "notification": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "requirement_types": [],
        "billing_surfaces": [],
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "empty-renewal"
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No subscription renewal requirements were found" in empty.to_markdown()
    assert negated.records == ()
    assert malformed.records == ()
    assert invalid.records == ()


def _source_brief(
    *,
    source_id="source-renewal",
    title="Subscription renewal requirements",
    domain="billing",
    summary="General subscription renewal requirements.",
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


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "impl-renewal",
        "source_brief_id": "source-renewal",
        "title": "Subscription renewal handling",
        "domain": "billing",
        "target_user": "customers",
        "buyer": None,
        "workflow_context": "Subscription renewal and dunning planning.",
        "problem_statement": "Customers need predictable renewal handling.",
        "mvp_goal": "Ship subscription renewal constraints.",
        "product_surface": "billing settings",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run subscription renewal validation.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
