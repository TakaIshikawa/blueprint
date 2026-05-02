import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_payment_method_update_requirements import (
    SourcePaymentMethodUpdateRequirement,
    SourcePaymentMethodUpdateRequirementsReport,
    build_source_payment_method_update_requirements,
    derive_source_payment_method_update_requirements,
    extract_source_payment_method_update_requirements,
    generate_source_payment_method_update_requirements,
    source_payment_method_update_requirements_to_dict,
    source_payment_method_update_requirements_to_dicts,
    source_payment_method_update_requirements_to_markdown,
    summarize_source_payment_method_update_requirements,
)


def test_structured_billing_metadata_extracts_payment_method_update_requirements():
    result = build_source_payment_method_update_requirements(
        _source_brief(
            source_payload={
                "billing": {
                    "card_update": (
                        "Expired card updates must support customer replacement cards, set the new card "
                        "as the default payment method, require SCA and 3DS reauthentication, resume "
                        "invoice retry for past due invoices, notify customers, and record audit evidence."
                    ),
                    "bank_account": (
                        "Bank account changes should require mandate acceptance, send confirmation email, "
                        "and track actor and timestamp in the audit log."
                    ),
                }
            }
        )
    )

    assert isinstance(result, SourcePaymentMethodUpdateRequirementsReport)
    assert all(isinstance(record, SourcePaymentMethodUpdateRequirement) for record in result.records)
    assert result.source_id == "source-payment-methods"
    assert len(result.records) == 2
    by_type = {record.payment_method_type: record for record in result.records}

    assert by_type["card"].update_trigger == "expired_or_expiring"
    assert by_type["card"].defaulting_behavior == "default payment method"
    assert by_type["card"].authentication_requirement == "sca"
    assert by_type["card"].retry_or_dunning_linkage == "invoice retry"
    assert by_type["card"].notification_requirement == "notify"
    assert any("audit evidence" in note.casefold() for note in by_type["card"].planning_notes)
    assert by_type["bank_account"].authentication_requirement == "mandate acceptance"
    assert by_type["bank_account"].notification_requirement == "email"
    assert by_type["bank_account"].confidence == "high"
    assert result.summary["requirement_count"] == 2
    assert result.summary["payment_method_types"] == ["card", "bank_account"]
    assert result.summary["requires_defaulting"] is True
    assert result.summary["requires_authentication"] is True
    assert result.summary["requires_retry_or_dunning_linkage"] is True
    assert result.summary["requires_notifications"] is True
    assert result.summary["status"] == "ready_for_payment_method_update_planning"


def test_acceptance_criteria_and_implementation_brief_extract_multiple_requirements():
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Customers must update credit cards from the billing portal and make the new card the default.",
                "Failed payment dunning should link successful card updates to invoice retry.",
            ],
            definition_of_done=[
                "3DS reauthentication is required before the updated card is saved.",
                "Customer notification email is sent after payment method changes.",
            ],
        )
    )
    text_result = build_source_payment_method_update_requirements(
        """
# Payment method updates

- Account owners can replace bank account details for ACH direct debit.
- The updated bank account requires mandate acceptance and becomes the default bank for future invoices.
"""
    )
    implementation_result = generate_source_payment_method_update_requirements(implementation)

    assert [record.payment_method_type for record in text_result.records] == ["bank_account"]
    assert text_result.records[0].authentication_requirement == "mandate acceptance"
    assert text_result.records[0].defaulting_behavior == "default bank"
    assert implementation_result.source_id == "implementation-payment-methods"
    assert {record.payment_method_type for record in implementation_result.records} == {"card", "payment_method"}
    assert any(record.retry_or_dunning_linkage == "invoice retry" for record in implementation_result.records)
    assert any(record.authentication_requirement == "3ds" for record in implementation_result.records)


def test_duplicate_merging_stable_dicts_markdown_aliases_and_no_mutation():
    source = _source_brief(
        source_id="payment-method-model",
        source_payload={
            "acceptance_criteria": [
                "Card updates must set the new card as default for customer | invoice collection.",
                "Card updates must set the new card as default for customer | invoice collection.",
                "Updated cards must trigger invoice retry after SCA reauthentication.",
                "Payment method update notification email is required.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_payment_method_update_requirements(source)
    model_result = extract_source_payment_method_update_requirements(model)
    derived = derive_source_payment_method_update_requirements(model)
    payload = source_payment_method_update_requirements_to_dict(model_result)
    markdown = source_payment_method_update_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_payment_method_update_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_payment_method_update_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_payment_method_update_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "source_field",
        "update_trigger",
        "payment_method_type",
        "defaulting_behavior",
        "authentication_requirement",
        "retry_or_dunning_linkage",
        "notification_requirement",
        "evidence",
        "confidence",
        "planning_notes",
    ]
    assert len(model_result.records) == 2
    by_type = {record.payment_method_type: record for record in model_result.records}
    assert by_type["card"].defaulting_behavior == "as default"
    assert by_type["card"].authentication_requirement == "sca"
    assert by_type["card"].retry_or_dunning_linkage == "invoice retry"
    assert by_type["payment_method"].notification_requirement == "email"
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Source Field | Update Trigger | Payment Method Type |" in markdown
    assert "customer \\| invoice collection" in markdown


def test_explicit_out_of_scope_unrelated_malformed_and_object_inputs_are_stable_empty():
    class BriefLike:
        id = "object-no-updates"
        summary = "No payment method updates, card changes, or bank account changes are required for this release."

    object_result = build_source_payment_method_update_requirements(
        SimpleNamespace(
            id="object-payment-methods",
            summary="Payment method updates must notify customers and record audit evidence.",
            metadata={"defaulting": "Default payment method selection should apply to future invoices."},
        )
    )
    negated = build_source_payment_method_update_requirements(BriefLike())
    no_scope = build_source_payment_method_update_requirements(
        _source_brief(summary="Payment method updates are out of scope and no card update work is planned.")
    )
    unrelated_refund = build_source_payment_method_update_requirements(
        _source_brief(
            title="Refund workflow",
            summary="Refunds should store provider reference ids and notify customers.",
            source_payload={"requirements": ["Invoice retry labels and dunning copy are updated."]},
        )
    )
    malformed = build_source_payment_method_update_requirements({"source_payload": {"notes": object()}})
    blank = build_source_payment_method_update_requirements("")

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "payment_method_types": [],
        "update_triggers": [],
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "requires_defaulting": False,
        "requires_authentication": False,
        "requires_retry_or_dunning_linkage": False,
        "requires_notifications": False,
        "status": "no_payment_method_update_language",
    }
    assert [record.payment_method_type for record in object_result.records] == ["payment_method"]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated_refund.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert unrelated_refund.summary == expected_summary
    assert unrelated_refund.to_dicts() == []
    assert "No source payment method update requirements were inferred." in unrelated_refund.to_markdown()


def _source_brief(
    *,
    source_id="source-payment-methods",
    title="Payment method update requirements",
    domain="billing",
    summary="General payment method update requirements.",
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


def _implementation_brief(
    *,
    scope=None,
    definition_of_done=None,
):
    return {
        "id": "implementation-payment-methods",
        "source_brief_id": "source-payment-methods",
        "title": "Payment method update implementation",
        "domain": "billing",
        "target_user": "customers",
        "buyer": "finance",
        "workflow_context": "Subscription billing and payment recovery.",
        "problem_statement": "Customers need reliable payment method updates.",
        "mvp_goal": "Plan customer payment method update flows.",
        "product_surface": "billing",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "risks": [],
        "architecture_notes": None,
        "data_requirements": None,
        "validation_plan": "Run payment method update extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "integration_points": [],
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
