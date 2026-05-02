import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_subscription_cancellation_requirements import (
    SourceSubscriptionCancellationRequirement,
    SourceSubscriptionCancellationRequirementsReport,
    build_source_subscription_cancellation_requirements,
    derive_source_subscription_cancellation_requirements,
    extract_source_subscription_cancellation_requirements,
    generate_source_subscription_cancellation_requirements,
    source_subscription_cancellation_requirements_to_dict,
    source_subscription_cancellation_requirements_to_dicts,
    source_subscription_cancellation_requirements_to_markdown,
    summarize_source_subscription_cancellation_requirements,
)


def test_markdown_brief_extracts_all_subscription_cancellation_categories():
    result = build_source_subscription_cancellation_requirements(
        _source_brief(
            source_payload={
                "body": """
# Subscription Cancellation Requirements

- Self-service cancel must let account owners cancel a subscription from billing settings.
- Cancellation window requires 30 days advance notice before renewal.
- End-of-term access keeps access through the paid billing period.
- Immediate termination must revoke access immediately for fraud or abuse.
- Refund policy defines partial refund eligibility and account credit for unused value.
- Retention offer should show a pause plan option before final cancellation.
- Reactivation path must let customers resubscribe and restore the subscription.
- Cancellation receipt sends a confirmation email and records an audit trail.
""",
                "metadata": {
                    "audit_receipt": "Support audit logs include cancellation timestamp and cancellation reason.",
                },
            }
        )
    )

    assert isinstance(result, SourceSubscriptionCancellationRequirementsReport)
    assert all(
        isinstance(record, SourceSubscriptionCancellationRequirement) for record in result.records
    )
    assert [record.category for record in result.records] == [
        "self_service_cancel",
        "cancellation_window",
        "end_of_term_access",
        "immediate_termination",
        "refund_credit_policy",
        "retention_offer",
        "reactivation_path",
        "audit_receipt",
    ]
    by_category = {record.category: record for record in result.records}
    assert any("source_payload.body" in item for item in by_category["self_service_cancel"].evidence)
    assert any(
        "source_payload.metadata.audit_receipt" in item
        for item in by_category["audit_receipt"].evidence
    )
    assert by_category["refund_credit_policy"].suggested_owner == "finance_ops"
    assert "confirmation receipts" in by_category["audit_receipt"].suggested_planning_note
    assert result.summary["requirement_count"] == 8
    assert result.summary["category_counts"]["retention_offer"] == 1
    assert result.summary["high_confidence_count"] >= 6


def test_structured_payload_metadata_and_free_text_contribute_without_mutation():
    source = _source_brief(
        source_id="cancel-model",
        summary="Cancellation must include self-service cancel and cancellation receipt behavior.",
        source_payload={
            "cancellation": {
                "window": "Cancellation window requires notice before renewal.",
                "access": "End-of-term access keeps access through the billing cycle.",
            },
            "metadata": {
                "refund_credit_policy": "Credit policy must credit unused value after cancellation.",
                "retention_offer": "Retention offer should include a discount to stay.",
            },
        },
    )
    original = copy.deepcopy(source)

    mapping_result = build_source_subscription_cancellation_requirements(source)
    model_result = generate_source_subscription_cancellation_requirements(
        SourceBrief.model_validate(source)
    )

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert [record.category for record in model_result.records] == [
        "self_service_cancel",
        "cancellation_window",
        "end_of_term_access",
        "refund_credit_policy",
        "retention_offer",
        "audit_receipt",
    ]
    by_category = {record.category: record for record in model_result.records}
    assert any("summary" in item for item in by_category["self_service_cancel"].evidence)
    assert any(
        "source_payload.cancellation.window" in item
        for item in by_category["cancellation_window"].evidence
    )
    assert any(
        "source_payload.metadata.refund_credit_policy" in item
        for item in by_category["refund_credit_policy"].evidence
    )


def test_implementation_brief_fields_and_object_inputs_are_supported():
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Cancel flow must support self-service cancel from account settings.",
                "Immediate termination must shut off access for abuse cases.",
            ],
            risks=[
                "Refund policy needs finance review before launch.",
                "Reactivation path can fail if canceled subscription state is not retained.",
            ],
            definition_of_done=[
                "Cancellation confirmation email records a cancellation event for audit.",
                "End-of-term access remains active until the end of current term.",
            ],
        )
    )
    object_result = build_source_subscription_cancellation_requirements(
        SimpleNamespace(
            id="object-cancel",
            summary="Retention flow should show a save offer before customers cancel.",
            metadata={"window": "Cancel deadline must be before renewal."},
        )
    )
    text_result = build_source_subscription_cancellation_requirements(
        "Customers should resubscribe through a reactivation path after cancellation."
    )

    model_result = build_source_subscription_cancellation_requirements(brief)

    assert model_result.source_id == "impl-cancel"
    assert [record.category for record in model_result.records] == [
        "self_service_cancel",
        "end_of_term_access",
        "immediate_termination",
        "refund_credit_policy",
        "reactivation_path",
        "audit_receipt",
    ]
    assert model_result.records[0].evidence == (
        "scope[0]: Cancel flow must support self-service cancel from account settings.",
    )
    assert [record.category for record in object_result.records] == [
        "cancellation_window",
        "retention_offer",
    ]
    assert text_result.records[0].category == "reactivation_path"


def test_duplicate_evidence_merges_deterministically_and_limits_categories():
    result = build_source_subscription_cancellation_requirements(
        {
            "id": "dupe-cancel",
            "source_payload": {
                "acceptance_criteria": [
                    "Cancellation receipt must send a confirmation email.",
                    "Cancellation receipt must send a confirmation email.",
                    "Refund policy must issue account credit for unused value.",
                ],
                "metadata": {
                    "same_receipt": "Cancellation receipt must send a confirmation email.",
                    "same_refund": "Refund policy must issue account credit for unused value.",
                },
            },
        }
    )

    assert [record.category for record in result.records] == [
        "refund_credit_policy",
        "audit_receipt",
    ]
    assert result.records[0].evidence == (
        "source_payload.acceptance_criteria[2]: Refund policy must issue account credit for unused value.",
    )
    assert result.records[1].evidence == (
        "source_payload.acceptance_criteria[0]: Cancellation receipt must send a confirmation email.",
    )
    assert result.records[0].confidence == 0.95
    assert result.records[1].confidence == 0.95
    assert result.summary["categories"] == ["refund_credit_policy", "audit_receipt"]


def test_serialization_markdown_summary_helpers_and_iterable_inputs_are_stable():
    source = _source_brief(
        source_id="source-cancel-json",
        summary="Self-service cancel must expose a cancellation settings button.",
        source_payload={
            "requirements": [
                "End-of-term access keeps access through the paid term.",
                "Cancellation receipt must escape support | finance notes.",
            ],
        },
    )
    second = {
        "id": "source-cancel-json-2",
        "source_payload": {
            "requirements": [
                "Retention offer should include pause plan eligibility.",
                "Reactivation path must restore the subscription after resubscribe.",
            ]
        },
    }

    model_result = build_source_subscription_cancellation_requirements(
        SourceBrief.model_validate(source)
    )
    iterable_result = derive_source_subscription_cancellation_requirements([source, second])
    extracted = extract_source_subscription_cancellation_requirements(source)
    payload = source_subscription_cancellation_requirements_to_dict(model_result)
    markdown = source_subscription_cancellation_requirements_to_markdown(model_result)

    assert extracted == model_result.requirements
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_subscription_cancellation_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_subscription_cancellation_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_subscription_cancellation_requirements(model_result) == model_result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "category",
        "confidence",
        "evidence",
        "suggested_owner",
        "suggested_planning_note",
    ]
    assert iterable_result.source_id is None
    assert "retention_offer" in iterable_result.summary["categories"]
    assert markdown == model_result.to_markdown()
    assert markdown.startswith(
        "# Source Subscription Cancellation Requirements Report: source-cancel-json"
    )
    assert "| Category | Confidence | Evidence | Suggested Owner | Suggested Planning Note |" in markdown
    assert "support \\| finance notes" in markdown


def test_empty_invalid_and_negated_inputs_return_stable_empty_reports():
    empty = build_source_subscription_cancellation_requirements(
        _source_brief(source_id="empty-cancel", summary="Update billing settings copy only.")
    )
    repeat = build_source_subscription_cancellation_requirements(
        _source_brief(source_id="empty-cancel", summary="Update billing settings copy only.")
    )
    negated = build_source_subscription_cancellation_requirements(
        {
            "id": "negated-cancel",
            "summary": "No cancellation changes required for this account settings copy update.",
            "source_payload": {
                "non_goals": [
                    "No subscription cancellation requirements are needed in this release."
                ]
            },
        }
    )
    invalid = build_source_subscription_cancellation_requirements(42)
    malformed = build_source_subscription_cancellation_requirements(
        {"source_payload": {"notes": object()}}
    )

    expected_summary = {
        "requirement_count": 0,
        "category_counts": {
            "self_service_cancel": 0,
            "cancellation_window": 0,
            "end_of_term_access": 0,
            "immediate_termination": 0,
            "refund_credit_policy": 0,
            "retention_offer": 0,
            "reactivation_path": 0,
            "audit_receipt": 0,
        },
        "high_confidence_count": 0,
        "categories": [],
        "suggested_owner_counts": {},
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "empty-cancel"
    assert empty.requirements == ()
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No subscription cancellation requirements were found" in empty.to_markdown()
    assert negated.requirements == ()
    assert invalid.source_id is None
    assert invalid.requirements == ()
    assert malformed.requirements == ()
    assert invalid.summary == expected_summary


def _source_brief(
    *,
    source_id="source-cancel",
    title="Subscription cancellation requirements",
    domain="billing",
    summary="General subscription cancellation requirements.",
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


def _implementation_brief(*, scope=None, risks=None, definition_of_done=None):
    return {
        "id": "impl-cancel",
        "source_brief_id": "source-cancel",
        "title": "Subscription cancellation",
        "domain": "billing",
        "target_user": "subscribers",
        "buyer": None,
        "workflow_context": "Subscription cancellation before task generation.",
        "problem_statement": "Customers need predictable subscription cancellation behavior.",
        "mvp_goal": "Ship cancellation lifecycle planning.",
        "product_surface": "billing settings",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [] if risks is None else risks,
        "validation_plan": "Validate subscription cancellation scenarios.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
