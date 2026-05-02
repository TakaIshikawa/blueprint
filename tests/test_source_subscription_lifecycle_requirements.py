import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_subscription_lifecycle_requirements import (
    SourceSubscriptionLifecycleRequirement,
    SourceSubscriptionLifecycleRequirementsReport,
    build_source_subscription_lifecycle_requirements,
    extract_source_subscription_lifecycle_requirements,
    generate_source_subscription_lifecycle_requirements,
    source_subscription_lifecycle_requirements_to_dict,
    source_subscription_lifecycle_requirements_to_dicts,
    source_subscription_lifecycle_requirements_to_markdown,
    summarize_source_subscription_lifecycle_requirements,
)


def test_markdown_and_structured_source_payload_extract_lifecycle_categories_with_evidence():
    result = build_source_subscription_lifecycle_requirements(
        _source_brief(
            source_payload={
                "body": """
# Subscription Lifecycle Requirements

- Trial conversion must convert free trial customers to paid billing at trial end.
- Cancellation flow must support cancel at period end and confirmation messaging.
- Pause and resume must support temporarily suspended subscriptions.
- Renewal notice emails are required before auto-renewal.
- Plan change should support upgrades, downgrades, and proration.
- Entitlement sync must grant and revoke access from subscription status.
- Dunning notices are needed for failed payment retries and grace periods.
""",
                "subscription": {
                    "entitlement_sync": "Seat entitlement changes must sync product access.",
                    "dunning_notice": "Past due subscriptions receive a payment failure notice.",
                },
            }
        )
    )

    assert isinstance(result, SourceSubscriptionLifecycleRequirementsReport)
    assert all(isinstance(record, SourceSubscriptionLifecycleRequirement) for record in result.records)
    assert [record.category for record in result.requirements] == [
        "trial_conversion",
        "cancellation",
        "pause_resume",
        "renewal_notice",
        "plan_change",
        "entitlement_sync",
        "dunning_notice",
    ]
    by_category = {record.category: record for record in result.records}
    assert any("source_payload.body" in item for item in by_category["trial_conversion"].evidence)
    assert any(
        "source_payload.subscription.entitlement_sync" in item
        for item in by_category["entitlement_sync"].evidence
    )
    assert by_category["renewal_notice"].suggested_owner == "customer_comms"
    assert "retry cadence" in by_category["dunning_notice"].suggested_planning_note
    assert result.summary["requirement_count"] == 7
    assert result.summary["category_counts"]["pause_resume"] == 1
    assert result.summary["high_confidence_count"] >= 4


def test_implementation_brief_risks_architecture_notes_and_done_contribute_evidence():
    model = ImplementationBrief.model_validate(
        _implementation(
            architecture_notes=(
                "Billing service must sync entitlements from subscription status and revoke access on cancellation."
            ),
            risks=[
                "Trial conversion can fail if paid billing does not start when the free trial ends.",
                "Dunning notices and payment retry grace periods are required before launch.",
            ],
            definition_of_done=[
                "Renewal notice emails are sent before auto-renewal.",
                "Plan change supports upgrade, downgrade, and prorated seat changes.",
            ],
        )
    )

    result = build_source_subscription_lifecycle_requirements(model)

    assert result.source_id == "impl-subscription"
    assert [record.category for record in result.records] == [
        "trial_conversion",
        "cancellation",
        "renewal_notice",
        "plan_change",
        "entitlement_sync",
        "dunning_notice",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["entitlement_sync"].evidence == (
        "architecture_notes: Billing service must sync entitlements from subscription status and revoke access on cancellation.",
    )
    assert (
        "risks[0]: Trial conversion can fail if paid billing does not start when the free trial ends."
        in by_category["trial_conversion"].evidence
    )
    assert by_category["renewal_notice"].evidence == (
        "definition_of_done[0]: Renewal notice emails are sent before auto-renewal.",
    )
    assert by_category["dunning_notice"].confidence >= 0.85


def test_duplicate_categories_merge_deterministically_with_stable_confidence():
    result = build_source_subscription_lifecycle_requirements(
        {
            "id": "dupe-subscription",
            "source_payload": {
                "subscription": {
                    "trial_conversion": "Trial conversion must convert free trial customers to paid billing.",
                    "same_trial_conversion": "Trial conversion must convert free trial customers to paid billing.",
                    "plan_change": "Plan change must support upgrades and downgrades with proration.",
                },
                "acceptance_criteria": [
                    "Trial conversion must convert free trial customers to paid billing.",
                    "Plan change must support upgrades and downgrades with proration.",
                ],
            },
        }
    )

    assert [record.category for record in result.records] == [
        "trial_conversion",
        "plan_change",
    ]
    assert result.records[0].evidence == (
        "source_payload.acceptance_criteria[0]: Trial conversion must convert free trial customers to paid billing.",
    )
    assert result.records[0].confidence == 0.95
    assert result.records[1].confidence == 0.95
    assert result.summary["categories"] == ["trial_conversion", "plan_change"]


def test_sourcebrief_object_serialization_markdown_and_summary_helpers_are_stable():
    source = _source_brief(
        source_id="source-subscription-model",
        summary="Subscriptions need cancellation and renewal notice handling.",
        source_payload={
            "requirements": [
                "Pause subscription and resume subscription states must be supported.",
                "Dunning notice must cover failed payment retries.",
            ],
            "metadata": {"entitlement_sync": "Entitlements sync after plan changes."},
        },
    )
    original = copy.deepcopy(source)
    mapping_result = build_source_subscription_lifecycle_requirements(source)
    model_result = generate_source_subscription_lifecycle_requirements(SourceBrief.model_validate(source))
    extracted = extract_source_subscription_lifecycle_requirements(SourceBrief.model_validate(source))
    payload = source_subscription_lifecycle_requirements_to_dict(model_result)
    markdown = source_subscription_lifecycle_requirements_to_markdown(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert extracted == model_result.requirements
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_subscription_lifecycle_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_subscription_lifecycle_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_subscription_lifecycle_requirements(model_result) == model_result.summary
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "category",
        "confidence",
        "evidence",
        "suggested_owner",
        "suggested_planning_note",
    ]
    assert markdown.startswith(
        "# Source Subscription Lifecycle Requirements Report: source-subscription-model"
    )
    assert (
        "| Category | Confidence | Evidence | Suggested Owner | Suggested Planning Note |"
        in markdown
    )


def test_empty_invalid_and_negated_inputs_return_stable_empty_reports():
    empty = build_source_subscription_lifecycle_requirements(
        _source_brief(source_id="empty-subscription", summary="Update billing copy only.")
    )
    repeat = build_source_subscription_lifecycle_requirements(
        _source_brief(source_id="empty-subscription", summary="Update billing copy only.")
    )
    negated = build_source_subscription_lifecycle_requirements(
        {
            "id": "negated-subscription",
            "summary": "No subscription cancellation or dunning changes are required for this copy update.",
        }
    )
    invalid = build_source_subscription_lifecycle_requirements("not a source brief")

    expected_summary = {
        "requirement_count": 0,
        "category_counts": {
            "trial_conversion": 0,
            "cancellation": 0,
            "pause_resume": 0,
            "renewal_notice": 0,
            "plan_change": 0,
            "entitlement_sync": 0,
            "dunning_notice": 0,
        },
        "high_confidence_count": 0,
        "categories": [],
        "suggested_owner_counts": {},
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "empty-subscription"
    assert empty.requirements == ()
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No subscription lifecycle requirements were found" in empty.to_markdown()
    assert negated.requirements == ()
    assert invalid.source_id is None
    assert invalid.requirements == ()
    assert invalid.summary == expected_summary


def _source_brief(
    *,
    source_id="source-subscription",
    title="Subscription lifecycle requirements",
    domain="commerce",
    summary="General subscription lifecycle requirements.",
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


def _implementation(*, architecture_notes=None, risks=None, definition_of_done=None):
    return {
        "id": "impl-subscription",
        "source_brief_id": "source-subscription",
        "title": "Subscription lifecycle handling",
        "domain": "commerce",
        "target_user": "subscription admins",
        "buyer": "enterprise",
        "workflow_context": "Normalize subscription lifecycle obligations before task generation.",
        "problem_statement": "Operations needs reliable subscription lifecycle handling.",
        "mvp_goal": "Capture lifecycle obligations in the execution plan.",
        "product_surface": "billing",
        "scope": ["Subscriptions", "Entitlements"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": architecture_notes,
        "data_requirements": None,
        "integration_points": [],
        "risks": risks or [],
        "validation_plan": "Review subscription lifecycle scenarios.",
        "definition_of_done": definition_of_done or [],
    }
