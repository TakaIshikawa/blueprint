import copy
import json

from blueprint.domain.models import SourceBrief
from blueprint.source_billing_entitlements import (
    SourceBillingEntitlementRequirement,
    SourceBillingEntitlementsReport,
    build_source_billing_entitlements,
    generate_source_billing_entitlements,
    source_billing_entitlements_to_dict,
    source_billing_entitlements_to_markdown,
)


def test_detects_billing_entitlement_areas_across_brief_fields_with_audience_inference():
    result = build_source_billing_entitlements(
        _source_brief(
            summary=(
                "Paid users must have active subscription status before export, and admins can upgrade or downgrade "
                "plans with proration."
            ),
            source_payload={
                "requirements": [
                    "Trial users need trial access for 14 days before feature gates lock premium reports.",
                    "Members have seat limits and licensed user counts enforced by plan.",
                    "Usage metering records billable events and overages for paid customers.",
                ],
                "risks": [
                    "Payment failure, dunning retry, and grace period behavior can block access.",
                    "Admins need invoices and billing history in the billing portal.",
                ],
                "metadata": {
                    "entitlement_check": "Users require entitlement checks before gated feature access."
                },
            },
        )
    )

    assert isinstance(result, SourceBillingEntitlementsReport)
    assert all(
        isinstance(requirement, SourceBillingEntitlementRequirement)
        for requirement in result.requirements
    )
    by_area_audience = {
        (requirement.entitlement_area, requirement.affected_audience): requirement
        for requirement in result.requirements
    }

    assert by_area_audience[("subscription", "paid_users")].confidence == "high"
    assert by_area_audience[("upgrade_downgrade", "admins")].confidence == "high"
    assert by_area_audience[("proration", "admins")].confidence == "high"
    assert ("trial", "trial_users") in by_area_audience
    assert ("plan_limit", "members") in by_area_audience
    assert ("seat", "members") in by_area_audience
    assert ("usage_metering", "paid_users") in by_area_audience
    assert ("invoice", "admins") in by_area_audience
    assert ("payment_failure", "users") in by_area_audience
    assert ("grace_period", "users") in by_area_audience
    assert ("entitlement_check", "users") in by_area_audience
    assert any(
        "subscription" in item for item in by_area_audience[("subscription", "paid_users")].evidence
    )
    assert (
        "entitlement checks"
        in by_area_audience[("entitlement_check", "users")].suggested_acceptance_criterion
    )
    assert result.summary["entitlement_area_counts"]["subscription"] == 1
    assert result.summary["audience_counts"]["admins"] >= 1


def test_duplicate_signals_are_merged_with_deduplicated_evidence_and_stable_ordering():
    result = build_source_billing_entitlements(
        {
            "id": "dupe-billing",
            "summary": "Admins need plan limits for exports. Admins need plan limits for exports.",
            "requirements": [
                "Plan limits must block admins above quota.",
                "plan limits must block admins above quota.",
            ],
            "metadata": {"plan_limit": "Admins need plan limits for exports."},
        }
    )

    plan_limit = next(
        requirement
        for requirement in result.requirements
        if requirement.entitlement_area == "plan_limit"
        and requirement.affected_audience == "admins"
    )

    assert plan_limit.evidence == tuple(
        sorted(set(plan_limit.evidence), key=lambda item: item.casefold())
    )
    assert len(plan_limit.evidence) == len(set(plan_limit.evidence))
    assert result.summary["requirement_count"] == len(result.requirements)


def test_mapping_and_sourcebrief_inputs_match_and_serialize_to_json_compatible_payload():
    source = _source_brief(
        source_id="billing-model",
        summary="Paid users need invoice access and subscription status checks.",
        source_payload={
            "requirements": ["Admins can change plan with proration for seat count changes."],
            "metadata": {"trial": "Trial users lose premium entitlement after the trial period."},
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_billing_entitlements(source)
    model_result = generate_source_billing_entitlements(model)
    payload = source_billing_entitlements_to_dict(model_result)
    markdown = source_billing_entitlements_to_markdown(model_result)

    assert source == original
    assert payload == source_billing_entitlements_to_dict(mapping_result)
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert payload["records"] == payload["requirements"]
    assert list(payload) == ["source_id", "requirements", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "entitlement_area",
        "affected_audience",
        "confidence",
        "evidence",
        "suggested_acceptance_criterion",
    ]
    assert markdown.startswith("# Source Billing Entitlements Report: billing-model")
    assert (
        "| Area | Audience | Confidence | Evidence | Suggested Acceptance Criterion |" in markdown
    )


def test_generic_users_empty_and_invalid_inputs_are_handled():
    result = build_source_billing_entitlements(
        {
            "id": "generic-billing",
            "body": "Entitlement checks are required before users can access the billing-gated report.",
        }
    )
    empty = build_source_billing_entitlements(
        {"id": "empty", "summary": "Update internal copy only."}
    )
    invalid = build_source_billing_entitlements("not a source brief")

    assert any(
        requirement.entitlement_area == "entitlement_check"
        and requirement.affected_audience == "users"
        for requirement in result.requirements
    )
    assert empty.source_id == "empty"
    assert empty.requirements == ()
    assert empty.records == ()
    assert empty.summary["requirement_count"] == 0
    assert "No billing entitlement requirements were found" in empty.to_markdown()
    assert invalid.source_id is None
    assert invalid.requirements == ()


def _source_brief(
    *,
    source_id="sb-billing",
    title="Billing entitlements",
    domain="platform",
    summary="General billing requirements.",
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
