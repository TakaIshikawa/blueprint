import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_account_reactivation_requirements import (
    SourceAccountReactivationRequirement,
    SourceAccountReactivationRequirementsReport,
    build_source_account_reactivation_requirements,
    derive_source_account_reactivation_requirements,
    extract_source_account_reactivation_requirements,
    generate_source_account_reactivation_requirements,
    source_account_reactivation_requirements_to_dict,
    source_account_reactivation_requirements_to_dicts,
    source_account_reactivation_requirements_to_markdown,
    summarize_source_account_reactivation_requirements,
)


def test_nested_source_payload_extracts_reactivation_categories_in_order():
    result = build_source_account_reactivation_requirements(
        _source_brief(
            source_payload={
                "account_reactivation": {
                    "eligibility": "Reactivation eligibility must allow dormant accounts within 30 days before deletion.",
                    "identity": "Identity verification requires verified email and MFA step-up before restoring access.",
                    "billing": "Billing status checks must block past due accounts until unpaid invoices are settled.",
                    "entitlements": "Entitlement restoration should restore roles, permissions, and workspace access.",
                    "audit": "Audit trail records reactivation events, actor, reason, and timestamp.",
                    "support": "Support handoff creates a support ticket when manual approval is required.",
                    "notifications": "Customer notification sends email | in-app confirmation after account restoration.",
                    "risk": "Abuse and fraud review must run a manual risk review for chargeback accounts.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceAccountReactivationRequirementsReport)
    assert all(isinstance(record, SourceAccountReactivationRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "reactivation_eligibility",
        "identity_verification",
        "billing_status_checks",
        "entitlement_restoration",
        "audit_trail",
        "support_handoff",
        "customer_notification",
        "abuse_fraud_review",
    ]
    assert by_category["reactivation_eligibility"].value == "30 days"
    assert by_category["identity_verification"].value == "verified email"
    assert by_category["billing_status_checks"].value == "past due"
    assert by_category["entitlement_restoration"].value == "roles"
    assert by_category["support_handoff"].source_field == "source_payload.account_reactivation.support"
    assert by_category["customer_notification"].suggested_owners == ("lifecycle_messaging", "support")
    assert by_category["abuse_fraud_review"].suggested_owners == ("trust_and_safety", "security")
    assert result.summary["requirement_count"] == 8
    assert result.summary["category_counts"]["audit_trail"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_fields_and_structured_metadata_are_scanned_without_bookkeeping_noise():
    result = build_source_account_reactivation_requirements(
        _source_brief(
            title="Account reactivation launch",
            summary="Account reactivation requirements include reactivation eligibility and identity verification.",
            requirements=[
                "Billing status checks must require a valid payment method before reactivation.",
                "Entitlement restoration must reinstate project access and feature flags.",
            ],
            acceptance_criteria=[
                "Support handoff escalates blocked accounts to an agent review queue.",
                "Audit log records actor and timestamp for each reactivation event.",
            ],
            source_payload={
                "metadata": {
                    "customer_notification": "Customer notification sends SMS and email after restoration.",
                    "abuse_review": "Fraud review must block suspicious activity until trust and safety approves.",
                },
                "updated_by": "support handoff should not be inferred from bookkeeping owner names",
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert {
        "reactivation_eligibility",
        "identity_verification",
        "billing_status_checks",
        "entitlement_restoration",
        "audit_trail",
        "support_handoff",
        "customer_notification",
        "abuse_fraud_review",
    } <= set(by_category)
    assert by_category["billing_status_checks"].source_field == "requirements[0]"
    assert by_category["support_handoff"].source_field == "acceptance_criteria[0]"
    assert by_category["customer_notification"].source_field == "source_payload.metadata.customer_notification"
    assert all("updated_by" not in item for record in result.records for item in record.evidence)


def test_plain_text_and_implementation_brief_support_multiple_categories_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "Reactivation eligibility must allow inactive accounts during a 14 days grace period.",
            "Billing status checks validate subscription status and payment method.",
        ],
        definition_of_done=[
            "Entitlement restoration restores workspace access and roles.",
            "Customer notification sends confirmation email and audit trail logs security events.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    text_result = build_source_account_reactivation_requirements(
        """
# Account reactivation

- Reactivation eligibility allows dormant accounts within 7 days.
- Identity verification requires MFA.
- Abuse fraud review requires risk review before restoring restricted accounts.
"""
    )
    implementation_result = generate_source_account_reactivation_requirements(implementation)

    assert implementation_payload == original
    assert [record.category for record in text_result.records] == [
        "reactivation_eligibility",
        "identity_verification",
        "abuse_fraud_review",
    ]
    assert text_result.records[0].source_field == "body"
    assert {
        "reactivation_eligibility",
        "billing_status_checks",
        "entitlement_restoration",
        "audit_trail",
        "customer_notification",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-reactivation"
    assert implementation_result.title == "Account reactivation implementation"


def test_duplicate_evidence_merges_without_mutating_mapping():
    source = _source_brief(
        source_id="reactivation-dupes",
        source_payload={
            "account_reactivation": {
                "eligibility": "Reactivation eligibility must allow closed accounts within 30 days.",
                "same_eligibility": "Reactivation eligibility must allow closed accounts within 30 days.",
                "window": "Retention window is required before account reactivation.",
            },
            "acceptance_criteria": [
                "Reactivation eligibility must allow closed accounts within 30 days.",
                "Audit trail must log reactivation events.",
            ],
        },
    )
    original = copy.deepcopy(source)

    result = build_source_account_reactivation_requirements(source)
    eligibility = next(record for record in result.records if record.category == "reactivation_eligibility")

    assert source == original
    assert eligibility.evidence == (
        "source_payload.account_reactivation.eligibility: Reactivation eligibility must allow closed accounts within 30 days.",
        "source_payload.account_reactivation.window: Retention window is required before account reactivation.",
    )
    assert eligibility.value == "30 days"
    assert eligibility.confidence == "high"
    assert [record.category for record in result.records] == [
        "reactivation_eligibility",
        "audit_trail",
    ]


def test_serialization_markdown_aliases_and_sorting_are_stable():
    source = _source_brief(
        source_id="reactivation-model",
        title="Account reactivation source",
        summary="Account reactivation requirements include reactivation eligibility and identity verification.",
        source_payload={
            "requirements": [
                "Customer notification must send email | in-app account restored notices.",
                "Billing status checks require paid invoice verification.",
                "Entitlement restoration supports workspace access.",
            ]
        },
    )
    model = SourceBrief.model_validate(
        {
            key: value
            for key, value in source.items()
            if key not in {"description", "requirements", "acceptance_criteria", "billing"}
        }
    )

    result = build_source_account_reactivation_requirements(model)
    extracted = extract_source_account_reactivation_requirements(model)
    derived = derive_source_account_reactivation_requirements(model)
    payload = source_account_reactivation_requirements_to_dict(result)
    markdown = source_account_reactivation_requirements_to_markdown(result)

    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_account_reactivation_requirements(result) == result.summary
    assert source_account_reactivation_requirements_to_dicts(result) == payload["requirements"]
    assert source_account_reactivation_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.requirements
    assert result.findings == result.requirements
    assert result.to_dicts() == payload["requirements"]
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "suggested_owners",
        "suggested_plan_impacts",
    ]
    assert [record["category"] for record in payload["requirements"]] == [
        "reactivation_eligibility",
        "identity_verification",
        "billing_status_checks",
        "entitlement_restoration",
        "customer_notification",
    ]
    assert markdown.startswith("# Source Account Reactivation Requirements Report: reactivation-model")
    assert "| Category | Value | Confidence | Source Field | Owners | Evidence | Suggested Plan Impacts |" in markdown
    assert "email \\| in-app account restored notices" in markdown


def test_negated_scope_empty_invalid_mapping_and_object_inputs_are_stable():
    class BriefLike:
        id = "object-no-reactivation"
        summary = "No account reactivation work is required for this release."

    object_result = build_source_account_reactivation_requirements(
        SimpleNamespace(
            id="object-reactivation",
            summary="Reactivation eligibility must allow inactive accounts and identity verification requires MFA.",
            metadata={"billing": "Billing status checks require paid invoice verification."},
        )
    )
    negated = build_source_account_reactivation_requirements(BriefLike())
    no_scope = build_source_account_reactivation_requirements(
        _source_brief(summary="Reactivation is out of scope and no restore account flow is planned.")
    )
    unrelated_auth = build_source_account_reactivation_requirements(
        _source_brief(
            title="Login copy",
            summary="Authentication page copy should explain remember me labels.",
            source_payload={"requirements": ["Show login button and profile menu."]},
        )
    )
    malformed = build_source_account_reactivation_requirements({"source_payload": {"notes": object()}})
    blank = build_source_account_reactivation_requirements("")
    invalid = build_source_account_reactivation_requirements(42)

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "reactivation_eligibility": 0,
            "identity_verification": 0,
            "billing_status_checks": 0,
            "entitlement_restoration": 0,
            "audit_trail": 0,
            "support_handoff": 0,
            "customer_notification": 0,
            "abuse_fraud_review": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_account_reactivation_language",
    }
    assert [record.category for record in object_result.records] == [
        "reactivation_eligibility",
        "identity_verification",
        "billing_status_checks",
    ]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated_auth.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated_auth.summary == expected_summary
    assert unrelated_auth.to_dicts() == []
    assert "No source account reactivation requirements were inferred" in unrelated_auth.to_markdown()
    assert summarize_source_account_reactivation_requirements(unrelated_auth) == expected_summary


def _source_brief(
    *,
    source_id="source-reactivation",
    title="Account reactivation requirements",
    domain="identity",
    summary="General account reactivation requirements.",
    description=None,
    requirements=None,
    acceptance_criteria=None,
    billing=None,
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "description": description,
        "requirements": [] if requirements is None else requirements,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
        "billing": {} if billing is None else billing,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(
    *,
    brief_id="implementation-reactivation",
    title="Account reactivation implementation",
    problem_statement="Implement source-backed account reactivation workflows.",
    mvp_goal="Ship account reactivation planning support.",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-reactivation",
        "title": title,
        "domain": "identity",
        "target_user": "returning customer",
        "buyer": "support operations",
        "workflow_context": "Account lifecycle recovery",
        "problem_statement": problem_statement,
        "mvp_goal": mvp_goal,
        "product_surface": "account settings",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run account reactivation extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
