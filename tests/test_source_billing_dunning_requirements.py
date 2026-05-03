import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_billing_dunning_requirements import (
    SourceBillingDunningRequirement,
    SourceBillingDunningRequirementsReport,
    build_source_billing_dunning_requirements,
    derive_source_billing_dunning_requirements,
    extract_source_billing_dunning_requirements,
    generate_source_billing_dunning_requirements,
    source_billing_dunning_requirements_to_dict,
    source_billing_dunning_requirements_to_dicts,
    source_billing_dunning_requirements_to_markdown,
    summarize_source_billing_dunning_requirements,
)


def test_nested_source_payload_extracts_billing_dunning_categories_and_owners():
    result = build_source_billing_dunning_requirements(
        _source_brief(
            source_payload={
                "billing_dunning": {
                    "retry": "Failed payment retry cadence must run 3 attempts over 7 days.",
                    "notices": "Customer communication sends dunning emails and a final notice before suspension.",
                    "grace": "Grace period allows 14 days past due before account suspension.",
                    "suspension": "Suspend service with read-only access after grace expires.",
                    "downgrade": "Plan downgrade moves delinquent accounts to a limited plan.",
                    "invoice": "Invoice recovery tracks unpaid invoices in a collections queue.",
                    "payment_method": "Payment method update handoff uses the hosted billing portal.",
                    "engineering": "Billing engineering owns the retry state machine and provider integration.",
                    "finance": "Finance owner reviews invoice collection and accounts receivable reports.",
                    "support": "Support owner gets a support queue handoff for escalations.",
                    "lifecycle": "Lifecycle messaging owner manages notice templates and suppression rules.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceBillingDunningRequirementsReport)
    assert all(isinstance(record, SourceBillingDunningRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "retry_cadence",
        "customer_communication",
        "grace_period",
        "service_suspension",
        "plan_downgrade",
        "invoice_recovery",
        "payment_method_update_handoff",
        "billing_engineering_ownership",
        "finance_ownership",
        "support_ownership",
        "lifecycle_messaging_ownership",
    ]
    assert by_category["retry_cadence"].value == "3 attempts"
    assert by_category["grace_period"].value == "14 days"
    assert by_category["customer_communication"].value == "dunning emails"
    assert by_category["payment_method_update_handoff"].value == "hosted billing portal"
    assert by_category["invoice_recovery"].source_field == "source_payload.billing_dunning.invoice"
    assert by_category["billing_engineering_ownership"].suggested_owners == ("billing_engineering",)
    assert by_category["finance_ownership"].suggested_owners == ("finance",)
    assert by_category["support_ownership"].suggested_owners == ("support",)
    assert by_category["lifecycle_messaging_ownership"].suggested_owners == ("lifecycle_messaging",)
    assert by_category["service_suspension"].suggested_owners == ("billing_engineering", "support")
    assert result.summary["requirement_count"] == 11
    assert result.summary["category_counts"]["retry_cadence"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_titles_descriptions_requirements_acceptance_billing_fields_and_metadata_are_scanned():
    result = build_source_billing_dunning_requirements(
        _source_brief(
            title="Billing dunning launch",
            summary="Past due customers need billing recovery requirements.",
            description="Payment failure recovery should notify customers after each failed payment.",
            requirements=[
                "Retry cadence must run automatic retries every 2 days.",
                "Payment method update handoff must route customers to a billing portal.",
            ],
            acceptance_criteria=[
                "Grace period keeps access active for 10 days.",
                "Service suspension blocks access after final notice.",
            ],
            billing_fields={
                "invoice_state": "Invoice recovery must track unpaid invoices by past due status.",
                "downgrade": "Plan downgrade should reduce entitlements after delinquency.",
            },
            source_payload={
                "metadata": {
                    "owners": "Finance owner reviews collections and support owner handles escalation tickets.",
                    "messages": "Lifecycle messaging owns dunning email templates.",
                }
            },
        )
    )

    categories = {record.category for record in result.records}

    assert {
        "retry_cadence",
        "customer_communication",
        "grace_period",
        "service_suspension",
        "plan_downgrade",
        "invoice_recovery",
        "payment_method_update_handoff",
        "finance_ownership",
        "support_ownership",
        "lifecycle_messaging_ownership",
    } <= categories
    assert next(record for record in result.records if record.category == "retry_cadence").source_field == "requirements[0]"
    assert next(record for record in result.records if record.category == "grace_period").source_field == "acceptance_criteria[0]"
    assert next(record for record in result.records if record.category == "finance_ownership").source_field == "source_payload.metadata.owners"


def test_plain_text_and_implementation_brief_support_multiple_categories():
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Billing recovery must retry failed payments 4 times and send customer payment notices.",
                "Grace period should last 7 days before service suspension.",
            ],
            definition_of_done=[
                "Invoice recovery tracks unpaid invoices and finance ownership is documented.",
                "Support owner can restore service after payment method update handoff succeeds.",
            ],
        )
    )
    text_result = build_source_billing_dunning_requirements(
        """
# Dunning workflow

- Retry schedule runs failed payment retries daily.
- Payment method update uses a payment link.
- Plan downgrade applies to overdue accounts after grace.
"""
    )
    implementation_result = generate_source_billing_dunning_requirements(implementation)

    assert [record.category for record in text_result.records] == [
        "retry_cadence",
        "grace_period",
        "plan_downgrade",
        "payment_method_update_handoff",
    ]
    assert text_result.records[0].source_field == "body"
    assert {
        "retry_cadence",
        "customer_communication",
        "grace_period",
        "service_suspension",
        "invoice_recovery",
        "payment_method_update_handoff",
        "finance_ownership",
        "support_ownership",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-dunning"
    assert implementation_result.title == "Billing dunning implementation"


def test_duplicate_evidence_merges_without_mutating_mapping():
    source = _source_brief(
        source_id="dunning-dupes",
        source_payload={
            "dunning": {
                "retry": "Failed payment retry cadence must run 3 attempts over 7 days.",
                "same_retry": "Failed payment retry cadence must run 3 attempts over 7 days.",
                "schedule": "Retry schedule is required for payment recovery.",
            },
            "acceptance_criteria": [
                "Failed payment retry cadence must run 3 attempts over 7 days.",
                "Customer notices must include a final notice for past due accounts.",
            ],
        },
    )
    original = copy.deepcopy(source)

    result = build_source_billing_dunning_requirements(source)
    retry = next(record for record in result.records if record.category == "retry_cadence")

    assert source == original
    assert retry.evidence == (
        "source_payload.acceptance_criteria[0]: Failed payment retry cadence must run 3 attempts over 7 days.",
        "source_payload.dunning.schedule: Retry schedule is required for payment recovery.",
    )
    assert retry.confidence == "high"
    assert [record.category for record in result.records] == ["retry_cadence", "customer_communication"]


def test_serialization_markdown_aliases_and_sorting_are_stable():
    source = _source_brief(
        source_id="dunning-model",
        title="Dunning source",
        summary="Billing recovery requirements include retry cadence and customer notices.",
        source_payload={
            "requirements": [
                "Customer communication must send billing notices | final notices.",
                "Billing engineering owner documents retry cadence.",
                "Support owner receives payment method update handoff tickets.",
            ]
        },
    )
    model = SourceBrief.model_validate(
        {
            key: value
            for key, value in source.items()
            if key not in {"description", "requirements", "acceptance_criteria", "billing_fields"}
        }
    )

    result = build_source_billing_dunning_requirements(model)
    extracted = extract_source_billing_dunning_requirements(model)
    derived = derive_source_billing_dunning_requirements(model)
    payload = source_billing_dunning_requirements_to_dict(result)
    markdown = source_billing_dunning_requirements_to_markdown(result)

    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_billing_dunning_requirements(result) == result.summary
    assert source_billing_dunning_requirements_to_dicts(result) == payload["requirements"]
    assert source_billing_dunning_requirements_to_dicts(result.records) == payload["records"]
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
        "retry_cadence",
        "customer_communication",
        "payment_method_update_handoff",
        "billing_engineering_ownership",
        "support_ownership",
    ]
    assert markdown.startswith("# Source Billing Dunning Requirements Report: dunning-model")
    assert "| Category | Value | Confidence | Source Field | Owners | Evidence | Suggested Plan Impacts |" in markdown
    assert "billing notices \\| final notices" in markdown


def test_negated_scope_unrelated_billing_copy_malformed_and_object_inputs_are_stable_empty():
    class BriefLike:
        id = "object-no-dunning"
        summary = "No dunning, billing recovery, payment recovery, or invoice recovery work is required."

    object_result = build_source_billing_dunning_requirements(
        SimpleNamespace(
            id="object-dunning",
            summary="Dunning emails must notify customers and support owner handles escalations.",
            metadata={"payment_method_update": "Payment method update handoff is required through the billing portal."},
        )
    )
    negated = build_source_billing_dunning_requirements(BriefLike())
    no_scope = build_source_billing_dunning_requirements(
        _source_brief(summary="Billing recovery is out of scope and no dunning support is planned.")
    )
    unrelated_billing = build_source_billing_dunning_requirements(
        _source_brief(
            title="Billing settings copy",
            summary="Billing page copy should explain tax IDs and invoice download labels.",
            source_payload={"requirements": ["Show billing address and VAT fields."]},
        )
    )
    malformed = build_source_billing_dunning_requirements({"source_payload": {"notes": object()}})
    blank = build_source_billing_dunning_requirements("")

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "retry_cadence": 0,
            "customer_communication": 0,
            "grace_period": 0,
            "service_suspension": 0,
            "plan_downgrade": 0,
            "invoice_recovery": 0,
            "payment_method_update_handoff": 0,
            "billing_engineering_ownership": 0,
            "finance_ownership": 0,
            "support_ownership": 0,
            "lifecycle_messaging_ownership": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_billing_dunning_language",
    }
    assert [record.category for record in object_result.records] == [
        "customer_communication",
        "payment_method_update_handoff",
        "support_ownership",
    ]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated_billing.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert unrelated_billing.summary == expected_summary
    assert unrelated_billing.to_dicts() == []
    assert "No source billing dunning requirements were inferred" in unrelated_billing.to_markdown()
    assert summarize_source_billing_dunning_requirements(unrelated_billing) == expected_summary


def _source_brief(
    *,
    source_id="source-dunning",
    title="Billing dunning requirements",
    domain="billing",
    summary="General billing dunning requirements.",
    description=None,
    requirements=None,
    acceptance_criteria=None,
    billing_fields=None,
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
        "billing_fields": {} if billing_fields is None else billing_fields,
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
    brief_id="implementation-dunning",
    title="Billing dunning implementation",
    problem_statement="Implement source-backed billing dunning workflows.",
    mvp_goal="Ship billing dunning planning support.",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-dunning",
        "title": title,
        "domain": "billing",
        "target_user": "billing operator",
        "buyer": "finance",
        "workflow_context": "Billing recovery operations",
        "problem_statement": problem_statement,
        "mvp_goal": mvp_goal,
        "product_surface": "billing",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run billing dunning extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
