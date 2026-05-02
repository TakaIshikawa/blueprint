import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_customer_support_expectations import (
    SourceCustomerSupportExpectation,
    SourceCustomerSupportExpectationsReport,
    build_source_customer_support_expectations,
    derive_source_customer_support_expectations,
    extract_source_customer_support_expectations,
    generate_source_customer_support_expectations,
    source_customer_support_expectations_to_dict,
    source_customer_support_expectations_to_dicts,
    source_customer_support_expectations_to_markdown,
    summarize_source_customer_support_expectations,
)


def test_prose_extracts_all_customer_support_expectation_categories():
    result = build_source_customer_support_expectations(
        _source_brief(
            source_payload={
                "body": """
# Support launch requirements

- Support SLA: first response must be within 2 hours during business hours.
- Escalation path must route Sev1 cases to the engineering on-call.
- Configure Zendesk support tooling with macros for launch questions.
- Customer messaging should include email and in-app message copy.
- Knowledge base FAQ and help center article must be published.
- Ticket triage needs priority tags and severity classification.
- Refund or credit handling should allow goodwill credits for billing errors.
- Support training must include an agent script and playbook.
"""
            }
        )
    )

    assert isinstance(result, SourceCustomerSupportExpectationsReport)
    assert all(isinstance(record, SourceCustomerSupportExpectation) for record in result.records)
    assert [record.category for record in result.records] == [
        "support_sla",
        "escalation_path",
        "support_tooling",
        "customer_messaging",
        "knowledge_base",
        "ticket_triage",
        "refund_or_credit",
        "support_training",
    ]
    assert all(record.confidence == "high" for record in result.records)
    assert result.summary["expectation_count"] == 8
    assert result.summary["category_counts"] == {
        "support_sla": 1,
        "escalation_path": 1,
        "support_tooling": 1,
        "customer_messaging": 1,
        "knowledge_base": 1,
        "ticket_triage": 1,
        "refund_or_credit": 1,
        "support_training": 1,
    }
    assert result.summary["confidence_counts"] == {"high": 8, "medium": 0, "low": 0}


def test_nested_structured_metadata_and_implementation_brief_contribute_evidence():
    model = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Support response commitment: P1 tickets receive first response within 30 minutes.",
                "Escalation path routes tier 2 cases to the launch support owner.",
            ],
            definition_of_done=[
                "Intercom support queue and ticket routing tags are configured.",
                "Help center FAQ is published and support training is completed.",
            ],
        )
    )
    result = build_source_customer_support_expectations(model)

    assert result.source_id == "impl-support"
    assert [record.category for record in result.records] == [
        "support_sla",
        "escalation_path",
        "support_tooling",
        "knowledge_base",
        "ticket_triage",
        "support_training",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["support_sla"].evidence == (
        "scope[0]: Support response commitment: P1 tickets receive first response within 30 minutes.",
    )
    assert any("definition_of_done[0]" in item for item in by_category["ticket_triage"].evidence)
    assert by_category["support_training"].owner_suggestion == "support_enablement"


def test_duplicate_category_candidates_merge_deterministically_with_stable_confidence():
    result = build_source_customer_support_expectations(
        {
            "id": "dupes",
            "source_payload": {
                "support": {
                    "sla": "Support SLA must respond within 2 hours.",
                    "same_sla": "Support SLA must respond within 2 hours.",
                    "routing": "Ticket triage must route P1 cases to the priority queue.",
                },
                "acceptance_criteria": [
                    "Support SLA must respond within 2 hours.",
                    "Ticket triage must route P1 cases to the priority queue.",
                ],
            },
        }
    )

    assert [record.category for record in result.records] == ["support_sla", "ticket_triage"]
    assert result.records[0].evidence == (
        "source_payload.acceptance_criteria[0]: Support SLA must respond within 2 hours.",
    )
    assert result.records[0].confidence == "high"
    assert result.records[1].confidence == "high"


def test_suggested_planning_notes_are_category_specific_for_support_work():
    result = build_source_customer_support_expectations(
        _source_brief(
            source_payload={
                "support_plan": {
                    "tooling": "Zendesk macros and support queue tags must be ready.",
                    "refunds": "Refund policy needs support handling for account credits.",
                    "comms": "Customer communication must include support reply wording.",
                }
            }
        )
    )
    by_category = {record.category: record for record in result.records}

    assert "tooling configuration" in by_category["support_tooling"].planning_notes[0]
    assert "refund" in by_category["refund_or_credit"].planning_notes[0].casefold()
    assert "customer-facing messaging" in by_category["customer_messaging"].planning_notes[0]
    assert by_category["support_tooling"].owner_suggestion == "support_ops"


def test_no_match_negated_and_invalid_inputs_return_stable_empty_reports():
    class BriefLike:
        id = "object-empty"
        summary = "No support, ticket triage, escalation, refund, or training changes are in scope."

    empty = build_source_customer_support_expectations(
        _source_brief(summary="Polish onboarding copy.", source_payload={"body": "No support changes are required."})
    )
    negated = build_source_customer_support_expectations(BriefLike())
    malformed = build_source_customer_support_expectations({"source_payload": {"notes": object()}})
    invalid = build_source_customer_support_expectations(42)

    assert empty.source_id == "sb-support"
    assert empty.records == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "expectation_count": 0,
        "category_counts": {
            "support_sla": 0,
            "escalation_path": 0,
            "support_tooling": 0,
            "customer_messaging": 0,
            "knowledge_base": 0,
            "ticket_triage": 0,
            "refund_or_credit": 0,
            "support_training": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "categories": [],
    }
    assert "No customer support expectations were found" in empty.to_markdown()
    assert negated.records == ()
    assert malformed.records == ()
    assert invalid.records == ()


def test_aliases_serialization_markdown_object_string_and_no_input_mutation():
    source = _source_brief(
        source_id="support-model",
        summary="Support launch readiness requires customer messaging and help center updates.",
        source_payload={
            "support": [
                "Customer messaging must escape plan | account notes in support replies.",
                "Knowledge base FAQ must cover the changed account workflow.",
            ],
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_customer_support_expectations(source)
    model_result = generate_source_customer_support_expectations(model)
    derived = derive_source_customer_support_expectations(model)
    extracted = extract_source_customer_support_expectations(model)
    text_result = build_source_customer_support_expectations(
        "Support training should include an agent script before launch."
    )
    object_result = build_source_customer_support_expectations(
        SimpleNamespace(
            id="object-support",
            metadata={"ticket_triage": "Triage priority tags must route launch cases."},
        )
    )
    payload = source_customer_support_expectations_to_dict(model_result)
    markdown = source_customer_support_expectations_to_markdown(model_result)

    assert source == original
    assert payload == source_customer_support_expectations_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert extracted.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.expectations
    assert source_customer_support_expectations_to_dicts(model_result) == payload["expectations"]
    assert source_customer_support_expectations_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_customer_support_expectations(model_result) == model_result.summary
    assert list(payload) == ["source_id", "expectations", "summary", "records"]
    assert list(payload["expectations"][0]) == [
        "source_brief_id",
        "category",
        "evidence",
        "confidence",
        "owner_suggestion",
        "planning_notes",
    ]
    assert [
        (record.source_brief_id, record.category)
        for record in model_result.records
    ] == [
        ("support-model", "customer_messaging"),
        ("support-model", "knowledge_base"),
    ]
    assert markdown == model_result.to_markdown()
    assert "| Source Brief | Category | Confidence | Owner | Planning Notes | Evidence |" in markdown
    assert "plan \\| account notes" in markdown
    assert text_result.records[0].category == "support_training"
    assert object_result.records[0].category == "ticket_triage"


def _source_brief(
    *,
    source_id="sb-support",
    title="Customer support expectations",
    domain="support",
    summary="General customer support expectations.",
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
        "id": "impl-support",
        "source_brief_id": "source-support",
        "title": "Support readiness",
        "domain": "support",
        "target_user": "customers",
        "buyer": None,
        "workflow_context": "Launch support expectations.",
        "problem_statement": "Customers need reliable help during rollout.",
        "mvp_goal": "Ship with support readiness.",
        "product_surface": "support",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review support handoff.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
