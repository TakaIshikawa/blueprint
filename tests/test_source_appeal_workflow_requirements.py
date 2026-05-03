import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_appeal_workflow_requirements import (
    SourceAppealWorkflowRequirement,
    SourceAppealWorkflowRequirementsReport,
    build_source_appeal_workflow_requirements,
    derive_source_appeal_workflow_requirements,
    extract_source_appeal_workflow_requirements,
    generate_source_appeal_workflow_requirements,
    source_appeal_workflow_requirements_to_dict,
    source_appeal_workflow_requirements_to_dicts,
    source_appeal_workflow_requirements_to_markdown,
    summarize_source_appeal_workflow_requirements,
)


def test_free_text_extracts_appeal_categories_with_evidence_and_notes():
    result = build_source_appeal_workflow_requirements(
        _source_brief(
            source_payload={
                "body": """
# Appeal workflow after enforcement

- Appeal submission must let users submit appeals after moderation removal with an appeal reason.
- Evidence collection should collect attachments and enforcement context for fraud hold cases.
- Reviewer assignment must route chargeback restriction appeals to an independent reviewer.
- SLA response timing requires support to respond within 3 business days after account lockout.
- Customer notifications must send email and in-app status updates for access denial appeals.
- Reversal remediation should restore content, unlock accounts, or release holds when an appeal is approved.
- Audit trail must record decision history, timestamps, reviewer identity, and evidence.
- Escalation policy must escalate high-risk appeal cases to Trust and Safety or legal review.
"""
            }
        )
    )

    assert isinstance(result, SourceAppealWorkflowRequirementsReport)
    assert result.source_id == "sb-appeal"
    assert all(isinstance(record, SourceAppealWorkflowRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "appeal_submission",
        "evidence_collection",
        "reviewer_assignment",
        "sla_response_timing",
        "customer_notifications",
        "reversal_remediation",
        "audit_trail",
        "escalation_policy",
    ]
    by_category = {record.category: record for record in result.records}
    assert by_category["appeal_submission"].enforcement_context == "moderation removal"
    assert by_category["evidence_collection"].enforcement_context == "fraud hold"
    assert by_category["reviewer_assignment"].enforcement_context == "chargeback restriction"
    assert by_category["sla_response_timing"].suggested_owner == "support"
    assert by_category["audit_trail"].suggested_owner == "compliance"
    assert by_category["escalation_policy"].suggested_owner == "trust_and_safety"
    assert "appeal reason" in by_category["appeal_submission"].requirement_text
    assert "reviewer identity" in by_category["audit_trail"].evidence[0]
    assert "submission channels" in by_category["appeal_submission"].planning_note
    assert result.summary["requirement_count"] == 8
    assert result.summary["category_counts"] == {category: 1 for category in result.summary["categories"]}
    assert result.summary["status"] == "ready_for_appeal_workflow_planning"


def test_structured_fields_and_implementation_brief_inputs_are_supported():
    structured = build_source_appeal_workflow_requirements(
        _source_brief(
            source_payload={
                "moderation": {
                    "appeal_submission": {
                        "category": "appeal_submission",
                        "enforcement_context": "content removal",
                        "requirement": "Users must submit appeals from the removed content notice.",
                    },
                    "audit": "Audit trail must record moderation appeal decisions and timestamps.",
                },
                "fraud": {
                    "evidence_collection": "Evidence collection requires transaction history for fraud hold appeal review.",
                    "reversal_remediation": "Approved fraud hold appeals should release hold and restore access.",
                },
                "support": {
                    "customer_notifications": "Support must notify customers by email when access denial appeals are received.",
                },
                "acceptance_criteria": [
                    "Reviewer assignment must route account lockout appeals to a support reviewer.",
                    "SLA response timing requires a response within 48 hours.",
                ],
            }
        )
    )
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Escalation policy must escalate chargeback restriction appeals to legal review.",
            ],
            definition_of_done=[
                "Customer notifications send appeal decision notices for denied access cases.",
            ],
        )
    )
    object_result = build_source_appeal_workflow_requirements(
        SimpleNamespace(
            id="object-appeal",
            support="Appeal submission should let locked account users file an appeal from support.",
        )
    )

    assert [record.category for record in structured.records] == [
        "appeal_submission",
        "evidence_collection",
        "reviewer_assignment",
        "sla_response_timing",
        "customer_notifications",
        "reversal_remediation",
        "audit_trail",
    ]
    by_category = {record.category: record for record in structured.records}
    assert by_category["appeal_submission"].confidence == "high"
    assert by_category["appeal_submission"].source_field == "source_payload.moderation.appeal_submission"
    assert by_category["appeal_submission"].enforcement_context == "content removal"
    assert by_category["reviewer_assignment"].source_field == "source_payload.acceptance_criteria[0]"
    assert by_category["evidence_collection"].source_field == "source_payload.fraud.evidence_collection"
    assert by_category["customer_notifications"].source_field == "source_payload.support.customer_notifications"

    impl_result = generate_source_appeal_workflow_requirements(implementation)
    assert impl_result.source_id == "impl-appeal"
    assert [record.category for record in impl_result.records] == [
        "customer_notifications",
        "escalation_policy",
    ]
    assert impl_result.records[1].source_field == "scope[0]"
    assert object_result.records[0].category == "appeal_submission"
    assert object_result.records[0].confidence == "high"


def test_negated_out_of_scope_invalid_and_unrelated_inputs_return_stable_empty_reports():
    empty = build_source_appeal_workflow_requirements(
        _source_brief(
            summary="Support copy update.",
            source_payload={
                "requirements": [
                    "No appeals or dispute workflow changes are required for this release.",
                    "Appeals are out of scope and no support work is planned.",
                ]
            },
        )
    )
    repeat = build_source_appeal_workflow_requirements(
        _source_brief(
            summary="Support copy update.",
            source_payload={
                "requirements": [
                    "No appeals or dispute workflow changes are required for this release.",
                    "Appeals are out of scope and no support work is planned.",
                ]
            },
        )
    )
    malformed = build_source_appeal_workflow_requirements({"id": "brief-empty", "source_payload": {"notes": object()}})
    invalid = build_source_appeal_workflow_requirements(42)
    unrelated = build_source_appeal_workflow_requirements(
        _source_brief(
            title="Billing settings",
            summary="Improve invoice copy and notification preferences.",
            source_payload={"support": "Agents can edit customer note templates."},
        )
    )

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "appeal_submission": 0,
            "evidence_collection": 0,
            "reviewer_assignment": 0,
            "sla_response_timing": 0,
            "customer_notifications": 0,
            "reversal_remediation": 0,
            "audit_trail": 0,
            "escalation_policy": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "owner_counts": {
            "compliance": 0,
            "operations": 0,
            "product": 0,
            "support": 0,
            "trust_and_safety": 0,
        },
        "categories": [],
        "status": "no_appeal_workflow_language",
    }
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "sb-appeal"
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No source appeal workflow requirements were found" in empty.to_markdown()
    assert malformed.summary == expected_summary
    assert invalid.summary == expected_summary
    assert unrelated.summary == expected_summary


def test_deduped_evidence_order_low_confidence_and_markdown_escaping_are_stable():
    result = build_source_appeal_workflow_requirements(
        _source_brief(
            source_id="appeal-dedupe",
            source_payload={
                "requirements": [
                    "Appeal submission must accept account lockout appeals for customer | partner cases.",
                    "Appeal submission must accept account lockout appeals for customer | partner cases.",
                    "Audit trail for appeals.",
                    "Reviewer assignment.",
                ]
            },
        )
    )

    assert [record.category for record in result.records] == [
        "appeal_submission",
        "reviewer_assignment",
        "audit_trail",
    ]
    assert result.records[0].evidence == (
        "source_payload.requirements[0]: Appeal submission must accept account lockout appeals for customer | partner cases.",
    )
    assert result.records[1].confidence == "medium"
    assert any("Which enforcement action" in question for question in result.records[1].unresolved_questions)
    assert result.records[2].confidence == "medium"
    markdown = result.to_markdown()
    assert "| Source Brief | Category | Requirement | Enforcement Context | Source Field | Confidence | Owner | Planning Note | Unresolved Questions | Evidence |" in markdown
    assert "customer \\| partner cases" in markdown


def test_serialization_aliases_json_ordering_and_no_input_mutation_are_stable():
    source = _source_brief(
        source_id="appeal-model",
        source_payload={
            "requirements": [
                "Appeal submission must collect a reason after account lockout and escape plan | review notes.",
                "Customer notifications must notify users when fraud hold appeals are approved.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_appeal_workflow_requirements(source)
    model_result = extract_source_appeal_workflow_requirements(model)
    derived = derive_source_appeal_workflow_requirements(model)
    text_result = build_source_appeal_workflow_requirements(
        "Reversal remediation should restore access after access denial appeal approval."
    )
    payload = source_appeal_workflow_requirements_to_dict(model_result)
    markdown = source_appeal_workflow_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_appeal_workflow_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_appeal_workflow_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_appeal_workflow_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_appeal_workflow_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "category",
        "requirement_text",
        "enforcement_context",
        "source_field",
        "evidence",
        "matched_terms",
        "confidence",
        "suggested_owner",
        "planning_note",
        "unresolved_questions",
    ]
    assert [record.category for record in model_result.records] == [
        "appeal_submission",
        "customer_notifications",
    ]
    assert model_result.records[0].requirement_category == "appeal_submission"
    assert model_result.records[0].suggested_planning_note == model_result.records[0].planning_note
    assert markdown == model_result.to_markdown()
    assert "plan \\| review notes" in markdown
    assert text_result.records[0].category == "reversal_remediation"


def _source_brief(
    *,
    source_id="sb-appeal",
    title="Appeal workflow requirements",
    domain="trust_safety",
    summary="General appeal workflow requirements.",
    source_payload=None,
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
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "impl-appeal",
        "source_brief_id": "source-appeal",
        "title": "Appeal workflow rollout",
        "domain": "trust_safety",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need appeal workflow requirements before task generation.",
        "problem_statement": "Appeal workflow requirements need to be extracted early.",
        "mvp_goal": "Plan appeal workflow work from source briefs.",
        "product_surface": "trust_safety",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review generated plan for appeal workflow coverage.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
    }
