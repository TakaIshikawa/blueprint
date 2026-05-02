import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_refund_workflow_requirements import (
    SourceRefundWorkflowRequirement,
    SourceRefundWorkflowRequirementsReport,
    build_source_refund_workflow_requirements,
    derive_source_refund_workflow_requirements,
    extract_source_refund_workflow_requirements,
    generate_source_refund_workflow_requirements,
    source_refund_workflow_requirements_to_dict,
    source_refund_workflow_requirements_to_dicts,
    source_refund_workflow_requirements_to_markdown,
    summarize_source_refund_workflow_requirements,
)


def test_nested_source_payload_extracts_refund_workflow_concerns():
    result = build_source_refund_workflow_requirements(
        _source_brief(
            source_payload={
                "refund_workflow": {
                    "eligibility": "Refund eligibility must allow refunds within 30 days of purchase.",
                    "amounts": "Support full refunds, partial refunds, and prorated refund amounts.",
                    "approval": "Refunds over $500 require finance approval before submission.",
                    "ledger": "Post refund journal entries to the ledger and create a credit note.",
                    "notifications": "Notify customer with a refund receipt after provider success.",
                    "provider": "Store the Stripe refund id and original charge id as provider reference.",
                    "idempotency": "Refund requests must use an idempotency key to prevent duplicate refunds.",
                    "audit": "Audit log records actor, reason code, approval actor, and timestamp.",
                    "disputes": "Disputes, chargebacks, reversals, and account credit states are tracked.",
                }
            },
        )
    )

    by_concern = {record.concern: record for record in result.records}

    assert isinstance(result, SourceRefundWorkflowRequirementsReport)
    assert all(isinstance(record, SourceRefundWorkflowRequirement) for record in result.records)
    assert [record.concern for record in result.records] == [
        "eligibility",
        "refund_scope",
        "approval",
        "ledger_accounting",
        "customer_notification",
        "provider_reference",
        "idempotency",
        "audit_evidence",
        "dispute_credit_reversal",
    ]
    assert by_concern["eligibility"].value == "within 30 days"
    assert by_concern["approval"].value == "over $500"
    assert by_concern["provider_reference"].value == "stripe refund id"
    assert by_concern["idempotency"].value == "idempotency key"
    assert by_concern["ledger_accounting"].source_field == "source_payload.refund_workflow.ledger"
    assert by_concern["audit_evidence"].confidence == "high"
    assert by_concern["approval"].suggested_plan_impacts[0].startswith("Plan approval thresholds")
    assert result.summary["requirement_count"] == 9
    assert result.summary["concern_counts"]["provider_reference"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_plain_text_and_implementation_brief_support_multiple_concerns_per_brief():
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Refund approval should auto-approve refunds under $50 and require manager approval above that.",
                "Ledger accounting must reverse revenue and reconcile credits for partial refunds.",
            ],
            definition_of_done=[
                "Refund audit evidence records actor, timestamp, provider refund id, and refund customer notification status.",
                "Idempotent refund requests use request id dedupe for retry safe provider calls.",
            ],
        )
    )
    text_result = build_source_refund_workflow_requirements(
        """
# Refund workflow

- Customers are eligible for refund requests within 14 days.
- Support full or partial refunds and store credit for disputes and reversals.
"""
    )
    implementation_result = generate_source_refund_workflow_requirements(implementation)

    assert [record.concern for record in text_result.records] == [
        "eligibility",
        "refund_scope",
        "dispute_credit_reversal",
    ]
    assert text_result.records[0].source_field == "body"
    assert {
        "approval",
        "ledger_accounting",
        "customer_notification",
        "provider_reference",
        "idempotency",
        "audit_evidence",
        "dispute_credit_reversal",
    } <= {record.concern for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-refunds"
    assert implementation_result.title == "Refund workflow implementation"


def test_duplicate_evidence_merges_with_field_paths_and_does_not_mutate_mapping():
    source = _source_brief(
        source_id="refund-dupes",
        source_payload={
            "refunds": {
                "eligibility": "Refund eligibility must allow refunds within 30 days.",
                "same_eligibility": "Refund eligibility must allow refunds within 30 days.",
                "window": "Refund window is required for customer refunds.",
            },
            "acceptance_criteria": [
                "Refund eligibility must allow refunds within 30 days.",
                "Idempotent refund request must suppress duplicate refunds.",
            ],
        },
    )
    original = copy.deepcopy(source)

    result = build_source_refund_workflow_requirements(source)
    eligibility = next(record for record in result.records if record.concern == "eligibility")

    assert source == original
    assert eligibility.evidence == (
        "source_payload.acceptance_criteria[0]: Refund eligibility must allow refunds within 30 days.",
        "source_payload.refunds.window: Refund window is required for customer refunds.",
    )
    assert eligibility.confidence == "high"
    assert [record.concern for record in result.records] == ["eligibility", "idempotency"]


def test_serialization_markdown_aliases_and_sorting_are_stable():
    source = _source_brief(
        source_id="refund-model",
        title="Refund source",
        summary="Refund workflow requirements include approval, audit, and provider references.",
        source_payload={
            "requirements": [
                "Provider reference must persist Stripe refund id for support | finance views.",
                "Audit evidence records refund reason code and timestamp.",
                "Customer notification sends refund receipt emails.",
            ]
        },
    )
    model = SourceBrief.model_validate(source)

    result = build_source_refund_workflow_requirements(model)
    extracted = extract_source_refund_workflow_requirements(model)
    derived = derive_source_refund_workflow_requirements(model)
    payload = source_refund_workflow_requirements_to_dict(result)
    markdown = source_refund_workflow_requirements_to_markdown(result)

    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_refund_workflow_requirements(result) == result.summary
    assert source_refund_workflow_requirements_to_dicts(result) == payload["requirements"]
    assert source_refund_workflow_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.requirements
    assert result.findings == result.requirements
    assert result.to_dicts() == payload["requirements"]
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "concern",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "suggested_plan_impacts",
    ]
    assert [record["concern"] for record in payload["requirements"]] == [
        "customer_notification",
        "provider_reference",
        "audit_evidence",
    ]
    assert markdown.startswith("# Source Refund Workflow Requirements Report: refund-model")
    assert "| Concern | Value | Confidence | Source Field | Evidence | Suggested Plan Impacts |" in markdown
    assert "support \\| finance views" in markdown


def test_negated_scope_unrelated_payment_copy_malformed_and_object_inputs_are_stable_empty():
    class BriefLike:
        id = "object-no-refunds"
        summary = "No refund workflow, credits, disputes, or reversals are required for this release."

    object_result = build_source_refund_workflow_requirements(
        SimpleNamespace(
            id="object-refunds",
            summary="Refund requests must notify customers and record audit evidence.",
            metadata={"provider_reference": "Provider refund id is required for each refund."},
        )
    )
    negated = build_source_refund_workflow_requirements(BriefLike())
    no_scope = build_source_refund_workflow_requirements(
        _source_brief(summary="Refunds are out of scope and no refund support is planned.")
    )
    unrelated_payment = build_source_refund_workflow_requirements(
        _source_brief(
            title="Payment failure copy",
            summary="Payment failure emails should explain card declines and retry schedule.",
            source_payload={"requirements": ["Store payment method update copy and invoice retry labels."]},
        )
    )
    malformed = build_source_refund_workflow_requirements({"source_payload": {"notes": object()}})
    blank = build_source_refund_workflow_requirements("")

    expected_summary = {
        "requirement_count": 0,
        "concerns": [],
        "concern_counts": {
            "eligibility": 0,
            "refund_scope": 0,
            "approval": 0,
            "ledger_accounting": 0,
            "customer_notification": 0,
            "provider_reference": 0,
            "idempotency": 0,
            "audit_evidence": 0,
            "dispute_credit_reversal": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_refund_workflow_language",
    }
    assert [record.concern for record in object_result.records] == [
        "customer_notification",
        "provider_reference",
        "audit_evidence",
    ]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated_payment.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert unrelated_payment.summary == expected_summary
    assert unrelated_payment.to_dicts() == []
    assert "No source refund workflow requirements were inferred" in unrelated_payment.to_markdown()
    assert summarize_source_refund_workflow_requirements(unrelated_payment) == expected_summary


def _source_brief(
    *,
    source_id="source-refunds",
    title="Refund workflow requirements",
    domain="billing",
    summary="General refund workflow requirements.",
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
    brief_id="implementation-refunds",
    title="Refund workflow implementation",
    problem_statement="Implement source-backed refund workflows.",
    mvp_goal="Ship refund workflow planning support.",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-refunds",
        "title": title,
        "domain": "billing",
        "target_user": "support agent",
        "buyer": "finance",
        "workflow_context": "Refund operations",
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
        "validation_plan": "Run refund workflow extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
