import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_constraint_confidence import (
    SourceConstraintConfidenceFinding,
    SourceConstraintConfidenceReport,
    build_source_constraint_confidence_report,
    source_constraint_confidence_report_to_dict,
    source_constraint_confidence_report_to_markdown,
    summarize_source_constraint_confidence,
)


def test_source_brief_like_constraints_score_by_source_acceptance_and_repetition():
    result = build_source_constraint_confidence_report(
        {
            "id": "source-brief",
            "title": "Checkout rollout",
            "summary": "Add checkout capture safeguards.",
            "source_project": "linear",
            "source_entity_type": "issue",
            "source_id": "PAY-42",
            "source_links": {"issue": "https://linear.example/PAY-42"},
            "source_payload": {
                "constraints": [
                    {
                        "text": "Checkout capture must use idempotency keys.",
                        "source_ref": "PAY-42 comment 3",
                    },
                    "Assume the receipt email can be delayed until a later release.",
                    "Support PayPal refunds?",
                ],
                "acceptance_criteria": [
                    "Checkout capture uses idempotency keys and rejects duplicate capture requests.",
                    "Receipts are not part of MVP.",
                ],
                "assumptions": ["Receipt email can be delayed until after checkout launch."],
                "risks": ["Support PayPal refunds? Unknown provider owner and refund policy."],
                "open_questions": ["Should Support PayPal refunds in MVP?"],
            },
        }
    )

    assert isinstance(result, SourceConstraintConfidenceReport)
    assert result.brief_id == "source-brief"
    assert result.summary["confidence_counts"] == {"high": 1, "medium": 1, "low": 1}
    assert result.summary["source_reference_count"] == 3

    checkout = _finding(result, "Checkout capture must use idempotency keys.")
    assert isinstance(checkout, SourceConstraintConfidenceFinding)
    assert checkout.confidence_level == "high"
    assert checkout.evidence_score >= 7
    assert "PAY-42 comment 3" in checkout.source_references
    assert any("duplicate capture" in item for item in checkout.acceptance_criteria_support)
    assert any("acceptance_criteria" in item for item in checkout.repeated_evidence)
    assert checkout.recommended_follow_up == ()
    assert checkout.owner_suggestion == "engineering"

    receipt = _finding(result, "Assume the receipt email can be delayed until a later release.")
    assert receipt.confidence_level == "medium"
    assert any("assumptions" in item for item in receipt.assumption_support)
    assert any("Receipts are not part of MVP" in item for item in receipt.acceptance_criteria_support)

    refunds = _finding(result, "Support PayPal refunds?")
    assert refunds.confidence_level == "low"
    assert refunds.question_signals
    assert refunds.contradiction_signals
    assert any("Answer the linked open question" in item for item in refunds.recommended_follow_up)
    assert refunds.owner_suggestion == "product"


def test_implementation_brief_model_dict_and_object_inputs_are_supported():
    brief_dict = _implementation_brief(
        scope=[
            "Admin dashboard must preserve existing role permissions.",
            "Analytics event names must remain backward compatible.",
        ],
        definition_of_done=[
            "Existing role permissions are covered by regression tests.",
            "Analytics event names remain backward compatible across dashboard changes.",
        ],
        assumptions=["Admin dashboard permission model is unchanged."],
        risks=["Legacy tracking plan needs analytics event mapping."],
    )
    original = copy.deepcopy(brief_dict)

    model_result = build_source_constraint_confidence_report(ImplementationBrief.model_validate(brief_dict))
    dict_result = summarize_source_constraint_confidence(brief_dict)
    object_result = build_source_constraint_confidence_report(
        SimpleNamespace(
            id="object-brief",
            constraints=["API latency must stay under 200 ms for account lookup."],
            acceptance_criteria=["Account lookup API latency stays under 200 ms in smoke tests."],
            risks=["Performance budget is unclear until staging data is available."],
            open_questions=["Confirm whether 200 ms is p95 or p99?"],
        )
    )

    assert brief_dict == original
    assert dict_result.to_dict() == model_result.to_dict()
    assert model_result.brief_id == "impl-brief"
    assert [finding.confidence_level for finding in model_result.constraints] == ["high", "medium"]
    assert _finding(model_result, "Admin dashboard must preserve existing role permissions.").owner_suggestion == "security"
    assert _finding(model_result, "Analytics event names must remain backward compatible.").confidence_level == "medium"
    assert object_result.brief_id == "object-brief"
    assert object_result.constraints[0].confidence_level == "low"
    assert object_result.constraints[0].question_signals


def test_serialization_markdown_alias_and_stable_json_ordering():
    source = SourceBrief.model_validate(
        {
            "id": "source-model",
            "title": "Profile photos",
            "domain": "consumer",
            "summary": "Add profile photo moderation rules.",
            "source_project": "notion",
            "source_entity_type": "brief",
            "source_id": "PHOTO-7",
            "source_payload": {
                "requirements": {
                    "moderation": {
                        "text": "Profile photos must pass moderation before publishing.",
                        "citation": "Notion PHOTO-7 moderation section",
                    }
                },
                "acceptance_criteria": [
                    "Profile photos pass moderation before publishing and rejected photos stay private."
                ],
                "metadata": {"owner": "trust and safety"},
            },
            "source_links": {"notion": "https://notion.example/photo-7"},
        }
    )

    result = build_source_constraint_confidence_report(source)
    payload = source_constraint_confidence_report_to_dict(result)
    markdown = source_constraint_confidence_report_to_markdown(result)

    assert result.to_dicts() == payload["constraints"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "constraints", "summary"]
    assert list(payload["constraints"][0]) == [
        "constraint_text",
        "confidence_level",
        "evidence_score",
        "source_references",
        "acceptance_criteria_support",
        "repeated_evidence",
        "assumption_support",
        "risk_support",
        "contradiction_signals",
        "question_signals",
        "recommended_follow_up",
        "owner_suggestion",
    ]
    assert markdown.startswith("# Source Constraint Confidence Report: source-model")
    assert "## Summary" in markdown
    assert "| Constraint | Confidence | Score | Evidence | Signals | Follow-up | Owner |" in markdown
    assert "https://notion.example/photo-7" in markdown
    assert source_constraint_confidence_report_to_markdown(result) == result.to_markdown()


def test_empty_invalid_and_unsupported_assumptions_are_stable():
    empty = build_source_constraint_confidence_report({"id": "empty", "constraints": []})
    invalid = build_source_constraint_confidence_report(17)
    unsupported = build_source_constraint_confidence_report(
        {
            "id": "weak",
            "assumptions": ["The new importer will only receive CSV files."],
            "constraints": ["The new importer will only receive CSV files."],
        }
    )

    assert empty.brief_id == "empty"
    assert empty.constraints == ()
    assert empty.summary["constraint_count"] == 0
    assert "No implementation constraints were available" in empty.to_markdown()
    assert invalid.brief_id is None
    assert invalid.constraints == ()
    assert invalid.summary["confidence_counts"] == {"high": 0, "medium": 0, "low": 0}
    assert unsupported.constraints[0].confidence_level == "low"
    assert unsupported.constraints[0].assumption_support
    assert any("Attach a source reference" in item for item in unsupported.constraints[0].recommended_follow_up)


def _finding(result, text):
    return next(finding for finding in result.constraints if finding.constraint_text == text)


def _implementation_brief(
    *,
    constraints=None,
    scope=None,
    definition_of_done=None,
    assumptions=None,
    risks=None,
):
    brief = {
        "id": "impl-brief",
        "source_brief_id": "source-brief",
        "title": "Admin dashboard",
        "domain": "internal",
        "target_user": "Ops",
        "buyer": "Ops",
        "workflow_context": "Account administration",
        "problem_statement": "Admins need safer account updates.",
        "mvp_goal": "Ship safer dashboard changes.",
        "product_surface": "Admin dashboard",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [] if assumptions is None else assumptions,
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [] if risks is None else risks,
        "validation_plan": "Run focused regression tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
    }
    if constraints is not None:
        brief["constraints"] = constraints
    return brief
