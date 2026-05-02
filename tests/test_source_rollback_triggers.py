import copy
import json

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_rollback_triggers import (
    SourceRollbackTrigger,
    SourceRollbackTriggerReport,
    build_source_rollback_trigger_report,
    extract_source_rollback_triggers,
    source_rollback_trigger_report_to_dict,
)


def test_markdown_like_brief_body_extracts_rollback_thresholds():
    result = build_source_rollback_trigger_report(
        _source(
            source_payload={
                "body": """
# Rollback Criteria

- Roll back if 5xx error rate exceeds 2% for 10 minutes.
- Revert if p95 latency is above 800ms during canary.
- Abort launch if customer complaints spike above 25 reports in one hour.
"""
            }
        )
    )

    assert isinstance(result, SourceRollbackTriggerReport)
    assert [trigger.category for trigger in result.triggers] == [
        "error_rate",
        "latency",
        "customer_complaints",
    ]
    assert result.summary["trigger_count"] == 3
    assert result.summary["high_confidence_count"] == 3
    assert result.triggers[0].recommended_owner == "engineering_oncall"
    assert "error-rate" in result.triggers[0].suggested_plan_note


def test_structured_acceptance_and_risk_fields_extract_triggers_from_models():
    source = _implementation(
        risks=[
            "Abort rollout if payment conversion drops below 97% of baseline.",
            "Kill switch if support tickets exceed 40 per day.",
        ],
        definition_of_done=[
            "Rollback if reconciliation finds duplicate records or data integrity mismatches.",
            "Security must give manual go/no-go sign-off before 100% rollout.",
        ],
    )
    model = ImplementationBrief.model_validate(source)

    result = build_source_rollback_trigger_report(model)

    assert result.brief_id == "impl-rollback"
    assert [trigger.category for trigger in result.triggers] == [
        "data_integrity",
        "revenue_impact",
        "support_volume",
        "security_signal",
        "manual_decision",
    ]
    owners = {trigger.category: trigger.recommended_owner for trigger in result.triggers}
    assert owners["data_integrity"] == "data_owner"
    assert owners["security_signal"] == "security_owner"
    assert owners["manual_decision"] == "release_owner"


def test_duplicate_evidence_for_same_category_is_collapsed_deterministically():
    result = build_source_rollback_trigger_report(
        _source(
            source_payload={
                "rollback": {
                    "error_rate": "Roll back if error rate exceeds 1%.",
                    "same_error_rate": "Roll back if error rate exceeds 1%.",
                    "latency": "Revert if p99 latency exceeds 2s.",
                },
                "acceptance_criteria": [
                    "Roll back if error rate exceeds 1%.",
                    "Revert if p99 latency exceeds 2s.",
                ],
            }
        )
    )

    assert [trigger.category for trigger in result.triggers] == ["error_rate", "latency"]
    assert len(result.triggers[0].evidence) == 1
    assert result.triggers[0].evidence == (
        "source_payload.acceptance_criteria[0]: Roll back if error rate exceeds 1%.",
    )


def test_stable_category_ordering_ignores_source_field_order():
    result = build_source_rollback_trigger_report(
        {
            "id": "brief-order",
            "source_payload": {
                "rollback": {
                    "manual": "Manual go/no-go approval is required before expanding rollout.",
                    "security": "Abort if fraud or suspicious auth activity appears.",
                    "revenue": "Revert if checkout revenue drops more than 3%.",
                    "latency": "Rollback if timeout count or p95 latency exceeds 600ms.",
                }
            },
        }
    )

    assert [trigger.category for trigger in result.triggers] == [
        "latency",
        "revenue_impact",
        "security_signal",
        "manual_decision",
    ]


def test_no_trigger_summary_is_empty_but_complete():
    result = build_source_rollback_trigger_report(
        _source(
            summary="Improve onboarding copy and analytics labels.",
            source_payload={"body": "Ship when copy review is complete."},
        )
    )

    assert result.triggers == ()
    assert result.records == ()
    assert result.to_dicts() == []
    assert result.summary["trigger_count"] == 0
    assert result.summary["high_confidence_count"] == 0
    assert set(result.summary["category_counts"]) == {
        "error_rate",
        "latency",
        "data_integrity",
        "revenue_impact",
        "support_volume",
        "security_signal",
        "customer_complaints",
        "manual_decision",
    }


def test_mapping_sourcebrief_and_serialization_are_stable_without_mutation():
    source = _source(
        source_payload={
            "acceptance": [
                "Abort if CSAT complaints exceed 10 accounts during beta.",
                "Rollback if security incident alerts fire.",
            ],
            "risks": [{"rollback": "Revert if data loss or corrupt records are detected."}],
        }
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_rollback_trigger_report(source)
    model_result = build_source_rollback_trigger_report(model)
    extracted = extract_source_rollback_triggers(model)
    payload = source_rollback_trigger_report_to_dict(model_result)

    assert source == original
    assert mapping_result.to_dict() == model_result.to_dict()
    assert extracted == model_result.triggers
    assert model_result.records == model_result.triggers
    assert isinstance(model_result.triggers[0], SourceRollbackTrigger)
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["brief_id", "triggers", "summary"]
    assert list(payload["triggers"][0]) == [
        "category",
        "confidence",
        "evidence",
        "recommended_owner",
        "suggested_plan_note",
    ]


def _source(*, summary="Launch guarded checkout rollout.", source_payload=None):
    return {
        "id": "source-rollback",
        "title": "Checkout rollout",
        "domain": "commerce",
        "summary": summary,
        "source_project": "blueprint",
        "source_entity_type": "issue",
        "source_id": "ISSUE-1",
        "source_payload": source_payload or {},
        "source_links": {},
    }


def _implementation(*, risks=None, definition_of_done=None):
    return {
        "id": "impl-rollback",
        "source_brief_id": "source-rollback",
        "title": "Checkout rollout",
        "domain": "commerce",
        "target_user": "merchant",
        "buyer": "ops",
        "workflow_context": "Release checkout changes behind a feature flag.",
        "problem_statement": "Checkout rollout needs explicit rollback thresholds.",
        "mvp_goal": "Guard rollout with measurable rollback gates.",
        "product_surface": "checkout",
        "scope": ["Feature flag rollout"],
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": risks or [],
        "validation_plan": "Watch dashboards during canary.",
        "definition_of_done": definition_of_done or [],
    }
