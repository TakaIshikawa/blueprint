import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_fraud_abuse_requirements import (
    SourceFraudAbuseRequirement,
    SourceFraudAbuseRequirementsReport,
    build_source_fraud_abuse_requirements,
    derive_source_fraud_abuse_requirements,
    extract_source_fraud_abuse_requirements,
    generate_source_fraud_abuse_requirements,
    source_fraud_abuse_requirements_to_dict,
    source_fraud_abuse_requirements_to_dicts,
    source_fraud_abuse_requirements_to_markdown,
    summarize_source_fraud_abuse_requirements,
)


def test_free_text_extracts_fraud_and_abuse_signals_with_planning_notes():
    result = build_source_fraud_abuse_requirements(
        _source_brief(
            source_payload={
                "body": """
# Fraud and abuse controls

- Fraud scoring must assign checkout risk score tiers with a score threshold before payment.
- Account abuse controls should detect duplicate accounts and suspend fake account takeover attempts.
- Signup abuse needs verify email for suspicious account creation.
- Payment fraud should block stolen card payment attempts in checkout.
- Suspicious activity must alert support when abnormal billing activity is flagged.
- Velocity limits require 3 transactions per card per hour before throttling.
- Device fingerprinting must capture device reputation for login risk decisions.
- IP reputation should block proxy detection, VPN detection, and Tor exit traffic during signup.
- Manual review must route fraud review cases to the risk review queue.
- Chargeback risk requires dispute evidence and refund holds for billing.
- Bot detection should challenge headless browser and credential stuffing attempts.
- Abuse reporting must let users report abuse and route each report to a moderation queue.
"""
            }
        )
    )

    assert isinstance(result, SourceFraudAbuseRequirementsReport)
    assert result.source_id == "sb-fraud"
    assert all(isinstance(record, SourceFraudAbuseRequirement) for record in result.records)
    assert [record.abuse_signal for record in result.records] == [
        "fraud_scoring",
        "account_abuse",
        "signup_abuse",
        "payment_fraud",
        "suspicious_activity",
        "velocity_limits",
        "device_fingerprinting",
        "ip_reputation",
        "manual_review",
        "chargeback_risk",
        "bot_detection",
        "abuse_reporting",
    ]
    by_signal = {record.abuse_signal: record for record in result.records}
    assert by_signal["fraud_scoring"].protected_flow == "checkout"
    assert by_signal["account_abuse"].enforcement_action == "suspend"
    assert by_signal["signup_abuse"].enforcement_action == "verify"
    assert by_signal["signup_abuse"].protected_flow == "signup"
    assert by_signal["payment_fraud"].enforcement_action == "block"
    assert by_signal["manual_review"].review_path == "manual review"
    assert by_signal["abuse_reporting"].review_path == "moderation queue"
    assert all(record.planning_note for record in result.records)
    assert "threshold" in by_signal["fraud_scoring"].planning_note
    assert result.summary["requirement_count"] == 12
    assert result.summary["signal_counts"] == {signal: 1 for signal in result.summary["signals"]}


def test_structured_payload_and_implementation_brief_inputs_are_supported():
    structured = build_source_fraud_abuse_requirements(
        _source_brief(
            source_payload={
                "trust_safety": {
                    "fraud_scoring": {
                        "abuse_signal": "fraud_scoring",
                        "protected_flow": "checkout",
                        "enforcement_action": "hold payment when risk score exceeds 80",
                        "review_path": "manual review queue",
                    },
                    "chargeback_risk": "Chargeback risk requires dispute evidence for billing support.",
                    "bot_detection": "Bot detection must challenge automated signup attempts.",
                }
            }
        )
    )
    implementation = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Velocity limits require 10 login attempts per account per hour.",
                "IP reputation should block Tor exit traffic during signup.",
            ],
            definition_of_done=[
                "Manual review queue receives suspicious activity alerts before enforcement.",
            ],
        )
    )
    object_result = build_source_fraud_abuse_requirements(
        SimpleNamespace(
            id="object-fraud",
            summary="Payment fraud controls must decline card testing attempts during checkout.",
        )
    )

    assert [record.abuse_signal for record in structured.records] == [
        "fraud_scoring",
        "chargeback_risk",
        "bot_detection",
    ]
    assert structured.records[0].confidence == "high"
    assert structured.records[0].source_field == "source_payload.trust_safety.fraud_scoring"
    assert structured.records[0].protected_flow == "checkout"
    assert structured.records[0].review_path == "manual review queue"
    assert structured.records[1].confidence == "high"

    impl_result = generate_source_fraud_abuse_requirements(implementation)
    assert impl_result.source_id == "impl-fraud"
    assert [record.abuse_signal for record in impl_result.records] == [
        "suspicious_activity",
        "velocity_limits",
        "ip_reputation",
        "manual_review",
    ]
    assert impl_result.records[1].source_field == "scope[0]"
    assert object_result.records[0].abuse_signal == "payment_fraud"
    assert object_result.records[0].confidence == "medium"


def test_negated_invalid_and_no_signal_inputs_return_stable_empty_reports():
    empty = build_source_fraud_abuse_requirements(
        _source_brief(
            summary="Billing copy update.",
            source_payload={"body": "No fraud, abuse, chargeback, bot, or rate limit changes are required."},
        )
    )
    repeat = build_source_fraud_abuse_requirements(
        _source_brief(
            summary="Billing copy update.",
            source_payload={"body": "No fraud, abuse, chargeback, bot, or rate limit changes are required."},
        )
    )
    malformed = build_source_fraud_abuse_requirements({"id": "brief-empty", "source_payload": {"notes": object()}})
    invalid = build_source_fraud_abuse_requirements(42)

    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "sb-fraud"
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == {
        "source_count": 1,
        "requirement_count": 0,
        "signal_counts": {
            "fraud_scoring": 0,
            "account_abuse": 0,
            "signup_abuse": 0,
            "payment_fraud": 0,
            "suspicious_activity": 0,
            "velocity_limits": 0,
            "device_fingerprinting": 0,
            "ip_reputation": 0,
            "manual_review": 0,
            "chargeback_risk": 0,
            "bot_detection": 0,
            "abuse_reporting": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "signals": [],
    }
    assert "No fraud or abuse-prevention requirements were found" in empty.to_markdown()
    assert malformed.records == ()
    assert invalid.records == ()


def test_deduped_evidence_stable_order_low_confidence_and_markdown_escaping():
    result = build_source_fraud_abuse_requirements(
        _source_brief(
            source_id="fraud-dedupe",
            source_payload={
                "requirements": [
                    "Abuse reporting must route report abuse submissions for customer | partner cases.",
                    "Abuse reporting must route report abuse submissions for customer | partner cases.",
                    "Device fingerprinting notes for login risk.",
                    "Manual review.",
                ]
            },
        )
    )

    assert [record.abuse_signal for record in result.records] == [
        "device_fingerprinting",
        "manual_review",
        "abuse_reporting",
    ]
    device = result.records[0]
    assert device.confidence == "low"
    assert any("What enforcement action" in question for question in device.unresolved_questions)
    assert result.records[1].confidence == "medium"
    report = result.records[2]
    assert report.evidence == (
        "source_payload.requirements[0]: Abuse reporting must route report abuse submissions for customer | partner cases.",
    )
    markdown = result.to_markdown()
    assert "| Source Brief | Abuse Signal | Requirement | Protected Flow | Enforcement Action | Review Path | Source Field | Confidence | Planning Note | Unresolved Questions | Evidence |" in markdown
    assert "customer \\| partner cases" in markdown


def test_serialization_aliases_json_ordering_and_no_input_mutation_are_stable():
    source = _source_brief(
        source_id="fraud-model",
        source_payload={
            "requirements": [
                "Fraud scoring must set a checkout risk score threshold and escape plan | review notes.",
                "Payment fraud controls must hold checkout payment attempts for manual review.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_fraud_abuse_requirements(source)
    model_result = extract_source_fraud_abuse_requirements(model)
    derived = derive_source_fraud_abuse_requirements(model)
    text_result = build_source_fraud_abuse_requirements("Bot detection should challenge scripted signup attempts.")
    payload = source_fraud_abuse_requirements_to_dict(model_result)
    markdown = source_fraud_abuse_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_fraud_abuse_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert source_fraud_abuse_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_fraud_abuse_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_fraud_abuse_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "summary", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "source_brief_id",
        "abuse_signal",
        "requirement_text",
        "protected_flow",
        "enforcement_action",
        "review_path",
        "source_field",
        "evidence",
        "confidence",
        "planning_note",
        "unresolved_questions",
    ]
    assert [record.abuse_signal for record in model_result.records] == [
        "fraud_scoring",
        "payment_fraud",
        "manual_review",
    ]
    assert model_result.records[0].category == "fraud_scoring"
    assert model_result.records[0].requirement_category == "fraud_scoring"
    assert model_result.records[0].planning_notes == (model_result.records[0].planning_note,)
    assert markdown == model_result.to_markdown()
    assert "plan \\| review notes" in markdown
    assert text_result.records[0].abuse_signal == "signup_abuse"
    assert text_result.records[1].abuse_signal == "bot_detection"


def _source_brief(
    *,
    source_id="sb-fraud",
    title="Fraud and abuse requirements",
    domain="trust_safety",
    summary="General fraud and abuse requirements.",
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
        "id": "impl-fraud",
        "source_brief_id": "source-fraud",
        "title": "Fraud and abuse rollout",
        "domain": "trust_safety",
        "target_user": "operators",
        "buyer": None,
        "workflow_context": "Teams need fraud and abuse requirements before task generation.",
        "problem_statement": "Fraud and abuse requirements need to be extracted early.",
        "mvp_goal": "Plan fraud and abuse work from source briefs.",
        "product_surface": "trust_safety",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Review generated plan for fraud and abuse coverage.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
    }
