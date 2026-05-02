import json

from blueprint.domain.models import SourceBrief
from blueprint.source_fraud_prevention_requirements import (
    SourceFraudPreventionRequirement,
    SourceFraudPreventionRequirementsReport,
    build_source_fraud_prevention_requirements,
    derive_source_fraud_prevention_requirements,
    extract_source_fraud_prevention_requirements,
    generate_source_fraud_prevention_requirements,
    source_fraud_prevention_requirements_to_dict,
    source_fraud_prevention_requirements_to_dicts,
    source_fraud_prevention_requirements_to_markdown,
    summarize_source_fraud_prevention_requirements,
)


def test_extracts_explicit_fraud_abuse_controls_from_text_and_structured_fields():
    result = build_source_fraud_prevention_requirements(
        _source_brief(
            summary=(
                "Signup must detect suspicious activity and block account takeover attempts. "
                "Risk scoring should flag high-risk accounts for manual review."
            ),
            source_payload={
                "security": {
                    "fraud_controls": [
                        "Velocity limits require no more than 5 signup attempts per device per hour.",
                        "Device fingerprint signals are required for new marketplace sellers.",
                    ],
                    "enforcement": "Suspend accounts after confirmed abuse review.",
                }
            },
        )
    )

    by_category = {record.requirement_category: record for record in result.records}

    assert isinstance(result, SourceFraudPreventionRequirementsReport)
    assert all(isinstance(record, SourceFraudPreventionRequirement) for record in result.records)
    assert {
        "suspicious_activity_detection",
        "velocity_limits",
        "account_takeover",
        "manual_review",
        "risk_scoring",
        "device_fingerprint",
        "enforcement_actions",
    } <= set(by_category)
    assert by_category["velocity_limits"].value == "no more than 5 signup attempts"
    assert by_category["manual_review"].suggested_owner == "Risk Operations"
    assert by_category["risk_scoring"].planning_note.startswith("Define score inputs")
    assert any(
        "suspicious activity" in item
        for item in by_category["suspicious_activity_detection"].evidence
    )
    assert any(
        "source_payload.security.fraud_controls[1]" in item
        for item in by_category["device_fingerprint"].evidence
    )
    assert result.summary["requirement_category_counts"]["device_fingerprint"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_extracts_payment_chargeback_nested_metadata_and_serialization_helpers():
    source = _source_brief(
        source_id="fraud-marketplace",
        title="Marketplace fraud prevention",
        summary="Checkout should prevent payment fraud for high-risk payments.",
        source_payload={
            "acceptance_criteria": [
                "Payment fraud rules must detect card testing before order submission.",
                "Chargeback monitoring is required for dispute rate spikes.",
            ],
            "metadata": {
                "risk": {
                    "signals": {
                        "device_fingerprint": "Collect device reputation and IP reputation for seller payouts.",
                    }
                },
                "operations": {
                    "manual_review": "Fraud review queue routes high-risk sellers to analysts.",
                },
            },
        },
    )
    model = SourceBrief.model_validate(source)

    result = build_source_fraud_prevention_requirements(model)
    generated = generate_source_fraud_prevention_requirements(model)
    derived = derive_source_fraud_prevention_requirements(model)
    extracted = extract_source_fraud_prevention_requirements(model)
    summarized = summarize_source_fraud_prevention_requirements(model)
    payload = source_fraud_prevention_requirements_to_dict(result)
    markdown = source_fraud_prevention_requirements_to_markdown(result)

    categories = {record.requirement_category for record in result.records}
    assert {
        "payment_fraud",
        "manual_review",
        "device_fingerprint",
        "chargeback_monitoring",
    } <= categories
    assert generated.to_dict() == result.to_dict()
    assert derived.to_dict() == result.to_dict()
    assert extracted == result.requirements
    assert summarized.to_dict() == result.to_dict()
    assert source_fraud_prevention_requirements_to_dicts(result) == payload["requirements"]
    assert source_fraud_prevention_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert list(payload) == ["source_brief_id", "title", "summary", "requirements", "records"]
    assert list(payload["requirements"][0]) == [
        "requirement_category",
        "value",
        "source_field",
        "evidence",
        "matched_terms",
        "confidence",
        "suggested_owner",
        "planning_note",
    ]
    assert any(
        "source_payload.metadata.risk.signals.device_fingerprint" in item
        for item in next(
            record
            for record in result.records
            if record.requirement_category == "device_fingerprint"
        ).evidence
    )
    assert markdown.startswith("# Source Fraud Prevention Requirements Report: fraud-marketplace")


def test_plain_text_velocity_chargeback_and_enforcement_are_extracted():
    result = build_source_fraud_prevention_requirements(
        "Payment abuse controls must enforce velocity limits under 10 transactions per card per day. "
        "Chargeback monitoring should alert risk operations and block accounts with confirmed fraud."
    )

    by_category = {record.requirement_category: record for record in result.records}

    assert "velocity_limits" in by_category
    assert "chargeback_monitoring" in by_category
    assert "enforcement_actions" in by_category
    assert by_category["velocity_limits"].value == "under 10 transactions"
    assert any(
        "Chargeback monitoring" in item for item in by_category["chargeback_monitoring"].evidence
    )


def test_unrelated_and_benign_payment_copy_return_stable_empty_reports():
    unrelated = build_source_fraud_prevention_requirements(
        _source_brief(
            title="Payment confirmation copy",
            summary="Show ordinary payment copy after a successful checkout.",
            source_payload={
                "requirements": ["Keep invoice labels and card brand display unchanged."]
            },
        )
    )
    malformed = build_source_fraud_prevention_requirements({"source_payload": {"notes": object()}})
    blank_text = build_source_fraud_prevention_requirements("")

    assert unrelated.records == ()
    assert unrelated.to_dicts() == []
    assert unrelated.summary == {
        "requirement_count": 0,
        "requirement_categories": [],
        "requirement_category_counts": {
            "suspicious_activity_detection": 0,
            "velocity_limits": 0,
            "payment_fraud": 0,
            "account_takeover": 0,
            "manual_review": 0,
            "risk_scoring": 0,
            "device_fingerprint": 0,
            "chargeback_monitoring": 0,
            "enforcement_actions": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_fraud_prevention_language",
    }
    assert malformed.records == ()
    assert blank_text.records == ()
    assert "No source fraud prevention requirements were inferred" in unrelated.to_markdown()


def _source_brief(
    *,
    source_id="source-fraud-prevention",
    title="Fraud prevention requirements",
    domain="platform",
    summary="General fraud prevention requirements.",
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
