import json

from blueprint.source_payment_dispute_evidence_requirements import (
    build_source_payment_dispute_evidence_requirements,
    derive_source_payment_dispute_evidence_requirements,
    source_payment_dispute_evidence_requirements_to_dict,
    source_payment_dispute_evidence_requirements_to_dicts,
    source_payment_dispute_evidence_requirements_to_markdown,
)


def test_extracts_payment_dispute_evidence_categories_in_order():
    result = build_source_payment_dispute_evidence_requirements(
        _source(
            [
                "Payment dispute evidence collection must build an evidence packet with receipt, invoice, communication, and tracking artifacts.",
                "Chargeback transaction timeline must include authorized, captured, disputed, and submitted timestamps.",
                "Payment dispute customer communication must include email, ticket, and chat transcript artifacts.",
                "Payment dispute receipt and invoice artifacts must attach PDF order confirmation.",
                "Chargeback fulfillment proof must include tracking number and delivered carrier status.",
                "Processor submission deadlines must alert 3 business days before representment deadline.",
                "Representment status tracking must sync processor status from Stripe webhooks.",
                "Payment dispute retention must keep evidence for 24 months before purge.",
                "Payment dispute compliance review must route card network rules to legal approval.",
            ]
        )
    )

    assert [record.requirement_type for record in result.records] == [
        "evidence_collection",
        "transaction_timeline",
        "customer_communication",
        "receipt_invoice_artifacts",
        "fulfillment_proof",
        "processor_submission_deadlines",
        "representment_status_tracking",
        "retention",
        "compliance_review",
    ]
    assert result.summary["missing_detail_flags"] == []


def test_partial_dispute_brief_flags_artifacts_deadlines_and_retention_compliance():
    result = derive_source_payment_dispute_evidence_requirements(
        _source(
            [
                "Payment dispute evidence collection is required.",
                "Chargeback processor submission deadlines are required.",
                "Representment status tracking should be visible.",
                "Payment dispute retention and compliance review are required.",
            ],
            source_id="pay-dispute-partial",
        )
    )

    assert result.source_id == "pay-dispute-partial"
    assert result.summary["missing_detail_flags"] == [
        "missing_evidence_artifacts",
        "missing_deadline_handling",
        "missing_retention_compliance_details",
    ]


def test_serializers_aliases_negated_and_invoice_dispute_noise():
    result = build_source_payment_dispute_evidence_requirements(
        _source(["Invoice dispute workflow needs comment resolution but no payment dispute evidence changes are required."])
    )
    populated = build_source_payment_dispute_evidence_requirements(
        "Payment dispute evidence collection must include receipt artifacts."
    )

    assert result.records == ()
    payload = source_payment_dispute_evidence_requirements_to_dict(populated)
    assert json.loads(json.dumps(payload, sort_keys=True))["summary"]["requirement_count"] == 2
    assert source_payment_dispute_evidence_requirements_to_dicts(populated) == payload["records"]
    assert "# Source Payment Dispute Evidence Requirements Report" in source_payment_dispute_evidence_requirements_to_markdown(populated)


def _source(lines, source_id="pay-dispute"):
    return {
        "id": source_id,
        "source_project": "requirements",
        "source_entity_type": "brief",
        "source_id": f"{source_id}-upstream",
        "title": "Payment dispute evidence",
        "summary": "Payment dispute evidence planning",
        "source_payload": {"requirements": lines},
        "source_links": {},
    }
