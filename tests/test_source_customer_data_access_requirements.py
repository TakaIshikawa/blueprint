from blueprint.source_customer_data_access_requirements import build_source_customer_data_access_requirements


def test_customer_data_access_brief_extracts_ordered_records():
    report = build_source_customer_data_access_requirements(
        _source(
            [
                "DSAR request must be accepted through portal intake for GDPR customers with privacy team owner.",
                "Identity verification must use email challenge and ID evidence with manual review fallback.",
                "Access scope must include profile, billing, usage systems, and exclude legal hold records.",
                "Delivery format must be JSON and CSV via encrypted secure download with retention expiry.",
                "Fulfillment SLA requires response within 30 days after verified intake date with legal owner.",
                "Audit evidence must retain request log schema and case history for compliance review owner.",
                "Denial handling must list denial reasons, appeal path, and privacy escalation owner.",
            ]
        )
    )

    assert [record.requirement_type for record in report.records] == [
        "dsar_request",
        "identity_verification",
        "access_scope",
        "delivery_format",
        "fulfillment_sla",
        "audit_evidence",
        "denial_escalation",
    ]
    assert all(record.evidence for record in report.records)


def test_partial_customer_access_brief_reports_missing_details():
    report = build_source_customer_data_access_requirements(_source(["DSAR request is required.", "Delivery format is needed."]))
    by_type = {record.requirement_type: record for record in report.records}

    assert by_type["dsar_request"].missing_details == ("intake_channel", "request_owner", "customer_regions")
    assert by_type["delivery_format"].missing_details == ("format", "secure_delivery", "retention")
    assert by_type["delivery_format"].missing_detail_guidance == "format; secure_delivery; retention"


def test_negated_scope_returns_empty_and_serializes():
    empty = build_source_customer_data_access_requirements(_source(["No DSAR or customer data access request support is required."]))
    report = build_source_customer_data_access_requirements("Customer data access request must verify identity and deliver JSON.")

    assert empty.records == ()
    assert report.to_dicts() == report.to_dict()["requirements"]
    assert "Customer Data Access" in report.to_markdown()


def _source(requirements):
    return {
        "id": "sb-customer-access",
        "title": "Customer data access",
        "domain": "privacy",
        "summary": "Privacy planning.",
        "source_project": "project",
        "source_entity_type": "issue",
        "source_id": "issue-customer-access",
        "source_payload": {"requirements": requirements},
        "source_links": {},
    }
