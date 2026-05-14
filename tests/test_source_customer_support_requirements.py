import json

from blueprint.source_customer_support_requirements import (
    build_source_customer_support_requirements,
    derive_source_customer_support_requirements,
    extract_source_customer_support_requirements,
    generate_source_customer_support_requirements,
    source_customer_support_requirements_to_dict,
    source_customer_support_requirements_to_dicts,
    source_customer_support_requirements_to_markdown,
    summarize_source_customer_support_requirements,
)


def test_extracts_all_customer_support_categories():
    result = build_source_customer_support_requirements(_source([
        "Customer support ticket intake must accept email, chat, portal form, and API channels.",
        "Customer support routing assignment must route by tier, region, and skill queue rules.",
        "Customer support SLA priority must set P1 response time within 15 minutes.",
        "Customer support customer context must show plan, subscription, orders, and support history.",
        "Customer support internal notes must keep private agent collaboration notes.",
        "Customer support escalation must hand off tier 2 issues to engineering managers.",
        "Customer support resolution notification must send customer email message templates.",
        "Customer support reporting metrics must track CSAT, first response, and resolution time.",
    ]))

    assert [record.requirement_type for record in result.records] == ["ticket_intake", "routing_assignment", "sla_priority", "customer_context", "internal_notes", "escalation", "resolution_notification", "reporting_metrics"]
    assert result.summary["missing_detail_flags"] == []


def test_partial_brief_flags_routing_sla_and_escalation_details():
    result = derive_source_customer_support_requirements("Customer support routing assignment is required. Customer support SLA priority is required. Customer support escalation is required.")

    assert result.summary["missing_detail_flags"] == ["missing_routing", "missing_sla", "missing_escalation"]


def test_dict_dicts_markdown_serializers_and_public_aliases():
    result = extract_source_customer_support_requirements(_source(["Customer support ticket intake must accept portal form channels."], "support-1"))
    payload = source_customer_support_requirements_to_dict(result)

    assert generate_source_customer_support_requirements("Customer support reporting metrics must track CSAT dashboard metrics.").summary["requirement_count"] == 1
    assert summarize_source_customer_support_requirements(result)["requirement_count"] == 1
    assert json.loads(json.dumps(payload))["source_id"] == "support-1"
    assert source_customer_support_requirements_to_dicts(result) == payload["records"]
    assert source_customer_support_requirements_to_dicts(result.records) == payload["records"]
    assert "# Source Customer Support Requirements Report: support-1" in source_customer_support_requirements_to_markdown(result)
    assert build_source_customer_support_requirements("No customer support workflow changes are required.").records == ()


def _source(lines, source_id="support-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "title": "Customer support workflow", "summary": "Customer support workflow planning", "source_payload": {"requirements": lines}, "source_links": {}}
