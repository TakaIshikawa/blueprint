import json

from blueprint.source_plan_downgrade_requirements import (
    build_source_plan_downgrade_requirements,
    derive_source_plan_downgrade_requirements,
    extract_source_plan_downgrade_requirements,
    generate_source_plan_downgrade_requirements,
    source_plan_downgrade_requirements_to_dict,
    source_plan_downgrade_requirements_to_dicts,
    source_plan_downgrade_requirements_to_markdown,
    summarize_source_plan_downgrade_requirements,
)


def test_extracts_all_plan_downgrade_categories_from_nested_payload():
    result = build_source_plan_downgrade_requirements(_source({
        "requirements": [
            "Plan downgrade eligibility must allow downgrade only if the minimum term is complete.",
            "Plan downgrade feature removal must disable premium exports and advanced limits.",
            "Plan downgrade entitlement transition must map current access level to the lower tier entitlement state.",
            "Plan downgrade billing proration must credit unused time on the next invoice.",
            "Plan downgrade scheduled effective date must take effect at billing cycle end.",
            "Plan downgrade customer notice must send email and in-app messages 7 days before.",
            "Plan downgrade data retention must retain read-only data for 30 days.",
            "Plan downgrade support exception must require an agent approval ticket.",
        ]
    }))

    assert [record.requirement_type for record in result.records] == ["downgrade_eligibility", "feature_removal", "entitlement_transition", "billing_proration", "scheduled_effective_date", "customer_notice", "data_retention", "support_exception"]
    assert result.summary["missing_detail_flags"] == []


def test_partial_downgrade_requirements_need_detail():
    result = derive_source_plan_downgrade_requirements("Plan downgrade eligibility is required. Plan downgrade effective date is required. Plan downgrade billing proration is required. Plan downgrade customer notice is required. Plan downgrade entitlement transition is required.")

    assert result.summary["missing_detail_flags"] == ["missing_eligibility", "missing_effective_date", "missing_proration", "missing_notice", "missing_entitlement_transition"]


def test_helpers_serializers_negation_and_invalid_inputs_are_stable():
    report = extract_source_plan_downgrade_requirements(_source({"requirements": ["Plan downgrade support exception must create a support ticket."]}, "downgrade-model"))
    payload = source_plan_downgrade_requirements_to_dict(report)

    assert generate_source_plan_downgrade_requirements("Plan downgrade data retention must retain data for 90 days.") .summary["requirement_count"] == 1
    assert summarize_source_plan_downgrade_requirements(report)["requirement_count"] == 1
    assert build_source_plan_downgrade_requirements("").records == ()
    assert build_source_plan_downgrade_requirements(3.14).records == ()
    assert build_source_plan_downgrade_requirements("No plan downgrade changes are required.").records == ()
    assert json.loads(json.dumps(payload))["source_id"] == "downgrade-model"
    assert source_plan_downgrade_requirements_to_dicts(report) == payload["records"]
    assert "Source Plan Downgrade Requirements Report" in source_plan_downgrade_requirements_to_markdown(report)


def _source(payload, source_id="downgrade-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "source_id": f"{source_id}-upstream", "title": "Plan downgrade", "summary": "Plan downgrade planning", "source_payload": payload, "source_links": {}}
