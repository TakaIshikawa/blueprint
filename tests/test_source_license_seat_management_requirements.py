import json

from blueprint.domain.models import SourceBrief
from blueprint.source_license_seat_management_requirements import (
    build_source_license_seat_management_requirements,
    derive_source_license_seat_management_requirements,
    extract_source_license_seat_management_requirements,
    generate_source_license_seat_management_requirements,
    source_license_seat_management_requirements_to_dict,
    source_license_seat_management_requirements_to_dicts,
    source_license_seat_management_requirements_to_markdown,
    summarize_source_license_seat_management_requirements,
)


def test_extracts_all_license_seat_management_categories():
    result = build_source_license_seat_management_requirements(_source([
        "License seat management seat assignment must assign user seats automatically by team.",
        "License seat management seat limit must enforce a maximum of 50 licensed seats per plan.",
        "License seat management overage policy must block extra seats unless admin approval allows true-up billing.",
        "License seat management role mapping must map owner, admin, member, and viewer roles.",
        "License seat management deprovisioning must revoke and release seats when users are disabled.",
        "License seat management audit trail must log actor, timestamp, and change events.",
        "License seat management admin controls must support bulk manage seats in the admin console.",
        "License seat management usage reporting must show utilization, unused seats, and dashboard metrics.",
    ]))

    assert [record.requirement_type for record in result.records] == ["seat_assignment", "seat_limit", "overage_policy", "role_mapping", "deprovisioning", "audit_trail", "admin_controls", "usage_reporting"]
    assert result.summary["missing_detail_flags"] == []


def test_structured_payload_and_free_text_flag_missing_details():
    result = derive_source_license_seat_management_requirements({
        "title": "License seat management",
        "source_payload": {
            "billing": "Seat limit for license seat management is required. Overage policy for license seat management is required.",
            "admin": "Role mapping and deprovisioning are required for seat management.",
            "analytics": "Usage reporting is required for license management.",
        },
    })

    assert result.summary["missing_detail_flags"] == ["missing_seat_limit", "missing_overage_policy", "missing_role_mapping", "missing_deprovisioning", "missing_usage_reporting"]
    assert summarize_source_license_seat_management_requirements("License seat management usage reporting must include a utilization dashboard.")["requirement_count"] == 1


def test_model_serializers_negation_and_invalid_inputs_are_stable():
    model = SourceBrief.model_validate(_source(["License seat management audit trail must log actor and timestamp events."], "seat-model"))
    payload = source_license_seat_management_requirements_to_dict(extract_source_license_seat_management_requirements(model))

    assert generate_source_license_seat_management_requirements(_source(["License seat management admin controls must allow bulk seat management."])).summary["requirement_count"] == 1
    assert build_source_license_seat_management_requirements("").records == ()
    assert build_source_license_seat_management_requirements(object()).records == ()
    assert build_source_license_seat_management_requirements("No seat management changes are required.").records == ()
    assert json.loads(json.dumps(payload))["source_id"] == "seat-model"
    assert source_license_seat_management_requirements_to_dicts(extract_source_license_seat_management_requirements(model)) == payload["records"]
    assert "Source License Seat Management Requirements Report" in source_license_seat_management_requirements_to_markdown(extract_source_license_seat_management_requirements(model))


def _source(lines, source_id="seat-source"):
    return {"id": source_id, "source_project": "requirements", "source_entity_type": "brief", "source_id": f"{source_id}-upstream", "title": "License seat management", "summary": "License seat management planning", "source_payload": {"requirements": lines}, "source_links": {}}
