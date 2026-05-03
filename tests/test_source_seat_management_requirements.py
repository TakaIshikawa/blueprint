import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_seat_management_requirements import (
    SourceSeatManagementRequirement,
    SourceSeatManagementRequirementsReport,
    build_source_seat_management_requirements,
    derive_source_seat_management_requirements,
    extract_source_seat_management_requirements,
    generate_source_seat_management_requirements,
    source_seat_management_requirements_to_dict,
    source_seat_management_requirements_to_dicts,
    source_seat_management_requirements_to_markdown,
    summarize_source_seat_management_requirements,
)


def test_nested_source_payload_extracts_seat_management_categories_in_order():
    result = build_source_seat_management_requirements(
        _source_brief(
            source_payload={
                "seat_management": {
                    "allocation": "Seat allocation must reserve paid seats from the workspace seat pool.",
                    "invites": "Invite acceptance should consume a seat only after the invitation is accepted.",
                    "roles": "Role and license assignment must support admin, member, and viewer license tiers.",
                    "overage": "Overage handling charges $12 per seat after a 10% overage threshold.",
                    "limits": "Seat limits enforce a hard cap of 50 seats for the Team plan.",
                    "deprovisioning": "Deprovisioning must deactivate users and release seats.",
                    "reassignment": "Admins can reassign or transfer seats when replacing users.",
                    "proration": "Billing proration applies invoice adjustment credits within 30 days.",
                    "audit": "Admin audit evidence records seat events, actor, target user, and timestamp.",
                    "notifications": "Notifications send email and in-app overage warning alerts to billing admins.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceSeatManagementRequirementsReport)
    assert all(isinstance(record, SourceSeatManagementRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "seat_allocation",
        "invite_acceptance",
        "role_license_assignment",
        "overage_handling",
        "seat_limits",
        "deprovisioning",
        "reassignment",
        "billing_proration",
        "admin_audit_evidence",
        "notifications",
    ]
    assert by_category["seat_limits"].value == "50 seats"
    assert by_category["overage_handling"].value == "$12 per seat"
    assert by_category["billing_proration"].value == "30 days"
    assert by_category["invite_acceptance"].source_field == "source_payload.seat_management.invites"
    assert by_category["admin_audit_evidence"].source_field == "source_payload.seat_management.audit"
    assert by_category["billing_proration"].suggested_owners == ("billing", "finance")
    assert by_category["notifications"].suggested_owners == ("lifecycle_messaging", "billing")
    assert result.summary["requirement_count"] == 10
    assert result.summary["category_counts"]["overage_handling"] == 1
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_brief_fields_and_nested_payload_evidence_paths_are_scanned():
    result = build_source_seat_management_requirements(
        _source_brief(
            title="Seat management launch",
            summary="Seat management requirements include seat allocation and invite acceptance behavior.",
            description="License assignment should map roles to paid license tiers.",
            requirements=[
                "Seat limits must allow up to 25 seats before blocking new members.",
                "Overage handling should true-up extra seats at $15 per seat.",
            ],
            acceptance_criteria=[
                "Deprovisioning releases seats after users are deactivated.",
                "Reassignment supports transfer seats between users.",
            ],
            billing={
                "proration": "Billing proration must apply prorated credits for mid-cycle removals.",
                "audit": "Audit log records license events and seat change history.",
            },
            source_payload={
                "metadata": {
                    "notifications": "Notifications must send Slack and email limit warning alerts.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert {
        "seat_allocation",
        "invite_acceptance",
        "role_license_assignment",
        "overage_handling",
        "seat_limits",
        "deprovisioning",
        "reassignment",
        "billing_proration",
        "admin_audit_evidence",
        "notifications",
    } <= set(by_category)
    assert by_category["seat_limits"].source_field == "requirements[0]"
    assert by_category["reassignment"].source_field == "acceptance_criteria[1]"
    assert by_category["notifications"].source_field == "source_payload.metadata.notifications"
    assert any(item.startswith("summary:") for item in by_category["seat_allocation"].evidence)


def test_plain_text_and_implementation_brief_support_multiple_categories_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "Seat allocation must assign 100 seats and role license assignment maps owner roles.",
            "Overage handling should allow a 15% overage threshold before true-up billing.",
        ],
        definition_of_done=[
            "Billing proration records prorated charges within 14 days.",
            "Admin audit evidence logs seat events and notifications email billing admins.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    text_result = build_source_seat_management_requirements(
        """
# Seat management

- Seat allocation reserves paid seats.
- Seat limits enforce 10 seats.
- Deprovisioning releases seats when users are removed.
"""
    )
    implementation_result = generate_source_seat_management_requirements(implementation)

    assert implementation_payload == original
    assert [record.category for record in text_result.records] == [
        "seat_allocation",
        "seat_limits",
        "deprovisioning",
    ]
    assert text_result.records[0].source_field == "body"
    assert {
        "seat_allocation",
        "role_license_assignment",
        "overage_handling",
        "billing_proration",
        "admin_audit_evidence",
        "notifications",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-seats"
    assert implementation_result.title == "Seat management implementation"


def test_duplicate_evidence_merges_without_mutating_mapping():
    source = _source_brief(
        source_id="seat-dupes",
        source_payload={
            "seat_management": {
                "limits": "Seat limits must allow up to 25 seats before blocking new members.",
                "same_limits": "Seat limits must allow up to 25 seats before blocking new members.",
                "cap": "Seat cap is required for license management.",
            },
            "acceptance_criteria": [
                "Seat limits must allow up to 25 seats before blocking new members.",
                "Admin audit evidence must log seat events.",
            ],
        },
    )
    original = copy.deepcopy(source)

    result = build_source_seat_management_requirements(source)
    limits = next(record for record in result.records if record.category == "seat_limits")

    assert source == original
    assert limits.evidence == (
        "source_payload.seat_management.cap: Seat cap is required for license management.",
        "source_payload.seat_management.limits: Seat limits must allow up to 25 seats before blocking new members.",
    )
    assert limits.value == "25 seats"
    assert limits.confidence == "high"
    assert [record.category for record in result.records] == [
        "seat_allocation",
        "seat_limits",
        "admin_audit_evidence",
    ]


def test_serialization_markdown_aliases_and_sorting_are_stable():
    source = _source_brief(
        source_id="seat-model",
        title="Seat management source",
        summary="Seat management requirements include seat allocation and seat limits.",
        source_payload={
            "requirements": [
                "Notifications must send email | Slack limit warning alerts.",
                "Billing proration supports invoice adjustment credits.",
                "Role license assignment supports billing admin.",
            ]
        },
    )
    model = SourceBrief.model_validate(
        {
            key: value
            for key, value in source.items()
            if key not in {"description", "requirements", "acceptance_criteria", "billing"}
        }
    )

    result = build_source_seat_management_requirements(model)
    extracted = extract_source_seat_management_requirements(model)
    derived = derive_source_seat_management_requirements(model)
    payload = source_seat_management_requirements_to_dict(result)
    markdown = source_seat_management_requirements_to_markdown(result)

    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_seat_management_requirements(result) == result.summary
    assert source_seat_management_requirements_to_dicts(result) == payload["requirements"]
    assert source_seat_management_requirements_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload)) == payload
    assert result.records == result.requirements
    assert result.findings == result.requirements
    assert result.to_dicts() == payload["requirements"]
    assert list(payload) == ["brief_id", "title", "summary", "requirements", "records", "findings"]
    assert list(payload["requirements"][0]) == [
        "category",
        "source_field",
        "evidence",
        "confidence",
        "value",
        "suggested_owners",
        "suggested_plan_impacts",
    ]
    assert [record["category"] for record in payload["requirements"]] == [
        "seat_allocation",
        "role_license_assignment",
        "seat_limits",
        "billing_proration",
        "notifications",
    ]
    assert markdown.startswith("# Source Seat Management Requirements Report: seat-model")
    assert "| Category | Value | Confidence | Source Field | Owners | Evidence | Suggested Plan Impacts |" in markdown
    assert "email \\| Slack limit warning alerts" in markdown


def test_negated_scope_empty_invalid_mapping_and_object_inputs_are_stable():
    class BriefLike:
        id = "object-no-seats"
        summary = "No seat management or license management work is required for this release."

    object_result = build_source_seat_management_requirements(
        SimpleNamespace(
            id="object-seats",
            summary="Seat allocation must assign seats and invite acceptance consumes seats.",
            metadata={"billing": "Billing proration requires invoice adjustment credits."},
        )
    )
    negated = build_source_seat_management_requirements(BriefLike())
    no_seat_scope = build_source_seat_management_requirements(
        _source_brief(summary="Single-user only: no seat management changes are in scope.")
    )
    unrelated_auth = build_source_seat_management_requirements(
        _source_brief(
            title="Login copy",
            summary="Authentication page copy should explain remember me labels.",
            source_payload={"requirements": ["Show login button and profile menu."]},
        )
    )
    malformed = build_source_seat_management_requirements({"source_payload": {"notes": object()}})
    blank = build_source_seat_management_requirements("")
    invalid = build_source_seat_management_requirements(42)

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "seat_allocation": 0,
            "invite_acceptance": 0,
            "role_license_assignment": 0,
            "overage_handling": 0,
            "seat_limits": 0,
            "deprovisioning": 0,
            "reassignment": 0,
            "billing_proration": 0,
            "admin_audit_evidence": 0,
            "notifications": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "status": "no_seat_management_language",
    }
    assert [record.category for record in object_result.records] == [
        "seat_allocation",
        "invite_acceptance",
        "billing_proration",
    ]
    assert negated.records == ()
    assert no_seat_scope.records == ()
    assert unrelated_auth.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated_auth.summary == expected_summary
    assert unrelated_auth.to_dicts() == []
    assert "No source seat management requirements were inferred" in unrelated_auth.to_markdown()
    assert summarize_source_seat_management_requirements(unrelated_auth) == expected_summary


def _source_brief(
    *,
    source_id="source-seats",
    title="Seat management requirements",
    domain="billing",
    summary="General seat management requirements.",
    description=None,
    requirements=None,
    acceptance_criteria=None,
    billing=None,
    source_payload=None,
):
    return {
        "id": source_id,
        "title": title,
        "domain": domain,
        "summary": summary,
        "description": description,
        "requirements": [] if requirements is None else requirements,
        "acceptance_criteria": [] if acceptance_criteria is None else acceptance_criteria,
        "billing": {} if billing is None else billing,
        "source_project": "blueprint",
        "source_entity_type": "manual",
        "source_id": source_id,
        "source_payload": {} if source_payload is None else source_payload,
        "source_links": {},
        "created_at": None,
        "updated_at": None,
    }


def _implementation_brief(
    *,
    brief_id="implementation-seats",
    title="Seat management implementation",
    problem_statement="Implement source-backed seat and license management workflows.",
    mvp_goal="Ship seat management planning support.",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-seats",
        "title": title,
        "domain": "billing",
        "target_user": "workspace admin",
        "buyer": "operations",
        "workflow_context": "Seat and license administration",
        "problem_statement": problem_statement,
        "mvp_goal": mvp_goal,
        "product_surface": "billing",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run seat management extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
