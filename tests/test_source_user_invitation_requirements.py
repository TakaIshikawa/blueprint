import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_user_invitation_requirements import (
    SourceUserInvitationRequirement,
    SourceUserInvitationRequirementsReport,
    build_source_user_invitation_requirements,
    derive_source_user_invitation_requirements,
    extract_source_user_invitation_requirements,
    generate_source_user_invitation_requirements,
    source_user_invitation_requirements_to_dict,
    source_user_invitation_requirements_to_dicts,
    source_user_invitation_requirements_to_markdown,
    summarize_source_user_invitation_requirements,
)


def test_structured_source_payload_extracts_all_invitation_categories():
    result = build_source_user_invitation_requirements(
        _source_brief(
            source_payload={
                "invitation_flow": {
                    "delivery": "Invite email must send a magic link through the email channel.",
                    "expiration": "Invitation links expire after 7 days.",
                    "resend_cancel": "Admins can resend and revoke invitations before acceptance.",
                    "roles": "Invited users must be assigned a member role or admin role.",
                    "domains": "Only allowed domains on the company domain allowlist may be invited.",
                    "bulk": "CSV bulk invite supports 100 invitees per batch.",
                    "pending": "Pending users remain in invited status until acceptance.",
                    "audit": "Audit log records accepted invitation actor, IP address, and timestamp.",
                    "redirect": "After accepting the invite, redirect to the onboarding welcome flow.",
                }
            }
        )
    )

    assert isinstance(result, SourceUserInvitationRequirementsReport)
    assert result.source_id == "source-invitations"
    assert all(isinstance(record, SourceUserInvitationRequirement) for record in result.records)
    assert [record.requirement_category for record in result.records] == [
        "invitation_delivery",
        "invite_expiration",
        "resend_cancel",
        "role_assignment",
        "domain_restriction",
        "bulk_invite",
        "pending_user_state",
        "acceptance_audit",
        "onboarding_redirect",
    ]
    assert result.summary["requirement_count"] == 9
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["category_counts"] == {
        "invitation_delivery": 1,
        "invite_expiration": 1,
        "resend_cancel": 1,
        "role_assignment": 1,
        "domain_restriction": 1,
        "bulk_invite": 1,
        "pending_user_state": 1,
        "acceptance_audit": 1,
        "onboarding_redirect": 1,
    }
    assert any(
        "source_payload.invitation_flow.audit" in evidence
        for record in result.records
        for evidence in record.evidence
    )
    by_category = {record.requirement_category: record for record in result.records}
    assert by_category["invite_expiration"].value == "7 days"
    assert by_category["bulk_invite"].value == "100 invitees"


def test_implementation_brief_and_plain_text_inputs_are_supported():
    brief = ImplementationBrief.model_validate(
        _implementation_brief(
            scope=[
                "Invitation delivery must send email invites and show pending user state.",
                "Role assignment should default new invitees to viewer role.",
            ],
            definition_of_done=[
                "Accepted invitation audit event records actor and timestamp.",
                "Post-acceptance redirect routes users to onboarding.",
            ],
        )
    )
    text_result = build_source_user_invitation_requirements(
        """
# Invite Rollout

- Bulk invite via CSV import is required for workspace admins.
- Domain restriction must block invitees outside allowed domains.
- Invitation links expire after 48 hours.
"""
    )

    model_records = extract_source_user_invitation_requirements(brief)

    assert {record.requirement_category for record in model_records} == {
        "invitation_delivery",
        "role_assignment",
        "pending_user_state",
        "acceptance_audit",
        "onboarding_redirect",
    }
    assert [record.requirement_category for record in text_result.records] == [
        "invite_expiration",
        "domain_restriction",
        "bulk_invite",
    ]
    assert text_result.source_id is None
    assert any("body:" in evidence and "CSV import" in evidence for record in text_result.records for evidence in record.evidence)


def test_missing_detail_flags_are_reported_when_invitation_brief_is_under_specified():
    result = build_source_user_invitation_requirements(
        _source_brief(
            summary="Admins need to invite teammates into the workspace.",
            source_payload={
                "requirements": [
                    "Invitation delivery must send invites to new users.",
                    "Pending users should appear on the member list.",
                    "Resend invitation is supported when users miss the first invite.",
                ]
            },
        )
    )

    assert [record.requirement_category for record in result.records] == [
        "invitation_delivery",
        "resend_cancel",
        "pending_user_state",
    ]
    assert result.summary["missing_detail_flags"] == [
        "unspecified_expiration",
        "unspecified_role",
        "unspecified_email_channel_behavior",
        "unspecified_cancellation",
        "unspecified_audit_evidence",
    ]
    assert all(record.missing_detail_flags == tuple(result.summary["missing_detail_flags"]) for record in result.records)
    assert result.summary["missing_detail_counts"]["unspecified_expiration"] == 3


def test_duplicate_categories_merge_predictably_with_field_path_evidence():
    result = build_source_user_invitation_requirements(
        {
            "id": "dupe-invite",
            "source_payload": {
                "invites": {
                    "expiration": "Invitation links expire after 24 hours.",
                    "same_expiration": "Invitation links expire after 24 hours.",
                    "soft_expiration": "Invite expiry is part of onboarding requirements.",
                },
                "acceptance_criteria": [
                    "Invitation links expire after 24 hours.",
                    "Cancel invitation must revoke the invite link.",
                ],
            },
        }
    )

    assert [record.requirement_category for record in result.records] == [
        "invite_expiration",
        "resend_cancel",
    ]
    expiration = result.records[0]
    assert expiration.value == "24 hours"
    assert expiration.confidence == "high"
    assert expiration.evidence == (
        "source_payload.acceptance_criteria[0]: Invitation links expire after 24 hours.",
        "source_payload.invites.soft_expiration: Invite expiry is part of onboarding requirements.",
    )


def test_serialization_markdown_aliases_sorting_and_no_mutation():
    source = _source_brief(
        source_id="invitation-model",
        summary="Invitation requirements include role assignment and audit records.",
        source_payload={
            "requirements": [
                "Role assignment must invite users as member role.",
                "Audit log records accepted invitation evidence.",
                "Invite email copy contains old | new workspace labels.",
            ]
        },
    )
    original = copy.deepcopy(source)
    model = SourceBrief.model_validate(source)

    mapping_result = build_source_user_invitation_requirements(source)
    model_result = generate_source_user_invitation_requirements(model)
    derived = derive_source_user_invitation_requirements(model)
    payload = source_user_invitation_requirements_to_dict(model_result)
    markdown = source_user_invitation_requirements_to_markdown(model_result)

    assert source == original
    assert payload == source_user_invitation_requirements_to_dict(mapping_result)
    assert derived.to_dict() == model_result.to_dict()
    assert json.loads(json.dumps(payload)) == payload
    assert model_result.records == model_result.requirements
    assert model_result.findings == model_result.requirements
    assert model_result.to_dicts() == payload["requirements"]
    assert source_user_invitation_requirements_to_dicts(model_result) == payload["requirements"]
    assert source_user_invitation_requirements_to_dicts(model_result.records) == payload["records"]
    assert summarize_source_user_invitation_requirements(model_result) == model_result.summary
    assert list(payload) == ["source_id", "requirements", "findings", "summary", "records"]
    assert list(payload["requirements"][0]) == [
        "requirement_category",
        "value",
        "evidence",
        "missing_detail_flags",
        "confidence",
        "source_id",
    ]
    assert [(record.source_id, record.requirement_category) for record in model_result.records] == [
        ("invitation-model", "invitation_delivery"),
        ("invitation-model", "role_assignment"),
        ("invitation-model", "acceptance_audit"),
    ]
    assert markdown == model_result.to_markdown()
    assert "| Category | Value | Missing Details | Confidence | Source | Evidence |" in markdown
    assert "old \\| new workspace labels" in markdown


def test_object_negated_empty_invalid_and_malformed_inputs_return_stable_empty_reports():
    class BriefLike:
        id = "object-empty"
        summary = "No invitation, invite, role, domain restriction, or onboarding work is required for this copy update."

    object_result = build_source_user_invitation_requirements(
        SimpleNamespace(
            id="object-invite",
            summary="Bulk invite should import multiple invitees from CSV.",
            metadata={"domains": "Domain restriction must use allowed domains."},
        )
    )
    empty = build_source_user_invitation_requirements(
        _source_brief(source_id="empty-invite", summary="Update billing copy only.")
    )
    repeat = build_source_user_invitation_requirements(
        _source_brief(source_id="empty-invite", summary="Update billing copy only.")
    )
    negated = build_source_user_invitation_requirements(BriefLike())
    malformed = build_source_user_invitation_requirements({"source_payload": {"notes": object()}})
    invalid = build_source_user_invitation_requirements(42)

    expected_summary = {
        "source_count": 1,
        "requirement_count": 0,
        "category_counts": {
            "invitation_delivery": 0,
            "invite_expiration": 0,
            "resend_cancel": 0,
            "role_assignment": 0,
            "domain_restriction": 0,
            "bulk_invite": 0,
            "pending_user_state": 0,
            "acceptance_audit": 0,
            "onboarding_redirect": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "requirement_categories": [],
        "missing_detail_flags": [],
        "missing_detail_counts": {
            "unspecified_expiration": 0,
            "unspecified_role": 0,
            "unspecified_email_channel_behavior": 0,
            "unspecified_cancellation": 0,
            "unspecified_audit_evidence": 0,
        },
    }
    assert [record.requirement_category for record in object_result.records] == ["domain_restriction", "bulk_invite"]
    assert empty.to_dict() == repeat.to_dict()
    assert empty.source_id == "empty-invite"
    assert empty.records == ()
    assert empty.findings == ()
    assert empty.to_dicts() == []
    assert empty.summary == expected_summary
    assert "No user invitation requirements were found" in empty.to_markdown()
    assert negated.records == ()
    assert malformed.records == ()
    assert invalid.records == ()


def _source_brief(
    *,
    source_id="source-invitations",
    title="Invitation requirements",
    domain="identity",
    summary="General user invitation requirements.",
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


def _implementation_brief(*, scope=None, definition_of_done=None):
    return {
        "id": "impl-invitation",
        "source_brief_id": "source-invitations",
        "title": "User invitation onboarding",
        "domain": "identity",
        "target_user": "workspace admins",
        "buyer": None,
        "workflow_context": "Admins invite users and onboard new members.",
        "problem_statement": "Workspace admins need predictable invitation handling.",
        "mvp_goal": "Ship user invitation onboarding constraints.",
        "product_surface": "admin member management",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "risks": [],
        "validation_plan": "Run invitation onboarding validation.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
