import copy
import json
from types import SimpleNamespace

from blueprint.domain.models import ImplementationBrief, SourceBrief
from blueprint.source_organization_invite_requirements import (
    SourceOrganizationInviteRequirement,
    SourceOrganizationInviteRequirementsReport,
    build_source_organization_invite_requirements,
    derive_source_organization_invite_requirements,
    extract_source_organization_invite_requirements,
    generate_source_organization_invite_requirements,
    source_organization_invite_requirements_to_dict,
    source_organization_invite_requirements_to_dicts,
    source_organization_invite_requirements_to_markdown,
    summarize_source_organization_invite_requirements,
)


def test_nested_source_payload_extracts_organization_invite_categories_in_order():
    result = build_source_organization_invite_requirements(
        _source_brief(
            source_payload={
                "organization_invites": {
                    "delivery": "Invitation email must send organization invites to admins and invitees.",
                    "magic": "Magic link invite tokens must be single-use and bound to the workspace.",
                    "expiry": "Invite expiry requires links to expire after 7 days.",
                    "resend_revoke": "Workspace admins can resend and revoke pending invitations.",
                    "pending": "Pending invite state must show invited members before acceptance.",
                    "roles_seats": "Role and seat assignment must assign member role and check available seats.",
                    "domain_sso": "Domain restrictions must enforce allowed domains and SSO-only organizations.",
                    "notifications": "Notifications send email | in-app reminders for accepted and declined invites.",
                    "audit": "Audit trail records invitation events, actor, invitee, IP address, and timestamp.",
                    "states": "Accepted and declined states are tracked after invitees accept or decline.",
                }
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert isinstance(result, SourceOrganizationInviteRequirementsReport)
    assert all(isinstance(record, SourceOrganizationInviteRequirement) for record in result.records)
    assert [record.category for record in result.records] == [
        "invitation_delivery",
        "magic_link",
        "invite_expiry",
        "resend_revoke",
        "pending_invite_state",
        "role_seat_assignment",
        "domain_sso_restriction",
        "notifications",
        "audit_trail",
        "accepted_declined_state",
    ]
    assert by_category["invite_expiry"].value == "7 days"
    assert by_category["magic_link"].value == "magic link"
    assert by_category["role_seat_assignment"].value == "member"
    assert by_category["domain_sso_restriction"].value == "allowed domains"
    assert by_category["audit_trail"].source_field == "source_payload.organization_invites.audit"
    assert by_category["role_seat_assignment"].suggested_owners == ("authorization", "billing")
    assert by_category["audit_trail"].planning_notes[0].startswith("Record actor")
    assert result.summary["requirement_count"] == 10
    assert result.summary["missing_detail_flags"] == []
    assert result.summary["status"] == "ready_for_planning"


def test_top_level_fields_and_structured_source_payload_are_scanned():
    result = build_source_organization_invite_requirements(
        _source_brief(
            title="Workspace organization invite launch",
            summary="Organization invite requirements include invitation delivery and magic link handling.",
            requirements=[
                "Invite expiry must expire after 48 hours before invitees can join.",
                "Domain SSO restriction should block non-allowed domains for SSO-only organizations.",
            ],
            acceptance_criteria=[
                "Resend and revoke invite actions are available to workspace admins.",
                "Pending invite state appears in the member list until acceptance.",
            ],
            security={
                "audit": "Audit log records invitation events, actor, target invitee, and timestamp.",
            },
            source_payload={
                "metadata": {
                    "notifications": "Notifications must send Slack and email invite reminders.",
                    "roles": "Role seat assignment checks seat availability for member role.",
                    "states": "Accepted and declined invitation states are visible to admins.",
                },
                "updated_by": "invite text in bookkeeping should not create evidence",
            },
        )
    )

    by_category = {record.category: record for record in result.records}

    assert {
        "invitation_delivery",
        "magic_link",
        "invite_expiry",
        "resend_revoke",
        "pending_invite_state",
        "role_seat_assignment",
        "domain_sso_restriction",
        "notifications",
        "audit_trail",
        "accepted_declined_state",
    } <= set(by_category)
    assert by_category["invite_expiry"].source_field == "requirements[0]"
    assert by_category["resend_revoke"].source_field == "acceptance_criteria[0]"
    assert by_category["notifications"].source_field == "source_payload.metadata.notifications"
    assert all("updated_by" not in item for record in result.records for item in record.evidence)


def test_plain_text_and_implementation_brief_support_multiple_categories_without_mutation():
    implementation_payload = _implementation_brief(
        scope=[
            "Invitation delivery must send workspace invite emails and pending invite state is tracked.",
            "Role assignment should set viewer role and seat availability must be checked.",
        ],
        definition_of_done=[
            "Domain restrictions enforce SSO-only organizations and allowed domains.",
            "Audit trail logs invite events and notifications email organization admins.",
        ],
    )
    original = copy.deepcopy(implementation_payload)
    implementation = ImplementationBrief.model_validate(implementation_payload)
    text_result = build_source_organization_invite_requirements(
        """
# Organization invites

- Magic link invite tokens are required for workspace invitations.
- Invite expiry expires after 24 hours.
- Accepted and declined invitation states are shown to admins.
"""
    )
    implementation_result = generate_source_organization_invite_requirements(implementation)

    assert implementation_payload == original
    assert [record.category for record in text_result.records] == [
        "magic_link",
        "invite_expiry",
        "accepted_declined_state",
    ]
    assert text_result.records[0].source_field == "body"
    assert {
        "invitation_delivery",
        "pending_invite_state",
        "role_seat_assignment",
        "domain_sso_restriction",
        "notifications",
        "audit_trail",
    } <= {record.category for record in implementation_result.records}
    assert implementation_result.brief_id == "implementation-org-invites"
    assert implementation_result.title == "Organization invite implementation"


def test_missing_detail_gap_messages_are_reported_for_under_specified_invite_flow():
    result = build_source_organization_invite_requirements(
        _source_brief(
            summary="Admins need to invite teammates into the organization.",
            source_payload={
                "requirements": [
                    "Invitation delivery must send invites to new workspace members.",
                    "Pending invite state should appear on the member list.",
                    "Accepted and declined invitation states are tracked.",
                ]
            },
        )
    )

    assert [record.category for record in result.records] == [
        "invitation_delivery",
        "pending_invite_state",
        "accepted_declined_state",
    ]
    assert result.summary["missing_detail_flags"] == [
        "missing_expiry",
        "missing_resend_or_revoke",
        "missing_authorization",
        "missing_seat_handling",
        "missing_audit_details",
    ]
    assert "Specify invitation expiry or token lifetime." in result.summary["gap_messages"]
    assert all(record.gap_messages == tuple(result.summary["gap_messages"]) for record in result.records)
    assert result.summary["missing_detail_counts"]["missing_expiry"] == 3
    assert result.summary["status"] == "needs_invite_details"


def test_duplicate_evidence_merges_without_mutating_mapping():
    source = _source_brief(
        source_id="org-invite-dupes",
        source_payload={
            "organization_invites": {
                "expiry": "Invite expiry must expire after 72 hours.",
                "same_expiry": "Invite expiry must expire after 72 hours.",
                "ttl": "Invitation token lifetime is required for organization invites.",
            },
            "acceptance_criteria": [
                "Invite expiry must expire after 72 hours.",
                "Audit trail must log invitation events.",
            ],
        },
    )
    original = copy.deepcopy(source)

    result = build_source_organization_invite_requirements(source)
    expiry = next(record for record in result.records if record.category == "invite_expiry")

    assert source == original
    assert expiry.evidence == (
        "source_payload.organization_invites.expiry: Invite expiry must expire after 72 hours.",
        "source_payload.organization_invites.ttl: Invitation token lifetime is required for organization invites.",
    )
    assert expiry.value == "72 hours"
    assert expiry.confidence == "high"
    assert [record.category for record in result.records] == [
        "magic_link",
        "invite_expiry",
        "audit_trail",
    ]


def test_serialization_markdown_aliases_and_sorting_are_stable():
    source = _source_brief(
        source_id="org-invite-model",
        title="Organization invite source",
        summary="Organization invite requirements include invitation delivery and magic link invites.",
        source_payload={
            "requirements": [
                "Notifications must send email | in-app invite reminders.",
                "Role seat assignment supports billing admin role and available seats.",
                "Domain restriction requires SSO-only organizations.",
            ]
        },
    )
    model = SourceBrief.model_validate(
        {
            key: value
            for key, value in source.items()
            if key not in {"description", "requirements", "acceptance_criteria", "security"}
        }
    )

    result = build_source_organization_invite_requirements(model)
    extracted = extract_source_organization_invite_requirements(model)
    derived = derive_source_organization_invite_requirements(model)
    payload = source_organization_invite_requirements_to_dict(result)
    markdown = source_organization_invite_requirements_to_markdown(result)

    assert extracted == result.requirements
    assert derived.to_dict() == result.to_dict()
    assert summarize_source_organization_invite_requirements(result) == result.summary
    assert source_organization_invite_requirements_to_dicts(result) == payload["requirements"]
    assert source_organization_invite_requirements_to_dicts(result.records) == payload["records"]
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
        "planning_notes",
        "gap_messages",
    ]
    assert [record["category"] for record in payload["requirements"]] == [
        "invitation_delivery",
        "magic_link",
        "role_seat_assignment",
        "domain_sso_restriction",
        "notifications",
    ]
    assert result.records[0].requirement_category == result.records[0].category
    assert result.records[0].concern == result.records[0].category
    assert result.records[0].suggested_plan_impacts == result.records[0].planning_notes
    assert markdown.startswith("# Source Organization Invite Requirements Report: org-invite-model")
    assert "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes | Gap Messages |" in markdown
    assert "email \\| in-app invite reminders" in markdown


def test_negated_scope_empty_invalid_mapping_and_object_inputs_are_stable():
    class BriefLike:
        id = "object-no-invites"
        summary = "No organization invite or workspace invitation work is required for this release."

    object_result = build_source_organization_invite_requirements(
        SimpleNamespace(
            id="object-org-invites",
            summary="Organization invite delivery must send email and magic link invites.",
            metadata={"domain": "Domain SSO restriction requires allowed domains."},
        )
    )
    negated = build_source_organization_invite_requirements(BriefLike())
    no_scope = build_source_organization_invite_requirements(
        _source_brief(summary="Organization invitations are out of scope and no member invite flow is planned.")
    )
    unrelated_auth = build_source_organization_invite_requirements(
        _source_brief(
            title="Login copy",
            summary="Authentication page copy should explain remember me labels.",
            source_payload={"requirements": ["Show login button and profile menu."]},
        )
    )
    malformed = build_source_organization_invite_requirements({"source_payload": {"notes": object()}})
    blank = build_source_organization_invite_requirements("")
    invalid = build_source_organization_invite_requirements(42)

    expected_summary = {
        "requirement_count": 0,
        "categories": [],
        "category_counts": {
            "invitation_delivery": 0,
            "magic_link": 0,
            "invite_expiry": 0,
            "resend_revoke": 0,
            "pending_invite_state": 0,
            "role_seat_assignment": 0,
            "domain_sso_restriction": 0,
            "notifications": 0,
            "audit_trail": 0,
            "accepted_declined_state": 0,
        },
        "confidence_counts": {"high": 0, "medium": 0, "low": 0},
        "missing_detail_flags": [],
        "missing_detail_counts": {
            "missing_expiry": 0,
            "missing_resend_or_revoke": 0,
            "missing_authorization": 0,
            "missing_seat_handling": 0,
            "missing_audit_details": 0,
        },
        "gap_messages": [],
        "status": "no_organization_invite_language",
    }
    assert [record.category for record in object_result.records] == [
        "invitation_delivery",
        "magic_link",
        "domain_sso_restriction",
        "notifications",
    ]
    assert negated.records == ()
    assert no_scope.records == ()
    assert unrelated_auth.records == ()
    assert malformed.records == ()
    assert blank.records == ()
    assert invalid.records == ()
    assert unrelated_auth.summary == expected_summary
    assert unrelated_auth.to_dicts() == []
    assert "No source organization invite requirements were inferred" in unrelated_auth.to_markdown()
    assert summarize_source_organization_invite_requirements(unrelated_auth) == expected_summary


def _source_brief(
    *,
    source_id="source-org-invites",
    title="Organization invite requirements",
    domain="identity",
    summary="General organization invite requirements.",
    description=None,
    requirements=None,
    acceptance_criteria=None,
    security=None,
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
        "security": {} if security is None else security,
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
    brief_id="implementation-org-invites",
    title="Organization invite implementation",
    problem_statement="Implement source-backed organization invitation workflows.",
    mvp_goal="Ship organization invite planning support.",
    scope=None,
    definition_of_done=None,
):
    return {
        "id": brief_id,
        "source_brief_id": "source-org-invites",
        "title": title,
        "domain": "identity",
        "target_user": "workspace admin",
        "buyer": "operations",
        "workflow_context": "Organization member invitation administration",
        "problem_statement": problem_statement,
        "mvp_goal": mvp_goal,
        "product_surface": "admin member management",
        "scope": [] if scope is None else scope,
        "non_goals": [],
        "risks": [],
        "assumptions": [],
        "architecture_notes": None,
        "data_requirements": None,
        "integration_points": [],
        "validation_plan": "Run organization invite extractor tests.",
        "definition_of_done": [] if definition_of_done is None else definition_of_done,
        "status": "draft",
        "created_at": None,
        "updated_at": None,
        "generation_model": None,
        "generation_tokens": None,
        "generation_prompt": None,
    }
