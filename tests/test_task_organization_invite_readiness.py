import copy
import json

from blueprint.domain.models import ExecutionPlan
from blueprint.task_organization_invite_readiness import (
    build_task_organization_invite_readiness_plan,
    summarize_task_organization_invite_readiness,
    task_organization_invite_readiness_plan_to_dict,
    task_organization_invite_readiness_plan_to_dicts,
    task_organization_invite_readiness_plan_to_markdown,
)


def test_complete_organization_invite_task_is_ready():
    result = build_task_organization_invite_readiness_plan(
        _plan(
            [
                _task(
                    "invite-ready",
                    title="Build organization invitation lifecycle",
                    description="Create organization invitations with resend, expiration, acceptance, role assignment, and domain restrictions.",
                    acceptance_criteria=[
                        "Lifecycle states include pending, accepted, declined, expired, and revoked.",
                        "Authorization rules require owner or admin permission for create, resend, and revoke.",
                        "Expiration TTL and resend cooldown behavior are specified.",
                        "Role assignment and license seat assignment are applied on acceptance.",
                        "Invite email notification path uses the invitation email template.",
                        "Audit trail records who invited, accepted, resent, and revoked invitations.",
                        "Acceptance tests and workflow tests cover invitation flows.",
                    ],
                    files_or_modules=["src/org/invitations.py"],
                )
            ]
        )
    )

    record = result.records[0]
    assert result.impacted_task_ids == ("invite-ready",)
    assert record.readiness == "ready"
    assert record.missing_criteria == ()
    assert record.present_criteria == (
        "lifecycle_states",
        "authorization_rules",
        "expiration_resend_behavior",
        "role_or_seat_assignment",
        "email_notification_path",
        "auditability",
        "acceptance_tests",
    )


def test_partial_invite_task_reports_ordered_gaps_and_actions():
    result = build_task_organization_invite_readiness_plan(
        [_task("invite-partial", title="Resend org invitations", description="Add resend invitations for pending users.")]
    )

    record = result.records[0]
    assert record.readiness == "partial"
    assert record.present_criteria == ("lifecycle_states", "expiration_resend_behavior")
    assert record.missing_criteria == (
        "authorization_rules",
        "role_or_seat_assignment",
        "email_notification_path",
        "auditability",
        "acceptance_tests",
    )
    assert record.recommended_follow_up_actions == (
        "Document authorization rules for who can create, resend, expire, accept, or restrict invitations.",
        "Describe role or seat assignment, default role, license seat, or member role handling.",
        "Document the invite email, invitation email, notification path, mailer, or template.",
        "Add audit trail, activity log, event log, inviter attribution, or traceability requirements.",
        "Add acceptance, workflow, integration, unit, pytest, or end-to-end tests for invitation flows.",
    )


def test_invite_path_hints_nested_metadata_no_impact_and_conversion_are_stable():
    source = _plan(
        [
            _task(
                "invite-paths",
                title="Invite acceptance",
                description="Implement accept invitation.",
                files_or_modules=["app/invites/accept.py", "app/org_members/seats.py", "emails/invitation_email.html"],
                metadata={"security": {"authorization": "Admin permission policy required.", "audit": "Activity log records inviter."}},
            ),
            _task("invite-noop", title="Docs", description="No organization invitations are required for this copy change."),
        ],
        plan_id="plan-invites",
    )
    original = copy.deepcopy(source)

    result = summarize_task_organization_invite_readiness(ExecutionPlan.model_validate(source))
    payload = task_organization_invite_readiness_plan_to_dict(result)

    assert source == original
    assert result.impacted_task_ids == ("invite-paths",)
    assert result.ignored_task_ids == ("invite-noop",)
    record = result.records[0]
    assert record.detected_signals == ("invite_lifecycle", "invite_membership_assignment", "invite_email")
    assert record.present_criteria == (
        "lifecycle_states",
        "authorization_rules",
        "email_notification_path",
        "auditability",
    )
    assert any("metadata.security.authorization" in item for item in record.evidence)
    assert any("files_or_modules: app/org_members/seats.py" in item for item in record.evidence)
    assert task_organization_invite_readiness_plan_to_dicts(result.records) == payload["records"]
    assert json.loads(json.dumps(payload, sort_keys=True))["plan_id"] == "plan-invites"
    assert task_organization_invite_readiness_plan_to_markdown(result).startswith("# Task Organization Invite Readiness: plan-invites")


def _plan(tasks, *, plan_id="plan-invites"):
    return {"id": plan_id, "implementation_brief_id": "brief-invites", "milestones": [], "tasks": tasks}


def _task(task_id, *, title=None, description=None, acceptance_criteria=None, files_or_modules=None, metadata=None):
    task = {"id": task_id, "title": title or task_id, "description": description or "", "acceptance_criteria": acceptance_criteria or []}
    if files_or_modules is not None:
        task["files_or_modules"] = files_or_modules
    if metadata is not None:
        task["metadata"] = metadata
    return task
