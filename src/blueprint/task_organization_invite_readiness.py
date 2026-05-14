"""Assess readiness for organization invitation workflow tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskOrganizationInviteReadinessPlan = SimpleReadinessPlan
TaskOrganizationInviteReadinessRecord = SimpleReadinessRecord
TaskOrganizationInviteReadinessFinding = SimpleReadinessRecord
TaskOrganizationInviteReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "invite_lifecycle": re.compile(r"\b(?:organization invitations?|org invitations?|invite creation|invitation lifecycle|pending invite|accept invitations?)\b", re.I),
    "invite_resend_expiration": re.compile(r"\b(?:resend invitations?|invite resend|invitation expiration|expired invitations?|invite expiry|expiration)\b", re.I),
    "invite_membership_assignment": re.compile(r"\b(?:seat assignment|assign seats?|role assignment|assign roles?|org members?|organization members?)\b", re.I),
    "invite_restrictions": re.compile(r"\b(?:domain restrictions?|allowed domains?|email domain allowlist|restricted invites?)\b", re.I),
    "invite_email": re.compile(r"\b(?:invite email|invitation email|notification path|email notification|send invitation)\b", re.I),
}
_PATH_SIGNALS = {
    "invite_lifecycle": re.compile(r"(?:invites?|invitations?)", re.I),
    "invite_membership_assignment": re.compile(r"(?:org[_-]?members?|seats?|roles?)", re.I),
    "invite_email": re.compile(r"(?:invitation[_-]?email|invite[_-]?email|notifications?)", re.I),
    "invite_restrictions": re.compile(r"(?:domain[_-]?restrictions?|allowed[_-]?domains?)", re.I),
}
_CRITERIA = {
    "lifecycle_states": re.compile(r"\b(?:lifecycle states?|pending|accepted|declined|expired|revoked|cancelled|canceled|state machine)\b", re.I),
    "authorization_rules": re.compile(r"\b(?:authorization|authorize|permission|admin only|owner only|rbac|access control|policy)\b", re.I),
    "expiration_resend_behavior": re.compile(r"\b(?:expiration|expires?|expiry|ttl|resend|re-send|rate limit|cooldown)\b", re.I),
    "role_or_seat_assignment": re.compile(r"\b(?:role assignment|assign roles?|seat assignment|assign seats?|license seat|member role|default role)\b", re.I),
    "email_notification_path": re.compile(r"\b(?:invite email|invitation email|email notification|notification path|mailer|template|deliverability)\b", re.I),
    "auditability": re.compile(r"\b(?:audit|audit trail|event log|activity log|invitation log|who invited|traceability)\b", re.I),
    "acceptance_tests": re.compile(r"\b(?:acceptance tests?|integration tests?|workflow tests?|unit tests?|pytest|e2e|end-to-end)\b", re.I),
}
_GUIDANCE = {
    "lifecycle_states": "Define invitation lifecycle states such as pending, accepted, declined, expired, revoked, or cancelled.",
    "authorization_rules": "Document authorization rules for who can create, resend, expire, accept, or restrict invitations.",
    "expiration_resend_behavior": "Specify expiration, TTL, resend, rate-limit, or cooldown behavior.",
    "role_or_seat_assignment": "Describe role or seat assignment, default role, license seat, or member role handling.",
    "email_notification_path": "Document the invite email, invitation email, notification path, mailer, or template.",
    "auditability": "Add audit trail, activity log, event log, inviter attribution, or traceability requirements.",
    "acceptance_tests": "Add acceptance, workflow, integration, unit, pytest, or end-to-end tests for invitation flows.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:organization invitations?|org invitations?|invites?|invitation emails?)\b.{0,80}\b(?:impact|changes?|planned|scope|required|needed)\b",
    re.I,
)


def build_task_organization_invite_readiness_plan(source: Any) -> TaskOrganizationInviteReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Organization Invite Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_organization_invite_readiness = build_task_organization_invite_readiness_plan
extract_task_organization_invite_readiness = build_task_organization_invite_readiness_plan
generate_task_organization_invite_readiness = build_task_organization_invite_readiness_plan
derive_task_organization_invite_readiness = build_task_organization_invite_readiness_plan
summarize_task_organization_invite_readiness = build_task_organization_invite_readiness_plan
summarize_task_organization_invite_readiness_plan = build_task_organization_invite_readiness_plan


def recommend_task_organization_invite_readiness(source: Any) -> tuple[TaskOrganizationInviteReadinessRecord, ...]:
    return build_task_organization_invite_readiness_plan(source).records


def task_organization_invite_readiness_plan_to_dict(result: TaskOrganizationInviteReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_organization_invite_readiness_plan_to_dict.__test__ = False


def task_organization_invite_readiness_plan_to_dicts(
    result: TaskOrganizationInviteReadinessPlan | Iterable[TaskOrganizationInviteReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_organization_invite_readiness_plan_to_dicts.__test__ = False
task_organization_invite_readiness_to_dicts = task_organization_invite_readiness_plan_to_dicts
task_organization_invite_readiness_to_dicts.__test__ = False


def task_organization_invite_readiness_plan_to_markdown(result: TaskOrganizationInviteReadinessPlan) -> str:
    return result.to_markdown()


task_organization_invite_readiness_plan_to_markdown.__test__ = False
