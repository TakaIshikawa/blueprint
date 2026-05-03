"""Extract source-level organization invitation requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


OrganizationInviteCategory = Literal[
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
OrganizationInviteMissingDetail = Literal[
    "missing_expiry",
    "missing_resend_or_revoke",
    "missing_authorization",
    "missing_seat_handling",
    "missing_audit_details",
]
OrganizationInviteConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[OrganizationInviteCategory, ...] = (
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
)
_MISSING_DETAIL_ORDER: tuple[OrganizationInviteMissingDetail, ...] = (
    "missing_expiry",
    "missing_resend_or_revoke",
    "missing_authorization",
    "missing_seat_handling",
    "missing_audit_details",
)
_CONFIDENCE_ORDER: dict[OrganizationInviteConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_INVITE_CONTEXT_RE = re.compile(
    r"\b(?:organization invite|organisation invite|workspace invite|team invite|member invite|"
    r"invitation|invite|invites|invitee|invited|inviting|pending invite|pending invitation|"
    r"accepted invite|declined invite|organization member|workspace member|team member|"
    r"magic link|invite link|invitation token|seat availability|seat assignment|role assignment|"
    r"domain restriction|allowed domain|domain allowlist|sso[- ]?only|single sign[- ]?on|"
    r"audit trail|audit log|invitation event|notification|email)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:organi[sz]ation|workspace|team|member|invitation|invite|invitee|magic|token|link|"
    r"expiry|expiration|ttl|resend|revoke|cancel|pending|accepted|declined|role|seat|"
    r"domain|sso|notification|email|audit|requirements?|acceptance|criteria|"
    r"definition[-_ ]?of[-_ ]?done|metadata|source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|send|email|invite|assign|check|validate|enforce|block|"
    r"expire|resend|revoke|cancel|record|track|audit|log|notify|acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:organi[sz]ation invites?|workspace invites?|team invites?|member invites?|"
    r"invitation flow|magic links?|invite expiry|resend|revoke|seat handling|sso invites?|invite audit)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:organi[sz]ation invites?|workspace invites?|team invites?|member invites?|"
    r"invitation flow|magic links?|invite expiry|resend|revoke|seat handling|sso invites?|invite audit)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_INVITES_RE = re.compile(
    r"\b(?:no organi[sz]ation invite|organization invitations? are out of scope|"
    r"workspace invitations? are out of scope|team invitations? are out of scope|"
    r"no member invite flow|invitation flows? are excluded)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:minutes?|hours?|days?|weeks?|months?)|"
    r"\d+\s*(?:seats?|users?|members?|invitees?|invites?)|"
    r"(?:email|magic link|invite link|invitation token|pending|accepted|declined|expired|revoked|"
    r"admin|owner|member|viewer|guest|billing admin|seat availability|available seats?|"
    r"allowed domains?|domain allowlist|same domain|sso[- ]?only|single sign[- ]?on|audit log))\b",
    re.I,
)
_DURATION_RE = re.compile(r"\b\d+\s*(?:minutes?|hours?|days?|weeks?|months?)\b", re.I)
_COUNT_RE = re.compile(r"\b\d+\s*(?:seats?|users?|members?|invitees?|invites?)\b", re.I)
_EXPIRY_DETAIL_RE = re.compile(r"\b(?:expir|ttl|time[- ]?to[- ]?live|valid for|\d+\s*(?:hours?|days?|weeks?))\b", re.I)
_RESEND_REVOKE_DETAIL_RE = re.compile(r"\b(?:resend|re-send|reinvite|re-invite|revoke|cancel|withdraw|void)\b", re.I)
_AUTHORIZATION_DETAIL_RE = re.compile(
    r"\b(?:admin|owner|authorized|permission|role|rbac|can invite|invite permission|manage members)\b",
    re.I,
)
_SEAT_DETAIL_RE = re.compile(r"\b(?:seat|seats|seat availability|seat limit|seat cap|license|licence)\b", re.I)
_AUDIT_DETAIL_RE = re.compile(r"\b(?:audit|log|event|record actor|actor|timestamp|ip address|accepted by|declined by)\b", re.I)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
    "id",
    "source_id",
    "source_brief_id",
    "status",
    "created_by",
    "updated_by",
    "owner",
    "last_editor",
}
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "mvp_goal",
    "context",
    "workflow_context",
    "requirements",
    "constraints",
    "scope",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "non_goals",
    "assumptions",
    "authentication",
    "auth_requirements",
    "security",
    "organization_invites",
    "organisation_invites",
    "workspace_invites",
    "team_invites",
    "member_invites",
    "invitations",
    "invites",
    "seat_management",
    "sso",
    "notifications",
    "audit",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[OrganizationInviteCategory, re.Pattern[str]] = {
    "invitation_delivery": re.compile(
        r"\b(?:invitation delivery|invite delivery|send(?:s|ing)? invitations?|send(?:s|ing)? invites?|"
        r"invitation emails?|invite emails?|email invitations?|deliver(?:y|ed)? invitations?|"
        r"deliver(?:y|ed)? invites?)\b",
        re.I,
    ),
    "magic_link": re.compile(
        r"\b(?:magic links?|invite links?|invitation links?|join links?|invitation tokens?|invite tokens?|"
        r"tokenized invite|single[- ]use link|signed invite)\b",
        re.I,
    ),
    "invite_expiry": re.compile(
        r"\b(?:invite expir(?:y|ation|es?)|invitation expir(?:y|ation|es?)|expires?|expired|"
        r"expiration window|valid for|ttl|time[- ]?to[- ]?live|token lifetime|link lifetime)\b",
        re.I,
    ),
    "resend_revoke": re.compile(
        r"\b(?:resend|re-send|reinvite|re-invite|send again|revoke invitations?|revoke invites?|"
        r"cancel invitations?|cancel invites?|withdraw invitations?|void invitations?)\b",
        re.I,
    ),
    "pending_invite_state": re.compile(
        r"\b(?:pending invites?|pending invitations?|pending invitees?|pending members?|invited members?|"
        r"invited users?|invite status|invitation status|unaccepted invite|pre[- ]acceptance)\b",
        re.I,
    ),
    "role_seat_assignment": re.compile(
        r"\b(?:role assignment|assign(?:ed|s)? roles?|default role|invited as|permission set|"
        r"workspace role|team role|organi[sz]ation role|admin role|owner role|member role|"
        r"seat availability|available seats?|seat assignment|seat handling|seat limit|seat cap|license assignment)\b",
        re.I,
    ),
    "domain_sso_restriction": re.compile(
        r"\b(?:domain restriction|allowed domains?|allowlisted domains?|domain allowlist|email domain|"
        r"same domain|company domain|approved domain|blocked domains?|sso[- ]?only|single sign[- ]?on|"
        r"sso domain|saml|identity provider|idp)\b",
        re.I,
    ),
    "notifications": re.compile(
        r"\b(?:notifications?|notify|email|in[- ]?app|slack|webhook|invite reminder|"
        r"accepted notification|declined notification|send.{0,50}(?:notice|alert|email))\b",
        re.I,
    ),
    "audit_trail": re.compile(
        r"\b(?:audit trail|audit log|audited|logged|logging|invitation events?|invite events?|"
        r"accepted invitation event|declined invitation event|record actor|actor and timestamp|"
        r"ip address|audit evidence)\b",
        re.I,
    ),
    "accepted_declined_state": re.compile(
        r"\b(?:accepted invites?|accepted invitations?|declined invites?|declined invitations?|"
        r"accepted state|declined state|accept(?:ed|ance)? status|decline(?:d)? status|"
        r"accept or decline|accepted\/declined)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[OrganizationInviteCategory, tuple[str, ...]] = {
    "invitation_delivery": ("lifecycle_messaging", "identity"),
    "magic_link": ("identity", "security"),
    "invite_expiry": ("identity", "security"),
    "resend_revoke": ("identity", "admin_experience"),
    "pending_invite_state": ("identity", "admin_experience"),
    "role_seat_assignment": ("authorization", "billing"),
    "domain_sso_restriction": ("identity", "security"),
    "notifications": ("lifecycle_messaging", "admin_experience"),
    "audit_trail": ("security", "compliance"),
    "accepted_declined_state": ("identity", "admin_experience"),
}
_PLANNING_NOTES: dict[OrganizationInviteCategory, tuple[str, ...]] = {
    "invitation_delivery": ("Define invite email content, delivery channel, bounce handling, and sender identity.",),
    "magic_link": ("Specify token lifetime, single-use behavior, replay protection, and target organization binding.",),
    "invite_expiry": ("Set invite expiry windows and expired-link recovery behavior for admins and invitees.",),
    "resend_revoke": ("Describe resend, revoke, cancel, and idempotency behavior for pending invitations.",),
    "pending_invite_state": ("Model pending invitations separately from accepted members in admin lists and APIs.",),
    "role_seat_assignment": ("Map invited roles, permission sets, and whether pending or accepted invites consume seats.",),
    "domain_sso_restriction": ("Enforce allowed domains, SSO-only organizations, and identity-provider restrictions before acceptance.",),
    "notifications": ("Notify invitees and admins about sent, resent, expired, revoked, accepted, and declined states.",),
    "audit_trail": ("Record actor, invitee, organization, role, seat impact, IP, timestamp, and state transitions.",),
    "accepted_declined_state": ("Define accepted and declined terminal states and whether admins can re-invite afterward.",),
}
_GAP_MESSAGES: dict[OrganizationInviteMissingDetail, str] = {
    "missing_expiry": "Specify invitation expiry or token lifetime.",
    "missing_resend_or_revoke": "Specify resend and revoke or cancel behavior for pending invitations.",
    "missing_authorization": "Specify which admins, owners, roles, or permissions can send and manage invitations.",
    "missing_seat_handling": "Specify whether pending or accepted invitations reserve, consume, or require available seats.",
    "missing_audit_details": "Specify audit details such as actor, invitee, organization, timestamp, IP, and state transition.",
}


@dataclass(frozen=True, slots=True)
class SourceOrganizationInviteRequirement:
    """One source-backed organization invitation requirement."""

    category: OrganizationInviteCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: OrganizationInviteConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> OrganizationInviteCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> OrganizationInviteCategory:
        """Compatibility view for extractors that expose concern naming."""
        return self.category

    @property
    def suggested_plan_impacts(self) -> tuple[str, ...]:
        """Compatibility view matching adjacent source extractors."""
        return self.planning_notes

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owners": list(self.suggested_owners),
            "planning_notes": list(self.planning_notes),
            "gap_messages": list(self.gap_messages),
        }


@dataclass(frozen=True, slots=True)
class SourceOrganizationInviteRequirementsReport:
    """Source-level organization invitation requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceOrganizationInviteRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceOrganizationInviteRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceOrganizationInviteRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return organization invitation requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Organization Invite Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Missing detail flags: " + ", ".join(self.summary.get("missing_detail_flags", [])),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source organization invite requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes | Gap Messages |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell(', '.join(requirement.suggested_owners))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{_markdown_cell('; '.join(requirement.gap_messages))} |"
            )
        return "\n".join(lines)


def build_source_organization_invite_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceOrganizationInviteRequirementsReport:
    """Build an organization invitation requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    gap_flags = tuple(_missing_detail_flags(candidate.evidence for candidate in candidates))
    requirements = tuple(_merge_candidates(candidates, gap_flags))
    return SourceOrganizationInviteRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements, gap_flags if requirements else ()),
    )


def summarize_source_organization_invite_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceOrganizationInviteRequirementsReport
        | str
        | object
    ),
) -> SourceOrganizationInviteRequirementsReport | dict[str, Any]:
    """Compatibility helper for callers that use summarize_* naming."""
    if isinstance(source, SourceOrganizationInviteRequirementsReport):
        return dict(source.summary)
    return build_source_organization_invite_requirements(source)


def derive_source_organization_invite_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceOrganizationInviteRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_organization_invite_requirements(source)


def generate_source_organization_invite_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceOrganizationInviteRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_organization_invite_requirements(source)


def extract_source_organization_invite_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceOrganizationInviteRequirement, ...]:
    """Return organization invitation requirement records from brief-shaped input."""
    return build_source_organization_invite_requirements(source).requirements


def source_organization_invite_requirements_to_dict(
    report: SourceOrganizationInviteRequirementsReport,
) -> dict[str, Any]:
    """Serialize an organization invitation requirements report to a plain dictionary."""
    return report.to_dict()


source_organization_invite_requirements_to_dict.__test__ = False


def source_organization_invite_requirements_to_dicts(
    requirements: (
        tuple[SourceOrganizationInviteRequirement, ...]
        | list[SourceOrganizationInviteRequirement]
        | SourceOrganizationInviteRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize organization invitation requirement records to dictionaries."""
    if isinstance(requirements, SourceOrganizationInviteRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_organization_invite_requirements_to_dicts.__test__ = False


def source_organization_invite_requirements_to_markdown(
    report: SourceOrganizationInviteRequirementsReport,
) -> str:
    """Render an organization invitation requirements report as Markdown."""
    return report.to_markdown()


source_organization_invite_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: OrganizationInviteCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: OrganizationInviteConfidence


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _brief_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _brief_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _brief_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _brief_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _brief_id(payload), payload
    return None, {}


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        if not _is_requirement(segment):
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = [
            category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)
        ]
        for category in _dedupe(categories):
            candidates.append(
                _Candidate(
                    category=category,
                    value=_value(category, segment.text),
                    source_field=segment.source_field,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    confidence=_confidence(segment),
                )
            )
    return candidates


def _has_global_no_scope(payload: Mapping[str, Any]) -> bool:
    for segment in _candidate_segments(payload):
        if segment.source_field.split("[", 1)[0].split(".", 1)[0] not in {
            "title",
            "summary",
            "body",
            "description",
            "scope",
            "non_goals",
            "constraints",
            "source_payload",
        }:
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        if _NO_INVITES_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(
    candidates: Iterable[_Candidate],
    gap_flags: tuple[OrganizationInviteMissingDetail, ...],
) -> list[SourceOrganizationInviteRequirement]:
    grouped: dict[OrganizationInviteCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceOrganizationInviteRequirement] = []
    gap_messages = tuple(_GAP_MESSAGES[flag] for flag in gap_flags)
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda field: (
                min(_CONFIDENCE_ORDER[item.confidence] for item in items if item.source_field == field),
                _field_category_rank(category, field),
                field.casefold(),
            ),
        )[0]
        requirements.append(
            SourceOrganizationInviteRequirement(
                category=category,
                source_field=source_field,
                evidence=tuple(
                    sorted(
                        _dedupe_evidence(
                            item.evidence
                            for item in sorted(
                                items,
                                key=lambda item: (
                                    _field_category_rank(category, item.source_field),
                                    1 if "same" in item.source_field.casefold() else 0,
                                    item.source_field.casefold(),
                                ),
                            )
                        ),
                        key=str.casefold,
                    )
                )[:5],
                confidence=confidence,
                value=_best_value(items),
                suggested_owners=_OWNER_SUGGESTIONS[category],
                planning_notes=_PLANNING_NOTES[category],
                gap_messages=gap_messages,
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.value or "",
            requirement.source_field.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            if str(key) in _IGNORED_FIELDS:
                continue
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _INVITE_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        raw_text = str(value) if isinstance(value, str) else text
        for segment_text, segment_context in _segments(raw_text, field_context):
            segments.append(_Segment(source_field, segment_text, segment_context))


def _segments(value: str, inherited_context: bool) -> list[tuple[str, bool]]:
    segments: list[tuple[str, bool]] = []
    section_context = inherited_context
    for raw_line in value.splitlines() or [value]:
        line = raw_line.strip()
        if not line:
            continue
        heading = _HEADING_RE.match(line)
        if heading:
            title = _clean_text(heading.group("title"))
            section_context = inherited_context or bool(
                _INVITE_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = (
                [part]
                if _NEGATED_SCOPE_RE.search(part) and _INVITE_CONTEXT_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    field_words = _field_words(segment.source_field)
    if _NO_INVITES_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if not (_INVITE_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _INVITE_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:sent|emailed|generated|expires?|expired|resent|revoked|cancelled|canceled|"
            r"pending|assigned|available|restricted|notified|logged|accepted|declined)\b",
            segment.text,
            re.I,
        )
    )


def _missing_detail_flags(evidence_values: Iterable[str]) -> list[OrganizationInviteMissingDetail]:
    text = " ".join(evidence_values)
    if not text:
        return []
    flags: list[OrganizationInviteMissingDetail] = []
    if not _EXPIRY_DETAIL_RE.search(text):
        flags.append("missing_expiry")
    if not _RESEND_REVOKE_DETAIL_RE.search(text):
        flags.append("missing_resend_or_revoke")
    if not _AUTHORIZATION_DETAIL_RE.search(text):
        flags.append("missing_authorization")
    if not _SEAT_DETAIL_RE.search(text):
        flags.append("missing_seat_handling")
    if not _AUDIT_DETAIL_RE.search(text):
        flags.append("missing_audit_details")
    return [flag for flag in _MISSING_DETAIL_ORDER if flag in flags]


def _value(category: OrganizationInviteCategory, text: str) -> str | None:
    if category == "invite_expiry":
        if match := _DURATION_RE.search(text):
            return _clean_text(match.group(0)).casefold()
        if match := re.search(r"\b(?P<value>ttl|expiration window|token lifetime|link lifetime)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "role_seat_assignment":
        if match := _COUNT_RE.search(text):
            return _clean_text(match.group(0)).casefold()
        if match := re.search(r"\b(?P<value>admin|owner|member|viewer|guest|billing admin|available seats?|seat availability|seat cap|seat limit)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "domain_sso_restriction":
        if match := re.search(r"\b(?P<value>sso[- ]?only|single sign[- ]?on|allowed domains?|domain allowlist|same domain|saml|idp)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "magic_link":
        if match := re.search(r"\b(?P<value>magic link|invite link|invitation link|invitation token|single[- ]use link)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "resend_revoke":
        if match := re.search(r"\b(?P<value>resend|re-send|reinvite|re-invite|revoke|cancel|withdraw|void)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category in {"pending_invite_state", "accepted_declined_state"}:
        if match := re.search(r"\b(?P<value>pending|accepted|declined|expired|revoked|invited)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "notifications":
        if match := re.search(r"\b(?P<value>email|in[- ]?app|slack|webhook|notification|reminder)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "audit_trail":
        if match := re.search(r"\b(?P<value>audit log|audit trail|invitation events?|actor and timestamp|ip address)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if match := _VALUE_RE.search(text):
        return _clean_text(match.group(0)).casefold()
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    values = sorted(
        {item.value for item in items if item.value},
        key=lambda value: (
            0 if re.search(r"\d", value) else 1,
            0 if _VALUE_RE.search(value) or _DURATION_RE.search(value) else 1,
            len(value),
            value.casefold(),
        ),
    )
    return values[0] if values else None


def _confidence(segment: _Segment) -> OrganizationInviteConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _REQUIREMENT_RE.search(segment.text) and (
        segment.section_context
        or any(
            marker in segment.source_field.replace("-", "_").casefold()
            for marker in (
                "acceptance_criteria",
                "definition_of_done",
                "success_criteria",
                "scope",
                "authorization",
                "security",
                "organization",
                "organisation",
                "workspace",
                "team",
                "invite",
                "invitation",
                "seat",
                "sso",
                "audit",
                "notification",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _INVITE_CONTEXT_RE.search(searchable):
        return "medium"
    if _INVITE_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(
    requirements: tuple[SourceOrganizationInviteRequirement, ...],
    gap_flags: tuple[OrganizationInviteMissingDetail, ...],
) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "categories": [requirement.category for requirement in requirements],
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "missing_detail_flags": list(gap_flags),
        "missing_detail_counts": {
            flag: sum(1 for requirement in requirements if _GAP_MESSAGES[flag] in requirement.gap_messages)
            for flag in _MISSING_DETAIL_ORDER
        },
        "gap_messages": [_GAP_MESSAGES[flag] for flag in gap_flags],
        "status": "ready_for_planning" if requirements and not gap_flags else "needs_invite_details" if requirements else "no_organization_invite_language",
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "summary",
        "body",
        "description",
        "problem",
        "problem_statement",
        "goal",
        "goals",
        "mvp_goal",
        "context",
        "workflow_context",
        "requirements",
        "constraints",
        "scope",
        "non_goals",
        "assumptions",
        "acceptance",
        "success_criteria",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "authentication",
        "auth_requirements",
        "security",
        "organization_invites",
        "organisation_invites",
        "workspace_invites",
        "team_invites",
        "member_invites",
        "invitations",
        "invites",
        "seat_management",
        "sso",
        "notifications",
        "audit",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: OrganizationInviteCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[OrganizationInviteCategory, tuple[str, ...]] = {
        "invitation_delivery": ("delivery", "email", "send", "invitation"),
        "magic_link": ("magic", "link", "token"),
        "invite_expiry": ("expiry", "expiration", "ttl", "lifetime"),
        "resend_revoke": ("resend", "revoke", "cancel", "withdraw"),
        "pending_invite_state": ("pending", "status", "invited"),
        "role_seat_assignment": ("role", "seat", "permission", "assignment"),
        "domain_sso_restriction": ("domain", "sso", "saml", "idp"),
        "notifications": ("notification", "notify", "email", "reminder"),
        "audit_trail": ("audit", "event", "log", "timestamp"),
        "accepted_declined_state": ("accepted", "declined", "state"),
    }
    return 0 if any(marker in field_words for marker in markers[category]) else 1


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(value)
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        _, _, statement = value.partition(": ")
        key = _clean_text(statement or value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "OrganizationInviteCategory",
    "OrganizationInviteConfidence",
    "OrganizationInviteMissingDetail",
    "SourceOrganizationInviteRequirement",
    "SourceOrganizationInviteRequirementsReport",
    "build_source_organization_invite_requirements",
    "derive_source_organization_invite_requirements",
    "extract_source_organization_invite_requirements",
    "generate_source_organization_invite_requirements",
    "summarize_source_organization_invite_requirements",
    "source_organization_invite_requirements_to_dict",
    "source_organization_invite_requirements_to_dicts",
    "source_organization_invite_requirements_to_markdown",
]
