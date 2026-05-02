"""Extract source-level user invitation and onboarding requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


UserInvitationRequirementCategory = Literal[
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
UserInvitationMissingDetail = Literal[
    "unspecified_expiration",
    "unspecified_role",
    "unspecified_email_channel_behavior",
    "unspecified_cancellation",
    "unspecified_audit_evidence",
]
UserInvitationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[UserInvitationRequirementCategory, ...] = (
    "invitation_delivery",
    "invite_expiration",
    "resend_cancel",
    "role_assignment",
    "domain_restriction",
    "bulk_invite",
    "pending_user_state",
    "acceptance_audit",
    "onboarding_redirect",
)
_MISSING_DETAIL_ORDER: tuple[UserInvitationMissingDetail, ...] = (
    "unspecified_expiration",
    "unspecified_role",
    "unspecified_email_channel_behavior",
    "unspecified_cancellation",
    "unspecified_audit_evidence",
)
_CONFIDENCE_ORDER: dict[UserInvitationConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|provide|define|document|record|track|audit|notify|send|expire|"
    r"cancel|resend|invite|onboard|redirect|before launch|cannot ship|done when|acceptance)\b",
    re.I,
)
_INVITATION_CONTEXT_RE = re.compile(
    r"\b(?:invitation|invitations|invite|invites|invited|inviting|onboarding|onboard|"
    r"new user|pending user|pending member|accept invite|accept invitation|join link|"
    r"signup link|sign-up link|magic link|invitation token|workspace member|team member)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:invitation|invite|onboarding|onboard|resend|cancel|expiration|expiry|ttl|"
    r"role|permission|domain|bulk|pending|acceptance|audit|redirect|requirements?|"
    r"acceptance|criteria|definition[-_ ]?of[-_ ]?done|metadata|source[-_ ]?payload)",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,120}"
    r"\b(?:invitation|invite|onboarding|pending users?|roles?|domain restrictions?|bulk invites?|"
    r"resend|cancel|expiry|expiration|audit)\b.{0,120}"
    r"\b(?:required|needed|in scope|changes?|work|support|planned|for this release)\b|"
    r"\b(?:invitation|invite|onboarding|pending users?|roles?|domain restrictions?|bulk invites?|"
    r"resend|cancel|expiry|expiration|audit)\b.{0,120}"
    r"\b(?:out of scope|not required|not needed|no changes?|no work|non-goal|non goal)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:minutes?|hours?|days?|weeks?|months?)|"
    r"\d+\s*(?:invites?|invitations?|users?|members?|recipients?)|"
    r"\d+(?:st|nd|rd|th)\s+(?:day|week|month)|"
    r"(?:admin|owner|manager|member|viewer|editor|guest|billing admin|read[- ]?only)|"
    r"(?:email|sms|slack|webhook|in-app|magic link|invite link|sso)|"
    r"(?:same domain|allowed domains?|allowlist|denylist|blocklist)|"
    r"(?:csv|bulk import|batch(?:es)?\s+of\s+\d+)|"
    r"(?:pending|invited|active|accepted|expired|revoked|cancelled|canceled))\b",
    re.I,
)
_DURATION_RE = re.compile(r"\b\d+\s*(?:minutes?|hours?|days?|weeks?|months?)\b", re.I)
_COUNT_RE = re.compile(r"\b\d+\s*(?:invites?|invitations?|users?|members?|recipients?|invitees)\b", re.I)
_ROLE_DETAIL_RE = re.compile(
    r"\b(?:admin|owner|manager|viewer|editor|guest|billing admin|role|roles|permission|permissions|group)\b",
    re.I,
)
_CHANNEL_DETAIL_RE = re.compile(
    r"\b(?:email|e-mail|sms|slack|webhook|in-app|notification|magic link|invite link|channel)\b",
    re.I,
)
_CANCELLATION_DETAIL_RE = re.compile(r"\b(?:cancel|canceled|cancelled|revoke|revoked|withdraw|void)\b", re.I)
_AUDIT_DETAIL_RE = re.compile(
    r"\b(?:audit|log|event|evidence|record actor|actor|ip address|timestamp|accepted by|acceptance record)\b",
    re.I,
)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
}

_CATEGORY_PATTERNS: dict[UserInvitationRequirementCategory, re.Pattern[str]] = {
    "invitation_delivery": re.compile(
        r"\b(?:invitation delivery|invite delivery|send(?:s|ing)? invitations?|send(?:s|ing)? invites?|"
        r"deliver(?:y|ed)? invitations?|deliver(?:y|ed)? invites?|invite email|email invites?|"
        r"invitation email|send.{0,60}magic link|notification channel|delivery channel|"
        r"sms invite|slack invite|webhook invite)\b",
        re.I,
    ),
    "invite_expiration": re.compile(
        r"\b(?:invite expiration|invitation expiration|invite expiry|invitation expiry|expires?|expired|"
        r"expiration window|valid for|ttl|time[- ]?to[- ]?live|token lifetime|link lifetime)\b",
        re.I,
    ),
    "resend_cancel": re.compile(
        r"\b(?:resend|re-send|reinvite|re-invite|send again|cancel invitation|cancel invite|"
        r"revoke invitation|revoke invite|withdraw invitation|void invitation)\b",
        re.I,
    ),
    "role_assignment": re.compile(
        r"\b(?:assign(?:ed|s)? roles?|role assignment|default role|invited as|permission set|"
        r"workspace role|team role|member role|admin role|owner role|group assignment)\b",
        re.I,
    ),
    "domain_restriction": re.compile(
        r"\b(?:domain restriction|allowed domains?|allowlisted domains?|domain allowlist|email domain|"
        r"same domain|company domain|approved domain|denylisted domains?|blocked domains?|sso domain)\b",
        re.I,
    ),
    "bulk_invite": re.compile(
        r"\b(?:bulk invite|bulk invitation|invite in bulk|batch invite|batch invitation|csv invite|"
        r"csv import|bulk import|import invitees|multiple invitees|invite multiple|mass invite)\b",
        re.I,
    ),
    "pending_user_state": re.compile(
        r"\b(?:pending users?|pending members?|pending invitees?|invited users?|invited members?|pending invitation|"
        r"invitation status|invite status|provisional account|pre[- ]acceptance|unaccepted invite)\b",
        re.I,
    ),
    "acceptance_audit": re.compile(
        r"\b(?:acceptance audit|invitation audit|audit log|audit event|accepted invitation|invite accepted|"
        r"acceptance timestamp|accepted by|record actor|ip address|audit evidence)\b",
        re.I,
    ),
    "onboarding_redirect": re.compile(
        r"\b(?:onboarding redirect|redirect after accept(?:ance)?|post[- ]accept(?:ance)? redirect|"
        r"welcome flow|first[- ]run|getting started|after accepting.{0,60}(?:redirect|onboard|welcome)|"
        r"route to onboarding)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class SourceUserInvitationRequirement:
    """One source-backed user invitation or onboarding requirement."""

    requirement_category: UserInvitationRequirementCategory
    value: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    missing_detail_flags: tuple[UserInvitationMissingDetail, ...] = field(default_factory=tuple)
    confidence: UserInvitationConfidence = "medium"
    source_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_category": self.requirement_category,
            "value": self.value,
            "evidence": list(self.evidence),
            "missing_detail_flags": list(self.missing_detail_flags),
            "confidence": self.confidence,
            "source_id": self.source_id,
        }


@dataclass(frozen=True, slots=True)
class SourceUserInvitationRequirementsReport:
    """Source-level user invitation and onboarding requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceUserInvitationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceUserInvitationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceUserInvitationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "findings": [finding.to_dict() for finding in self.findings],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return user invitation requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source User Invitation Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Source count: {self.summary.get('source_count', 0)}",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Missing detail flags: " + ", ".join(self.summary.get("missing_detail_flags", [])),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No user invitation requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Value | Missing Details | Confidence | Source | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_category} | "
                f"{_markdown_cell(requirement.value)} | "
                f"{_markdown_cell(', '.join(requirement.missing_detail_flags))} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_id or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_user_invitation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceUserInvitationRequirementsReport:
    """Extract source-level user invitation requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    candidates, matched_texts = _candidates_for_briefs(brief_payloads)
    missing_detail_flags = tuple(_missing_detail_flags(matched_texts))
    requirements = tuple(_merge_candidates(candidates, missing_detail_flags))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceUserInvitationRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads), missing_detail_flags),
    )


def generate_source_user_invitation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceUserInvitationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_user_invitation_requirements(source)


def derive_source_user_invitation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceUserInvitationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_user_invitation_requirements(source)


def extract_source_user_invitation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> tuple[SourceUserInvitationRequirement, ...]:
    """Return user invitation requirement records extracted from brief-shaped input."""
    return build_source_user_invitation_requirements(source).requirements


def summarize_source_user_invitation_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceUserInvitationRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted user invitation requirements."""
    if isinstance(source_or_result, SourceUserInvitationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_user_invitation_requirements(source_or_result).summary


def source_user_invitation_requirements_to_dict(
    report: SourceUserInvitationRequirementsReport,
) -> dict[str, Any]:
    """Serialize a user invitation requirements report to a plain dictionary."""
    return report.to_dict()


source_user_invitation_requirements_to_dict.__test__ = False


def source_user_invitation_requirements_to_dicts(
    requirements: (
        tuple[SourceUserInvitationRequirement, ...]
        | list[SourceUserInvitationRequirement]
        | SourceUserInvitationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize user invitation requirement records to dictionaries."""
    if isinstance(requirements, SourceUserInvitationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_user_invitation_requirements_to_dicts.__test__ = False


def source_user_invitation_requirements_to_markdown(
    report: SourceUserInvitationRequirementsReport,
) -> str:
    """Render a user invitation requirements report as Markdown."""
    return report.to_markdown()


source_user_invitation_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_category: UserInvitationRequirementCategory
    value: str
    evidence: str
    confidence: UserInvitationConfidence
    source_id: str | None


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)) or hasattr(
        source, "model_dump"
    ):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


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


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> tuple[list[_Candidate], list[str]]:
    candidates: list[_Candidate] = []
    matched_texts: list[str] = []
    for source_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            if not _is_requirement(segment):
                continue
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            categories = [
                category
                for category in _CATEGORY_ORDER
                if _CATEGORY_PATTERNS[category].search(searchable)
            ]
            if categories:
                matched_texts.append(searchable)
            for category in _dedupe(categories):
                candidates.append(
                    _Candidate(
                        requirement_category=category,
                        value=_value(segment.text),
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(segment),
                        source_id=source_id,
                    )
                )
    return candidates, matched_texts


def _merge_candidates(
    candidates: Iterable[_Candidate],
    missing_detail_flags: tuple[UserInvitationMissingDetail, ...],
) -> list[SourceUserInvitationRequirement]:
    grouped: dict[tuple[str | None, UserInvitationRequirementCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_id, candidate.requirement_category), []).append(candidate)

    requirements: list[SourceUserInvitationRequirement] = []
    for (source_id, category), items in grouped.items():
        evidence = tuple(sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold))[:5]
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        requirements.append(
            SourceUserInvitationRequirement(
                requirement_category=category,
                value=_strongest_value(items),
                evidence=evidence,
                missing_detail_flags=missing_detail_flags,
                confidence=confidence,
                source_id=source_id,
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_id) or "",
            _CATEGORY_ORDER.index(requirement.requirement_category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.value.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in (
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
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "invitation",
        "invitations",
        "invite",
        "invites",
        "onboarding",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(
    segments: list[_Segment],
    source_field: str,
    value: Any,
    section_context: bool,
) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _INVITATION_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text, segment_context in _segments(text, field_context):
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
                _INVITATION_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
                if _NEGATED_SCOPE_RE.search(part) and _INVITATION_CONTEXT_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if _INVITATION_CONTEXT_RE.search(segment.text) and re.search(
        r"\b(?:sends?|sent|expires?|expired|resends?|cancels?|revokes?|assigns?|redirects?|audits?|records?)\b",
        segment.text,
        re.I,
    ):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        return True
    return False


def _missing_detail_flags(matched_texts: Iterable[str]) -> list[UserInvitationMissingDetail]:
    text = " ".join(matched_texts)
    if not text:
        return []
    flags: list[UserInvitationMissingDetail] = []
    has_expiration = bool(_CATEGORY_PATTERNS["invite_expiration"].search(text) and _DURATION_RE.search(text))
    if not has_expiration:
        flags.append("unspecified_expiration")
    if not _ROLE_DETAIL_RE.search(text):
        flags.append("unspecified_role")
    if not _CHANNEL_DETAIL_RE.search(text):
        flags.append("unspecified_email_channel_behavior")
    if not _CANCELLATION_DETAIL_RE.search(text):
        flags.append("unspecified_cancellation")
    if not _AUDIT_DETAIL_RE.search(text):
        flags.append("unspecified_audit_evidence")
    return [flag for flag in _MISSING_DETAIL_ORDER if flag in flags]


def _value(text: str) -> str:
    if match := _DURATION_RE.search(text):
        return _clean_text(match.group(0)).casefold()
    if match := _COUNT_RE.search(text):
        return _clean_text(match.group(0)).casefold()
    if match := _VALUE_RE.search(text):
        return _clean_text(match.group(0)).casefold()
    return _clean_text(text)


def _strongest_value(items: Iterable[_Candidate]) -> str:
    ordered = sorted(
        items,
        key=lambda item: (
            _CONFIDENCE_ORDER[item.confidence],
            0 if _VALUE_RE.search(item.value) or _DURATION_RE.search(item.value) else 1,
            len(item.value),
            item.value.casefold(),
        ),
    )
    return ordered[0].value


def _confidence(segment: _Segment) -> UserInvitationConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _VALUE_RE.search(segment.text) and (_REQUIREMENT_RE.search(segment.text) or segment.section_context):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _INVITATION_CONTEXT_RE.search(searchable):
        return "high"
    if _INVITATION_CONTEXT_RE.search(searchable):
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceUserInvitationRequirement, ...],
    source_count: int,
    missing_detail_flags: tuple[UserInvitationMissingDetail, ...],
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.requirement_category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "requirement_categories": [requirement.requirement_category for requirement in requirements],
        "missing_detail_flags": list(missing_detail_flags) if requirements else [],
        "missing_detail_counts": {
            flag: sum(1 for requirement in requirements if flag in requirement.missing_detail_flags)
            for flag in _MISSING_DETAIL_ORDER
        },
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
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "requirements",
        "constraints",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "invitation",
        "invitations",
        "invite",
        "invites",
        "onboarding",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = str(value).strip()
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
    "SourceUserInvitationRequirement",
    "SourceUserInvitationRequirementsReport",
    "UserInvitationConfidence",
    "UserInvitationMissingDetail",
    "UserInvitationRequirementCategory",
    "build_source_user_invitation_requirements",
    "derive_source_user_invitation_requirements",
    "extract_source_user_invitation_requirements",
    "generate_source_user_invitation_requirements",
    "source_user_invitation_requirements_to_dict",
    "source_user_invitation_requirements_to_dicts",
    "source_user_invitation_requirements_to_markdown",
    "summarize_source_user_invitation_requirements",
]
