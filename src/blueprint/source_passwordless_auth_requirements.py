"""Extract source-level passwordless authentication requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


PasswordlessAuthRequirementMode = Literal[
    "magic_link",
    "passkey_webauthn",
    "otp_code",
    "device_binding",
    "session_expiry",
    "fallback_recovery",
    "rate_limit",
    "audit_event",
]
PasswordlessAuthRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_MODE_ORDER: tuple[PasswordlessAuthRequirementMode, ...] = (
    "magic_link",
    "passkey_webauthn",
    "otp_code",
    "device_binding",
    "session_expiry",
    "fallback_recovery",
    "rate_limit",
    "audit_event",
)
_CONFIDENCE_ORDER: dict[PasswordlessAuthRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_PLANNING_NOTES: dict[PasswordlessAuthRequirementMode, str] = {
    "magic_link": "Plan link generation, delivery, single-use token storage, expiry, and replay handling.",
    "passkey_webauthn": "Plan WebAuthn registration, assertion verification, attestation policy, browser support, and recovery.",
    "otp_code": "Plan code generation, delivery channel, verification attempts, expiry, and throttling.",
    "device_binding": "Plan device binding identifiers, trusted-device lifecycle, revocation, and risk exceptions.",
    "session_expiry": "Plan authentication session lifetime, idle timeout, token expiry, and renewal behavior.",
    "fallback_recovery": "Plan fallback sign-in, account recovery, support reset, and anti-takeover controls.",
    "rate_limit": "Plan challenge/link/code throttles, abuse limits, lockout behavior, and observability.",
    "audit_event": "Plan audit events for enrollment, challenge, verification, recovery, and admin actions.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_PASSWORDLESS_CONTEXT_RE = re.compile(
    r"\b(?:passwordless|magic links?|email links?|sign[- ]?in links?|login links?|one[- ]time links?|"
    r"otp|one[- ]time pass(?:code|word)|verification codes?|email codes?|sms codes?|"
    r"passkeys?|webauthn|fido2?|security keys?|platform authenticator|biometric sign[- ]?in|"
    r"device binding|device trust|trusted devices?|remember(?:ed)? devices?|"
    r"session expir(?:y|ation)|session lifetime|idle timeout|auth(?:entication)? session|"
    r"fallback|account recovery|recovery flow|lost device|support reset|rate limits?|throttl(?:e|ing)|"
    r"audit|logged|logging|security event)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:passwordless|magic[-_ ]?link|login[-_ ]?link|otp|one[-_ ]?time|code|passkey|webauthn|"
    r"device[-_ ]?(?:binding|trust)|trusted[-_ ]?device|session|expiry|expiration|timeout|"
    r"fallback|recovery|rate[-_ ]?limit|throttle|audit|event|authentication|auth|security|"
    r"policy|audience|trigger|channel|requirements?|acceptance|criteria|source_payload|metadata)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"enforce|challenge|prompt|verify|allow|support|provide|offer|enable|disable|"
    r"expire|expires?|limit|throttle|log|record|audit|mandate|policy|gate|blocked until|"
    r"cannot ship|acceptance|done when)\b",
    re.I,
)
_NO_PASSWORDLESS_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,90}\b(?:passwordless|magic links?|login links?|otp|one[- ]time|"
    r"passkeys?|webauthn|device binding|trusted devices?|session expir(?:y|ation)|rate limits?|audit)\b"
    r".{0,90}\b(?:required|needed|in scope|scope|impact|changes?)\b|"
    r"\b(?:passwordless|magic links?|login links?|otp|one[- ]time|passkeys?|webauthn|device binding|"
    r"trusted devices?|session expir(?:y|ation)|rate limits?|audit)\b.{0,90}\b"
    r"(?:not required|out of scope|non[- ]goal|excluded)\b",
    re.I,
)
_MODE_PATTERNS: dict[PasswordlessAuthRequirementMode, re.Pattern[str]] = {
    "magic_link": re.compile(
        r"\b(?:magic links?|email links?|sign[- ]?in links?|login links?|one[- ]time links?|"
        r"passwordless links?|link token)\b",
        re.I,
    ),
    "passkey_webauthn": re.compile(
        r"\b(?:passkeys?|webauthn|fido2?|security keys?|platform authenticator|biometric sign[- ]?in)\b",
        re.I,
    ),
    "otp_code": re.compile(
        r"\b(?:otp|one[- ]time pass(?:code|word)|one[- ]time codes?|verification codes?|"
        r"email codes?|sms codes?|6[- ]?digit codes?|numeric codes?)\b",
        re.I,
    ),
    "device_binding": re.compile(
        r"\b(?:device binding|bind(?:ing)? devices?|device trust|trusted devices?|remember(?:ed)? devices?|"
        r"known devices?|new device verification)\b",
        re.I,
    ),
    "session_expiry": re.compile(
        r"\b(?:session expir(?:y|ation)|session lifetime|session duration|idle timeout|"
        r"auth(?:entication)? session|token expir(?:y|ation))\b",
        re.I,
    ),
    "fallback_recovery": re.compile(
        r"\b(?:fallback|backup sign[- ]?in|account recovery|recovery flow|recover access|lost device|"
        r"support reset|break glass|recovery email|fallback channel)\b",
        re.I,
    ),
    "rate_limit": re.compile(
        r"\b(?:rate limits?|rate limiting|throttl(?:e|es|ed|ing)|attempt limits?|retry limits?|"
        r"resend limits?|abuse limits?|lockout|too many requests|cooldown|cool[- ]down)\b",
        re.I,
    ),
    "audit_event": re.compile(
        r"\b(?:audit|audited|logged|logging|security events?|event log|admin report|"
        r"compliance evidence|attestation|proof)\b",
        re.I,
    ),
}
_DETAIL_PATTERNS: dict[PasswordlessAuthRequirementMode, re.Pattern[str]] = {
    "magic_link": re.compile(
        r"\b(?:magic links?|email links?|sign[- ]?in links?|login links?|one[- ]time links?)[^.;\n]*",
        re.I,
    ),
    "passkey_webauthn": re.compile(r"\b(?:passkeys?|webauthn|fido2?|security keys?)[^.;\n]*", re.I),
    "otp_code": re.compile(
        r"\b(?:otp|one[- ]time pass(?:code|word)|verification codes?|email codes?|sms codes?)[^.;\n]*",
        re.I,
    ),
    "device_binding": re.compile(
        r"\b(?:device binding|trusted devices?|remember(?:ed)? devices?|new device verification)[^.;\n]*",
        re.I,
    ),
    "session_expiry": re.compile(
        r"\b(?:session expir(?:y|ation)|session lifetime|idle timeout|token expir(?:y|ation)|"
        r"link expir(?:y|ation)|code expir(?:y|ation)|expires? after)[^.;\n]*",
        re.I,
    ),
    "fallback_recovery": re.compile(
        r"\b(?:fallback|account recovery|recovery flow|recover access|lost device|support reset|break glass)[^.;\n]*",
        re.I,
    ),
    "rate_limit": re.compile(
        r"\b(?:rate limits?|throttl(?:e|ing)|attempt limits?|retry limits?|resend limits?|lockout|cooldown)[^.;\n]*",
        re.I,
    ),
    "audit_event": re.compile(r"\b(?:audit|logged|logging|security events?|event log|admin report)[^.;\n]*", re.I),
}
_TRIGGER_RE = re.compile(
    r"\b(?:on|when|whenever|before|after|during|for)\s+"
    r"((?:login|log[- ]?in|sign[- ]?in|registration|enrollment|new device|lost device|risky login|"
    r"account recovery|password reset|admin access|session renewal|sensitive export|"
    r"high[- ]risk action|payment change|profile change|privileged action)[^.;\n]*)",
    re.I,
)
_AUDIENCE_RE = re.compile(
    r"\b(?:for|applies to|required for|enforced for|available to)\s+(?:the\s+)?"
    r"((?:admins?|administrators?|owners?|operators?|support agents?|employees?|staff|"
    r"all users|customers?|members?|enterprise users?|privileged users?|new users|existing users)[^.;,\n]*)",
    re.I,
)
_SUBJECT_AUDIENCE_RE = re.compile(
    r"^\s*((?:admins?|administrators?|owners?|operators?|support agents?|employees?|staff|"
    r"all users|customers?|members?|enterprise users?|privileged users?|new users|existing users))\b"
    r"\s+(?:must|shall|required|requires?|need|needs|should|can|may)\b",
    re.I,
)
_CHANNEL_RE = re.compile(r"\b(?:via|by|through|over)\s+((?:email|sms|text message|push|mobile app|support)[^.;,\n]*)", re.I)
_EXPIRY_RE = re.compile(
    r"\b(?:expires?|expire|valid for|session expir(?:y|ation)|session lifetime|idle timeout|timeout)\s*"
    r"(?:must be|should be|after|in|of|:|=|is)?\s*"
    r"((?:\d+\s*(?:seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d))[^.;,\n]*)",
    re.I,
)
_LIMIT_RE = re.compile(
    r"\b((?:\d+\s*)?(?:attempts?|tries|requests?|resends?|links?|codes?)\s*(?:per|/)\s*"
    r"(?:minute|hour|day|user|account|email|phone|device|ip)|"
    r"(?:cooldown|cool[- ]down|lockout)\s*(?:of|for|after|:)?\s*\d+\s*(?:seconds?|minutes?|hours?))\b",
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


@dataclass(frozen=True, slots=True)
class SourcePasswordlessAuthRequirement:
    """One source-backed passwordless authentication requirement."""

    source_brief_id: str | None
    mode: PasswordlessAuthRequirementMode
    detail: str | None = None
    trigger: str | None = None
    audience: str | None = None
    channel: str | None = None
    expiry: str | None = None
    limit: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field: str | None = None
    confidence: PasswordlessAuthRequirementConfidence = "medium"
    missing_detail_flags: tuple[str, ...] = field(default_factory=tuple)
    planning_note: str | None = None

    @property
    def requirement_mode(self) -> PasswordlessAuthRequirementMode:
        """Compatibility alias for callers that use requirement_mode naming."""
        return self.mode

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "mode": self.mode,
            "requirement_mode": self.requirement_mode,
            "detail": self.detail,
            "trigger": self.trigger,
            "audience": self.audience,
            "channel": self.channel,
            "expiry": self.expiry,
            "limit": self.limit,
            "evidence": list(self.evidence),
            "source_field": self.source_field,
            "confidence": self.confidence,
            "missing_detail_flags": list(self.missing_detail_flags),
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourcePasswordlessAuthRequirementsReport:
    """Source-level passwordless authentication requirements report."""

    source_id: str | None = None
    requirements: tuple[SourcePasswordlessAuthRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourcePasswordlessAuthRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourcePasswordlessAuthRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return passwordless authentication requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Passwordless Auth Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        mode_counts = self.summary.get("mode_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Mode counts: " + ", ".join(f"{mode} {mode_counts.get(mode, 0)}" for mode in _MODE_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No passwordless authentication requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Mode | Detail | Trigger | Audience | Channel | Expiry | Limit | Source Field | Confidence | Missing Details | Planning Note | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.mode} | "
                f"{_markdown_cell(requirement.detail or '')} | "
                f"{_markdown_cell(requirement.trigger or '')} | "
                f"{_markdown_cell(requirement.audience or '')} | "
                f"{_markdown_cell(requirement.channel or '')} | "
                f"{_markdown_cell(requirement.expiry or '')} | "
                f"{_markdown_cell(requirement.limit or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(', '.join(requirement.missing_detail_flags))} | "
                f"{_markdown_cell(requirement.planning_note or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_passwordless_auth_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePasswordlessAuthRequirementsReport:
    """Extract source-level passwordless authentication requirement records."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourcePasswordlessAuthRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_passwordless_auth_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePasswordlessAuthRequirementsReport:
    """Compatibility alias for building a passwordless auth requirements report."""
    return build_source_passwordless_auth_requirements(source)


def generate_source_passwordless_auth_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePasswordlessAuthRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_passwordless_auth_requirements(source)


def derive_source_passwordless_auth_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePasswordlessAuthRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_passwordless_auth_requirements(source)


def summarize_source_passwordless_auth_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourcePasswordlessAuthRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted passwordless auth requirements."""
    if isinstance(source_or_result, SourcePasswordlessAuthRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_passwordless_auth_requirements(source_or_result).summary


def source_passwordless_auth_requirements_to_dict(report: SourcePasswordlessAuthRequirementsReport) -> dict[str, Any]:
    """Serialize a passwordless auth requirements report to a plain dictionary."""
    return report.to_dict()


source_passwordless_auth_requirements_to_dict.__test__ = False


def source_passwordless_auth_requirements_to_dicts(
    requirements: (
        tuple[SourcePasswordlessAuthRequirement, ...]
        | list[SourcePasswordlessAuthRequirement]
        | SourcePasswordlessAuthRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize passwordless auth requirement records to dictionaries."""
    if isinstance(requirements, SourcePasswordlessAuthRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_passwordless_auth_requirements_to_dicts.__test__ = False


def source_passwordless_auth_requirements_to_markdown(report: SourcePasswordlessAuthRequirementsReport) -> str:
    """Render a passwordless auth requirements report as Markdown."""
    return report.to_markdown()


source_passwordless_auth_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    mode: PasswordlessAuthRequirementMode
    detail: str | None
    trigger: str | None
    audience: str | None
    channel: str | None
    expiry: str | None
    limit: str | None
    source_field: str
    evidence: str
    confidence: PasswordlessAuthRequirementConfidence


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
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_id(payload), payload
    return None, {}


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _candidates_for_briefs(brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            if _NO_PASSWORDLESS_RE.search(searchable):
                continue
            mode_searchable = _structured_mode_text(segment.text) or searchable
            modes = [mode for mode in _MODE_ORDER if _MODE_PATTERNS[mode].search(mode_searchable)]
            if mode_searchable != searchable:
                modes.extend(mode for mode in _MODE_ORDER if _MODE_PATTERNS[mode].search(searchable))
            if not modes and re.search(r"\bpasswordless\b", searchable, re.I) and _REQUIREMENT_RE.search(segment.text):
                modes = ["magic_link"]
            if not modes or not _is_requirement(segment):
                continue
            for mode in _dedupe(modes):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        mode=mode,
                        detail=_mode_detail(mode, segment.text),
                        trigger=_trigger(segment.text),
                        audience=_audience(segment.text),
                        channel=_channel(segment.text),
                        expiry=_expiry(segment.text),
                        limit=_limit(segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourcePasswordlessAuthRequirement]:
    grouped: dict[tuple[str | None, PasswordlessAuthRequirementMode], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.mode), []).append(candidate)

    requirements: list[SourcePasswordlessAuthRequirement] = []
    for (source_brief_id, mode), items in grouped.items():
        best = max(items, key=_candidate_score)
        detail = _joined_details(item.detail for item in items)
        trigger = _joined_details(item.trigger for item in items)
        audience = _joined_details(item.audience for item in items)
        channel = _joined_details(item.channel for item in items)
        expiry = _joined_details(item.expiry for item in items)
        limit = _joined_details(item.limit for item in items)
        requirements.append(
            SourcePasswordlessAuthRequirement(
                source_brief_id=source_brief_id,
                mode=mode,
                detail=detail,
                trigger=trigger,
                audience=audience,
                channel=channel,
                expiry=expiry,
                limit=limit,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                source_field=best.source_field,
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                missing_detail_flags=_missing_detail_flags(mode, detail, trigger, audience, channel, expiry, limit),
                planning_note=_PLANNING_NOTES[mode],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _MODE_ORDER.index(requirement.mode),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.source_field or "",
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
        "authentication",
        "auth_requirements",
        "security",
        "passwordless",
        "passwordless_auth",
        "passwordless_requirements",
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


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        if _has_structured_shape(value):
            evidence = _structured_text(value)
            if evidence:
                segments.append(_Segment(source_field, evidence, True))
            return
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _clean_text(str(key).replace("_", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _PASSWORDLESS_CONTEXT_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _MODE_PATTERNS.values())
            )
            _append_value(segments, f"{source_field}.{key}", value[key], child_context)
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
                _PASSWORDLESS_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NO_PASSWORDLESS_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text and not _NO_PASSWORDLESS_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NO_PASSWORDLESS_RE.search(searchable) or not _PASSWORDLESS_CONTEXT_RE.search(searchable):
        return False
    return bool(
        _REQUIREMENT_RE.search(segment.text)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
        or _TRIGGER_RE.search(searchable)
        or _CHANNEL_RE.search(searchable)
        or _EXPIRY_RE.search(searchable)
        or _LIMIT_RE.search(searchable)
    )


def _confidence(segment: _Segment) -> PasswordlessAuthRequirementConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    has_detail = any(
        (
            _TRIGGER_RE.search(searchable),
            _AUDIENCE_RE.search(searchable),
            _CHANNEL_RE.search(searchable),
            _EXPIRY_RE.search(searchable),
            _LIMIT_RE.search(searchable),
            any(pattern.search(searchable) for pattern in _DETAIL_PATTERNS.values()),
        )
    )
    if (_REQUIREMENT_RE.search(segment.text) or segment.section_context) and has_detail:
        return "high"
    if segment.section_context or _REQUIREMENT_RE.search(segment.text) or has_detail:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourcePasswordlessAuthRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "mode_counts": {mode: sum(1 for requirement in requirements if requirement.mode == mode) for mode in _MODE_ORDER},
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "modes": [requirement.mode for requirement in requirements],
        "missing_detail_flags": sorted(
            {flag for requirement in requirements for flag in requirement.missing_detail_flags}
        ),
    }


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    return bool(
        keys
        & {
            "mode",
            "method",
            "auth_mode",
            "passwordless_mode",
            "trigger",
            "audience",
            "role",
            "channel",
            "expiry",
            "expiration",
            "timeout",
            "limit",
            "rate_limit",
            "fallback",
            "recovery",
            "evidence",
            "audit",
            "policy",
        }
    )


def _structured_text(item: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = ", ".join(_strings(value)) if isinstance(value, (list, tuple, set)) else _clean_text(value)
        if text:
            parts.append(f"{key}: {text}")
    return "; ".join(parts)


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
        "authentication",
        "auth_requirements",
        "security",
        "passwordless",
        "passwordless_auth",
        "passwordless_requirements",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _match_detail(pattern: re.Pattern[str], text: str) -> str | None:
    if match := pattern.search(text):
        group = match.group(1) if match.lastindex else match.group(0)
        return _detail(group)
    return None


def _labeled_detail(label: str, text: str) -> str | None:
    if match := re.search(rf"(?:^|;\s*)(?:{label})\s*:\s*([^;]+)", text, re.I):
        return _detail(match.group(1))
    return None


def _mode_detail(mode: PasswordlessAuthRequirementMode, text: str) -> str | None:
    return _labeled_detail("detail|requirement|policy|mode|method|auth_mode|passwordless_mode", text) or _match_detail(
        _DETAIL_PATTERNS[mode], text
    )


def _trigger(text: str) -> str | None:
    return _labeled_detail("trigger", text) or _match_detail(_TRIGGER_RE, text)


def _audience(text: str) -> str | None:
    return _labeled_detail("audience|role", text) or _match_detail(_AUDIENCE_RE, text) or _match_detail(_SUBJECT_AUDIENCE_RE, text)


def _channel(text: str) -> str | None:
    return _labeled_detail("channel|delivery", text) or _match_detail(_CHANNEL_RE, text)


def _expiry(text: str) -> str | None:
    return _labeled_detail("expiry|expiration|timeout|ttl|lifetime", text) or _match_detail(_EXPIRY_RE, text)


def _limit(text: str) -> str | None:
    return _labeled_detail("limit|rate_limit|throttle|throttling", text) or _match_detail(_LIMIT_RE, text)


def _structured_mode_text(text: str) -> str | None:
    if match := re.search(r"(?:^|;\s*)(?:mode|method|auth_mode|passwordless_mode)\s*:\s*([^;]+)", text, re.I):
        return match.group(1)
    return None


def _missing_detail_flags(
    mode: PasswordlessAuthRequirementMode,
    detail: str | None,
    trigger: str | None,
    audience: str | None,
    channel: str | None,
    expiry: str | None,
    limit: str | None,
) -> tuple[str, ...]:
    flags: list[str] = []
    if not detail:
        flags.append("detail")
    if not audience:
        flags.append("audience")
    if mode in {"magic_link", "otp_code"} and not channel:
        flags.append("channel")
    if mode in {"magic_link", "otp_code", "session_expiry"} and not expiry:
        flags.append("expiry")
    if mode in {"magic_link", "otp_code", "rate_limit"} and not limit:
        flags.append("rate_limit")
    if mode in {"passkey_webauthn", "device_binding", "fallback_recovery", "audit_event"} and not trigger:
        flags.append("trigger")
    return tuple(_dedupe(flags))


def _joined_details(values: Iterable[str | None]) -> str | None:
    details = _dedupe(value for value in values if value)
    return ", ".join(details) if details else None


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, int, int, str]:
    detail_count = sum(bool(value) for value in (candidate.detail, candidate.trigger, candidate.audience, candidate.channel, candidate.expiry, candidate.limit))
    return (
        detail_count,
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int("[" in candidate.source_field),
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        -_field_rank(candidate.source_field),
        candidate.evidence,
    )


def _field_rank(source_field: str) -> int:
    if match := re.search(r"\[(\d+)\]", source_field):
        return int(match.group(1))
    return 0


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _clean_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _clean_text(value)
    return [text] if text else []


def _field_words(source_field: str) -> str:
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


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


def _detail(value: Any) -> str | None:
    text = _clean_text(value).strip("`'\" ;,.")
    return text[:160].rstrip() if text else None


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
    "PasswordlessAuthRequirementConfidence",
    "PasswordlessAuthRequirementMode",
    "SourcePasswordlessAuthRequirement",
    "SourcePasswordlessAuthRequirementsReport",
    "build_source_passwordless_auth_requirements",
    "derive_source_passwordless_auth_requirements",
    "extract_source_passwordless_auth_requirements",
    "generate_source_passwordless_auth_requirements",
    "source_passwordless_auth_requirements_to_dict",
    "source_passwordless_auth_requirements_to_dicts",
    "source_passwordless_auth_requirements_to_markdown",
    "summarize_source_passwordless_auth_requirements",
]
