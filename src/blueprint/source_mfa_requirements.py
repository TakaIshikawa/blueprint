"""Extract source-level multi-factor authentication requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


MfaRequirementMethod = Literal[
    "totp",
    "sms_otp",
    "email_otp",
    "webauthn_passkey",
    "backup_codes",
    "step_up",
    "enrollment",
    "recovery",
    "remembered_devices",
    "admin_enforcement",
]
MfaRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_METHOD_ORDER: tuple[MfaRequirementMethod, ...] = (
    "totp",
    "sms_otp",
    "email_otp",
    "webauthn_passkey",
    "backup_codes",
    "step_up",
    "enrollment",
    "recovery",
    "remembered_devices",
    "admin_enforcement",
)
_CONFIDENCE_ORDER: dict[MfaRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_PLANNING_NOTES: dict[MfaRequirementMethod, str] = {
    "totp": "Plan authenticator app enrollment, verification, clock drift handling, and reset support.",
    "sms_otp": "Plan phone capture, delivery provider behavior, rate limits, and fallback risk controls.",
    "email_otp": "Plan email code delivery, expiry, throttling, and account recovery interaction.",
    "webauthn_passkey": "Plan WebAuthn/passkey registration, device binding, browser support, and recovery paths.",
    "backup_codes": "Plan backup code generation, one-time use storage, download/display UX, and regeneration.",
    "step_up": "Plan trigger evaluation, session challenge flow, and post-challenge authorization behavior.",
    "enrollment": "Plan required MFA setup, grace periods, setup reminders, and enforcement gates.",
    "recovery": "Plan recovery verification, support escalation, audit evidence, and anti-takeover controls.",
    "remembered_devices": "Plan remembered device lifetime, device revocation, cookie binding, and risk exceptions.",
    "admin_enforcement": "Plan admin policy controls, role targeting, rollout state, and reporting.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_MFA_CONTEXT_RE = re.compile(
    r"\b(?:mfa|multi[- ]?factor|two[- ]?factor|2fa|otp|one[- ]time pass(?:code|word)|"
    r"authenticator app|totp|sms code|email code|webauthn|passkeys?|security key|"
    r"backup code|recovery code|account recovery|factor reset|lost device|step[- ]up|"
    r"remembered device|trusted device|factor enrollment|mfa enrollment|admin enforcement)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:mfa|multi[-_ ]?factor|two[-_ ]?factor|2fa|otp|totp|passkey|webauthn|"
    r"backup[-_ ]?code|recovery[-_ ]?code|step[-_ ]?up|remembered[-_ ]?device|"
    r"trusted[-_ ]?device|auth[-_ ]?method|authentication|security|policy|audience|"
    r"trigger|fallback|recovery|evidence|enrollment|admin)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"enforce|challenge|prompt|verify|allow|support|provide|offer|enable|disable|"
    r"mandate|policy|gate|blocked until|cannot ship|acceptance|done when)\b",
    re.I,
)
_NO_MFA_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:mfa|multi[- ]?factor|two[- ]?factor|2fa|otp|"
    r"step[- ]up|passkey|webauthn|backup codes?|remembered devices?)\b.{0,80}\b"
    r"(?:required|needed|in scope|scope|impact|changes?)\b|"
    r"\b(?:mfa|multi[- ]?factor|two[- ]?factor|2fa|otp|step[- ]up|passkey|webauthn|"
    r"backup codes?|remembered devices?)\b.{0,80}\b(?:not required|out of scope|non[- ]goal)\b",
    re.I,
)
_METHOD_PATTERNS: dict[MfaRequirementMethod, re.Pattern[str]] = {
    "totp": re.compile(r"\b(?:totp|time[- ]based otp|authenticator app|google authenticator|authy)\b", re.I),
    "sms_otp": re.compile(r"\b(?:sms otp|sms code|text message code|texted code|phone otp)\b", re.I),
    "email_otp": re.compile(r"\b(?:email otp|email code|emailed code|magic code|one[- ]time email)\b", re.I),
    "webauthn_passkey": re.compile(r"\b(?:webauthn|passkeys?|security keys?|fido2|u2f|platform authenticator)\b", re.I),
    "backup_codes": re.compile(r"\b(?:backup codes?|recovery codes?|scratch codes?)\b", re.I),
    "step_up": re.compile(r"\b(?:step[- ]up|re[- ]authenticate|reauthenticate|additional challenge|risk[- ]based challenge)\b", re.I),
    "enrollment": re.compile(r"\b(?:mfa enrollment|factor enrollment|enroll(?:ment)?|setup mfa|set up mfa|register a factor)\b", re.I),
    "recovery": re.compile(r"\b(?:account recovery|mfa recovery|factor reset|reset mfa|lost device|recover access|recovery)\b", re.I),
    "remembered_devices": re.compile(r"\b(?:remembered devices?|trusted devices?|remember this device|device trust|skip mfa for)\b", re.I),
    "admin_enforcement": re.compile(r"\b(?:admin enforcement|admins? (?:must|shall|can) enforce|mandatory mfa|require mfa for admins?|role[- ]based mfa)\b", re.I),
}
_TRIGGER_RE = re.compile(
    r"\b(?:on|when|whenever|before|after|during|for)\s+"
    r"((?:login|sign[- ]?in|admin access|admin console|password reset|new device|"
    r"risky login|high[- ]risk action|payment change|profile change|privileged action|"
    r"role change|session renewal|sensitive export|api key creation)[^.;\n]*)",
    re.I,
)
_AUDIENCE_RE = re.compile(
    r"\b(?:for|applies to|required for|enforced for|available to)\s+(?:the\s+)?"
    r"((?:admins?|administrators?|owners?|operators?|support agents?|employees?|staff|"
    r"all users|customers?|enterprise users?|privileged users?|new users|existing users)[^.;,\n]*)",
    re.I,
)
_SUBJECT_AUDIENCE_RE = re.compile(
    r"^\s*((?:admins?|administrators?|owners?|operators?|support agents?|employees?|staff|"
    r"all users|customers?|enterprise users?|privileged users?|new users|existing users))\b"
    r"\s+(?:must|shall|required|requires?|need|needs|should|can|may)\b",
    re.I,
)
_FALLBACK_RE = re.compile(
    r"\b(?:fallback|backup codes?|recovery codes?|account recovery|recover access|lost device|"
    r"support reset|factor reset|break glass|email fallback|sms fallback|recovery flow|recovery)\b[^.;\n]*",
    re.I,
)
_EVIDENCE_RE = re.compile(
    r"\b(?:audit|logged|logging|evidence|report|attestation|admin report|security event|"
    r"export|compliance|proof)\b[^.;\n]*",
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
class SourceMfaRequirement:
    """One source-backed MFA requirement."""

    source_brief_id: str | None
    method: MfaRequirementMethod
    trigger: str | None = None
    audience: str | None = None
    fallback_recovery: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field: str | None = None
    confidence: MfaRequirementConfidence = "medium"
    planning_note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "method": self.method,
            "trigger": self.trigger,
            "audience": self.audience,
            "fallback_recovery": self.fallback_recovery,
            "evidence": list(self.evidence),
            "source_field": self.source_field,
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceMfaRequirementsReport:
    """Source-level MFA requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceMfaRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceMfaRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceMfaRequirement, ...]:
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
        """Return MFA requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source MFA Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        method_counts = self.summary.get("method_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Method counts: "
            + ", ".join(f"{method} {method_counts.get(method, 0)}" for method in _METHOD_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No MFA requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Method | Trigger | Audience | Fallback/Recovery | Source Field | Confidence | Planning Note | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.method} | "
                f"{_markdown_cell(requirement.trigger or '')} | "
                f"{_markdown_cell(requirement.audience or '')} | "
                f"{_markdown_cell(requirement.fallback_recovery or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.planning_note or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_mfa_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMfaRequirementsReport:
    """Extract source-level MFA requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceMfaRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_mfa_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMfaRequirementsReport:
    """Compatibility alias for building an MFA requirements report."""
    return build_source_mfa_requirements(source)


def generate_source_mfa_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMfaRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_mfa_requirements(source)


def derive_source_mfa_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceMfaRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_mfa_requirements(source)


def summarize_source_mfa_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceMfaRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted MFA requirements."""
    if isinstance(source_or_result, SourceMfaRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_mfa_requirements(source_or_result).summary


def source_mfa_requirements_to_dict(report: SourceMfaRequirementsReport) -> dict[str, Any]:
    """Serialize an MFA requirements report to a plain dictionary."""
    return report.to_dict()


source_mfa_requirements_to_dict.__test__ = False


def source_mfa_requirements_to_dicts(
    requirements: tuple[SourceMfaRequirement, ...] | list[SourceMfaRequirement] | SourceMfaRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize MFA requirement records to dictionaries."""
    if isinstance(requirements, SourceMfaRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_mfa_requirements_to_dicts.__test__ = False


def source_mfa_requirements_to_markdown(report: SourceMfaRequirementsReport) -> str:
    """Render an MFA requirements report as Markdown."""
    return report.to_markdown()


source_mfa_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    method: MfaRequirementMethod
    trigger: str | None
    audience: str | None
    fallback_recovery: str | None
    source_field: str
    evidence: str
    confidence: MfaRequirementConfidence


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
    if isinstance(
        source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)
    ) or hasattr(source, "model_dump"):
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
            if _NO_MFA_RE.search(searchable):
                continue
            method_searchable = _structured_method_text(segment.text) or searchable
            methods = [method for method in _METHOD_ORDER if _METHOD_PATTERNS[method].search(method_searchable)]
            if not methods and re.search(r"\b(?:mfa|multi[- ]?factor|two[- ]?factor|2fa)\b", searchable, re.I):
                methods = ["enrollment"]
            if not methods or not _is_requirement(segment):
                continue
            for method in _dedupe(methods):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        method=method,
                        trigger=_trigger(segment.text),
                        audience=_audience(segment.text),
                        fallback_recovery=_fallback_recovery(segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceMfaRequirement]:
    grouped: dict[tuple[str | None, MfaRequirementMethod], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.method), []).append(candidate)

    requirements: list[SourceMfaRequirement] = []
    for (source_brief_id, method), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceMfaRequirement(
                source_brief_id=source_brief_id,
                method=method,
                trigger=_joined_details(item.trigger for item in items),
                audience=_joined_details(item.audience for item in items),
                fallback_recovery=_joined_details(item.fallback_recovery for item in items),
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                source_field=best.source_field,
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                planning_note=_PLANNING_NOTES[method],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _METHOD_ORDER.index(requirement.method),
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
        "mfa",
        "mfa_requirements",
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
                or _MFA_CONTEXT_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _METHOD_PATTERNS.values())
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
            section_context = inherited_context or bool(_MFA_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NO_MFA_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text and not _NO_MFA_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NO_MFA_RE.search(searchable) or not _MFA_CONTEXT_RE.search(searchable):
        return False
    return bool(
        _REQUIREMENT_RE.search(segment.text)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
        or _TRIGGER_RE.search(searchable)
        or _FALLBACK_RE.search(searchable)
        or _EVIDENCE_RE.search(searchable)
    )


def _confidence(segment: _Segment) -> MfaRequirementConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    has_detail = any((_TRIGGER_RE.search(searchable), _AUDIENCE_RE.search(searchable), _FALLBACK_RE.search(searchable), _EVIDENCE_RE.search(searchable)))
    if (_REQUIREMENT_RE.search(segment.text) or segment.section_context) and has_detail:
        return "high"
    if segment.section_context or _REQUIREMENT_RE.search(segment.text) or has_detail:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceMfaRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "method_counts": {
            method: sum(1 for requirement in requirements if requirement.method == method)
            for method in _METHOD_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "methods": [requirement.method for requirement in requirements],
    }


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    return bool(
        keys
        & {
            "method",
            "mfa_method",
            "factor",
            "trigger",
            "audience",
            "role",
            "fallback",
            "recovery",
            "backup_codes",
            "evidence",
            "policy",
            "enforcement",
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
        "mfa",
        "mfa_requirements",
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


def _trigger(text: str) -> str | None:
    return _labeled_detail("trigger", text) or _match_detail(_TRIGGER_RE, text)


def _audience(text: str) -> str | None:
    return _labeled_detail("audience|role", text) or _match_detail(_AUDIENCE_RE, text) or _match_detail(_SUBJECT_AUDIENCE_RE, text)


def _fallback_recovery(text: str) -> str | None:
    return _labeled_detail("fallback|recovery", text) or _match_detail(_FALLBACK_RE, text)


def _structured_method_text(text: str) -> str | None:
    if match := re.search(r"(?:^|;\s*)(?:method|mfa_method|factor)\s*:\s*([^;]+)", text, re.I):
        return match.group(1)
    return None


def _joined_details(values: Iterable[str | None]) -> str | None:
    details = _dedupe(value for value in values if value)
    return ", ".join(details) if details else None


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, int, int, str]:
    detail_count = sum(bool(value) for value in (candidate.trigger, candidate.audience, candidate.fallback_recovery))
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
    "MfaRequirementConfidence",
    "MfaRequirementMethod",
    "SourceMfaRequirement",
    "SourceMfaRequirementsReport",
    "build_source_mfa_requirements",
    "derive_source_mfa_requirements",
    "extract_source_mfa_requirements",
    "generate_source_mfa_requirements",
    "source_mfa_requirements_to_dict",
    "source_mfa_requirements_to_dicts",
    "source_mfa_requirements_to_markdown",
    "summarize_source_mfa_requirements",
]
