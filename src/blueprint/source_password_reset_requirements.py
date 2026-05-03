"""Extract source-level password reset and account recovery requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


PasswordResetCategory = Literal[
    "password_reset",
    "token_lifetime",
    "one_time_token",
    "email_verification",
    "mfa_recovery",
    "lockout",
    "support_recovery",
    "audit_trail",
    "abuse_prevention",
]
PasswordResetConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[PasswordResetCategory, ...] = (
    "password_reset",
    "token_lifetime",
    "one_time_token",
    "email_verification",
    "mfa_recovery",
    "lockout",
    "support_recovery",
    "audit_trail",
    "abuse_prevention",
)
_CONFIDENCE_ORDER: dict[PasswordResetConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_RESET_CONTEXT_RE = re.compile(
    r"\b(?:password reset|reset password|forgot password|forgotten password|account recovery|"
    r"recover account|recover access|credential recovery|login recovery|password recovery|"
    r"reset links?|reset tokens?|recovery tokens?|email verification|verify email|"
    r"mfa recovery|multi[- ]?factor recovery|2fa recovery|authenticator recovery|backup codes?|"
    r"lost device|account lock(?:out|ed)|lock(?:ed)? out|failed reset attempts?|"
    r"support[- ]assisted recovery|support recovery|manual recovery|identity verification|"
    r"recovery audit|security event|abuse prevention|rate limit|throttl(?:e|ed|ing))\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:password|reset|forgot|recovery|recover|account[_ -]?recovery|token|expiry|expiration|"
    r"ttl|lifetime|one[_ -]?time|single[_ -]?use|email[_ -]?verification|verify[_ -]?email|"
    r"mfa|2fa|multi[_ -]?factor|authenticator|backup[_ -]?code|lockout|lock[_ -]?out|"
    r"support|manual|identity|audit|security|abuse|rate[_ -]?limit|throttle|requirements?|"
    r"acceptance|criteria|definition[-_ ]?of[-_ ]?done|metadata|source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|send|email|verify|expire|expires?|invalidate|"
    r"single[- ]?use|one[- ]?time|lock|unlock|throttle|rate[- ]?limit|challenge|"
    r"audit|log|record|track|notify|review|approve|escalate|acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:passwords?|password reset|forgot password|account recovery|recovery flows?|"
    r"mfa recovery|support recovery|reset tokens?|email verification)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:passwords?|password reset|forgot password|account recovery|recovery flows?|"
    r"mfa recovery|support recovery|reset tokens?|email verification)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_ACCOUNT_RE = re.compile(
    r"\b(?:no password|no passwords|passwordless only|no account recovery|without account recovery|"
    r"no recovery flow|account recovery is out of scope|password reset is out of scope)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:seconds?|minutes?|mins?|hours?|hrs?|days?)|single[- ]?use|one[- ]?time|"
    r"reset link|reset token|recovery token|email verification|backup codes?|support ticket|"
    r"identity verification|audit log|security event|rate limit|throttle|captcha|lockout)\b",
    re.I,
)
_DURATION_RE = re.compile(r"\b\d+\s*(?:seconds?|minutes?|mins?|hours?|hrs?|days?)\b", re.I)
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
    "domain",
    "status",
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
    "password_reset",
    "account_recovery",
    "recovery",
    "mfa",
    "support",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[PasswordResetCategory, re.Pattern[str]] = {
    "password_reset": re.compile(
        r"\b(?:password reset|reset password|forgot password|forgotten password|password recovery|"
        r"reset flow|reset links?|reset emails?|change forgotten password)\b",
        re.I,
    ),
    "token_lifetime": re.compile(
        r"\b(?:token lifetime|token ttl|reset token expir(?:y|ation)|recovery token expir(?:y|ation)|"
        r"link expir(?:y|ation)|expires? (?:after|in)|valid for|validity window|"
        r"\d+\s*(?:seconds?|minutes?|mins?|hours?|hrs?|days?))\b",
        re.I,
    ),
    "one_time_token": re.compile(
        r"\b(?:one[- ]?time tokens?|single[- ]?use tokens?|one[- ]?time links?|single[- ]?use links?|"
        r"one[- ]?time reset|single[- ]?use [^.;,\n]{0,40}tokens?|"
        r"one[- ]?time [^.;,\n]{0,40}tokens?|invalidate(?:d)? after use|cannot be reused|"
        r"replay protection|token reuse)\b",
        re.I,
    ),
    "email_verification": re.compile(
        r"\b(?:email verification|verify email|verified email|confirm email|email ownership|"
        r"send reset email|reset email|email link|deliver reset link)\b",
        re.I,
    ),
    "mfa_recovery": re.compile(
        r"\b(?:mfa recovery|multi[- ]?factor recovery|2fa recovery|authenticator recovery|"
        r"lost authenticator|lost device|backup codes?|recovery codes?|factor reset|mfa reset)\b",
        re.I,
    ),
    "lockout": re.compile(
        r"\b(?:account lock(?:out|ed)|lock(?:ed)? out|reset lockout|temporary lockout|"
        r"too many reset attempts|failed reset attempts|attempt limit|cooldown|unlock)\b",
        re.I,
    ),
    "support_recovery": re.compile(
        r"\b(?:support[- ]assisted recovery|support recovery|support reset|manual recovery|"
        r"help desk recovery|support ticket|agent assisted|identity verification|manual identity review|"
        r"support approval|escalation)\b",
        re.I,
    ),
    "audit_trail": re.compile(
        r"\b(?:audit trail|audit log|audited|logged|logging|security events?|recovery events?|"
        r"reset events?|admin report|evidence|record recovery|track reset)\b",
        re.I,
    ),
    "abuse_prevention": re.compile(
        r"\b(?:abuse prevention|rate limits?|rate limiting|throttl(?:e|es|ed|ing)|captcha|"
        r"bot protection|enumeration protection|anti[- ]?enumeration|brute force|credential stuffing|"
        r"resend limits?|attempt limits?|ip limits?)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[PasswordResetCategory, tuple[str, ...]] = {
    "password_reset": ("identity", "backend"),
    "token_lifetime": ("identity", "security"),
    "one_time_token": ("identity", "security"),
    "email_verification": ("identity", "lifecycle_messaging"),
    "mfa_recovery": ("identity", "security", "support"),
    "lockout": ("identity", "security"),
    "support_recovery": ("support", "security", "identity"),
    "audit_trail": ("security", "compliance"),
    "abuse_prevention": ("security", "identity"),
}
_PLAN_IMPACTS: dict[PasswordResetCategory, tuple[str, ...]] = {
    "password_reset": ("Define reset request, delivery, verification, and password update states.",),
    "token_lifetime": ("Specify reset token TTL, expiry messaging, renewal behavior, and cleanup.",),
    "one_time_token": ("Enforce single-use reset tokens with replay detection and invalidation after success.",),
    "email_verification": ("Confirm email ownership and delivery requirements for reset and recovery messages.",),
    "mfa_recovery": ("Design MFA loss, backup code, factor reset, and account takeover safeguards.",),
    "lockout": ("Define lockout thresholds, cooldowns, unlock flows, and user/support messaging.",),
    "support_recovery": ("Document support-assisted identity checks, approvals, queues, and escalation handling.",),
    "audit_trail": ("Record reset, recovery, lockout, support, and MFA recovery events for review.",),
    "abuse_prevention": ("Add throttles, CAPTCHA or risk controls, enumeration protection, and monitoring.",),
}


@dataclass(frozen=True, slots=True)
class SourcePasswordResetRequirement:
    """One source-backed password reset or recovery requirement."""

    category: PasswordResetCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: PasswordResetConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> PasswordResetCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> PasswordResetCategory:
        """Compatibility view for extractors that expose concern naming."""
        return self.category

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owners": list(self.suggested_owners),
            "suggested_plan_impacts": list(self.suggested_plan_impacts),
        }


@dataclass(frozen=True, slots=True)
class SourcePasswordResetRequirementsReport:
    """Source-level password reset and account recovery requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourcePasswordResetRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourcePasswordResetRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourcePasswordResetRequirement, ...]:
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
        """Return password reset requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Password Reset Requirements Report"
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
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source password reset requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Value | Confidence | Source Field | Owners | Evidence | Suggested Plan Impacts |",
                "| --- | --- | --- | --- | --- | --- | --- |",
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
                f"{_markdown_cell('; '.join(requirement.suggested_plan_impacts))} |"
            )
        return "\n".join(lines)


def build_source_password_reset_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourcePasswordResetRequirementsReport:
    """Build a password reset requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    requirements = () if _has_global_no_scope(payload) else tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourcePasswordResetRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_password_reset_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourcePasswordResetRequirementsReport
        | str
        | object
    ),
) -> SourcePasswordResetRequirementsReport | dict[str, Any]:
    """Compatibility helper for callers that use summarize_* naming."""
    if isinstance(source, SourcePasswordResetRequirementsReport):
        return dict(source.summary)
    return build_source_password_reset_requirements(source)


def derive_source_password_reset_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourcePasswordResetRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_password_reset_requirements(source)


def generate_source_password_reset_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourcePasswordResetRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_password_reset_requirements(source)


def extract_source_password_reset_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourcePasswordResetRequirement, ...]:
    """Return password reset requirement records from brief-shaped input."""
    return build_source_password_reset_requirements(source).requirements


def source_password_reset_requirements_to_dict(report: SourcePasswordResetRequirementsReport) -> dict[str, Any]:
    """Serialize a password reset requirements report to a plain dictionary."""
    return report.to_dict()


source_password_reset_requirements_to_dict.__test__ = False


def source_password_reset_requirements_to_dicts(
    requirements: (
        tuple[SourcePasswordResetRequirement, ...]
        | list[SourcePasswordResetRequirement]
        | SourcePasswordResetRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize password reset requirement records to dictionaries."""
    if isinstance(requirements, SourcePasswordResetRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_password_reset_requirements_to_dicts.__test__ = False


def source_password_reset_requirements_to_markdown(report: SourcePasswordResetRequirementsReport) -> str:
    """Render a password reset requirements report as Markdown."""
    return report.to_markdown()


source_password_reset_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: PasswordResetCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: PasswordResetConfidence


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
        if _NO_ACCOUNT_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourcePasswordResetRequirement]:
    grouped: dict[PasswordResetCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourcePasswordResetRequirement] = []
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
            SourcePasswordResetRequirement(
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
                suggested_plan_impacts=_PLAN_IMPACTS[category],
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
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _RESET_CONTEXT_RE.search(key_text)
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
                _RESET_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
                if _NEGATED_SCOPE_RE.search(part) and _RESET_CONTEXT_RE.search(part)
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
    if _NO_ACCOUNT_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if not (_RESET_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _RESET_CONTEXT_RE.search(segment.text)
        and re.search(r"\b(?:sent|expired|invalidated|verified|locked|audited|throttled|recovered)\b", segment.text, re.I)
    )


def _value(category: PasswordResetCategory, text: str) -> str | None:
    if category == "token_lifetime":
        if match := re.search(
            r"\b(?P<value>(?:after|within|for|valid for|expires? after|expires? in)?\s*"
            r"\d+\s*(?:seconds?|minutes?|mins?|hours?|hrs?|days?))\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if category == "one_time_token":
        if match := re.search(r"\b(?P<value>single[- ]?use|one[- ]?time|cannot be reused|invalidate(?:d)? after use)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "email_verification":
        if match := re.search(r"\b(?P<value>email verification|verify email|reset email|email link|email ownership)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "mfa_recovery":
        if match := re.search(r"\b(?P<value>backup codes?|recovery codes?|lost device|factor reset)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(r"\b(?P<value>mfa recovery|2fa recovery)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "lockout":
        if match := re.search(r"\b(?P<value>\d+\s*(?:attempts?|tries)|cooldown|temporary lockout|account lockout|unlock)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "support_recovery":
        if match := re.search(r"\b(?P<value>support ticket|support recovery|support reset|manual recovery|identity verification|support approval)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "audit_trail":
        if match := re.search(r"\b(?P<value>audit trail|audit log|security events?|recovery events?|reset events?)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "abuse_prevention":
        if match := re.search(r"\b(?P<value>rate limits?|throttl(?:e|ing)|captcha|enumeration protection|attempt limits?|ip limits?)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "password_reset":
        if match := re.search(r"\b(?P<value>password reset|forgot password|reset link|reset token|password recovery)\b", text, re.I):
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


def _confidence(segment: _Segment) -> PasswordResetConfidence:
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
                "authentication",
                "auth",
                "security",
                "password",
                "recovery",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _RESET_CONTEXT_RE.search(searchable):
        return "medium"
    if _RESET_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(requirements: tuple[SourcePasswordResetRequirement, ...]) -> dict[str, Any]:
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
        "status": "ready_for_planning" if requirements else "no_password_reset_language",
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
        "password_reset",
        "account_recovery",
        "recovery",
        "mfa",
        "support",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: PasswordResetCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[PasswordResetCategory, tuple[str, ...]] = {
        "password_reset": ("password", "reset", "forgot"),
        "token_lifetime": ("ttl", "lifetime", "expiry", "expiration"),
        "one_time_token": ("one time", "single use", "token"),
        "email_verification": ("email", "verification"),
        "mfa_recovery": ("mfa", "2fa", "factor", "backup"),
        "lockout": ("lockout", "lock", "cooldown"),
        "support_recovery": ("support", "manual", "identity"),
        "audit_trail": ("audit", "event", "log"),
        "abuse_prevention": ("abuse", "rate", "throttle", "captcha"),
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
    "PasswordResetCategory",
    "PasswordResetConfidence",
    "SourcePasswordResetRequirement",
    "SourcePasswordResetRequirementsReport",
    "build_source_password_reset_requirements",
    "derive_source_password_reset_requirements",
    "extract_source_password_reset_requirements",
    "generate_source_password_reset_requirements",
    "summarize_source_password_reset_requirements",
    "source_password_reset_requirements_to_dict",
    "source_password_reset_requirements_to_dicts",
    "source_password_reset_requirements_to_markdown",
]
