"""Extract source-level password and credential policy requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


PasswordPolicyType = Literal[
    "minimum_length",
    "complexity",
    "rotation",
    "reuse_history",
    "breach_check",
    "reset_flow",
    "lockout",
    "mfa_adjacent",
    "admin_enforced_policy",
]
PasswordPolicyConfidence = Literal["high", "medium", "low"]

_POLICY_ORDER: tuple[PasswordPolicyType, ...] = (
    "minimum_length",
    "complexity",
    "rotation",
    "reuse_history",
    "breach_check",
    "reset_flow",
    "lockout",
    "mfa_adjacent",
    "admin_enforced_policy",
)
_CONFIDENCE_ORDER: dict[PasswordPolicyConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"enforce|prevent|reject|block|expire|rotate|lock|limit|validate|verify|check|"
    r"reset|recover|configure|admin(?:istrator)?s? can|policy)\b",
    re.I,
)
_PASSWORD_CONTEXT_RE = re.compile(
    r"\b(?:passwords?|passphrases?|credentials?|credential policy|password policy|"
    r"login|log in|sign in|signin|authentication|auth|account security|account recovery|"
    r"forgot password|reset password|password reset|passwordless|mfa|2fa|multi[- ]factor|"
    r"step[- ]up|reauth(?:entication)?|failed attempts?|lockout|locked accounts?|"
    r"breached passwords?|compromised passwords?|known leaked|credential stuffing)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:password|credential|auth|authentication|login|signin|sign[-_ ]?in|security|"
    r"account|identity|access|mfa|2fa|reset|recovery|lockout|policy|requirements?|"
    r"constraints?|acceptance|criteria|metadata|source[-_ ]?payload)",
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
    "non_goals",
    "assumptions",
    "success_criteria",
    "acceptance_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "security",
    "auth",
    "authentication",
    "identity",
    "account_security",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_POLICY_PATTERNS: dict[PasswordPolicyType, re.Pattern[str]] = {
    "minimum_length": re.compile(
        r"\b(?:(?:minimum|min\.?|at least|>=|no fewer than)\s*\d+\s*(?:characters?|chars?)|"
        r"\d+\s*(?:characters?|chars?)\s*(?:minimum|min\.?|required)|"
        r"min(?:imum)?(?: password)?[_ -]?length(?: of)?\s*\d+\s*(?:characters?|chars?)?|"
        r"min(?:imum)?[_ -]?length)\b",
        re.I,
    ),
    "complexity": re.compile(
        r"\b(?:complexity|complex passwords?|upper(?:case)?|lower(?:case)?|numbers?|digits?|"
        r"special characters?|symbols?|alphanumeric|character classes?|mixed case)\b",
        re.I,
    ),
    "rotation": re.compile(
        r"\b(?:password rotation|rotate passwords?|rotation period|password expiry|password expiration|"
        r"expire passwords?|force password change|change passwords? every|temporary passwords? expire)\b",
        re.I,
    ),
    "reuse_history": re.compile(
        r"\b(?:password reuse|reuse previous passwords?|reusing passwords?|password history|"
        r"last\s+\d+\s+passwords?|previous\s+\d+\s+passwords?|recent passwords?)\b",
        re.I,
    ),
    "breach_check": re.compile(
        r"\b(?:breached passwords?|compromised passwords?|known leaked passwords?|leaked credentials?|"
        r"credential stuffing|have ?i ?been ?pwned|pwned passwords?|common passwords?|dictionary passwords?)\b",
        re.I,
    ),
    "reset_flow": re.compile(
        r"\b(?:password reset|reset password|forgot password|account recovery|recovery link|"
        r"reset token|reset link|one[- ]time reset|password recovery)\b",
        re.I,
    ),
    "lockout": re.compile(
        r"\b(?:lockout|lock out|locked account|failed login attempts?|failed sign[- ]?in attempts?|"
        r"too many attempts|attempt limit|login throttl(?:e|ing)|rate limit login|"
        r"brute force|credential stuffing protection)\b",
        re.I,
    ),
    "mfa_adjacent": re.compile(
        r"\b(?:(?:mfa|2fa|multi[- ]factor|step[- ]up|reauth(?:entication)?).{0,80}"
        r"(?:password|credential|reset|change)|(?:password|credential|reset|change).{0,80}"
        r"(?:mfa|2fa|multi[- ]factor|step[- ]up|reauth(?:entication)?))\b",
        re.I,
    ),
    "admin_enforced_policy": re.compile(
        r"\b(?:admin[- ]enforced|administrator[- ]enforced|admins? can (?:configure|enforce|set)|"
        r"organization password policy|org password policy|tenant password policy|workspace password policy|"
        r"central(?:ly)? enforced password policy|policy enforced by admins?)\b",
        re.I,
    ),
}
_UNRESOLVED_QUESTIONS: dict[PasswordPolicyType, tuple[str, ...]] = {
    "minimum_length": ("Confirm the exact minimum length and whether passphrases have different rules.",),
    "complexity": ("Confirm required character classes and whether common-password checks replace complexity rules.",),
    "rotation": ("Confirm rotation interval, exemptions, and whether rotation applies to all users or only privileged roles.",),
    "reuse_history": ("Confirm password history depth and how long previous password hashes are retained.",),
    "breach_check": ("Confirm breach corpus/provider, check timing, and failure messaging.",),
    "reset_flow": ("Confirm reset token expiry, delivery channels, replay handling, and post-reset session behavior.",),
    "lockout": ("Confirm attempt threshold, lockout duration, unlock path, and monitoring requirements.",),
    "mfa_adjacent": ("Confirm when MFA or reauthentication is required for password-sensitive actions.",),
    "admin_enforced_policy": ("Confirm policy scope, admin override behavior, defaults, and audit logging.",),
}
_PLAN_IMPACTS: dict[PasswordPolicyType, tuple[str, ...]] = {
    "minimum_length": ("Add validation rules to signup, password change, reset, and admin policy flows.",),
    "complexity": ("Add shared password validation and localized error copy for complexity failures.",),
    "rotation": ("Plan expiry tracking, forced-change UX, background reminders, and privileged-account handling.",),
    "reuse_history": ("Store password history safely and validate new passwords against retained hashes.",),
    "breach_check": ("Integrate breached-password screening into credential creation and reset paths.",),
    "reset_flow": ("Design reset token lifecycle, notification delivery, abuse limits, and session invalidation.",),
    "lockout": ("Add failed-attempt counters, throttling or lockout state, unlock flows, and support visibility.",),
    "mfa_adjacent": ("Add step-up or reauthentication checks around password changes and recovery actions.",),
    "admin_enforced_policy": ("Expose policy configuration, enforcement points, defaults, and admin audit evidence.",),
}


@dataclass(frozen=True, slots=True)
class SourcePasswordPolicyRequirement:
    """One source-backed password or credential policy requirement."""

    policy_type: PasswordPolicyType
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: PasswordPolicyConfidence = "medium"
    value: str | None = None
    unresolved_questions: tuple[str, ...] = field(default_factory=tuple)
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "policy_type": self.policy_type,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "unresolved_questions": list(self.unresolved_questions),
            "suggested_plan_impacts": list(self.suggested_plan_impacts),
        }


@dataclass(frozen=True, slots=True)
class SourcePasswordPolicyRequirementsReport:
    """Source-level password and credential policy requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourcePasswordPolicyRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourcePasswordPolicyRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [requirement.to_dict() for requirement in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return password policy requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Password Policy Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        policy_counts = self.summary.get("policy_type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Policy type counts: "
            + ", ".join(f"{policy} {policy_counts.get(policy, 0)}" for policy in _POLICY_ORDER),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source password policy requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Policy Type | Value | Confidence | Source Field | Evidence | Unresolved Questions | Suggested Plan Impacts |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.policy_type} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.unresolved_questions))} | "
                f"{_markdown_cell('; '.join(requirement.suggested_plan_impacts))} |"
            )
        return "\n".join(lines)


def build_source_password_policy_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourcePasswordPolicyRequirementsReport:
    """Build a password policy requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourcePasswordPolicyRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_password_policy_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourcePasswordPolicyRequirementsReport
        | str
        | object
    ),
) -> SourcePasswordPolicyRequirementsReport | dict[str, Any]:
    """Compatibility helper for callers that use summarize_* naming."""
    if isinstance(source, SourcePasswordPolicyRequirementsReport):
        return source.summary
    return build_source_password_policy_requirements(source)


def derive_source_password_policy_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourcePasswordPolicyRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_password_policy_requirements(source)


def generate_source_password_policy_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourcePasswordPolicyRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_password_policy_requirements(source)


def extract_source_password_policy_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourcePasswordPolicyRequirement, ...]:
    """Return password policy requirement records from brief-shaped input."""
    return build_source_password_policy_requirements(source).requirements


def source_password_policy_requirements_to_dict(
    report: SourcePasswordPolicyRequirementsReport,
) -> dict[str, Any]:
    """Serialize a password policy requirements report to a plain dictionary."""
    return report.to_dict()


source_password_policy_requirements_to_dict.__test__ = False


def source_password_policy_requirements_to_dicts(
    requirements: (
        tuple[SourcePasswordPolicyRequirement, ...]
        | list[SourcePasswordPolicyRequirement]
        | SourcePasswordPolicyRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize password policy requirement records to dictionaries."""
    if isinstance(requirements, SourcePasswordPolicyRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_password_policy_requirements_to_dicts.__test__ = False


def source_password_policy_requirements_to_markdown(
    report: SourcePasswordPolicyRequirementsReport,
) -> str:
    """Render a password policy requirements report as Markdown."""
    return report.to_markdown()


source_password_policy_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    policy_type: PasswordPolicyType
    value: str | None
    source_field: str
    evidence: str
    confidence: PasswordPolicyConfidence


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
    return None, _object_payload(source)


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_field, segment in _candidate_segments(payload):
        if source_field == "title" and not _REQUIRED_RE.search(segment):
            continue
        searchable = f"{_field_words(source_field)} {segment}"
        if not _is_password_policy_signal(source_field, segment):
            continue
        policy_types = [
            policy for policy in _POLICY_ORDER if _POLICY_PATTERNS[policy].search(searchable)
        ]
        for policy_type in policy_types:
            candidates.append(
                _Candidate(
                    policy_type=policy_type,
                    value=_value(policy_type, segment),
                    source_field=source_field,
                    evidence=_evidence_snippet(source_field, segment),
                    confidence=_confidence(source_field, segment),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourcePasswordPolicyRequirement]:
    grouped: dict[PasswordPolicyType, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.policy_type, []).append(candidate)

    requirements: list[SourcePasswordPolicyRequirement] = []
    for policy_type in _POLICY_ORDER:
        items = grouped.get(policy_type, [])
        if not items:
            continue
        confidence = min(
            (item.confidence for item in items), key=lambda item: _CONFIDENCE_ORDER[item]
        )
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda field: (
                min(
                    _CONFIDENCE_ORDER[item.confidence]
                    for item in items
                    if item.source_field == field
                ),
                field.casefold(),
            ),
        )[0]
        requirements.append(
            SourcePasswordPolicyRequirement(
                policy_type=policy_type,
                source_field=source_field,
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:5],
                confidence=confidence,
                value=_best_value(items),
                unresolved_questions=_UNRESOLVED_QUESTIONS[policy_type],
                suggested_plan_impacts=_PLAN_IMPACTS[policy_type],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _CONFIDENCE_ORDER[requirement.confidence],
            _POLICY_ORDER.index(requirement.policy_type),
            requirement.value or "",
            requirement.source_field.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key], False)
    return [(field, segment) for field, segment in values if segment]


def _append_value(
    values: list[tuple[str, str]],
    source_field: str,
    value: Any,
    section_context: bool,
) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _PASSWORD_CONTEXT_RE.search(key_text)
            )
            if child_context and _PASSWORD_CONTEXT_RE.search(key_text):
                values.append((child_field, key_text))
            _append_value(values, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment in _segments(text):
            if field_context or _PASSWORD_CONTEXT_RE.search(segment):
                values.append((source_field, segment))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for raw_line in value.splitlines() or [value]:
        cleaned = _clean_text(raw_line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(raw_line) or _CHECKBOX_RE.match(raw_line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append(text)
    return segments


def _is_password_policy_signal(source_field: str, text: str) -> bool:
    searchable = f"{_field_words(source_field)} {text}"
    if not _PASSWORD_CONTEXT_RE.search(searchable):
        return False
    if any(pattern.search(searchable) for pattern in _POLICY_PATTERNS.values()):
        return True
    return bool(
        _REQUIRED_RE.search(text) and _STRUCTURED_FIELD_RE.search(_field_words(source_field))
    )


def _value(policy_type: PasswordPolicyType, text: str) -> str | None:
    if policy_type == "minimum_length":
        if match := re.search(
            r"\b(?P<value>(?:minimum|min\.?|at least|>=|no fewer than)?\s*\d+\s*(?:characters?|chars?))\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value"))
        if match := re.search(
            r"\b(?:min(?:imum)?(?: password)?[_ -]?length(?: of)?\s*)"
            r"(?P<value>\d+\s*(?:characters?|chars?))\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value"))
    if policy_type in {"rotation", "reset_flow", "lockout"}:
        if match := re.search(
            r"\b(?P<value>(?:after|within|under|less than|no more than|up to|every|for)?\s*"
            r"\d+(?:\.\d+)?\s*(?:seconds?|minutes?|mins?|hours?|days?|weeks?|months?))\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value"))
    if policy_type in {"reuse_history", "lockout"}:
        if match := re.search(
            r"\b(?P<value>(?:last|previous|after|within|under|no more than|up to)?\s*"
            r"\d+\s*(?:passwords?|attempts?|failed attempts?|failed login attempts?|"
            r"failed sign[- ]?in attempts?|logins?|sign[- ]?ins?))\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value"))
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    values = sorted(
        {item.value for item in items if item.value},
        key=lambda value: (0 if re.search(r"\d", value) else 1, len(value), value.casefold()),
    )
    return values[0] if values else None


def _confidence(source_field: str, text: str) -> PasswordPolicyConfidence:
    normalized_field = source_field.replace("-", "_").casefold()
    if _REQUIRED_RE.search(text) and any(
        marker in normalized_field
        for marker in (
            "acceptance_criteria",
            "definition_of_done",
            "success_criteria",
            "scope",
            "security",
            "auth",
            "authentication",
            "identity",
            "password",
            "credential",
            "policy",
        )
    ):
        return "high"
    if _REQUIRED_RE.search(text):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourcePasswordPolicyRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "policy_types": [requirement.policy_type for requirement in requirements],
        "policy_type_counts": {
            policy_type: sum(
                1 for requirement in requirements if requirement.policy_type == policy_type
            )
            for policy_type in _POLICY_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "status": "ready_for_planning" if requirements else "no_password_policy_language",
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
        "success_criteria",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "security",
        "auth",
        "authentication",
        "identity",
        "account_security",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
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
    return sorted(deduped, key=lambda item: item.casefold())


__all__ = [
    "PasswordPolicyConfidence",
    "PasswordPolicyType",
    "SourcePasswordPolicyRequirement",
    "SourcePasswordPolicyRequirementsReport",
    "build_source_password_policy_requirements",
    "derive_source_password_policy_requirements",
    "extract_source_password_policy_requirements",
    "generate_source_password_policy_requirements",
    "summarize_source_password_policy_requirements",
    "source_password_policy_requirements_to_dict",
    "source_password_policy_requirements_to_dicts",
    "source_password_policy_requirements_to_markdown",
]
