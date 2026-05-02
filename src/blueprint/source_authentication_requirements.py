"""Extract authentication requirements from source and implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceAuthenticationSurface = Literal[
    "login",
    "sso",
    "saml",
    "oidc",
    "mfa",
    "passkey",
    "password_reset",
    "session",
    "device_trust",
    "service_account",
    "anonymous_access",
]
SourceAuthenticationRequirementType = Literal[
    "login",
    "sso",
    "mfa",
    "passkey",
    "password_reset",
    "session",
    "service_account",
    "anonymous_access",
]
SourceAuthenticationRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SURFACE_ORDER: tuple[SourceAuthenticationSurface, ...] = (
    "login",
    "sso",
    "saml",
    "oidc",
    "mfa",
    "passkey",
    "password_reset",
    "session",
    "device_trust",
    "service_account",
    "anonymous_access",
)
_TYPE_ORDER: tuple[SourceAuthenticationRequirementType, ...] = (
    "login",
    "sso",
    "mfa",
    "passkey",
    "password_reset",
    "session",
    "service_account",
    "anonymous_access",
)
_CONFIDENCE_ORDER: dict[SourceAuthenticationRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_DIRECTIVE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|enable|allow|block|disable|enforce|acceptance|done when|before launch|"
    r"only if|cannot ship)\b",
    re.I,
)
_AUTH_CONTEXT_RE = re.compile(
    r"\b(?:auth(?:entication|n)?|login|log in|sign[- ]?in|account access|identity|"
    r"credential|session|sso|saml|oidc|mfa|2fa|passkey|password|service account|"
    r"anonymous|unauthenticated|guest access|device trust)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:auth(?:entication|n)?|login|identity|security|session|sso|saml|oidc|mfa|"
    r"passkey|password|service_account|anonymous|guest|requirements?|constraints?|"
    r"acceptance|criteria|goals?|source_payload|metadata)",
    re.I,
)
_IGNORED_FIELDS = {
    "id",
    "source_id",
    "source_brief_id",
    "source_project",
    "source_entity_type",
    "created_at",
    "updated_at",
}
_SURFACE_PATTERNS: dict[SourceAuthenticationSurface, re.Pattern[str]] = {
    "login": re.compile(r"\b(?:login|log in|sign[- ]?in|signin|account access|authentication)\b", re.I),
    "sso": re.compile(r"\b(?:sso|single sign[- ]?on|single sign on|federated login|federated auth)\b", re.I),
    "saml": re.compile(r"\b(?:saml|saml2|saml 2\.0|metadata xml|acs url|entity id)\b", re.I),
    "oidc": re.compile(r"\b(?:oidc|openid connect|oauth2? login|oauth 2\.0 login|issuer url|jwks)\b", re.I),
    "mfa": re.compile(
        r"\b(?:mfa|2fa|two[- ]factor|multi[- ]factor|multifactor|one[- ]time passcode|"
        r"otp|totp|authenticator app|sms code|step[- ]up auth)\b",
        re.I,
    ),
    "passkey": re.compile(r"\b(?:passkeys?|webauthn|fido2?|security keys?|biometric sign[- ]?in)\b", re.I),
    "password_reset": re.compile(
        r"\b(?:password reset|reset password|forgot password|password recovery|recovery email|"
        r"reset token|reset link|account recovery)\b",
        re.I,
    ),
    "session": re.compile(
        r"\b(?:session lifetime|session duration|session timeout|idle timeout|max session|"
        r"session expiry|session expiration|remember me|reauth(?:entication)?|reauthori[sz]e)\b",
        re.I,
    ),
    "device_trust": re.compile(
        r"\b(?:device trust|trusted devices?|remember(?:ed)? devices?|device posture|"
        r"device verification|managed devices?)\b",
        re.I,
    ),
    "service_account": re.compile(
        r"\b(?:service accounts?|machine[- ]to[- ]machine|m2m|client credentials|api keys?|"
        r"bot accounts?|non[- ]human users?|workload identity)\b",
        re.I,
    ),
    "anonymous_access": re.compile(
        r"\b(?:anonymous access|anonymous users?|unauthenticated access|unauthenticated users?|"
        r"public access|guest access|no login required|without (?:login|sign[- ]?in|authentication))\b",
        re.I,
    ),
}
_SURFACE_TO_TYPE: dict[SourceAuthenticationSurface, SourceAuthenticationRequirementType] = {
    "login": "login",
    "sso": "sso",
    "saml": "sso",
    "oidc": "sso",
    "mfa": "mfa",
    "passkey": "passkey",
    "password_reset": "password_reset",
    "session": "session",
    "device_trust": "session",
    "service_account": "service_account",
    "anonymous_access": "anonymous_access",
}
_ACTOR_RE = re.compile(
    r"\b(?P<actor>(?:enterprise\s+)?(?:admins?|administrators?|users?|members?|customers?|"
    r"employees?|contractors?|guests?|support agents?|developers?|operators?|owners?|"
    r"service accounts?|bots?|partners?)(?:\s+(?:and|or)\s+(?:admins?|users?|customers?|"
    r"guests?|service accounts?))?)\s+(?:must|shall|should|need(?:s)?|require(?:s)?|"
    r"can|cannot|may|use|login|sign[- ]?in|authenticate)\b",
    re.I,
)
_ACTOR_FOR_RE = re.compile(
    r"\b(?:for|by|to)\s+(?P<actor>(?:enterprise\s+)?(?:admins?|administrators?|users?|members?|"
    r"customers?|employees?|contractors?|guests?|support agents?|developers?|operators?|"
    r"owners?|service accounts?|bots?|partners?))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class SourceAuthenticationRequirement:
    """One source-backed authentication requirement."""

    source_id: str | None
    auth_surface: SourceAuthenticationSurface
    requirement_type: SourceAuthenticationRequirementType
    actor: str | None = None
    evidence: str = ""
    confidence: SourceAuthenticationRequirementConfidence = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "auth_surface": self.auth_surface,
            "requirement_type": self.requirement_type,
            "actor": self.actor,
            "evidence": self.evidence,
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class SourceAuthenticationRequirementsReport:
    """Source-level authentication requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceAuthenticationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAuthenticationRequirement, ...]:
        """Compatibility view matching extractors that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return authentication requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Authentication Requirements"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        type_counts = self.summary.get("type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
            "- Requirement type counts: "
            + ", ".join(f"{key} {type_counts.get(key, 0)}" for key in _TYPE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No authentication requirements were found in the source brief."])
            return "\n".join(lines)
        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source | Surface | Type | Actor | Confidence | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_id or '')} | "
                f"{requirement.auth_surface} | "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell(requirement.actor or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.evidence)} |"
            )
        return "\n".join(lines)


def build_source_authentication_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief | object]
        | str
        | object
    ),
) -> SourceAuthenticationRequirementsReport:
    """Extract authentication requirement records from SourceBrief-like input."""
    payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_payloads(payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_id) or "",
                _CONFIDENCE_ORDER[requirement.confidence],
                _SURFACE_ORDER.index(requirement.auth_surface),
                _TYPE_ORDER.index(requirement.requirement_type),
                requirement.actor or "",
                requirement.evidence.casefold(),
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in payloads if source_id)
    return SourceAuthenticationRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(payloads)),
    )


def summarize_source_authentication_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief | object]
        | str
        | object
    ),
) -> SourceAuthenticationRequirementsReport:
    """Compatibility helper for callers that use summarize_* naming."""
    return build_source_authentication_requirements(source)


def derive_source_authentication_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief | object]
        | str
        | object
    ),
) -> SourceAuthenticationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_authentication_requirements(source)


def generate_source_authentication_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief | object]
        | str
        | object
    ),
) -> SourceAuthenticationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_authentication_requirements(source)


def extract_source_authentication_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief | object]
        | str
        | object
    ),
) -> tuple[SourceAuthenticationRequirement, ...]:
    """Return authentication requirement records from brief-shaped input."""
    return build_source_authentication_requirements(source).requirements


def source_authentication_requirements_to_dict(
    report: SourceAuthenticationRequirementsReport,
) -> dict[str, Any]:
    """Serialize a source authentication requirements report to a plain dictionary."""
    return report.to_dict()


source_authentication_requirements_to_dict.__test__ = False


def source_authentication_requirements_to_dicts(
    requirements: (
        tuple[SourceAuthenticationRequirement, ...]
        | list[SourceAuthenticationRequirement]
        | SourceAuthenticationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize authentication requirement records to dictionaries."""
    if isinstance(requirements, SourceAuthenticationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_authentication_requirements_to_dicts.__test__ = False


def source_authentication_requirements_to_markdown(
    report: SourceAuthenticationRequirementsReport,
) -> str:
    """Render a source authentication requirements report as Markdown."""
    return report.to_markdown()


source_authentication_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_id: str | None
    auth_surface: SourceAuthenticationSurface
    requirement_type: SourceAuthenticationRequirementType
    actor: str | None
    evidence: str
    confidence: SourceAuthenticationRequirementConfidence


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief | object]
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
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
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
        return _source_id(source), dict(source)
    payload = _object_payload(source)
    return _source_id(payload), payload


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _candidates_for_payloads(
    payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_id, payload in payloads:
        if not payload:
            continue
        for source_field, segment in _candidate_segments(payload):
            surfaces = _surfaces(segment, source_field)
            if not surfaces:
                continue
            evidence = _evidence_snippet(source_field, segment)
            actor = _actor(segment)
            confidence = _confidence(segment, source_field)
            for surface in surfaces:
                candidates.append(
                    _Candidate(
                        source_id=source_id,
                        auth_surface=surface,
                        requirement_type=_SURFACE_TO_TYPE[surface],
                        actor=actor,
                        evidence=evidence,
                        confidence=confidence,
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceAuthenticationRequirement]:
    grouped: dict[
        tuple[str | None, SourceAuthenticationSurface, SourceAuthenticationRequirementType, str | None, str],
        list[_Candidate],
    ] = {}
    for candidate in candidates:
        grouped.setdefault(
            (
                candidate.source_id,
                candidate.auth_surface,
                candidate.requirement_type,
                _actor_key(candidate.actor),
                _dedupe_key(candidate.evidence),
            ),
            [],
        ).append(candidate)

    requirements: list[SourceAuthenticationRequirement] = []
    for (source_id, auth_surface, requirement_type, _, _), items in grouped.items():
        best = min(items, key=lambda item: _CONFIDENCE_ORDER[item.confidence])
        requirements.append(
            SourceAuthenticationRequirement(
                source_id=source_id,
                auth_surface=auth_surface,
                requirement_type=requirement_type,
                actor=best.actor,
                evidence=best.evidence,
                confidence=best.confidence,
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "problem_statement",
        "mvp_goal",
        "goals",
        "requirements",
        "constraints",
        "implementation_constraints",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "scope",
        "security",
        "identity",
        "authentication",
        "auth",
        "session",
        "implementation_notes",
        "validation_plan",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            if _any_signal(key_text):
                values.append((child_field, key_text))
            _append_value(values, child_field, child)
            if _any_signal(key_text) and not isinstance(child, (Mapping, list, tuple, set)):
                if text := _optional_text(child):
                    values.extend((child_field, segment) for segment in _segments(f"{key_text}: {text}"))
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        values.extend((source_field, segment) for segment in _segments(text))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for line in value.splitlines() or [value]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for sentence in parts:
            segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _surfaces(text: str, source_field: str) -> tuple[SourceAuthenticationSurface, ...]:
    surfaces = [surface for surface in _SURFACE_ORDER if _SURFACE_PATTERNS[surface].search(text)]
    field_text = source_field.replace("_", " ").replace("-", " ")
    for surface in _SURFACE_ORDER:
        if (
            surface not in surfaces
            and _SURFACE_PATTERNS[surface].search(field_text)
            and _authentication_context(text, source_field)
        ):
            surfaces.append(surface)
    if "login" in surfaces and len(surfaces) > 1:
        surfaces.remove("login")
    if "anonymous_access" in surfaces:
        surfaces = [surface for surface in surfaces if surface not in {"login"}]
    if "saml" in surfaces and "sso" not in surfaces:
        surfaces.insert(_SURFACE_ORDER.index("sso"), "sso")
    if "oidc" in surfaces and "sso" not in surfaces and re.search(r"\b(?:sso|single sign|federated)\b", text, re.I):
        surfaces.insert(_SURFACE_ORDER.index("sso"), "sso")
    return tuple(_dedupe(surfaces))


def _authentication_context(text: str, source_field: str) -> bool:
    return bool(
        _AUTH_CONTEXT_RE.search(text)
        or _STRUCTURED_FIELD_RE.search(source_field.replace("-", "_"))
        or _DIRECTIVE_RE.search(text)
    )


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in _SURFACE_PATTERNS.values())


def _actor(text: str) -> str | None:
    for pattern in (_ACTOR_RE, _ACTOR_FOR_RE):
        match = pattern.search(text)
        if match:
            return _clean_text(match.group("actor")).casefold()
    return None


def _confidence(text: str, source_field: str) -> SourceAuthenticationRequirementConfidence:
    field_text = source_field.replace("-", "_").casefold()
    if _DIRECTIVE_RE.search(text) or any(
        marker in field_text for marker in ("acceptance", "criteria", "constraint", "requirement", "definition_of_done")
    ):
        return "high"
    if _AUTH_CONTEXT_RE.search(text):
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceAuthenticationRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "requirement_types": [requirement.requirement_type for requirement in requirements],
        "auth_surfaces": [requirement.auth_surface for requirement in requirements],
        "type_counts": {
            requirement_type: sum(1 for requirement in requirements if requirement.requirement_type == requirement_type)
            for requirement_type in _TYPE_ORDER
        },
        "surface_counts": {
            surface: sum(1 for requirement in requirements if requirement.auth_surface == surface)
            for surface in _SURFACE_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "source_ids": sorted(_dedupe(requirement.source_id for requirement in requirements if requirement.source_id)),
    }


def _object_payload(value: object) -> dict[str, Any]:
    if isinstance(value, (bytes, bytearray)):
        return {}
    fields = (
        "id",
        "source_id",
        "source_brief_id",
        "title",
        "domain",
        "summary",
        "body",
        "description",
        "problem_statement",
        "mvp_goal",
        "goals",
        "requirements",
        "constraints",
        "implementation_constraints",
        "acceptance_criteria",
        "definition_of_done",
        "scope",
        "security",
        "identity",
        "authentication",
        "auth",
        "session",
        "implementation_notes",
        "validation_plan",
        "metadata",
        "brief_metadata",
        "source_payload",
        "source_links",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _dedupe_key(value: str) -> str:
    text = re.sub(r"^[^:]+:\s*", "", value)
    text = re.sub(r"\b(?:must|shall|should|is required to|are required to|requires?)\b", "", text, flags=re.I)
    text = re.sub(r"[^a-z0-9]+", " ", text.casefold())
    return _SPACE_RE.sub(" ", text).strip()


def _actor_key(value: str | None) -> str | None:
    return _SPACE_RE.sub(" ", value.casefold()).strip() if value else None


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    result: list[_T] = []
    seen: set[Any] = set()
    for value in values:
        key = value.casefold() if isinstance(value, str) else value
        if not value or key in seen:
            continue
        result.append(value)
        seen.add(key)
    return result


__all__ = [
    "SourceAuthenticationRequirement",
    "SourceAuthenticationRequirementConfidence",
    "SourceAuthenticationRequirementType",
    "SourceAuthenticationRequirementsReport",
    "SourceAuthenticationSurface",
    "build_source_authentication_requirements",
    "derive_source_authentication_requirements",
    "extract_source_authentication_requirements",
    "generate_source_authentication_requirements",
    "source_authentication_requirements_to_dict",
    "source_authentication_requirements_to_dicts",
    "source_authentication_requirements_to_markdown",
    "summarize_source_authentication_requirements",
]
