"""Extract single sign-on and identity-provider requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


SourceSSORequirementType = Literal[
    "sso",
    "saml",
    "oidc",
    "scim",
    "idp",
    "jit_provisioning",
    "group_mapping",
    "domain_verification",
    "logout",
    "session_lifetime",
]
SourceSSORequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[SourceSSORequirementType, ...] = (
    "sso",
    "saml",
    "oidc",
    "scim",
    "idp",
    "jit_provisioning",
    "group_mapping",
    "domain_verification",
    "logout",
    "session_lifetime",
)
_CONFIDENCE_ORDER: dict[SourceSSORequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but)\s+", re.I)
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|need(?:s)?|support|enable|allow|block|only if|"
    r"before launch|acceptance|done when)\b",
    re.I,
)
_IDENTITY_CONTEXT_RE = re.compile(
    r"\b(?:identity|identities|authentication|authn|login|sign[- ]?in|single sign[- ]?on|"
    r"sso|saml|oidc|scim|idp|directory|provision(?:ing)?|session|logout)\b",
    re.I,
)
_STRUCTURED_CONTEXT_FIELD_RE = re.compile(
    r"(?:requirement|requirements|acceptance|criteria|constraints?|risks?|metadata|"
    r"identity|auth|authentication|sso|security|compliance|enterprise|provisioning|"
    r"directory|session|login|logout)",
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
_TYPE_PATTERNS: dict[SourceSSORequirementType, re.Pattern[str]] = {
    "sso": re.compile(
        r"\b(?:sso|single sign[- ]?on|single sign on|federated login|"
        r"federated auth(?:entication)?)\b",
        re.I,
    ),
    "saml": re.compile(r"\b(?:saml|saml2|saml 2\.0|metadata xml|acs url|entity id)\b", re.I),
    "oidc": re.compile(
        r"\b(?:oidc|openid connect|openid|oauth2 login|oauth 2\.0 login|client id|"
        r"issuer url|jwks)\b",
        re.I,
    ),
    "scim": re.compile(r"\b(?:scim|scim 2\.0|directory sync|user provisioning)\b", re.I),
    "idp": re.compile(
        r"\b(?:idp|identity provider|okta|azure ad|microsoft entra|entra id|"
        r"google workspace|google apps|google identity|ping identity|onelogin)\b",
        re.I,
    ),
    "jit_provisioning": re.compile(
        r"\b(?:jit provisioning|just[- ]in[- ]time provisioning|just in time provisioning|"
        r"auto[- ]provision|auto provision|provision users? on first login|"
        r"create users? on login)\b",
        re.I,
    ),
    "group_mapping": re.compile(
        r"\b(?:group mapping|group mappings|map groups?|role mapping|attribute mapping|"
        r"claims? mapping|groups? claim|department mapping)\b",
        re.I,
    ),
    "domain_verification": re.compile(
        r"\b(?:domain verification|verified domain|verify domain|domain ownership|"
        r"email domain|allowed domains?|domain claim)\b",
        re.I,
    ),
    "logout": re.compile(
        r"\b(?:single logout|slo|idp[- ]initiated logout|sp[- ]initiated logout|"
        r"logout|log out|sign out|global sign[- ]out)\b",
        re.I,
    ),
    "session_lifetime": re.compile(
        r"\b(?:session lifetime|session duration|session timeout|idle timeout|"
        r"reauth(?:entication)?|reauthori[sz]e|remember me|max session|session expiry)\b",
        re.I,
    ),
}
_QUESTION_TEMPLATES: dict[SourceSSORequirementType, tuple[str, ...]] = {
    "sso": (
        "Which customer plans or tenants require SSO enforcement?",
        "Should password login remain available when SSO is enabled?",
    ),
    "saml": (
        "Which SAML bindings, metadata exchange, and certificate rotation behavior are required?",
        "Is IdP-initiated SAML login in scope?",
    ),
    "oidc": (
        "Which OIDC issuer, scopes, and claim mappings are required?",
        "How should key rotation and issuer discovery failures be handled?",
    ),
    "scim": (
        "Which SCIM user and group lifecycle operations are required?",
        "What should happen when SCIM deprovisioning conflicts with active ownership?",
    ),
    "idp": (
        "Which identity providers must be supported at launch?",
        "Are provider-specific setup guides or admin validation steps required?",
    ),
    "jit_provisioning": (
        "Which default role and workspace should just-in-time users receive?",
        "Should JIT provisioning be restricted by verified domains or invited users?",
    ),
    "group_mapping": (
        "Which IdP groups or claims map to application roles?",
        "How should missing, renamed, or conflicting group mappings be resolved?",
    ),
    "domain_verification": (
        "How will domain ownership be verified and rechecked?",
        "Can multiple tenants claim the same email domain?",
    ),
    "logout": (
        "Should logout terminate only the application session or also the IdP session?",
        "Are IdP-initiated and service-provider-initiated logout both required?",
    ),
    "session_lifetime": (
        "What maximum and idle session lifetimes are required for SSO users?",
        "Which events should force reauthentication?",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceSSORequirement:
    """One source-backed SSO or identity-provider requirement."""

    source_brief_id: str | None
    requirement_type: SourceSSORequirementType
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: SourceSSORequirementConfidence = "medium"
    recommended_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "matched_terms": list(self.matched_terms),
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "recommended_questions": list(self.recommended_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceSSORequirementsReport:
    """Brief-level SSO and identity-provider requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceSSORequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSSORequirement, ...]:
        """Compatibility view matching extractors that name findings records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [requirement.to_dict() for requirement in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return SSO requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]


def build_source_sso_requirements(
    source: Mapping[str, Any] | SourceBrief | object,
) -> SourceSSORequirementsReport:
    """Extract SSO and identity-provider requirement signals from a source brief."""
    source_brief_id, payload = _source_payload(source)
    grouped: dict[SourceSSORequirementType, dict[str, Any]] = {}
    for source_field, segment in _candidate_segments(payload):
        requirement_types = _requirement_types(segment, source_field)
        if not requirement_types:
            continue
        evidence = _evidence_snippet(source_field, segment)
        for requirement_type in requirement_types:
            bucket = grouped.setdefault(
                requirement_type,
                {"requirement_type": requirement_type, "matched_terms": [], "evidence": []},
            )
            bucket["matched_terms"].extend(_matched_terms(requirement_type, segment, source_field))
            bucket["evidence"].append(evidence)

    requirements = tuple(
        sorted(
            (
                _requirement_from_bucket(source_brief_id, bucket)
                for bucket in grouped.values()
                if bucket["evidence"]
            ),
            key=lambda requirement: (
                _CONFIDENCE_ORDER[requirement.confidence],
                _type_index(requirement.requirement_type),
                requirement.evidence,
            ),
        )
    )
    return SourceSSORequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_sso_requirements(
    source: Mapping[str, Any] | SourceBrief | object,
) -> SourceSSORequirementsReport:
    """Compatibility alias for building a source SSO requirements report."""
    return build_source_sso_requirements(source)


def source_sso_requirements_to_dict(report: SourceSSORequirementsReport) -> dict[str, Any]:
    """Serialize a source SSO requirements report to a plain dictionary."""
    return report.to_dict()


source_sso_requirements_to_dict.__test__ = False


def source_sso_requirements_to_dicts(
    requirements: tuple[SourceSSORequirement, ...] | list[SourceSSORequirement],
) -> list[dict[str, Any]]:
    """Serialize source SSO requirement records to dictionaries."""
    return [requirement.to_dict() for requirement in requirements]


source_sso_requirements_to_dicts.__test__ = False


def _requirement_from_bucket(
    source_brief_id: str | None,
    bucket: Mapping[str, Any],
) -> SourceSSORequirement:
    requirement_type = bucket["requirement_type"]
    matched_terms = tuple(sorted(_dedupe(_strings(bucket.get("matched_terms"))), key=str.casefold))
    evidence = tuple(sorted(_dedupe(_strings(bucket.get("evidence"))), key=str.casefold))[:5]
    evidence_text = " ".join((*matched_terms, *evidence))
    return SourceSSORequirement(
        source_brief_id=source_brief_id,
        requirement_type=requirement_type,
        matched_terms=matched_terms,
        evidence=evidence,
        confidence=_confidence(requirement_type, evidence_text, len(evidence)),
        recommended_questions=_QUESTION_TEMPLATES[requirement_type],
    )


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, SourceBrief):
        payload = source.model_dump(mode="python")
        return _source_brief_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _source_brief_id(payload), payload
    if isinstance(source, Mapping):
        try:
            payload = dict(SourceBrief.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
        return _source_brief_id(payload), payload
    if not isinstance(source, (str, bytes, bytearray)):
        payload = _object_payload(source)
        return _source_brief_id(payload), payload
    return None, {}


def _source_brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    segments: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "requirements",
        "constraints",
        "risks",
        "metadata",
        "brief_metadata",
        "identity",
        "authentication",
        "auth",
        "sso",
        "security",
        "acceptance_criteria",
        "implementation_notes",
    ):
        if field_name in payload:
            segments.extend(_field_segments(payload.get(field_name), field_name))
            visited.add(field_name)
    source_payload = (
        payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    )
    for field_name in (
        "requirements",
        "constraints",
        "risks",
        "metadata",
        "identity",
        "authentication",
        "auth",
        "sso",
        "security",
        "acceptance_criteria",
        "implementation_notes",
        "body",
        "description",
        "markdown",
    ):
        if field_name in source_payload:
            segments.extend(
                _field_segments(source_payload.get(field_name), f"source_payload.{field_name}")
            )
            visited.add(f"source_payload.{field_name}")
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or key == "source_payload" or str(key) in _IGNORED_FIELDS:
            continue
        segments.extend(_field_segments(payload[key], str(key)))
    for key in sorted(source_payload, key=lambda item: str(item)):
        field_name = f"source_payload.{key}"
        if field_name in visited or str(key) in _IGNORED_FIELDS:
            continue
        segments.extend(_field_segments(source_payload[key], field_name))
    return [(field, segment) for field, segment in segments if segment]


def _field_segments(value: Any, field_name: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        segments: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            source_field = f"{field_name}.{key}"
            key_text = str(key).replace("_", " ")
            if _any_signal(key_text):
                segments.extend((source_field, segment) for segment in _segments(key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                segments.extend(_field_segments(child, source_field))
            elif text := _optional_text(child):
                segments.extend((source_field, segment) for segment in _segments(text))
                if _any_signal(key_text):
                    segments.extend(
                        (source_field, segment) for segment in _segments(f"{key_text}: {text}")
                    )
        return segments
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        segments: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            segments.extend(_field_segments(item, f"{field_name}[{index}]"))
        return segments
    return [(field_name, segment) for segment in _segments(value)]


def _segments(value: Any) -> list[str]:
    text = _optional_text(value)
    if text is None:
        return []
    segments: list[str] = []
    for line in text.splitlines() or [text]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        sentence_parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else (
            _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for sentence in sentence_parts:
            segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _requirement_types(text: str, source_field: str) -> tuple[SourceSSORequirementType, ...]:
    types = [
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(text)
    ]
    field_text = source_field.replace("_", " ").replace("-", " ")
    for requirement_type in _TYPE_ORDER:
        if (
            requirement_type not in types
            and _TYPE_PATTERNS[requirement_type].search(field_text)
            and _identity_context(text, source_field)
        ):
            types.append(requirement_type)
    if "logout" in types and not _identity_context(text, source_field):
        types.remove("logout")
    return tuple(types)


def _matched_terms(
    requirement_type: SourceSSORequirementType,
    text: str,
    source_field: str,
) -> tuple[str, ...]:
    pattern = _TYPE_PATTERNS[requirement_type]
    terms = [_clean_text(match.group(0)) for match in pattern.finditer(text)]
    field_text = source_field.replace("_", " ").replace("-", " ")
    terms.extend(_clean_text(match.group(0)) for match in pattern.finditer(field_text))
    return tuple(_dedupe(terms))


def _confidence(
    requirement_type: SourceSSORequirementType,
    evidence_text: str,
    evidence_count: int,
) -> SourceSSORequirementConfidence:
    if _REQUIRED_RE.search(evidence_text) or requirement_type in {
        "saml",
        "oidc",
        "scim",
        "jit_provisioning",
        "domain_verification",
        "session_lifetime",
    }:
        return "high"
    if evidence_count > 1 or requirement_type in {"idp", "group_mapping", "logout"}:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceSSORequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "type_counts": {
            requirement_type: sum(
                1
                for requirement in requirements
                if requirement.requirement_type == requirement_type
            )
            for requirement_type in _TYPE_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "requirement_types": [requirement.requirement_type for requirement in requirements],
    }


def _identity_context(text: str, source_field: str) -> bool:
    return bool(
        _IDENTITY_CONTEXT_RE.search(text)
        or _STRUCTURED_CONTEXT_FIELD_RE.search(source_field.replace("-", "_"))
    )


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in _TYPE_PATTERNS.values())


def _type_index(requirement_type: SourceSSORequirementType) -> int:
    return _TYPE_ORDER.index(requirement_type)


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
        "requirements",
        "constraints",
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
        "source_links",
        "identity",
        "authentication",
        "auth",
        "sso",
        "security",
        "acceptance_criteria",
        "implementation_notes",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
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
    text = _optional_text(value)
    return [text] if text else []


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
    "SourceSSORequirement",
    "SourceSSORequirementConfidence",
    "SourceSSORequirementType",
    "SourceSSORequirementsReport",
    "build_source_sso_requirements",
    "extract_source_sso_requirements",
    "source_sso_requirements_to_dict",
    "source_sso_requirements_to_dicts",
]
