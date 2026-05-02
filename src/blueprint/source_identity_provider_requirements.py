"""Extract identity-provider implementation requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


SourceIdentityProviderArea = Literal[
    "sso",
    "saml",
    "oidc",
    "scim",
    "identity_provider",
    "group_claim",
    "role_mapping",
    "jit_provisioning",
    "deprovisioning",
]
SourceIdentityProviderRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_AREA_ORDER: tuple[SourceIdentityProviderArea, ...] = (
    "sso",
    "saml",
    "oidc",
    "scim",
    "identity_provider",
    "group_claim",
    "role_mapping",
    "jit_provisioning",
    "deprovisioning",
)
_RISK_ORDER: dict[SourceIdentityProviderRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|need(?:s)?|support|enable|allow|ensure|"
    r"acceptance|done when|before launch|block(?:er|ing)?)\b",
    re.I,
)
_IDENTITY_CONTEXT_RE = re.compile(
    r"\b(?:identity|identities|authentication|authn|login|sign[- ]?in|enterprise|"
    r"sso|single sign[- ]?on|saml|oidc|openid|oauth|scim|idp|identity provider|"
    r"directory|provision(?:ing)?|deprovision(?:ing)?|claims?|roles?)\b",
    re.I,
)
_STRUCTURED_CONTEXT_FIELD_RE = re.compile(
    r"(?:requirement|requirements|acceptance|criteria|constraints?|risks?|metadata|"
    r"identity|idp|auth|authentication|sso|security|compliance|enterprise|provisioning|"
    r"directory|claims?|roles?|login)",
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
_AREA_PATTERNS: dict[SourceIdentityProviderArea, re.Pattern[str]] = {
    "sso": re.compile(
        r"\b(?:sso|single sign[- ]?on|single sign on|federated login|federated auth(?:entication)?)\b",
        re.I,
    ),
    "saml": re.compile(r"\b(?:saml|saml2|saml 2\.0|saml assertion|metadata xml|acs url|entity id)\b", re.I),
    "oidc": re.compile(
        r"\b(?:oidc|openid connect|openid|oauth2?|oauth 2\.0|id token|issuer url|jwks|authorization code)\b",
        re.I,
    ),
    "scim": re.compile(r"\b(?:scim|scim 2\.0|directory sync|user provisioning|group provisioning)\b", re.I),
    "identity_provider": re.compile(
        r"\b(?:idp|identity providers?|okta|azure ad|microsoft entra|entra id|auth0|"
        r"google workspace|google apps|google identity|ping identity|onelogin)\b",
        re.I,
    ),
    "group_claim": re.compile(
        r"\b(?:group claims?|groups? claim|claim groups?|groups? attribute|memberof|"
        r"member of|group attribute|idp groups?)\b",
        re.I,
    ),
    "role_mapping": re.compile(
        r"\b(?:role mappings?|map roles?|map idp groups?|groups? to roles?|claims? to roles?|"
        r"rbac mapping|attribute mapping|department mapping)\b",
        re.I,
    ),
    "jit_provisioning": re.compile(
        r"\b(?:jit provisioning|just[- ]in[- ]time provisioning|just in time provisioning|"
        r"auto[- ]provision|auto provision|provision users? on first login|create users? on login)\b",
        re.I,
    ),
    "deprovisioning": re.compile(
        r"\b(?:deprovision(?:ing|ed)?|deactivate users?|disable users?|suspend users?|"
        r"remove users?|user offboarding|revoke access|terminated employees?)\b",
        re.I,
    ),
}
_PROVIDER_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("Okta", re.compile(r"\bokta\b", re.I)),
    ("Azure AD", re.compile(r"\b(?:azure ad|azure active directory|microsoft entra|entra id)\b", re.I)),
    ("Auth0", re.compile(r"\bauth0\b", re.I)),
    ("Google Workspace", re.compile(r"\b(?:google workspace|google apps|google identity)\b", re.I)),
)
_AUDIENCE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("enterprise admins", re.compile(r"\benterprise admins?\b", re.I)),
    ("admins", re.compile(r"\badmins?|administrators?\b", re.I)),
    ("enterprise customers", re.compile(r"\benterprise customers?\b", re.I)),
    ("customers", re.compile(r"\bcustomers?\b", re.I)),
    ("employees", re.compile(r"\bemployees?|workforce|staff\b", re.I)),
    ("contractors", re.compile(r"\bcontractors?\b", re.I)),
    ("users", re.compile(r"\busers?|members?\b", re.I)),
)
_ACCEPTANCE_CRITERIA_HINTS: dict[SourceIdentityProviderArea, tuple[str, ...]] = {
    "sso": ("Verify SSO can be enabled, enforced, and bypassed only by approved fallback access.",),
    "saml": ("Validate SAML metadata, ACS URL, entity ID, certificate rotation, and assertion parsing.",),
    "oidc": ("Validate OIDC issuer discovery, JWKS rotation, scopes, and ID token claims.",),
    "scim": ("Test SCIM user and group create, update, deactivate, and conflict handling flows.",),
    "identity_provider": ("Document provider setup and verify provider-specific admin configuration.",),
    "group_claim": ("Test expected, missing, malformed, and multi-group claim payloads.",),
    "role_mapping": ("Verify IdP groups or claims map to the correct application roles.",),
    "jit_provisioning": ("Verify first-login provisioning assigns the correct tenant, profile, and default role.",),
    "deprovisioning": ("Verify deprovisioning disables access, preserves audit history, and handles active ownership.",),
}


@dataclass(frozen=True, slots=True)
class SourceIdentityProviderRequirement:
    """One source-backed identity-provider implementation requirement."""

    identity_area: SourceIdentityProviderArea
    provider_hint: str | None = None
    affected_audience: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    acceptance_criteria_hints: tuple[str, ...] = field(default_factory=tuple)
    risk_level: SourceIdentityProviderRiskLevel = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "identity_area": self.identity_area,
            "provider_hint": self.provider_hint,
            "affected_audience": self.affected_audience,
            "evidence": list(self.evidence),
            "acceptance_criteria_hints": list(self.acceptance_criteria_hints),
            "risk_level": self.risk_level,
        }


@dataclass(frozen=True, slots=True)
class SourceIdentityProviderRequirementsReport:
    """Brief-level identity-provider requirements report."""

    source_brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceIdentityProviderRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceIdentityProviderRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return identity-provider requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Identity Provider Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        area_counts = self.summary.get("area_counts", {})
        provider_counts = self.summary.get("provider_counts", {})
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
            "- Area counts: " + ", ".join(f"{area} {area_counts.get(area, 0)}" for area in _AREA_ORDER),
            "- Provider counts: "
            + (", ".join(f"{provider} {provider_counts[provider]}" for provider in sorted(provider_counts)) or "none"),
        ]
        if not self.requirements:
            lines.extend(["", "No source identity provider requirements were found in the brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Area | Provider | Audience | Risk | Evidence | Acceptance Criteria Hints |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.identity_area} | "
                f"{_markdown_cell(requirement.provider_hint or 'unspecified')} | "
                f"{_markdown_cell(requirement.affected_audience or 'unspecified')} | "
                f"{requirement.risk_level} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.acceptance_criteria_hints))} |"
            )
        return "\n".join(lines)


def build_source_identity_provider_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceIdentityProviderRequirementsReport:
    """Extract identity-provider requirements from a source brief."""
    source_brief_id, payload = _source_payload(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_requirement_candidates(payload)),
            key=lambda requirement: (
                _RISK_ORDER[requirement.risk_level],
                _area_index(requirement.identity_area),
                requirement.provider_hint or "",
                requirement.affected_audience or "",
                requirement.evidence,
            ),
        )
    )
    return SourceIdentityProviderRequirementsReport(
        source_brief_id=source_brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def generate_source_identity_provider_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceIdentityProviderRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_identity_provider_requirements(source)


def extract_source_identity_provider_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SourceIdentityProviderRequirement, ...]:
    """Return identity-provider requirement records from brief-shaped input."""
    return build_source_identity_provider_requirements(source).requirements


def source_identity_provider_requirements_to_dict(
    report: SourceIdentityProviderRequirementsReport,
) -> dict[str, Any]:
    """Serialize an identity-provider requirements report to a plain dictionary."""
    return report.to_dict()


source_identity_provider_requirements_to_dict.__test__ = False


def source_identity_provider_requirements_to_dicts(
    requirements: (
        tuple[SourceIdentityProviderRequirement, ...]
        | list[SourceIdentityProviderRequirement]
        | SourceIdentityProviderRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize identity-provider requirement records to dictionaries."""
    if isinstance(requirements, SourceIdentityProviderRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_identity_provider_requirements_to_dicts.__test__ = False


def source_identity_provider_requirements_to_markdown(
    report: SourceIdentityProviderRequirementsReport,
) -> str:
    """Render an identity-provider requirements report as Markdown."""
    return report.to_markdown()


source_identity_provider_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    identity_area: SourceIdentityProviderArea
    provider_hint: str | None
    affected_audience: str | None
    evidence: str
    required: bool


def _source_payload(source: Mapping[str, Any] | SourceBrief | str | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
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
    payload = _object_payload(source)
    return _source_brief_id(payload), payload


def _source_brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_field, segment in _candidate_segments(payload):
        areas = _identity_areas(segment, source_field)
        if not areas:
            continue
        providers = _provider_hints(segment, source_field) or (None,)
        audience = _affected_audience(segment, source_field)
        evidence = _evidence_snippet(source_field, segment)
        required = bool(_REQUIRED_RE.search(segment))
        for area in areas:
            for provider in providers:
                candidates.append(
                    _Candidate(
                        identity_area=area,
                        provider_hint=provider,
                        affected_audience=audience,
                        evidence=evidence,
                        required=required,
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceIdentityProviderRequirement]:
    grouped: dict[tuple[SourceIdentityProviderArea, str | None, str | None], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.identity_area, candidate.provider_hint, candidate.affected_audience), []).append(
            candidate
        )

    requirements: list[SourceIdentityProviderRequirement] = []
    for (area, provider, audience), items in grouped.items():
        evidence = tuple(sorted(_dedupe(item.evidence for item in items), key=str.casefold))[:6]
        requirements.append(
            SourceIdentityProviderRequirement(
                identity_area=area,
                provider_hint=provider,
                affected_audience=audience,
                evidence=evidence,
                acceptance_criteria_hints=_ACCEPTANCE_CRITERIA_HINTS[area],
                risk_level=_risk_level(area, any(item.required for item in items), len(evidence)),
            )
        )
    return requirements


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
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "metadata",
        "brief_metadata",
        "identity",
        "authentication",
        "auth",
        "sso",
        "security",
        "implementation_notes",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key])
    return [(field, segment) for field, segment in segments if segment]


def _append_value(segments: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            if _any_signal(key_text):
                segments.append((child_field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                _append_value(segments, child_field, child)
            elif text := _optional_text(child):
                for segment in _segments(text):
                    segments.append((child_field, segment))
                if _any_signal(key_text):
                    for segment in _segments(f"{key_text}: {text}"):
                        segments.append((child_field, segment))
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        for segment in _segments(text):
            segments.append((source_field, segment))


def _segments(value: str) -> list[str]:
    parts: list[str] = []
    for line in value.splitlines() or [value]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        sentence_parts = [_clean_text(cleaned)] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else (
            _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for sentence in sentence_parts:
            parts.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in parts if _clean_text(part)]


def _identity_areas(text: str, source_field: str) -> tuple[SourceIdentityProviderArea, ...]:
    areas = [area for area in _AREA_ORDER if _AREA_PATTERNS[area].search(text)]
    field_text = source_field.replace("_", " ").replace("-", " ")
    for area in _AREA_ORDER:
        if area not in areas and _AREA_PATTERNS[area].search(field_text) and _identity_context(text, source_field):
            areas.append(area)
    if "scim" in areas and _AREA_PATTERNS["deprovisioning"].search(text):
        areas.append("deprovisioning") if "deprovisioning" not in areas else None
    return tuple(areas)


def _provider_hints(text: str, source_field: str) -> tuple[str, ...]:
    searchable = f"{source_field.replace('_', ' ').replace('-', ' ')} {text}"
    return tuple(provider for provider, pattern in _PROVIDER_PATTERNS if pattern.search(searchable))


def _affected_audience(text: str, source_field: str) -> str | None:
    searchable = f"{source_field.replace('_', ' ').replace('-', ' ')} {text}"
    for audience, pattern in _AUDIENCE_PATTERNS:
        if pattern.search(searchable):
            return audience
    return None


def _risk_level(
    area: SourceIdentityProviderArea,
    required: bool,
    evidence_count: int,
) -> SourceIdentityProviderRiskLevel:
    if area in {"scim", "group_claim", "role_mapping", "deprovisioning"}:
        return "high"
    if area in {"saml", "oidc", "jit_provisioning"} or required or evidence_count > 1:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceIdentityProviderRequirement, ...]) -> dict[str, Any]:
    providers = sorted(
        {requirement.provider_hint for requirement in requirements if requirement.provider_hint},
        key=str.casefold,
    )
    audiences = sorted(
        {requirement.affected_audience for requirement in requirements if requirement.affected_audience},
        key=str.casefold,
    )
    return {
        "requirement_count": len(requirements),
        "area_counts": {
            area: sum(1 for requirement in requirements if requirement.identity_area == area)
            for area in _AREA_ORDER
        },
        "provider_counts": {
            provider: sum(1 for requirement in requirements if requirement.provider_hint == provider)
            for provider in providers
        },
        "audience_counts": {
            audience: sum(1 for requirement in requirements if requirement.affected_audience == audience)
            for audience in audiences
        },
        "risk_counts": {
            risk: sum(1 for requirement in requirements if requirement.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "identity_areas": [requirement.identity_area for requirement in requirements],
        "provider_hints": providers,
    }


def _identity_context(text: str, source_field: str) -> bool:
    return bool(
        _IDENTITY_CONTEXT_RE.search(text)
        or _STRUCTURED_CONTEXT_FIELD_RE.search(source_field.replace("-", "_"))
    )


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in _AREA_PATTERNS.values()) or any(
        pattern.search(text) for _, pattern in _PROVIDER_PATTERNS
    )


def _area_index(area: SourceIdentityProviderArea) -> int:
    return _AREA_ORDER.index(area)


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
        "definition_of_done",
        "validation_plan",
        "implementation_notes",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
    "SourceIdentityProviderArea",
    "SourceIdentityProviderRequirement",
    "SourceIdentityProviderRequirementsReport",
    "SourceIdentityProviderRiskLevel",
    "build_source_identity_provider_requirements",
    "extract_source_identity_provider_requirements",
    "generate_source_identity_provider_requirements",
    "source_identity_provider_requirements_to_dict",
    "source_identity_provider_requirements_to_dicts",
    "source_identity_provider_requirements_to_markdown",
]
