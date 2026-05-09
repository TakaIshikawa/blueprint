"""Analyze SSO integration readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for SSO integration concepts
_PROTOCOL_SUPPORT_RE = re.compile(
    r"(?:\bsso\s+protocol|\bsaml\s+(?:2\.0|support|integration|flow)|"
    r"\boauth\s+(?:2\.0|support|integration)|\bopenid\s+connect|\boidc\s+(?:support|integration)|"
    r"\bfederated\s+(?:authentication|identity|protocol)|"
    r"\bsupport(?:s|ing)?\s+(?:saml|oauth|oidc)|\bimplement(?:s|ing)?\s+(?:saml|oauth|oidc)|"
    r"\b(?:sp|idp)[- ]initiated(?:\s+(?:flow|login|sso|saml))?|"
    r"test_(?:sso|saml|oauth|oidc)_|(?:sso|saml|oauth|oidc)_(?:integration|authentication)\.py)\b",
    re.I,
)
_IDP_CONFIGURATION_RE = re.compile(
    r"\b(?:identity\s+provider\s+(?:configuration|config|setup|integration)?|"
    r"idp\s+(?:configuration|config|setup|integration|metadata)|"
    r"idp[- ]initiated|"
    r"(?:mock|test)\s+(?:idp|identity\s+provider)|"
    r"okta|azure\s+ad|auth0|google\s+workspace|ping(?:identity|federate|one)?|onelogin|"
    r"configure\s+(?:idp|identity\s+provider)|setup\s+(?:idp|identity\s+provider))\b",
    re.I,
)
_USER_PROVISIONING_RE = re.compile(
    r"\b(?:user\s+provisioning|(?:auto|automatic)(?:ally)?\s+provision|"
    r"scim\s+(?:2\.0|endpoint|integration|provisioning)|"
    r"just[- ]in[- ]time\s+provisioning|jit\s+provisioning|"
    r"(?:de)?provision(?:ing)?\s+user|user\s+(?:creation|lifecycle))\b",
    re.I,
)
_ROLE_MAPPING_RE = re.compile(
    r"\b(?:role\s+(?:mapping|map)|attribute\s+(?:mapping|map)|claim\s+(?:mapping|map)|"
    r"(?:map|mapping)\s+(?:role|attribute|claim)s?|"
    r"saml\s+attribute|oidc\s+claim|profile\s+mapping|group\s+(?:mapping|sync))\b",
    re.I,
)
_SESSION_MANAGEMENT_RE = re.compile(
    r"\b(?:sso\s+session|session\s+(?:management|handling|timeout|expir(?:y|ation))|"
    r"session\s+(?:revocation|invalidation)|manage\s+session)\b",
    re.I,
)
_LOGOUT_HANDLING_RE = re.compile(
    r"\b(?:single\s+logout|slo|logout\s+(?:handling|handler|endpoint)|"
    r"federated\s+logout|global\s+logout|implement\s+logout|handle\s+logout)\b",
    re.I,
)
_GROUP_SYNC_RE = re.compile(
    r"\b(?:group\s+sync(?:hronization)?|sync\s+group|"
    r"(?:synchronize|sync)\s+(?:team|organization|group)s?|"
    r"group\s+membership|team\s+sync)\b",
    re.I,
)
_MULTI_TENANT_ISOLATION_RE = re.compile(
    r"\b(?:multi[- ]tenant(?:ancy)?|tenant\s+isolation|"
    r"tenant[- ]specific\s+(?:sso|configuration|idp)|"
    r"per[- ]tenant\s+(?:sso|configuration|idp)|"
    r"isolate\s+tenant|tenant\s+separation)\b",
    re.I,
)

# Security and testing patterns
_SECURITY_MEASURES_RE = re.compile(
    r"\b(?:(?:sso|saml|oauth|oidc).{0,60}(?:security|secure|validation|verify|certificate|signing)|"
    r"signature\s+(?:verification|validation)|validate\s+(?:assertion|token|certificate|metadata)|"
    r"(?:x509|ssl|tls)\s+certificate|(?:idp|metadata)\s+(?:verification|validation)|"
    r"verify\s+(?:idp|metadata|ssl))\b",
    re.I,
)
_ERROR_HANDLING_RE = re.compile(
    r"\b(?:(?:sso|saml|oauth|oidc).{0,60}(?:error|failure|exception|fallback)|"
    r"(?:error|failure|exception|fallback).{0,60}(?:sso|saml|oauth|oidc)|"
    r"handle\s+(?:sso|authentication)\s+(?:error|failure)|"
    r"authentication\s+fallback|sso\s+(?:error|failure))\b",
    re.I,
)
_SSO_TESTING_RE = re.compile(
    r"(?:\bsso\s+(?:integration\s+)?test(?:s|ing)?|\btest(?:s|ing)?\s+sso\b|"
    r"\b(?:saml|oauth|oidc)\s+test(?:s|ing)?|\btest(?:s|ing)?\s+(?:saml|oauth|oidc)\b|"
    r"\bmock\s+(?:idp|identity\s+provider)|\btest\s+(?:idp|identity\s+provider)\b|"
    r"\bintegration\s+test.{0,40}(?:sso|saml|oauth|oidc)|"
    r"test_(?:sso|saml|oauth|oidc)[_\w]*|(?:sso|saml|oauth|oidc)_test|"
    r"tests?/test_(?:sso|saml|oauth|oidc)|(?:sso|saml|oauth)_(?:integration|authentication)\.py)",
    re.I,
)


@dataclass(frozen=True, slots=True)
class SsoIntegrationReadiness:
    """SSO integration readiness analysis for a change brief."""

    protocol_support_defined: bool = False
    idp_configuration_specified: bool = False
    user_provisioning_addressed: bool = False
    role_mapping_configured: bool = False
    session_management_implemented: bool = False
    logout_handling_implemented: bool = False
    group_sync_configured: bool = False
    multi_tenant_isolation_considered: bool = False
    security_measures_included: bool = False
    error_handling_implemented: bool = False
    sso_testing_planned: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "protocol_support_defined": self.protocol_support_defined,
            "idp_configuration_specified": self.idp_configuration_specified,
            "user_provisioning_addressed": self.user_provisioning_addressed,
            "role_mapping_configured": self.role_mapping_configured,
            "session_management_implemented": self.session_management_implemented,
            "logout_handling_implemented": self.logout_handling_implemented,
            "group_sync_configured": self.group_sync_configured,
            "multi_tenant_isolation_considered": self.multi_tenant_isolation_considered,
            "security_measures_included": self.security_measures_included,
            "error_handling_implemented": self.error_handling_implemented,
            "sso_testing_planned": self.sso_testing_planned,
        }


def analyze_sso_integration_readiness(change_brief: Mapping[str, Any]) -> SsoIntegrationReadiness:
    """
    Analyze SSO integration readiness from a change brief.

    Args:
        change_brief: A mapping containing change information with fields like
                     'title', 'description', 'acceptance_criteria', etc.

    Returns:
        SsoIntegrationReadiness with boolean flags for each SSO aspect.
    """
    if not isinstance(change_brief, Mapping):
        return SsoIntegrationReadiness()

    searchable_text = _extract_searchable_text(change_brief)

    return SsoIntegrationReadiness(
        protocol_support_defined=bool(_PROTOCOL_SUPPORT_RE.search(searchable_text)),
        idp_configuration_specified=bool(_IDP_CONFIGURATION_RE.search(searchable_text)),
        user_provisioning_addressed=bool(_USER_PROVISIONING_RE.search(searchable_text)),
        role_mapping_configured=bool(_ROLE_MAPPING_RE.search(searchable_text)),
        session_management_implemented=bool(_SESSION_MANAGEMENT_RE.search(searchable_text)),
        logout_handling_implemented=bool(_LOGOUT_HANDLING_RE.search(searchable_text)),
        group_sync_configured=bool(_GROUP_SYNC_RE.search(searchable_text)),
        multi_tenant_isolation_considered=bool(_MULTI_TENANT_ISOLATION_RE.search(searchable_text)),
        security_measures_included=bool(_SECURITY_MEASURES_RE.search(searchable_text)),
        error_handling_implemented=bool(_ERROR_HANDLING_RE.search(searchable_text)),
        sso_testing_planned=bool(_SSO_TESTING_RE.search(searchable_text)),
    )


def _extract_searchable_text(change_brief: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the change brief for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = change_brief.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("acceptance_criteria", "requirements", "notes", "risks", "definition_of_done"):
        value = change_brief.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Extract validation commands
    validation = change_brief.get("validation_command") or change_brief.get("validation_commands")
    if isinstance(validation, str):
        parts.append(validation)
    elif isinstance(validation, (list, tuple)):
        parts.extend(str(cmd) for cmd in validation if cmd)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "SsoIntegrationReadiness",
    "analyze_sso_integration_readiness",
]
