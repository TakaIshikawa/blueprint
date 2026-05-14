"""Extract source-level OAuth token refresh requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import (
    KeywordRequirement as SourceOAuthTokenRefreshRequirement,
    KeywordRequirementSpec,
    KeywordRequirementsReport as SourceOAuthTokenRefreshRequirementsReport,
    build_keyword_requirements_report,
)


_SPECS = (
    KeywordRequirementSpec("refresh_token_storage", re.compile(r"\b(?:refresh token storage|store refresh token|token vault|encrypted token|secret storage)\b", re.I), ("token storage",), {"token storage": re.compile(r"\b(?:encrypt|encrypted|vault|kms|secret|at rest|database|keychain)\b", re.I)}),
    KeywordRequirementSpec("rotation", re.compile(r"\b(?:rotation|token rotation|refresh token rotation|rotate refresh token|rotating refresh tokens?|one[- ]time refresh token)\b", re.I), ("rotation behavior",), {"rotation behavior": re.compile(r"\b(?:rotate|single use|one[- ]time|replace|reuse detection|new refresh token)\b", re.I)}),
    KeywordRequirementSpec("expiry_handling", re.compile(r"\b(?:expiry handling|token expiry|expires?|expiration|expired token|refresh before expiry)\b", re.I), ("expiry behavior",), {"expiry behavior": re.compile(r"\b(?:expires?|expiration|ttl|refresh before|grace period|clock skew|lifetime)\b", re.I)}),
    KeywordRequirementSpec("retry_backoff", re.compile(r"\b(?:retry backoff|refresh retry|token refresh retry|backoff|transient refresh failure)\b", re.I), ("retry behavior",), {"retry behavior": re.compile(r"\b(?:retry|backoff|jitter|attempt|rate limit|429|5xx|timeout)\b", re.I)}),
    KeywordRequirementSpec("revocation", re.compile(r"\b(?:revocation|revoke token|token revoke|revoked refresh token|disconnect oauth|provider disconnect)\b", re.I), ("revocation path",), {"revocation path": re.compile(r"\b(?:revoke|disconnect|unlink|logout|invalid_grant|provider callback)\b", re.I)}),
    KeywordRequirementSpec("consent_scope_changes", re.compile(r"\b(?:consent scope changes?|scope changes?|oauth scopes?|reconsent|incremental consent|permission changes?)\b", re.I), ("scope change handling",), {"scope change handling": re.compile(r"\b(?:scope|consent|reconsent|permission|incremental|downgrade|upgrade)\b", re.I)}),
    KeywordRequirementSpec("error_recovery", re.compile(r"\b(?:error recovery|refresh error|invalid_grant|reauthori[sz]e|reauthentication|token refresh failure)\b", re.I), ("recovery path",), {"recovery path": re.compile(r"\b(?:reauthori[sz]e|reauthentication|invalid_grant|user action|fallback|recover|disconnect)\b", re.I)}),
    KeywordRequirementSpec("audit_logging", re.compile(r"\b(?:audit logging|audit log|token audit|refresh audit|security log)\b", re.I), ("audit events",), {"audit events": re.compile(r"\b(?:audit|log|actor|client id|timestamp|success|failure|scope|provider)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:oauth token refresh|oauth refresh token|refresh token|token refresh|oauth connection)\b", re.I)
_STRUCTURED = re.compile(r"(?:oauth|token|refresh|authorization|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:oauth token refresh|oauth refresh token|refresh token|token refresh)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:oauth token refresh|oauth refresh token|refresh token|token refresh)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {
    "missing_token_storage": ("token storage",),
    "missing_rotation": ("rotation behavior",),
    "missing_revocation": ("revocation path",),
}


def build_source_oauth_token_refresh_requirements(source: Any) -> SourceOAuthTokenRefreshRequirementsReport:
    return build_keyword_requirements_report(source, title="Source OAuth Token Refresh Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_oauth_token_refresh_requirements(source: Any) -> SourceOAuthTokenRefreshRequirementsReport:
    return build_source_oauth_token_refresh_requirements(source)


def generate_source_oauth_token_refresh_requirements(source: Any) -> SourceOAuthTokenRefreshRequirementsReport:
    return build_source_oauth_token_refresh_requirements(source)


def derive_source_oauth_token_refresh_requirements(source: Any) -> SourceOAuthTokenRefreshRequirementsReport:
    return build_source_oauth_token_refresh_requirements(source)


def summarize_source_oauth_token_refresh_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceOAuthTokenRefreshRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_oauth_token_refresh_requirements(source_or_result).summary


def source_oauth_token_refresh_requirements_to_dict(report: SourceOAuthTokenRefreshRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_oauth_token_refresh_requirements_to_dict.__test__ = False


def source_oauth_token_refresh_requirements_to_dicts(requirements: SourceOAuthTokenRefreshRequirementsReport | list[SourceOAuthTokenRefreshRequirement] | tuple[SourceOAuthTokenRefreshRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceOAuthTokenRefreshRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_oauth_token_refresh_requirements_to_dicts.__test__ = False


def source_oauth_token_refresh_requirements_to_markdown(report: SourceOAuthTokenRefreshRequirementsReport) -> str:
    return report.to_markdown()


source_oauth_token_refresh_requirements_to_markdown.__test__ = False
