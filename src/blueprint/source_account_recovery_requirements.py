"""Extract source-level account recovery requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceAccountRecoveryRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceAccountRecoveryRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("identity_proofing", re.compile(r"\b(?:identity proofing|identity verification|prove identity|proofing checks?|verification questions?)\b", re.I), ("proofing method",), {"proofing method": re.compile(r"\b(?:document|knowledge|security question|support review|verified device|government id|proof)\b", re.I)}),
    KeywordRequirementSpec("recovery_channel", re.compile(r"\b(?:recovery channel|email recovery|sms recovery|recovery email|backup email|phone recovery)\b", re.I), ("channel",), {"channel": re.compile(r"\b(?:email|sms|phone|backup email|authenticator|support|channel)\b", re.I)}),
    KeywordRequirementSpec("reset_token_lifecycle", re.compile(r"\b(?:reset token lifecycle|reset token|recovery token|token expiry|one-time link)\b", re.I), ("token lifecycle",), {"token lifecycle": re.compile(r"\b(?:expire|expiry|ttl|one-time|single use|rotate|invalidate|minutes?|hours?)\b", re.I)}),
    KeywordRequirementSpec("mfa_recovery", re.compile(r"\b(?:mfa recovery|2fa recovery|factor recovery|backup codes?|lost authenticator)\b", re.I), ("mfa path",), {"mfa path": re.compile(r"\b(?:backup code|lost device|authenticator|webauthn|support approval|factor)\b", re.I)}),
    KeywordRequirementSpec("abuse_rate_limiting", re.compile(r"\b(?:abuse controls?|rate limiting|rate limits?|lockout|brute force|attempt limits?)\b", re.I), ("abuse control",), {"abuse control": re.compile(r"\b(?:rate limit|lockout|captcha|throttle|attempt limit|risk score|ip limit)\b", re.I)}),
    KeywordRequirementSpec("notification", re.compile(r"\b(?:recovery notification|account recovery email|notify user|security notification|reset notification)\b", re.I), ("notification channel",), {"notification channel": re.compile(r"\b(?:email|sms|push|security notification|template|message)\b", re.I)}),
    KeywordRequirementSpec("audit_logging", re.compile(r"\b(?:audit logging|audit log|recovery audit|security log|event log)\b", re.I), ("audit events",), {"audit events": re.compile(r"\b(?:audit|event|ip address|device|timestamp|actor|log)\b", re.I)}),
    KeywordRequirementSpec("support_escalation", re.compile(r"\b(?:support escalation|manual review|support handoff|escalate to support|agent review)\b", re.I), ("support path",), {"support path": re.compile(r"\b(?:support|agent|manual review|ticket|escalation|approval)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:account recovery|password recovery|reset flow|recovery flow|recover account)\b", re.I)
_STRUCTURED = re.compile(r"(?:account|recovery|reset|security|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:account recovery|password recovery|reset flow|recovery flow)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:account recovery|password recovery|reset flow|recovery flow)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_proofing": ("proofing method",), "missing_token_lifecycle": ("token lifecycle",), "missing_abuse_controls": ("abuse control",)}


def build_source_account_recovery_requirements(source: Any) -> SourceAccountRecoveryRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Account Recovery Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_account_recovery_requirements(source: Any) -> SourceAccountRecoveryRequirementsReport:
    return build_source_account_recovery_requirements(source)


def generate_source_account_recovery_requirements(source: Any) -> SourceAccountRecoveryRequirementsReport:
    return build_source_account_recovery_requirements(source)


def derive_source_account_recovery_requirements(source: Any) -> SourceAccountRecoveryRequirementsReport:
    return build_source_account_recovery_requirements(source)


def summarize_source_account_recovery_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceAccountRecoveryRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_account_recovery_requirements(source_or_result).summary


def source_account_recovery_requirements_to_dict(report: SourceAccountRecoveryRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_account_recovery_requirements_to_dict.__test__ = False


def source_account_recovery_requirements_to_dicts(requirements: SourceAccountRecoveryRequirementsReport | list[SourceAccountRecoveryRequirement] | tuple[SourceAccountRecoveryRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceAccountRecoveryRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_account_recovery_requirements_to_dicts.__test__ = False


def source_account_recovery_requirements_to_markdown(report: SourceAccountRecoveryRequirementsReport) -> str:
    return report.to_markdown()


source_account_recovery_requirements_to_markdown.__test__ = False
