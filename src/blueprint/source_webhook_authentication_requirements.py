"""Extract source-level webhook authentication requirements from briefs."""

from __future__ import annotations

import re
from typing import Any, Mapping

from blueprint._keyword_source_requirements import (
    KeywordRequirement as SourceWebhookAuthenticationRequirement,
    KeywordRequirementSpec,
    KeywordRequirementsReport as SourceWebhookAuthenticationRequirementsReport,
    build_keyword_requirements_report,
)


_SPECS = (
    KeywordRequirementSpec("sender_identity", re.compile(r"\b(?:sender identity|provider identity|identify sender|source identity|webhook sender|trusted sender|tenant id|client id)\b", re.I), ("sender identifier source",), {"sender identifier source": re.compile(r"\b(?:tenant id|client id|account id|provider id|issuer|sender id)\b", re.I)}),
    KeywordRequirementSpec("credential_provisioning", re.compile(r"\b(?:credential provisioning|shared secret|secret provisioning|api key|client secret|webhook secret|provision credentials|credential lifecycle)\b", re.I), ("credential lifecycle",), {"credential lifecycle": re.compile(r"\b(?:provision|rotate|revoke|expire|lifecycle|vault|secret manager)\b", re.I)}),
    KeywordRequirementSpec("signature_algorithm_negotiation", re.compile(r"\b(?:signature algorithm|hmac|sha(?:256|512)|rsa|ed25519|algorithm negotiation|signing algorithm)\b", re.I), ("algorithm/version negotiation",), {"algorithm/version negotiation": re.compile(r"\b(?:version|negotiate|algorithm|hmac|sha(?:256|512)|rsa|ed25519)\b", re.I)}),
    KeywordRequirementSpec("timestamp_nonce_replay_prevention", re.compile(r"\b(?:timestamp|nonce|replay prevention|replay attack|replay window|idempotency token)\b", re.I), ("replay prevention",), {"replay prevention": re.compile(r"\b(?:nonce|timestamp|replay window|dedupe|expire|ttl|clock skew)\b", re.I)}),
    KeywordRequirementSpec("secret_rotation_coordination", re.compile(r"\b(?:secret rotation|rotate secret|key rotation|dual secret|overlap window|rotation coordination)\b", re.I), ("rotation coordination",), {"rotation coordination": re.compile(r"\b(?:overlap|dual|grace period|coordinate|rollback|cutover)\b", re.I)}),
    KeywordRequirementSpec("receiver_verification", re.compile(r"\b(?:receiver verification|verify signature|validate signature|authenticate webhook|receiver validates|signature verification)\b", re.I), ("verification inputs",), {"verification inputs": re.compile(r"\b(?:header|payload|body|timestamp|signature|canonical)\b", re.I)}),
    KeywordRequirementSpec("failure_response", re.compile(r"\b(?:verification failure|authentication failure|invalid signature|reject webhook|401|403|failure response)\b", re.I), ("verification failure behavior",), {"verification failure behavior": re.compile(r"\b(?:reject|401|403|dead letter|quarantine|alert|log|retry)\b", re.I)}),
    KeywordRequirementSpec("audit_logging", re.compile(r"\b(?:audit log|audit logging|auth log|authentication event|verification event|security log)\b", re.I), ("audit fields",), {"audit fields": re.compile(r"\b(?:sender|signature|failure|timestamp|request id|event id|actor)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:webhook|callback|event delivery|provider event|signed payload)\b", re.I)
_STRUCTURED = re.compile(r"(?:webhook|auth|security|credential|signature|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:webhook authentication|webhook auth|signature|credential|replay)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:webhook authentication|webhook auth|signature|credential|replay)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {
    "missing_credential_lifecycle": ("credential lifecycle",),
    "missing_replay_prevention": ("replay prevention",),
    "missing_verification_failure_behavior": ("verification failure behavior",),
}


def build_source_webhook_authentication_requirements(source: Any) -> SourceWebhookAuthenticationRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Webhook Authentication Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_webhook_authentication_requirements(source: Any) -> SourceWebhookAuthenticationRequirementsReport:
    return build_source_webhook_authentication_requirements(source)


def generate_source_webhook_authentication_requirements(source: Any) -> SourceWebhookAuthenticationRequirementsReport:
    return build_source_webhook_authentication_requirements(source)


def derive_source_webhook_authentication_requirements(source: Any) -> SourceWebhookAuthenticationRequirementsReport:
    return build_source_webhook_authentication_requirements(source)


def summarize_source_webhook_authentication_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceWebhookAuthenticationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_webhook_authentication_requirements(source_or_result).summary


def source_webhook_authentication_requirements_to_dict(report: SourceWebhookAuthenticationRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_webhook_authentication_requirements_to_dict.__test__ = False


def source_webhook_authentication_requirements_to_dicts(requirements: SourceWebhookAuthenticationRequirementsReport | list[SourceWebhookAuthenticationRequirement] | tuple[SourceWebhookAuthenticationRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceWebhookAuthenticationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_webhook_authentication_requirements_to_dicts.__test__ = False


def source_webhook_authentication_requirements_to_markdown(report: SourceWebhookAuthenticationRequirementsReport) -> str:
    return report.to_markdown()


source_webhook_authentication_requirements_to_markdown.__test__ = False
