"""Extract source-level payment failure recovery requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourcePaymentFailureRecoveryRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourcePaymentFailureRecoveryRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("failure_classification", re.compile(r"\b(?:failure classification|decline classification|failure reason|hard decline|soft decline)\b", re.I), ("classification rules",), {"classification rules": re.compile(r"\b(?:hard decline|soft decline|insufficient funds|expired card|processor code|reason code)\b", re.I)}),
    KeywordRequirementSpec("retry_schedule", re.compile(r"\b(?:retry schedule|payment retry|retry cadence|reattempt schedule|collection retry)\b", re.I), ("retry timing",), {"retry timing": re.compile(r"\b(?:day|hour|after|cadence|\d+|smart retry|retry on)\b", re.I)}),
    KeywordRequirementSpec("payment_method_update", re.compile(r"\b(?:payment method update|update card|card updater|billing method|payment method link)\b", re.I), ("update path",), {"update path": re.compile(r"\b(?:hosted link|portal|card updater|update card|payment method|billing portal)\b", re.I)}),
    KeywordRequirementSpec("customer_notification", re.compile(r"\b(?:customer notification|payment failure email|dunning email|notify customer|in-app notice)\b", re.I), ("notification channel",), {"notification channel": re.compile(r"\b(?:email|sms|push|in-app|template|message)\b", re.I)}),
    KeywordRequirementSpec("dunning_state", re.compile(r"\b(?:dunning state|collections state|recovery state|past due state|delinquency state)\b", re.I), ("state model",), {"state model": re.compile(r"\b(?:past due|grace|suspended|canceled|state machine|collections|delinquent)\b", re.I)}),
    KeywordRequirementSpec("grace_period", re.compile(r"\b(?:grace period|grace window|access grace|payment grace)\b", re.I), ("grace duration",), {"grace duration": re.compile(r"\b(?:\d+\s*(?:day|hour)s?|until|through|window|duration)\b", re.I)}),
    KeywordRequirementSpec("subscription_impact", re.compile(r"\b(?:subscription impact|subscription status|entitlement impact|access impact|plan impact)\b", re.I), ("subscription action",), {"subscription action": re.compile(r"\b(?:pause|suspend|cancel|downgrade|entitlement|access|renewal|status)\b", re.I)}),
    KeywordRequirementSpec("recovery_metrics", re.compile(r"\b(?:recovery metrics?|dunning metrics?|payment recovery dashboard|recovered revenue|collection rate)\b", re.I), ("metric definition",), {"metric definition": re.compile(r"\b(?:recovered revenue|recovery rate|collection rate|churn|dashboard|metric|cohort)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:payment failure recovery|payment failure|dunning|failed payment|payment recovery|collection recovery)\b", re.I)
_STRUCTURED = re.compile(r"(?:payment|failure|recovery|dunning|billing|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:payment failure recovery|payment failure|dunning|payment recovery)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:payment failure recovery|payment failure|dunning|payment recovery)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_retry_schedule": ("retry timing",), "missing_notification": ("notification channel",), "missing_subscription_impact": ("subscription action",)}


def build_source_payment_failure_recovery_requirements(source: Any) -> SourcePaymentFailureRecoveryRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Payment Failure Recovery Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_payment_failure_recovery_requirements(source: Any) -> SourcePaymentFailureRecoveryRequirementsReport:
    return build_source_payment_failure_recovery_requirements(source)


def generate_source_payment_failure_recovery_requirements(source: Any) -> SourcePaymentFailureRecoveryRequirementsReport:
    return build_source_payment_failure_recovery_requirements(source)


def derive_source_payment_failure_recovery_requirements(source: Any) -> SourcePaymentFailureRecoveryRequirementsReport:
    return build_source_payment_failure_recovery_requirements(source)


def summarize_source_payment_failure_recovery_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourcePaymentFailureRecoveryRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_payment_failure_recovery_requirements(source_or_result).summary


def source_payment_failure_recovery_requirements_to_dict(report: SourcePaymentFailureRecoveryRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_payment_failure_recovery_requirements_to_dict.__test__ = False


def source_payment_failure_recovery_requirements_to_dicts(requirements: SourcePaymentFailureRecoveryRequirementsReport | list[SourcePaymentFailureRecoveryRequirement] | tuple[SourcePaymentFailureRecoveryRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourcePaymentFailureRecoveryRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_payment_failure_recovery_requirements_to_dicts.__test__ = False


def source_payment_failure_recovery_requirements_to_markdown(report: SourcePaymentFailureRecoveryRequirementsReport) -> str:
    return report.to_markdown()


source_payment_failure_recovery_requirements_to_markdown.__test__ = False
