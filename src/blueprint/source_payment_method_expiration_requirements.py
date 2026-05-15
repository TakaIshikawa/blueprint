"""Extract source-level payment method expiration requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourcePaymentMethodExpirationRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourcePaymentMethodExpirationRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("expiration_detection", re.compile(r"\b(?:expiration detection|expired card detection|card expiration detection|payment method expiration detection|expiry detection)\b", re.I), ("detection rule",), {"detection rule": re.compile(r"\b(?:detect|month|year|expires in|expired|card updater|daily scan)\b", re.I)}),
    KeywordRequirementSpec("pre_expiry_notification", re.compile(r"\b(?:pre[- ]?expiry notification|expiry notification|expiration notice|expiring card email|notify before expiration)\b", re.I), ("notification timing",), {"notification timing": re.compile(r"\b(?:\d+\s*days?|before|monthly|email|sms|in-app|template)\b", re.I)}),
    KeywordRequirementSpec("update_link", re.compile(r"\b(?:update link|payment update link|hosted update|update payment method|billing portal link)\b", re.I), ("update path",), {"update path": re.compile(r"\b(?:portal|hosted link|secure link|deep link|account settings|checkout)\b", re.I)}),
    KeywordRequirementSpec("retry_after_expiry", re.compile(r"\b(?:retry after expiry|retry after expiration|post[- ]?expiry retry|expired payment retry)\b", re.I), ("retry timing",), {"retry timing": re.compile(r"\b(?:after update|next invoice|daily|hours?|days?|retry once|reattempt)\b", re.I)}),
    KeywordRequirementSpec("account_grace_period", re.compile(r"\b(?:account grace period|grace period|grace window|expiration grace)\b", re.I), ("grace period",), {"grace period": re.compile(r"\b(?:\d+\s*days?|until|through|window|duration|grace ends)\b", re.I)}),
    KeywordRequirementSpec("subscription_impact", re.compile(r"\b(?:subscription impact|subscription status|entitlement impact|access impact|renewal impact)\b", re.I), ("subscription impact",), {"subscription impact": re.compile(r"\b(?:pause|suspend|cancel|renewal|entitlement|access|status|downgrade)\b", re.I)}),
    KeywordRequirementSpec("processor_sync", re.compile(r"\b(?:processor sync|processor updater|gateway sync|card updater sync|stripe updater|adyen updater)\b", re.I), ("processor sync",), {"processor sync": re.compile(r"\b(?:stripe|adyen|braintree|gateway|webhook|sync job|card updater)\b", re.I)}),
    KeywordRequirementSpec("recovery_metrics", re.compile(r"\b(?:recovery metrics?|expiration metrics?|updated card rate|expiry dashboard|card update conversion)\b", re.I), ("metric definition",), {"metric definition": re.compile(r"\b(?:updated card rate|conversion|dashboard|metric|cohort|saved revenue)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:payment method expiration|card expiration|card expiry|expired payment method|expiring card)\b", re.I)
_STRUCTURED = re.compile(r"(?:payment|card|expiration|expiry|billing|processor|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:payment method expiration|card expiration|card expiry|expired payment method)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:payment method expiration|card expiration|card expiry|expired payment method)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_notification_timing": ("notification timing",), "missing_update_path": ("update path",), "missing_grace_period": ("grace period",), "missing_processor_sync": ("processor sync",), "missing_subscription_impact": ("subscription impact",)}


def build_source_payment_method_expiration_requirements(source: Any) -> SourcePaymentMethodExpirationRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Payment Method Expiration Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_payment_method_expiration_requirements(source: Any) -> SourcePaymentMethodExpirationRequirementsReport:
    return build_source_payment_method_expiration_requirements(source)


def generate_source_payment_method_expiration_requirements(source: Any) -> SourcePaymentMethodExpirationRequirementsReport:
    return build_source_payment_method_expiration_requirements(source)


def derive_source_payment_method_expiration_requirements(source: Any) -> SourcePaymentMethodExpirationRequirementsReport:
    return build_source_payment_method_expiration_requirements(source)


def summarize_source_payment_method_expiration_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourcePaymentMethodExpirationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_payment_method_expiration_requirements(source_or_result).summary


def source_payment_method_expiration_requirements_to_dict(report: SourcePaymentMethodExpirationRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_payment_method_expiration_requirements_to_dict.__test__ = False


def source_payment_method_expiration_requirements_to_dicts(requirements: SourcePaymentMethodExpirationRequirementsReport | list[SourcePaymentMethodExpirationRequirement] | tuple[SourcePaymentMethodExpirationRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourcePaymentMethodExpirationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_payment_method_expiration_requirements_to_dicts.__test__ = False


def source_payment_method_expiration_requirements_to_markdown(report: SourcePaymentMethodExpirationRequirementsReport) -> str:
    return report.to_markdown()


source_payment_method_expiration_requirements_to_markdown.__test__ = False
