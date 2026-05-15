"""Extract source-level trial expiration requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceTrialExpirationRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceTrialExpirationRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("expiration_source", re.compile(r"\b(?:expiration source|expiration date|trial end date|trial expires?|expiry source)\b", re.I), ("expiration source",), {"expiration source": re.compile(r"\b(?:billing|subscription|crm|created_at|start date|end date|truth)\b", re.I)}),
    KeywordRequirementSpec("grace_period", re.compile(r"\b(?:grace period|grace window|post[- ]?trial grace|extension window)\b", re.I), ("grace period",), {"grace period": re.compile(r"\b(?:days?|hours?|until|extension|window|\d+)\b", re.I)}),
    KeywordRequirementSpec("notification_cadence", re.compile(r"\b(?:notification cadence|reminder cadence|expiration reminders?|trial reminders?|notice schedule)\b", re.I), ("notification cadence",), {"notification cadence": re.compile(r"\b(?:days?|weekly|email|in-app|sms|before|after|\d+)\b", re.I)}),
    KeywordRequirementSpec("conversion_cta", re.compile(r"\b(?:conversion cta|upgrade cta|convert button|purchase cta|upgrade prompt)\b", re.I), ("conversion CTA",), {"conversion CTA": re.compile(r"\b(?:upgrade|checkout|buy|purchase|contact sales|cta|button|link)\b", re.I)}),
    KeywordRequirementSpec("data_retention", re.compile(r"\b(?:data retention|retain trial data|delete trial data|post[- ]?expiry data|retention after expiry)\b", re.I), ("data retention",), {"data retention": re.compile(r"\b(?:retain|delete|archive|days?|months?|export|purge|\d+)\b", re.I)}),
    KeywordRequirementSpec("feature_lock_behavior", re.compile(r"\b(?:feature lock behavior|feature lock|lock behavior|access lock|read[- ]?only mode)\b", re.I), ("feature lock behavior",), {"feature lock behavior": re.compile(r"\b(?:read-only|locked|disable|block|access|entitlement)\b", re.I)}),
    KeywordRequirementSpec("support_exception", re.compile(r"\b(?:support exception|manual exception|support extension|override trial|exception workflow)\b", re.I), ("support exception",), {"support exception": re.compile(r"\b(?:support|admin|override|extension|approval|exception|agent)\b", re.I)}),
    KeywordRequirementSpec("analytics_reporting", re.compile(r"\b(?:analytics reporting|expiration analytics|trial reporting|conversion reporting|expiry dashboard)\b", re.I), ("analytics reporting",), {"analytics reporting": re.compile(r"\b(?:dashboard|report|analytics|conversion|cohort|funnel|metric)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:trial expiration|trial expiry|free trial expiration|trial end|expired trial)\b", re.I)
_STRUCTURED = re.compile(r"(?:trial|expiration|expiry|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:trial expiration|trial expiry|free trial expiration|trial end)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:trial expiration|trial expiry|free trial expiration|trial end)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_expiration_source": ("expiration source",), "missing_notification_cadence": ("notification cadence",), "missing_feature_lock_behavior": ("feature lock behavior",)}


def build_source_trial_expiration_requirements(source: Any) -> SourceTrialExpirationRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Trial Expiration Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_trial_expiration_requirements(source: Any) -> SourceTrialExpirationRequirementsReport:
    return build_source_trial_expiration_requirements(source)


def generate_source_trial_expiration_requirements(source: Any) -> SourceTrialExpirationRequirementsReport:
    return build_source_trial_expiration_requirements(source)


def derive_source_trial_expiration_requirements(source: Any) -> SourceTrialExpirationRequirementsReport:
    return build_source_trial_expiration_requirements(source)


def summarize_source_trial_expiration_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceTrialExpirationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_trial_expiration_requirements(source_or_result).summary


def source_trial_expiration_requirements_to_dict(report: SourceTrialExpirationRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_trial_expiration_requirements_to_dict.__test__ = False


def source_trial_expiration_requirements_to_dicts(requirements: SourceTrialExpirationRequirementsReport | list[SourceTrialExpirationRequirement] | tuple[SourceTrialExpirationRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceTrialExpirationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_trial_expiration_requirements_to_dicts.__test__ = False


def source_trial_expiration_requirements_to_markdown(report: SourceTrialExpirationRequirementsReport) -> str:
    return report.to_markdown()


source_trial_expiration_requirements_to_markdown.__test__ = False
