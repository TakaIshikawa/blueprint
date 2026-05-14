"""Extract source-level notification digest requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceNotificationDigestRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceNotificationDigestRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("digest_schedule", re.compile(r"\b(?:digest schedule|send schedule|digest cadence|daily digest|weekly digest)\b", re.I), ("schedule",), {"schedule": re.compile(r"\b(?:daily|weekly|monthly|cron|time zone|hour|day|send at|\d+)\b", re.I)}),
    KeywordRequirementSpec("recipient_segmentation", re.compile(r"\b(?:recipient segmentation|audience segment|recipient segment|user segment|target recipients?)\b", re.I), ("segment rule",), {"segment rule": re.compile(r"\b(?:role|plan|team|cohort|segment|recipient|locale|tenant)\b", re.I)}),
    KeywordRequirementSpec("channel_delivery", re.compile(r"\b(?:channel delivery|delivery channel|email digest|slack digest|push digest)\b", re.I), ("delivery channel",), {"delivery channel": re.compile(r"\b(?:email|slack|push|sms|in-app|channel)\b", re.I)}),
    KeywordRequirementSpec("grouping_deduplication", re.compile(r"\b(?:grouping|deduplication|dedupe|group similar|collapse notifications?)\b", re.I), ("grouping rule",), {"grouping rule": re.compile(r"\b(?:group|dedupe|collapse|thread|entity|duplicate|summary)\b", re.I)}),
    KeywordRequirementSpec("preference_controls", re.compile(r"\b(?:preference controls?|digest preferences?|frequency preference|notification preference)\b", re.I), ("preference control",), {"preference control": re.compile(r"\b(?:frequency|opt in|opt out|settings|user control)\b", re.I)}),
    KeywordRequirementSpec("unsubscribe_suppression", re.compile(r"\b(?:unsubscribe|suppression|suppressions?|do not send|opt out)\b", re.I), ("suppression rule",), {"suppression rule": re.compile(r"\b(?:opt out|bounce|complaint|do not send|global)\b", re.I)}),
    KeywordRequirementSpec("preview_testing", re.compile(r"\b(?:preview testing|digest preview|test send|qa preview|sample digest)\b", re.I), ("preview path",), {"preview path": re.compile(r"\b(?:preview|test send|sample|qa|seed list|render)\b", re.I)}),
    KeywordRequirementSpec("delivery_metrics", re.compile(r"\b(?:delivery metrics?|digest metrics?|open rate|click rate|delivery dashboard)\b", re.I), ("metric definition",), {"metric definition": re.compile(r"\b(?:delivered|open rate|click rate|bounce|dashboard|metric|conversion)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:notification digest|digest notification|email digest|weekly digest|daily digest)\b", re.I)
_STRUCTURED = re.compile(r"(?:notification|digest|email|delivery|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:notification digest|digest notification|email digest|weekly digest|daily digest)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:notification digest|digest notification|email digest|weekly digest|daily digest)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_schedule": ("schedule",), "missing_preference_controls": ("preference control",), "missing_suppression": ("suppression rule",)}


def build_source_notification_digest_requirements(source: Any) -> SourceNotificationDigestRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Notification Digest Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_notification_digest_requirements(source: Any) -> SourceNotificationDigestRequirementsReport:
    return build_source_notification_digest_requirements(source)


def generate_source_notification_digest_requirements(source: Any) -> SourceNotificationDigestRequirementsReport:
    return build_source_notification_digest_requirements(source)


def derive_source_notification_digest_requirements(source: Any) -> SourceNotificationDigestRequirementsReport:
    return build_source_notification_digest_requirements(source)


def summarize_source_notification_digest_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceNotificationDigestRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_notification_digest_requirements(source_or_result).summary


def source_notification_digest_requirements_to_dict(report: SourceNotificationDigestRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_notification_digest_requirements_to_dict.__test__ = False


def source_notification_digest_requirements_to_dicts(requirements: SourceNotificationDigestRequirementsReport | list[SourceNotificationDigestRequirement] | tuple[SourceNotificationDigestRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceNotificationDigestRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_notification_digest_requirements_to_dicts.__test__ = False


def source_notification_digest_requirements_to_markdown(report: SourceNotificationDigestRequirementsReport) -> str:
    return report.to_markdown()


source_notification_digest_requirements_to_markdown.__test__ = False
