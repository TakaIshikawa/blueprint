"""Extract source-level background job retry requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceBackgroundJobRetryRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceBackgroundJobRetryRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("retry_policy", re.compile(r"\b(?:retry policy|retry rules?|retryable jobs?|retry behavior|job retries?)\b", re.I), ("retry condition",), {"retry condition": re.compile(r"\b(?:transient|timeout|5xx|network|retryable|condition|exception)\b", re.I)}),
    KeywordRequirementSpec("backoff_jitter", re.compile(r"\b(?:backoff|jitter|exponential delay|retry delay|delay schedule)\b", re.I), ("backoff and jitter",), {"backoff and jitter": re.compile(r"\b(?:exponential|linear|jitter|delay|seconds?|minutes?|multiplier|\d+)\b", re.I)}),
    KeywordRequirementSpec("max_attempts", re.compile(r"\b(?:max attempts?|maximum attempts?|attempt limit|retry limit|try up to)\b", re.I), ("attempt limit",), {"attempt limit": re.compile(r"\b(?:\d+|limit|maximum|max attempts?|attempts?)\b", re.I)}),
    KeywordRequirementSpec("idempotency", re.compile(r"\b(?:idempotency|idempotent|dedupe key|idempotency key|safe replay)\b", re.I), ("idempotency safeguard",), {"idempotency safeguard": re.compile(r"\b(?:idempotency key|dedupe key|unique key|transaction id|safe replay|side effects?)\b", re.I)}),
    KeywordRequirementSpec("dead_letter_handling", re.compile(r"\b(?:dead[- ]letter|dlq|parking queue|failed jobs? queue)\b", re.I), ("dead-letter destination",), {"dead-letter destination": re.compile(r"\b(?:dlq|dead[- ]letter queue|parking queue|failed jobs? queue|topic|table)\b", re.I)}),
    KeywordRequirementSpec("poison_message_detection", re.compile(r"\b(?:poison message|poison pill|stuck job|permanent failure|non[- ]retryable)\b", re.I), ("poison detection",), {"poison detection": re.compile(r"\b(?:poison|permanent|non[- ]retryable|validation error|threshold|detect)\b", re.I)}),
    KeywordRequirementSpec("retry_observability", re.compile(r"\b(?:retry observability|retry metrics?|retry alerts?|retry dashboard|attempt metrics?)\b", re.I), ("retry telemetry",), {"retry telemetry": re.compile(r"\b(?:metric|dashboard|alert|log|trace|attempt count|failure rate)\b", re.I)}),
    KeywordRequirementSpec("manual_replay", re.compile(r"\b(?:manual replay|manual retry|operator replay|rerun job|requeue)\b", re.I), ("manual replay path",), {"manual replay path": re.compile(r"\b(?:manual|operator|admin|replay|rerun|requeue|approval)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:background job retry|job retry|retry policy|worker retry|queue retry|background jobs?)\b", re.I)
_STRUCTURED = re.compile(r"(?:background|job|retry|queue|worker|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:background job retry|job retry|worker retry|queue retry)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:background job retry|job retry|worker retry|queue retry)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_backoff": ("backoff and jitter",), "missing_idempotency": ("idempotency safeguard",), "missing_dead_letter": ("dead-letter destination",)}


def build_source_background_job_retry_requirements(source: Any) -> SourceBackgroundJobRetryRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Background Job Retry Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_background_job_retry_requirements(source: Any) -> SourceBackgroundJobRetryRequirementsReport:
    return build_source_background_job_retry_requirements(source)


def generate_source_background_job_retry_requirements(source: Any) -> SourceBackgroundJobRetryRequirementsReport:
    return build_source_background_job_retry_requirements(source)


def derive_source_background_job_retry_requirements(source: Any) -> SourceBackgroundJobRetryRequirementsReport:
    return build_source_background_job_retry_requirements(source)


def summarize_source_background_job_retry_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceBackgroundJobRetryRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_background_job_retry_requirements(source_or_result).summary


def source_background_job_retry_requirements_to_dict(report: SourceBackgroundJobRetryRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_background_job_retry_requirements_to_dict.__test__ = False


def source_background_job_retry_requirements_to_dicts(requirements: SourceBackgroundJobRetryRequirementsReport | list[SourceBackgroundJobRetryRequirement] | tuple[SourceBackgroundJobRetryRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceBackgroundJobRetryRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_background_job_retry_requirements_to_dicts.__test__ = False


def source_background_job_retry_requirements_to_markdown(report: SourceBackgroundJobRetryRequirementsReport) -> str:
    return report.to_markdown()


source_background_job_retry_requirements_to_markdown.__test__ = False
