"""Analyze notification digest readiness for execution-plan tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, build_simple_readiness_plan


_SIGNAL_PATTERNS = {
    "digest": re.compile(r"\b(?:notification[_\s-]+digest|digest[_\s-]+notification|daily[_\s-]+digest|weekly[_\s-]+digest|summary[_\s-]+email|email[_\s-]+summary|digest[_\s-]+delivery)\b", re.I),
    "batching": re.compile(r"\b(?:batch(?:ed|ing)?[_\s-]+notifications?|notification[_\s-]+batch(?:ing)?|roll[_\s-]?up|group(?:ed|ing)[_\s-]+notifications?)\b", re.I),
    "cadence": re.compile(r"\b(?:digest|summary|batched).{0,80}\b(?:cadence|schedule|daily|weekly|timezone|time[_\s-]+zone)\b", re.I),
}
_PATH_SIGNAL_PATTERNS = {
    "notification_digest_path": re.compile(r"\b(?:notifications?|email|mail|messaging).*(?:digest|summary|batch|roll[_\s-]*up)\b|\b(?:digest|summary|batch|roll[_\s-]*up).*(?:notifications?|email|mail|messaging)\b", re.I),
}
_CRITERIA_PATTERNS = {
    "recipient_selection": re.compile(r"\b(?:recipient[_\s-]+selection|eligible[_\s-]+recipients?|audience|subscribers?|user[_\s-]+selection|segment)\b", re.I),
    "grouping_rules": re.compile(r"\b(?:grouping[_\s-]+rules?|group[_\s-]+by|roll[_\s-]?up[_\s-]+rules?|dedupe|thread|bucket|summar(?:y|ize)[_\s-]+by)\b", re.I),
    "cadence_timezone": re.compile(r"\b(?:cadence|schedule|daily|weekly|hourly|timezone|time[_\s-]+zone|local[_\s-]+time|send[_\s-]+window)\b", re.I),
    "channel_template": re.compile(r"\b(?:channel|email|sms|push|in[_\s-]+app|template|subject[_\s-]+line|render(?:ing)?)\b", re.I),
    "preference_opt_out": re.compile(r"\b(?:preferences?|opt[_\s-]?out|unsubscribe|suppression|quiet[_\s-]+hours|notification[_\s-]+settings)\b", re.I),
    "delivery_failure_handling": re.compile(r"\b(?:delivery[_\s-]+failure|bounce|retry|retries|backoff|dead[_\s-]+letter|dlq|provider[_\s-]+error)\b", re.I),
    "observability": re.compile(r"\b(?:observability|monitoring|metrics?|dashboard|alerts?|delivery[_\s-]+rate|open[_\s-]+rate|failure[_\s-]+rate)\b", re.I),
    "tests": re.compile(r"\b(?:tests?|unit[_\s-]+tests?|integration[_\s-]+tests?|e2e|fixture|coverage|verification)\b", re.I),
}
_GUIDANCE = {
    "recipient_selection": "Define eligible recipients, audience filters, and subscriber selection for each digest.",
    "grouping_rules": "Document grouping, deduplication, roll-up, and summary rules.",
    "cadence_timezone": "Specify digest cadence, send windows, timezone handling, and scheduling behavior.",
    "channel_template": "Name the delivery channel, template, subject, and rendering expectations.",
    "preference_opt_out": "Honor preferences, opt-outs, unsubscribe state, suppression, and quiet-hour rules.",
    "delivery_failure_handling": "Describe retry, bounce, provider-error, and DLQ handling for digest delivery.",
    "observability": "Add metrics, dashboards, and alerts for digest volume, latency, and failures.",
    "tests": "Cover recipient selection, grouping, cadence/timezone, preferences, failures, and rendering in tests.",
}


def build_task_notification_digest_readiness_plan(source: Any) -> SimpleReadinessPlan:
    """Build notification digest readiness records from task-shaped input."""
    return build_simple_readiness_plan(
        source,
        title="Task Notification Digest Readiness",
        signal_patterns=_SIGNAL_PATTERNS,
        path_signal_patterns=_PATH_SIGNAL_PATTERNS,
        criteria_patterns=_CRITERIA_PATTERNS,
        criterion_guidance=_GUIDANCE,
    )


analyze_task_notification_digest_readiness = build_task_notification_digest_readiness_plan
summarize_task_notification_digest_readiness = build_task_notification_digest_readiness_plan
generate_task_notification_digest_readiness = build_task_notification_digest_readiness_plan
extract_task_notification_digest_readiness = build_task_notification_digest_readiness_plan
recommend_task_notification_digest_readiness = build_task_notification_digest_readiness_plan


def task_notification_digest_readiness_plan_to_dict(plan: SimpleReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


def task_notification_digest_readiness_plan_to_dicts(plan: SimpleReadinessPlan) -> list[dict[str, Any]]:
    return plan.to_dicts()


def task_notification_digest_readiness_plan_to_markdown(plan: SimpleReadinessPlan) -> str:
    return plan.to_markdown()


__all__ = [
    "SimpleReadinessPlan",
    "analyze_task_notification_digest_readiness",
    "build_task_notification_digest_readiness_plan",
    "extract_task_notification_digest_readiness",
    "generate_task_notification_digest_readiness",
    "recommend_task_notification_digest_readiness",
    "summarize_task_notification_digest_readiness",
    "task_notification_digest_readiness_plan_to_dict",
    "task_notification_digest_readiness_plan_to_dicts",
    "task_notification_digest_readiness_plan_to_markdown",
]
