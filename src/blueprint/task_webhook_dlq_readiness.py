"""Assess task-level readiness for webhook DLQ and failed-delivery drain work."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._task_safeguard_readiness import (
    TaskSafeguardReadinessPlan,
    TaskSafeguardReadinessRecord,
    build_task_safeguard_readiness_plan,
)


TaskWebhookDlqReadinessRecord = TaskSafeguardReadinessRecord
TaskWebhookDlqReadinessPlan = TaskSafeguardReadinessPlan

_SIGNALS = {
    "webhook_dlq": re.compile(r"\b(?:webhook dlq|webhook dead[- ]letter|dead[- ]letter queue|dlq)\b", re.I),
    "failed_callback": re.compile(r"\b(?:failed callbacks?|failed webhook deliveries?|delivery failures?|callback failures?)\b", re.I),
    "retry_exhaustion": re.compile(r"\b(?:retry exhaustion|retries exhausted|max attempts?|terminal failure|retry limit)\b", re.I),
    "poison_payload": re.compile(r"\b(?:poison payload|poison event|poison message|malformed payload|quarantine)\b", re.I),
    "replay": re.compile(r"\b(?:replay|redeliver|redelivery|resend webhook|reprocess event)\b", re.I),
    "drain": re.compile(r"\b(?:drain|dlq drain|drain job|drain worker|failed delivery drain)\b", re.I),
    "retention": re.compile(r"\b(?:retention|ttl|expiry|expiration|purge old failures)\b", re.I),
}
_PATH_SIGNALS = {
    "webhook_dlq": re.compile(r"webhook.*(?:dlq|dead[_-]?letter)|(?:dlq|dead[_-]?letter).*webhook", re.I),
    "failed_callback": re.compile(r"failed|failure|callback|delivery", re.I),
    "retry_exhaustion": re.compile(r"retry|attempt|exhaust", re.I),
    "poison_payload": re.compile(r"poison|quarantine|malformed", re.I),
    "replay": re.compile(r"replay|redeliver|resend|reprocess", re.I),
    "drain": re.compile(r"drain", re.I),
    "retention": re.compile(r"retention|ttl|expiry|expiration|purge", re.I),
}
_SAFEGUARDS = {
    "dlq_routing": re.compile(r"\b(?:dlq routing|route to dlq|dead[- ]letter routing|failed delivery queue|failure queue)\b", re.I),
    "retry_exhaustion_policy": re.compile(r"\b(?:retry exhaustion policy|max attempts?|terminal failure policy|retry limit|final attempt)\b", re.I),
    "poison_isolation": re.compile(r"\b(?:poison isolation|quarantine poison|poison payload isolation|malformed payload quarantine)\b", re.I),
    "replay_tooling": re.compile(r"\b(?:replay tooling|replay tool|operator replay|admin replay|redelivery tool|resend command)\b", re.I),
    "idempotent_drain": re.compile(r"\b(?:idempotent drain|idempotent replay|dedupe|duplicate delivery|idempotency key|safe redelivery)\b", re.I),
    "retention_policy": re.compile(r"\b(?:retention policy|ttl policy|expiry policy|expiration policy|purge schedule)\b", re.I),
    "alerting": re.compile(r"\b(?:alerting|alerts?|monitoring|metrics?|queue depth|failure rate|dlq depth)\b", re.I),
    "runbook": re.compile(r"\b(?:runbook|playbook|operator guide|on[- ]call guide|drain procedure|support guide)\b", re.I),
}
_GUIDANCE = {
    "dlq_routing": "Route terminal webhook delivery failures into a dedicated DLQ with enough context for recovery.",
    "retry_exhaustion_policy": "Define retry exhaustion, max attempts, final failure states, and handoff into DLQ.",
    "poison_isolation": "Isolate poison payloads so malformed events cannot block normal DLQ drain work.",
    "replay_tooling": "Provide operator replay or redelivery tooling for selected failed webhook events.",
    "idempotent_drain": "Make drain and replay behavior idempotent with dedupe protection for duplicate callbacks.",
    "retention_policy": "Document retention, TTL, purge, and archival policy for failed delivery records.",
    "alerting": "Alert on DLQ depth, failed callback rate, retry exhaustion, and stuck drain jobs.",
    "runbook": "Publish the operator runbook for triage, replay, drain, retention, and partner escalation.",
}
_HIGH_IMPACT = {"webhook_dlq", "failed_callback", "retry_exhaustion", "poison_payload", "drain"}


def build_task_webhook_dlq_readiness_plan(source: Any) -> TaskWebhookDlqReadinessPlan:
    return build_task_safeguard_readiness_plan(
        source,
        title="Task Webhook DLQ Readiness",
        task_count_label="webhook_dlq_task_count",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        safeguard_patterns=_SAFEGUARDS,
        safeguard_guidance=_GUIDANCE,
        high_impact_signals=_HIGH_IMPACT,
    )


def analyze_task_webhook_dlq_readiness(source: Any) -> TaskWebhookDlqReadinessPlan:
    return build_task_webhook_dlq_readiness_plan(source)


def extract_task_webhook_dlq_readiness(source: Any) -> TaskWebhookDlqReadinessPlan:
    return build_task_webhook_dlq_readiness_plan(source)


def generate_task_webhook_dlq_readiness(source: Any) -> TaskWebhookDlqReadinessPlan:
    return build_task_webhook_dlq_readiness_plan(source)


def derive_task_webhook_dlq_readiness(source: Any) -> TaskWebhookDlqReadinessPlan:
    return build_task_webhook_dlq_readiness_plan(source)


def summarize_task_webhook_dlq_readiness(source: Any) -> TaskWebhookDlqReadinessPlan:
    return build_task_webhook_dlq_readiness_plan(source)


def recommend_task_webhook_dlq_readiness(source: Any) -> TaskWebhookDlqReadinessPlan:
    return build_task_webhook_dlq_readiness_plan(source)


def task_webhook_dlq_readiness_plan_to_dict(report: TaskWebhookDlqReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_webhook_dlq_readiness_plan_to_dict.__test__ = False


def task_webhook_dlq_readiness_plan_to_dicts(
    report: TaskWebhookDlqReadinessPlan | Iterable[TaskWebhookDlqReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(report, TaskSafeguardReadinessPlan):
        return report.to_dicts()
    return [record.to_dict() for record in report]


task_webhook_dlq_readiness_plan_to_dicts.__test__ = False
task_webhook_dlq_readiness_to_dicts = task_webhook_dlq_readiness_plan_to_dicts
task_webhook_dlq_readiness_to_dicts.__test__ = False


def task_webhook_dlq_readiness_plan_to_markdown(report: TaskWebhookDlqReadinessPlan) -> str:
    return report.to_markdown()


task_webhook_dlq_readiness_plan_to_markdown.__test__ = False
