"""Plan third-party webhook consumer readiness for execution tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskThirdPartyWebhookReadinessPlan = SimpleReadinessPlan
TaskThirdPartyWebhookReadinessRecord = SimpleReadinessRecord

_SIGNALS = {
    "third_party_webhook": re.compile(
        r"\b(?:third[- ]party webhook|provider webhook|vendor webhook|external webhook|webhook consumer|"
        r"consume webhooks?|webhook endpoint|incoming webhook)\b",
        re.I,
    ),
    "provider_event": re.compile(
        r"\b(?:stripe|github|shopify|slack|twilio|sendgrid|adyen|checkout\.com|provider event|vendor event)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "third_party_webhook": re.compile(r"(?:webhook|webhooks|incoming[_-]?hook|provider[_-]?events?)", re.I),
    "provider_event": re.compile(r"(?:stripe|github|shopify|slack|twilio|sendgrid|adyen)", re.I),
}
_CRITERIA = {
    "provider_event_scope": re.compile(
        r"\b(?:provider|vendor|stripe|github|shopify|slack|twilio|sendgrid|event scope|event types?|"
        r"webhook events?|subscribed events?)\b",
        re.I,
    ),
    "signature_verification": re.compile(
        r"\b(?:signature verification|verify signature|webhook signature|hmac|signing secret|"
        r"timestamp tolerance|replay attack|authenticity)\b",
        re.I,
    ),
    "idempotency_deduplication": re.compile(
        r"\b(?:idempotenc|dedupe|deduplication|duplicate event|event id|delivery id|processed events?|exactly once)\b",
        re.I,
    ),
    "retry_behavior": re.compile(
        r"\b(?:retry|retries|retry behavior|redelivery|backoff|retry-after|timeout|acknowledg|2xx|non[- ]2xx)\b",
        re.I,
    ),
    "payload_schema_handling": re.compile(
        r"\b(?:payload schema|schema validation|event schema|versioned payload|unknown fields?|"
        r"malformed payload|deserialize|json schema|contract test)\b",
        re.I,
    ),
    "dead_letter_replay": re.compile(
        r"\b(?:dead[- ]letter|dlq|replay|manual replay|failed event queue|parking lot|reprocess|re-ingest)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|metrics?|logs?|traces?|alerts?|dashboard|delivery status|failure reason|webhook audit)\b",
        re.I,
    ),
    "security_owner": re.compile(
        r"\b(?:security owner|owner|dri|owning team|on[- ]call|security review|appsec|integration owner)\b",
        re.I,
    ),
    "validation_evidence": re.compile(
        r"\b(?:tests?|test coverage|unit tests?|integration tests?|fixture|fixtures|contract tests?|"
        r"signature tests?|idempotency tests?|replay tests?|validation command|pytest)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "provider_event_scope": "Identify the provider and exact webhook event types in scope.",
    "signature_verification": "Verify webhook signatures, timestamp tolerance, and signing secret handling.",
    "idempotency_deduplication": "Add idempotency and deduplication using stable event or delivery identifiers.",
    "retry_behavior": "Define acknowledgement, timeout, provider retry, redelivery, and backoff behavior.",
    "payload_schema_handling": "Validate payload schemas, versioning, malformed payloads, and unknown fields.",
    "dead_letter_replay": "Provide dead-letter, failed-event, replay, or reprocessing paths.",
    "observability": "Instrument metrics, logs, traces, alerts, dashboards, and failure diagnostics.",
    "security_owner": "Name the security, integration, product, or on-call owner.",
    "validation_evidence": "Add tests or validation commands for signature, idempotency, schema, retry, and replay behavior.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:third[- ]party webhook|provider webhook|webhook consumer|incoming webhook)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


def build_task_third_party_webhook_readiness_plan(source: Any) -> TaskThirdPartyWebhookReadinessPlan:
    """Build third-party webhook consumer readiness findings for relevant tasks."""
    return build_simple_readiness_plan(
        source,
        title="Task Third Party Webhook Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_third_party_webhook_readiness(source: Any) -> TaskThirdPartyWebhookReadinessPlan:
    return build_task_third_party_webhook_readiness_plan(source)


def summarize_task_third_party_webhook_readiness(source: Any) -> TaskThirdPartyWebhookReadinessPlan:
    return build_task_third_party_webhook_readiness_plan(source)


def extract_task_third_party_webhook_readiness(source: Any) -> TaskThirdPartyWebhookReadinessPlan:
    return build_task_third_party_webhook_readiness_plan(source)


def generate_task_third_party_webhook_readiness(source: Any) -> TaskThirdPartyWebhookReadinessPlan:
    return build_task_third_party_webhook_readiness_plan(source)


def recommend_task_third_party_webhook_readiness(source: Any) -> TaskThirdPartyWebhookReadinessPlan:
    return build_task_third_party_webhook_readiness_plan(source)


def task_third_party_webhook_readiness_plan_to_dict(result: TaskThirdPartyWebhookReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_third_party_webhook_readiness_plan_to_dict.__test__ = False


def task_third_party_webhook_readiness_plan_to_dicts(result: Any) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [item.to_dict() for item in result]


task_third_party_webhook_readiness_plan_to_dicts.__test__ = False


def task_third_party_webhook_readiness_plan_to_markdown(result: TaskThirdPartyWebhookReadinessPlan) -> str:
    return result.to_markdown()


task_third_party_webhook_readiness_plan_to_markdown.__test__ = False
