"""Recommend replay-safety safeguards for event ingestion execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


WebhookReplaySafeguard = Literal[
    "idempotency_key_handling",
    "duplicate_event_tests",
    "signature_timestamp_validation",
    "dead_letter_handling",
    "replay_window_limits",
    "audit_evidence",
]
WebhookReplayRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[WebhookReplayRisk, int] = {"high": 0, "medium": 1, "low": 2}
_SAFEGUARD_ORDER: tuple[WebhookReplaySafeguard, ...] = (
    "idempotency_key_handling",
    "duplicate_event_tests",
    "signature_timestamp_validation",
    "dead_letter_handling",
    "replay_window_limits",
    "audit_evidence",
)
_INGESTION_RE = re.compile(
    r"\b(?:webhooks?|callbacks?|event payloads?|event ingestion|external event|"
    r"incoming event|inbound event|queue consumers?|consumer worker|message consumer|"
    r"event consumers?|event receiver|receiver endpoint|subscription event)\b",
    re.I,
)
_PATH_INGESTION_RE = re.compile(
    r"(?:^|/)(?:webhooks?|callbacks?|consumers?|event[_-]?ingestion|event[_-]?receivers?|"
    r"queue[_-]?consumers?|dlq)(?:/|\.|_|-|$)",
    re.I,
)
_EXTERNAL_RE = re.compile(
    r"\b(?:external|third[- ]party|partner|provider|vendor|public webhook|incoming webhook|"
    r"stripe|shopify|slack|github|twilio|sendgrid|adyen|paypal|salesforce|zendesk|"
    r"hubspot|intercom|segment|netsuite|quickbooks)\b",
    re.I,
)
_PROVIDER_PATTERNS: dict[str, re.Pattern[str]] = {
    "Adyen": re.compile(r"\badyen\b", re.I),
    "GitHub": re.compile(r"\bgithub\b", re.I),
    "HubSpot": re.compile(r"\bhubspot\b", re.I),
    "Intercom": re.compile(r"\bintercom\b", re.I),
    "NetSuite": re.compile(r"\bnetsuite\b", re.I),
    "PayPal": re.compile(r"\bpaypal\b", re.I),
    "QuickBooks": re.compile(r"\bquickbooks\b", re.I),
    "Salesforce": re.compile(r"\bsalesforce\b", re.I),
    "Segment": re.compile(r"\bsegment\b", re.I),
    "SendGrid": re.compile(r"\bsendgrid\b", re.I),
    "Shopify": re.compile(r"\bshopify\b", re.I),
    "Slack": re.compile(r"\bslack\b", re.I),
    "Stripe": re.compile(r"\bstripe\b", re.I),
    "Twilio": re.compile(r"\btwilio\b", re.I),
    "Zendesk": re.compile(r"\bzendesk\b", re.I),
}
_SAFEGUARD_PATTERNS: dict[WebhookReplaySafeguard, re.Pattern[str]] = {
    "idempotency_key_handling": re.compile(
        r"\b(?:idempotenc|dedup(?:e|lication)?|duplicate prevention|event id|event_id|"
        r"idempotency key)\b",
        re.I,
    ),
    "duplicate_event_tests": re.compile(
        r"\b(?:(?:duplicate|replayed?) event(?:s)?|duplicate delivery|redelivery|"
        r"at[- ]least[- ]once|duplicate.*test|test.*duplicate|replay.*test)\b",
        re.I,
    ),
    "signature_timestamp_validation": re.compile(
        r"\b(?:signature|signed payload|hmac|secret validation|timestamp validation|"
        r"timestamp tolerance|clock skew)\b",
        re.I,
    ),
    "dead_letter_handling": re.compile(
        r"\b(?:dead[- ]letter|dlq|poison message|quarantine|failed event queue)\b",
        re.I,
    ),
    "replay_window_limits": re.compile(
        r"\b(?:replay window|timestamp window|freshness window|expiration window|"
        r"max age|older than \d+|stale event|replay limit)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit|trace|observability|log(?:ging)?|metric|evidence|receipt|"
        r"delivery id|event receipt)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class TaskWebhookReplaySafetyRecommendation:
    """Replay-safety guidance for one webhook or event-ingestion task."""

    task_id: str
    title: str
    ingestion_surfaces: tuple[str, ...]
    missing_safeguards: tuple[WebhookReplaySafeguard, ...]
    risk_level: WebhookReplayRisk = "medium"
    external_service_signals: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "ingestion_surfaces": list(self.ingestion_surfaces),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "external_service_signals": list(self.external_service_signals),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskWebhookReplaySafetyPlan:
    """Task-level replay-safety recommendations for webhook ingestion work."""

    plan_id: str | None = None
    recommendations: tuple[TaskWebhookReplaySafetyRecommendation, ...] = field(default_factory=tuple)
    webhook_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "recommendations": [
                recommendation.to_dict() for recommendation in self.recommendations
            ],
            "webhook_task_ids": list(self.webhook_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return recommendation records as plain dictionaries."""
        return [recommendation.to_dict() for recommendation in self.recommendations]


def build_task_webhook_replay_safety_plan(
    source: Mapping[str, Any] | ExecutionPlan,
) -> TaskWebhookReplaySafetyPlan:
    """Recommend replay-safety safeguards for webhook and event ingestion tasks."""
    plan = _plan_payload(source)
    tasks = _task_payloads(plan.get("tasks"))
    recommendations = [
        recommendation
        for index, task in enumerate(tasks, start=1)
        if (recommendation := _recommendation(task, index)) is not None
    ]
    recommendations.sort(
        key=lambda item: (_RISK_ORDER[item.risk_level], item.task_id, item.title.casefold())
    )
    result = tuple(recommendations)
    risk_counts = {
        risk: sum(1 for item in result if item.risk_level == risk) for risk in _RISK_ORDER
    }

    return TaskWebhookReplaySafetyPlan(
        plan_id=_optional_text(plan.get("id")),
        recommendations=result,
        webhook_task_ids=tuple(item.task_id for item in result),
        summary={
            "task_count": len(tasks),
            "webhook_task_count": len(result),
            "high_risk_count": risk_counts["high"],
            "medium_risk_count": risk_counts["medium"],
            "low_risk_count": risk_counts["low"],
            "missing_safeguard_count": sum(len(item.missing_safeguards) for item in result),
        },
    )


def task_webhook_replay_safety_plan_to_dict(
    result: TaskWebhookReplaySafetyPlan,
) -> dict[str, Any]:
    """Serialize a webhook replay-safety plan to a plain dictionary."""
    return result.to_dict()


task_webhook_replay_safety_plan_to_dict.__test__ = False


def recommend_task_webhook_replay_safety(
    source: Mapping[str, Any] | ExecutionPlan,
) -> TaskWebhookReplaySafetyPlan:
    """Compatibility alias for building webhook replay-safety recommendations."""
    return build_task_webhook_replay_safety_plan(source)


def _recommendation(
    task: Mapping[str, Any],
    index: int,
) -> TaskWebhookReplaySafetyRecommendation | None:
    evidence = _ingestion_evidence(task)
    if not evidence:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    context = _task_context(task)
    missing = _missing_safeguards(context)
    external_signals = _external_service_signals(context)
    return TaskWebhookReplaySafetyRecommendation(
        task_id=task_id,
        title=title,
        ingestion_surfaces=_ingestion_surfaces(task, context),
        missing_safeguards=missing,
        risk_level=_risk_level(missing, external_signals),
        external_service_signals=external_signals,
        evidence=tuple(_dedupe(evidence)),
    )


def _ingestion_evidence(task: Mapping[str, Any]) -> list[str]:
    evidence: list[str] = []
    for path in _strings(task.get("files_or_modules") or task.get("files")):
        if _PATH_INGESTION_RE.search(_normalized_path(path)):
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _task_texts(task):
        if _INGESTION_RE.search(text):
            evidence.append(_evidence_snippet(source_field, text))

    for source_field, text in _metadata_texts(task.get("metadata")):
        if _INGESTION_RE.search(f"{source_field} {text}"):
            evidence.append(_evidence_snippet(source_field, text))

    for source_field, text in _tag_texts(task):
        if _INGESTION_RE.search(text):
            evidence.append(_evidence_snippet(source_field, text))

    return evidence


def _ingestion_surfaces(task: Mapping[str, Any], context: str) -> tuple[str, ...]:
    surfaces: list[str] = []
    if re.search(r"\bwebhooks?\b", context, re.I):
        surfaces.append(f"Webhook receiver: {_surface_name(task)}")
    if re.search(r"\bcallbacks?\b", context, re.I):
        surfaces.append(f"Callback handler: {_surface_name(task)}")
    if re.search(r"\b(?:queue consumers?|message consumer|consumer worker)\b", context, re.I):
        surfaces.append(f"Queue consumer: {_surface_name(task)}")
    if re.search(r"\b(?:event payloads?|event ingestion|external event|incoming event|inbound event|event receiver)\b", context, re.I):
        surfaces.append(f"Event ingestion: {_surface_name(task)}")
    return tuple(_dedupe(surfaces)) or (f"Event ingestion: {_surface_name(task)}",)


def _missing_safeguards(context: str) -> tuple[WebhookReplaySafeguard, ...]:
    return tuple(
        safeguard
        for safeguard in _SAFEGUARD_ORDER
        if not _SAFEGUARD_PATTERNS[safeguard].search(context)
    )


def _risk_level(
    missing_safeguards: tuple[WebhookReplaySafeguard, ...],
    external_signals: tuple[str, ...],
) -> WebhookReplayRisk:
    missing = set(missing_safeguards)
    if external_signals and (
        "idempotency_key_handling" in missing
        or "signature_timestamp_validation" in missing
    ):
        return "high"
    if len(missing_safeguards) >= 4:
        return "medium"
    if external_signals and missing_safeguards:
        return "medium"
    return "low"


def _external_service_signals(context: str) -> tuple[str, ...]:
    signals: list[str] = []
    for name, pattern in _PROVIDER_PATTERNS.items():
        if pattern.search(context):
            signals.append(name)
    if re.search(r"\bthird[- ]party\b", context, re.I):
        signals.append("third-party")
    if re.search(r"\bpartner\b", context, re.I):
        signals.append("partner")
    if re.search(r"\bprovider|vendor\b", context, re.I):
        signals.append("provider")
    if re.search(r"\bexternal\b", context, re.I):
        signals.append("external")
    if re.search(r"\bpublic webhook|incoming webhook\b", context, re.I):
        signals.append("incoming webhook")
    if _EXTERNAL_RE.search(context) and not signals:
        signals.append("external")
    return tuple(_dedupe(signals))


def _surface_name(task: Mapping[str, Any]) -> str:
    metadata = task.get("metadata")
    for value in (
        task.get("surface"),
        task.get("ingestion_surface"),
        metadata.get("surface") if isinstance(metadata, Mapping) else None,
        metadata.get("ingestion_surface") if isinstance(metadata, Mapping) else None,
    ):
        if text := _optional_text(value):
            return text
    return _optional_text(task.get("title")) or _optional_text(task.get("id")) or "event ingestion"


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "test_command",
        "risk_level",
    ):
        text = _optional_text(task.get(field_name))
        if text:
            texts.append((field_name, text))
    for index, text in enumerate(_strings(task.get("acceptance_criteria"))):
        texts.append((f"acceptance_criteria[{index}]", text))
    for index, text in enumerate(_strings(task.get("depends_on"))):
        texts.append((f"depends_on[{index}]", text))
    return texts


def _tag_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in ("tags", "labels"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _task_context(task: Mapping[str, Any]) -> str:
    values = [text for _, text in _task_texts(task)]
    values.extend(_strings(task.get("files_or_modules") or task.get("files")))
    values.extend(text for _, text in _metadata_texts(task.get("metadata")))
    values.extend(text for _, text in _tag_texts(task))
    return " ".join(values)


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


def _evidence_snippet(source_field: str, text: str) -> str:
    return f"{source_field}: {_text(text)[:160]}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "TaskWebhookReplaySafetyPlan",
    "TaskWebhookReplaySafetyRecommendation",
    "WebhookReplayRisk",
    "WebhookReplaySafeguard",
    "build_task_webhook_replay_safety_plan",
    "recommend_task_webhook_replay_safety",
    "task_webhook_replay_safety_plan_to_dict",
]
