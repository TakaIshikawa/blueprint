"""Assess task-level readiness for webhook delivery implementation work."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


WebhookDeliverySignal = Literal[
    "webhook_delivery",
    "event_publishing",
    "signing",
    "retries",
    "idempotency",
    "dead_letter_queue",
    "delivery_logs",
    "replay",
    "tenant_scoping",
    "schema_versioning",
]
WebhookDeliverySafeguard = Literal[
    "retry_tests",
    "signature_verification_tests",
    "replay_tooling",
    "observability",
    "rate_limiting",
    "runbook",
]
WebhookDeliveryReadiness = Literal["weak", "partial", "strong"]
WebhookDeliveryImpact = Literal["low", "medium", "high"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[WebhookDeliveryReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_IMPACT_ORDER: dict[WebhookDeliveryImpact, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_ORDER: tuple[WebhookDeliverySignal, ...] = (
    "webhook_delivery",
    "event_publishing",
    "signing",
    "retries",
    "idempotency",
    "dead_letter_queue",
    "delivery_logs",
    "replay",
    "tenant_scoping",
    "schema_versioning",
)
_SAFEGUARD_ORDER: tuple[WebhookDeliverySafeguard, ...] = (
    "retry_tests",
    "signature_verification_tests",
    "replay_tooling",
    "observability",
    "rate_limiting",
    "runbook",
)
_HIGH_IMPACT_SIGNALS: frozenset[WebhookDeliverySignal] = frozenset(
    {"webhook_delivery", "event_publishing", "signing", "retries", "dead_letter_queue", "replay"}
)

_SIGNAL_PATTERNS: dict[WebhookDeliverySignal, re.Pattern[str]] = {
    "webhook_delivery": re.compile(
        r"\b(?:webhook delivery|webhook deliveries|deliver webhooks?|outbound webhook|webhook endpoint|"
        r"webhook dispatcher|webhook worker|subscriber delivery|callback delivery|partner callback)\b",
        re.I,
    ),
    "event_publishing": re.compile(
        r"\b(?:event publishing|publish events?|event publisher|domain events?|integration events?|"
        r"event bus|event stream|emit events?|outbox event|message publishing)\b",
        re.I,
    ),
    "signing": re.compile(
        r"\b(?:sign(?:ed|ing)? webhook|webhook signature|signature verification|verify signature|"
        r"hmac|sha256 signature|secret rotation|signing secret)\b",
        re.I,
    ),
    "retries": re.compile(
        r"\b(?:retr(?:y|ies)|retry policy|retry queue|exponential backoff|backoff|attempts?|"
        r"redeliver|redelivery|delivery retry)\b",
        re.I,
    ),
    "idempotency": re.compile(
        r"\b(?:idempot(?:ent|ency)|dedupe|deduplicate|duplicate delivery|delivery token|"
        r"idempotency key|exactly once|at[- ]least[- ]once)\b",
        re.I,
    ),
    "dead_letter_queue": re.compile(
        r"\b(?:dead[- ]letter|dead letter queue|dlq|poison message|failed delivery queue|"
        r"quarantine queue|failure queue)\b",
        re.I,
    ),
    "delivery_logs": re.compile(
        r"\b(?:delivery logs?|delivery history|delivery attempts?|attempt logs?|webhook logs?|"
        r"audit trail|request log|response log)\b",
        re.I,
    ),
    "replay": re.compile(
        r"\b(?:replay(?:ing)?|manual replay|redeliver|redelivery|resend webhook|retry from history|"
        r"reprocess event|replay tool)\b",
        re.I,
    ),
    "tenant_scoping": re.compile(
        r"\b(?:tenant scoping|tenant isolation|tenant scoped|account scoped|workspace scoped|"
        r"customer scoped|organization scoped|per tenant|cross[- ]tenant)\b",
        re.I,
    ),
    "schema_versioning": re.compile(
        r"\b(?:schema version(?:ing)?|event version(?:ing)?|payload version(?:ing)?|versioned payload|"
        r"contract version|backward compat(?:ible|ibility)|breaking change|webhook schema)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[WebhookDeliverySignal, re.Pattern[str]] = {
    "webhook_delivery": re.compile(r"webhooks?|callbacks?|deliver(?:y|ies)|dispatcher|subscriber", re.I),
    "event_publishing": re.compile(r"events?|publisher|event[_-]?bus|outbox|stream", re.I),
    "signing": re.compile(r"sign(?:ed|ing|ature)|hmac|secret", re.I),
    "retries": re.compile(r"retr(?:y|ies)|backoff|attempts?|redeliver", re.I),
    "idempotency": re.compile(r"idempot|dedup|duplicate", re.I),
    "dead_letter_queue": re.compile(r"dead[_-]?letter|dlq|poison|failure[_-]?queue", re.I),
    "delivery_logs": re.compile(r"logs?|history|audit|attempts?", re.I),
    "replay": re.compile(r"replay|redeliver|resend|reprocess", re.I),
    "tenant_scoping": re.compile(r"tenant|account|workspace|organization|org[_-]?scope", re.I),
    "schema_versioning": re.compile(r"schema|version|contract|payload", re.I),
}
_SAFEGUARD_PATTERNS: dict[WebhookDeliverySafeguard, re.Pattern[str]] = {
    "retry_tests": re.compile(
        r"\b(?:retry tests?|retry coverage|test retries|backoff tests?|redelivery tests?|"
        r"failed delivery tests?|attempt limit tests?)\b",
        re.I,
    ),
    "signature_verification_tests": re.compile(
        r"\b(?:signature verification tests?|signature tests?|verify signature tests?|hmac tests?|"
        r"signed webhook tests?|invalid signature tests?)\b",
        re.I,
    ),
    "replay_tooling": re.compile(
        r"\b(?:replay tool(?:ing)?|manual replay|admin replay|operator replay|redelivery tool|"
        r"resend tool|retry from history|replay command|webhook replay|replay runbook)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:observability|monitoring|metrics?|alerts?|dashboard|logs?|tracing|delivery success|"
        r"delivery failures?|failure rate|queue depth|latency|attempt count)\b",
        re.I,
    ),
    "rate_limiting": re.compile(
        r"\b(?:rate limit(?:ing)?|throttl(?:e|ing)|concurrency limit|delivery budget|queue limit|"
        r"provider limit|subscriber limit|circuit breaker|load shedding)\b",
        re.I,
    ),
    "runbook": re.compile(
        r"\b(?:runbook|playbook|operational guide|on[- ]call guide|incident guide|support guide|"
        r"escalation path|operating procedure)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[WebhookDeliverySafeguard, str] = {
    "retry_tests": "Add tests for retry attempts, backoff, terminal failures, and redelivery behavior.",
    "signature_verification_tests": "Exercise valid, invalid, expired, and rotated webhook signing secrets.",
    "replay_tooling": "Provide operator tooling to replay or redeliver events from delivery history safely.",
    "observability": "Track delivery success, failures, attempts, latency, queue depth, and alert thresholds.",
    "rate_limiting": "Bound delivery throughput with rate limits, concurrency caps, queue limits, or circuit breakers.",
    "runbook": "Document the on-call runbook for failed deliveries, replay, DLQ drain, and partner escalation.",
}


@dataclass(frozen=True, slots=True)
class TaskWebhookDeliveryReadinessRecord:
    """Webhook delivery readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[WebhookDeliverySignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[WebhookDeliverySafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[WebhookDeliverySafeguard, ...] = field(default_factory=tuple)
    readiness: WebhookDeliveryReadiness = "weak"
    impact: WebhookDeliveryImpact = "medium"
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def signals(self) -> tuple[WebhookDeliverySignal, ...]:
        return self.detected_signals

    @property
    def safeguards(self) -> tuple[WebhookDeliverySafeguard, ...]:
        return self.present_safeguards

    @property
    def recommendations(self) -> tuple[str, ...]:
        return self.recommended_checks

    @property
    def recommended_actions(self) -> tuple[str, ...]:
        return self.recommended_checks

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "impact": self.impact,
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskWebhookDeliveryReadinessPlan:
    """Plan-level webhook delivery readiness review."""

    plan_id: str | None = None
    records: tuple[TaskWebhookDeliveryReadinessRecord, ...] = field(default_factory=tuple)
    webhook_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskWebhookDeliveryReadinessRecord, ...]:
        return self.records

    @property
    def recommendations(self) -> tuple[TaskWebhookDeliveryReadinessRecord, ...]:
        return self.records

    @property
    def impacted_task_ids(self) -> tuple[str, ...]:
        return self.webhook_task_ids

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "webhook_task_ids": list(self.webhook_task_ids),
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        title = "# Task Webhook Delivery Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        impact_counts = self.summary.get("impact_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Webhook task count: {self.summary.get('webhook_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            "- Impact counts: " + ", ".join(f"{level} {impact_counts.get(level, 0)}" for level in _IMPACT_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task webhook delivery readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Impact | Detected Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{record.impact} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_webhook_delivery_readiness_plan(source: Any) -> TaskWebhookDeliveryReadinessPlan:
    """Build webhook delivery readiness records for task-shaped input."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (
                record
                for index, task in enumerate(tasks, start=1)
                if (record := _task_record(task, index)) is not None
            ),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                _IMPACT_ORDER[record.impact],
                -len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    webhook_task_ids = tuple(record.task_id for record in records)
    webhook_task_id_set = set(webhook_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in webhook_task_id_set
    )
    return TaskWebhookDeliveryReadinessPlan(
        plan_id=plan_id,
        records=records,
        webhook_task_ids=webhook_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_webhook_delivery_readiness(source: Any) -> TaskWebhookDeliveryReadinessPlan:
    return build_task_webhook_delivery_readiness_plan(source)


def recommend_task_webhook_delivery_readiness(source: Any) -> TaskWebhookDeliveryReadinessPlan:
    return build_task_webhook_delivery_readiness_plan(source)


def summarize_task_webhook_delivery_readiness(source: Any) -> TaskWebhookDeliveryReadinessPlan:
    return build_task_webhook_delivery_readiness_plan(source)


def generate_task_webhook_delivery_readiness(source: Any) -> TaskWebhookDeliveryReadinessPlan:
    return build_task_webhook_delivery_readiness_plan(source)


def extract_task_webhook_delivery_readiness(source: Any) -> TaskWebhookDeliveryReadinessPlan:
    return build_task_webhook_delivery_readiness_plan(source)


def derive_task_webhook_delivery_readiness(source: Any) -> TaskWebhookDeliveryReadinessPlan:
    return build_task_webhook_delivery_readiness_plan(source)


def task_webhook_delivery_readiness_plan_to_dict(result: TaskWebhookDeliveryReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_webhook_delivery_readiness_plan_to_dict.__test__ = False


def task_webhook_delivery_readiness_plan_to_dicts(
    result: TaskWebhookDeliveryReadinessPlan | Iterable[TaskWebhookDeliveryReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, TaskWebhookDeliveryReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_webhook_delivery_readiness_plan_to_dicts.__test__ = False
task_webhook_delivery_readiness_to_dicts = task_webhook_delivery_readiness_plan_to_dicts
task_webhook_delivery_readiness_to_dicts.__test__ = False


def task_webhook_delivery_readiness_plan_to_markdown(result: TaskWebhookDeliveryReadinessPlan) -> str:
    return result.to_markdown()


task_webhook_delivery_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[WebhookDeliverySignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[WebhookDeliverySafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskWebhookDeliveryReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    return TaskWebhookDeliveryReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        detected_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        readiness=_readiness(signals.present_safeguards, missing),
        impact=_impact(signals.signals, missing),
        recommended_checks=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[WebhookDeliverySignal] = set()
    safeguard_hits: set[WebhookDeliverySafeguard] = set()
    evidence: list[str] = []

    for path in _strings(
        task.get("files_or_modules")
        or task.get("files")
        or task.get("expected_file_paths")
        or task.get("expected_files")
        or task.get("paths")
    ):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _PATH_SIGNAL_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(source_field, text))

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        evidence=tuple(_dedupe(evidence)),
    )


def _readiness(
    present: tuple[WebhookDeliverySafeguard, ...],
    missing: tuple[WebhookDeliverySafeguard, ...],
) -> WebhookDeliveryReadiness:
    if not missing:
        return "strong"
    if len(present) >= 3:
        return "partial"
    return "weak"


def _impact(
    signals: tuple[WebhookDeliverySignal, ...],
    missing: tuple[WebhookDeliverySafeguard, ...],
) -> WebhookDeliveryImpact:
    high_signal = any(signal in _HIGH_IMPACT_SIGNALS for signal in signals)
    if high_signal and (
        len(missing) >= 3
        or "retry_tests" in missing
        or "signature_verification_tests" in missing and "signing" in signals
    ):
        return "high"
    if high_signal or len(missing) >= 3:
        return "medium"
    return "low"


def _summary(
    records: tuple[TaskWebhookDeliveryReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "webhook_task_count": len(records),
        "webhook_task_ids": [record.task_id for record in records],
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_count": len(no_impact_task_ids),
        "no_impact_task_ids": list(no_impact_task_ids),
        "signal_count": sum(len(record.detected_signals) for record in records),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "impact_counts": {
            impact: sum(1 for record in records if record.impact == impact)
            for impact in _IMPACT_ORDER
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "present_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.present_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
        "missing_safeguard_counts": {
            safeguard: sum(1 for record in records if safeguard in record.missing_safeguards)
            for safeguard in _SAFEGUARD_ORDER
        },
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if task := _task_payload(item):
            tasks.append(task)
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan | object) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
        return _object_payload(plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    tasks: list[dict[str, Any]] = []
    for item in items:
        if task := _task_payload(item):
            tasks.append(task)
    return tasks


def _task_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    if _looks_like_task(value):
        return _object_payload(value)
    return {}


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes, bytearray)) and any(
        hasattr(value, field_name) for field_name in ("id", "title", "description")
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "files_or_modules",
        "files",
        "expected_file_paths",
        "expected_files",
        "paths",
        "acceptance_criteria",
        "validation_plan",
        "validation_command",
        "validation_commands",
        "test_command",
        "test_commands",
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "status",
        "metadata",
        "blocked_reason",
        "tasks",
        "tags",
        "labels",
        "notes",
        "risks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "blocked_reason",
        "validation_plan",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "risks",
        "depends_on",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for source_field, text in _metadata_texts(task.get("metadata")):
        texts.append((source_field, text))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _metadata_key_is_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _metadata_key_is_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
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


def _metadata_key_is_signal(value: str) -> bool:
    return any(pattern.search(value) for pattern in [*_SIGNAL_PATTERNS.values(), *_SAFEGUARD_PATTERNS.values()])


def _validation_command_texts(task: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        value = task.get(key)
        if isinstance(value, Mapping):
            commands.extend(flatten_validation_commands(value) or _strings(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value) or _strings(metadata_value))
            else:
                commands.extend(_strings(metadata_value))
    return tuple(("validation_commands", command) for command in _dedupe(commands))


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


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
    path = value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")
    return str(PurePosixPath(path)) if path else ""


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None or isinstance(value, (bytes, bytearray)):
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "TaskWebhookDeliveryReadinessPlan",
    "TaskWebhookDeliveryReadinessRecord",
    "WebhookDeliveryImpact",
    "WebhookDeliveryReadiness",
    "WebhookDeliverySafeguard",
    "WebhookDeliverySignal",
    "analyze_task_webhook_delivery_readiness",
    "build_task_webhook_delivery_readiness_plan",
    "derive_task_webhook_delivery_readiness",
    "extract_task_webhook_delivery_readiness",
    "generate_task_webhook_delivery_readiness",
    "recommend_task_webhook_delivery_readiness",
    "summarize_task_webhook_delivery_readiness",
    "task_webhook_delivery_readiness_plan_to_dict",
    "task_webhook_delivery_readiness_plan_to_dicts",
    "task_webhook_delivery_readiness_plan_to_markdown",
    "task_webhook_delivery_readiness_to_dicts",
]
