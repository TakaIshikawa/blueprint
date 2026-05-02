"""Plan payment failure and dunning readiness for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


PaymentFailureSignal = Literal[
    "failed_charge",
    "card_decline",
    "retry_schedule",
    "dunning",
    "invoice_collection",
    "grace_period",
    "subscription_suspension",
    "payment_method_update",
    "billing_webhook",
]
PaymentFailureSafeguard = Literal[
    "retry_policy",
    "customer_notification",
    "entitlement_state_handling",
    "webhook_idempotency",
    "support_visibility",
    "audit_trail",
    "recovery_path",
]
PaymentFailureReadiness = Literal["weak", "partial", "strong"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[PaymentFailureReadiness, int] = {"weak": 0, "partial": 1, "strong": 2}
_SIGNAL_ORDER: tuple[PaymentFailureSignal, ...] = (
    "failed_charge",
    "card_decline",
    "retry_schedule",
    "dunning",
    "invoice_collection",
    "grace_period",
    "subscription_suspension",
    "payment_method_update",
    "billing_webhook",
)
_SAFEGUARD_ORDER: tuple[PaymentFailureSafeguard, ...] = (
    "retry_policy",
    "customer_notification",
    "entitlement_state_handling",
    "webhook_idempotency",
    "support_visibility",
    "audit_trail",
    "recovery_path",
)

_SIGNAL_PATTERNS: dict[PaymentFailureSignal, re.Pattern[str]] = {
    "failed_charge": re.compile(
        r"\b(?:failed charge|charge failure|payment fail(?:ed|ure)?|failed payment|payment error|"
        r"charge declined|payment unsuccessful|collection failure)\b",
        re.I,
    ),
    "card_decline": re.compile(
        r"\b(?:card decline|declined card|card declined|issuer decline|soft decline|hard decline|"
        r"insufficient funds|expired card|do not honor|payment declined)\b",
        re.I,
    ),
    "retry_schedule": re.compile(
        r"\b(?:retry schedule|retry policy|payment retr(?:y|ies)|charge retr(?:y|ies)|smart retr(?:y|ies)|"
        r"retry attempt|backoff|retry cadence|next retry)\b",
        re.I,
    ),
    "dunning": re.compile(r"\b(?:dunning|past due|overdue|delinquen(?:t|cy)|collections? flow)\b", re.I),
    "invoice_collection": re.compile(
        r"\b(?:invoice collection|collect invoice|invoice payment|open invoice|unpaid invoice|"
        r"invoice retry|invoice finalization|hosted invoice)\b",
        re.I,
    ),
    "grace_period": re.compile(
        r"\b(?:grace period|grace window|access grace|billing grace|temporary access|"
        r"extend access|payment grace)\b",
        re.I,
    ),
    "subscription_suspension": re.compile(
        r"\b(?:subscription suspension|suspend subscription|subscription suspended|cancel subscription|"
        r"subscription cancellation|disable account|access suspension|revoke access|pause subscription)\b",
        re.I,
    ),
    "payment_method_update": re.compile(
        r"\b(?:payment method update|update payment method|card update|update card|new card|"
        r"payment method recovery|billing portal|card updater|customer balance)\b",
        re.I,
    ),
    "billing_webhook": re.compile(
        r"\b(?:billing webhook|payment webhook|invoice webhook|subscription webhook|webhook event|"
        r"webhook handler|stripe webhook|charge\.failed|invoice\.payment_failed|customer\.subscription)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS: dict[PaymentFailureSignal, re.Pattern[str]] = {
    "failed_charge": re.compile(r"(?:failed[_-]?charges?|payment[_-]?fail(?:ed|ure)|charge[_-]?failure)", re.I),
    "card_decline": re.compile(r"(?:card[_-]?declines?|declined[_-]?cards?|issuer[_-]?decline)", re.I),
    "retry_schedule": re.compile(r"(?:payment[_-]?retr(?:y|ies)|charge[_-]?retr(?:y|ies)|retry[_-]?schedule)", re.I),
    "dunning": re.compile(r"(?:dunning|past[_-]?due|delinquen)", re.I),
    "invoice_collection": re.compile(r"(?:invoice[_-]?collection|invoice[_-]?payment|unpaid[_-]?invoice)", re.I),
    "grace_period": re.compile(r"(?:grace[_-]?period|grace[_-]?window|access[_-]?grace)", re.I),
    "subscription_suspension": re.compile(r"(?:subscription[_-]?suspension|suspend[_-]?subscription|revoke[_-]?access)", re.I),
    "payment_method_update": re.compile(r"(?:payment[_-]?method|update[_-]?card|billing[_-]?portal|card[_-]?update)", re.I),
    "billing_webhook": re.compile(r"(?:billing[_-]?webhook|payment[_-]?webhook|stripe[_-]?webhook|invoice[_-]?webhook)", re.I),
}
_SAFEGUARD_PATTERNS: dict[PaymentFailureSafeguard, re.Pattern[str]] = {
    "retry_policy": re.compile(
        r"\b(?:retry policy|retry schedule|smart retr(?:y|ies)|retry cadence|backoff|retry limit|"
        r"max retr(?:y|ies)|next retry|hard decline no retry|do not retry)\b",
        re.I,
    ),
    "customer_notification": re.compile(
        r"\b(?:customer notification|notify customer|email customer|payment failure email|dunning email|"
        r"in-app notice|billing notice|sms reminder|reminder email|receipt failure)\b",
        re.I,
    ),
    "entitlement_state_handling": re.compile(
        r"\b(?:entitlement state|access state|grace entitlement|subscription status|past_due status|"
        r"access suspension|revoke access|disable access|feature entitlement|customer access)\b",
        re.I,
    ),
    "webhook_idempotency": re.compile(
        r"\b(?:webhook idempotenc(?:y|e)|idempotent webhook|duplicate webhook|event dedupe|deduplicate events?|"
        r"processed event id|webhook replay|exactly once|at least once)\b",
        re.I,
    ),
    "support_visibility": re.compile(
        r"\b(?:support visibility|support dashboard|admin dashboard|support view|agent view|billing status visible|"
        r"support notes|customer support)\b",
        re.I,
    ),
    "audit_trail": re.compile(
        r"\b(?:audit trail|audit log|event log|ledger|billing event history|status history|"
        r"change history|who changed|timestamped)\b",
        re.I,
    ),
    "recovery_path": re.compile(
        r"\b(?:recovery path|recover access|restore access|reactivat(?:e|ion)|resume subscription|"
        r"payment method recovery|self[- ]serve recovery|billing portal|pay outstanding|settle invoice)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[PaymentFailureSafeguard, str] = {
    "retry_policy": "Define retry cadence, limits, hard-decline handling, and manual retry behavior for failed payments.",
    "customer_notification": "Verify customer-facing notices explain the failure, deadline, and payment update path.",
    "entitlement_state_handling": "Check entitlement and subscription state transitions for grace, suspension, and restored access.",
    "webhook_idempotency": "Make billing webhook processing idempotent and resilient to duplicate, delayed, or replayed events.",
    "support_visibility": "Expose payment failure, retry, dunning, and access state clearly to support or admin users.",
    "audit_trail": "Record timestamped billing failure, retry, notification, entitlement, and recovery events for auditability.",
    "recovery_path": "Provide a tested path to update payment details, settle invoices, and restore service after recovery.",
}


@dataclass(frozen=True, slots=True)
class TaskPaymentFailureReadinessRecord:
    """Payment-failure readiness guidance for one execution task."""

    task_id: str
    title: str
    detected_signals: tuple[PaymentFailureSignal, ...]
    present_safeguards: tuple[PaymentFailureSafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[PaymentFailureSafeguard, ...] = field(default_factory=tuple)
    readiness: PaymentFailureReadiness = "weak"
    recommended_checks: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def matched_signals(self) -> tuple[PaymentFailureSignal, ...]:
        """Compatibility view for planners that name detected signals matched signals."""
        return self.detected_signals

    @property
    def recommended_actions(self) -> tuple[str, ...]:
        """Compatibility view for planners that name checks recommended actions."""
        return self.recommended_checks

    @property
    def recommendations(self) -> tuple[str, ...]:
        """Compatibility view for planners that name checks recommendations."""
        return self.recommended_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "recommended_checks": list(self.recommended_checks),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskPaymentFailureReadinessPlan:
    """Plan-level payment-failure readiness review."""

    plan_id: str | None = None
    records: tuple[TaskPaymentFailureReadinessRecord, ...] = field(default_factory=tuple)
    payment_failure_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskPaymentFailureReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskPaymentFailureReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    @property
    def impacted_task_ids(self) -> tuple[str, ...]:
        """Compatibility view matching planners that expose impacted task ids."""
        return self.payment_failure_task_ids

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "payment_failure_task_ids": list(self.payment_failure_task_ids),
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return payment-failure readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render payment-failure readiness guidance as deterministic Markdown."""
        title = "# Task Payment Failure Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        readiness_counts = self.summary.get("readiness_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Payment-failure task count: {self.summary.get('payment_failure_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Readiness counts: "
            + ", ".join(f"{level} {readiness_counts.get(level, 0)}" for level in _READINESS_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task payment failure readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Readiness | Detected Signals | Present Safeguards | Missing Safeguards | Recommended Checks | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_checks) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_payment_failure_readiness_plan(source: Any) -> TaskPaymentFailureReadinessPlan:
    """Build payment-failure readiness records for task-shaped input."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _READINESS_ORDER[record.readiness],
                -len(record.missing_safeguards),
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    payment_failure_task_ids = tuple(record.task_id for record in records)
    payment_failure_task_id_set = set(payment_failure_task_ids)
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if _task_id(task, index) not in payment_failure_task_id_set
    )
    return TaskPaymentFailureReadinessPlan(
        plan_id=plan_id,
        records=records,
        payment_failure_task_ids=payment_failure_task_ids,
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_payment_failure_readiness(source: Any) -> TaskPaymentFailureReadinessPlan:
    """Compatibility alias for building payment-failure readiness plans."""
    return build_task_payment_failure_readiness_plan(source)


def recommend_task_payment_failure_readiness(source: Any) -> TaskPaymentFailureReadinessPlan:
    """Compatibility alias for recommending payment-failure readiness checks."""
    return build_task_payment_failure_readiness_plan(source)


def summarize_task_payment_failure_readiness(source: Any) -> TaskPaymentFailureReadinessPlan:
    """Compatibility alias for summarizing payment-failure readiness plans."""
    return build_task_payment_failure_readiness_plan(source)


def generate_task_payment_failure_readiness(source: Any) -> TaskPaymentFailureReadinessPlan:
    """Compatibility alias for generating payment-failure readiness plans."""
    return build_task_payment_failure_readiness_plan(source)


def extract_task_payment_failure_readiness(source: Any) -> TaskPaymentFailureReadinessPlan:
    """Compatibility alias for extracting payment-failure readiness plans."""
    return build_task_payment_failure_readiness_plan(source)


def derive_task_payment_failure_readiness(source: Any) -> TaskPaymentFailureReadinessPlan:
    """Compatibility alias for deriving payment-failure readiness plans."""
    return build_task_payment_failure_readiness_plan(source)


def task_payment_failure_readiness_plan_to_dict(result: TaskPaymentFailureReadinessPlan) -> dict[str, Any]:
    """Serialize a payment-failure readiness plan to a plain dictionary."""
    return result.to_dict()


task_payment_failure_readiness_plan_to_dict.__test__ = False


def task_payment_failure_readiness_plan_to_dicts(
    result: TaskPaymentFailureReadinessPlan | Iterable[TaskPaymentFailureReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize payment-failure readiness records to plain dictionaries."""
    if isinstance(result, TaskPaymentFailureReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_payment_failure_readiness_plan_to_dicts.__test__ = False
task_payment_failure_readiness_to_dicts = task_payment_failure_readiness_plan_to_dicts
task_payment_failure_readiness_to_dicts.__test__ = False


def task_payment_failure_readiness_plan_to_markdown(result: TaskPaymentFailureReadinessPlan) -> str:
    """Render a payment-failure readiness plan as Markdown."""
    return result.to_markdown()


task_payment_failure_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[PaymentFailureSignal, ...] = field(default_factory=tuple)
    present_safeguards: tuple[PaymentFailureSafeguard, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskPaymentFailureReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    return TaskPaymentFailureReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        detected_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        readiness=_readiness(signals.present_safeguards, missing),
        recommended_checks=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[PaymentFailureSignal] = set()
    safeguard_hits: set[PaymentFailureSafeguard] = set()
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
            if pattern.search(normalized) or _SIGNAL_PATTERNS[signal].search(searchable):
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
    present: tuple[PaymentFailureSafeguard, ...],
    missing: tuple[PaymentFailureSafeguard, ...],
) -> PaymentFailureReadiness:
    if not missing:
        return "strong"
    present_count = len(present)
    if present_count >= 4:
        return "partial"
    return "weak"


def _summary(
    records: tuple[TaskPaymentFailureReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "payment_failure_task_count": len(records),
        "payment_failure_task_ids": [record.task_id for record in records],
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
        "signal_count": sum(len(record.detected_signals) for record in records),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in _READINESS_ORDER
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
            commands.extend(flatten_validation_commands(value))
        else:
            commands.extend(_strings(value))
        if isinstance(metadata, Mapping):
            metadata_value = metadata.get(key)
            if isinstance(metadata_value, Mapping):
                commands.extend(flatten_validation_commands(metadata_value))
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
    "PaymentFailureReadiness",
    "PaymentFailureSafeguard",
    "PaymentFailureSignal",
    "TaskPaymentFailureReadinessPlan",
    "TaskPaymentFailureReadinessRecord",
    "analyze_task_payment_failure_readiness",
    "build_task_payment_failure_readiness_plan",
    "derive_task_payment_failure_readiness",
    "extract_task_payment_failure_readiness",
    "generate_task_payment_failure_readiness",
    "recommend_task_payment_failure_readiness",
    "summarize_task_payment_failure_readiness",
    "task_payment_failure_readiness_plan_to_dict",
    "task_payment_failure_readiness_plan_to_dicts",
    "task_payment_failure_readiness_plan_to_markdown",
    "task_payment_failure_readiness_to_dicts",
]
