"""Plan payment failure recovery readiness for payment execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


PaymentFailureRecoverySignal = Literal[
    "payment_collection",
    "billing_retry",
    "subscription_renewal",
    "invoicing",
    "charge_capture",
    "refund",
    "dunning",
    "failed_card",
    "provider_webhook",
]
PaymentFailureRecoverySafeguard = Literal[
    "retry_backoff_policy",
    "customer_notification",
    "idempotent_charge_handling",
    "ledger_reconciliation",
    "provider_webhook_handling",
    "manual_recovery_path",
]
PaymentFailureRecoveryRisk = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[PaymentFailureRecoverySignal, ...] = (
    "payment_collection",
    "billing_retry",
    "subscription_renewal",
    "invoicing",
    "charge_capture",
    "refund",
    "dunning",
    "failed_card",
    "provider_webhook",
)
_SAFEGUARD_ORDER: tuple[PaymentFailureRecoverySafeguard, ...] = (
    "retry_backoff_policy",
    "customer_notification",
    "idempotent_charge_handling",
    "ledger_reconciliation",
    "provider_webhook_handling",
    "manual_recovery_path",
)
_RISK_ORDER: dict[PaymentFailureRecoveryRisk, int] = {"high": 0, "medium": 1, "low": 2}

_SIGNAL_PATTERNS: dict[PaymentFailureRecoverySignal, re.Pattern[str]] = {
    "payment_collection": re.compile(
        r"\b(?:payment collection|collect(?:ing)? payment|payment capture|collect funds|"
        r"collect invoice|payment intent|payment method|card charge|payment processor|payments?)\b",
        re.I,
    ),
    "billing_retry": re.compile(
        r"\b(?:billing retr(?:y|ies)|retry billing|payment retr(?:y|ies)|retry payment|"
        r"retry schedule|re-attempt(?:ing)? charge|reattempt(?:ing)? charge|billing attempt(?:s)?)\b",
        re.I,
    ),
    "subscription_renewal": re.compile(
        r"\b(?:subscription renewal|renew subscription|renewal invoice|recurring billing|"
        r"recurring payment|membership renewal|plan renewal)\b",
        re.I,
    ),
    "invoicing": re.compile(
        r"\b(?:invoice|invoicing|invoice payment|invoice collection|open invoice|past[- ]due invoice|"
        r"billing statement)\b",
        re.I,
    ),
    "charge_capture": re.compile(
        r"\b(?:charge capture|capture charge|capture payment|authorize and capture|authorization capture|"
        r"card capture|settle charge|settlement capture)\b",
        re.I,
    ),
    "refund": re.compile(
        r"\b(?:refund(?:s|ed|ing)?|partial refund|refund failure|refund webhook|reverse charge|"
        r"reversal|credit memo)\b",
        re.I,
    ),
    "dunning": re.compile(
        r"\b(?:dunning|collections email|past[- ]due notice|payment reminder|account delinquen(?:t|cy)|"
        r"grace period|service suspension|subscription pause)\b",
        re.I,
    ),
    "failed_card": re.compile(
        r"\b(?:failed card|card declined|declined card|payment failed|payment failure|failed payment|"
        r"insufficient funds|expired card|authentication required|sca required)\b",
        re.I,
    ),
    "provider_webhook": re.compile(
        r"\b(?:payment provider webhook|provider webhook|stripe webhook|adyen webhook|braintree webhook|"
        r"paypal webhook|webhook event|invoice\.payment_failed|charge\.failed|payment_intent\.payment_failed)\b",
        re.I,
    ),
}
_SAFEGUARD_PATTERNS: dict[PaymentFailureRecoverySafeguard, re.Pattern[str]] = {
    "retry_backoff_policy": re.compile(
        r"\b(?:retry/backoff|retry backoff|backoff policy|exponential backoff|backoff with jitter|"
        r"retry schedule|dunning schedule|bounded retries|retry limit|max(?:imum)? payment attempts?)\b",
        re.I,
    ),
    "customer_notification": re.compile(
        r"\b(?:customer notification|notify customer|customer email|email customer|sms customer|"
        r"payment failure email|dunning email|in-app notice|billing notice|card update link)\b",
        re.I,
    ),
    "idempotent_charge_handling": re.compile(
        r"\b(?:idempotent(?: charge| payment| refund| capture)?|idempotency key|dedupe charge|"
        r"duplicate charge protection|no double charge|exactly[- ]once charge|safe to retry charge)\b",
        re.I,
    ),
    "ledger_reconciliation": re.compile(
        r"\b(?:ledger reconciliation|reconciliation|reconcile ledger|payment reconciliation|invoice reconciliation|"
        r"balance reconciliation|provider reconciliation|settlement reconciliation|journal entry|ledger entry)\b",
        re.I,
    ),
    "provider_webhook_handling": re.compile(
        r"\b(?:webhook handling|handle provider webhook|webhook signature|webhook verification|"
        r"webhook retry|webhook replay|webhook dedupe|provider event handling|payment event handler)\b",
        re.I,
    ),
    "manual_recovery_path": re.compile(
        r"\b(?:manual recovery|manual remediation|manual retry|operator recovery|admin recovery|"
        r"support playbook|runbook|replay tool|backfill tool|override path|manual unlock)\b",
        re.I,
    ),
}
_SAFEGUARD_GUIDANCE: dict[PaymentFailureRecoverySafeguard, str] = {
    "retry_backoff_policy": "Define bounded payment retry attempts with exponential backoff or a documented dunning schedule.",
    "customer_notification": "Notify customers about payment failures with clear timing, channel, and card-update instructions.",
    "idempotent_charge_handling": "Use idempotency keys or duplicate-charge protection for retries, captures, refunds, and webhook replays.",
    "ledger_reconciliation": "Reconcile invoices, ledger entries, provider events, settlements, refunds, and retry outcomes.",
    "provider_webhook_handling": "Handle provider webhooks with signature checks, event dedupe, retry tolerance, and replay behavior.",
    "manual_recovery_path": "Document an operator recovery path for stuck payments, missed webhooks, failed retries, or customer support fixes.",
}


@dataclass(frozen=True, slots=True)
class TaskPaymentFailureRecoveryReadinessRecord:
    """Payment failure recovery-readiness guidance for one payment task."""

    task_id: str
    title: str
    detected_signals: tuple[PaymentFailureRecoverySignal, ...]
    present_safeguards: tuple[PaymentFailureRecoverySafeguard, ...] = field(default_factory=tuple)
    missing_safeguards: tuple[PaymentFailureRecoverySafeguard, ...] = field(default_factory=tuple)
    risk_level: PaymentFailureRecoveryRisk = "medium"
    recommended_actions: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    @property
    def matched_signals(self) -> tuple[PaymentFailureRecoverySignal, ...]:
        """Compatibility view for planners that name detected signals matched signals."""
        return self.detected_signals

    @property
    def recommendations(self) -> tuple[str, ...]:
        """Compatibility view for planners that name recommended actions recommendations."""
        return self.recommended_actions

    @property
    def recommended_checks(self) -> tuple[str, ...]:
        """Compatibility view for planners that name recommended actions checks."""
        return self.recommended_actions

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "detected_signals": list(self.detected_signals),
            "present_safeguards": list(self.present_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "risk_level": self.risk_level,
            "recommended_actions": list(self.recommended_actions),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskPaymentFailureRecoveryReadinessPlan:
    """Plan-level payment failure recovery-readiness review for payment tasks."""

    plan_id: str | None = None
    records: tuple[TaskPaymentFailureRecoveryReadinessRecord, ...] = field(default_factory=tuple)
    impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_impact_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def findings(self) -> tuple[TaskPaymentFailureRecoveryReadinessRecord, ...]:
        """Compatibility view matching planners that expose findings."""
        return self.records

    @property
    def recommendations(self) -> tuple[TaskPaymentFailureRecoveryReadinessRecord, ...]:
        """Compatibility view matching planners that expose recommendations."""
        return self.records

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "findings": [record.to_dict() for record in self.findings],
            "recommendations": [record.to_dict() for record in self.recommendations],
            "impacted_task_ids": list(self.impacted_task_ids),
            "no_impact_task_ids": list(self.no_impact_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return payment failure recovery-readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render payment failure recovery-readiness guidance as deterministic Markdown."""
        title = "# Task Payment Failure Recovery Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        signal_counts = self.summary.get("signal_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Task count: {self.summary.get('task_count', 0)}",
            f"- Impacted task count: {self.summary.get('impacted_task_count', 0)}",
            f"- Missing safeguard count: {self.summary.get('missing_safeguard_count', 0)}",
            "- Risk counts: " + ", ".join(f"{risk} {risk_counts.get(risk, 0)}" for risk in _RISK_ORDER),
            "- Signal counts: "
            + ", ".join(f"{signal} {signal_counts.get(signal, 0)}" for signal in _SIGNAL_ORDER),
        ]
        if not self.records:
            lines.extend(["", "No task payment failure recovery-readiness records were inferred."])
            if self.no_impact_task_ids:
                lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Title | Risk | Detected Signals | Present Safeguards | Missing Safeguards | Recommended Actions | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(record.title)} | "
                f"{record.risk_level} | "
                f"{_markdown_cell(', '.join(record.detected_signals) or 'none')} | "
                f"{_markdown_cell(', '.join(record.present_safeguards) or 'none')} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(record.recommended_actions) or 'none')} | "
                f"{_markdown_cell('; '.join(record.evidence) or 'none')} |"
            )
        if self.no_impact_task_ids:
            lines.extend(["", f"No-impact tasks: {_markdown_cell(', '.join(self.no_impact_task_ids))}"])
        return "\n".join(lines)


def build_task_payment_failure_recovery_readiness_plan(source: Any) -> TaskPaymentFailureRecoveryReadinessPlan:
    """Build payment failure recovery-readiness records for payment implementation tasks."""
    plan_id, tasks = _source_payload(source)
    candidates = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    records = tuple(
        sorted(
            (record for record in candidates if record is not None),
            key=lambda record: (
                _RISK_ORDER[record.risk_level],
                record.task_id,
                record.title.casefold(),
            ),
        )
    )
    no_impact_task_ids = tuple(
        _task_id(task, index)
        for index, task in enumerate(tasks, start=1)
        if candidates[index - 1] is None
    )
    return TaskPaymentFailureRecoveryReadinessPlan(
        plan_id=plan_id,
        records=records,
        impacted_task_ids=tuple(record.task_id for record in records),
        no_impact_task_ids=no_impact_task_ids,
        summary=_summary(records, task_count=len(tasks), no_impact_task_ids=no_impact_task_ids),
    )


def analyze_task_payment_failure_recovery_readiness(source: Any) -> TaskPaymentFailureRecoveryReadinessPlan:
    """Compatibility alias for building payment failure recovery-readiness plans."""
    return build_task_payment_failure_recovery_readiness_plan(source)


def summarize_task_payment_failure_recovery_readiness(source: Any) -> TaskPaymentFailureRecoveryReadinessPlan:
    """Compatibility alias for building payment failure recovery-readiness plans."""
    return build_task_payment_failure_recovery_readiness_plan(source)


def extract_task_payment_failure_recovery_readiness(source: Any) -> TaskPaymentFailureRecoveryReadinessPlan:
    """Compatibility alias for extracting payment failure recovery-readiness plans."""
    return build_task_payment_failure_recovery_readiness_plan(source)


def generate_task_payment_failure_recovery_readiness(source: Any) -> TaskPaymentFailureRecoveryReadinessPlan:
    """Compatibility alias for generating payment failure recovery-readiness plans."""
    return build_task_payment_failure_recovery_readiness_plan(source)


def derive_task_payment_failure_recovery_readiness(source: Any) -> TaskPaymentFailureRecoveryReadinessPlan:
    """Compatibility alias for deriving payment failure recovery-readiness plans."""
    return build_task_payment_failure_recovery_readiness_plan(source)


def recommend_task_payment_failure_recovery_readiness(source: Any) -> TaskPaymentFailureRecoveryReadinessPlan:
    """Compatibility alias for recommending payment failure recovery safeguards."""
    return build_task_payment_failure_recovery_readiness_plan(source)


def task_payment_failure_recovery_readiness_plan_to_dict(result: TaskPaymentFailureRecoveryReadinessPlan) -> dict[str, Any]:
    """Serialize a payment failure recovery-readiness plan to a plain dictionary."""
    return result.to_dict()


task_payment_failure_recovery_readiness_plan_to_dict.__test__ = False


def task_payment_failure_recovery_readiness_plan_to_dicts(
    result: TaskPaymentFailureRecoveryReadinessPlan | Iterable[TaskPaymentFailureRecoveryReadinessRecord],
) -> list[dict[str, Any]]:
    """Serialize payment failure recovery-readiness records to plain dictionaries."""
    if isinstance(result, TaskPaymentFailureRecoveryReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_payment_failure_recovery_readiness_plan_to_dicts.__test__ = False


def task_payment_failure_recovery_readiness_plan_to_markdown(result: TaskPaymentFailureRecoveryReadinessPlan) -> str:
    """Render a payment failure recovery-readiness plan as Markdown."""
    return result.to_markdown()


task_payment_failure_recovery_readiness_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[PaymentFailureRecoverySignal, ...] = field(default_factory=tuple)
    signal_evidence: tuple[str, ...] = field(default_factory=tuple)
    present_safeguards: tuple[PaymentFailureRecoverySafeguard, ...] = field(default_factory=tuple)
    safeguard_evidence: tuple[str, ...] = field(default_factory=tuple)


def _task_record(task: Mapping[str, Any], index: int) -> TaskPaymentFailureRecoveryReadinessRecord | None:
    signals = _signals(task)
    if not signals.signals:
        return None

    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in signals.present_safeguards)
    task_id = _task_id(task, index)
    return TaskPaymentFailureRecoveryReadinessRecord(
        task_id=task_id,
        title=_optional_text(task.get("title")) or task_id,
        detected_signals=signals.signals,
        present_safeguards=signals.present_safeguards,
        missing_safeguards=missing,
        risk_level=_risk_level(signals.signals, signals.present_safeguards, missing),
        recommended_actions=tuple(_SAFEGUARD_GUIDANCE[safeguard] for safeguard in missing),
        evidence=tuple(_dedupe([*signals.signal_evidence, *signals.safeguard_evidence])),
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signal_hits: set[PaymentFailureRecoverySignal] = set()
    safeguard_hits: set[PaymentFailureRecoverySafeguard] = set()
    signal_evidence: list[str] = []
    safeguard_evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        searchable = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
        path_signals = _path_signals(normalized)
        if path_signals:
            signal_hits.update(path_signals)
            signal_evidence.append(f"files_or_modules: {path}")
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(normalized) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                safeguard_evidence.append(f"files_or_modules: {path}")

    for source_field, text in [*_candidate_texts(task), *_validation_command_texts(task)]:
        snippet = _evidence_snippet(source_field, text)
        searchable = text.replace("/", " ").replace("_", " ").replace("-", " ")
        matched_signal = False
        matched_safeguard = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                signal_hits.add(signal)
                matched_signal = True
        for safeguard, pattern in _SAFEGUARD_PATTERNS.items():
            if pattern.search(text) or pattern.search(searchable):
                safeguard_hits.add(safeguard)
                matched_safeguard = True
        if matched_signal:
            signal_evidence.append(snippet)
        if matched_safeguard:
            safeguard_evidence.append(snippet)

    return _Signals(
        signals=tuple(signal for signal in _SIGNAL_ORDER if signal in signal_hits),
        signal_evidence=tuple(_dedupe(signal_evidence)),
        present_safeguards=tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard in safeguard_hits),
        safeguard_evidence=tuple(_dedupe(safeguard_evidence)),
    )


def _path_signals(path: str) -> set[PaymentFailureRecoverySignal]:
    normalized = path.casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    name = posix.name
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals: set[PaymentFailureRecoverySignal] = set()
    if {"payment", "payments", "billing", "checkout", "charges"} & parts or any(
        token in name for token in ("payment", "billing", "checkout", "charge")
    ):
        signals.add("payment_collection")
    if any(token in text for token in ("billing retry", "payment retry", "retry payment", "dunning schedule")):
        signals.add("billing_retry")
    if any(token in text for token in ("subscription renewal", "recurring billing", "renewal invoice")):
        signals.add("subscription_renewal")
    if "invoice" in text or "invoicing" in text:
        signals.add("invoicing")
    if any(token in text for token in ("charge capture", "capture charge", "capture payment", "settle charge")):
        signals.add("charge_capture")
    if "refund" in text or "reversal" in text or "credit memo" in text:
        signals.add("refund")
    if any(token in text for token in ("dunning", "past due", "payment reminder", "delinquen", "grace period")):
        signals.add("dunning")
    if any(token in text for token in ("failed card", "card declined", "declined card", "payment failed", "failed payment")):
        signals.add("failed_card")
    if "webhook" in text and any(
        token in text for token in ("payment", "provider", "stripe", "adyen", "braintree", "paypal", "charge", "invoice")
    ):
        signals.add("provider_webhook")
    return signals


def _risk_level(
    signals: tuple[PaymentFailureRecoverySignal, ...],
    present: tuple[PaymentFailureRecoverySafeguard, ...],
    missing: tuple[PaymentFailureRecoverySafeguard, ...],
) -> PaymentFailureRecoveryRisk:
    if not missing:
        return "low"
    signal_set = set(signals)
    present_set = set(present)
    money_movement = bool(
        {"payment_collection", "charge_capture", "refund", "failed_card", "provider_webhook"} & signal_set
    )
    lacks_idempotency = "idempotent_charge_handling" not in present_set
    lacks_webhooks = "provider_webhook" in signal_set and "provider_webhook_handling" not in present_set
    if money_movement and (lacks_idempotency or lacks_webhooks):
        return "high"
    if len(missing) >= 4:
        return "high"
    return "medium"


def _summary(
    records: tuple[TaskPaymentFailureRecoveryReadinessRecord, ...],
    *,
    task_count: int,
    no_impact_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "task_count": task_count,
        "impacted_task_count": len(records),
        "impacted_task_ids": [record.task_id for record in records],
        "no_impact_task_ids": list(no_impact_task_ids),
        "missing_safeguard_count": sum(len(record.missing_safeguards) for record in records),
        "risk_counts": {
            risk: sum(1 for record in records if record.risk_level == risk)
            for risk in _RISK_ORDER
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
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


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
    "PaymentFailureRecoveryRisk",
    "PaymentFailureRecoverySafeguard",
    "PaymentFailureRecoverySignal",
    "TaskPaymentFailureRecoveryReadinessPlan",
    "TaskPaymentFailureRecoveryReadinessRecord",
    "analyze_task_payment_failure_recovery_readiness",
    "build_task_payment_failure_recovery_readiness_plan",
    "derive_task_payment_failure_recovery_readiness",
    "extract_task_payment_failure_recovery_readiness",
    "generate_task_payment_failure_recovery_readiness",
    "recommend_task_payment_failure_recovery_readiness",
    "summarize_task_payment_failure_recovery_readiness",
    "task_payment_failure_recovery_readiness_plan_to_dict",
    "task_payment_failure_recovery_readiness_plan_to_dicts",
    "task_payment_failure_recovery_readiness_plan_to_markdown",
]
