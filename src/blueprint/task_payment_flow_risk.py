"""Plan safeguards for execution tasks that touch payment flows."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask
from blueprint.validation_commands import flatten_validation_commands


PaymentFlowSignal = Literal[
    "payment",
    "checkout",
    "invoicing",
    "refunds",
    "subscriptions",
    "tax",
    "provider_integration",
    "idempotency",
    "reconciliation",
    "audit_logging",
]
PaymentFlowRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_RISK_ORDER: dict[PaymentFlowRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_ORDER: dict[PaymentFlowSignal, int] = {
    "payment": 0,
    "checkout": 1,
    "invoicing": 2,
    "refunds": 3,
    "subscriptions": 4,
    "tax": 5,
    "provider_integration": 6,
    "idempotency": 7,
    "reconciliation": 8,
    "audit_logging": 9,
}
_TEXT_SIGNAL_PATTERNS: dict[PaymentFlowSignal, re.Pattern[str]] = {
    "payment": re.compile(
        r"\b(?:payments?|payment flow|payment method|charge|charges|capture|authorize|"
        r"authorization|card|cards|credit card|debit card|wallet|ach|bank transfer|"
        r"settlement|payout)\b",
        re.I,
    ),
    "checkout": re.compile(r"\b(?:checkout|cart|purchase flow|order payment|place order)\b", re.I),
    "invoicing": re.compile(
        r"\b(?:invoice|invoices|invoicing|billing statement|billable|billing portal|"
        r"billing account)\b",
        re.I,
    ),
    "refunds": re.compile(r"\b(?:refund|refunds|refunded|chargeback|dispute|void)\b", re.I),
    "subscriptions": re.compile(
        r"\b(?:subscription|subscriptions|subscribe|renewal|recurring billing|plan change|"
        r"proration|trial|cancel plan)\b",
        re.I,
    ),
    "tax": re.compile(r"\b(?:tax|taxes|vat|gst|sales tax|tax calculation|tax rate|taxjar|avalara)\b", re.I),
    "provider_integration": re.compile(
        r"\b(?:stripe|adyen|paypal|braintree|checkout\.com|square|authorize\.net|"
        r"worldpay|klarna|affirm|afterpay|payment provider|payment gateway|psp|"
        r"webhook signature)\b",
        re.I,
    ),
    "idempotency": re.compile(r"\b(?:idempotenc(?:y|ies)|idempotency key|dedup(?:e|lication))\b", re.I),
    "reconciliation": re.compile(
        r"\b(?:reconciliation|reconcile|ledger|balance transaction|settlement report|"
        r"payout report|accounting export)\b",
        re.I,
    ),
    "audit_logging": re.compile(r"\b(?:audit log|audit trail|payment log|billing log)\b", re.I),
}
_HIGH_RISK_RE = re.compile(r"\b(?:high|critical|severe|payment-impacting|money movement)\b", re.I)
_MEDIUM_RISK_RE = re.compile(r"\b(?:medium|moderate)\b", re.I)


@dataclass(frozen=True, slots=True)
class TaskPaymentFlowRiskFinding:
    """Payment-flow risk guidance for one execution task."""

    task_id: str
    title: str
    risk_level: PaymentFlowRiskLevel
    detected_signals: tuple[PaymentFlowSignal, ...] = field(default_factory=tuple)
    recommended_safeguards: tuple[str, ...] = field(default_factory=tuple)
    rationale: str = "No payment, checkout, invoicing, refund, subscription, tax, or provider-integration signals detected."
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "risk_level": self.risk_level,
            "detected_signals": list(self.detected_signals),
            "recommended_safeguards": list(self.recommended_safeguards),
            "rationale": self.rationale,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskPaymentFlowRiskPlan:
    """Plan-level payment-flow risk review."""

    plan_id: str | None = None
    task_risks: tuple[TaskPaymentFlowRiskFinding, ...] = field(default_factory=tuple)
    payment_impacted_task_ids: tuple[str, ...] = field(default_factory=tuple)
    low_risk_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "task_risks": [risk.to_dict() for risk in self.task_risks],
            "payment_impacted_task_ids": list(self.payment_impacted_task_ids),
            "low_risk_task_ids": list(self.low_risk_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return payment-flow risk records as plain dictionaries."""
        return [risk.to_dict() for risk in self.task_risks]

    def to_markdown(self) -> str:
        """Render the payment-flow risk plan as deterministic Markdown."""
        title = "# Task Payment Flow Risk Plan"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.task_risks:
            lines.extend(["", "No tasks were available for payment-flow risk assessment."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Risk | Signals | Recommended Safeguards | Evidence / Rationale |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for risk in self.task_risks:
            lines.append(
                "| "
                f"`{_markdown_cell(risk.task_id)}` {_markdown_cell(risk.title)} | "
                f"{risk.risk_level} | "
                f"{_markdown_cell(', '.join(risk.detected_signals) or 'none')} | "
                f"{_markdown_cell('; '.join(risk.recommended_safeguards) or 'none')} | "
                f"{_markdown_cell('; '.join(risk.evidence) or risk.rationale)} |"
            )
        if self.low_risk_task_ids:
            lines.extend(["", f"Low-risk tasks: {_markdown_cell(', '.join(self.low_risk_task_ids))}"])
        return "\n".join(lines)


def build_task_payment_flow_risk_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskPaymentFlowRiskPlan:
    """Classify execution tasks by payment-flow implementation risk."""
    plan_id, tasks = _source_payload(source)
    risks_with_order = [
        (_task_risk(task, index), index) for index, task in enumerate(tasks, start=1)
    ]
    task_risks = tuple(
        risk
        for risk, _ in sorted(
            risks_with_order,
            key=lambda item: (_RISK_ORDER[item[0].risk_level], item[1]),
        )
    )
    payment_impacted_task_ids = tuple(
        risk.task_id for risk in task_risks if risk.risk_level != "low"
    )
    low_risk_task_ids = tuple(risk.task_id for risk in task_risks if risk.risk_level == "low")
    risk_counts = {
        risk_level: sum(1 for risk in task_risks if risk.risk_level == risk_level)
        for risk_level in _RISK_ORDER
    }
    signal_counts = {
        signal: sum(1 for risk in task_risks if signal in risk.detected_signals)
        for signal in _SIGNAL_ORDER
    }
    return TaskPaymentFlowRiskPlan(
        plan_id=plan_id,
        task_risks=task_risks,
        payment_impacted_task_ids=payment_impacted_task_ids,
        low_risk_task_ids=low_risk_task_ids,
        summary={
            "task_count": len(tasks),
            "payment_impacted_task_count": len(payment_impacted_task_ids),
            "low_risk_task_count": len(low_risk_task_ids),
            "risk_counts": risk_counts,
            "signal_counts": signal_counts,
        },
    )


def summarize_task_payment_flow_risk(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> TaskPaymentFlowRiskPlan:
    """Compatibility alias for building payment-flow risk plans."""
    return build_task_payment_flow_risk_plan(source)


def task_payment_flow_risk_plan_to_dict(
    result: TaskPaymentFlowRiskPlan,
) -> dict[str, Any]:
    """Serialize a payment-flow risk plan to a plain dictionary."""
    return result.to_dict()


task_payment_flow_risk_plan_to_dict.__test__ = False


def task_payment_flow_risk_plan_to_markdown(result: TaskPaymentFlowRiskPlan) -> str:
    """Render a payment-flow risk plan as Markdown."""
    return result.to_markdown()


task_payment_flow_risk_plan_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Signals:
    signals: tuple[PaymentFlowSignal, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    validation_commands: tuple[str, ...] = field(default_factory=tuple)


def _task_risk(task: Mapping[str, Any], index: int) -> TaskPaymentFlowRiskFinding:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    signals = _signals(task)
    risk_level = _risk_level(task, signals.signals)
    if risk_level == "low":
        return TaskPaymentFlowRiskFinding(
            task_id=task_id,
            title=title,
            risk_level="low",
            recommended_safeguards=(
                "Confirm the task does not alter payment, checkout, billing, refund, subscription, tax, or payment-provider code paths.",
            ),
        )

    return TaskPaymentFlowRiskFinding(
        task_id=task_id,
        title=title,
        risk_level=risk_level,
        detected_signals=signals.signals,
        recommended_safeguards=_safeguards(signals.signals, signals.validation_commands),
        rationale=_rationale(risk_level, signals.signals),
        evidence=signals.evidence,
    )


def _signals(task: Mapping[str, Any]) -> _Signals:
    signals: set[PaymentFlowSignal] = set()
    evidence: list[str] = []

    for path in _strings(task.get("files_or_modules") or task.get("files")):
        normalized = _normalized_path(path)
        if not normalized:
            continue
        path_signals = _path_signals(normalized)
        if path_signals:
            signals.update(path_signals)
            evidence.append(f"files_or_modules: {path}")

    for source_field, text in _candidate_texts(task):
        snippet = _evidence_snippet(source_field, text)
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(text):
                signals.add(signal)
                evidence.append(snippet)

    validation_commands = tuple(_validation_commands(task))
    for command in validation_commands:
        snippet = _evidence_snippet("validation_commands", command)
        command_text = command.replace("/", " ").replace("_", " ").replace("-", " ")
        for signal, pattern in _TEXT_SIGNAL_PATTERNS.items():
            if pattern.search(command) or pattern.search(command_text):
                signals.add(signal)
                evidence.append(snippet)

    ordered = tuple(signal for signal in _SIGNAL_ORDER if signal in signals)
    return _Signals(
        signals=ordered,
        evidence=tuple(_dedupe(evidence)),
        validation_commands=validation_commands,
    )


def _path_signals(path: str) -> set[PaymentFlowSignal]:
    normalized = path.casefold()
    posix = PurePosixPath(normalized)
    parts = set(posix.parts)
    name = posix.name
    text = normalized.replace("/", " ").replace("_", " ").replace("-", " ")
    signals: set[PaymentFlowSignal] = set()

    if {"payment", "payments", "pay", "billing", "charges", "ledger", "payouts"} & parts:
        signals.add("payment")
    if "checkout" in parts or "checkout" in text:
        signals.add("checkout")
    if any(token in text for token in ("invoice", "invoicing", "billing portal")):
        signals.add("invoicing")
    if any(token in text for token in ("refund", "chargeback", "dispute", "void")):
        signals.add("refunds")
    if any(token in text for token in ("subscription", "renewal", "recurring", "proration")):
        signals.add("subscriptions")
    if any(token in text for token in ("tax", "vat", "gst", "avalara", "taxjar")):
        signals.add("tax")
    if any(
        token in text
        for token in (
            "stripe",
            "adyen",
            "paypal",
            "braintree",
            "checkout.com",
            "square",
            "authorize.net",
            "webhook",
        )
    ):
        signals.add("provider_integration")
    if any(token in text for token in ("idempotency", "idempotent", "dedupe")):
        signals.add("idempotency")
    if any(token in text for token in ("reconciliation", "reconcile", "settlement", "ledger")):
        signals.add("reconciliation")
    if any(token in text for token in ("audit", "event log", "payment log", "billing log")):
        signals.add("audit_logging")
    if name in {"stripe.py", "paypal.py", "adyen.py", "payments.py", "checkout.py", "billing.py"}:
        signals.add("payment")
    return signals


def _risk_level(
    task: Mapping[str, Any],
    signals: tuple[PaymentFlowSignal, ...],
) -> PaymentFlowRiskLevel:
    if not signals:
        return "low"

    risk_text = _text(task.get("risk_level"))
    signal_set = set(signals)
    if _HIGH_RISK_RE.search(risk_text):
        return "high"
    if signal_set & {
        "checkout",
        "refunds",
        "subscriptions",
        "tax",
        "provider_integration",
        "idempotency",
        "reconciliation",
    }:
        return "high"
    if len(signal_set) >= 2:
        return "high"
    if _MEDIUM_RISK_RE.search(risk_text):
        return "medium"
    return "medium"


def _safeguards(
    signals: tuple[PaymentFlowSignal, ...],
    validation_commands: tuple[str, ...],
) -> tuple[str, ...]:
    signal_set = set(signals)
    safeguards = [
        "Run sandbox payment-provider tests for authorization, capture, failure, and webhook callback paths.",
        "Require idempotency keys for charge, checkout, subscription, refund, and webhook-processing operations.",
        "Add audit logging for payment state transitions, actor, provider request id, and provider response id.",
    ]
    if signal_set & {"payment", "checkout", "provider_integration"}:
        safeguards.append("Validate payment-provider sandbox credentials, webhook signatures, retries, and declined-card fixtures.")
    if "checkout" in signal_set:
        safeguards.append("Exercise checkout success, failure, retry, duplicate-submit, abandoned-cart, and post-payment confirmation flows.")
    if "refunds" in signal_set:
        safeguards.append("Validate full refund, partial refund, duplicate refund prevention, failed refund, dispute, and void paths.")
    if "subscriptions" in signal_set:
        safeguards.append("Validate renewal, cancellation, plan change, proration, trial expiry, and failed recurring-payment fixtures.")
    if "tax" in signal_set:
        safeguards.append("Run tax calculation fixtures across taxable, exempt, domestic, cross-border, VAT, GST, and rounding cases.")
    if signal_set & {"invoicing", "reconciliation"}:
        safeguards.append("Run reconciliation checks between internal ledger, invoices, provider balance transactions, payouts, and accounting exports.")
    if "idempotency" in signal_set:
        safeguards.append("Test duplicate requests and replayed webhooks to prove idempotency keys prevent double charges or double refunds.")
    if "audit_logging" in signal_set:
        safeguards.append("Verify audit records are immutable enough for support, finance, and compliance review without storing sensitive card data.")
    if validation_commands:
        safeguards.append("Run the detected validation commands before handoff and attach the payment-specific evidence.")
    return tuple(_dedupe(safeguards))


def _rationale(
    risk_level: PaymentFlowRiskLevel,
    signals: tuple[PaymentFlowSignal, ...],
) -> str:
    rendered = ", ".join(signal.replace("_", " ") for signal in signals)
    if risk_level == "high":
        return f"Task touches payment-impacting surfaces that require explicit safeguards: {rendered}."
    return f"Task has payment-adjacent implementation signals: {rendered}."


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask | object]
        | object
    ),
) -> tuple[str | None, list[dict[str, Any]]]:
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
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
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
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
        elif _looks_like_task(item):
            tasks.append(_object_payload(item))
    return tasks


def _looks_like_plan(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and hasattr(value, "tasks")


def _looks_like_task(value: object) -> bool:
    return not isinstance(value, (str, bytes)) and any(
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
        "estimated_complexity",
        "estimated_hours",
        "risk_level",
        "test_command",
        "test_commands",
        "validation_command",
        "validation_commands",
        "status",
        "tags",
        "labels",
        "notes",
        "metadata",
        "blocked_reason",
        "tasks",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes"):
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if any(pattern.search(key_text) for pattern in _TEXT_SIGNAL_PATTERNS.values()):
                    texts.append((field, f"{key_text}: {text}"))
            elif any(pattern.search(key_text) for pattern in _TEXT_SIGNAL_PATTERNS.values()):
                texts.append((field, str(key)))
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


def _validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    metadata = task.get("metadata")
    for key in ("validation_commands", "validation_command", "test_commands", "test_command"):
        if value := task.get(key):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
        if isinstance(metadata, Mapping) and (value := metadata.get(key)):
            if isinstance(value, Mapping):
                commands.extend(flatten_validation_commands(value))
            else:
                commands.extend(_strings(value))
    return _dedupe(commands)


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
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


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
    "PaymentFlowRiskLevel",
    "PaymentFlowSignal",
    "TaskPaymentFlowRiskFinding",
    "TaskPaymentFlowRiskPlan",
    "build_task_payment_flow_risk_plan",
    "summarize_task_payment_flow_risk",
    "task_payment_flow_risk_plan_to_dict",
    "task_payment_flow_risk_plan_to_markdown",
]
