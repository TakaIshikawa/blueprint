"""Plan reconciliation readiness safeguards for financial execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


FinancialTaskSignal = Literal[
    "payment",
    "invoice",
    "ledger",
    "refund",
    "payout",
    "credit",
    "tax",
    "reconciliation",
]
FinancialReadiness = Literal["ready", "partial", "missing"]
FinancialSafeguard = Literal[
    "double_entry_ledger_checks",
    "idempotent_payment_operations",
    "reconciliation_reports",
    "audit_evidence",
    "rounding_currency_tests",
    "refund_edge_cases",
    "rollback_manual_adjustments",
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_FINANCIAL_PATH_RE = re.compile(
    r"(?:^|/)(?:payments?|billing|invoices?|ledger|accounting|refunds?|payouts?|"
    r"credits?|tax|taxes|reconciliation|finance|financial)(?:/|\.|_|-|$)",
    re.I,
)
_SIGNAL_PATTERNS: dict[FinancialTaskSignal, re.Pattern[str]] = {
    "payment": re.compile(r"\b(?:payments?|charges?|checkout capture|payment intent|stripe|adyen|paypal)\b", re.I),
    "invoice": re.compile(r"\b(?:invoices?|billing statement|receipts?|billable|billing)\b", re.I),
    "ledger": re.compile(r"\b(?:ledgers?|journal entries?|double[- ]entry|debits?|credits?|accounting)\b", re.I),
    "refund": re.compile(r"\b(?:refunds?|reversals?|chargebacks?)\b", re.I),
    "payout": re.compile(r"\b(?:payouts?|disbursements?|payout settlements?|remittance)\b", re.I),
    "credit": re.compile(r"\b(?:credits?|credit notes?|account credit|store credit|adjustments?)\b", re.I),
    "tax": re.compile(r"\b(?:tax|taxes|vat|gst|sales tax|taxable|tax calculation)\b", re.I),
    "reconciliation": re.compile(r"\b(?:reconciliation|reconcile|matched balances?|settlement report|bank report)\b", re.I),
}
_SAFEGUARD_PATTERNS: dict[FinancialSafeguard, re.Pattern[str]] = {
    "double_entry_ledger_checks": re.compile(
        r"\b(?:double[- ]entry|balanced journal|ledger balance|debits? equal credits?|"
        r"posting checks?|journal entry checks?|ledger integrity)\b",
        re.I,
    ),
    "idempotent_payment_operations": re.compile(
        r"\b(?:idempot(?:ent|ency)|idempotency keys?|duplicate charges?|duplicate refunds?|"
        r"retry safe|safe retries?|exactly once)\b",
        re.I,
    ),
    "reconciliation_reports": re.compile(
        r"\b(?:reconciliation reports?|reconcile reports?|settlement reports?|variance reports?|"
        r"matched balances?|daily close|reconcile against)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit trail|audit evidence|audit log|auditable|who changed|approvals?|"
        r"evidence export|immutable logs?)\b",
        re.I,
    ),
    "rounding_currency_tests": re.compile(
        r"\b(?:rounding|currency|currencies|fx|foreign exchange|minor units?|decimal precision|"
        r"multi[- ]currency|iso 4217|tax rounding)\b",
        re.I,
    ),
    "refund_edge_cases": re.compile(
        r"\b(?:partial refunds?|over refunds?|refund edge cases?|chargebacks?|reversals?|"
        r"refund failures?|negative balances?)\b",
        re.I,
    ),
    "rollback_manual_adjustments": re.compile(
        r"\b(?:rollback|roll back|backout|manual adjustments?|manual journal|compensating entries?|"
        r"operator correction|finance correction|runbook)\b",
        re.I,
    ),
}
_SAFEGUARD_ORDER: tuple[FinancialSafeguard, ...] = (
    "double_entry_ledger_checks",
    "idempotent_payment_operations",
    "reconciliation_reports",
    "audit_evidence",
    "rounding_currency_tests",
    "refund_edge_cases",
    "rollback_manual_adjustments",
)
_SIGNAL_ORDER: dict[FinancialTaskSignal, int] = {
    "payment": 0,
    "invoice": 1,
    "ledger": 2,
    "refund": 3,
    "payout": 4,
    "credit": 5,
    "tax": 6,
    "reconciliation": 7,
}
_READINESS_ORDER: dict[FinancialReadiness, int] = {"missing": 0, "partial": 1, "ready": 2}
_CRITICAL_SAFEGUARDS = {
    "reconciliation_reports",
    "audit_evidence",
    "rounding_currency_tests",
}


@dataclass(frozen=True, slots=True)
class TaskFinancialReconciliationReadinessRecord:
    """Readiness guidance for one task touching financial movement or accounting."""

    task_id: str
    title: str
    financial_signals: tuple[FinancialTaskSignal, ...]
    financial_surfaces: tuple[str, ...]
    detected_safeguards: tuple[FinancialSafeguard, ...]
    missing_safeguards: tuple[FinancialSafeguard, ...]
    readiness: FinancialReadiness
    required_readiness_steps: tuple[str, ...]
    owner_assumptions: tuple[str, ...]
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "financial_signals": list(self.financial_signals),
            "financial_surfaces": list(self.financial_surfaces),
            "detected_safeguards": list(self.detected_safeguards),
            "missing_safeguards": list(self.missing_safeguards),
            "readiness": self.readiness,
            "required_readiness_steps": list(self.required_readiness_steps),
            "owner_assumptions": list(self.owner_assumptions),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskFinancialReconciliationReadinessPlan:
    """Plan-level financial reconciliation readiness records."""

    plan_id: str | None = None
    records: tuple[TaskFinancialReconciliationReadinessRecord, ...] = field(default_factory=tuple)
    financial_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "records": [record.to_dict() for record in self.records],
            "financial_task_ids": list(self.financial_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return readiness records as plain dictionaries."""
        return [record.to_dict() for record in self.records]

    def to_markdown(self) -> str:
        """Render financial reconciliation readiness as deterministic Markdown."""
        title = "# Task Financial Reconciliation Readiness"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.records:
            lines.extend(["", "No financial reconciliation readiness records were inferred."])
            if self.not_applicable_task_ids:
                lines.append(f"Not-applicable tasks: {', '.join(self.not_applicable_task_ids)}")
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Task | Signals | Surfaces | Readiness | Missing Safeguards |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for record in self.records:
            lines.append(
                "| "
                f"`{_markdown_cell(record.task_id)}` | "
                f"{_markdown_cell(', '.join(record.financial_signals))} | "
                f"{_markdown_cell(', '.join(record.financial_surfaces))} | "
                f"{record.readiness} | "
                f"{_markdown_cell(', '.join(record.missing_safeguards) or 'None')} |"
            )
        return "\n".join(lines)


def build_task_financial_reconciliation_readiness_plan(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskFinancialReconciliationReadinessPlan:
    """Build readiness records for payment, ledger, tax, and reconciliation tasks."""
    plan_id, tasks = _source_payload(source)
    records = tuple(
        sorted(
            (
                record
                for index, task in enumerate(tasks, start=1)
                if (record := _task_record(task, index)) is not None
            ),
            key=lambda record: (_READINESS_ORDER[record.readiness], record.task_id, record.title.casefold()),
        )
    )
    financial_task_ids = tuple(record.task_id for record in records)
    all_task_ids = tuple(
        _optional_text(task.get("id")) or f"task-{index}"
        for index, task in enumerate(tasks, start=1)
    )
    not_applicable_task_ids = tuple(
        task_id for task_id in all_task_ids if task_id not in financial_task_ids
    )
    return TaskFinancialReconciliationReadinessPlan(
        plan_id=plan_id,
        records=records,
        financial_task_ids=financial_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(records, not_applicable_task_ids),
    )


def summarize_task_financial_reconciliation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskFinancialReconciliationReadinessPlan:
    """Compatibility alias for building financial reconciliation readiness plans."""
    return build_task_financial_reconciliation_readiness_plan(source)


def generate_task_financial_reconciliation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskFinancialReconciliationReadinessPlan:
    """Compatibility alias for generating financial reconciliation readiness plans."""
    return build_task_financial_reconciliation_readiness_plan(source)


def extract_task_financial_reconciliation_readiness(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> TaskFinancialReconciliationReadinessPlan:
    """Compatibility alias for building financial reconciliation readiness plans."""
    return build_task_financial_reconciliation_readiness_plan(source)


def task_financial_reconciliation_readiness_plan_to_dict(
    result: TaskFinancialReconciliationReadinessPlan,
) -> dict[str, Any]:
    """Serialize a financial reconciliation readiness plan to a plain dictionary."""
    return result.to_dict()


task_financial_reconciliation_readiness_plan_to_dict.__test__ = False


def task_financial_reconciliation_readiness_plan_to_markdown(
    result: TaskFinancialReconciliationReadinessPlan,
) -> str:
    """Render a financial reconciliation readiness plan as Markdown."""
    return result.to_markdown()


task_financial_reconciliation_readiness_plan_to_markdown.__test__ = False


def _task_record(
    task: Mapping[str, Any],
    index: int,
) -> TaskFinancialReconciliationReadinessRecord | None:
    signals = _financial_signals(task)
    if not signals:
        return None

    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    context = _task_context(task)
    detected = _detected_safeguards(context)
    missing = tuple(safeguard for safeguard in _SAFEGUARD_ORDER if safeguard not in detected)
    signal_names = tuple(sorted(signals, key=lambda signal: _SIGNAL_ORDER[signal]))
    surfaces = _financial_surfaces(task, context, signal_names)
    return TaskFinancialReconciliationReadinessRecord(
        task_id=task_id,
        title=title,
        financial_signals=signal_names,
        financial_surfaces=surfaces,
        detected_safeguards=detected,
        missing_safeguards=missing,
        readiness=_readiness(detected, missing),
        required_readiness_steps=_required_readiness_steps(missing, signal_names),
        owner_assumptions=_owner_assumptions(task, signal_names),
        evidence=tuple(_dedupe(item for signal in signals.values() for item in signal)),
    )


def _financial_signals(task: Mapping[str, Any]) -> dict[FinancialTaskSignal, tuple[str, ...]]:
    detected: dict[FinancialTaskSignal, list[str]] = {}
    for index, path in enumerate(_strings(task.get("files_or_modules") or task.get("files"))):
        normalized = _normalized_path(path)
        if _FINANCIAL_PATH_RE.search(normalized):
            for signal, pattern in _SIGNAL_PATTERNS.items():
                if pattern.search(normalized):
                    detected.setdefault(signal, []).append(f"files_or_modules: {path}")
            if not any(pattern.search(normalized) for pattern in _SIGNAL_PATTERNS.values()):
                detected.setdefault("payment", []).append(f"files_or_modules: {path}")
        elif _path_name_has_signal(normalized):
            detected.setdefault("reconciliation", []).append(f"files_or_modules[{index}]: {path}")

    for source_field, text in _candidate_texts(task):
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text):
                detected.setdefault(signal, []).append(_evidence_snippet(source_field, text))

    return {
        signal: tuple(_dedupe(evidence))
        for signal, evidence in detected.items()
        if evidence
    }


def _detected_safeguards(context: str) -> tuple[FinancialSafeguard, ...]:
    return tuple(
        safeguard
        for safeguard in _SAFEGUARD_ORDER
        if _SAFEGUARD_PATTERNS[safeguard].search(context)
    )


def _financial_surfaces(
    task: Mapping[str, Any],
    context: str,
    signals: tuple[FinancialTaskSignal, ...],
) -> tuple[str, ...]:
    surfaces: list[str] = []
    paths = " ".join(_strings(task.get("files_or_modules") or task.get("files")))
    combined = f"{context} {paths}"
    if "payment" in signals or re.search(r"\b(?:checkout|payment|charge|capture)\b", combined, re.I):
        surfaces.append("payment_operations")
    if "invoice" in signals or re.search(r"\b(?:invoice|billing|receipt)\b", combined, re.I):
        surfaces.append("billing_documents")
    if "ledger" in signals or re.search(r"\b(?:ledger|journal|accounting)\b", combined, re.I):
        surfaces.append("ledger_postings")
    if "refund" in signals:
        surfaces.append("refund_adjustments")
    if "payout" in signals:
        surfaces.append("payout_settlements")
    if "credit" in signals:
        surfaces.append("credit_adjustments")
    if "tax" in signals:
        surfaces.append("tax_calculation")
    if "reconciliation" in signals or re.search(r"\b(?:reconcile|reconciliation|settlement report)\b", combined, re.I):
        surfaces.append("reconciliation_reporting")
    return tuple(_dedupe(surfaces)) or ("financial_operations",)


def _readiness(
    detected: tuple[FinancialSafeguard, ...],
    missing: tuple[FinancialSafeguard, ...],
) -> FinancialReadiness:
    if not missing:
        return "ready"
    if not detected:
        return "missing"
    if _CRITICAL_SAFEGUARDS.issubset(set(missing)):
        return "missing"
    return "partial"


def _required_readiness_steps(
    missing: tuple[FinancialSafeguard, ...],
    signals: tuple[FinancialTaskSignal, ...],
) -> tuple[str, ...]:
    labels: dict[FinancialSafeguard, str] = {
        "double_entry_ledger_checks": "Add double-entry or ledger-balance checks before financial posting.",
        "idempotent_payment_operations": "Document idempotency keys and retry-safe payment, refund, and payout operations.",
        "reconciliation_reports": "Define reconciliation reports that compare internal records with provider or bank settlement evidence.",
        "audit_evidence": "Capture auditable evidence for financial changes, approvals, and manual corrections.",
        "rounding_currency_tests": "Cover currency, minor-unit, rounding, and tax precision cases in tests.",
        "refund_edge_cases": "Cover partial refunds, reversals, chargebacks, and failed refund paths.",
        "rollback_manual_adjustments": "Define rollback, compensating entry, or manual adjustment procedures for finance operations.",
    }
    steps = [labels[safeguard] for safeguard in missing]
    if "tax" in signals and "rounding_currency_tests" not in missing:
        steps.append("Verify tax rounding evidence stays attached to reconciliation outputs.")
    return tuple(_dedupe(steps))


def _owner_assumptions(
    task: Mapping[str, Any],
    signals: tuple[FinancialTaskSignal, ...],
) -> tuple[str, ...]:
    metadata = task.get("metadata") if isinstance(task.get("metadata"), Mapping) else {}
    explicit_owner = _first_metadata_value(
        metadata,
        "finance_owner",
        "accounting_owner",
        "reconciliation_owner",
        "owner",
        "owner_team",
    )
    if explicit_owner:
        return (f"{explicit_owner} owns financial reconciliation sign-off.",)
    if "tax" in signals:
        return ("Finance or tax owner signs off on rounding, currency, and audit evidence.",)
    return ("Finance or accounting owner signs off on reconciliation readiness.",)


def _summary(
    records: tuple[TaskFinancialReconciliationReadinessRecord, ...],
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    return {
        "financial_task_count": len(records),
        "not_applicable_task_ids": list(not_applicable_task_ids),
        "readiness_counts": {
            readiness: sum(1 for record in records if record.readiness == readiness)
            for readiness in ("ready", "partial", "missing")
        },
        "signal_counts": {
            signal: sum(1 for record in records if signal in record.financial_signals)
            for signal in _SIGNAL_ORDER
        },
    }


def _source_payload(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
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

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        task = _task_payload(item)
        if task:
            tasks.append(task)
    return None, tasks


def _plan_payload(source: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(source).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(source) if isinstance(source, Mapping) else {}


def _task_payload(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        task = value.model_dump(mode="python")
        return dict(task) if isinstance(task, Mapping) else {}
    return dict(value) if isinstance(value, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    return [task for item in items if (task := _task_payload(item))]


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "title",
        "description",
        "milestone",
        "owner_type",
        "suggested_engine",
        "test_command",
        "risk_level",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in ("acceptance_criteria", "tags", "labels", "notes", "depends_on"):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    for index, text in enumerate(_strings(task.get("files_or_modules") or task.get("files"))):
        texts.append((f"files_or_modules[{index}]", text))
    texts.extend(_metadata_texts(task.get("metadata")))
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
                if _is_financial_key(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _is_financial_key(key_text):
                texts.append((field, key_text))
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
    return " ".join(text for _, text in _candidate_texts(task))


def _path_name_has_signal(value: str) -> bool:
    path = PurePosixPath(value.casefold())
    return any(part in path.name for part in ("recon", "ledger", "invoice", "refund", "payout"))


def _is_financial_key(value: str) -> bool:
    return bool(
        re.search(
            r"\b(?:payment|invoice|ledger|refund|payout|credit|tax|reconciliation|finance|accounting)\b",
            value,
            re.I,
        )
    )


def _first_metadata_value(metadata: Mapping[str, Any], *keys: str) -> str | None:
    wanted = {key.casefold() for key in keys}
    for key in sorted(metadata, key=lambda item: str(item)):
        value = metadata[key]
        if str(key).casefold() in wanted:
            return next(iter(_strings(value)), None)
        if isinstance(value, Mapping):
            nested = _first_metadata_value(value, *keys)
            if nested:
                return nested
    return None


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


def _evidence_snippet(source_field: str, text: str) -> str:
    return f"{source_field}: {_text(text)}"


def _normalized_path(value: str) -> str:
    return value.replace("\\", "/").strip().strip("`'\",;:(){}[] ").strip("/")


def _markdown_cell(value: str) -> str:
    return _SPACE_RE.sub(" ", value.replace("|", "\\|").replace("\n", "<br>")).strip()


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
    "FinancialReadiness",
    "FinancialSafeguard",
    "FinancialTaskSignal",
    "TaskFinancialReconciliationReadinessPlan",
    "TaskFinancialReconciliationReadinessRecord",
    "build_task_financial_reconciliation_readiness_plan",
    "extract_task_financial_reconciliation_readiness",
    "generate_task_financial_reconciliation_readiness",
    "summarize_task_financial_reconciliation_readiness",
    "task_financial_reconciliation_readiness_plan_to_dict",
    "task_financial_reconciliation_readiness_plan_to_markdown",
]
