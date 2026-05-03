"""Build plan-level billing reconciliation readiness matrices."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


BillingReconciliationReadiness = Literal["ready", "partial", "blocked"]
BillingReconciliationSeverity = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_READINESS_ORDER: dict[BillingReconciliationReadiness, int] = {
    "blocked": 0,
    "partial": 1,
    "ready": 2,
}
_SEVERITY_ORDER: dict[BillingReconciliationSeverity, int] = {"high": 0, "medium": 1, "low": 2}
_BILLING_TRIGGER_RE = re.compile(
    r"\b(?:billing reconciliation|reconcile|reconciliation|ledger settlement|invoice totals?|"
    r"payment processor balances?|processor balances?|refunds?|chargebacks?|tax totals?|"
    r"revenue reporting|settlement reports?|payout settlement|variance reports?|month[- ]end close)\b",
    re.I,
)
_SOURCE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "payment_processor",
        re.compile(
            r"\b(?:payment processor|stripe|adyen|paypal|processor balance|checkout payments?)\b",
            re.I,
        ),
    ),
    (
        "invoice_system",
        re.compile(r"\b(?:invoice|invoices|invoice totals?|receipts?|billing statement)\b", re.I),
    ),
    (
        "billing_system",
        re.compile(
            r"\b(?:billing system|billing records?|subscription billing|charges?|payments?)\b", re.I
        ),
    ),
    (
        "ledger",
        re.compile(
            r"\b(?:ledger|journal entries?|double[- ]entry|accounting postings?|debits?|credits?)\b",
            re.I,
        ),
    ),
    ("refund_system", re.compile(r"\b(?:refunds?|chargebacks?|reversals?|disputes?)\b", re.I)),
    (
        "tax_engine",
        re.compile(r"\b(?:tax engine|tax totals?|tax calculation|vat|gst|sales tax)\b", re.I),
    ),
    (
        "revenue_reporting",
        re.compile(r"\b(?:revenue reporting|revenue report|arr|mrr|finance reporting)\b", re.I),
    ),
    (
        "bank_settlement",
        re.compile(
            r"\b(?:bank settlement|payout settlement|settlement report|bank report|payouts?)\b",
            re.I,
        ),
    ),
)
_FLOW_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "payment_processor_balances",
        re.compile(
            r"\b(?:payment processor balances?|processor balance|stripe balance|payment balance)\b",
            re.I,
        ),
    ),
    (
        "ledger_settlement",
        re.compile(
            r"\b(?:ledger settlement|ledger balance|journal entr(?:y|ies)|double[- ]entry|settlement postings?)\b",
            re.I,
        ),
    ),
    (
        "invoice_total_reconciliation",
        re.compile(
            r"\b(?:invoice totals?|invoice reconciliation|billing totals?|receipt totals?)\b", re.I
        ),
    ),
    (
        "refund_chargeback_reconciliation",
        re.compile(r"\b(?:refunds?|chargebacks?|reversals?|disputes?)\b", re.I),
    ),
    (
        "tax_total_reconciliation",
        re.compile(r"\b(?:tax totals?|tax reconciliation|vat|gst|sales tax|tax rounding)\b", re.I),
    ),
    (
        "revenue_reporting",
        re.compile(
            r"\b(?:revenue reporting|revenue report|arr|mrr|deferred revenue|recognized revenue)\b",
            re.I,
        ),
    ),
    (
        "settlement_reporting",
        re.compile(
            r"\b(?:settlement reports?|payout settlement|bank report|variance reports?|month[- ]end close)\b",
            re.I,
        ),
    ),
)
_OWNER_KEYS = (
    "owner",
    "owners",
    "consumer",
    "consumers",
    "finance_owner",
    "accounting_owner",
    "billing_owner",
    "reconciliation_owner",
    "revenue_owner",
    "tax_owner",
    "owner_team",
    "team",
    "dri",
)
_CONSUMER_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    (
        "finance_operations",
        re.compile(r"\b(?:finance operations|finance ops|finance team|month[- ]end close)\b", re.I),
    ),
    ("accounting", re.compile(r"\b(?:accounting|accountants?|controller|close team)\b", re.I)),
    (
        "revenue_operations",
        re.compile(r"\b(?:revenue operations|revenue ops|revops|revenue team)\b", re.I),
    ),
    ("tax", re.compile(r"\b(?:tax team|tax owner|tax reporting)\b", re.I)),
    (
        "billing_operations",
        re.compile(r"\b(?:billing operations|billing ops|billing team)\b", re.I),
    ),
    (
        "executive_reporting",
        re.compile(r"\b(?:executive reporting|board reporting|cfo reporting)\b", re.I),
    ),
)
_VALIDATION_EVIDENCE_RE = re.compile(
    r"\b(?:validation|validated|compare|compares|matched|matching|tie[- ]out|tie out|"
    r"totals match|variance reports?|balance checks?|reconciliation reports?|settlement reports?|"
    r"audit evidence|audit trail|control evidence|row counts?|checksums?|reviewer sign[- ]?off|approved)\b",
    re.I,
)
_EXPLICIT_GAP_RE = re.compile(
    r"\b(?:gap|missing|unknown|unresolved|tbd|todo|not documented|not defined|"
    r"source unclear|owner unclear|consumer unclear|validation unclear|needs validation)\b",
    re.I,
)
_SOURCE_MISSING_RE = re.compile(
    r"\b(?:source unclear|source unknown|missing source|source tbd|needs source|upstream source is tbd)\b",
    re.I,
)
_OWNER_MISSING_RE = re.compile(
    r"\b(?:owner unclear|owner unknown|missing owner|owner tbd|consumer unclear|consumer unknown|needs owner)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class PlanBillingReconciliationReadinessRow:
    """One grouped billing reconciliation readiness row."""

    reconciliation_source: str
    financial_flow: str
    owner_or_consumer: str
    task_ids: tuple[str, ...]
    titles: tuple[str, ...]
    validation_evidence: tuple[str, ...] = field(default_factory=tuple)
    gaps: tuple[str, ...] = field(default_factory=tuple)
    readiness: BillingReconciliationReadiness = "partial"
    severity: BillingReconciliationSeverity = "medium"
    source_evidence: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "reconciliation_source": self.reconciliation_source,
            "financial_flow": self.financial_flow,
            "owner_or_consumer": self.owner_or_consumer,
            "task_ids": list(self.task_ids),
            "titles": list(self.titles),
            "validation_evidence": list(self.validation_evidence),
            "gaps": list(self.gaps),
            "readiness": self.readiness,
            "severity": self.severity,
            "source_evidence": list(self.source_evidence),
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class PlanBillingReconciliationReadinessMatrix:
    """Plan-level billing reconciliation readiness matrix and rollup counts."""

    plan_id: str | None = None
    rows: tuple[PlanBillingReconciliationReadinessRow, ...] = field(default_factory=tuple)
    billing_reconciliation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_source_task_ids: tuple[str, ...] = field(default_factory=tuple)
    missing_owner_task_ids: tuple[str, ...] = field(default_factory=tuple)
    no_billing_reconciliation_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanBillingReconciliationReadinessRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "billing_reconciliation_task_ids": list(self.billing_reconciliation_task_ids),
            "missing_source_task_ids": list(self.missing_source_task_ids),
            "missing_owner_task_ids": list(self.missing_owner_task_ids),
            "no_billing_reconciliation_task_ids": list(self.no_billing_reconciliation_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return billing reconciliation rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the billing reconciliation readiness matrix as deterministic Markdown."""
        title = "# Plan Billing Reconciliation Readiness Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('billing_reconciliation_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks need billing reconciliation readiness "
                f"(high: {severity_counts.get('high', 0)}, "
                f"medium: {severity_counts.get('medium', 0)}, "
                f"low: {severity_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No billing reconciliation readiness rows were inferred."])
            if self.no_billing_reconciliation_task_ids:
                lines.extend(
                    [
                        "",
                        f"No billing reconciliation signals: {_markdown_cell(', '.join(self.no_billing_reconciliation_task_ids))}",
                    ]
                )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Source | Flow | Owner/Consumer | Tasks | Readiness | Severity | Validation Evidence | Gaps | Source Evidence | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"{_markdown_cell(row.reconciliation_source)} | "
                f"{_markdown_cell(row.financial_flow)} | "
                f"{_markdown_cell(row.owner_or_consumer)} | "
                f"{_markdown_cell(', '.join(row.task_ids))} | "
                f"{row.readiness} | "
                f"{row.severity} | "
                f"{_markdown_cell('; '.join(row.validation_evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(row.gaps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.source_evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        if self.missing_source_task_ids:
            lines.extend(
                [
                    "",
                    f"Missing source tasks: {_markdown_cell(', '.join(self.missing_source_task_ids))}",
                ]
            )
        if self.missing_owner_task_ids:
            lines.extend(
                [
                    "",
                    f"Missing owner tasks: {_markdown_cell(', '.join(self.missing_owner_task_ids))}",
                ]
            )
        if self.no_billing_reconciliation_task_ids:
            lines.extend(
                [
                    "",
                    f"No billing reconciliation signals: {_markdown_cell(', '.join(self.no_billing_reconciliation_task_ids))}",
                ]
            )
        return "\n".join(lines)


def build_plan_billing_reconciliation_readiness_matrix(
    source: Any,
) -> PlanBillingReconciliationReadinessMatrix:
    """Build grouped billing reconciliation readiness for an execution plan."""
    plan_id, tasks = _source_payload(source)
    grouped: dict[tuple[str, str, str], list[_TaskBillingSignals]] = {}
    no_signal_task_ids: list[str] = []
    missing_source_task_ids: list[str] = []
    missing_owner_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        signals = _task_signals(task, index)
        if not signals.has_billing_reconciliation:
            no_signal_task_ids.append(signals.task_id)
            continue
        if signals.reconciliation_source == "missing_source":
            missing_source_task_ids.append(signals.task_id)
        if signals.owner_or_consumer == "missing_owner_or_consumer":
            missing_owner_task_ids.append(signals.task_id)
        grouped.setdefault(
            (signals.reconciliation_source, signals.financial_flow, signals.owner_or_consumer),
            [],
        ).append(signals)

    rows = tuple(
        sorted((_row_from_group(key, values) for key, values in grouped.items()), key=_row_sort_key)
    )
    reconciliation_task_ids = tuple(_dedupe(task_id for row in rows for task_id in row.task_ids))
    missing_source_task_ids_tuple = tuple(
        task_id for task_id in reconciliation_task_ids if task_id in set(missing_source_task_ids)
    )
    missing_owner_task_ids_tuple = tuple(
        task_id for task_id in reconciliation_task_ids if task_id in set(missing_owner_task_ids)
    )

    return PlanBillingReconciliationReadinessMatrix(
        plan_id=plan_id,
        rows=rows,
        billing_reconciliation_task_ids=reconciliation_task_ids,
        missing_source_task_ids=missing_source_task_ids_tuple,
        missing_owner_task_ids=missing_owner_task_ids_tuple,
        no_billing_reconciliation_task_ids=tuple(no_signal_task_ids),
        summary=_summary(
            len(tasks),
            rows,
            missing_source_task_ids_tuple,
            missing_owner_task_ids_tuple,
            no_signal_task_ids,
        ),
    )


def generate_plan_billing_reconciliation_readiness_matrix(
    source: Any,
) -> PlanBillingReconciliationReadinessMatrix:
    """Generate a billing reconciliation readiness matrix from a plan-like source."""
    return build_plan_billing_reconciliation_readiness_matrix(source)


def analyze_plan_billing_reconciliation_readiness_matrix(
    source: Any,
) -> PlanBillingReconciliationReadinessMatrix:
    """Analyze an execution plan for billing reconciliation readiness."""
    if isinstance(source, PlanBillingReconciliationReadinessMatrix):
        return source
    return build_plan_billing_reconciliation_readiness_matrix(source)


def derive_plan_billing_reconciliation_readiness_matrix(
    source: Any,
) -> PlanBillingReconciliationReadinessMatrix:
    """Derive a billing reconciliation readiness matrix from a plan-like source."""
    return analyze_plan_billing_reconciliation_readiness_matrix(source)


def extract_plan_billing_reconciliation_readiness_matrix(
    source: Any,
) -> PlanBillingReconciliationReadinessMatrix:
    """Extract a billing reconciliation readiness matrix from a plan-like source."""
    return derive_plan_billing_reconciliation_readiness_matrix(source)


def summarize_plan_billing_reconciliation_readiness_matrix(
    source: (
        PlanBillingReconciliationReadinessMatrix
        | Iterable[PlanBillingReconciliationReadinessRow]
        | Any
    ),
) -> dict[str, Any] | PlanBillingReconciliationReadinessMatrix:
    """Return deterministic summary counts for a matrix, row iterable, or source."""
    if isinstance(source, PlanBillingReconciliationReadinessMatrix):
        return dict(source.summary)
    if (
        _looks_like_plan(source)
        or _looks_like_task(source)
        or isinstance(source, (Mapping, ExecutionPlan, ExecutionTask))
    ):
        return build_plan_billing_reconciliation_readiness_matrix(source)
    rows = tuple(source)
    missing_source_task_ids = tuple(
        _dedupe(
            task_id
            for row in rows
            if row.reconciliation_source == "missing_source"
            for task_id in row.task_ids
        )
    )
    missing_owner_task_ids = tuple(
        _dedupe(
            task_id
            for row in rows
            if row.owner_or_consumer == "missing_owner_or_consumer"
            for task_id in row.task_ids
        )
    )
    return _summary(len(rows), rows, missing_source_task_ids, missing_owner_task_ids, ())


def plan_billing_reconciliation_readiness_matrix_to_dict(
    matrix: PlanBillingReconciliationReadinessMatrix,
) -> dict[str, Any]:
    """Serialize a billing reconciliation readiness matrix to a plain dictionary."""
    return matrix.to_dict()


plan_billing_reconciliation_readiness_matrix_to_dict.__test__ = False


def plan_billing_reconciliation_readiness_matrix_to_dicts(
    matrix: (
        PlanBillingReconciliationReadinessMatrix | Iterable[PlanBillingReconciliationReadinessRow]
    ),
) -> list[dict[str, Any]]:
    """Serialize billing reconciliation rows to plain dictionaries."""
    if isinstance(matrix, PlanBillingReconciliationReadinessMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_billing_reconciliation_readiness_matrix_to_dicts.__test__ = False


def plan_billing_reconciliation_readiness_matrix_to_markdown(
    matrix: PlanBillingReconciliationReadinessMatrix,
) -> str:
    """Render a billing reconciliation readiness matrix as Markdown."""
    return matrix.to_markdown()


plan_billing_reconciliation_readiness_matrix_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskBillingSignals:
    task_id: str
    title: str
    reconciliation_source: str
    financial_flow: str
    owner_or_consumer: str
    validation_evidence: tuple[str, ...]
    gaps: tuple[str, ...]
    source_evidence: tuple[str, ...]
    evidence: tuple[str, ...]
    has_billing_reconciliation: bool


def _task_signals(task: Mapping[str, Any], index: int) -> _TaskBillingSignals:
    task_id = _task_id(task, index)
    title = _optional_text(task.get("title")) or task_id
    texts = _candidate_texts(task)
    context = " ".join(text for _, text in texts)
    source, source_evidence = _first_match(_SOURCE_PATTERNS, texts)
    flow, flow_evidence = _first_match(_FLOW_PATTERNS, texts)
    owner = _owner_or_consumer(task, texts)
    if _SOURCE_MISSING_RE.search(context):
        source = None
        source_evidence = ()
    if _OWNER_MISSING_RE.search(context):
        owner = None
    validation_evidence = tuple(
        _dedupe(
            _evidence_snippet(field, text)
            for field, text in texts
            if _VALIDATION_EVIDENCE_RE.search(text)
        )
    )
    explicit_gaps = tuple(
        _dedupe(
            _evidence_snippet(field, text) for field, text in texts if _EXPLICIT_GAP_RE.search(text)
        )
    )
    billing_evidence = _billing_evidence(texts)
    has_billing_reconciliation = bool(_BILLING_TRIGGER_RE.search(context)) or bool(source and flow)

    gaps: list[str] = list(explicit_gaps)
    if has_billing_reconciliation and not source:
        gaps.append(
            "Missing reconciliation source such as billing, ledger, processor, tax, refund, or revenue system."
        )
    if has_billing_reconciliation and not owner:
        gaps.append("Missing finance owner or downstream consumer for reconciliation sign-off.")
    if has_billing_reconciliation and not flow:
        gaps.append("Missing financial flow being reconciled.")
    if has_billing_reconciliation and not validation_evidence:
        gaps.append(
            "Missing validation evidence that proves reconciled balances, totals, variances, or reports."
        )

    return _TaskBillingSignals(
        task_id=task_id,
        title=title,
        reconciliation_source=source or "missing_source",
        financial_flow=flow or "unspecified_financial_flow",
        owner_or_consumer=owner or "missing_owner_or_consumer",
        validation_evidence=validation_evidence,
        gaps=tuple(_dedupe(gaps)),
        source_evidence=source_evidence,
        evidence=tuple(_dedupe((*source_evidence, *flow_evidence, *billing_evidence))),
        has_billing_reconciliation=has_billing_reconciliation,
    )


def _row_from_group(
    key: tuple[str, str, str],
    signals: list[_TaskBillingSignals],
) -> PlanBillingReconciliationReadinessRow:
    reconciliation_source, financial_flow, owner_or_consumer = key
    gaps = tuple(_dedupe(gap for signal in signals for gap in signal.gaps))
    validation_evidence = tuple(
        _dedupe(item for signal in signals for item in signal.validation_evidence)
    )
    readiness = _readiness(
        reconciliation_source, financial_flow, owner_or_consumer, validation_evidence, gaps
    )
    return PlanBillingReconciliationReadinessRow(
        reconciliation_source=reconciliation_source,
        financial_flow=financial_flow,
        owner_or_consumer=owner_or_consumer,
        task_ids=tuple(
            _dedupe(signal.task_id for signal in sorted(signals, key=lambda item: item.task_id))
        ),
        titles=tuple(
            _dedupe(signal.title for signal in sorted(signals, key=lambda item: item.task_id))
        ),
        validation_evidence=validation_evidence,
        gaps=gaps,
        readiness=readiness,
        severity=_severity(
            reconciliation_source, financial_flow, owner_or_consumer, validation_evidence, gaps
        ),
        source_evidence=tuple(
            _dedupe(item for signal in signals for item in signal.source_evidence)
        ),
        evidence=tuple(_dedupe(item for signal in signals for item in signal.evidence)),
    )


def _readiness(
    reconciliation_source: str,
    financial_flow: str,
    owner_or_consumer: str,
    validation_evidence: tuple[str, ...],
    gaps: tuple[str, ...],
) -> BillingReconciliationReadiness:
    if (
        reconciliation_source == "missing_source"
        or owner_or_consumer == "missing_owner_or_consumer"
    ):
        return "blocked"
    if financial_flow == "unspecified_financial_flow" or not validation_evidence or gaps:
        return "partial"
    return "ready"


def _severity(
    reconciliation_source: str,
    financial_flow: str,
    owner_or_consumer: str,
    validation_evidence: tuple[str, ...],
    gaps: tuple[str, ...],
) -> BillingReconciliationSeverity:
    if (
        reconciliation_source == "missing_source"
        or owner_or_consumer == "missing_owner_or_consumer"
    ):
        return "high"
    if financial_flow == "unspecified_financial_flow" or not validation_evidence or gaps:
        return "medium"
    return "low"


def _summary(
    task_count: int,
    rows: Iterable[PlanBillingReconciliationReadinessRow],
    missing_source_task_ids: tuple[str, ...],
    missing_owner_task_ids: tuple[str, ...],
    no_signal_task_ids: Iterable[str],
) -> dict[str, Any]:
    row_list = list(rows)
    reconciliation_task_ids = tuple(
        _dedupe(task_id for row in row_list for task_id in row.task_ids)
    )
    return {
        "task_count": task_count,
        "row_count": len(row_list),
        "billing_reconciliation_task_count": len(reconciliation_task_ids),
        "missing_source_task_count": len(missing_source_task_ids),
        "missing_owner_task_count": len(missing_owner_task_ids),
        "no_billing_reconciliation_task_count": len(tuple(no_signal_task_ids)),
        "readiness_counts": {
            readiness: sum(1 for row in row_list if row.readiness == readiness)
            for readiness in _READINESS_ORDER
        },
        "severity_counts": {
            severity: sum(1 for row in row_list if row.severity == severity)
            for severity in _SEVERITY_ORDER
        },
        "source_counts": {
            source: sum(1 for row in row_list if row.reconciliation_source == source)
            for source in sorted({row.reconciliation_source for row in row_list})
        },
        "owner_or_consumer_counts": {
            owner: sum(1 for row in row_list if row.owner_or_consumer == owner)
            for owner in sorted({row.owner_or_consumer for row in row_list})
        },
    }


def _row_sort_key(
    row: PlanBillingReconciliationReadinessRow,
) -> tuple[int, int, str, str, str, str]:
    return (
        _SEVERITY_ORDER[row.severity],
        _READINESS_ORDER[row.readiness],
        row.reconciliation_source,
        row.financial_flow,
        row.owner_or_consumer,
        ",".join(row.task_ids),
    )


def _first_match(
    patterns: tuple[tuple[str, re.Pattern[str]], ...],
    texts: Iterable[tuple[str, str]],
) -> tuple[str | None, tuple[str, ...]]:
    for value, pattern in patterns:
        for source_field, text in texts:
            if pattern.search(text):
                return value, (_evidence_snippet(source_field, text),)
    return None, ()


def _owner_or_consumer(task: Mapping[str, Any], texts: Iterable[tuple[str, str]]) -> str | None:
    metadata = task.get("metadata") if isinstance(task.get("metadata"), Mapping) else {}
    owner = _first_metadata_value(metadata, *_OWNER_KEYS)
    if owner:
        return _slug(owner)
    for key in _OWNER_KEYS:
        if value := next(iter(_strings(task.get(key))), None):
            return _slug(value)
    consumer, _ = _first_match(_CONSUMER_PATTERNS, texts)
    return consumer


def _billing_evidence(texts: Iterable[tuple[str, str]]) -> tuple[str, ...]:
    evidence: list[str] = []
    patterns = (
        _BILLING_TRIGGER_RE,
        _VALIDATION_EVIDENCE_RE,
        *[pattern for _, pattern in _SOURCE_PATTERNS],
        *[pattern for _, pattern in _FLOW_PATTERNS],
    )
    for source_field, text in texts:
        if source_field.startswith(("depends_on", "dependencies")) or any(
            pattern.search(text) for pattern in patterns
        ):
            evidence.append(_evidence_snippet(source_field, text))
    return tuple(_dedupe(evidence))


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


def _plan_payload(plan: Mapping[str, Any] | object) -> dict[str, Any]:
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
        "owner",
        "owner_type",
        "suggested_engine",
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "acceptance_criteria",
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
        "owner",
        "owner_type",
        "suggested_engine",
        "risk_level",
        "test_command",
        "validation_command",
        "blocked_reason",
    ):
        if text := _optional_text(task.get(field_name)):
            texts.append((field_name, text))
    for field_name in (
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "tags",
        "labels",
        "notes",
        "test_commands",
        "validation_commands",
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
            key_text = str(key).replace("_", " ")
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                texts.append((field, f"{key_text}: {text}"))
            elif key_text:
                texts.append((field, key_text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts: list[tuple[str, str]] = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


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


def _slug(value: str) -> str:
    text = _text(value).casefold().replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return text or "missing_owner_or_consumer"


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
    "BillingReconciliationReadiness",
    "BillingReconciliationSeverity",
    "PlanBillingReconciliationReadinessMatrix",
    "PlanBillingReconciliationReadinessRow",
    "analyze_plan_billing_reconciliation_readiness_matrix",
    "build_plan_billing_reconciliation_readiness_matrix",
    "derive_plan_billing_reconciliation_readiness_matrix",
    "extract_plan_billing_reconciliation_readiness_matrix",
    "generate_plan_billing_reconciliation_readiness_matrix",
    "plan_billing_reconciliation_readiness_matrix_to_dict",
    "plan_billing_reconciliation_readiness_matrix_to_dicts",
    "plan_billing_reconciliation_readiness_matrix_to_markdown",
    "summarize_plan_billing_reconciliation_readiness_matrix",
]
