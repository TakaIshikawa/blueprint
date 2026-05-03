"""Build regional pricing rollout decision matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


RegionalPricingSignal = Literal[
    "regional_pricing",
    "currency",
    "localized_price_book",
    "tax_inclusive_price",
    "region_availability",
]
RegionalPricingControl = Literal[
    "rollout_gate",
    "feature_flag",
    "price_book_approval",
    "tax_validation",
    "billing_reconciliation",
    "localization_review",
    "availability_gate",
    "monitoring",
]
RegionalPricingRiskLevel = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_SIGNAL_ORDER: tuple[RegionalPricingSignal, ...] = (
    "regional_pricing",
    "currency",
    "localized_price_book",
    "tax_inclusive_price",
    "region_availability",
)
_CONTROL_ORDER: tuple[RegionalPricingControl, ...] = (
    "rollout_gate",
    "feature_flag",
    "price_book_approval",
    "tax_validation",
    "billing_reconciliation",
    "localization_review",
    "availability_gate",
    "monitoring",
)
_RISK_ORDER: dict[RegionalPricingRiskLevel, int] = {"high": 0, "medium": 1, "low": 2}
_SIGNAL_PATTERNS: dict[RegionalPricingSignal, re.Pattern[str]] = {
    "regional_pricing": re.compile(
        r"\b(?:regional pricing|region(?:al)? price|price by region|market pricing|"
        r"country pricing|geo(?:graphic)? pricing|regional price rollout)\b",
        re.I,
    ),
    "currency": re.compile(
        r"\b(?:currency|currencies|fx|foreign exchange|exchange rate|multi[- ]currency|"
        r"usd|eur|gbp|jpy|cad|aud|nzd|inr|brl|mxn|sgd|sek|nok|dkk|chf|zar)\b",
        re.I,
    ),
    "localized_price_book": re.compile(
        r"\b(?:localized price book|localised price book|price book|pricebook|price[_ -]?books?|pricing catalog|"
        r"pricing catalogue|localized prices?|localised prices?|localized plan prices?)\b",
        re.I,
    ),
    "tax_inclusive_price": re.compile(
        r"\b(?:tax[- ]inclusive|tax inclusive|vat[- ]inclusive|gst[- ]inclusive|inclusive tax|"
        r"tax included|vat included|gross price|tax display)\b",
        re.I,
    ),
    "region_availability": re.compile(
        r"\b(?:region availability|regional availability|available in (?:us|eu|uk|apac|emea|"
        r"latam|canada|australia|japan|india|brazil|mexico)|country availability|"
        r"market availability|launch regions?|blocked regions?|unsupported regions?)\b",
        re.I,
    ),
}
_CONTROL_PATTERNS: dict[RegionalPricingControl, re.Pattern[str]] = {
    "rollout_gate": re.compile(r"\b(?:rollout gate|go/no[- ]?go|launch gate|approval gate|decision gate|stage gate)\b", re.I),
    "feature_flag": re.compile(r"\b(?:feature flag|flag|toggle|kill switch|allowlist|cohort|canary|gradual rollout)\b", re.I),
    "price_book_approval": re.compile(r"\b(?:price book approval|pricing approval|finance approval|approved price book|pricing sign[- ]off|revenue approval)\b", re.I),
    "tax_validation": re.compile(r"\b(?:tax validation|vat validation|gst validation|tax engine|avalara|taxjar|tax calculation|tax audit)\b", re.I),
    "billing_reconciliation": re.compile(r"\b(?:billing reconciliation|invoice reconciliation|charge reconciliation|reconcile invoices?|refund check|credit memo)\b", re.I),
    "localization_review": re.compile(r"\b(?:localization review|localisation review|translation review|locale review|localized copy review|i18n review)\b", re.I),
    "availability_gate": re.compile(r"\b(?:availability gate|region allowlist|country allowlist|geo allowlist|market enablement|availability check)\b", re.I),
    "monitoring": re.compile(r"\b(?:monitor|monitoring|dashboard|alert|synthetic|smoke test|validation command)\b", re.I),
}
_REGION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("US", re.compile(r"\b(?:us|u\.s\.|usa|united states|north america)\b", re.I)),
    ("EU", re.compile(r"\b(?:eu|europe|european union)\b", re.I)),
    ("UK", re.compile(r"\b(?:uk|u\.k\.|united kingdom|great britain)\b", re.I)),
    ("APAC", re.compile(r"\b(?:apac|asia pacific)\b", re.I)),
    ("EMEA", re.compile(r"\bemea\b", re.I)),
    ("LATAM", re.compile(r"\b(?:latam|latin america)\b", re.I)),
    ("CA", re.compile(r"\b(?:canada|ca)\b", re.I)),
    ("AU", re.compile(r"\b(?:australia|au)\b", re.I)),
    ("JP", re.compile(r"\b(?:japan|jp)\b", re.I)),
    ("IN", re.compile(r"\b(?:india|india market|in market)\b", re.I)),
    ("BR", re.compile(r"\b(?:brazil|br)\b", re.I)),
    ("MX", re.compile(r"\b(?:mexico|mx)\b", re.I)),
)
_CURRENCY_RE = re.compile(r"\b(?:USD|EUR|GBP|JPY|CAD|AUD|NZD|INR|BRL|MXN|SGD|SEK|NOK|DKK|CHF|ZAR)\b")
_OWNER_KEY_RE = re.compile(r"\b(?:owner|owners|dri|responsible|team|lead|approver|approval|finance|tax|billing)\b", re.I)
_RISK_TEXT_RE = re.compile(r"\b(?:high|critical|blocked|blocker|must approve|legal|compliance|tax exposure)\b", re.I)


@dataclass(frozen=True, slots=True)
class PlanRegionalPricingRolloutMatrixRow:
    """One regional pricing rollout decision row grouped by scope and signals."""

    task_ids: tuple[str, ...]
    regions_or_currencies: tuple[str, ...]
    detected_signals: tuple[RegionalPricingSignal, ...]
    present_controls: tuple[RegionalPricingControl, ...] = field(default_factory=tuple)
    missing_decisions: tuple[str, ...] = field(default_factory=tuple)
    risk_level: RegionalPricingRiskLevel = "medium"
    owners: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_steps: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_ids": list(self.task_ids),
            "regions_or_currencies": list(self.regions_or_currencies),
            "detected_signals": list(self.detected_signals),
            "present_controls": list(self.present_controls),
            "missing_decisions": list(self.missing_decisions),
            "risk_level": self.risk_level,
            "owners": list(self.owners),
            "evidence": list(self.evidence),
            "recommended_steps": list(self.recommended_steps),
        }


@dataclass(frozen=True, slots=True)
class PlanRegionalPricingRolloutMatrix:
    """Plan-level regional pricing rollout matrix and summary counts."""

    plan_id: str | None = None
    rows: tuple[PlanRegionalPricingRolloutMatrixRow, ...] = field(default_factory=tuple)
    affected_task_ids: tuple[str, ...] = field(default_factory=tuple)
    not_applicable_task_ids: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[PlanRegionalPricingRolloutMatrixRow, ...]:
        """Compatibility view for consumers that call matrix rows records."""
        return self.rows

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "rows": [row.to_dict() for row in self.rows],
            "records": [row.to_dict() for row in self.records],
            "affected_task_ids": list(self.affected_task_ids),
            "not_applicable_task_ids": list(self.not_applicable_task_ids),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return regional pricing rollout rows as plain dictionaries."""
        return [row.to_dict() for row in self.rows]

    def to_markdown(self) -> str:
        """Render the matrix as deterministic Markdown."""
        title = "# Plan Regional Pricing Rollout Matrix"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        risk_counts = self.summary.get("risk_counts", {})
        lines = [
            title,
            "",
            (
                f"Summary: {self.summary.get('affected_task_count', 0)} of "
                f"{self.summary.get('task_count', 0)} tasks affect regional pricing "
                f"(high: {risk_counts.get('high', 0)}, medium: {risk_counts.get('medium', 0)}, "
                f"low: {risk_counts.get('low', 0)})."
            ),
        ]
        if not self.rows:
            lines.extend(["", "No regional pricing rollout rows were inferred."])
            if self.not_applicable_task_ids:
                lines.extend(["", f"Not applicable: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                (
                    "| Tasks | Regions/Currencies | Signals | Controls | Missing Decisions | "
                    "Risk | Owners | Recommended Steps | Evidence |"
                ),
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for row in self.rows:
            lines.append(
                "| "
                f"`{_markdown_cell(', '.join(row.task_ids))}` | "
                f"{_markdown_cell(', '.join(row.regions_or_currencies))} | "
                f"{_markdown_cell(', '.join(row.detected_signals))} | "
                f"{_markdown_cell(', '.join(row.present_controls) or 'none')} | "
                f"{_markdown_cell('; '.join(row.missing_decisions) or 'none')} | "
                f"{row.risk_level} | "
                f"{_markdown_cell('; '.join(row.owners) or 'none')} | "
                f"{_markdown_cell('; '.join(row.recommended_steps) or 'none')} | "
                f"{_markdown_cell('; '.join(row.evidence) or 'none')} |"
            )
        if self.not_applicable_task_ids:
            lines.extend(["", f"Not applicable: {_markdown_cell(', '.join(self.not_applicable_task_ids))}"])
        return "\n".join(lines)


def build_plan_regional_pricing_rollout_matrix(source: Any) -> PlanRegionalPricingRolloutMatrix:
    """Build rollout decision rows for tasks that touch regional prices or availability."""
    plan_id, tasks = _source_payload(source)
    task_scans = [_task_scan(task, index) for index, task in enumerate(tasks, start=1)]
    applicable = [scan for scan in task_scans if scan["signals"]]
    groups: dict[tuple[tuple[str, ...], tuple[RegionalPricingSignal, ...]], list[dict[str, Any]]] = {}
    for scan in applicable:
        key = (scan["scope"], scan["signals"])
        groups.setdefault(key, []).append(scan)

    rows = tuple(
        sorted(
            (_group_row(scans) for scans in groups.values()),
            key=lambda row: (
                _RISK_ORDER[row.risk_level],
                row.regions_or_currencies,
                tuple(_SIGNAL_ORDER.index(signal) for signal in row.detected_signals),
                row.task_ids,
            ),
        )
    )
    affected_task_ids = tuple(_dedupe(task_id for row in rows for task_id in row.task_ids))
    not_applicable_task_ids = tuple(scan["task_id"] for scan in task_scans if not scan["signals"])
    return PlanRegionalPricingRolloutMatrix(
        plan_id=plan_id,
        rows=rows,
        affected_task_ids=affected_task_ids,
        not_applicable_task_ids=not_applicable_task_ids,
        summary=_summary(len(tasks), rows, not_applicable_task_ids),
    )


def generate_plan_regional_pricing_rollout_matrix(source: Any) -> PlanRegionalPricingRolloutMatrix:
    """Generate a regional pricing rollout matrix from a plan-like source."""
    return build_plan_regional_pricing_rollout_matrix(source)


def analyze_plan_regional_pricing_rollout_matrix(source: Any) -> PlanRegionalPricingRolloutMatrix:
    """Analyze an execution plan for regional pricing rollout decisions."""
    if isinstance(source, PlanRegionalPricingRolloutMatrix):
        return source
    return build_plan_regional_pricing_rollout_matrix(source)


def derive_plan_regional_pricing_rollout_matrix(source: Any) -> PlanRegionalPricingRolloutMatrix:
    """Derive a regional pricing rollout matrix from a plan-like source."""
    return analyze_plan_regional_pricing_rollout_matrix(source)


def extract_plan_regional_pricing_rollout_matrix(source: Any) -> PlanRegionalPricingRolloutMatrix:
    """Extract a regional pricing rollout matrix from a plan-like source."""
    return derive_plan_regional_pricing_rollout_matrix(source)


def summarize_plan_regional_pricing_rollout_matrix(
    matrix: PlanRegionalPricingRolloutMatrix | Iterable[PlanRegionalPricingRolloutMatrixRow] | Any,
) -> dict[str, Any] | PlanRegionalPricingRolloutMatrix:
    """Return deterministic summary counts for a matrix, row iterable, or source."""
    if isinstance(matrix, PlanRegionalPricingRolloutMatrix):
        return dict(matrix.summary)
    if _looks_like_plan(matrix) or _looks_like_task(matrix) or isinstance(matrix, (Mapping, ExecutionPlan, ExecutionTask)):
        return build_plan_regional_pricing_rollout_matrix(matrix)
    rows = tuple(matrix)
    return _summary(len(rows), rows, ())


def plan_regional_pricing_rollout_matrix_to_dict(matrix: PlanRegionalPricingRolloutMatrix) -> dict[str, Any]:
    """Serialize a regional pricing rollout matrix to a plain dictionary."""
    return matrix.to_dict()


plan_regional_pricing_rollout_matrix_to_dict.__test__ = False


def plan_regional_pricing_rollout_matrix_to_dicts(
    matrix: PlanRegionalPricingRolloutMatrix | Iterable[PlanRegionalPricingRolloutMatrixRow],
) -> list[dict[str, Any]]:
    """Serialize regional pricing rollout rows to plain dictionaries."""
    if isinstance(matrix, PlanRegionalPricingRolloutMatrix):
        return matrix.to_dicts()
    return [row.to_dict() for row in matrix]


plan_regional_pricing_rollout_matrix_to_dicts.__test__ = False


def plan_regional_pricing_rollout_matrix_to_markdown(matrix: PlanRegionalPricingRolloutMatrix) -> str:
    """Render a regional pricing rollout matrix as Markdown."""
    return matrix.to_markdown()


plan_regional_pricing_rollout_matrix_to_markdown.__test__ = False


def _task_scan(task: Mapping[str, Any], index: int) -> dict[str, Any]:
    text_pairs = _candidate_texts(task)
    joined = " ".join(text for _, text in text_pairs)
    signals, evidence = _signals(text_pairs)
    controls = _controls(text_pairs)
    scope = _scope(joined)
    owners = _owners(task, text_pairs, signals)
    missing = _missing_decisions(signals, controls, owners)
    return {
        "task_id": _task_id(task, index),
        "signals": signals,
        "controls": controls,
        "scope": scope,
        "owners": owners,
        "missing": missing,
        "evidence": evidence,
        "risk_text": joined,
    }


def _group_row(scans: list[dict[str, Any]]) -> PlanRegionalPricingRolloutMatrixRow:
    ordered_scans = sorted(scans, key=lambda scan: scan["task_id"])
    signals = tuple(_ordered_dedupe((signal for scan in ordered_scans for signal in scan["signals"]), _SIGNAL_ORDER))
    controls = tuple(_ordered_dedupe((control for scan in ordered_scans for control in scan["controls"]), _CONTROL_ORDER))
    owners = tuple(_ordered_owners(ordered_scans, signals))
    missing = tuple(_dedupe(decision for scan in ordered_scans for decision in scan["missing"]))
    joined_text = " ".join(scan["risk_text"] for scan in ordered_scans)
    risk_level = _risk_level(signals, controls, missing, joined_text)
    return PlanRegionalPricingRolloutMatrixRow(
        task_ids=tuple(scan["task_id"] for scan in ordered_scans),
        regions_or_currencies=ordered_scans[0]["scope"],
        detected_signals=signals,
        present_controls=controls,
        missing_decisions=missing,
        risk_level=risk_level,
        owners=owners,
        evidence=tuple(_dedupe(evidence for scan in ordered_scans for evidence in scan["evidence"])),
        recommended_steps=_recommended_steps(signals, controls, missing),
    )


def _signals(text_pairs: Iterable[tuple[str, str]]) -> tuple[tuple[RegionalPricingSignal, ...], tuple[str, ...]]:
    found: set[RegionalPricingSignal] = set()
    evidence: list[str] = []
    for field, text in text_pairs:
        matched = False
        for signal, pattern in _SIGNAL_PATTERNS.items():
            if pattern.search(text):
                found.add(signal)
                matched = True
        if matched:
            evidence.append(_evidence_snippet(field, text))
    return tuple(signal for signal in _SIGNAL_ORDER if signal in found), tuple(_dedupe(evidence))


def _controls(text_pairs: Iterable[tuple[str, str]]) -> tuple[RegionalPricingControl, ...]:
    found: set[RegionalPricingControl] = set()
    for _, text in text_pairs:
        for control, pattern in _CONTROL_PATTERNS.items():
            if pattern.search(text):
                found.add(control)
    return tuple(control for control in _CONTROL_ORDER if control in found)


def _scope(text: str) -> tuple[str, ...]:
    values: list[str] = []
    for label, pattern in _REGION_PATTERNS:
        if pattern.search(text):
            values.append(label)
    values.extend(match.group(0).upper() for match in _CURRENCY_RE.finditer(text))
    return tuple(_dedupe(values)) or ("unspecified-region-or-currency",)


def _missing_decisions(
    signals: tuple[RegionalPricingSignal, ...],
    controls: tuple[RegionalPricingControl, ...],
    owners: tuple[str, ...],
) -> tuple[str, ...]:
    if not signals:
        return ()
    missing: list[str] = []
    if not owners:
        missing.append("Assign regional pricing rollout owner.")
    if "rollout_gate" not in controls and "feature_flag" not in controls:
        missing.append("Define regional rollout gate, cohort, or kill-switch decision.")
    if any(signal in signals for signal in ("currency", "localized_price_book")) and "price_book_approval" not in controls:
        missing.append("Approve localized price book, currency conversion, and rounding rules.")
    if "tax_inclusive_price" in signals and "tax_validation" not in controls:
        missing.append("Validate tax-inclusive display and tax engine calculations per market.")
    if "region_availability" in signals and "availability_gate" not in controls:
        missing.append("Decide region availability eligibility, exclusions, and launch sequencing.")
    if any(signal in signals for signal in ("regional_pricing", "currency", "tax_inclusive_price")) and "billing_reconciliation" not in controls:
        missing.append("Define billing, invoice, refund, and revenue reconciliation checks.")
    if any(signal in signals for signal in ("regional_pricing", "localized_price_book")) and "localization_review" not in controls:
        missing.append("Complete localization review for price presentation and market copy.")
    return tuple(_dedupe(missing))


def _risk_level(
    signals: tuple[RegionalPricingSignal, ...],
    controls: tuple[RegionalPricingControl, ...],
    missing: tuple[str, ...],
    text: str,
) -> RegionalPricingRiskLevel:
    if _RISK_TEXT_RE.search(text):
        return "high"
    if "tax_inclusive_price" in signals and "tax_validation" not in controls:
        return "high"
    if len(missing) >= 4 or len(signals) >= 4:
        return "high"
    if missing:
        return "medium"
    return "low"


def _recommended_steps(
    signals: tuple[RegionalPricingSignal, ...],
    controls: tuple[RegionalPricingControl, ...],
    missing: tuple[str, ...],
) -> tuple[str, ...]:
    steps: list[str] = []
    if missing:
        steps.append("Close missing rollout decisions before enabling regional prices.")
    if "tax_inclusive_price" in signals:
        steps.append("Run tax-inclusive price validation against billing and tax providers.")
    if "currency" in signals or "localized_price_book" in signals:
        steps.append("Freeze approved price book, currency rounding, and FX assumptions for launch.")
    if "region_availability" in signals:
        steps.append("Gate launch by region availability and customer eligibility checks.")
    if "monitoring" not in controls:
        steps.append("Add launch monitoring for checkout, invoice, and conversion anomalies.")
    if not steps:
        steps.append("Proceed with documented owner, controls, and validation evidence.")
    return tuple(_dedupe(steps))


def _owners(
    task: Mapping[str, Any],
    text_pairs: Iterable[tuple[str, str]],
    signals: tuple[RegionalPricingSignal, ...],
) -> tuple[str, ...]:
    owners: list[str] = []
    for field, text in text_pairs:
        normalized = field.replace("_", " ")
        if normalized == "owner type":
            continue
        if _OWNER_KEY_RE.search(normalized):
            owners.extend(_strings(text))
        elif _OWNER_KEY_RE.search(text) and len(text) <= 160:
            owners.extend(_owner_values(text))

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("owner", "owners", "pricing_owner", "billing_owner", "tax_owner", "dri", "team", "approver"):
            owners.extend(_strings(metadata.get(key)))
    owners.extend(_default_owners(signals))
    return tuple(_dedupe(_clean_owner(owner) for owner in owners if _clean_owner(owner)))


def _owner_values(text: str) -> list[str]:
    match = re.search(
        r"\b(?:owner|owners|dri|responsible|team|lead|approver)\s*(?::|is|=)\s*([A-Z][A-Za-z0-9_.@& /-]{1,80})\b",
        _text(text),
        re.I,
    )
    return [match.group(1)] if match else []


def _default_owners(signals: tuple[RegionalPricingSignal, ...]) -> tuple[str, ...]:
    owners: list[str] = ["pricing owner", "billing owner"]
    if "tax_inclusive_price" in signals:
        owners.append("tax owner")
    if "localized_price_book" in signals or "regional_pricing" in signals:
        owners.append("localization owner")
    if "region_availability" in signals:
        owners.append("product availability owner")
    return tuple(_dedupe(owners))


def _ordered_owners(
    scans: Iterable[dict[str, Any]],
    signals: tuple[RegionalPricingSignal, ...],
) -> list[str]:
    defaults = set(_default_owners(signals))
    explicit = _dedupe(owner for scan in scans for owner in scan["owners"] if owner not in defaults)
    return _dedupe([*explicit, *_default_owners(signals)])


def _summary(
    task_count: int,
    rows: tuple[PlanRegionalPricingRolloutMatrixRow, ...],
    not_applicable_task_ids: tuple[str, ...],
) -> dict[str, Any]:
    affected_task_ids = tuple(_dedupe(task_id for row in rows for task_id in row.task_ids))
    return {
        "task_count": task_count,
        "row_count": len(rows),
        "affected_task_count": len(affected_task_ids),
        "not_applicable_task_count": len(not_applicable_task_ids),
        "risk_counts": {
            risk: sum(1 for row in rows if row.risk_level == risk)
            for risk in _RISK_ORDER
        },
        "signal_counts": {
            signal: sum(1 for row in rows if signal in row.detected_signals)
            for signal in _SIGNAL_ORDER
        },
        "control_counts": {
            control: sum(1 for row in rows if control in row.present_controls)
            for control in _CONTROL_ORDER
        },
        "missing_decision_count": sum(len(row.missing_decisions) for row in rows),
        "affected_task_ids": list(affected_task_ids),
        "not_applicable_task_ids": list(not_applicable_task_ids),
    }


def _source_payload(source: Any) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _item_payloads(payload.get("tasks"))
        return None, [dict(source)]
    if _looks_like_task(source):
        return None, [_object_payload(source)]
    if _looks_like_plan(source):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), _item_payloads(payload.get("tasks"))

    try:
        iterator = iter(source)  # type: ignore[arg-type]
    except TypeError:
        return None, []
    return None, [task for task in (_item_payload(item) for item in iterator) if task]


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


def _item_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple, set)):
        return []
    items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
    return [payload for payload in (_item_payload(item) for item in items) if payload]


def _item_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, ExecutionTask):
        return value.model_dump(mode="python")
    if hasattr(value, "model_dump"):
        payload = value.model_dump(mode="python")
        return dict(payload) if isinstance(payload, Mapping) else {}
    if isinstance(value, Mapping):
        return dict(value)
    if _looks_like_task(value) or _looks_like_plan(value):
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
        "definition_of_done",
        "validation_commands",
        "estimated_complexity",
        "risk_level",
        "test_command",
        "status",
        "metadata",
        "blocked_reason",
        "tags",
        "labels",
        "notes",
        "tasks",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_texts(task: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in (
        "id",
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
    for field_name in (
        "depends_on",
        "dependencies",
        "files_or_modules",
        "files",
        "acceptance_criteria",
        "definition_of_done",
        "validation_commands",
        "tags",
        "labels",
        "notes",
    ):
        for index, text in enumerate(_strings(task.get(field_name))):
            texts.append((f"{field_name}[{index}]", text))
    texts.extend(_metadata_texts(task.get("metadata")))
    return texts


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ").replace("-", " ")
            if _any_signal(key_text):
                texts.append((field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _any_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
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


def _any_signal(text: str) -> bool:
    return (
        any(pattern.search(text) for pattern in _SIGNAL_PATTERNS.values())
        or any(pattern.search(text) for pattern in _CONTROL_PATTERNS.values())
    )


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        values: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            values.extend(_strings(value[key]))
        return values
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        values: list[str] = []
        for item in items:
            values.extend(_strings(item))
        return values
    text = _optional_text(value)
    return [text] if text else []


def _clean_owner(value: str) -> str:
    return re.sub(r"^(?:owner|owners|dri|team|approver|lead)\s*[:=]\s*", "", _text(value), flags=re.I)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _evidence_snippet(source_field: str, text: str, *, limit: int = 180) -> str:
    normalized = _text(text)
    if len(normalized) > limit:
        normalized = f"{normalized[: limit - 1].rstrip()}..."
    return f"{source_field}: {normalized}"


def _dedupe(values: Iterable[_T]) -> list[_T]:
    seen: set[_T] = set()
    result: list[_T] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _ordered_dedupe(values: Iterable[_T], order: tuple[_T, ...]) -> list[_T]:
    found = set(_dedupe(values))
    return [value for value in order if value in found]


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


__all__ = [
    "RegionalPricingControl",
    "RegionalPricingRiskLevel",
    "RegionalPricingSignal",
    "PlanRegionalPricingRolloutMatrix",
    "PlanRegionalPricingRolloutMatrixRow",
    "analyze_plan_regional_pricing_rollout_matrix",
    "build_plan_regional_pricing_rollout_matrix",
    "derive_plan_regional_pricing_rollout_matrix",
    "extract_plan_regional_pricing_rollout_matrix",
    "generate_plan_regional_pricing_rollout_matrix",
    "plan_regional_pricing_rollout_matrix_to_dict",
    "plan_regional_pricing_rollout_matrix_to_dicts",
    "plan_regional_pricing_rollout_matrix_to_markdown",
    "summarize_plan_regional_pricing_rollout_matrix",
]
