"""Extract pricing and monetization assumptions from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


PricingModelAssumptionType = Literal[
    "seat_based_pricing",
    "usage_based_pricing",
    "tiered_plan",
    "trial",
    "discount",
    "overage",
    "invoice",
    "tax_vat",
    "grandfathering",
    "plan_limit",
]
PricingModelConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_ASSUMPTION_ORDER: tuple[PricingModelAssumptionType, ...] = (
    "seat_based_pricing",
    "usage_based_pricing",
    "tiered_plan",
    "trial",
    "discount",
    "overage",
    "invoice",
    "tax_vat",
    "grandfathering",
    "plan_limit",
)
_CONFIDENCE_ORDER: dict[PricingModelConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but)\s+", re.I)
_MONEY_RE = re.compile(
    r"(?:[$€£]\s*\d+(?:[.,]\d+)?|\b\d+(?:[.,]\d+)?\s*(?:usd|eur|gbp|dollars?|euros?|yen)\b)",
    re.I,
)
_PRICING_CONTEXT_RE = re.compile(
    r"\b(?:price|pricing|paid|billing|billable|charge|charged|cost|costs|fee|fees|"
    r"subscription|plan|plans|tier|tiers|invoice|invoices|tax|vat|discount|coupon|"
    r"trial|overage|metered|usage[- ]based|per[- ]seat|per user|per member)\b",
    re.I,
)
_ENTITLEMENT_ONLY_RE = re.compile(
    r"\b(?:entitlement|entitlements|feature gate|feature flag|gated feature|access check|"
    r"permission check|paywall|locked feature|unlock|allowed to access)\b",
    re.I,
)
_PLAN_RE = re.compile(
    r"\b(?:free|starter|basic|standard|pro|team|teams|business|premium|paid|enterprise|"
    r"trial)\s+(?:plan|tier|workspace|account|customer|customers|users?)\b|"
    r"\b(?:free|starter|basic|standard|pro|team|teams|business|premium|paid|enterprise)\b",
    re.I,
)
_AUDIENCE_RE = re.compile(
    r"\b(?:admins?|members?|users?|customers?|accounts?|workspaces?|tenants?|developers?|"
    r"students?|nonprofits?|annual customers?|monthly customers?)\b",
    re.I,
)
_HIGH_CONFIDENCE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|need(?:s)?|billable|charged?|invoice|tax|vat|"
    r"discount|coupon|overage|grandfather(?:ed|ing)?|per[- ]seat|per user|per member|"
    r"usage[- ]based|metered)\b",
    re.I,
)

_ASSUMPTION_PATTERNS: dict[PricingModelAssumptionType, re.Pattern[str]] = {
    "seat_based_pricing": re.compile(
        r"\b(?:seat[- ]based|per[- ]seat|per user|per member|per licensed user|"
        r"seat price|seat pricing|seat fee|seats? (?:cost|priced|billed|charged)|"
        r"licensed users? (?:cost|priced|billed|charged))\b",
        re.I,
    ),
    "usage_based_pricing": re.compile(
        r"\b(?:usage[- ]based pricing|usage[- ]based billing|metered billing|metered usage|"
        r"usage metering|billable usage|billable events?|consumption[- ]based|"
        r"pay as you go|pay[- ]as[- ]you[- ]go|per api call|per transaction|per message)\b",
        re.I,
    ),
    "tiered_plan": re.compile(
        r"\b(?:pricing tiers?|plan tiers?|tiered pricing|free tier|paid tier|pro tier|"
        r"enterprise tier|starter plan|pro plan|business plan|enterprise plan|"
        r"plans? include|plans? start|upgrade to (?:pro|business|enterprise))\b",
        re.I,
    ),
    "trial": re.compile(
        r"\b(?:free trial|trial period|trial pricing|trial converts?|trial conversion|"
        r"trial ends?|trial expires?|evaluation period)\b",
        re.I,
    ),
    "discount": re.compile(
        r"\b(?:discounts?|coupon|promo code|promotion|introductory price|launch price|"
        r"annual discount|volume discount|nonprofit discount|student discount|"
        r"percent off|\d+\s*% off)\b",
        re.I,
    ),
    "overage": re.compile(
        r"\b(?:overage|overages|overage fees?|overage charges?|charged for extra|"
        r"extra usage|above quota|beyond allowance|exceed(?:s|ed|ing)? allowance)\b",
        re.I,
    ),
    "invoice": re.compile(
        r"\b(?:invoice|invoices|invoicing|receipt|receipts|billing history|billing portal|"
        r"billing statement|purchase order|po number)\b",
        re.I,
    ),
    "tax_vat": re.compile(
        r"\b(?:tax|taxes|vat|gst|sales tax|tax id|vat id|tax invoice|tax exempt|"
        r"reverse charge)\b",
        re.I,
    ),
    "grandfathering": re.compile(
        r"\b(?:grandfather(?:ed|ing)?|legacy pricing|legacy plan|keep existing price|"
        r"honou?r existing price|price protection|existing customers keep)\b",
        re.I,
    ),
    "plan_limit": re.compile(
        r"\b(?:(?:plan|tier|pricing) limits?|limits? per plan|quota by plan|plan quota|"
        r"tier quota|included (?:seats?|usage|credits|projects|storage)|"
        r"\d+\s+(?:seats?|users?|projects|credits|gb|api calls|messages)\s+(?:included|per month|/month))\b",
        re.I,
    ),
}
_PLANNING_NOTES: dict[PricingModelAssumptionType, str] = {
    "seat_based_pricing": "Plan seat counting, billing ownership, add/remove timing, and reconciliation behavior.",
    "usage_based_pricing": "Plan usage event capture, aggregation windows, billing attribution, and auditability.",
    "tiered_plan": "Plan tier definitions, upgrade paths, packaging copy, and support impact by plan.",
    "trial": "Plan trial start, expiry, conversion, reminders, and post-trial billing behavior.",
    "discount": "Plan discount eligibility, coupon validation, stacking rules, and renewal behavior.",
    "overage": "Plan allowance tracking, overage calculation, customer messaging, and billing review.",
    "invoice": "Plan invoice generation, billing history access, invoice metadata, and support workflows.",
    "tax_vat": "Plan tax/VAT collection, exemptions, invoice display, and regional compliance handling.",
    "grandfathering": "Plan legacy-price eligibility, migration policy, support handling, and audit trails.",
    "plan_limit": "Plan limit enforcement, packaging alignment, limit messaging, and upgrade prompts.",
}


@dataclass(frozen=True, slots=True)
class SourcePricingModelAssumption:
    """One source-backed pricing or monetization assumption."""

    assumption_type: PricingModelAssumptionType
    audience_or_plan: str | None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field: str = ""
    confidence: PricingModelConfidence = "medium"
    planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "assumption_type": self.assumption_type,
            "audience_or_plan": self.audience_or_plan,
            "evidence": list(self.evidence),
            "source_field": self.source_field,
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourcePricingModelAssumptionsReport:
    """Source-level pricing and monetization assumptions report."""

    source_id: str | None = None
    assumptions: tuple[SourcePricingModelAssumption, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourcePricingModelAssumption, ...]:
        """Compatibility view matching reports that expose rows as records."""
        return self.assumptions

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "assumptions": [assumption.to_dict() for assumption in self.assumptions],
            "summary": dict(self.summary),
            "records": [assumption.to_dict() for assumption in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return pricing model assumptions as plain dictionaries."""
        return [assumption.to_dict() for assumption in self.assumptions]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Pricing Model Assumptions Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        type_counts = self.summary.get("assumption_type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Assumptions found: {self.summary.get('assumption_count', 0)}",
            "- Confidence counts: "
            f"high {confidence_counts.get('high', 0)}, "
            f"medium {confidence_counts.get('medium', 0)}, "
            f"low {confidence_counts.get('low', 0)}",
            "- Assumption type counts: "
            + (", ".join(f"{key} {type_counts[key]}" for key in sorted(type_counts)) or "none"),
        ]
        if not self.assumptions:
            lines.extend(["", "No pricing model assumptions were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Assumptions",
                "",
                "| Type | Audience Or Plan | Confidence | Source Field | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for assumption in self.assumptions:
            lines.append(
                "| "
                f"{assumption.assumption_type} | "
                f"{_markdown_cell(assumption.audience_or_plan or '')} | "
                f"{assumption.confidence} | "
                f"{_markdown_cell(assumption.source_field)} | "
                f"{_markdown_cell('; '.join(assumption.evidence))} | "
                f"{_markdown_cell(assumption.planning_note)} |"
            )
        return "\n".join(lines)


def build_source_pricing_model_assumptions(
    source: Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object,
) -> SourcePricingModelAssumptionsReport:
    """Extract pricing and monetization assumption records from source briefs."""
    brief_payloads = _source_payloads(source)
    assumptions = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda assumption: (
                _optional_text(assumption.source_field) or "",
                _assumption_index(assumption.assumption_type),
                _optional_text(assumption.audience_or_plan) or "",
                _CONFIDENCE_ORDER[assumption.confidence],
                assumption.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourcePricingModelAssumptionsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        assumptions=assumptions,
        summary=_summary(assumptions, len(brief_payloads)),
    )


def generate_source_pricing_model_assumptions(
    source: Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object,
) -> SourcePricingModelAssumptionsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_pricing_model_assumptions(source)


def derive_source_pricing_model_assumptions(
    source: Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object,
) -> SourcePricingModelAssumptionsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_pricing_model_assumptions(source)


def extract_source_pricing_model_assumptions(
    source: Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object,
) -> tuple[SourcePricingModelAssumption, ...]:
    """Return pricing model assumption records from brief-shaped input."""
    return build_source_pricing_model_assumptions(source).assumptions


def source_pricing_model_assumptions_to_dict(
    report: SourcePricingModelAssumptionsReport,
) -> dict[str, Any]:
    """Serialize a pricing model assumptions report to a plain dictionary."""
    return report.to_dict()


source_pricing_model_assumptions_to_dict.__test__ = False


def source_pricing_model_assumptions_to_dicts(
    assumptions: tuple[SourcePricingModelAssumption, ...] | list[SourcePricingModelAssumption],
) -> list[dict[str, Any]]:
    """Serialize pricing model assumption records to dictionaries."""
    return [assumption.to_dict() for assumption in assumptions]


source_pricing_model_assumptions_to_dicts.__test__ = False


def source_pricing_model_assumptions_to_markdown(
    report: SourcePricingModelAssumptionsReport,
) -> str:
    """Render a pricing model assumptions report as Markdown."""
    return report.to_markdown()


source_pricing_model_assumptions_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    assumption_type: PricingModelAssumptionType
    audience_or_plan: str | None
    evidence: str
    source_field: str
    confidence: PricingModelConfidence


def _source_payloads(
    source: Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object,
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief)) or hasattr(
        source, "model_dump"
    ):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(source: Mapping[str, Any] | SourceBrief | str | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        try:
            value = SourceBrief.model_validate(source).model_dump(mode="python")
            payload = dict(value)
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
        return _source_id(payload), payload
    return None, _object_payload(source)


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for source_field, segment in _candidate_segments(payload):
            if not _is_pricing_signal(segment):
                continue
            assumption_types = _assumption_types(segment)
            if not assumption_types:
                continue
            audience_or_plan = _audience_or_plan(segment)
            evidence = _evidence_snippet(source_field, segment)
            confidence = _confidence(segment, assumption_types)
            for assumption_type in assumption_types:
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        assumption_type=assumption_type,
                        audience_or_plan=audience_or_plan,
                        evidence=evidence,
                        source_field=source_field,
                        confidence=confidence,
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourcePricingModelAssumption]:
    grouped: dict[tuple[PricingModelAssumptionType, str | None, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (candidate.assumption_type, candidate.audience_or_plan, candidate.source_field), []
        ).append(candidate)

    assumptions: list[SourcePricingModelAssumption] = []
    for (assumption_type, audience_or_plan, source_field), items in grouped.items():
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        assumptions.append(
            SourcePricingModelAssumption(
                assumption_type=assumption_type,
                audience_or_plan=audience_or_plan,
                evidence=tuple(
                    sorted(
                        _dedupe(item.evidence for item in items), key=lambda item: item.casefold()
                    )
                ),
                source_field=source_field,
                confidence=confidence,
                planning_note=_PLANNING_NOTES[assumption_type],
            )
        )
    return assumptions


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "requirements",
        "constraints",
        "risks",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "billing",
        "pricing",
        "monetization",
        "implementation_notes",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and key not in {
            "id",
            "source_brief_id",
            "source_id",
            "source_project",
            "source_entity_type",
            "created_at",
            "updated_at",
            "source_links",
        }:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            if _any_signal(key_text):
                values.append((child_field, key_text))
            _append_value(values, child_field, child)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        for segment in _segments(text):
            values.append((source_field, segment))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for sentence in _SENTENCE_SPLIT_RE.split(value):
        segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _assumption_types(text: str) -> tuple[PricingModelAssumptionType, ...]:
    return tuple(
        assumption_type
        for assumption_type in _ASSUMPTION_ORDER
        if _ASSUMPTION_PATTERNS[assumption_type].search(text)
    )


def _is_pricing_signal(text: str) -> bool:
    if _MONEY_RE.search(text):
        return True
    if _PRICING_CONTEXT_RE.search(text):
        return True
    if _ENTITLEMENT_ONLY_RE.search(text):
        return False
    return False


def _audience_or_plan(text: str) -> str | None:
    if plan := _PLAN_RE.search(text):
        return _normalize_plan_label(plan.group(0))
    if audience := _AUDIENCE_RE.search(text):
        return _normalize_audience_label(audience.group(0))
    return None


def _normalize_plan_label(value: str) -> str:
    label = _clean_text(value).casefold()
    label = re.sub(r"\s+(?:plan|tier|workspace|account|customer|customers|users?)$", "", label)
    return label.replace("teams", "team")


def _normalize_audience_label(value: str) -> str:
    return _clean_text(value).casefold().replace("teams", "team")


def _confidence(
    text: str, assumption_types: tuple[PricingModelAssumptionType, ...]
) -> PricingModelConfidence:
    if _MONEY_RE.search(text) or _HIGH_CONFIDENCE_RE.search(text):
        return "high"
    if any(
        assumption_type in {"tiered_plan", "plan_limit", "trial"} for assumption_type in assumption_types
    ):
        return "medium"
    return "low"


def _summary(
    assumptions: tuple[SourcePricingModelAssumption, ...], source_count: int
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "assumption_count": len(assumptions),
        "assumption_type_counts": {
            assumption_type: sum(
                1 for assumption in assumptions if assumption.assumption_type == assumption_type
            )
            for assumption_type in _ASSUMPTION_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for assumption in assumptions if assumption.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
    }


def _assumption_index(assumption_type: PricingModelAssumptionType) -> int:
    return _ASSUMPTION_ORDER.index(assumption_type)


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "summary",
        "body",
        "description",
        "requirements",
        "constraints",
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
        "source_links",
        "billing",
        "pricing",
        "monetization",
        "acceptance_criteria",
        "implementation_notes",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _any_signal(text: str) -> bool:
    return _PRICING_CONTEXT_RE.search(text) is not None or any(
        pattern.search(text) for pattern in _ASSUMPTION_PATTERNS.values()
    )


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    result: list[_T] = []
    seen: set[Any] = set()
    for value in values:
        key = value.casefold() if isinstance(value, str) else value
        if not value or key in seen:
            continue
        result.append(value)
        seen.add(key)
    return result


__all__ = [
    "PricingModelAssumptionType",
    "PricingModelConfidence",
    "SourcePricingModelAssumption",
    "SourcePricingModelAssumptionsReport",
    "build_source_pricing_model_assumptions",
    "derive_source_pricing_model_assumptions",
    "extract_source_pricing_model_assumptions",
    "generate_source_pricing_model_assumptions",
    "source_pricing_model_assumptions_to_dict",
    "source_pricing_model_assumptions_to_dicts",
    "source_pricing_model_assumptions_to_markdown",
]
