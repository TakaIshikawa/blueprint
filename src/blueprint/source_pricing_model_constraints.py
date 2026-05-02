"""Extract pricing model constraints from source and implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


PricingModelConstraintType = Literal[
    "pricing",
    "billing",
    "plan_tier",
    "entitlement",
    "quota",
    "discount",
    "invoice",
    "tax",
    "trial",
]
PricingModelConstraintConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_QUESTION_RE = re.compile(r"\?|(?:\b(?:tbd|unknown|unclear|confirm|verify|whether|which|legal review)\b)", re.I)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|requires?|required|needs? to|should|ensure|support|block|limit|only|cannot|unless)\b",
    re.I,
)
_MONEY_OR_LIMIT_RE = re.compile(
    r"(?:[$€£]\s*\d+|\b\d+(?:\.\d+)?\s*(?:%|percent|users?|seats?|credits?|requests?|invoices?|days?|months?|years?)\b)",
    re.I,
)
_CONFIDENCE_ORDER: dict[PricingModelConstraintConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_TYPE_ORDER: tuple[PricingModelConstraintType, ...] = (
    "pricing",
    "billing",
    "plan_tier",
    "entitlement",
    "quota",
    "discount",
    "invoice",
    "tax",
    "trial",
)
_TYPE_PATTERNS: dict[PricingModelConstraintType, re.Pattern[str]] = {
    "pricing": re.compile(
        r"\b(?:pricing|price|paid|free|subscription|usage[- ]based|metered|per[- ]seat|per[- ]user|sku|package)\b",
        re.I,
    ),
    "billing": re.compile(
        r"\b(?:billing|billable|charge|charged|recurring|monthly|annual|yearly|prorat(?:e|ion)|payment method)\b",
        re.I,
    ),
    "plan_tier": re.compile(
        r"\b(?:plan|tier|free tier|starter|basic|pro|premium|enterprise|paid tier|upgrade|downgrade)\b",
        re.I,
    ),
    "entitlement": re.compile(
        r"\b(?:entitlement|entitled|feature gate|gated|license|licensed|seat|seats|add[- ]on|addon|access only)\b",
        re.I,
    ),
    "quota": re.compile(
        r"\b(?:quotas?|limits?|usage caps?|cap|overage|rate limits?|credits|allowance|threshold|max(?:imum)?)\b",
        re.I,
    ),
    "discount": re.compile(r"\b(?:discount|coupon|promo|promotion|markdown|contract price|waive|waiver)\b", re.I),
    "invoice": re.compile(r"\b(?:invoices?|invoicing|receipts?|purchase order|po number|net\s*\d+)\b", re.I),
    "tax": re.compile(r"\b(?:tax|taxes|vat|gst|sales tax|tax exempt|taxable|reverse charge)\b", re.I),
    "trial": re.compile(r"\b(?:trials?|free trial|evaluation|sandbox period|expires?|expiration|grace period)\b", re.I),
}
_PLAN_IMPACTS: dict[PricingModelConstraintType, str] = {
    "pricing": "Confirm the pricing model, SKU mapping, and source of truth before implementation.",
    "billing": "Align billing cadence, charging events, proration, and payment failure behavior with plan logic.",
    "plan_tier": "Map each behavior to the exact plan tiers that should receive or lose access.",
    "entitlement": "Model entitlements as enforceable feature gates with audit-ready plan state.",
    "quota": "Define quota counters, reset cadence, overage behavior, and user-facing limit messaging.",
    "discount": "Clarify discount eligibility, stacking rules, duration, and contract override handling.",
    "invoice": "Confirm invoice fields, timing, purchase-order requirements, and customer-visible receipts.",
    "tax": "Confirm tax jurisdiction, exemption handling, and whether displayed prices include taxes.",
    "trial": "Define trial eligibility, duration, conversion, expiration, and grace-period behavior.",
}
_EXPLICIT_FIELDS = (
    "pricing_model_constraints",
    "pricing_constraints",
    "billing_constraints",
    "entitlement_constraints",
    "quota_constraints",
    "plan_constraints",
    "pricing",
    "billing",
    "plans",
    "entitlements",
    "quotas",
    "discounts",
    "invoices",
    "taxes",
    "trials",
)


@dataclass(frozen=True, slots=True)
class SourcePricingModelConstraint:
    """One source-backed pricing model constraint."""

    constraint_type: PricingModelConstraintType
    constraint: str
    confidence: PricingModelConstraintConfidence
    source_field: str
    evidence: str
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)
    unresolved_questions: tuple[str, ...] = field(default_factory=tuple)
    explicit: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "constraint_type": self.constraint_type,
            "constraint": self.constraint,
            "confidence": self.confidence,
            "source_field": self.source_field,
            "evidence": self.evidence,
            "suggested_plan_impacts": list(self.suggested_plan_impacts),
            "unresolved_questions": list(self.unresolved_questions),
            "explicit": self.explicit,
        }


@dataclass(frozen=True, slots=True)
class SourcePricingModelConstraintsReport:
    """Brief-level pricing model constraints report."""

    brief_id: str | None = None
    constraints: tuple[SourcePricingModelConstraint, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "constraints": [constraint.to_dict() for constraint in self.constraints],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return pricing constraints as plain dictionaries."""
        return [constraint.to_dict() for constraint in self.constraints]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Pricing Model Constraints Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        confidence_counts = self.summary.get("confidence_counts", {})
        type_counts = self.summary.get("constraint_counts_by_type", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Constraints found: {self.summary.get('constraint_count', 0)}",
            "- Confidence counts: "
            f"high {confidence_counts.get('high', 0)}, "
            f"medium {confidence_counts.get('medium', 0)}, "
            f"low {confidence_counts.get('low', 0)}",
            f"- Explicit constraints: {self.summary.get('explicit_constraint_count', 0)}",
            f"- Suggested plan impacts: {self.summary.get('suggested_plan_impact_count', 0)}",
            f"- Unresolved questions: {self.summary.get('unresolved_question_count', 0)}",
            "- Constraint type counts: "
            + (", ".join(f"{key} {type_counts[key]}" for key in sorted(type_counts)) or "none"),
        ]
        if not self.constraints:
            lines.extend(["", "No pricing model constraints were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Constraints",
                "",
                "| Type | Constraint | Confidence | Source | Evidence | Suggested Plan Impacts | Questions |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for constraint in self.constraints:
            lines.append(
                "| "
                f"{constraint.constraint_type} | "
                f"{_markdown_cell(constraint.constraint)} | "
                f"{constraint.confidence} | "
                f"{_markdown_cell(constraint.source_field)} | "
                f"{_markdown_cell(constraint.evidence)} | "
                f"{_markdown_cell('; '.join(constraint.suggested_plan_impacts) or 'none')} | "
                f"{_markdown_cell('; '.join(constraint.unresolved_questions) or 'none')} |"
            )
        return "\n".join(lines)


def build_source_pricing_model_constraints(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourcePricingModelConstraintsReport:
    """Extract pricing model constraints from SourceBrief-like and implementation brief inputs."""
    brief_id, payload = _source_payload(source)
    constraints = tuple(
        sorted(
            _dedupe_constraints([*_explicit_constraints(payload), *_inferred_constraints(payload)]),
            key=lambda constraint: (
                _CONFIDENCE_ORDER[constraint.confidence],
                _TYPE_ORDER.index(constraint.constraint_type),
                constraint.source_field,
                constraint.constraint.casefold(),
                constraint.evidence.casefold(),
            ),
        )
    )
    confidence_counts = {
        confidence: sum(1 for constraint in constraints if constraint.confidence == confidence)
        for confidence in _CONFIDENCE_ORDER
    }
    type_counts = {
        constraint_type: sum(1 for constraint in constraints if constraint.constraint_type == constraint_type)
        for constraint_type in sorted({constraint.constraint_type for constraint in constraints})
    }
    suggested_plan_impacts = tuple(
        _dedupe(impact for constraint in constraints for impact in constraint.suggested_plan_impacts)
    )
    unresolved_questions = tuple(
        _dedupe(question for constraint in constraints for question in constraint.unresolved_questions)
    )
    return SourcePricingModelConstraintsReport(
        brief_id=brief_id,
        constraints=constraints,
        summary={
            "constraint_count": len(constraints),
            "constraint_counts_by_type": type_counts,
            "confidence_counts": confidence_counts,
            "explicit_constraint_count": sum(1 for constraint in constraints if constraint.explicit),
            "suggested_plan_impact_count": len(suggested_plan_impacts),
            "unresolved_question_count": len(unresolved_questions),
            "suggested_plan_impacts": list(suggested_plan_impacts),
            "unresolved_questions": list(unresolved_questions),
        },
    )


def source_pricing_model_constraints_to_dict(report: SourcePricingModelConstraintsReport) -> dict[str, Any]:
    """Serialize a source pricing model constraints report to a plain dictionary."""
    return report.to_dict()


source_pricing_model_constraints_to_dict.__test__ = False


def source_pricing_model_constraints_to_markdown(report: SourcePricingModelConstraintsReport) -> str:
    """Render a source pricing model constraints report as Markdown."""
    return report.to_markdown()


source_pricing_model_constraints_to_markdown.__test__ = False


def _explicit_constraints(payload: Mapping[str, Any]) -> list[SourcePricingModelConstraint]:
    constraints: list[SourcePricingModelConstraint] = []
    source_payload = payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    for metadata_field, metadata in _metadata_sources(payload):
        for field_name in _EXPLICIT_FIELDS:
            for index, item in enumerate(_list_items(metadata.get(field_name))):
                constraint = _explicit_constraint(item, f"{metadata_field}.{field_name}[{index}]", field_name)
                if constraint:
                    constraints.append(constraint)
    for field_name in _EXPLICIT_FIELDS:
        if field_name in payload:
            for index, item in enumerate(_list_items(payload.get(field_name))):
                constraint = _explicit_constraint(item, f"{field_name}[{index}]", field_name)
                if constraint:
                    constraints.append(constraint)
        if field_name in source_payload:
            for index, item in enumerate(_list_items(source_payload.get(field_name))):
                constraint = _explicit_constraint(item, f"source_payload.{field_name}[{index}]", field_name)
                if constraint:
                    constraints.append(constraint)
    return constraints


def _explicit_constraint(item: Any, source_field: str, field_name: str) -> SourcePricingModelConstraint | None:
    if isinstance(item, Mapping):
        evidence = _item_text(item) or _text(item)
        constraint_type = _constraint_type(
            _optional_text(item.get("type") or item.get("constraint_type") or item.get("category"))
            or f"{field_name} {evidence}"
        )
        constraint_text = _optional_text(item.get("constraint") or item.get("requirement") or item.get("name")) or evidence
        impacts = tuple(_dedupe(_strings(item.get("suggested_plan_impacts") or item.get("plan_impacts"))))
        questions = tuple(_dedupe(_strings(item.get("unresolved_questions") or item.get("questions"))))
        confidence = _confidence_value(item.get("confidence"), default="high")
    else:
        evidence = _optional_text(item) or ""
        constraint_type = _constraint_type(f"{field_name} {evidence}")
        constraint_text = evidence
        impacts = ()
        questions = ()
        confidence = "high"
    if not evidence:
        return None
    return SourcePricingModelConstraint(
        constraint_type=constraint_type,
        constraint=_clip(constraint_text),
        confidence=confidence,
        source_field=source_field,
        evidence=_clip(evidence),
        suggested_plan_impacts=impacts or (_PLAN_IMPACTS[constraint_type],),
        unresolved_questions=questions or _unresolved_questions(evidence, constraint_type),
        explicit=True,
    )


def _inferred_constraints(payload: Mapping[str, Any]) -> list[SourcePricingModelConstraint]:
    constraints: list[SourcePricingModelConstraint] = []
    for source_field, text in _candidate_texts(payload):
        matches = [constraint_type for constraint_type, pattern in _TYPE_PATTERNS.items() if pattern.search(text)]
        for constraint_type in matches:
            constraints.append(
                SourcePricingModelConstraint(
                    constraint_type=constraint_type,
                    constraint=_constraint_text(text, constraint_type),
                    confidence=_inferred_confidence(text, source_field),
                    source_field=source_field,
                    evidence=_clip(text),
                    suggested_plan_impacts=(_PLAN_IMPACTS[constraint_type],),
                    unresolved_questions=_unresolved_questions(text, constraint_type),
                    explicit=False,
                )
            )
    return constraints


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        return _optional_text(source.id), source.model_dump(mode="python")
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _optional_text(payload.get("id")), payload
    if isinstance(source, Mapping):
        payload = _validated_payload(source)
        return _optional_text(payload.get("id")), payload
    if not isinstance(source, (str, bytes)):
        payload = _object_payload(source)
        return _optional_text(payload.get("id")), payload
    return None, {}


def _validated_payload(source: Mapping[str, Any]) -> dict[str, Any]:
    for model in (SourceBrief, ImplementationBrief):
        try:
            return dict(model.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            continue
    return dict(source)


def _candidate_texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    source_payload = payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "problem_statement",
        "mvp_goal",
        "workflow_context",
        "architecture_notes",
        "data_requirements",
        "validation_plan",
    ):
        if text := _optional_text(payload.get(field_name)):
            texts.append((field_name, text))
        if text := _optional_text(source_payload.get(field_name)):
            texts.append((f"source_payload.{field_name}", text))
    for field_name in (
        "goals",
        "constraints",
        "implementation_constraints",
        "acceptance_criteria",
        "definition_of_done",
        "requirements",
        "scope",
        "non_goals",
        "assumptions",
        "risks",
        "open_questions",
        "questions",
        "integration_points",
    ):
        texts.extend(_field_texts(payload.get(field_name), field_name))
        texts.extend(_field_texts(source_payload.get(field_name), f"source_payload.{field_name}"))
    for field, text in _metadata_texts(payload.get("metadata")):
        texts.append((field, text))
    for field, text in _metadata_texts(payload.get("brief_metadata"), "brief_metadata"):
        texts.append((field, text))
    for field, text in _metadata_texts(source_payload.get("metadata"), "source_payload.metadata"):
        texts.append((field, text))
    return [(field, text) for field, text in texts if text]


def _field_texts(value: Any, field_name: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            source_field = f"{field_name}.{key}"
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_field_texts(child, source_field))
            elif text := _optional_text(child):
                texts.append((source_field, text))
        return texts
    return [(f"{field_name}[{index}]", text) for index, text in enumerate(_strings(value))]


def _metadata_sources(payload: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    sources = []
    source_payload = payload.get("source_payload") if isinstance(payload.get("source_payload"), Mapping) else {}
    for field_name, value in (
        ("metadata", payload.get("metadata")),
        ("brief_metadata", payload.get("brief_metadata")),
        ("source_payload.metadata", source_payload.get("metadata")),
    ):
        if isinstance(value, Mapping):
            sources.append((field_name, value))
    return sources


def _metadata_texts(value: Any, prefix: str = "metadata") -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            field = f"{prefix}.{key}"
            child = value[key]
            key_text = str(key).replace("_", " ")
            if str(key) in _EXPLICIT_FIELDS:
                continue
            if isinstance(child, (Mapping, list, tuple, set)):
                if _any_signal(key_text):
                    texts.append((field, str(key)))
                texts.extend(_metadata_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
                if _any_signal(key_text):
                    texts.append((field, f"{key_text}: {text}"))
            elif _any_signal(key_text):
                texts.append((field, str(key)))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            texts.extend(_metadata_texts(item, f"{prefix}[{index}]"))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in _TYPE_PATTERNS.values())


def _constraint_type(value: str) -> PricingModelConstraintType:
    field_slug = _slug(value.split(" ", 1)[0])
    explicit_field_types = {
        "billing_constraints": "billing",
        "entitlement_constraints": "entitlement",
        "quota_constraints": "quota",
        "plan_constraints": "plan_tier",
        "billing": "billing",
        "plans": "plan_tier",
        "entitlements": "entitlement",
        "quotas": "quota",
        "discounts": "discount",
        "invoices": "invoice",
        "taxes": "tax",
        "trials": "trial",
    }
    if field_slug in explicit_field_types:
        return explicit_field_types[field_slug]  # type: ignore[return-value]
    for constraint_type, pattern in _TYPE_PATTERNS.items():
        if pattern.search(value):
            return constraint_type
    normalized = _slug(value)
    if normalized in _TYPE_ORDER:
        return normalized  # type: ignore[return-value]
    return "pricing"


def _constraint_text(text: str, constraint_type: PricingModelConstraintType) -> str:
    cleaned = _clip(text)
    if _REQUIREMENT_RE.search(cleaned):
        return cleaned
    return f"Clarify {constraint_type.replace('_', ' ')} constraint: {cleaned}"


def _inferred_confidence(text: str, source_field: str) -> PricingModelConstraintConfidence:
    if _REQUIREMENT_RE.search(text) and (_MONEY_OR_LIMIT_RE.search(text) or "constraint" in source_field):
        return "high"
    if _REQUIREMENT_RE.search(text) or _MONEY_OR_LIMIT_RE.search(text):
        return "medium"
    return "low"


def _unresolved_questions(text: str, constraint_type: PricingModelConstraintType) -> tuple[str, ...]:
    questions = []
    if _QUESTION_RE.search(text):
        questions.append(f"Resolve source question for {constraint_type.replace('_', ' ')}: {_clip(text)}")
    if constraint_type == "tax" and not re.search(r"\b(?:us|eu|uk|vat|gst|sales tax|jurisdiction|country|state)\b", text, re.I):
        questions.append("Confirm tax jurisdictions, exemption rules, and display requirements.")
    if constraint_type == "trial" and not re.search(r"\b\d+\s*(?:days?|weeks?|months?)\b", text, re.I):
        questions.append("Confirm trial duration and conversion behavior.")
    return tuple(_dedupe(questions))


def _dedupe_constraints(
    constraints: Iterable[SourcePricingModelConstraint],
) -> list[SourcePricingModelConstraint]:
    deduped: list[SourcePricingModelConstraint] = []
    seen: set[tuple[str, str]] = set()
    for constraint in constraints:
        key = (constraint.constraint_type, constraint.evidence.casefold())
        if key in seen:
            continue
        deduped.append(constraint)
        seen.add(key)
    return deduped


def _item_text(item: Any) -> str:
    if isinstance(item, Mapping):
        for key in ("text", "evidence", "description", "summary", "requirement", "constraint", "name", "value"):
            if text := _optional_text(item.get(key)):
                return text
        return ""
    return _optional_text(item) or ""


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "title",
        "domain",
        "summary",
        "body",
        "description",
        "source_project",
        "source_entity_type",
        "source_id",
        "source_payload",
        "source_links",
        "source_brief_id",
        "target_user",
        "buyer",
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "product_surface",
        "goals",
        "constraints",
        "implementation_constraints",
        "acceptance_criteria",
        "definition_of_done",
        "requirements",
        "scope",
        "non_goals",
        "assumptions",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "validation_plan",
        "open_questions",
        "questions",
        "metadata",
        "brief_metadata",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _list_items(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        return [value]
    if isinstance(value, (list, tuple, set)):
        return sorted(value, key=lambda item: str(item)) if isinstance(value, set) else list(value)
    return [value]


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


def _confidence_value(value: Any, *, default: PricingModelConstraintConfidence) -> PricingModelConstraintConfidence:
    text = _text(value).casefold()
    return text if text in _CONFIDENCE_ORDER else default  # type: ignore[return-value]


def _slug(value: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value.casefold())).strip("_") or "pricing"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _clip(value: str) -> str:
    text = _text(value)
    return f"{text[:177].rstrip()}..." if len(text) > 180 else text


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
    "PricingModelConstraintConfidence",
    "PricingModelConstraintType",
    "SourcePricingModelConstraint",
    "SourcePricingModelConstraintsReport",
    "build_source_pricing_model_constraints",
    "source_pricing_model_constraints_to_dict",
    "source_pricing_model_constraints_to_markdown",
]
