"""Extract billing proration and subscription-change requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


BillingProrationCategory = Literal[
    "plan_upgrade_proration",
    "downgrade_credit",
    "seat_count_change",
    "billing_cycle_alignment",
    "trial_conversion",
    "refund_policy",
    "invoice_adjustment",
    "tax_interaction",
]
BillingProrationConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[BillingProrationCategory, ...] = (
    "plan_upgrade_proration",
    "downgrade_credit",
    "seat_count_change",
    "billing_cycle_alignment",
    "trial_conversion",
    "refund_policy",
    "invoice_adjustment",
    "tax_interaction",
)
_CONFIDENCE_ORDER: dict[BillingProrationConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"acceptance|done when|before launch|cannot ship|support|calculate|apply|document|"
    r"define|defines|defined)\b",
    re.I,
)
_BILLING_CONTEXT_RE = re.compile(
    r"\b(?:billing|subscription|subscrib(?:e|er|ers|ed|ing)|plan|pricing|charge|"
    r"invoice|credit|refund|tax|vat|sales tax|prorat(?:e|ed|ion|ing)|billing cycle|"
    r"renewal|seat|license|trial|checkout|accounting|ledger|revenue)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:billing|subscription|prorat(?:e|ed|ion|ing)|invoice|"
    r"refund|credit|tax|seat|plan change).*?\b(?:in scope|required|requirements?|needed|changes?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:billing|subscription|prorat|plan|downgrade|upgrade|seat|license|credit|refund|"
    r"invoice|tax|vat|trial|cycle|renewal|accounting|revenue|requirements?|acceptance|"
    r"constraints?)",
    re.I,
)
_CATEGORY_PATTERNS: dict[BillingProrationCategory, re.Pattern[str]] = {
    "plan_upgrade_proration": re.compile(
        r"\b(?:(?:upgrade|plan upgrade|upgrade plan|upgrade tier|higher tier|move to a paid tier|"
        r"change to premium).{0,90}(?:prorat(?:e|ed|ion|ing)|partial[- ]?period|"
        r"remaining billing period|mid[- ]?cycle|immediate charge|charge the difference)|"
        r"(?:prorat(?:e|ed|ion|ing)|partial[- ]?period|remaining billing period|mid[- ]?cycle)"
        r".{0,90}(?:upgrade|higher tier|premium plan))\b",
        re.I,
    ),
    "downgrade_credit": re.compile(
        r"\b(?:(?:downgrade|lower tier|reduce plan|cancel add[- ]?on).{0,90}"
        r"(?:credit|account credit|service credit|credit balance|carry[- ]?forward|"
        r"unused time|unused value|partial credit)|"
        r"(?:credit|account credit|credit balance|carry[- ]?forward|unused time|unused value)"
        r".{0,90}(?:downgrade|lower tier|reduce plan))\b",
        re.I,
    ),
    "seat_count_change": re.compile(
        r"\b(?:(?:seat|seats|user licenses?|licenses?|license count|member count|quantity)"
        r".{0,90}(?:add|added|remove|removed|change|increase|decrease|true[- ]?up|true[- ]?down|"
        r"prorat(?:e|ed|ion|ing)|charge|credit)|"
        r"(?:add|remove|increase|decrease|change).{0,50}(?:seat|seats|user licenses?|licenses?))\b",
        re.I,
    ),
    "billing_cycle_alignment": re.compile(
        r"\b(?:billing cycle alignment|align(?:ed|ment)? to (?:the )?billing cycle|"
        r"co[- ]?term(?:ination|ed)?|coterm(?:ination|ed)?|anniversary date|renewal date|"
        r"billing anchor|cycle anchor|calendar billing|same billing date|next billing cycle|"
        r"mid[- ]?cycle.{0,60}(?:align|cycle|renewal|anniversary))\b",
        re.I,
    ),
    "trial_conversion": re.compile(
        r"\b(?:(?:trial|free trial|trialing|trial period).{0,90}(?:convert|conversion|"
        r"paid|charge|subscription|billing starts?|first invoice|prorat(?:e|ed|ion|ing))|"
        r"(?:convert|conversion).{0,60}(?:trial|free trial).{0,60}(?:paid|subscription|billing))\b",
        re.I,
    ),
    "refund_policy": re.compile(
        r"\b(?:refund(?:s|ed|ing)?|refund policy|refundable|non[- ]?refundable|money back|"
        r"partial refund|refund window|refund eligibility|refund calculation|refund rules?)\b",
        re.I,
    ),
    "invoice_adjustment": re.compile(
        r"\b(?:invoice adjustment|adjust(?:ed|ment|ments)? invoice|invoice correction|"
        r"corrective invoice|credit memo|debit memo|invoice line adjustment|adjust invoice lines?|"
        r"amend invoice|void and reissue invoice|invoice true[- ]?up|invoice true[- ]?down)\b",
        re.I,
    ),
    "tax_interaction": re.compile(
        r"\b(?:(?:tax|taxes|sales tax|vat|gst|hst|tax jurisdiction|tax rate|tax calculation|"
        r"tax inclusive|tax exclusive).{0,90}(?:prorat(?:e|ed|ion|ing)|credit|refund|invoice|"
        r"plan change|seat change|billing cycle)|"
        r"(?:prorat(?:e|ed|ion|ing)|credit|refund|invoice|plan change|seat change).{0,90}"
        r"(?:tax|taxes|sales tax|vat|gst|hst))\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[BillingProrationCategory, str] = {
    "plan_upgrade_proration": "billing",
    "downgrade_credit": "billing",
    "seat_count_change": "billing",
    "billing_cycle_alignment": "billing",
    "trial_conversion": "product",
    "refund_policy": "support",
    "invoice_adjustment": "finance",
    "tax_interaction": "tax",
}
_PLANNING_NOTES: dict[BillingProrationCategory, str] = {
    "plan_upgrade_proration": "Define how mid-cycle upgrades calculate immediate charges and remaining-period proration.",
    "downgrade_credit": "Define how downgrades create, carry forward, or suppress credits for unused value.",
    "seat_count_change": "Define how added and removed seats affect charges, credits, and invoice quantities.",
    "billing_cycle_alignment": "Define how plan changes align to billing anchors, renewal dates, and co-termed cycles.",
    "trial_conversion": "Define when trial conversion starts billing and whether partial periods are prorated.",
    "refund_policy": "Define refund eligibility, refund windows, and partial-period refund calculations.",
    "invoice_adjustment": "Define when invoice corrections, credit memos, debit memos, or line adjustments are issued.",
    "tax_interaction": "Define how taxes are recalculated for prorations, credits, refunds, and invoice adjustments.",
}
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "problem",
    "problem_statement",
    "goal",
    "goals",
    "mvp_goal",
    "context",
    "workflow_context",
    "requirements",
    "constraints",
    "scope",
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
    "files",
    "file_paths",
    "paths",
)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
    "generation_model",
    "generation_tokens",
    "generation_prompt",
}


@dataclass(frozen=True, slots=True)
class SourceBillingProrationRequirement:
    """One source-backed billing proration or subscription-change requirement."""

    source_brief_id: str | None
    category: BillingProrationCategory
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: BillingProrationConfidence = "medium"
    owner_suggestion: str | None = None
    planning_notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "owner_suggestion": self.owner_suggestion,
            "planning_notes": list(self.planning_notes),
        }


@dataclass(frozen=True, slots=True)
class SourceBillingProrationRequirementsReport:
    """Source-level billing proration requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceBillingProrationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceBillingProrationRequirement, ...]:
        """Compatibility view matching extractors that name findings records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [requirement.to_dict() for requirement in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return billing proration requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Billing Proration Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Category counts: "
            + ", ".join(
                f"{category} {category_counts.get(category, 0)}"
                for category in _CATEGORY_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(
                ["", "No billing proration requirements were found in the source brief."]
            )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Confidence | Owner | Planning Notes | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.category} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.owner_suggestion or '')} | "
                f"{_markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_billing_proration_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceBillingProrationRequirementsReport:
    """Extract billing proration requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _category_index(requirement.category),
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceBillingProrationRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_billing_proration_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceBillingProrationRequirementsReport:
    """Compatibility alias for building a billing proration requirements report."""
    return build_source_billing_proration_requirements(source)


def generate_source_billing_proration_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceBillingProrationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_billing_proration_requirements(source)


def derive_source_billing_proration_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceBillingProrationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_billing_proration_requirements(source)


def summarize_source_billing_proration_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceBillingProrationRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted billing proration requirements."""
    if isinstance(source_or_result, SourceBillingProrationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_billing_proration_requirements(source_or_result).summary


def source_billing_proration_requirements_to_dict(
    report: SourceBillingProrationRequirementsReport,
) -> dict[str, Any]:
    """Serialize a billing proration requirements report to a plain dictionary."""
    return report.to_dict()


source_billing_proration_requirements_to_dict.__test__ = False


def source_billing_proration_requirements_to_dicts(
    requirements: (
        tuple[SourceBillingProrationRequirement, ...]
        | list[SourceBillingProrationRequirement]
        | SourceBillingProrationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize billing proration requirement records to dictionaries."""
    if isinstance(requirements, SourceBillingProrationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_billing_proration_requirements_to_dicts.__test__ = False


def source_billing_proration_requirements_to_markdown(
    report: SourceBillingProrationRequirementsReport,
) -> str:
    """Render a billing proration requirements report as Markdown."""
    return report.to_markdown()


source_billing_proration_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: BillingProrationCategory
    evidence: str
    confidence: BillingProrationConfidence


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(
        source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)
    ) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                value = model.model_validate(source).model_dump(mode="python")
                payload = dict(value)
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
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
            if _NEGATED_SCOPE_RE.search(segment):
                continue
            categories = _categories(segment, source_field)
            if not categories:
                continue
            evidence = _evidence_snippet(source_field, segment)
            for category in categories:
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        evidence=evidence,
                        confidence=_confidence(category, segment, source_field),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceBillingProrationRequirement]:
    grouped: dict[tuple[str | None, BillingProrationCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.category), []).append(candidate)

    requirements: list[SourceBillingProrationRequirement] = []
    for (source_brief_id, category), items in grouped.items():
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        requirements.append(
            SourceBillingProrationRequirement(
                source_brief_id=source_brief_id,
                category=category,
                evidence=tuple(
                    sorted(
                        _dedupe(item.evidence for item in items),
                        key=lambda item: item.casefold(),
                    )
                )[:5],
                confidence=confidence,
                owner_suggestion=_OWNER_SUGGESTIONS[category],
                planning_notes=(_PLANNING_NOTES[category],),
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            if _any_signal(key_text) and not isinstance(child, (Mapping, list, tuple, set)):
                if text := _optional_text(child):
                    values.append((child_field, _clean_text(f"{key_text}: {text}")))
                continue
            _append_value(values, child_field, child)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        values.extend((source_field, segment) for segment in _segments(text))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for line in value.splitlines() or [value]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for sentence in parts:
            segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _categories(text: str, source_field: str) -> tuple[BillingProrationCategory, ...]:
    searchable = _searchable_text(source_field, text)
    categories = [
        category
        for category in _CATEGORY_ORDER
        if _CATEGORY_PATTERNS[category].search(searchable)
    ]
    if source_field == "title" and categories and not _REQUIRED_RE.search(text):
        return ()
    if categories and not (
        _BILLING_CONTEXT_RE.search(text)
        or _STRUCTURED_FIELD_RE.search(_field_words(source_field))
        or _REQUIRED_RE.search(text)
    ):
        return ()
    return tuple(_dedupe(categories))


def _confidence(
    category: BillingProrationCategory, text: str, source_field: str
) -> BillingProrationConfidence:
    field_text = source_field.replace("-", "_").casefold()
    if _REQUIRED_RE.search(text) or any(
        marker in field_text
        for marker in (
            "requirements",
            "acceptance_criteria",
            "success_criteria",
            "definition_of_done",
            "constraints",
            "scope",
            "billing_rules",
            "billing_requirements",
            "proration_rules",
            "subscription_rules",
        )
    ):
        return "high"
    if _CATEGORY_PATTERNS[category].search(_field_words(source_field)):
        return "high"
    if _BILLING_CONTEXT_RE.search(text) or _STRUCTURED_FIELD_RE.search(_field_words(source_field)):
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceBillingProrationRequirement, ...], source_count: int
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "categories": [
            category
            for category in _CATEGORY_ORDER
            if any(requirement.category == category for requirement in requirements)
        ],
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "target_user",
        "buyer",
        "summary",
        "body",
        "description",
        "problem",
        "problem_statement",
        "goal",
        "goals",
        "mvp_goal",
        "context",
        "workflow_context",
        "product_surface",
        "requirements",
        "constraints",
        "scope",
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
        "files",
        "file_paths",
        "paths",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _any_signal(text: str) -> bool:
    return _BILLING_CONTEXT_RE.search(text) is not None or any(
        pattern.search(text) for pattern in _CATEGORY_PATTERNS.values()
    )


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    text = _CHECKBOX_RE.sub("", _BULLET_RE.sub("", value.strip()))
    return _SPACE_RE.sub(" ", text).strip()


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _searchable_text(source_field: str, text: str) -> str:
    value = f"{_field_words(source_field)} {text}"
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    return value.replace("/", " ").replace("_", " ").replace("-", " ")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _category_index(category: BillingProrationCategory) -> int:
    return _CATEGORY_ORDER.index(category)


def _dedupe(values: Iterable[Any]) -> list[Any]:
    seen: set[str] = set()
    result: list[Any] = []
    for value in values:
        key = _dedupe_key(value)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _dedupe_key(value: Any) -> str:
    return _clean_text(str(value)).casefold() if value is not None else ""


__all__ = [
    "BillingProrationCategory",
    "BillingProrationConfidence",
    "SourceBillingProrationRequirement",
    "SourceBillingProrationRequirementsReport",
    "build_source_billing_proration_requirements",
    "derive_source_billing_proration_requirements",
    "extract_source_billing_proration_requirements",
    "generate_source_billing_proration_requirements",
    "source_billing_proration_requirements_to_dict",
    "source_billing_proration_requirements_to_dicts",
    "source_billing_proration_requirements_to_markdown",
    "summarize_source_billing_proration_requirements",
]
