"""Extract source-level billing proration requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

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
_T = TypeVar("_T")

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
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_BILLING_CONTEXT_RE = re.compile(
    r"\b(?:billing|subscription|subscriptions?|plan|plans?|pricing|invoice|invoices?|"
    r"charge|charges?|charged|credit|credits?|refund|refunds?|tax|vat|sales tax|"
    r"trial|renewal|billing cycle|cycle|seat|seats?|quantity|account balance)\b",
    re.I,
)
_PRORATION_CONTEXT_RE = re.compile(
    r"\b(?:prorat(?:e|ed|es|ion)|pro[- ]?rat(?:e|ed|es|ion)|mid[- ]cycle|"
    r"remaining billing period|unused time|unused value|partial period|co[- ]?term|"
    r"renewal date|upgrade|downgrade|plan change|subscription change|seat count|"
    r"added seats?|removed seats?|trial conversion|convert(?:s|ed)? to paid|"
    r"first paid subscription|credit memo|debit memo|invoice adjustment|line item|"
    r"partial refund|cancelled subscriptions?|canceled subscriptions?)\b",
    re.I,
)
_AMBIGUOUS_BILLING_CHANGE_RE = re.compile(
    r"\b(?:billing|subscription|plan|pricing)\s+(?:change|changes|update|updates|"
    r"migration|migrations|adjustment|adjustments)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:billing|subscription|pricing|plan|upgrade|downgrade|seat|quantity|cycle|"
    r"renewal|trial|refund|credit|invoice|adjustment|tax|vat|prorat|accounting|"
    r"requirements?|acceptance|criteria|definition[-_ ]?of[-_ ]?done|metadata|"
    r"source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|provide|define|document|calculate|recalculate|validat(?:e|es|ed|ing)|charge|credit|"
    r"refund|issue|create|apply|validate|done when|acceptance|cannot ship)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:billing|subscription|plan|pricing|invoice|"
    r"prorat(?:e|ed|es|ion)|credit|refund)\b.{0,80}\b(?:in scope|required|"
    r"requirements?|needed|changes?)\b",
    re.I,
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

_CATEGORY_PATTERNS: dict[BillingProrationCategory, re.Pattern[str]] = {
    "plan_upgrade_proration": re.compile(
        r"\b(?:(?:plan|subscription)?\s*upgrades?.{0,80}\b(?:prorat|remaining billing period|"
        r"charge(?:s|d)? the difference|difference immediately|mid[- ]cycle)|"
        r"upgrade proration|prorat(?:e|ed|es|ion).{0,80}\bupgrade)\b",
        re.I,
    ),
    "downgrade_credit": re.compile(
        r"\b(?:downgrades?.{0,100}\b(?:credit|unused|carry forward|account balance)|"
        r"downgrade credits?|credit(?:s)?.{0,80}\b(?:downgrade|unused subscription value|unused time))\b",
        re.I,
    ),
    "seat_count_change": re.compile(
        r"\b(?:seat count changes?|added seats?|removed seats?|seat quantity|quantity changes?|"
        r"per[- ]seat|seats?.{0,80}\b(?:prorat|charge|credit|quantity))\b",
        re.I,
    ),
    "billing_cycle_alignment": re.compile(
        r"\b(?:billing cycle alignment|billing cycle|co[- ]?term|coterm|co[- ]?terminal|"
        r"renewal date|align(?:s|ed|ment)? to (?:the )?renewal|cycle alignment)\b",
        re.I,
    ),
    "trial_conversion": re.compile(
        r"\b(?:trial conversion|free trial conversion|convert(?:s|ed|ing)? to paid|"
        r"trial.{0,80}\b(?:paid subscription|first invoice|billing starts?|starts billing|"
        r"subscription invoice)|first paid subscription invoice)\b",
        re.I,
    ),
    "refund_policy": re.compile(
        r"\b(?:refund policy|partial refunds?|refund window|refund eligibility|"
        r"non[- ]refundable|cancelled subscriptions?|canceled subscriptions?|refunds?.{0,80}\bpolicy)\b",
        re.I,
    ),
    "invoice_adjustment": re.compile(
        r"\b(?:invoice adjustments?|invoice line(?: item)? corrections?|line item clarity|"
        r"invoice line items?|credit memos?|debit memos?|invoice corrections?|"
        r"invoice adjustment rules?)\b",
        re.I,
    ),
    "tax_interaction": re.compile(
        r"\b(?:(?:sales tax|vat|tax(?:es)?|tax calculation).{0,100}\b(?:prorat\w*|credit\w*|"
        r"invoice adjustment\w*|recalculat\w*)|(?:prorat\w*|credit\w*|invoice adjustment\w*).{0,100}\b"
        r"(?:sales tax|vat|tax(?:es)?|tax calculation)|tax interaction)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[BillingProrationCategory, str] = {
    "plan_upgrade_proration": "billing_engineering",
    "downgrade_credit": "finance",
    "seat_count_change": "billing_engineering",
    "billing_cycle_alignment": "billing_engineering",
    "trial_conversion": "product",
    "refund_policy": "support",
    "invoice_adjustment": "finance",
    "tax_interaction": "finance",
}
_PLANNING_NOTES_BY_CATEGORY: dict[BillingProrationCategory, tuple[str, ...]] = {
    "plan_upgrade_proration": (
        "Define upgrade proration timing, immediate charge behavior, and customer-visible explanation.",
    ),
    "downgrade_credit": (
        "Document downgrade credit calculation, account balance handling, and customer-visible explanation.",
    ),
    "seat_count_change": (
        "Specify added and removed seat proration, quantity boundaries, and invoice line item presentation.",
    ),
    "billing_cycle_alignment": (
        "Confirm renewal-date alignment, co-terming rules, and mid-cycle effective dates.",
    ),
    "trial_conversion": (
        "Define trial conversion timing, first paid invoice behavior, and subscription start dates.",
    ),
    "refund_policy": (
        "Capture refund eligibility, refund window, non-refundable items, and support handling.",
    ),
    "invoice_adjustment": (
        "Plan invoice adjustment, debit memo, credit memo, and line item clarity requirements.",
    ),
    "tax_interaction": (
        "Validate tax recalculation for prorated charges, credits, refunds, and invoice adjustments.",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceBillingProrationRequirement:
    """One source-backed billing proration requirement."""

    source_brief_id: str | None
    category: BillingProrationCategory
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: BillingProrationConfidence = "medium"
    owner_suggestion: str = ""
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
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
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
            lines.extend(["", "No billing proration requirements were found in the source brief."])
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
                f"{_markdown_cell(requirement.owner_suggestion)} | "
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
    """Extract source-level billing proration requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
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
class _Segment:
    source_field: str
    text: str
    section_context: bool


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
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_id(payload), payload
    return None, {}


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
        for segment in _candidate_segments(payload):
            if not _is_requirement(segment):
                continue
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            categories = [
                category
                for category in _CATEGORY_ORDER
                if _CATEGORY_PATTERNS[category].search(searchable)
            ]
            if not categories and _AMBIGUOUS_BILLING_CHANGE_RE.search(searchable):
                categories = ["billing_cycle_alignment"]
            for category in _dedupe(categories):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(segment, category),
                    )
                )
    return candidates


def _merge_candidates(
    candidates: Iterable[_Candidate],
) -> list[SourceBillingProrationRequirement]:
    grouped: dict[tuple[str | None, BillingProrationCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.category), []).append(candidate)

    requirements: list[SourceBillingProrationRequirement] = []
    for (source_brief_id, category), items in grouped.items():
        evidence = tuple(
            sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold)
        )[:5]
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        requirements.append(
            SourceBillingProrationRequirement(
                source_brief_id=source_brief_id,
                category=category,
                evidence=evidence,
                confidence=confidence,
                owner_suggestion=_OWNER_BY_CATEGORY[category],
                planning_notes=_PLANNING_NOTES_BY_CATEGORY[category],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in (
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
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "billing",
        "subscription",
        "pricing",
        "invoice",
        "tax",
        "refund",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(
    segments: list[_Segment],
    source_field: str,
    value: Any,
    section_context: bool,
) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _BILLING_CONTEXT_RE.search(key_text)
                or _PRORATION_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text, segment_context in _segments(text, field_context):
            segments.append(_Segment(source_field, segment_text, segment_context))


def _segments(value: str, inherited_context: bool) -> list[tuple[str, bool]]:
    segments: list[tuple[str, bool]] = []
    section_context = inherited_context
    for raw_line in value.splitlines() or [value]:
        line = raw_line.strip()
        if not line:
            continue
        heading = _HEADING_RE.match(line)
        if heading:
            title = _clean_text(heading.group("title"))
            section_context = inherited_context or bool(
                _BILLING_CONTEXT_RE.search(title)
                or _PRORATION_CONTEXT_RE.search(title)
                or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    text = segment.text
    searchable = f"{_field_words(segment.source_field)} {text}"
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if has_category and (
        _REQUIREMENT_RE.search(text)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
    ):
        return True
    if has_category and _BILLING_CONTEXT_RE.search(searchable) and _PRORATION_CONTEXT_RE.search(searchable):
        return True
    if _AMBIGUOUS_BILLING_CHANGE_RE.search(searchable):
        return bool(
            _REQUIREMENT_RE.search(text)
            and (segment.section_context or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
        )
    return False


def _confidence(segment: _Segment, category: BillingProrationCategory) -> BillingProrationConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _AMBIGUOUS_BILLING_CHANGE_RE.search(searchable) and not _PRORATION_CONTEXT_RE.search(searchable):
        return "low"
    if _PRORATION_CONTEXT_RE.search(searchable) or category in {
        "refund_policy",
        "invoice_adjustment",
        "tax_interaction",
        "trial_conversion",
    }:
        return "high"
    if not _REQUIREMENT_RE.search(segment.text):
        return "medium"
    return "medium"


def _summary(
    requirements: tuple[SourceBillingProrationRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "categories": [requirement.category for requirement in requirements],
    }


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
        "workflow_context",
        "problem_statement",
        "mvp_goal",
        "requirements",
        "constraints",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
        "integration_points",
        "billing",
        "subscription",
        "pricing",
        "invoice",
        "tax",
        "refund",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = str(value).strip()
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value:
            continue
        _, _, statement = value.partition(": ")
        key = _clean_text(statement or value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


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
