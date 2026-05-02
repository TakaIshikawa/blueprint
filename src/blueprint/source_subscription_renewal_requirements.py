"""Extract source-level subscription renewal and dunning requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SubscriptionRenewalRequirementType = Literal[
    "auto_renewal",
    "renewal_notice",
    "grace_period",
    "payment_retry",
    "dunning_message",
    "cancellation_window",
    "renewal_price_change",
    "failed_payment_access",
]
SubscriptionRenewalBillingSurface = Literal[
    "subscription",
    "invoice",
    "payment_method",
    "account_access",
    "notification",
]
SubscriptionRenewalConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[SubscriptionRenewalRequirementType, ...] = (
    "auto_renewal",
    "renewal_notice",
    "grace_period",
    "payment_retry",
    "dunning_message",
    "cancellation_window",
    "renewal_price_change",
    "failed_payment_access",
)
_SURFACE_ORDER: tuple[SubscriptionRenewalBillingSurface, ...] = (
    "subscription",
    "invoice",
    "payment_method",
    "account_access",
    "notification",
)
_CONFIDENCE_ORDER: dict[SubscriptionRenewalConfidence, int] = {
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
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|provide|define|document|send|notify|email|retry|attempt|cancel|"
    r"cancel(?:s|led|ed|lation)?|suspend|"
    r"pause|disable|restore|before launch|cannot ship|done when|acceptance)\b",
    re.I,
)
_RENEWAL_CONTEXT_RE = re.compile(
    r"\b(?:subscription|subscriptions?|renew|renews?|renewal|auto[- ]?renew|"
    r"automatic renewal|billing cycle|term end|contract end|expiration|expires?|"
    r"grace period|payment retr(?:y|ies)|retry cadence|dunning|failed payment|"
    r"past due|payment failure|card failure|cancellation window|cancel before|"
    r"price change|price increase|renewal price|access after failed payment)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:subscription|renewal|auto[-_ ]?renew|billing|dunning|retry|payment|invoice|"
    r"grace|cancel|cancellation|notice|notification|email|price|access|account|"
    r"requirements?|acceptance|criteria|definition[-_ ]?of[-_ ]?done|metadata|"
    r"source[-_ ]?payload)",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,120}"
    r"\b(?:renewal|auto[- ]?renew|dunning|payment retr(?:y|ies)|grace period|"
    r"cancellation window|failed payment|renewal notice|price change)\b.{0,120}"
    r"\b(?:required|needed|in scope|changes?|work|support|planned|for this release)\b|"
    r"\b(?:renewal|auto[- ]?renew|dunning|payment retr(?:y|ies)|grace period|"
    r"cancellation window|failed payment|renewal notice|price change)\b.{0,120}"
    r"\b(?:out of scope|not required|not needed|no changes?|no work|non-goal|non goal)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:minutes?|hours?|days?|weeks?|months?)|"
    r"\d+(?:st|nd|rd|th)\s+(?:day|week|month)|"
    r"(?:daily|weekly|monthly|annual|annually|yearly|every\s+\d+\s+days?)|"
    r"\d+\s*(?:attempts?|retries)|"
    r"\d+\s*%\s*(?:increase|price change|discount)?)\b",
    re.I,
)
_BOOLEAN_VALUE_RE = re.compile(
    r"\b(?:auto[- ]?renew|automatic renewal|renews automatically)\b", re.I
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

_TYPE_PATTERNS: dict[SubscriptionRenewalRequirementType, re.Pattern[str]] = {
    "auto_renewal": re.compile(
        r"\b(?:auto[- ]?renew(?:al|s)?|automatic renewal|renews automatically|"
        r"subscription renews|renew subscriptions automatically)\b",
        re.I,
    ),
    "renewal_notice": re.compile(
        r"\b(?:renewal notice|renewal notification|renewal reminder|notify.{0,80}\brenewal|"
        r"notice.{0,80}\b(?:before|prior to).{0,40}\brenewal|"
        r"(?:email|message).{0,80}\b(?:before|prior to).{0,40}\brenewal)\b",
        re.I,
    ),
    "grace_period": re.compile(
        r"\b(?:grace period|grace window|past[- ]due period|payment grace|"
        r"allow.{0,60}\b(?:days?|weeks?).{0,60}\b(?:after|past due|failed payment))\b",
        re.I,
    ),
    "payment_retry": re.compile(
        r"\b(?:payment retr(?:y|ies)|retry cadence|retry schedule|retry failed payments?|"
        r"retries?.{0,80}\b(?:card|payment|charge|invoice)|"
        r"(?:card|payment|charge|invoice).{0,80}\bretries?)\b",
        re.I,
    ),
    "dunning_message": re.compile(
        r"\b(?:dunning|past[- ]due (?:email|message|notification)|failed payment (?:email|message|notification)|"
        r"payment failure (?:email|message|notification)|notify.{0,80}\b(?:failed payment|past due))\b",
        re.I,
    ),
    "cancellation_window": re.compile(
        r"\b(?:cancellation window|cancel(?:lation)?.{0,80}\b(?:before|prior to).{0,40}\brenewal|"
        r"cancel before renewal|cancel prior to renewal|"
        r"must cancel.{0,80}\b(?:days?|hours?|weeks?).{0,80}\b(?:before|prior to))\b",
        re.I,
    ),
    "renewal_price_change": re.compile(
        r"\b(?:renewal price change|renewal price|price change.{0,80}\brenewal|"
        r"price increase.{0,80}\brenewal|renewal.{0,80}\b(?:price increase|new price|pricing change))\b",
        re.I,
    ),
    "failed_payment_access": re.compile(
        r"\b(?:(?:failed payment|past due|payment failure).{0,100}\b(?:access|suspend|pause|disable|restore|read[- ]only)|"
        r"(?:access|suspend|pause|disable|restore|read[- ]only).{0,100}\b(?:failed payment|past due|payment failure))\b",
        re.I,
    ),
}
_SURFACE_PATTERNS: dict[SubscriptionRenewalBillingSurface, re.Pattern[str]] = {
    "subscription": re.compile(
        r"\b(?:subscription|subscriptions?|plan|plans?|renewal|term|contract|auto[- ]?renew)\b",
        re.I,
    ),
    "invoice": re.compile(r"\b(?:invoice|invoices?|receipt|receipts?|charge|charges?)\b", re.I),
    "payment_method": re.compile(
        r"\b(?:payment method|card|credit card|ach|bank account|billing method|payment instrument)\b",
        re.I,
    ),
    "account_access": re.compile(
        r"\b(?:account access|access|suspend|suspension|pause|disable|read[- ]only|restore|lockout)\b",
        re.I,
    ),
    "notification": re.compile(
        r"\b(?:notice|notification|notify|email|message|reminder|dunning|past[- ]due email)\b",
        re.I,
    ),
}
_DEFAULT_SURFACE_BY_TYPE: dict[
    SubscriptionRenewalRequirementType, SubscriptionRenewalBillingSurface
] = {
    "auto_renewal": "subscription",
    "renewal_notice": "notification",
    "grace_period": "subscription",
    "payment_retry": "payment_method",
    "dunning_message": "notification",
    "cancellation_window": "subscription",
    "renewal_price_change": "subscription",
    "failed_payment_access": "account_access",
}


@dataclass(frozen=True, slots=True)
class SourceSubscriptionRenewalRequirement:
    """One source-backed subscription renewal or dunning requirement."""

    requirement_type: SubscriptionRenewalRequirementType
    billing_surface: SubscriptionRenewalBillingSurface
    value: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: SubscriptionRenewalConfidence = "medium"
    source_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "billing_surface": self.billing_surface,
            "value": self.value,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "source_id": self.source_id,
        }


@dataclass(frozen=True, slots=True)
class SourceSubscriptionRenewalRequirementsReport:
    """Source-level subscription renewal and dunning requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceSubscriptionRenewalRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSubscriptionRenewalRequirement, ...]:
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
        """Return subscription renewal requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Subscription Renewal Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        type_counts = self.summary.get("type_counts", {})
        surface_counts = self.summary.get("billing_surface_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Source count: {self.summary.get('source_count', 0)}",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Type counts: "
            + ", ".join(
                f"{requirement_type} {type_counts.get(requirement_type, 0)}"
                for requirement_type in _TYPE_ORDER
            ),
            "- Billing surface counts: "
            + ", ".join(
                f"{surface} {surface_counts.get(surface, 0)}" for surface in _SURFACE_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(
                ["", "No subscription renewal requirements were found in the source brief."]
            )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Type | Billing Surface | Value | Confidence | Source | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_type} | "
                f"{requirement.billing_surface} | "
                f"{_markdown_cell(requirement.value)} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_id or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_subscription_renewal_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSubscriptionRenewalRequirementsReport:
    """Extract source-level subscription renewal requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceSubscriptionRenewalRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def generate_source_subscription_renewal_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSubscriptionRenewalRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_subscription_renewal_requirements(source)


def derive_source_subscription_renewal_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSubscriptionRenewalRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_subscription_renewal_requirements(source)


def extract_source_subscription_renewal_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> tuple[SourceSubscriptionRenewalRequirement, ...]:
    """Return subscription renewal requirement records extracted from brief-shaped input."""
    return build_source_subscription_renewal_requirements(source).requirements


def summarize_source_subscription_renewal_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceSubscriptionRenewalRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted subscription renewal requirements."""
    if isinstance(source_or_result, SourceSubscriptionRenewalRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_subscription_renewal_requirements(source_or_result).summary


def source_subscription_renewal_requirements_to_dict(
    report: SourceSubscriptionRenewalRequirementsReport,
) -> dict[str, Any]:
    """Serialize a subscription renewal requirements report to a plain dictionary."""
    return report.to_dict()


source_subscription_renewal_requirements_to_dict.__test__ = False


def source_subscription_renewal_requirements_to_dicts(
    requirements: (
        tuple[SourceSubscriptionRenewalRequirement, ...]
        | list[SourceSubscriptionRenewalRequirement]
        | SourceSubscriptionRenewalRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize subscription renewal requirement records to dictionaries."""
    if isinstance(requirements, SourceSubscriptionRenewalRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_subscription_renewal_requirements_to_dicts.__test__ = False


def source_subscription_renewal_requirements_to_markdown(
    report: SourceSubscriptionRenewalRequirementsReport,
) -> str:
    """Render a subscription renewal requirements report as Markdown."""
    return report.to_markdown()


source_subscription_renewal_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: SubscriptionRenewalRequirementType
    billing_surface: SubscriptionRenewalBillingSurface
    value: str
    evidence: str
    confidence: SubscriptionRenewalConfidence
    source_id: str | None


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
        return _brief_id(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _brief_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _brief_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _brief_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _brief_id(payload), payload
    return None, {}


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            if not _is_requirement(segment):
                continue
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            requirement_types = [
                requirement_type
                for requirement_type in _TYPE_ORDER
                if _TYPE_PATTERNS[requirement_type].search(searchable)
            ]
            if "renewal_price_change" in requirement_types:
                requirement_types = [
                    requirement_type
                    for requirement_type in requirement_types
                    if requirement_type != "renewal_notice"
                ]
            for requirement_type in _dedupe(requirement_types):
                candidates.append(
                    _Candidate(
                        requirement_type=requirement_type,
                        billing_surface=_billing_surface(searchable, requirement_type),
                        value=_value(segment.text, requirement_type),
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(segment, requirement_type),
                        source_id=source_id,
                    )
                )
    return candidates


def _merge_candidates(
    candidates: Iterable[_Candidate],
) -> list[SourceSubscriptionRenewalRequirement]:
    grouped: dict[
        tuple[
            str | None,
            SubscriptionRenewalRequirementType,
            SubscriptionRenewalBillingSurface,
        ],
        list[_Candidate],
    ] = {}
    for candidate in candidates:
        grouped.setdefault(
            (candidate.source_id, candidate.requirement_type, candidate.billing_surface), []
        ).append(candidate)

    requirements: list[SourceSubscriptionRenewalRequirement] = []
    for (source_id, requirement_type, billing_surface), items in grouped.items():
        evidence = tuple(
            sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold)
        )[:5]
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        requirements.append(
            SourceSubscriptionRenewalRequirement(
                requirement_type=requirement_type,
                billing_surface=billing_surface,
                value=_strongest_value(items),
                evidence=evidence,
                confidence=confidence,
                source_id=source_id,
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_id) or "",
            _TYPE_ORDER.index(requirement.requirement_type),
            _SURFACE_ORDER.index(requirement.billing_surface),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.value.casefold(),
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
        "renewal",
        "dunning",
        "invoice",
        "payment",
        "notification",
        "account_access",
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
                _STRUCTURED_FIELD_RE.search(key_text) or _RENEWAL_CONTEXT_RE.search(key_text)
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
                _RENEWAL_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    has_type = any(pattern.search(searchable) for pattern in _TYPE_PATTERNS.values())
    if not has_type:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        return True
    return False


def _billing_surface(
    searchable: str,
    requirement_type: SubscriptionRenewalRequirementType,
) -> SubscriptionRenewalBillingSurface:
    if requirement_type in {"renewal_notice", "dunning_message"}:
        return "notification"
    if requirement_type == "failed_payment_access":
        return "account_access"
    if requirement_type == "payment_retry":
        return "payment_method"
    for surface in _SURFACE_ORDER:
        if _SURFACE_PATTERNS[surface].search(searchable):
            return surface
    return _DEFAULT_SURFACE_BY_TYPE[requirement_type]


def _value(text: str, requirement_type: SubscriptionRenewalRequirementType) -> str:
    if match := _VALUE_RE.search(text):
        return _clean_text(match.group(0)).casefold()
    if requirement_type == "auto_renewal" and _BOOLEAN_VALUE_RE.search(text):
        return "enabled"
    return _clean_text(text)


def _strongest_value(items: Iterable[_Candidate]) -> str:
    ordered = sorted(
        items,
        key=lambda item: (
            _CONFIDENCE_ORDER[item.confidence],
            0 if _VALUE_RE.search(item.value) or item.value == "enabled" else 1,
            len(item.value),
            item.value.casefold(),
        ),
    )
    return ordered[0].value


def _confidence(
    segment: _Segment,
    requirement_type: SubscriptionRenewalRequirementType,
) -> SubscriptionRenewalConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if (
        _VALUE_RE.search(segment.text)
        or requirement_type
        in {"auto_renewal", "dunning_message", "failed_payment_access", "renewal_price_change"}
    ) and (_REQUIREMENT_RE.search(segment.text) or segment.section_context):
        return "high"
    if _RENEWAL_CONTEXT_RE.search(searchable):
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceSubscriptionRenewalRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "type_counts": {
            requirement_type: sum(
                1
                for requirement in requirements
                if requirement.requirement_type == requirement_type
            )
            for requirement_type in _TYPE_ORDER
        },
        "billing_surface_counts": {
            surface: sum(
                1 for requirement in requirements if requirement.billing_surface == surface
            )
            for surface in _SURFACE_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "requirement_types": [requirement.requirement_type for requirement in requirements],
        "billing_surfaces": _dedupe(requirement.billing_surface for requirement in requirements),
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
        "renewal",
        "dunning",
        "invoice",
        "payment",
        "notification",
        "account_access",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


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
    "SubscriptionRenewalRequirementType",
    "SubscriptionRenewalBillingSurface",
    "SubscriptionRenewalConfidence",
    "SourceSubscriptionRenewalRequirement",
    "SourceSubscriptionRenewalRequirementsReport",
    "build_source_subscription_renewal_requirements",
    "derive_source_subscription_renewal_requirements",
    "extract_source_subscription_renewal_requirements",
    "generate_source_subscription_renewal_requirements",
    "source_subscription_renewal_requirements_to_dict",
    "source_subscription_renewal_requirements_to_dicts",
    "source_subscription_renewal_requirements_to_markdown",
    "summarize_source_subscription_renewal_requirements",
]
