"""Extract subscription cancellation requirements from source and implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SubscriptionCancellationCategory = Literal[
    "self_service_cancel",
    "cancellation_window",
    "end_of_term_access",
    "immediate_termination",
    "refund_credit_policy",
    "retention_offer",
    "reactivation_path",
    "audit_receipt",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[SubscriptionCancellationCategory, ...] = (
    "self_service_cancel",
    "cancellation_window",
    "end_of_term_access",
    "immediate_termination",
    "refund_credit_policy",
    "retention_offer",
    "reactivation_path",
    "audit_receipt",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_CANCELLATION_CONTEXT_RE = re.compile(
    r"\b(?:cancel|cancels|cancelled|canceled|cancelling|canceling|cancellation|"
    r"subscription cancellation|subscription lifecycle|terminate|termination|"
    r"churn|retention|winback|reactivat(?:e|ion)|resubscribe|refund|credit|"
    r"account closure|access through|end of term|billing period|renewal|"
    r"receipt|confirmation|audit trail|audit log)\b",
    re.I,
)
_SUBSCRIPTION_CONTEXT_RE = re.compile(
    r"\b(?:subscription|subscriptions|subscriber|plan|billing|invoice|renewal|"
    r"contract|term|period|cycle|paid access|customer account|seat|seats)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:cancel|cancellation|termination|subscription|billing|plan|renewal|term|"
    r"refund|credit|retention|offer|reactivat|receipt|audit|confirmation|"
    r"requirements?|acceptance|criteria|definition[-_ ]?of[-_ ]?done|metadata|"
    r"risks?|architecture|support|access)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|provide|define|document|calculate|issue|send|display|show|"
    r"persist|record|validate|offer|route|done when|acceptance|cannot ship)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,90}\b(?:cancel|cancellation|cancellations|"
    r"cancelled|canceled|terminate|termination|subscription)\b.{0,90}\b(?:in scope|"
    r"required|requirements?|needed|changes?|work|updates?)\b",
    re.I,
)
_OUT_OF_SCOPE_RE = re.compile(
    r"\b(?:out of scope|non[- ]goal|not part of this release|future consideration)\b",
    re.I,
)
_SPECIFIC_CANCELLATION_RE = re.compile(
    r"\b(?:self[- ]service cancel|cancel button|cancel flow|cancellation window|"
    r"notice period|advance notice|end[- ]of[- ]term access|access through renewal|"
    r"immediate termination|terminate immediately|refund policy|credit policy|"
    r"retention offer|save offer|reactivation path|winback|resubscribe|"
    r"cancellation receipt|confirmation email|audit trail|audit log)\b",
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

_CATEGORY_PATTERNS: dict[SubscriptionCancellationCategory, re.Pattern[str]] = {
    "self_service_cancel": re.compile(
        r"\b(?:self[- ]service cancel(?:lation)?|cancel button|cancel flow|cancel in app|"
        r"customer(?:s)? can cancel|subscriber(?:s)? can cancel|account owner can cancel|"
        r"cancel subscription from|cancellation settings|cancellation portal)\b",
        re.I,
    ),
    "cancellation_window": re.compile(
        r"\b(?:cancellation window|cancel(?:lation)? deadline|notice period|advance notice|"
        r"before renewal|prior to renewal|within \d+ days?|by the renewal date|"
        r"minimum term|contract term|effective at renewal|cancel(?:s|led|ed)? after)\b",
        re.I,
    ),
    "end_of_term_access": re.compile(
        r"\b(?:end[- ]of[- ]term access|access through (?:the )?(?:end of )?(?:billing )?(?:period|term|cycle)|"
        r"access until renewal|keep access until|remain active until|"
        r"subscription stays active until|cancel(?:led|ed)? at period end|"
        r"end of current term|end of paid term)\b",
        re.I,
    ),
    "immediate_termination": re.compile(
        r"\b(?:immediate termination|terminate immediately|cancel immediately|"
        r"immediate cancellation|revoke access immediately|access stops immediately|"
        r"shut off access|disable subscription immediately|terminate access)\b",
        re.I,
    ),
    "refund_credit_policy": re.compile(
        r"\b(?:refund policy|refund eligibility|refund window|partial refund|"
        r"non[- ]refundable|credit policy|account credit|unused (?:time|value)|"
        r"refunds? for cancellation|cancellation credits?|credit unused)\b",
        re.I,
    ),
    "retention_offer": re.compile(
        r"\b(?:retention offer|save offer|discount to stay|pause plan|downgrade offer|"
        r"offer before cancel|winback offer|churn save|cancel deflection|"
        r"retention step|retention flow)\b",
        re.I,
    ),
    "reactivation_path": re.compile(
        r"\b(?:reactivation path|reactivat(?:e|es|ed|ion)|resubscribe|resume subscription|"
        r"restart subscription|restore subscription|winback path|recover cancelled account|"
        r"recover canceled account)\b",
        re.I,
    ),
    "audit_receipt": re.compile(
        r"\b(?:cancellation receipt|cancel(?:lation)? confirmation|confirmation email|"
        r"audit receipt|audit trail|audit log|record cancellation|cancellation event|"
        r"cancellation timestamp|cancellation reason|support audit|proof of cancellation)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[SubscriptionCancellationCategory, str] = {
    "self_service_cancel": "product",
    "cancellation_window": "billing_ops",
    "end_of_term_access": "billing_engineering",
    "immediate_termination": "platform_engineering",
    "refund_credit_policy": "finance_ops",
    "retention_offer": "growth_product",
    "reactivation_path": "lifecycle_product",
    "audit_receipt": "support_ops",
}
_PLANNING_NOTE_BY_CATEGORY: dict[SubscriptionCancellationCategory, str] = {
    "self_service_cancel": "Plan customer-owned cancellation entry points, authorization checks, and cancellation state changes.",
    "cancellation_window": "Confirm notice periods, renewal deadlines, and effective-date rules before task generation.",
    "end_of_term_access": "Specify access, entitlement, and billing state behavior through the paid term.",
    "immediate_termination": "Define immediate access revocation, data retention, and support escalation behavior.",
    "refund_credit_policy": "Capture refund eligibility, unused-value credits, and finance approval requirements.",
    "retention_offer": "Coordinate retention offers, eligibility, and analytics before final cancellation.",
    "reactivation_path": "Plan reactivation states, billing restart behavior, and data restoration constraints.",
    "audit_receipt": "Include confirmation receipts, audit events, timestamps, and support-visible evidence.",
}


@dataclass(frozen=True, slots=True)
class SourceSubscriptionCancellationRequirement:
    """One source-backed subscription cancellation requirement category."""

    category: SubscriptionCancellationCategory
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)
    suggested_owner: str = ""
    suggested_planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "suggested_owner": self.suggested_owner,
            "suggested_planning_note": self.suggested_planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceSubscriptionCancellationRequirementsReport:
    """Brief-level subscription cancellation requirements report before planning."""

    source_id: str | None = None
    requirements: tuple[SourceSubscriptionCancellationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSubscriptionCancellationRequirement, ...]:
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
        """Return subscription cancellation requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Subscription Cancellation Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        owner_counts = self.summary.get("suggested_owner_counts", {})
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
            "- Suggested owner counts: "
            + (", ".join(f"{owner} {owner_counts[owner]}" for owner in sorted(owner_counts)) or "none"),
        ]
        if not self.requirements:
            lines.extend(
                ["", "No subscription cancellation requirements were found in the source brief."]
            )
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Confidence | Evidence | Suggested Owner | Suggested Planning Note |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{requirement.confidence:.2f} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{requirement.suggested_owner} | "
                f"{_markdown_cell(requirement.suggested_planning_note)} |"
            )
        return "\n".join(lines)


def build_source_subscription_cancellation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | object
    ),
) -> SourceSubscriptionCancellationRequirementsReport:
    """Build a subscription cancellation requirements report from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceSubscriptionCancellationRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements),
    )


def generate_source_subscription_cancellation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | object
    ),
) -> SourceSubscriptionCancellationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_subscription_cancellation_requirements(source)


def derive_source_subscription_cancellation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | object
    ),
) -> SourceSubscriptionCancellationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_subscription_cancellation_requirements(source)


def extract_source_subscription_cancellation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | object
    ),
) -> tuple[SourceSubscriptionCancellationRequirement, ...]:
    """Return subscription cancellation requirement records extracted from input."""
    return build_source_subscription_cancellation_requirements(source).requirements


def summarize_source_subscription_cancellation_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceSubscriptionCancellationRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic subscription cancellation requirements summary."""
    if isinstance(source_or_result, SourceSubscriptionCancellationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_subscription_cancellation_requirements(source_or_result).summary


def source_subscription_cancellation_requirements_to_dict(
    report: SourceSubscriptionCancellationRequirementsReport,
) -> dict[str, Any]:
    """Serialize a subscription cancellation requirements report to a dictionary."""
    return report.to_dict()


source_subscription_cancellation_requirements_to_dict.__test__ = False


def source_subscription_cancellation_requirements_to_dicts(
    requirements: tuple[SourceSubscriptionCancellationRequirement, ...]
    | list[SourceSubscriptionCancellationRequirement]
    | SourceSubscriptionCancellationRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize subscription cancellation requirement records to dictionaries."""
    if isinstance(requirements, SourceSubscriptionCancellationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_subscription_cancellation_requirements_to_dicts.__test__ = False


def source_subscription_cancellation_requirements_to_markdown(
    report: SourceSubscriptionCancellationRequirementsReport,
) -> str:
    """Render a subscription cancellation requirements report as Markdown."""
    return report.to_markdown()


source_subscription_cancellation_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: SubscriptionCancellationCategory
    confidence: float
    evidence: str


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
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
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
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
    for _, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            categories = [
                category
                for category in _CATEGORY_ORDER
                if _CATEGORY_PATTERNS[category].search(searchable)
            ]
            if not categories:
                continue
            if not _is_requirement(segment):
                continue
            evidence = _evidence_snippet(segment.source_field, segment.text)
            confidence = _confidence(segment)
            for category in categories:
                candidates.append(_Candidate(category=category, confidence=confidence, evidence=evidence))
    return candidates


def _merge_candidates(
    candidates: Iterable[_Candidate],
) -> list[SourceSubscriptionCancellationRequirement]:
    by_category: dict[SubscriptionCancellationCategory, list[_Candidate]] = {}
    for candidate in candidates:
        by_category.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceSubscriptionCancellationRequirement] = []
    for category in _CATEGORY_ORDER:
        items = by_category.get(category, [])
        if not items:
            continue
        evidence = tuple(
            sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold)
        )[:5]
        requirements.append(
            SourceSubscriptionCancellationRequirement(
                category=category,
                confidence=round(max(item.confidence for item in items), 2),
                evidence=evidence,
                suggested_owner=_OWNER_BY_CATEGORY[category],
                suggested_planning_note=_PLANNING_NOTE_BY_CATEGORY[category],
            )
        )
    return requirements


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
        "cancellation",
        "support",
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
                or _CANCELLATION_CONTEXT_RE.search(key_text)
                or _SUBSCRIPTION_CONTEXT_RE.search(key_text)
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
                _CANCELLATION_CONTEXT_RE.search(title)
                or _SUBSCRIPTION_CONTEXT_RE.search(title)
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
    if _OUT_OF_SCOPE_RE.search(searchable) and not _REQUIREMENT_RE.search(text):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    if _REQUIREMENT_RE.search(text):
        return True
    if field_context or segment.section_context:
        return bool(
            _CANCELLATION_CONTEXT_RE.search(searchable)
            or _SUBSCRIPTION_CONTEXT_RE.search(searchable)
            or _SPECIFIC_CANCELLATION_RE.search(searchable)
        )
    return bool(
        _SPECIFIC_CANCELLATION_RE.search(searchable)
        and _CANCELLATION_CONTEXT_RE.search(searchable)
        and _SUBSCRIPTION_CONTEXT_RE.search(searchable)
    )


def _confidence(segment: _Segment) -> float:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    score = 0.68
    if _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        score += 0.08
    if segment.section_context or _CANCELLATION_CONTEXT_RE.search(searchable):
        score += 0.07
    if _REQUIREMENT_RE.search(segment.text):
        score += 0.07
    if _SPECIFIC_CANCELLATION_RE.search(searchable):
        score += 0.05
    return round(min(score, 0.95), 2)


def _summary(
    requirements: tuple[SourceSubscriptionCancellationRequirement, ...],
) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "high_confidence_count": sum(
            1 for requirement in requirements if requirement.confidence >= 0.85
        ),
        "categories": [requirement.category for requirement in requirements],
        "suggested_owner_counts": {
            owner: sum(1 for requirement in requirements if requirement.suggested_owner == owner)
            for owner in sorted({requirement.suggested_owner for requirement in requirements})
        },
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
        "cancellation",
        "support",
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
    "SubscriptionCancellationCategory",
    "SourceSubscriptionCancellationRequirement",
    "SourceSubscriptionCancellationRequirementsReport",
    "build_source_subscription_cancellation_requirements",
    "derive_source_subscription_cancellation_requirements",
    "extract_source_subscription_cancellation_requirements",
    "generate_source_subscription_cancellation_requirements",
    "summarize_source_subscription_cancellation_requirements",
    "source_subscription_cancellation_requirements_to_dict",
    "source_subscription_cancellation_requirements_to_dicts",
    "source_subscription_cancellation_requirements_to_markdown",
]
