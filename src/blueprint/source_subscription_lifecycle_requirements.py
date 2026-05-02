"""Extract subscription lifecycle requirements from source and implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SubscriptionLifecycleCategory = Literal[
    "trial_conversion",
    "cancellation",
    "pause_resume",
    "renewal_notice",
    "plan_change",
    "entitlement_sync",
    "dunning_notice",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[SubscriptionLifecycleCategory, ...] = (
    "trial_conversion",
    "cancellation",
    "pause_resume",
    "renewal_notice",
    "plan_change",
    "entitlement_sync",
    "dunning_notice",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_LIFECYCLE_CONTEXT_RE = re.compile(
    r"\b(?:subscription|subscriptions|subscriber|billing lifecycle|lifecycle|"
    r"recurring billing|recurring subscription|trial|trials|trial conversion|"
    r"cancel|cancellation|pause|resume|renewal|renewals|renewal notice|plan change|"
    r"upgrade|downgrade|entitlement|entitlements|access sync|dunning|past due|"
    r"failed payment|payment failure|grace period)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:subscription|subscriptions|subscriber|billing|lifecycle|trial|cancel|"
    r"cancellation|pause|resume|renewal|plan|entitlement|dunning|payment|invoice|"
    r"requirements?|acceptance|criteria|definition[-_ ]?of[-_ ]?done|risks?|"
    r"architecture|metadata|brief_metadata)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|prevent|send|notify|convert|cancel|pause|resume|renew|upgrade|"
    r"downgrade|sync|reconcile|revoke|grant|done when|acceptance|before launch|"
    r"cannot ship)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:subscription|trial|cancellation|cancel|"
    r"pause|resume|renewal|plan change|upgrade|downgrade|entitlement|dunning|"
    r"payment failure|past due)\b.{0,80}\b(?:in scope|required|requirements?|needed|changes?)\b",
    re.I,
)
_SPECIFIC_LIFECYCLE_RE = re.compile(
    r"\b(?:trial conversion|trial ends?|trial expiry|convert(?:ing)? trial|"
    r"cancel(?:lation)? flow|cancel at period end|immediate cancellation|pause subscription|"
    r"resume subscription|renewal notice|renewal reminder|auto[- ]?renew|plan change|"
    r"upgrade|downgrade|proration|seat change|entitlement sync|entitlements?|"
    r"provision(?:ing)? access|revoke access|dunning notice|past due|failed payment|"
    r"payment retry|grace period)\b",
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

_CATEGORY_PATTERNS: dict[SubscriptionLifecycleCategory, re.Pattern[str]] = {
    "trial_conversion": re.compile(
        r"\b(?:trial conversion|convert(?:s|ed|ing)? (?:from )?trial|trial converts?|"
        r"trial ends?|trial expiry|trial expiration|free trial|paid conversion|"
        r"trial[- ]to[- ]paid|convert to paid|trial billing start)\b",
        re.I,
    ),
    "cancellation": re.compile(
        r"\b(?:cancellation|cancel(?:s|ed|ing)? subscription|cancel flow|cancel at period end|"
        r"end subscription|termination|terminate subscription|immediate cancellation|"
        r"refund on cancel|retention offer)\b",
        re.I,
    ),
    "pause_resume": re.compile(
        r"\b(?:pause subscription|paused subscription|subscription pause|resume subscription|"
        r"resumed subscription|subscription resume|pause and resume|billing pause|"
        r"temporarily suspend|reactivate subscription|subscription reactivation)\b",
        re.I,
    ),
    "renewal_notice": re.compile(
        r"\b(?:renewal notice|renewal reminder|renewal email|renewal notification|"
        r"renewal warning|upcoming renewal|before renewal|auto[- ]?renew(?:al)? notice|"
        r"renewal disclosure|renewal consent)\b",
        re.I,
    ),
    "plan_change": re.compile(
        r"\b(?:plan change|change plans?|upgrade|downgrade|switch plans?|plan switch|"
        r"subscription tier|tier change|price plan|proration|prorated|seat change|"
        r"quantity change|billing interval change)\b",
        re.I,
    ),
    "entitlement_sync": re.compile(
        r"\b(?:entitlement sync|sync entitlements?|entitlements?|access sync|provision(?:ing)? access|"
        r"deprovision(?:ing)? access|revoke access|grant access|feature access|"
        r"licensed seats?|seat entitlement|billing entitlement|plan entitlement|"
        r"subscription status sync)\b",
        re.I,
    ),
    "dunning_notice": re.compile(
        r"\b(?:dunning notice|dunning email|dunning notification|payment failure notice|"
        r"failed payment|payment retry|retry schedule|past due|delinquent|grace period|"
        r"card decline|invoice payment failed|collection notice)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[SubscriptionLifecycleCategory, str] = {
    "trial_conversion": "billing_engineering",
    "cancellation": "billing_engineering",
    "pause_resume": "billing_engineering",
    "renewal_notice": "customer_comms",
    "plan_change": "billing_engineering",
    "entitlement_sync": "platform_engineering",
    "dunning_notice": "finance_ops",
}
_PLANNING_NOTE_BY_CATEGORY: dict[SubscriptionLifecycleCategory, str] = {
    "trial_conversion": "Confirm trial end timing, conversion triggers, payment collection, and customer notifications.",
    "cancellation": "Define cancellation timing, access impact, refunds, retention paths, and confirmation messaging.",
    "pause_resume": "Plan pause and resume states, billing impact, limits, and reactivation behavior.",
    "renewal_notice": "Confirm renewal notice timing, channels, consent requirements, and audit evidence.",
    "plan_change": "Specify upgrade, downgrade, proration, quantity, and billing interval behavior.",
    "entitlement_sync": "Plan entitlement synchronization between billing state, product access, and revocation paths.",
    "dunning_notice": "Define payment failure notices, retry cadence, grace periods, and escalation outcomes.",
}


@dataclass(frozen=True, slots=True)
class SourceSubscriptionLifecycleRequirement:
    """One source-backed subscription lifecycle requirement category."""

    category: SubscriptionLifecycleCategory
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
class SourceSubscriptionLifecycleRequirementsReport:
    """Brief-level subscription lifecycle requirements report before implementation planning."""

    source_id: str | None = None
    requirements: tuple[SourceSubscriptionLifecycleRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSubscriptionLifecycleRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
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
        """Return subscription lifecycle requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Subscription Lifecycle Requirements Report"
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
            lines.extend(["", "No subscription lifecycle requirements were found in the source brief."])
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


def build_source_subscription_lifecycle_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceSubscriptionLifecycleRequirementsReport:
    """Build a subscription lifecycle requirements report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceSubscriptionLifecycleRequirementsReport(
        source_id=source_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def generate_source_subscription_lifecycle_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceSubscriptionLifecycleRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_subscription_lifecycle_requirements(source)


def extract_source_subscription_lifecycle_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[SourceSubscriptionLifecycleRequirement, ...]:
    """Return subscription lifecycle requirement records extracted from brief-shaped input."""
    return build_source_subscription_lifecycle_requirements(source).requirements


def summarize_source_subscription_lifecycle_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceSubscriptionLifecycleRequirementsReport
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic subscription lifecycle requirements summary."""
    if isinstance(source_or_result, SourceSubscriptionLifecycleRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_subscription_lifecycle_requirements(source_or_result).summary


def source_subscription_lifecycle_requirements_to_dict(
    report: SourceSubscriptionLifecycleRequirementsReport,
) -> dict[str, Any]:
    """Serialize a subscription lifecycle requirements report to a plain dictionary."""
    return report.to_dict()


source_subscription_lifecycle_requirements_to_dict.__test__ = False


def source_subscription_lifecycle_requirements_to_dicts(
    requirements: tuple[SourceSubscriptionLifecycleRequirement, ...]
    | list[SourceSubscriptionLifecycleRequirement]
    | SourceSubscriptionLifecycleRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize source subscription lifecycle requirement records to dictionaries."""
    if isinstance(requirements, SourceSubscriptionLifecycleRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_subscription_lifecycle_requirements_to_dicts.__test__ = False


def source_subscription_lifecycle_requirements_to_markdown(
    report: SourceSubscriptionLifecycleRequirementsReport,
) -> str:
    """Render a subscription lifecycle requirements report as Markdown."""
    return report.to_markdown()


source_subscription_lifecycle_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: SubscriptionLifecycleCategory
    confidence: float
    evidence: str


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[str | None, dict[str, Any]]:
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
    if not isinstance(source, (str, bytes, bytearray)):
        payload = _object_payload(source)
        return _brief_id(payload), payload
    return None, {}


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = [
            category
            for category in _CATEGORY_ORDER
            if _CATEGORY_PATTERNS[category].search(searchable)
        ]
        if not categories:
            continue
        if not _is_requirement(segment.text, segment.source_field, segment.section_context):
            continue
        evidence = _evidence_snippet(segment.source_field, segment.text)
        confidence = _confidence(segment.text, segment.source_field, segment.section_context)
        for category in categories:
            candidates.append(_Candidate(category=category, confidence=confidence, evidence=evidence))
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceSubscriptionLifecycleRequirement]:
    by_category: dict[SubscriptionLifecycleCategory, list[_Candidate]] = {}
    for candidate in candidates:
        by_category.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceSubscriptionLifecycleRequirement] = []
    for category in _CATEGORY_ORDER:
        items = by_category.get(category, [])
        if not items:
            continue
        requirements.append(
            SourceSubscriptionLifecycleRequirement(
                category=category,
                confidence=round(max(item.confidence for item in items), 2),
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:5],
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
        "problem_statement",
        "mvp_goal",
        "workflow_context",
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
        "subscription",
        "subscriptions",
        "billing",
        "lifecycle",
        "entitlements",
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
                _STRUCTURED_FIELD_RE.search(key_text) or _LIFECYCLE_CONTEXT_RE.search(key_text)
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
                _LIFECYCLE_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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


def _is_requirement(text: str, source_field: str, section_context: bool) -> bool:
    if _NEGATED_SCOPE_RE.search(text):
        return False
    if len(text.split()) < 3 and not _REQUIREMENT_RE.search(text):
        return False
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if (field_context or section_context) and _LIFECYCLE_CONTEXT_RE.search(text):
        return True
    if _REQUIREMENT_RE.search(text) and (
        _LIFECYCLE_CONTEXT_RE.search(text) or _SPECIFIC_LIFECYCLE_RE.search(text)
    ):
        return True
    if _SPECIFIC_LIFECYCLE_RE.search(text) and _LIFECYCLE_CONTEXT_RE.search(text):
        return True
    return False


def _confidence(text: str, source_field: str, section_context: bool) -> float:
    score = 0.68
    if _STRUCTURED_FIELD_RE.search(_field_words(source_field)):
        score += 0.08
    if section_context or _LIFECYCLE_CONTEXT_RE.search(text):
        score += 0.07
    if _REQUIREMENT_RE.search(text):
        score += 0.07
    if _SPECIFIC_LIFECYCLE_RE.search(text):
        score += 0.05
    return round(min(score, 0.95), 2)


def _summary(requirements: tuple[SourceSubscriptionLifecycleRequirement, ...]) -> dict[str, Any]:
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
        "subscription",
        "subscriptions",
        "billing",
        "lifecycle",
        "entitlements",
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
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "SubscriptionLifecycleCategory",
    "SourceSubscriptionLifecycleRequirement",
    "SourceSubscriptionLifecycleRequirementsReport",
    "build_source_subscription_lifecycle_requirements",
    "extract_source_subscription_lifecycle_requirements",
    "generate_source_subscription_lifecycle_requirements",
    "summarize_source_subscription_lifecycle_requirements",
    "source_subscription_lifecycle_requirements_to_dict",
    "source_subscription_lifecycle_requirements_to_dicts",
    "source_subscription_lifecycle_requirements_to_markdown",
]
