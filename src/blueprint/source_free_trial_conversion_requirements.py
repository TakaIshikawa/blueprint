"""Extract free trial conversion requirements from source and implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


FreeTrialConversionCategory = Literal[
    "trial_start_eligibility",
    "trial_length",
    "conversion_trigger",
    "payment_method_requirement",
    "expiration_reminder",
    "grace_period_access",
    "trial_entitlements",
    "cancellation_before_conversion",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[FreeTrialConversionCategory, ...] = (
    "trial_start_eligibility",
    "trial_length",
    "conversion_trigger",
    "payment_method_requirement",
    "expiration_reminder",
    "grace_period_access",
    "trial_entitlements",
    "cancellation_before_conversion",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_TRIAL_CONTEXT_RE = re.compile(
    r"\b(?:free trial|trial|trialing|trialled|trialed|trial period|trial account|"
    r"trial plan|trial lifecycle|trial conversion|trial[- ]to[- ]paid|convert to paid|"
    r"paid conversion|trial expiration|trial expiry|trial expires?|trial ends?|"
    r"grace period|expiration reminder|entitlements?|payment method|card on file|"
    r"credit card|billing details|cancel before conversion|cancel trial)\b",
    re.I,
)
_BILLING_CONTEXT_RE = re.compile(
    r"\b(?:subscription|billing|checkout|invoice|payment|charge|paid plan|paid account|"
    r"plan|entitlement|access|seat|seats|workspace|account|customer lifecycle)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:free[_ -]?trial|trial|trial[_ -]?conversion|conversion|expiration|expiry|"
    r"reminder|grace|entitlement|payment|card|billing|checkout|subscription|plan|"
    r"requirements?|acceptance|criteria|definition[-_ ]?of[-_ ]?done|metadata|"
    r"risks?|architecture|support|access|source[_ -]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|provide|define|document|calculate|send|notify|display|show|"
    r"persist|record|validate|collect|charge|convert|start|begin|expire|extend|"
    r"cancel|upgrade|downgrade|done when|acceptance|cannot ship)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,140}\b(?:free trial|trial|trial conversion|"
    r"trial[- ]to[- ]paid|trial expiration|trial reminder|grace period|trial entitlement)\b"
    r".{0,140}\b(?:in scope|required|requirements?|needed|planned|changes?|work|updates?|impact)\b|"
    r"\b(?:free trial|trial|trial conversion|trial[- ]to[- ]paid|trial expiration|"
    r"trial reminder|grace period|trial entitlement)\b.{0,140}\b(?:out of scope|"
    r"not required|not needed|no support|unsupported|no changes?|no work|non[- ]?goal)\b",
    re.I,
)
_OUT_OF_SCOPE_RE = re.compile(
    r"\b(?:out of scope|non[- ]goal|not part of this release|future consideration)\b",
    re.I,
)
_SPECIFIC_TRIAL_RE = re.compile(
    r"\b(?:trial eligibility|eligible for (?:a )?(?:free )?trial|start (?:a )?(?:free )?trial|"
    r"trial duration|trial length|\d+\s*[- ]?day trial|trial expires?|convert to paid|"
    r"trial[- ]to[- ]paid|payment method|card on file|credit card|expiration reminder|"
    r"trial reminder|grace period|trial entitlements?|cancel before conversion|cancel trial)\b",
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

_CATEGORY_PATTERNS: dict[FreeTrialConversionCategory, re.Pattern[str]] = {
    "trial_start_eligibility": re.compile(
        r"\b(?:trial eligibility|eligible for (?:a )?(?:free )?trial|eligibility to start|"
        r"start (?:a )?(?:free )?trial|begin (?:a )?(?:free )?trial|trial signup|"
        r"free trial signup|first[- ]time (?:user|customer|account)|new (?:customer|account)s?|"
        r"once per (?:workspace|account|customer)|trial start)\b",
        re.I,
    ),
    "trial_length": re.compile(
        r"\b(?:trial length|trial duration|trial period|trial window|trial lasts?|"
        r"\d+\s*[- ]?(?:day|week|month)s? (?:free )?trial|free trial (?:lasts|duration)|"
        r"expires? after \d+\s*(?:days?|weeks?|months?)|trial ends? after)\b",
        re.I,
    ),
    "conversion_trigger": re.compile(
        r"\b(?:conversion trigger|trial[- ]to[- ]paid|convert(?:s|ed)? to paid|"
        r"auto[- ]?convert|automatic conversion|upgrade at trial end|paid subscription begins|"
        r"paid plan starts?|charge when trial ends?|bill(?:ing)? after trial|trial converts?)\b",
        re.I,
    ),
    "payment_method_requirement": re.compile(
        r"\b(?:payment method|card on file|credit card|billing details|billing information|"
        r"collect payment|payment required|card required|no card required|checkout before trial|"
        r"payment method before (?:starting|trial|conversion))\b",
        re.I,
    ),
    "expiration_reminder": re.compile(
        r"\b(?:expiration reminder|expiry reminder|trial reminder|trial ending reminder|"
        r"remind(?:er)? before (?:the )?trial ends?|notify before (?:the )?trial expires?|"
        r"trial expires? email|trial ending email|trial expiration notice|days? before expiration)\b",
        re.I,
    ),
    "grace_period_access": re.compile(
        r"\b(?:grace period|grace access|grace window|access after (?:the )?trial expires?|"
        r"access after expiration|post[- ]trial access|extend(?:ed)? trial access|"
        r"soft lock|read[- ]only after trial|trial extension)\b",
        re.I,
    ),
    "trial_entitlements": re.compile(
        r"\b(?:trial entitlements?|trial access|trial plan|trial features?|during (?:the )?trial|"
        r"full access during trial|limited (?:trial )?features?|feature limits?|trial limits?|"
        r"trial seats?|trial usage|trial quota|trial workspace access)\b",
        re.I,
    ),
    "cancellation_before_conversion": re.compile(
        r"\b(?:cancel before conversion|cancel before (?:the )?trial converts?|cancel (?:a )?trial|"
        r"trial cancellation|cancel during (?:the )?trial|cancel before billing|"
        r"cancel before charge|avoid conversion|prevent conversion|opt out before conversion)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[FreeTrialConversionCategory, str] = {
    "trial_start_eligibility": "growth_product",
    "trial_length": "billing_ops",
    "conversion_trigger": "billing_engineering",
    "payment_method_requirement": "payments_engineering",
    "expiration_reminder": "lifecycle_marketing",
    "grace_period_access": "customer_success",
    "trial_entitlements": "entitlements_engineering",
    "cancellation_before_conversion": "billing_product",
}
_PLANNING_NOTE_BY_CATEGORY: dict[FreeTrialConversionCategory, str] = {
    "trial_start_eligibility": "Define who can start a trial, repeat-trial limits, account ownership, and eligibility checks.",
    "trial_length": "Confirm trial duration, start and end timestamps, timezone behavior, and extension policy.",
    "conversion_trigger": "Specify when trials convert to paid, billing state transitions, and failure handling.",
    "payment_method_requirement": "Capture whether payment details are required before trial start or conversion and how they are validated.",
    "expiration_reminder": "Plan reminder timing, channels, content ownership, and suppression rules before expiration.",
    "grace_period_access": "Define post-expiration access, grace duration, lockout states, and customer-success overrides.",
    "trial_entitlements": "Map trial plan features, usage limits, seats, and entitlement differences from paid plans.",
    "cancellation_before_conversion": "Plan cancellation entry points, confirmation, audit records, and conversion suppression before billing.",
}


@dataclass(frozen=True, slots=True)
class SourceFreeTrialConversionRequirement:
    """One source-backed free trial conversion requirement category."""

    category: FreeTrialConversionCategory
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)
    suggested_owner: str = ""
    suggested_planning_note: str = ""

    @property
    def requirement_category(self) -> FreeTrialConversionCategory:
        """Compatibility alias for callers expecting requirement_category naming."""
        return self.category

    @property
    def planning_note(self) -> str:
        """Compatibility alias matching reports that use planning_note naming."""
        return self.suggested_planning_note

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
class SourceFreeTrialConversionRequirementsReport:
    """Brief-level free trial conversion requirements report before planning."""

    source_id: str | None = None
    requirements: tuple[SourceFreeTrialConversionRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceFreeTrialConversionRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceFreeTrialConversionRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return free trial conversion requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Free Trial Conversion Requirements Report"
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
                ["", "No free trial conversion requirements were found in the source brief."]
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


def build_source_free_trial_conversion_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | object
    ),
) -> SourceFreeTrialConversionRequirementsReport:
    """Build a free trial conversion requirements report from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceFreeTrialConversionRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements),
    )


def generate_source_free_trial_conversion_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | object
    ),
) -> SourceFreeTrialConversionRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_free_trial_conversion_requirements(source)


def derive_source_free_trial_conversion_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | object
    ),
) -> SourceFreeTrialConversionRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_free_trial_conversion_requirements(source)


def extract_source_free_trial_conversion_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | object
    ),
) -> tuple[SourceFreeTrialConversionRequirement, ...]:
    """Return free trial conversion requirement records extracted from input."""
    return build_source_free_trial_conversion_requirements(source).requirements


def summarize_source_free_trial_conversion_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceFreeTrialConversionRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic free trial conversion requirements summary."""
    if isinstance(source_or_result, SourceFreeTrialConversionRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_free_trial_conversion_requirements(source_or_result).summary


def source_free_trial_conversion_requirements_to_dict(
    report: SourceFreeTrialConversionRequirementsReport,
) -> dict[str, Any]:
    """Serialize a free trial conversion requirements report to a dictionary."""
    return report.to_dict()


source_free_trial_conversion_requirements_to_dict.__test__ = False


def source_free_trial_conversion_requirements_to_dicts(
    requirements: tuple[SourceFreeTrialConversionRequirement, ...]
    | list[SourceFreeTrialConversionRequirement]
    | SourceFreeTrialConversionRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize free trial conversion requirement records to dictionaries."""
    if isinstance(requirements, SourceFreeTrialConversionRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_free_trial_conversion_requirements_to_dicts.__test__ = False


def source_free_trial_conversion_requirements_to_markdown(
    report: SourceFreeTrialConversionRequirementsReport,
) -> str:
    """Render a free trial conversion requirements report as Markdown."""
    return report.to_markdown()


source_free_trial_conversion_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: FreeTrialConversionCategory
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
) -> list[SourceFreeTrialConversionRequirement]:
    by_category: dict[FreeTrialConversionCategory, list[_Candidate]] = {}
    for candidate in candidates:
        by_category.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceFreeTrialConversionRequirement] = []
    for category in _CATEGORY_ORDER:
        items = by_category.get(category, [])
        if not items:
            continue
        evidence = tuple(
            sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold)
        )[:5]
        requirements.append(
            SourceFreeTrialConversionRequirement(
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
        "pricing",
        "checkout",
        "trial",
        "free_trial",
        "conversion",
        "entitlements",
        "reminders",
        "support",
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
                or _TRIAL_CONTEXT_RE.search(key_text)
                or _BILLING_CONTEXT_RE.search(key_text)
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
                _TRIAL_CONTEXT_RE.search(title)
                or _BILLING_CONTEXT_RE.search(title)
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
            _TRIAL_CONTEXT_RE.search(searchable)
            or _BILLING_CONTEXT_RE.search(searchable)
            or _SPECIFIC_TRIAL_RE.search(searchable)
        )
    return bool(
        _SPECIFIC_TRIAL_RE.search(searchable)
        and _TRIAL_CONTEXT_RE.search(searchable)
        and _BILLING_CONTEXT_RE.search(searchable)
    )


def _confidence(segment: _Segment) -> float:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    score = 0.68
    if _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        score += 0.08
    if segment.section_context or _TRIAL_CONTEXT_RE.search(searchable):
        score += 0.07
    if _REQUIREMENT_RE.search(segment.text):
        score += 0.07
    if _SPECIFIC_TRIAL_RE.search(searchable):
        score += 0.05
    return round(min(score, 0.95), 2)


def _summary(
    requirements: tuple[SourceFreeTrialConversionRequirement, ...],
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
        "pricing",
        "checkout",
        "trial",
        "free_trial",
        "conversion",
        "entitlements",
        "reminders",
        "support",
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
    "FreeTrialConversionCategory",
    "SourceFreeTrialConversionRequirement",
    "SourceFreeTrialConversionRequirementsReport",
    "build_source_free_trial_conversion_requirements",
    "derive_source_free_trial_conversion_requirements",
    "extract_source_free_trial_conversion_requirements",
    "generate_source_free_trial_conversion_requirements",
    "summarize_source_free_trial_conversion_requirements",
    "source_free_trial_conversion_requirements_to_dict",
    "source_free_trial_conversion_requirements_to_dicts",
    "source_free_trial_conversion_requirements_to_markdown",
]
