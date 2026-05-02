"""Extract source-level customer support tier and SLA requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SupportTierRequirement = Literal["enterprise", "premium", "standard", "self_serve"]
SupportTierRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TIER_ORDER: tuple[SupportTierRequirement, ...] = (
    "enterprise",
    "premium",
    "standard",
    "self_serve",
)
_CONFIDENCE_ORDER: dict[SupportTierRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_PLANNING_NOTES: dict[SupportTierRequirement, str] = {
    "enterprise": "Plan enterprise support entitlements, named ownership, escalation routing, and committed SLA measurement.",
    "premium": "Plan premium support packaging, priority routing, paid entitlement checks, and customer-facing SLA copy.",
    "standard": "Plan standard support coverage, business-hours handling, support channel availability, and response tracking.",
    "self_serve": "Plan self-serve support limits, help content, entitlement messaging, and escalation exclusions.",
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SUPPORT_CONTEXT_RE = re.compile(
    r"\b(?:support|customer support|helpdesk|help desk|service desk|ticket|case|"
    r"sla|response time|first response|escalat(?:e|ion)|customer success|csm|"
    r"account owner|account manager|tam|support package|support plan|entitlement)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:support|support[-_ ]?tier|tier|sla|response[-_ ]?time|first[-_ ]?response|"
    r"support[-_ ]?channel|channel|escalat|account[-_ ]?owner|customer[-_ ]?success|"
    r"csm|tam|entitlement|package|premium|enterprise|standard|self[-_ ]?serve)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"provide|include|route|triage|escalat(?:e|es|ed|ion)|respond|resolve|"
    r"check|validate|gate|entitled|handoff|hand off|available|only|cannot ship)\b",
    re.I,
)
_NO_SUPPORT_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:support|support tier|support tiers|sla|"
    r"response time|customer success|escalation)\b.{0,80}\b(?:impact|changes?|"
    r"requirements?|needed|required|scope)\b|\bno[- ]support[- ]impact\b",
    re.I,
)
_TIER_PATTERNS: dict[SupportTierRequirement, re.Pattern[str]] = {
    "enterprise": re.compile(
        r"\b(?:enterprise|strategic account|contracted customers?|dedicated support|"
        r"named account owner|named csm|technical account manager|tam)\b",
        re.I,
    ),
    "premium": re.compile(
        r"\b(?:premium|priority support|paid support|premier support|gold support|"
        r"platinum support|support add[- ]?on|paid support package)\b",
        re.I,
    ),
    "standard": re.compile(
        r"\b(?:standard support|standard tier|default support|business[- ]hours support|"
        r"basic support|included support)\b",
        re.I,
    ),
    "self_serve": re.compile(
        r"\b(?:self[- ]?serve|self service|free tier|community support|help center only|"
        r"knowledge base only|docs only|no live support)\b",
        re.I,
    ),
}
_SLA_RE = re.compile(
    r"\b(?:(?:first )?response(?: time| commitment)?(?: is| must be| within|:)?\s*)?"
    r"(?:within\s+)?(?:\d+(?:\.\d+)?|one|two|three|four|five|six|eight|twelve|twenty[- ]four)\s+"
    r"(?:minutes?|mins?|hours?|hrs?|business hours?|days?)\b|"
    r"\b(?:same day|next business day|business hours?|24[/-]7|24x7|p[0-4]|sev(?:erity)?\s*[0-4])\b",
    re.I,
)
_CHANNEL_RE = re.compile(
    r"\b(?:email|phone|chat|live chat|in[- ]app chat|portal|support portal|ticket|"
    r"tickets|support case|helpdesk|help desk|zendesk|intercom|slack|teams|"
    r"community forum|help center|knowledge base|docs)\b",
    re.I,
)
_ESCALATION_RE = re.compile(
    r"\b(?:no live support escalation|escalat(?:e|es|ed|ion)(?: path)?|tier 2|tier two|tier 3|tier three|"
    r"on[- ]call|pager|engineering owner|named account owner|account owner|"
    r"account manager|dedicated csm|customer success manager|customer success|"
    r"csm|technical account manager|tam|handoff|hand off)\b[^.;\n]*",
    re.I,
)
_AUDIENCE_RE = re.compile(
    r"\b(?:for|to|available to|applies to|entitled customers?:?)\s+(?:the\s+)?"
    r"((?:enterprise|premium|standard|self[- ]?serve|free|paid|contracted|strategic|"
    r"eligible|affected)\s+(?:customers?|accounts?|tenants?|users?|plans?))\b",
    re.I,
)
_ENTITLEMENT_RE = re.compile(
    r"\b(?:entitlement|entitled|eligible|paid package|paid support|support package|"
    r"support add[- ]?on|contracted|plan check|tier check|verify support)\b",
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


@dataclass(frozen=True, slots=True)
class SourceSupportTierRequirement:
    """One source-backed support tier and SLA requirement."""

    source_brief_id: str | None
    tier: SupportTierRequirement
    sla_text: str | None = None
    customer_segment: str | None = None
    support_channel: str | None = None
    escalation_note: str | None = None
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: SupportTierRequirementConfidence = "medium"
    planning_note: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "tier": self.tier,
            "sla_text": self.sla_text,
            "customer_segment": self.customer_segment,
            "support_channel": self.support_channel,
            "escalation_note": self.escalation_note,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceSupportTierRequirementsReport:
    """Source-level support tier requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceSupportTierRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSupportTierRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceSupportTierRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
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
        """Return support tier requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Support Tier Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        tier_counts = self.summary.get("tier_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Tier counts: "
            + ", ".join(f"{tier} {tier_counts.get(tier, 0)}" for tier in _TIER_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No support tier requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Tier | SLA | Segment | Channel | Escalation | Source Field | Confidence | Planning Note | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.tier} | "
                f"{_markdown_cell(requirement.sla_text or '')} | "
                f"{_markdown_cell(requirement.customer_segment or '')} | "
                f"{_markdown_cell(requirement.support_channel or '')} | "
                f"{_markdown_cell(requirement.escalation_note or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.planning_note or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_support_tier_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSupportTierRequirementsReport:
    """Extract source-level support tier and SLA requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceSupportTierRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_support_tier_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSupportTierRequirementsReport:
    """Compatibility alias for building a support tier requirements report."""
    return build_source_support_tier_requirements(source)


def generate_source_support_tier_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSupportTierRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_support_tier_requirements(source)


def derive_source_support_tier_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSupportTierRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_support_tier_requirements(source)


def summarize_source_support_tier_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceSupportTierRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted support tier requirements."""
    if isinstance(source_or_result, SourceSupportTierRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_support_tier_requirements(source_or_result).summary


def source_support_tier_requirements_to_dict(
    report: SourceSupportTierRequirementsReport,
) -> dict[str, Any]:
    """Serialize a support tier requirements report to a plain dictionary."""
    return report.to_dict()


source_support_tier_requirements_to_dict.__test__ = False


def source_support_tier_requirements_to_dicts(
    requirements: (
        tuple[SourceSupportTierRequirement, ...]
        | list[SourceSupportTierRequirement]
        | SourceSupportTierRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize support tier requirement records to dictionaries."""
    if isinstance(requirements, SourceSupportTierRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_support_tier_requirements_to_dicts.__test__ = False


def source_support_tier_requirements_to_markdown(
    report: SourceSupportTierRequirementsReport,
) -> str:
    """Render a support tier requirements report as Markdown."""
    return report.to_markdown()


source_support_tier_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    tier: SupportTierRequirement
    sla_text: str | None
    customer_segment: str | None
    support_channel: str | None
    escalation_note: str | None
    source_field: str
    evidence: str
    confidence: SupportTierRequirementConfidence


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
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            if _NO_SUPPORT_IMPACT_RE.search(searchable):
                continue
            tiers = [tier for tier in _TIER_ORDER if _TIER_PATTERNS[tier].search(searchable)]
            if not tiers or not _is_requirement(segment):
                continue
            for tier in _dedupe(tiers):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        tier=tier,
                        sla_text=_sla_details(segment.text),
                        customer_segment=_customer_segment(segment.text, tier),
                        support_channel=_channel(segment.text),
                        escalation_note=_match_detail(_ESCALATION_RE, segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceSupportTierRequirement]:
    grouped: dict[tuple[str | None, SupportTierRequirement], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.tier), []).append(candidate)

    requirements: list[SourceSupportTierRequirement] = []
    for (source_brief_id, tier), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceSupportTierRequirement(
                source_brief_id=source_brief_id,
                tier=tier,
                sla_text=_joined_details(item.sla_text for item in items),
                customer_segment=_joined_details(item.customer_segment for item in items),
                support_channel=_joined_details(item.support_channel for item in items),
                escalation_note=_joined_details(item.escalation_note for item in items),
                source_field=best.source_field,
                evidence=tuple(sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold))[:5],
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                planning_note=_PLANNING_NOTES[tier],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _TIER_ORDER.index(requirement.tier),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.source_field or "",
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
        "support",
        "customer_support",
        "support_tiers",
        "support_tier_requirements",
        "sla",
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
        if _has_structured_shape(value):
            evidence = _structured_text(value)
            if evidence:
                segments.append(_Segment(source_field, evidence, True))
            return
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _SUPPORT_CONTEXT_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _TIER_PATTERNS.values())
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
                _SUPPORT_CONTEXT_RE.search(title)
                or _STRUCTURED_FIELD_RE.search(title)
                or any(pattern.search(title) for pattern in _TIER_PATTERNS.values())
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NO_SUPPORT_IMPACT_RE.search(cleaned):
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text and not _NO_SUPPORT_IMPACT_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NO_SUPPORT_IMPACT_RE.search(searchable):
        return False
    if not _SUPPORT_CONTEXT_RE.search(searchable):
        return False
    return bool(
        _REQUIREMENT_RE.search(segment.text)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
        or _SLA_RE.search(searchable)
        or _CHANNEL_RE.search(searchable)
        or _ESCALATION_RE.search(searchable)
        or _ENTITLEMENT_RE.search(searchable)
    )


def _confidence(segment: _Segment) -> SupportTierRequirementConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    has_detail = any(
        (
            _SLA_RE.search(searchable),
            _CHANNEL_RE.search(searchable),
            _ESCALATION_RE.search(searchable),
            _ENTITLEMENT_RE.search(searchable),
        )
    )
    if (_REQUIREMENT_RE.search(segment.text) or segment.section_context) and has_detail:
        return "high"
    if segment.section_context or _REQUIREMENT_RE.search(segment.text) or has_detail:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceSupportTierRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "tier_counts": {
            tier: sum(1 for requirement in requirements if requirement.tier == tier)
            for tier in _TIER_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "tiers": [requirement.tier for requirement in requirements],
    }


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    return bool(
        keys
        & {
            "tier",
            "support_tier",
            "support_plan",
            "sla",
            "response_time",
            "first_response",
            "channel",
            "support_channel",
            "escalation",
            "account_owner",
            "customer_segment",
            "audience",
            "entitlement",
            "package",
            "handoff",
        }
    )


def _structured_text(item: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = ", ".join(_strings(value)) if isinstance(value, (list, tuple, set)) else _clean_text(value)
        if text:
            parts.append(f"{key}: {text}")
    return "; ".join(parts)


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
        "support",
        "customer_support",
        "support_tiers",
        "support_tier_requirements",
        "sla",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _customer_segment(text: str, tier: SupportTierRequirement) -> str | None:
    if match := _AUDIENCE_RE.search(text):
        return _detail(match.group(1))
    fallback = {
        "enterprise": "enterprise customers",
        "premium": "premium customers",
        "standard": "standard customers",
        "self_serve": "self-serve users",
    }
    return fallback[tier]


def _channel(text: str) -> str | None:
    matches = _dedupe(_detail(match.group(0)) for match in _CHANNEL_RE.finditer(text))
    return ", ".join(match for match in matches if match) or None


def _sla_details(text: str) -> str | None:
    matches = _dedupe(_normalize_sla_match(match.group(0)) for match in _SLA_RE.finditer(text))
    return ", ".join(match for match in matches if match) or None


def _match_detail(pattern: re.Pattern[str], text: str) -> str | None:
    if match := pattern.search(text):
        detail = _detail(match.group(0))
        if pattern is _SLA_RE and detail:
            detail = _normalize_sla_match(detail)
        return detail
    return None


def _normalize_sla_match(value: str) -> str | None:
    detail = _detail(value)
    if not detail:
        return None
    detail = re.sub(
        r"^(?:(?:first )?response(?: time| commitment)?(?: is| must be| within|:)?\s*)",
        "",
        detail,
        flags=re.I,
    )
    if not re.match(r"\b(?:same day|next business day|business hours?|24[/-]7|24x7|p[0-4]|sev)", detail, re.I):
        detail = re.sub(r"^(?!(?:within|under|below|at least|same day|next business day)\b)", "within ", detail, flags=re.I)
    return _clean_text(detail)


def _joined_details(values: Iterable[str | None]) -> str | None:
    details = _dedupe(value for value in values if value)
    return ", ".join(details) if details else None


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, str]:
    detail_count = sum(
        bool(value)
        for value in (
            candidate.sla_text,
            candidate.customer_segment,
            candidate.support_channel,
            candidate.escalation_note,
        )
    )
    return (
        detail_count,
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int("[" in candidate.source_field),
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        candidate.evidence,
    )


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _clean_text(value)
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
    text = _clean_text(value)
    return [text] if text else []


def _field_words(source_field: str) -> str:
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(value)
    return text or None


def _detail(value: Any) -> str | None:
    text = _clean_text(value).strip("`'\" ;,.")
    return text[:140].rstrip() if text else None


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
    "SupportTierRequirement",
    "SupportTierRequirementConfidence",
    "SourceSupportTierRequirement",
    "SourceSupportTierRequirementsReport",
    "build_source_support_tier_requirements",
    "derive_source_support_tier_requirements",
    "extract_source_support_tier_requirements",
    "generate_source_support_tier_requirements",
    "source_support_tier_requirements_to_dict",
    "source_support_tier_requirements_to_dicts",
    "source_support_tier_requirements_to_markdown",
    "summarize_source_support_tier_requirements",
]
