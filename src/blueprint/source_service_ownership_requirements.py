"""Extract source-level service ownership requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ServiceOwnershipCategory = Literal[
    "service_owner",
    "operational_dri",
    "escalation_path",
    "support_channel",
    "maintenance_window",
    "handoff_requirement",
    "ownership_gap",
]
ServiceOwnershipConfidence = Literal["high", "medium", "low"]
MissingServiceOwnershipDetail = Literal[
    "absent_owner",
    "absent_escalation_target",
    "absent_support_channel",
    "absent_handoff_criteria",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[ServiceOwnershipCategory, ...] = (
    "service_owner",
    "operational_dri",
    "escalation_path",
    "support_channel",
    "maintenance_window",
    "handoff_requirement",
    "ownership_gap",
)
_CONFIDENCE_ORDER: dict[ServiceOwnershipConfidence, int] = {
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
    r"define|document|assign|identify|name|set|confirm|before launch|post[- ]launch|"
    r"cannot ship|acceptance|done when|handoff|hand over|operational review)\b",
    re.I,
)
_OWNERSHIP_CONTEXT_RE = re.compile(
    r"\b(?:service owner|owner(?:ship)?|owned by|dri|directly responsible|on[- ]call|"
    r"operations?|operational|runbook|escalation|support tier|tier [123]|support channel|"
    r"slack|pagerduty|pd service|maintenance window|maintenance owner|handoff|hand[- ]off|"
    r"hand over|post[- ]launch|after launch|operational review|production support|"
    r"ownership gap|no owner|owner tbd|unowned|responsibility)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:owner(?:ship)?|service[-_ ]?owner|dri|responsib|on[-_ ]?call|escalation|"
    r"support|slack|pagerduty|maintenance|handoff|hand[-_ ]?off|post[-_ ]?launch|"
    r"operational|runbook|acceptance|criteria|constraints?|requirements?|metadata|"
    r"source[-_ ]?payload)",
    re.I,
)
_GENERIC_STAKEHOLDER_RE = re.compile(
    r"\b(?:stakeholder|approv(?:al|e|ed)|sign[- ]?off|review(?:er)?|consulted|informed)\b",
    re.I,
)
_CHANNEL_RE = re.compile(
    r"(?:#[A-Za-z0-9][A-Za-z0-9_-]{1,80}|\b(?:Slack|PagerDuty|PD service|support queue|"
    r"support channel|ticket queue|Jira queue|ServiceNow queue|email alias)\b)",
    re.I,
)
_CHANNEL_TARGET_RE = re.compile(
    r"(?:#[A-Za-z0-9][A-Za-z0-9_-]{1,80}|\b(?:Slack|PagerDuty|PD service|Jira queue|"
    r"ServiceNow queue|[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})\b)",
    re.I,
)
_ESCALATION_TARGET_RE = re.compile(
    r"\b(?:escalat(?:e|ion|es|ed|ing)\s+(?:to|path to|through)|page|notify|route to)\s+"
    r"(?P<target>[A-Z#][A-Za-z0-9&/#@_. -]{1,80})",
    re.I,
)
_OWNER_RE = re.compile(
    r"\b(?:service owner|maintenance owner|owner|dri|directly responsible individual|"
    r"on[- ]call team|post[- ]launch owner)\s*(?:is|=|:|will be|must be|should be)?\s+"
    r"(?P<owner>[A-Z][A-Za-z0-9&/#@_. -]{1,80})",
    re.I,
)
_OWNED_BY_RE = re.compile(r"\bowned by\s+(?P<owner>[A-Z][A-Za-z0-9&/#@_. -]{1,80})", re.I)
_GAP_RE = re.compile(
    r"\b(?:owner\s+(?:tbd|unknown|missing)|no\s+(?:clear\s+)?owner|unowned|ownership gap|"
    r"who owns|which team owns|responsibility is unclear|owner not assigned)\b",
    re.I,
)
_HANDOFF_CRITERIA_RE = re.compile(
    r"\b(?:criteria|acceptance|done when|complete when|before handoff|handoff when|"
    r"after runbook|runbook complete|until|once|sign[- ]?off)\b",
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
_CATEGORY_PATTERNS: dict[ServiceOwnershipCategory, re.Pattern[str]] = {
    "service_owner": re.compile(
        r"\b(?:service owner|feature owner|component owner|system owner|service ownership|owned by)\b",
        re.I,
    ),
    "operational_dri": re.compile(
        r"\b(?:dri|directly responsible|operational owner|operations owner|on[- ]call team|"
        r"on[- ]call rotation|production support owner|post[- ]launch owner)\b",
        re.I,
    ),
    "escalation_path": re.compile(
        r"\b(?:escalation path|escalat(?:e|ion|es|ed|ing)|tier [123]|support tier|page)\b",
        re.I,
    ),
    "support_channel": re.compile(
        r"(?:#[A-Za-z0-9][A-Za-z0-9_-]{1,80}|\b(?:support channel|slack channel|slack|"
        r"pagerduty|pd service|support queue|ticket queue|email alias|service desk)\b)",
        re.I,
    ),
    "maintenance_window": re.compile(
        r"\b(?:maintenance window|maintenance owner|maintenance team|scheduled maintenance|"
        r"operational review|ops review|review cadence|runbook review)\b",
        re.I,
    ),
    "handoff_requirement": re.compile(
        r"\b(?:handoff|hand[- ]off|hand over|handover|transition to support|transition to ops|"
        r"post[- ]launch responsibility|after launch responsibility|runbook handoff)\b",
        re.I,
    ),
    "ownership_gap": _GAP_RE,
}


@dataclass(frozen=True, slots=True)
class SourceServiceOwnershipRequirement:
    """One source-backed service ownership or operational responsibility requirement."""

    category: ServiceOwnershipCategory
    requirement: str
    responsible_party: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    missing_detail_flags: tuple[MissingServiceOwnershipDetail, ...] = field(default_factory=tuple)
    confidence: ServiceOwnershipConfidence = "medium"
    source_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "requirement": self.requirement,
            "responsible_party": self.responsible_party,
            "evidence": list(self.evidence),
            "missing_detail_flags": list(self.missing_detail_flags),
            "confidence": self.confidence,
            "source_id": self.source_id,
        }


@dataclass(frozen=True, slots=True)
class SourceServiceOwnershipRequirementsReport:
    """Source-level service ownership requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceServiceOwnershipRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceServiceOwnershipRequirement, ...]:
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
        """Return service ownership requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Service Ownership Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        flag_counts = self.summary.get("missing_detail_flag_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Source count: {self.summary.get('source_count', 0)}",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Missing detail flags: "
            + ", ".join(
                f"{flag} {flag_counts.get(flag, 0)}"
                for flag in (
                    "absent_owner",
                    "absent_escalation_target",
                    "absent_support_channel",
                    "absent_handoff_criteria",
                )
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No service ownership requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Requirement | Responsible Party | Confidence | Missing Details | Source | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{_markdown_cell(requirement.requirement)} | "
                f"{_markdown_cell(requirement.responsible_party or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(', '.join(requirement.missing_detail_flags) or 'none')} | "
                f"{_markdown_cell(requirement.source_id or '')} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_service_ownership_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceServiceOwnershipRequirementsReport:
    """Extract source-level service ownership requirements from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceServiceOwnershipRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def generate_source_service_ownership_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceServiceOwnershipRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_service_ownership_requirements(source)


def derive_source_service_ownership_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceServiceOwnershipRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_service_ownership_requirements(source)


def extract_source_service_ownership_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> tuple[SourceServiceOwnershipRequirement, ...]:
    """Return service ownership requirement records extracted from brief-shaped input."""
    return build_source_service_ownership_requirements(source).requirements


def summarize_source_service_ownership_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceServiceOwnershipRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted service ownership requirements."""
    if isinstance(source_or_result, SourceServiceOwnershipRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_service_ownership_requirements(source_or_result).summary


def source_service_ownership_requirements_to_dict(
    report: SourceServiceOwnershipRequirementsReport,
) -> dict[str, Any]:
    """Serialize a service ownership requirements report to a plain dictionary."""
    return report.to_dict()


source_service_ownership_requirements_to_dict.__test__ = False


def source_service_ownership_requirements_to_dicts(
    requirements: (
        tuple[SourceServiceOwnershipRequirement, ...]
        | list[SourceServiceOwnershipRequirement]
        | SourceServiceOwnershipRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize service ownership requirement records to dictionaries."""
    if isinstance(requirements, SourceServiceOwnershipRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_service_ownership_requirements_to_dicts.__test__ = False


def source_service_ownership_requirements_to_markdown(
    report: SourceServiceOwnershipRequirementsReport,
) -> str:
    """Render a service ownership requirements report as Markdown."""
    return report.to_markdown()


source_service_ownership_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: ServiceOwnershipCategory
    requirement: str
    responsible_party: str | None
    evidence: str
    missing_detail_flags: tuple[MissingServiceOwnershipDetail, ...]
    confidence: ServiceOwnershipConfidence
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
            if not _is_ownership_requirement(segment):
                continue
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            categories = [
                category
                for category in _CATEGORY_ORDER
                if _CATEGORY_PATTERNS[category].search(searchable)
            ]
            for category in _dedupe(categories):
                candidates.append(
                    _Candidate(
                        category=category,
                        requirement=_requirement_value(segment.text),
                        responsible_party=_responsible_party(category, segment.text),
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        missing_detail_flags=_missing_detail_flags(category, segment.text),
                        confidence=_confidence(segment),
                        source_id=source_id,
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceServiceOwnershipRequirement]:
    grouped: dict[tuple[str | None, ServiceOwnershipCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_id, candidate.category), []).append(candidate)

    requirements: list[SourceServiceOwnershipRequirement] = []
    for (source_id, category), items in grouped.items():
        evidence = tuple(sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold))[:6]
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        flags = tuple(
            _dedupe(flag for item in items for flag in item.missing_detail_flags)
        )
        responsible_party = _preferred_responsible_party(category, items)
        requirements.append(
            SourceServiceOwnershipRequirement(
                category=category,
                requirement=_strongest_requirement(items),
                responsible_party=responsible_party,
                evidence=evidence,
                missing_detail_flags=flags,
                confidence=confidence,
                source_id=source_id,
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_id) or "",
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.requirement.casefold(),
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
        "operational_notes",
        "runbook",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
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
                _STRUCTURED_FIELD_RE.search(key_text) or _OWNERSHIP_CONTEXT_RE.search(key_text)
            )
            if _OWNERSHIP_CONTEXT_RE.search(key_text) and _truthy_signal_value(value[key]):
                segments.append(_Segment(child_field, key_text, child_context))
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
                _OWNERSHIP_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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


def _is_ownership_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _generic_stakeholder_only(segment.text):
        return False
    if _GAP_RE.search(searchable):
        return True
    if _responsible_party("service_owner", segment.text) or _CHANNEL_RE.search(segment.text):
        return True
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context and _OWNERSHIP_CONTEXT_RE.search(searchable):
        return True
    if _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)) and _OWNERSHIP_CONTEXT_RE.search(searchable):
        return True
    return False


def _generic_stakeholder_only(text: str) -> bool:
    return bool(_GENERIC_STAKEHOLDER_RE.search(text)) and not bool(_OWNERSHIP_CONTEXT_RE.search(text))


def _requirement_value(text: str) -> str:
    return _clean_text(text)


def _strongest_requirement(items: Iterable[_Candidate]) -> str:
    ordered = sorted(
        items,
        key=lambda item: (
            _CONFIDENCE_ORDER[item.confidence],
            len(item.missing_detail_flags),
            len(item.requirement),
            item.requirement.casefold(),
        ),
    )
    return ordered[0].requirement


def _responsible_party(category: ServiceOwnershipCategory, text: str) -> str | None:
    if category == "support_channel":
        if match := _CHANNEL_TARGET_RE.search(text):
            return _clean_party(match.group(0))
    if category == "escalation_path":
        if match := _ESCALATION_TARGET_RE.search(text):
            return _clean_party(match.group("target"))
        if match := _CHANNEL_RE.search(text):
            return _clean_party(match.group(0))
    for pattern in (_OWNER_RE, _OWNED_BY_RE):
        if match := pattern.search(text):
            party = _clean_party(match.group("owner"))
            if _valid_party(party):
                return party
    return None


def _missing_detail_flags(
    category: ServiceOwnershipCategory,
    text: str,
) -> tuple[MissingServiceOwnershipDetail, ...]:
    flags: list[MissingServiceOwnershipDetail] = []
    has_owner = bool(_responsible_party(category, text))
    has_escalation = bool(_ESCALATION_TARGET_RE.search(text))
    has_channel = bool(_CHANNEL_TARGET_RE.search(text))
    has_handoff_criteria = bool(_HANDOFF_CRITERIA_RE.search(text))

    if category in {"service_owner", "operational_dri", "maintenance_window", "handoff_requirement"} and not has_owner:
        flags.append("absent_owner")
    if category == "escalation_path" and not has_escalation:
        flags.append("absent_escalation_target")
    if category in {"support_channel", "operational_dri"} and not has_channel:
        flags.append("absent_support_channel")
    if category == "handoff_requirement" and not has_handoff_criteria:
        flags.append("absent_handoff_criteria")
    if category == "ownership_gap":
        flags.append("absent_owner")
    return tuple(_dedupe(flags))


def _confidence(segment: _Segment) -> ServiceOwnershipConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _REQUIREMENT_RE.search(segment.text) and _OWNERSHIP_CONTEXT_RE.search(searchable):
        return "high"
    if segment.section_context or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        return "medium"
    return "low"


def _preferred_responsible_party(
    category: ServiceOwnershipCategory,
    items: Iterable[_Candidate],
) -> str | None:
    candidates = [item for item in items if item.responsible_party]
    if category == "support_channel":
        candidates = sorted(
            candidates,
            key=lambda item: (
                0 if re.search(r"\bsupport channel\b|\bslack channel\b|#", item.requirement, re.I) else 1,
                item.requirement.casefold(),
            ),
        )
    return next((item.responsible_party for item in candidates if item.responsible_party), None)


def _summary(
    requirements: tuple[SourceServiceOwnershipRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    flag_names: tuple[MissingServiceOwnershipDetail, ...] = (
        "absent_owner",
        "absent_escalation_target",
        "absent_support_channel",
        "absent_handoff_criteria",
    )
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
        "missing_detail_flag_counts": {
            flag: sum(1 for requirement in requirements if flag in requirement.missing_detail_flags)
            for flag in flag_names
        },
        "categories": [requirement.category for requirement in requirements],
        "has_ownership_gaps": any(requirement.category == "ownership_gap" for requirement in requirements),
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
        "operational_notes",
        "runbook",
        "risks",
        "scope",
        "non_goals",
        "assumptions",
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


def _truthy_signal_value(value: Any) -> bool:
    if value is None or value is False:
        return False
    if isinstance(value, str) and value.strip().casefold() in {"", "false", "no", "none", "n/a"}:
        return False
    return True


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _clean_party(value: Any) -> str:
    text = _clean_text(value)
    text = re.sub(
        r"\s+(?:before|after|when|if|until|for|with|via|and|or|but|during)\b.*$",
        "",
        text,
        flags=re.I,
    )
    return text.strip(" .,:;-") or ""


def _valid_party(value: str) -> bool:
    if not value:
        return False
    return value.casefold() not in {
        "assigned",
        "defined",
        "documented",
        "identified",
        "named",
        "set",
        "tbd",
        "unknown",
        "missing",
    }


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
    "MissingServiceOwnershipDetail",
    "ServiceOwnershipCategory",
    "ServiceOwnershipConfidence",
    "SourceServiceOwnershipRequirement",
    "SourceServiceOwnershipRequirementsReport",
    "build_source_service_ownership_requirements",
    "derive_source_service_ownership_requirements",
    "extract_source_service_ownership_requirements",
    "generate_source_service_ownership_requirements",
    "source_service_ownership_requirements_to_dict",
    "source_service_ownership_requirements_to_dicts",
    "source_service_ownership_requirements_to_markdown",
    "summarize_source_service_ownership_requirements",
]
