"""Extract source-level escalation policy requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


EscalationPolicyRequirementType = Literal[
    "severity_level",
    "routing_path",
    "ownership_handoff",
    "response_target",
    "customer_notification",
    "unresolved_policy_gap",
]
EscalationPolicyConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[EscalationPolicyRequirementType, ...] = (
    "severity_level",
    "routing_path",
    "ownership_handoff",
    "response_target",
    "customer_notification",
    "unresolved_policy_gap",
)
_CONFIDENCE_ORDER: dict[EscalationPolicyConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_PLAN_IMPACTS: dict[EscalationPolicyRequirementType, tuple[str, ...]] = {
    "severity_level": (
        "Define severity taxonomy, examples, and decision criteria for support and incident triage.",
    ),
    "routing_path": (
        "Map escalation channels, queues, on-call rotations, paging rules, and fallback routes.",
    ),
    "ownership_handoff": (
        "Assign accountable teams and handoff points across support, incident, engineering, and operations.",
    ),
    "response_target": (
        "Add acknowledgement, response, update, and resolution targets to execution and support plans.",
    ),
    "customer_notification": (
        "Plan customer-facing notification triggers, channels, templates, and approval ownership.",
    ),
    "unresolved_policy_gap": (
        "Resolve open escalation policy questions before implementation planning is considered complete.",
    ),
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_ESCALATION_CONTEXT_RE = re.compile(
    r"\b(?:escalat(?:e|es|ed|ion|ions)|incident|sev(?:erity)?|p[0-4]|critical|"
    r"on[- ]?call|pagerduty|opsgenie|victorops|support(?: queue| tier)?|tier\s*[123]|"
    r"customer support|support handoff|handoff|triage|war room|incident commander|"
    r"status page|customer notification|customer comms|sla|response target|ack(?:nowledg(?:e|ement))?|"
    r"operations|ops|runbook)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:escalation|escalations|severity|incident|incidents|support|operations|ops|on[_ -]?call|"
    r"routing|route|handoff|owner|ownership|dri|response|sla|notification|customer[_ -]?comms|"
    r"status[_ -]?page|policy|policies|metadata|brief[_ -]?metadata|requirements?|acceptance|"
    r"criteria|definition[_ -]?of[_ -]?done|source[_ -]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"define|document|include|provide|routes?|page|notify|escalate|assign|own|owned by|"
    r"handoff|hand off|acknowledge|respond|resolve|within|sla|target|before launch|"
    r"acceptance|policy)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,120}"
    r"\b(?:escalation|incident|support handoff|on-call|on call|severity|customer notification)\b"
    r".{0,120}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:escalation|incident|support handoff|on-call|on call|severity|customer notification)\b"
    r".{0,120}\b(?:out of scope|not required|not needed|no support|unsupported|no work|non-goal|non goal)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:sev\s*[0-4]|sev[- ]?[0-4]|severity\s*[0-4]|p[0-4]|critical|blocker|major|minor|"
    r"tier\s*[123]|l[123]|level\s*[123]|pagerduty|opsgenie|on[- ]?call|incident commander|"
    r"support queue|support tier|status page|within\s+\d+\s*(?:minutes?|mins?|hours?|hrs?|days?)|"
    r"\d+\s*(?:minutes?|mins?|hours?|hrs?|days?)|sla|ack(?:nowledg(?:e|ement))?|"
    r"response target|resolution target|customer email|in[- ]app|slack|email)\b",
    re.I,
)
_OPEN_QUESTION_RE = re.compile(
    r"\b(?:tbd|todo|open question|unresolved|unclear|unknown|missing|needs decision|"
    r"not decided|to be defined|define owner|who owns|which team|confirm|clarify)\b|\?",
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
    "non_goals",
    "assumptions",
    "success_criteria",
    "acceptance_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "support",
    "customer_support",
    "incident",
    "incidents",
    "operations",
    "ops",
    "escalation",
    "escalations",
    "escalation_policy",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_TYPE_PATTERNS: dict[EscalationPolicyRequirementType, re.Pattern[str]] = {
    "severity_level": re.compile(
        r"\b(?:severity levels?|severity taxonomy|severity matrix|severity policy|"
        r"classif(?:y|ied|ication)\s+as\s+(?:sev\s*[0-4]|sev[- ]?[0-4]|p[0-4]|critical)|"
        r"(?:sev\s*[0-4]|sev[- ]?[0-4]|p[0-4]|critical|blocker|major|minor)\s+(?:severity|classification))\b",
        re.I,
    ),
    "routing_path": re.compile(
        r"(?:#[A-Za-z0-9_-]+)|\b(?:routing path|route to|routes? through|route\s+tier|"
        r"escalate to|escalates? to|paging|"
        r"pagerduty|opsgenie|victorops|on[- ]?call|support queue|support tier|tier\s*[123]|"
        r"l[123]|slack channel|war room|incident commander|triage queue)\b",
        re.I,
    ),
    "ownership_handoff": re.compile(
        r"\b(?:owner|owners|owned by|ownership|dri|responsible team|accountable|handoff|hand off|"
        r"support handoff|handover|transfer to|assigned to|assign to|raci|incident owner|"
        r"operations owner|engineering owner)\b",
        re.I,
    ),
    "response_target": re.compile(
        r"\b(?:response target|response time|acknowledg(?:e|ement)|ack target|first response|"
        r"resolution target|update cadence|sla|service level|within\s+\d+\s*(?:minutes?|mins?|hours?|hrs?|days?)|"
        r"\d+\s*(?:minutes?|mins?|hours?|hrs?)\s+(?:ack|response|resolution|update))\b",
        re.I,
    ),
    "customer_notification": re.compile(
        r"\b(?:customer notification|notify customers?|customer comms|customer communication|status page|"
        r"statuspage|customer email|in[- ]app banner|user notification|external update|"
        r"support brief|customer-facing update|announce to customers?)\b",
        re.I,
    ),
    "unresolved_policy_gap": re.compile(
        r"\b(?:tbd|todo|open question|unresolved|unclear|unknown|missing|needs decision|"
        r"not decided|to be defined|define owner|who owns|which team|confirm|clarify|policy gap|"
        r"gap in policy)\b|\?",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class SourceEscalationPolicyRequirement:
    """One source-backed escalation policy requirement."""

    requirement_type: EscalationPolicyRequirementType
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: EscalationPolicyConfidence = "medium"
    value: str | None = None
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    @property
    def category(self) -> EscalationPolicyRequirementType:
        """Compatibility view for extractors that expose category naming."""
        return self.requirement_type

    @property
    def requirement_category(self) -> EscalationPolicyRequirementType:
        """Compatibility view for extractors that expose requirement_category naming."""
        return self.requirement_type

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_plan_impacts": list(self.suggested_plan_impacts),
        }


@dataclass(frozen=True, slots=True)
class SourceEscalationPolicyRequirementsReport:
    """Source-level escalation policy requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceEscalationPolicyRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceEscalationPolicyRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceEscalationPolicyRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return escalation policy requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Escalation Policy Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        type_counts = self.summary.get("requirement_type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Requirement type counts: "
            + ", ".join(
                f"{requirement_type} {type_counts.get(requirement_type, 0)}"
                for requirement_type in _TYPE_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source escalation policy requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Requirement Type | Value | Confidence | Source Field | Matched Terms | Evidence | Suggested Plan Impacts |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell(', '.join(requirement.matched_terms))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.suggested_plan_impacts))} |"
            )
        return "\n".join(lines)


def build_source_escalation_policy_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceEscalationPolicyRequirementsReport:
    """Build an escalation policy requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    requirements = tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceEscalationPolicyRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_escalation_policy_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceEscalationPolicyRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted escalation policy requirements."""
    if isinstance(source, SourceEscalationPolicyRequirementsReport):
        return dict(source.summary)
    return build_source_escalation_policy_requirements(source).summary


def derive_source_escalation_policy_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceEscalationPolicyRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_escalation_policy_requirements(source)


def generate_source_escalation_policy_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceEscalationPolicyRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_escalation_policy_requirements(source)


def extract_source_escalation_policy_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceEscalationPolicyRequirement, ...]:
    """Return escalation policy requirement records from brief-shaped input."""
    return build_source_escalation_policy_requirements(source).requirements


def source_escalation_policy_requirements_to_dict(
    report: SourceEscalationPolicyRequirementsReport,
) -> dict[str, Any]:
    """Serialize an escalation policy requirements report to a plain dictionary."""
    return report.to_dict()


source_escalation_policy_requirements_to_dict.__test__ = False


def source_escalation_policy_requirements_to_dicts(
    requirements: (
        tuple[SourceEscalationPolicyRequirement, ...]
        | list[SourceEscalationPolicyRequirement]
        | SourceEscalationPolicyRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize escalation policy requirement records to dictionaries."""
    if isinstance(requirements, SourceEscalationPolicyRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_escalation_policy_requirements_to_dicts.__test__ = False


def source_escalation_policy_requirements_to_markdown(
    report: SourceEscalationPolicyRequirementsReport,
) -> str:
    """Render an escalation policy requirements report as Markdown."""
    return report.to_markdown()


source_escalation_policy_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: EscalationPolicyRequirementType
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: EscalationPolicyConfidence
    value: str | None


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


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    if _brief_out_of_scope(payload):
        return []
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        if not _is_requirement(segment):
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        requirement_types = [
            requirement_type
            for requirement_type in _TYPE_ORDER
            if _TYPE_PATTERNS[requirement_type].search(searchable)
        ]
        for requirement_type in _dedupe(requirement_types):
            terms = _matched_terms(_TYPE_PATTERNS[requirement_type], segment.text)
            if requirement_type == "severity_level":
                terms = _dedupe([*terms, *_severity_terms(segment.text)])
            candidates.append(
                _Candidate(
                    requirement_type=requirement_type,
                    source_field=segment.source_field,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    matched_terms=tuple(terms),
                    confidence=_confidence(segment, requirement_type),
                    value=_value(requirement_type, segment.text),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceEscalationPolicyRequirement]:
    grouped: dict[EscalationPolicyRequirementType, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.requirement_type, []).append(candidate)

    requirements: list[SourceEscalationPolicyRequirement] = []
    for requirement_type in _TYPE_ORDER:
        items = grouped.get(requirement_type, [])
        if not items:
            continue
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda field: (
                min(
                    _CONFIDENCE_ORDER[item.confidence]
                    for item in items
                    if item.source_field == field
                ),
                field.casefold(),
            ),
        )[0]
        requirements.append(
            SourceEscalationPolicyRequirement(
                requirement_type=requirement_type,
                source_field=source_field,
                evidence=tuple(
                    sorted(
                        _dedupe_evidence(
                            sorted(
                                (item.evidence for item in items),
                                key=lambda evidence: (
                                    len(evidence.partition(": ")[2] or evidence),
                                    evidence.casefold(),
                                ),
                            )
                        ),
                        key=str.casefold,
                    )
                )[:5],
                matched_terms=tuple(
                    _dedupe(
                        term
                        for item in sorted(
                            items, key=lambda candidate: candidate.source_field.casefold()
                        )
                        for term in item.matched_terms
                    )
                )[:8],
                confidence=confidence,
                value=_best_value(requirement_type, items),
                suggested_plan_impacts=_PLAN_IMPACTS[requirement_type],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _TYPE_ORDER.index(requirement.requirement_type),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.value or "",
            requirement.source_field.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    global_context = _brief_escalation_context(payload)
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], global_context)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(segments, str(key), payload[key], global_context)
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
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _ESCALATION_CONTEXT_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _TYPE_PATTERNS.values())
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
                _ESCALATION_CONTEXT_RE.search(title)
                or _STRUCTURED_FIELD_RE.search(title)
                or any(pattern.search(title) for pattern in _TYPE_PATTERNS.values())
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
            clauses = (
                [part]
                if (_NEGATED_SCOPE_RE.search(part) and _ESCALATION_CONTEXT_RE.search(part))
                or _OPEN_QUESTION_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    has_context = bool(
        _ESCALATION_CONTEXT_RE.search(searchable)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))
    )
    if not has_context:
        return False
    has_type = any(pattern.search(searchable) for pattern in _TYPE_PATTERNS.values())
    if not has_type:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if _TYPE_PATTERNS["unresolved_policy_gap"].search(searchable) and _OPEN_QUESTION_RE.search(
        segment.text
    ):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        return True
    return bool(_VALUE_RE.search(searchable))


def _matched_terms(pattern: re.Pattern[str], text: str) -> list[str]:
    return _dedupe(_clean_text(match.group(0)).casefold() for match in pattern.finditer(text))


def _severity_terms(text: str) -> list[str]:
    return _dedupe(
        _clean_text(match.group(0)).casefold()
        for match in re.finditer(
            r"\b(?:sev\s*[0-4]|sev[- ]?[0-4]|severity\s*[0-4]|p[0-4]|critical|blocker|major|minor)\b",
            text,
            re.I,
        )
    )


def _value(requirement_type: EscalationPolicyRequirementType, text: str) -> str | None:
    patterns: dict[EscalationPolicyRequirementType, re.Pattern[str]] = {
        "severity_level": re.compile(
            r"\b(?:sev\s*[0-4]|sev[- ]?[0-4]|severity\s*[0-4]|p[0-4]|critical|blocker|major|minor)\b",
            re.I,
        ),
        "routing_path": re.compile(
            r"\b(?:pagerduty|opsgenie|victorops|on[- ]?call|support queue|support tier|tier\s*[123]|"
            r"l[123]|#[A-Za-z0-9_-]+|incident commander|war room)\b",
            re.I,
        ),
        "ownership_handoff": re.compile(
            r"\b(?:owned by|owner(?: team)?|dri|responsible team|assigned to|handoff to|hand off to|transfer(?: ownership)? to)\b[:\s-]*(?P<tail>[^.;\n]+)?",
            re.I,
        ),
        "response_target": re.compile(
            r"\b(?:within\s+\d+\s*(?:minutes?|mins?|hours?|hrs?|days?)|\d+\s*(?:minutes?|mins?|hours?|hrs?)\s+(?:ack|response|resolution|update)|sla)\b",
            re.I,
        ),
        "customer_notification": re.compile(
            r"\b(?:status page|customer email|in[- ]app banner|customer notification|customer comms|support brief)\b",
            re.I,
        ),
        "unresolved_policy_gap": re.compile(
            r"\b(?:tbd|open question|unresolved|unclear|unknown|missing|needs decision|not decided|to be defined|who owns|which team|confirm|clarify|policy gap)\b|\?",
            re.I,
        ),
    }
    match = patterns[requirement_type].search(text)
    if not match:
        return None
    if requirement_type == "ownership_handoff" and match.groupdict().get("tail"):
        tail = _clean_text(match.group("tail")).strip(" :.-")
        if tail:
            return tail[:80]
    return _clean_text(match.group(0)).casefold()


def _best_value(
    requirement_type: EscalationPolicyRequirementType,
    items: Iterable[_Candidate],
) -> str | None:
    priority = {
        "pagerduty": 0,
        "opsgenie": 1,
        "victorops": 2,
        "on-call": 3,
    }
    values = sorted(
        {item.value for item in items if item.value},
        key=lambda value: (
            priority.get(value.casefold(), 10) if requirement_type == "routing_path" else 0,
            len(value),
            value.casefold(),
        ),
    )
    return values[0] if values else None


def _confidence(
    segment: _Segment,
    requirement_type: EscalationPolicyRequirementType,
) -> EscalationPolicyConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    has_value = bool(_VALUE_RE.search(searchable) or _value(requirement_type, segment.text))
    has_specific_context = bool(
        segment.section_context
        or any(
            marker in segment.source_field.replace("-", "_").casefold()
            for marker in (
                "acceptance_criteria",
                "definition_of_done",
                "success_criteria",
                "scope",
                "support",
                "incident",
                "operations",
                "ops",
                "escalation",
                "metadata",
            )
        )
    )
    if requirement_type == "unresolved_policy_gap":
        return "high" if has_specific_context and _OPEN_QUESTION_RE.search(segment.text) else "medium"
    if _REQUIREMENT_RE.search(segment.text) and has_specific_context and has_value:
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and has_specific_context:
        return "high"
    if _REQUIREMENT_RE.search(segment.text) or has_specific_context or has_value:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceEscalationPolicyRequirement, ...]) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "requirement_types": [
            requirement.requirement_type for requirement in requirements
        ],
        "requirement_type_counts": {
            requirement_type: sum(
                1
                for requirement in requirements
                if requirement.requirement_type == requirement_type
            )
            for requirement_type in _TYPE_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "status": (
            "ready_for_escalation_policy_planning"
            if requirements
            else "no_escalation_policy_language"
        ),
    }


def _brief_out_of_scope(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in (
            "title",
            "summary",
            "scope",
            "non_goals",
            "constraints",
            "source_payload",
        )
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_NEGATED_SCOPE_RE.search(scoped_text))


def _brief_escalation_context(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("title", "domain", "summary", "workflow_context", "product_surface")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(
        _ESCALATION_CONTEXT_RE.search(scoped_text)
        and not _NEGATED_SCOPE_RE.search(scoped_text)
    )


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
        "non_goals",
        "assumptions",
        "success_criteria",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "support",
        "customer_support",
        "incident",
        "incidents",
        "operations",
        "ops",
        "escalation",
        "escalations",
        "escalation_policy",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


def _strings(value: Any) -> list[str]:
    if value is None or isinstance(value, (bytes, bytearray)):
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
        tail_key = key.rsplit(" - ", 1)[-1]
        if key in seen or tail_key in seen:
            continue
        deduped.append(value)
        seen.add(key)
        seen.add(tail_key)
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
    "EscalationPolicyRequirementType",
    "EscalationPolicyConfidence",
    "SourceEscalationPolicyRequirement",
    "SourceEscalationPolicyRequirementsReport",
    "build_source_escalation_policy_requirements",
    "derive_source_escalation_policy_requirements",
    "extract_source_escalation_policy_requirements",
    "generate_source_escalation_policy_requirements",
    "summarize_source_escalation_policy_requirements",
    "source_escalation_policy_requirements_to_dict",
    "source_escalation_policy_requirements_to_dicts",
    "source_escalation_policy_requirements_to_markdown",
]
