"""Extract source-level SLA credit requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SlaCreditConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_DIMENSION_ORDER: tuple[str, ...] = (
    "credit_trigger",
    "credit_formula",
    "customer_segment",
    "claim_window",
    "exclusions",
    "approval_evidence",
    "notification_evidence",
)
_CONFIDENCE_ORDER: dict[SlaCreditConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SLA_CONTEXT_RE = re.compile(
    r"\b(?:sla|service level|uptime|availability|downtime|outage|service credits?|"
    r"uptime credits?|sla credits?|credit request|credit claim|missed sla|service unavailable|"
    r"claim window|credit window|scheduled maintenance|force majeure)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:sla|service[_ -]?credit|sla[_ -]?credit|credits?|uptime|availability|downtime|"
    r"outage|claim[_ -]?window|claims?|exclusions?|approval|notification|notify|"
    r"requirements?|acceptance|criteria|definition[-_ ]?of[-_ ]?done|metadata|source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|allow|provide|define|document|calculate|apply|issue|receive|grant|"
    r"submit|file|claim|approve|review|notify|log|exclude|included?|acceptance|done when|cannot ship)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:sla credits?|service credits?|uptime credits?|"
    r"credit claims?|claim window|credit policy|sla)\b.{0,100}\b(?:required|needed|in scope|"
    r"planned|changes?|impact|work)\b|"
    r"\b(?:sla credits?|service credits?|uptime credits?|credit claims?|claim window|credit policy)\b"
    r".{0,100}\b(?:not required|not needed|out of scope|no changes?|no work|non[- ]?goal)\b",
    re.I,
)
_TRIGGER_RE = re.compile(
    r"\b(?:trigger(?:s|ed)?|if|when|whenever|after|once)\s+(.{0,140}?"
    r"(?:falls? below|drops? below|miss(?:es|ed)?|breach(?:es|ed)?|outage|downtime|"
    r"unavailable|availability|uptime|service level)[^;\n]*)",
    re.I,
)
_FORMULA_RE = re.compile(
    r"\b(?:equal(?:s)?|credit(?:s)? (?:equal|are|is)|receive(?:s)?|issue(?:s|d)?|grant(?:s|ed)?|"
    r"calculated as|formula(?: is)?|amount(?: is)?)\s+([^.;\n]*(?:\d+\s*%|percent|monthly fees?|"
    r"invoice fees?|service fees?|one month|capped|tier)[^.;\n]*)",
    re.I,
)
_SEGMENT_RE = re.compile(
    r"\b((?:enterprise|paid|premium|business|pro|eligible|affected|all)\s+customers?|"
    r"(?:enterprise|paid|premium|business|pro)\s+accounts?|customers? on [^.;,\n]+)\b",
    re.I,
)
_CLAIM_WINDOW_RE = re.compile(
    r"\b(?:claim window(?: requires?)?|claims? must be|credit requests? must be|submit|file(?:d)?)\s+"
    r"([^.;\n]*(?:within|no later than|by)\s+\d+\s*(?:business\s+)?(?:days?|months?|hours?)[^.;\n]*)",
    re.I,
)
_EXCLUSIONS_RE = re.compile(
    r"\b(?:exclusions? (?:include|includes|are)|excluded downtime includes?|exclude(?:s|d)?|"
    r"not eligible (?:for|when)|credits? do not apply (?:to|when))\s+([^.;\n]+)",
    re.I,
)
_APPROVAL_RE = re.compile(
    r"\b((?:finance|support|legal|account|admin|manual)?\s*(?:review\s+)?approval[^.;\n]*|"
    r"(?:approve|approved|approver|reviewed)[^.;\n]*(?:credit|sla|claim)[^.;\n]*)",
    re.I,
)
_NOTIFICATION_RE = re.compile(
    r"\b((?:notify|notification|email|customer communication|status page)[^.;\n]*"
    r"(?:credit|claim|approved|outage|sla|customers?)[^.;\n]*)",
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
    "id",
    "source_id",
    "source_brief_id",
    "domain",
    "status",
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
    "risks",
    "non_goals",
    "assumptions",
    "integration_points",
    "billing",
    "sla",
    "sla_credits",
    "service_credit_policy",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_PLANNING_NOTES: tuple[str, ...] = (
    "Plan SLA credit calculation, eligibility checks, claim intake, approval evidence, exclusions, and customer communication.",
)


@dataclass(frozen=True, slots=True)
class SourceSlaCreditRequirement:
    """One source-backed SLA credit requirement."""

    source_brief_id: str | None
    requirement_text: str
    credit_trigger: str | None = None
    credit_formula: str | None = None
    customer_segment: str | None = None
    claim_window: str | None = None
    exclusions: str | None = None
    approval_evidence: str | None = None
    notification_evidence: str | None = None
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: SlaCreditConfidence = "medium"
    planning_notes: tuple[str, ...] = _PLANNING_NOTES

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_text": self.requirement_text,
            "credit_trigger": self.credit_trigger,
            "credit_formula": self.credit_formula,
            "customer_segment": self.customer_segment,
            "claim_window": self.claim_window,
            "exclusions": self.exclusions,
            "approval_evidence": self.approval_evidence,
            "notification_evidence": self.notification_evidence,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_notes": list(self.planning_notes),
        }


@dataclass(frozen=True, slots=True)
class SourceSlaCreditRequirementsReport:
    """Source-level SLA credit requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceSlaCreditRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSlaCreditRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceSlaCreditRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as findings."""
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
        """Return SLA credit requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source SLA Credit Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        dimension_counts = self.summary.get("dimension_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Dimension counts: "
            + ", ".join(f"{dimension} {dimension_counts.get(dimension, 0)}" for dimension in _DIMENSION_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No SLA credit requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Requirement | Trigger | Formula | Segment | Claim Window | Exclusions | Approval | Notification | Source Field | Confidence | Planning Notes | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.credit_trigger or '')} | "
                f"{_markdown_cell(requirement.credit_formula or '')} | "
                f"{_markdown_cell(requirement.customer_segment or '')} | "
                f"{_markdown_cell(requirement.claim_window or '')} | "
                f"{_markdown_cell(requirement.exclusions or '')} | "
                f"{_markdown_cell(requirement.approval_evidence or '')} | "
                f"{_markdown_cell(requirement.notification_evidence or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_sla_credit_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSlaCreditRequirementsReport:
    """Extract source-level SLA credit requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceSlaCreditRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_sla_credit_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSlaCreditRequirementsReport:
    """Compatibility alias for building an SLA credit requirements report."""
    return build_source_sla_credit_requirements(source)


def generate_source_sla_credit_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSlaCreditRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_sla_credit_requirements(source)


def derive_source_sla_credit_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSlaCreditRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_sla_credit_requirements(source)


def summarize_source_sla_credit_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceSlaCreditRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted SLA credit requirements."""
    if isinstance(source_or_result, SourceSlaCreditRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_sla_credit_requirements(source_or_result).summary


def source_sla_credit_requirements_to_dict(report: SourceSlaCreditRequirementsReport) -> dict[str, Any]:
    """Serialize an SLA credit requirements report to a plain dictionary."""
    return report.to_dict()


source_sla_credit_requirements_to_dict.__test__ = False


def source_sla_credit_requirements_to_dicts(
    requirements: (
        tuple[SourceSlaCreditRequirement, ...]
        | list[SourceSlaCreditRequirement]
        | SourceSlaCreditRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize SLA credit requirement records to dictionaries."""
    if isinstance(requirements, SourceSlaCreditRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_sla_credit_requirements_to_dicts.__test__ = False


def source_sla_credit_requirements_to_markdown(report: SourceSlaCreditRequirementsReport) -> str:
    """Render an SLA credit requirements report as Markdown."""
    return report.to_markdown()


source_sla_credit_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    requirement_text: str
    credit_trigger: str | None
    credit_formula: str | None
    customer_segment: str | None
    claim_window: str | None
    exclusions: str | None
    approval_evidence: str | None
    notification_evidence: str | None
    source_field: str
    evidence: str
    confidence: SlaCreditConfidence


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
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object) -> tuple[str | None, dict[str, Any]]:
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
    return _optional_text(payload.get("id")) or _optional_text(payload.get("source_brief_id")) or _optional_text(payload.get("source_id"))


def _candidates_for_briefs(brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            searchable = _searchable_text(segment.source_field, segment.text)
            if _NEGATED_RE.search(searchable) or not _is_requirement(segment):
                continue
            candidates.append(
                _Candidate(
                    source_brief_id=source_brief_id,
                    requirement_text=_requirement_text(segment.text),
                    credit_trigger=_credit_trigger(segment.text),
                    credit_formula=_credit_formula(segment.text),
                    customer_segment=_customer_segment(searchable),
                    claim_window=_claim_window(segment.text),
                    exclusions=_exclusions(segment.text),
                    approval_evidence=_approval_evidence(segment.text),
                    notification_evidence=_notification_evidence(segment.text),
                    source_field=segment.source_field,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    confidence=_confidence(segment),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceSlaCreditRequirement]:
    grouped: dict[tuple[str | None, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, _dedupe_requirement_key(candidate.requirement_text)), []).append(candidate)

    requirements: list[SourceSlaCreditRequirement] = []
    for (_source_brief_id, _key), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceSlaCreditRequirement(
                source_brief_id=best.source_brief_id,
                requirement_text=best.requirement_text,
                credit_trigger=_joined_details(item.credit_trigger for item in items),
                credit_formula=_joined_details(item.credit_formula for item in items),
                customer_segment=_joined_details(item.customer_segment for item in items),
                claim_window=_joined_details(item.claim_window for item in items),
                exclusions=_joined_details(item.exclusions for item in items),
                approval_evidence=_joined_details(item.approval_evidence for item in items),
                notification_evidence=_joined_details(item.notification_evidence for item in items),
                source_field=best.source_field,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _primary_dimension_index(requirement),
            requirement.source_field or "",
            requirement.requirement_text.casefold(),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text) or _SLA_CONTEXT_RE.search(key_text))
            _append_value(segments, f"{source_field}.{key}", value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        raw_text = str(value) if isinstance(value, str) else text
        for segment_text, segment_context in _segments(raw_text, field_context):
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
            section_context = inherited_context or bool(_SLA_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text and not _NEGATED_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    has_context = bool(_SLA_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words))
    has_dimension = any(
        pattern.search(searchable)
        for pattern in (
            _TRIGGER_RE,
            _FORMULA_RE,
            _SEGMENT_RE,
            _CLAIM_WINDOW_RE,
            _EXCLUSIONS_RE,
            _APPROVAL_RE,
            _NOTIFICATION_RE,
        )
    )
    return has_context and has_dimension and bool(
        _REQUIREMENT_RE.search(searchable) or segment.section_context or _STRUCTURED_FIELD_RE.search(field_words)
    )


def _confidence(segment: _Segment) -> SlaCreditConfidence:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    dimension_count = sum(
        1
        for pattern in (_TRIGGER_RE, _FORMULA_RE, _SEGMENT_RE, _CLAIM_WINDOW_RE, _EXCLUSIONS_RE, _APPROVAL_RE, _NOTIFICATION_RE)
        if pattern.search(searchable)
    )
    if dimension_count >= 2 and (_REQUIREMENT_RE.search(segment.text) or segment.section_context or _STRUCTURED_FIELD_RE.search(field_words)):
        return "high"
    if dimension_count >= 1:
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceSlaCreditRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "dimension_counts": {
            dimension: sum(1 for requirement in requirements if getattr(requirement, dimension))
            for dimension in _DIMENSION_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
    }


def _credit_trigger(text: str) -> str | None:
    return _match_detail(_TRIGGER_RE, text)


def _credit_formula(text: str) -> str | None:
    return _match_detail(_FORMULA_RE, text)


def _customer_segment(text: str) -> str | None:
    value = _match_detail(_SEGMENT_RE, text)
    return value.casefold() if value else None


def _claim_window(text: str) -> str | None:
    return _match_detail(_CLAIM_WINDOW_RE, text)


def _exclusions(text: str) -> str | None:
    return _match_detail(_EXCLUSIONS_RE, text)


def _approval_evidence(text: str) -> str | None:
    return _match_detail(_APPROVAL_RE, text)


def _notification_evidence(text: str) -> str | None:
    return _match_detail(_NOTIFICATION_RE, text)


def _match_detail(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    if not match:
        return None
    detail = _clean_text(match.group(1)).rstrip(".")
    detail = re.sub(r"^(?:when|if|whenever|after|once)\s+", "", detail, flags=re.I)
    return f"{detail[:1].casefold()}{detail[1:]}" if detail else None


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
        "data_requirements",
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, str]:
    return (
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int("[" in candidate.source_field),
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        candidate.evidence,
    )


def _primary_dimension_index(requirement: SourceSlaCreditRequirement) -> int:
    for index, dimension in enumerate(_DIMENSION_ORDER):
        if getattr(requirement, dimension):
            return index
    return len(_DIMENSION_ORDER)


def _field_words(source_field: str) -> str:
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


def _searchable_text(source_field: str, text: str) -> str:
    return f"{_field_words(source_field)} {text}".replace("_", " ").replace("-", " ")


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


def _requirement_text(value: str) -> str:
    text = _clean_text(value)
    if len(text) > 220:
        text = f"{text[:217].rstrip()}..."
    return text


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


def _dedupe_requirement_key(value: str) -> str:
    return _SPACE_RE.sub(" ", _clean_text(value).casefold()).strip()


def _joined_details(values: Iterable[str | None]) -> str | None:
    joined = "; ".join(_dedupe(value for value in values if value))
    return joined or None


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
    "SlaCreditConfidence",
    "SourceSlaCreditRequirement",
    "SourceSlaCreditRequirementsReport",
    "build_source_sla_credit_requirements",
    "derive_source_sla_credit_requirements",
    "extract_source_sla_credit_requirements",
    "generate_source_sla_credit_requirements",
    "source_sla_credit_requirements_to_dict",
    "source_sla_credit_requirements_to_dicts",
    "source_sla_credit_requirements_to_markdown",
    "summarize_source_sla_credit_requirements",
]
