"""Extract source-level audit log retention and event requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


AuditLogRetentionRequirementType = Literal[
    "audit_event_capture",
    "retention_window",
    "immutable_storage",
    "exportability",
    "tamper_evidence",
    "metadata_capture",
    "admin_access_logs",
    "compliance_evidence",
]
AuditLogRetentionConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_REQUIREMENT_ORDER: tuple[AuditLogRetentionRequirementType, ...] = (
    "audit_event_capture",
    "retention_window",
    "immutable_storage",
    "exportability",
    "tamper_evidence",
    "metadata_capture",
    "admin_access_logs",
    "compliance_evidence",
)
_CONFIDENCE_ORDER: dict[AuditLogRetentionConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_AUDIT_CONTEXT_RE = re.compile(
    r"\b(?:audit logs?|audit events?|audit trails?|event logs?|access logs?|activity logs?|history|"
    r"admin logs?|administrator logs?|compliance evidence|evidence export|soc\s*2 evidence|"
    r"security evidence|recordkeeping|records retention|log retention)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|support|"
    r"capture|record|track|log|retain|store|export|provide|make available|preserve|"
    r"acceptance|done when|before launch|cannot ship)\b",
    re.I,
)
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non-goal|non goal)\b.{0,140}"
    r"\b(?:audit logs?|audit trails?|event logs?|access logs?|admin logs?|log retention|"
    r"audit logging|compliance evidence)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|"
    r"impact|for this release)\b|"
    r"\b(?:audit logs?|audit trails?|event logs?|access logs?|admin logs?|log retention|"
    r"audit logging|compliance evidence)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|"
    r"no work|no impact|non-goal|non goal)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:audit|log|logs|logging|event|events|access|admin|administrator|retention|"
    r"immutable|append[_ -]?only|tamper|evidence|export|compliance|soc[_ -]?2|"
    r"requirements?|acceptance|criteria|definition[_ -]?of[_ -]?done|metadata|source[_ -]?payload)",
    re.I,
)
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
    "acceptance",
    "acceptance_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "audit",
    "audit_logs",
    "logging",
    "security",
    "compliance",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
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
_TYPE_PATTERNS: dict[AuditLogRetentionRequirementType, re.Pattern[str]] = {
    "audit_event_capture": re.compile(
        r"\b(?:capture|record|track|log|audit logged|audit event|audit trail|event log|activity log)"
        r".{0,120}\b(?:events?|actions?|changes?|creates?|updates?|deletes?|exports?|access|logins?|permissions?|roles?)\b|"
        r"\b(?:events?|actions?|changes?|creates?|updates?|deletes?|exports?|access|logins?|permissions?|roles?)"
        r".{0,120}\b(?:capture|record|track|log|audit logged)\b",
        re.I,
    ),
    "retention_window": re.compile(r"\b(?:retention|retain|retained|keep|kept|store|stored|archive|purge after)\b", re.I),
    "immutable_storage": re.compile(
        r"\b(?:immutable|append[- ]?only|write once|worm|cannot be modified|non[- ]?editable|read[- ]?only log)\b",
        re.I,
    ),
    "exportability": re.compile(
        r"\b(?:export|exportable|download|extract|evidence package|evidence export|csv|json|report)"
        r".{0,120}\b(?:audit|log|trail|evidence|access)\b|"
        r"\b(?:audit|log|trail|evidence|access).{0,120}\b(?:export|exportable|download|extract|csv|json|report)\b",
        re.I,
    ),
    "tamper_evidence": re.compile(
        r"\b(?:tamper[- ]?evident|tamper evidence|hash chain|signed log|checksum|integrity proof|"
        r"detect tampering|tamper detection|seal(?:ed)? log)\b",
        re.I,
    ),
    "metadata_capture": re.compile(
        r"\b(?:actor|user id|admin id|action|resource|object id|entity id|timestamp|ip address|"
        r"user agent|before and after|reason code|correlation id|request id)\b",
        re.I,
    ),
    "admin_access_logs": re.compile(
        r"\b(?:admin|administrator|superuser|support agent|staff|operator|privileged)"
        r".{0,100}\b(?:access|view|login|impersonat|permission|role|change)\b|"
        r"\b(?:access log|admin log|administrator log|privileged access)\b",
        re.I,
    ),
    "compliance_evidence": re.compile(
        r"\b(?:compliance evidence|audit evidence|soc\s*2|sox|hipaa|pci|gdpr|regulator|auditor|"
        r"evidence review|control evidence|security review)\b",
        re.I,
    ),
}
_RETENTION_RE = re.compile(
    r"\b(?:retain(?:ed)?|retention(?: period| window)?(?: of)?|keep|kept|store(?:d)?|archive(?:d)?|available for)\b"
    r"(?:\s+(?:is|are|must be|should be))?"
    r"(?:\s+\w+){0,8}?\s+"
    r"(?:at least\s+|minimum\s+of\s+|for\s+)?"
    r"(?P<value>\d+(?:\.\d+)?)\s*(?P<unit>days?|months?|years?|yrs?)\b|"
    r"\b(?P<value2>\d+(?:\.\d+)?)\s*(?P<unit2>days?|months?|years?|yrs?)\s+"
    r"(?:retention|audit log retention|log retention|of audit logs?)\b",
    re.I,
)
_SURFACE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("admin access", re.compile(r"\b(?:admin|administrator|privileged|support agent|staff|operator).{0,80}\baccess\b", re.I)),
    ("data export", re.compile(r"\b(?:export|download|extract|report)\b", re.I)),
    ("permission changes", re.compile(r"\b(?:permission|role|rbac|access control)\b", re.I)),
    ("data changes", re.compile(r"\b(?:create|update|delete|change|mutation|write)\b", re.I)),
    ("authentication", re.compile(r"\b(?:login|sign[- ]?in|authentication|session)\b", re.I)),
    ("audit logs", re.compile(r"\b(?:audit logs?|audit trails?|event logs?|access logs?)\b", re.I)),
)


@dataclass(frozen=True, slots=True)
class SourceAuditLogRetentionRequirement:
    """One source-backed audit log retention or audit event requirement."""

    source_brief_id: str | None
    source_field: str
    requirement_type: AuditLogRetentionRequirementType
    audit_surface: str
    retention_window: str | None = None
    retention_days: int | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: AuditLogRetentionConfidence = "medium"
    planning_notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "source_field": self.source_field,
            "requirement_type": self.requirement_type,
            "audit_surface": self.audit_surface,
            "retention_window": self.retention_window,
            "retention_days": self.retention_days,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_notes": list(self.planning_notes),
        }


@dataclass(frozen=True, slots=True)
class SourceAuditLogRetentionRequirementsReport:
    """Source-level audit log retention requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceAuditLogRetentionRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAuditLogRetentionRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAuditLogRetentionRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "summary": dict(self.summary),
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return audit log retention requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Audit Log Retention Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        confidence_counts = self.summary.get("confidence_counts", {})
        type_counts = self.summary.get("requirement_type_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Requirement types: " + ", ".join(f"{kind} {type_counts.get(kind, 0)}" for kind in _REQUIREMENT_ORDER),
            "- Retention windows: " + (", ".join(self.summary.get("retention_windows", [])) or "none"),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source audit log retention requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Source Field | Type | Surface | Retention Window | Retention Days | Confidence | Planning Notes | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell(requirement.audit_surface)} | "
                f"{_markdown_cell(requirement.retention_window or '')} | "
                f"{requirement.retention_days or ''} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_audit_log_retention_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAuditLogRetentionRequirementsReport:
    """Extract source-level audit log retention requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceAuditLogRetentionRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_audit_log_retention_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAuditLogRetentionRequirementsReport:
    """Compatibility alias for building an audit log retention requirements report."""
    return build_source_audit_log_retention_requirements(source)


def generate_source_audit_log_retention_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAuditLogRetentionRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_audit_log_retention_requirements(source)


def summarize_source_audit_log_retention_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAuditLogRetentionRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted audit log retention requirements."""
    if isinstance(source_or_result, SourceAuditLogRetentionRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_audit_log_retention_requirements(source_or_result).summary


def source_audit_log_retention_requirements_to_dict(
    report: SourceAuditLogRetentionRequirementsReport,
) -> dict[str, Any]:
    """Serialize an audit log retention requirements report to a plain dictionary."""
    return report.to_dict()


source_audit_log_retention_requirements_to_dict.__test__ = False


def source_audit_log_retention_requirements_to_dicts(
    requirements: (
        tuple[SourceAuditLogRetentionRequirement, ...]
        | list[SourceAuditLogRetentionRequirement]
        | SourceAuditLogRetentionRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize audit log retention requirement records to dictionaries."""
    if isinstance(requirements, SourceAuditLogRetentionRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_audit_log_retention_requirements_to_dicts.__test__ = False


def source_audit_log_retention_requirements_to_markdown(
    report: SourceAuditLogRetentionRequirementsReport,
) -> str:
    """Render an audit log retention requirements report as Markdown."""
    return report.to_markdown()


source_audit_log_retention_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    source_field: str
    requirement_type: AuditLogRetentionRequirementType
    audit_surface: str
    retention_window: str | None
    retention_days: int | None
    evidence: str
    confidence: AuditLogRetentionConfidence


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
        segments = _candidate_segments(payload)
        if any(_NO_IMPACT_RE.search(f"{_field_words(segment.source_field)} {segment.text}") for segment in segments):
            continue
        for segment in segments:
            if not _is_requirement(segment):
                continue
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            retention_window, retention_days = _retention(searchable)
            requirement_types = _requirement_types(searchable, retention_window)
            for requirement_type in requirement_types:
                candidate_retention_window = retention_window if requirement_type == "retention_window" else None
                candidate_retention_days = retention_days if requirement_type == "retention_window" else None
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        source_field=segment.source_field,
                        requirement_type=requirement_type,
                        audit_surface=_audit_surface(searchable, segment.source_field),
                        retention_window=candidate_retention_window,
                        retention_days=candidate_retention_days,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        confidence=_confidence(segment, requirement_type, retention_window),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceAuditLogRetentionRequirement]:
    grouped: dict[tuple[str | None, AuditLogRetentionRequirementType, str, str | None], list[_Candidate]] = {}
    for candidate in candidates:
        key = (
            candidate.source_brief_id,
            candidate.requirement_type,
            candidate.audit_surface,
            candidate.retention_window if candidate.requirement_type == "retention_window" else None,
        )
        grouped.setdefault(key, []).append(candidate)

    requirements: list[SourceAuditLogRetentionRequirement] = []
    for (source_brief_id, requirement_type, audit_surface, retention_window), items in grouped.items():
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        retention_days = _first_present(item.retention_days for item in items)
        requirements.append(
            SourceAuditLogRetentionRequirement(
                source_brief_id=source_brief_id,
                source_field=_best_source_field(items),
                requirement_type=requirement_type,
                audit_surface=audit_surface,
                retention_window=retention_window or _first_present(item.retention_window for item in items),
                retention_days=retention_days,
                evidence=tuple(sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold))[:6],
                confidence=confidence,
                planning_notes=_planning_notes(requirement_type, items),
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _REQUIREMENT_ORDER.index(requirement.requirement_type),
            requirement.audit_surface.casefold(),
            requirement.retention_days if requirement.retention_days is not None else 10**9,
            requirement.retention_window or "",
            requirement.source_field.casefold(),
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
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text) or _AUDIT_CONTEXT_RE.search(key_text))
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
            section_context = inherited_context or bool(_STRUCTURED_FIELD_RE.search(title) or _AUDIT_CONTEXT_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = [part] if _NO_IMPACT_RE.search(part) and _AUDIT_CONTEXT_RE.search(part) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _NO_IMPACT_RE.search(searchable):
        return False
    if not (_AUDIT_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field))):
        return False
    if _requirement_types(searchable, _retention(searchable)[0]):
        return True
    if _REQUIREMENT_RE.search(segment.text) and (segment.section_context or _AUDIT_CONTEXT_RE.search(searchable)):
        return True
    return False


def _requirement_types(text: str, retention_window: str | None) -> tuple[AuditLogRetentionRequirementType, ...]:
    values = [kind for kind in _REQUIREMENT_ORDER if _TYPE_PATTERNS[kind].search(text)]
    if not retention_window:
        values = [kind for kind in values if kind != "retention_window"]
    if not _AUDIT_CONTEXT_RE.search(text):
        values = [kind for kind in values if kind != "metadata_capture"]
    if retention_window and "retention_window" not in values:
        values.append("retention_window")
    if _AUDIT_CONTEXT_RE.search(text) and not values:
        values.append("audit_event_capture")
    return tuple(_dedupe(values))


def _retention(text: str) -> tuple[str | None, int | None]:
    match = _RETENTION_RE.search(text)
    if not match:
        return None, None
    raw_value = match.group("value") or match.group("value2")
    raw_unit = match.group("unit") or match.group("unit2")
    if not raw_value or not raw_unit:
        return None, None
    value = float(raw_value)
    unit = raw_unit.lower()
    canonical_unit = "year" if unit in {"year", "years", "yr", "yrs"} else "month" if unit in {"month", "months"} else "day"
    days = value * {"day": 1, "month": 30, "year": 365}[canonical_unit]
    display_value = str(int(value)) if value.is_integer() else str(value).rstrip("0").rstrip(".")
    display_unit = canonical_unit if value == 1 else f"{canonical_unit}s"
    return f"{display_value} {display_unit}", int(round(days))


def _audit_surface(text: str, source_field: str) -> str:
    for surface, pattern in _SURFACE_PATTERNS:
        if pattern.search(text):
            return surface
    field_parts = [
        part
        for part in re.split(r"[.\[\]_\-\s]+", source_field)
        if part and not part.isdigit() and part not in {"source", "payload", "metadata", "requirements"}
    ]
    if field_parts and _STRUCTURED_FIELD_RE.search(source_field):
        return _clean_text(" ".join(field_parts[-2:]))
    return "audit logs"


def _confidence(
    segment: _Segment,
    requirement_type: AuditLogRetentionRequirementType,
    retention_window: str | None,
) -> AuditLogRetentionConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    structured = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    if (_REQUIREMENT_RE.search(segment.text) or structured or segment.section_context) and (
        retention_window
        or requirement_type in {"immutable_storage", "tamper_evidence", "metadata_capture", "admin_access_logs"}
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) or _AUDIT_CONTEXT_RE.search(searchable) or structured:
        return "medium"
    return "low"


def _planning_notes(
    requirement_type: AuditLogRetentionRequirementType,
    items: Iterable[_Candidate],
) -> tuple[str, ...]:
    candidates = list(items)
    notes = {
        "audit_event_capture": "Define the audited events, event schema, delivery guarantees, and failure handling before task generation.",
        "retention_window": "Translate the retention window into storage lifecycle, purge, archive, and legal-hold behavior.",
        "immutable_storage": "Plan append-only or write-once storage and restrict mutation paths for audit records.",
        "exportability": "Specify filtered audit export formats, access controls, and evidence package contents.",
        "tamper_evidence": "Define integrity proof, signing, hash chaining, or review checks for tamper evidence.",
        "metadata_capture": "Capture actor, action, resource, timestamp, request context, and reason metadata consistently.",
        "admin_access_logs": "Scope privileged access, impersonation, permission, and support actions as audited events.",
        "compliance_evidence": "Map audit records and exports to the compliance controls or auditor evidence requests.",
    }
    result = [notes[requirement_type]]
    windows = _dedupe(item.retention_window for item in candidates if item.retention_window)
    if windows:
        result.append("Normalize retention window(s) for planning: " + ", ".join(windows) + ".")
    if any(item.requirement_type in {"tamper_evidence", "immutable_storage"} for item in candidates):
        result.append("Include operational review for log integrity and storage policy exceptions.")
    return tuple(result)


def _summary(
    requirements: tuple[SourceAuditLogRetentionRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    retention_windows = _dedupe(requirement.retention_window for requirement in requirements if requirement.retention_window)
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "requirement_type_counts": {
            kind: sum(1 for requirement in requirements if requirement.requirement_type == kind)
            for kind in _REQUIREMENT_ORDER
        },
        "audit_surfaces": _dedupe(requirement.audit_surface for requirement in requirements),
        "retention_windows": retention_windows,
        "max_retention_days": max((requirement.retention_days or 0 for requirement in requirements), default=0),
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "requires_immutable_storage": any(requirement.requirement_type == "immutable_storage" for requirement in requirements),
        "requires_export": any(requirement.requirement_type == "exportability" for requirement in requirements),
        "requires_tamper_evidence": any(requirement.requirement_type == "tamper_evidence" for requirement in requirements),
        "status": "ready_for_audit_log_retention_planning" if requirements else "no_audit_log_retention_language",
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
        "audit",
        "audit_logs",
        "logging",
        "security",
        "compliance",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _group_field(source_field: str) -> str:
    return re.sub(r"\[\d+\]$", "", source_field)


def _best_source_field(items: Iterable[_Candidate]) -> str:
    fields = sorted(
        {_group_field(item.source_field) for item in items if item.source_field},
        key=lambda field: (field.count("."), field.count("["), field.casefold()),
    )
    return fields[0] if fields else ""


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _any_signal(text: str) -> bool:
    return bool(
        _AUDIT_CONTEXT_RE.search(text)
        or _STRUCTURED_FIELD_RE.search(text)
        or any(pattern.search(text) for pattern in _TYPE_PATTERNS.values())
        or _RETENTION_RE.search(text)
    )


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


def _first_present(values: Iterable[_T | None]) -> _T | None:
    for value in values:
        if value is not None:
            return value
    return None


__all__ = [
    "AuditLogRetentionConfidence",
    "AuditLogRetentionRequirementType",
    "SourceAuditLogRetentionRequirement",
    "SourceAuditLogRetentionRequirementsReport",
    "build_source_audit_log_retention_requirements",
    "extract_source_audit_log_retention_requirements",
    "generate_source_audit_log_retention_requirements",
    "source_audit_log_retention_requirements_to_dict",
    "source_audit_log_retention_requirements_to_dicts",
    "source_audit_log_retention_requirements_to_markdown",
    "summarize_source_audit_log_retention_requirements",
]
