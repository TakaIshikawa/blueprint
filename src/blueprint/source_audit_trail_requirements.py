"""Extract source-level audit trail requirements from design briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceAuditTrailRequirementType = Literal[
    "actor_attribution",
    "timestamp_capture",
    "event_type_taxonomy",
    "retention_policy",
    "immutable_event_records",
    "exportability",
]
SourceAuditTrailRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[SourceAuditTrailRequirementType, ...] = (
    "actor_attribution",
    "timestamp_capture",
    "event_type_taxonomy",
    "retention_policy",
    "immutable_event_records",
    "exportability",
)
_CONFIDENCE_ORDER: dict[SourceAuditTrailRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"acceptance|done when|before launch|cannot ship|compliance|policy)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:audit trail|audit logs?|audit events?|event history|"
    r"activity history).*?\b(?:in scope|required|needed|changes?|impact)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:audit|trail|history|event|retention|immutab|export|compliance|"
    r"requirements?|constraints?|acceptance|definition[-_ ]?of[-_ ]?done)",
    re.I,
)
_AUDIT_CONTEXT_RE = re.compile(
    r"\b(?:audit trail|audit log(?:s)?|audit event(?:s)?|audit history|event history|activity history|"
    r"activity log(?:s)?|change history|source trail|compliance trail|auditability)\b",
    re.I,
)
_TIME_WINDOW_RE = re.compile(
    r"\b(?:(?:for|within|after|at least|minimum of|no less than|up to)\s+)?"
    r"(?:\d+(?:\.\d+)?|one|two|three|four|five|six|seven|eight|nine|ten|"
    r"eleven|twelve|thirteen|thirty|sixty|ninety)\s+"
    r"(?:days?|weeks?|months?|quarters?|years?|hrs?|hours?)\b",
    re.I,
)
_TYPE_PATTERNS: dict[SourceAuditTrailRequirementType, re.Pattern[str]] = {
    "actor_attribution": re.compile(
        r"\b(?:actor attribution|actor identity|who performed|who made|performed by|"
        r"initiated by|created by|changed by|user id|user email|admin id|actor id|"
        r"service account|principal|capture actor|record actor|attribut(?:e|ion))\b",
        re.I,
    ),
    "timestamp_capture": re.compile(
        r"\b(?:timestamp(?:s|ed|ing)?|time stamped|time-stamped|occurred at|event time|"
        r"created at|recorded at|utc|timezone)\b",
        re.I,
    ),
    "event_type_taxonomy": re.compile(
        r"\b(?:event type|event types|event taxonomy|action taxonomy|event name|event names|"
        r"action type|change type|operation type|create update delete|crud|"
        r"login event|permission change|record each event)\b",
        re.I,
    ),
    "retention_policy": re.compile(
        r"\b(?:retention policy|retention period|retain(?:ed|ing)? audit|retain(?:ed|ing)? events?|"
        r"keep audit|preserve audit|store audit|audit logs? for \d+|events? for \d+|"
        r"legal hold|purge audit)\b",
        re.I,
    ),
    "immutable_event_records": re.compile(
        r"\b(?:immutable|append[- ]only|tamper[- ]proof|tamper evident|cannot be edited|"
        r"cannot be deleted|write once|worm storage|non[- ]repudiation|sealed event|"
        r"event records?.{0,80}\b(?:immutable|append[- ]only|write once))\b",
        re.I,
    ),
    "exportability": re.compile(
        r"\b(?:export(?:able)? audit|audit export|export events?|download audit|download events?|"
        r"csv export|json export|auditor export|compliance export|retrieve audit|"
        r"api access to audit|exportable history|downloadable)\b",
        re.I,
    ),
}
_MISSING_DETAILS: dict[SourceAuditTrailRequirementType, tuple[str, ...]] = {
    "actor_attribution": ("actor identifier source", "service account handling"),
    "timestamp_capture": ("timestamp source", "timezone standard"),
    "event_type_taxonomy": ("event type taxonomy", "event payload schema"),
    "retention_policy": ("retention period", "deletion or legal hold policy"),
    "immutable_event_records": ("immutability mechanism", "privileged deletion policy"),
    "exportability": ("export format", "export authorization", "export delivery path"),
}
_IGNORED_FIELDS = {
    "id",
    "source_id",
    "source_brief_id",
    "source_project",
    "source_entity_type",
    "created_at",
    "updated_at",
}


@dataclass(frozen=True, slots=True)
class SourceAuditTrailRequirement:
    """One source-backed audit trail requirement."""

    source_brief_id: str | None
    requirement_type: SourceAuditTrailRequirementType
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    confidence: SourceAuditTrailRequirementConfidence = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "missing_details": list(self.missing_details),
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class SourceAuditTrailRequirementsReport:
    """Source-level audit trail requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceAuditTrailRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAuditTrailRequirement, ...]:
        """Compatibility view matching extractors that name findings records."""
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
        """Return audit trail requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Audit Trail Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        type_counts = self.summary.get("type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Confidence counts: "
            f"high {confidence_counts.get('high', 0)}, "
            f"medium {confidence_counts.get('medium', 0)}, "
            f"low {confidence_counts.get('low', 0)}",
            "- Requirement type counts: "
            + ", ".join(f"{key} {type_counts.get(key, 0)}" for key in _TYPE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No audit trail requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Type | Confidence | Missing Details | Evidence |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.requirement_type} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.missing_details))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_audit_trail_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAuditTrailRequirementsReport:
    """Extract audit trail requirement records from SourceBrief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _TYPE_ORDER.index(requirement.requirement_type),
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceAuditTrailRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_audit_trail_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAuditTrailRequirementsReport:
    """Compatibility alias for building an audit trail requirements report."""
    return build_source_audit_trail_requirements(source)


def generate_source_audit_trail_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAuditTrailRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_audit_trail_requirements(source)


def derive_source_audit_trail_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAuditTrailRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_audit_trail_requirements(source)


def summarize_source_audit_trail_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAuditTrailRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted audit trail requirements."""
    if isinstance(source_or_result, SourceAuditTrailRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_audit_trail_requirements(source_or_result).summary


def source_audit_trail_requirements_to_dict(
    report: SourceAuditTrailRequirementsReport,
) -> dict[str, Any]:
    """Serialize an audit trail requirements report to a plain dictionary."""
    return report.to_dict()


source_audit_trail_requirements_to_dict.__test__ = False


def source_audit_trail_requirements_to_dicts(
    requirements: (
        tuple[SourceAuditTrailRequirement, ...]
        | list[SourceAuditTrailRequirement]
        | SourceAuditTrailRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize audit trail requirement records to dictionaries."""
    if isinstance(requirements, SourceAuditTrailRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_audit_trail_requirements_to_dicts.__test__ = False


def source_audit_trail_requirements_to_markdown(
    report: SourceAuditTrailRequirementsReport,
) -> str:
    """Render an audit trail requirements report as Markdown."""
    return report.to_markdown()


source_audit_trail_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    requirement_type: SourceAuditTrailRequirementType
    missing_details: tuple[str, ...]
    confidence: SourceAuditTrailRequirementConfidence
    evidence: str


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
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                value = model.model_validate(source).model_dump(mode="python")
                payload = dict(value)
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    return None, _object_payload(source)


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
        for source_field, segment in _candidate_segments(payload):
            requirement_types = _requirement_types(segment, source_field)
            if not requirement_types:
                continue
            evidence = _evidence_snippet(source_field, segment)
            for requirement_type in requirement_types:
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        requirement_type=requirement_type,
                        missing_details=_missing_details(requirement_type, segment),
                        confidence=_confidence(requirement_type, segment, source_field),
                        evidence=evidence,
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceAuditTrailRequirement]:
    grouped: dict[tuple[str | None, SourceAuditTrailRequirementType], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.requirement_type), []).append(candidate)

    requirements: list[SourceAuditTrailRequirement] = []
    for (source_brief_id, requirement_type), items in grouped.items():
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        common_missing = set(items[0].missing_details)
        for item in items[1:]:
            common_missing.intersection_update(item.missing_details)
        requirements.append(
            SourceAuditTrailRequirement(
                source_brief_id=source_brief_id,
                requirement_type=requirement_type,
                missing_details=tuple(sorted(_dedupe(common_missing), key=str.casefold)),
                confidence=confidence,
                evidence=tuple(
                    sorted(_dedupe(item.evidence for item in items), key=lambda item: item.casefold())
                )[:5],
            )
        )
    return requirements


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "requirements",
        "constraints",
        "scope",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "compliance",
        "audit",
        "audit_trail",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            if _any_signal(key_text):
                values.append((child_field, key_text))
            _append_value(values, child_field, child)
            if _any_signal(key_text) and not isinstance(child, (Mapping, list, tuple, set)):
                if text := _optional_text(child):
                    values.extend(
                        (child_field, segment) for segment in _segments(f"{key_text}: {text}")
                    )
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        values.extend((source_field, segment) for segment in _segments(text))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for line in value.splitlines() or [value]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for sentence in parts:
            segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _requirement_types(
    text: str, source_field: str
) -> tuple[SourceAuditTrailRequirementType, ...]:
    if _NEGATED_SCOPE_RE.search(text):
        return ()
    field_text = source_field.replace("_", " ").replace("-", " ")
    types = [
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(text)
    ]
    for requirement_type in _TYPE_ORDER:
        if (
            requirement_type not in types
            and _TYPE_PATTERNS[requirement_type].search(field_text)
            and _audit_context(text, source_field)
        ):
            types.append(requirement_type)
    if not types or not _audit_context(text, source_field):
        return ()
    return tuple(_dedupe(types))


def _missing_details(
    requirement_type: SourceAuditTrailRequirementType, text: str
) -> tuple[str, ...]:
    missing = list(_MISSING_DETAILS[requirement_type])
    if _TYPE_PATTERNS["actor_attribution"].search(text):
        _remove(missing, "actor identifier source")
    if re.search(r"\b(?:service account|principal|system actor)\b", text, re.I):
        _remove(missing, "service account handling")
    if _TYPE_PATTERNS["timestamp_capture"].search(text):
        _remove(missing, "timestamp source")
    if re.search(r"\b(?:utc|timezone|time zone)\b", text, re.I):
        _remove(missing, "timezone standard")
    if _TYPE_PATTERNS["event_type_taxonomy"].search(text):
        _remove(missing, "event type taxonomy")
    if re.search(r"\b(?:payload schema|event schema|fields?|metadata|before and after)\b", text, re.I):
        _remove(missing, "event payload schema")
    if _TIME_WINDOW_RE.search(text):
        _remove(missing, "retention period")
    if re.search(r"\b(?:legal hold|purge|delete|deletion)\b", text, re.I):
        _remove(missing, "deletion or legal hold policy")
    if re.search(r"\b(?:immutable|append[- ]only|tamper[- ]proof|tamper evident|write once|worm)\b", text, re.I):
        _remove(missing, "immutability mechanism")
    if re.search(r"\b(?:privileged deletion|admin deletion|cannot be deleted|delete)\b", text, re.I):
        _remove(missing, "privileged deletion policy")
    if re.search(r"\b(?:csv|json|api|download|export)\b", text, re.I):
        _remove(missing, "export format")
    if re.search(r"\b(?:auditor|admin|permission|role|authorization|access control)\b", text, re.I):
        _remove(missing, "export authorization")
    if re.search(r"\b(?:download|email|api|webhook|delivery|retrieve)\b", text, re.I):
        _remove(missing, "export delivery path")
    return tuple(missing)


def _confidence(
    requirement_type: SourceAuditTrailRequirementType,
    text: str,
    source_field: str,
) -> SourceAuditTrailRequirementConfidence:
    structured_field = bool(_STRUCTURED_FIELD_RE.search(source_field))
    if _REQUIRED_RE.search(text) or (structured_field and _detail_present(requirement_type, text)):
        return "high"
    if structured_field:
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceAuditTrailRequirement, ...], source_count: int
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
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "requirement_types": [requirement.requirement_type for requirement in requirements],
    }


def _audit_context(text: str, source_field: str) -> bool:
    return bool(
        _AUDIT_CONTEXT_RE.search(text)
        or _STRUCTURED_FIELD_RE.search(source_field.replace("-", "_"))
    )


def _detail_present(requirement_type: SourceAuditTrailRequirementType, text: str) -> bool:
    return bool(
        _TYPE_PATTERNS[requirement_type].search(text)
        or _TIME_WINDOW_RE.search(text)
        or re.search(r"\b(?:csv|json|api|download|utc|append[- ]only|immutable)\b", text, re.I)
    )


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in _TYPE_PATTERNS.values()) or bool(
        _AUDIT_CONTEXT_RE.search(text)
    )


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "summary",
        "body",
        "description",
        "requirements",
        "constraints",
        "scope",
        "metadata",
        "brief_metadata",
        "source_payload",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "compliance",
        "audit",
        "audit_trail",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    text = _clean_text(value)
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    result: list[_T] = []
    seen: set[Any] = set()
    for value in values:
        key = value.casefold() if isinstance(value, str) else value
        if not value or key in seen:
            continue
        result.append(value)
        seen.add(key)
    return result


def _remove(items: list[str], value: str) -> None:
    if value in items:
        items.remove(value)


__all__ = [
    "SourceAuditTrailRequirement",
    "SourceAuditTrailRequirementConfidence",
    "SourceAuditTrailRequirementType",
    "SourceAuditTrailRequirementsReport",
    "build_source_audit_trail_requirements",
    "extract_source_audit_trail_requirements",
    "generate_source_audit_trail_requirements",
    "derive_source_audit_trail_requirements",
    "source_audit_trail_requirements_to_dict",
    "source_audit_trail_requirements_to_dicts",
    "source_audit_trail_requirements_to_markdown",
    "summarize_source_audit_trail_requirements",
]
