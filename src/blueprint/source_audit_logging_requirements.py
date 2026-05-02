"""Extract audit logging and audit trail requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceAuditLoggingRequirementType = Literal[
    "audit_trail",
    "admin_actions",
    "security_events",
    "audit_log_retention",
    "actor_action_resource_metadata",
    "actor_identity",
    "timestamping",
    "immutable_logs",
    "tamper_evidence",
    "exportable_audit_history",
    "compliance_review",
]
SourceAuditLoggingRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[SourceAuditLoggingRequirementType, ...] = (
    "audit_trail",
    "admin_actions",
    "security_events",
    "audit_log_retention",
    "actor_action_resource_metadata",
    "actor_identity",
    "timestamping",
    "immutable_logs",
    "tamper_evidence",
    "exportable_audit_history",
    "compliance_review",
)
_CONFIDENCE_ORDER: dict[SourceAuditLoggingRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"acceptance|done when|before launch|compliance|policy|cannot ship)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:audit|auditing|audit logging|audit logs?|security events?|"
    r"logging).*?\b(?:in scope|required|needed|changes?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:audit|logging|log|security|compliance|governance|admin|retention|"
    r"acceptance|requirements?|constraints?|controls?|history)",
    re.I,
)
_AUDIT_CONTEXT_RE = re.compile(
    r"\b(?:audit|auditing|auditability|audit trail|audit log|audit logs|activity log|"
    r"activity history|security log|security events?|event log|change history|"
    r"admin action|administrator action|compliance evidence)\b",
    re.I,
)
_TYPE_PATTERNS: dict[SourceAuditLoggingRequirementType, re.Pattern[str]] = {
    "audit_trail": re.compile(
        r"\b(?:audit trail|audit log(?:s)?|audit history|activity log(?:s)?|"
        r"activity history|change history|event log(?:s)?|record all changes|"
        r"track changes|auditability)\b",
        re.I,
    ),
    "admin_actions": re.compile(
        r"\b(?:admin(?:istrator)? action(?:s)?|admin changes?|privileged action(?:s)?|"
        r"role changes?|permission changes?|configuration changes?|settings changes?|"
        r"user management changes?|impersonation)\b",
        re.I,
    ),
    "security_events": re.compile(
        r"\b(?:security event(?:s)?|security log(?:s)?|login failures?|failed login(?:s)?|"
        r"successful login(?:s)?|authentication event(?:s)?|authorization event(?:s)?|"
        r"mfa change(?:s)?|password reset(?:s)?|suspicious activity|access denied)\b",
        re.I,
    ),
    "audit_log_retention": re.compile(
        r"\b(?:retain(?:ed|ing)? audit logs?|audit logs?.{0,40}\bretained|"
        r"audit log retention|keep audit logs?|preserve audit logs?|store audit logs?|"
        r"audit logs? for \d+|retention period)\b",
        re.I,
    ),
    "actor_action_resource_metadata": re.compile(
        r"\b(?:actor[,\s]+action[,\s]+(?:and\s+)?resource|actor/action/resource|"
        r"who did what to which|who did what|action metadata|resource metadata|"
        r"before and after values?|old and new values?|changed fields?|target resource|"
        r"affected resource|resource id|object id|entity id|ip address|user agent|"
        r"request id|correlation id)\b",
        re.I,
    ),
    "actor_identity": re.compile(
        r"\b(?:actor identity|who performed|who made|performed by|initiated by|"
        r"user id|user email|admin id|service account|principal|actor id|"
        r"originating user|capture actor|record actor)\b",
        re.I,
    ),
    "timestamping": re.compile(
        r"\b(?:timestamp(?:s|ed|ing)?|time stamped|time-stamped|occurred at|"
        r"event time|created at|recorded at|utc|timezone)\b",
        re.I,
    ),
    "immutable_logs": re.compile(
        r"\b(?:immutable audit logs?|tamper[- ]proof|tamper evident|append[- ]only|"
        r"cannot be edited|cannot be deleted|non[- ]repudiation|write once|worm storage|"
        r"audit logs?.{0,80}\b(?:immutable|append[- ]only|write once|worm storage))\b",
        re.I,
    ),
    "tamper_evidence": re.compile(
        r"\b(?:tamper[- ]evident|tamper evidence|tamper detection|tampering|"
        r"hash chain|signed audit events?|signature verification|integrity check|"
        r"integrity proof|detect audit log changes?)\b",
        re.I,
    ),
    "exportable_audit_history": re.compile(
        r"\b(?:export(?:able)? audit (?:history|logs?)|audit export|download audit logs?|"
        r"csv export|export history|auditor export|compliance export|export.*audit logs?|"
        r"retrieve audit history)\b",
        re.I,
    ),
    "compliance_review": re.compile(
        r"\b(?:compliance review|compliance audit|auditors? need reviewable|"
        r"auditors? need evidence|auditor review|audit review|"
        r"security review|sox review|soc ?2 review|iso ?27001 review|gdpr review|"
        r"reviewable by auditors?|auditor access|compliance evidence|evidence for auditors?|"
        r"regulatory review)\b",
        re.I,
    ),
}
_SUBJECT_RE = re.compile(
    r"\b(?:admin(?:istrator)?|security|audit|billing|invoice|payment|account|tenant|"
    r"workspace|organization|user|role|permission|configuration|settings|login|"
    r"authentication|authorization|mfa|password|data export|api key|service account)"
    r"(?:[- ](?:actions?|changes?|events?|history|logs?|records?|settings|users?|"
    r"roles?|permissions?|configuration|exports?|keys?|accounts?))*\b",
    re.I,
)
_TIME_WINDOW_RE = re.compile(
    r"\b(?:(?:for|within|after|at least|minimum of|no less than|up to)\s+)?"
    r"(?:\d+(?:\.\d+)?|one|two|three|four|five|six|seven|eight|nine|ten|"
    r"eleven|twelve|thirteen|thirty|sixty|ninety)\s+"
    r"(?:days?|weeks?|months?|quarters?|years?|hrs?|hours?)\b",
    re.I,
)
_IGNORED_FIELDS = {
    "id",
    "source_id",
    "source_brief_id",
    "source_project",
    "source_entity_type",
    "created_at",
    "updated_at",
}
_MISSING_DETAILS: dict[SourceAuditLoggingRequirementType, tuple[str, ...]] = {
    "audit_trail": ("event inventory", "actor identity", "timestamp source", "retention period"),
    "admin_actions": ("admin action inventory", "actor identity", "authorization context"),
    "security_events": ("security event inventory", "severity mapping", "alerting destination"),
    "audit_log_retention": ("retention period", "deletion policy", "legal hold behavior"),
    "actor_action_resource_metadata": (
        "actor identifier source",
        "action taxonomy",
        "resource identifier schema",
    ),
    "actor_identity": ("actor identifier source", "service account representation"),
    "timestamping": ("timestamp source", "timezone standard", "clock skew handling"),
    "immutable_logs": ("immutability mechanism", "privileged deletion policy"),
    "tamper_evidence": ("integrity proof mechanism", "verification workflow"),
    "exportable_audit_history": ("export format", "access control", "export retention"),
    "compliance_review": ("review cadence", "reviewer access", "evidence package format"),
}


@dataclass(frozen=True, slots=True)
class SourceAuditLoggingRequirement:
    """One source-backed audit logging requirement."""

    source_brief_id: str | None
    requirement_type: SourceAuditLoggingRequirementType
    subject_scope: str | None = None
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    confidence: SourceAuditLoggingRequirementConfidence = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirement_type": self.requirement_type,
            "subject_scope": self.subject_scope,
            "missing_details": list(self.missing_details),
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class SourceAuditLoggingRequirementsReport:
    """Source-level audit logging requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceAuditLoggingRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAuditLoggingRequirement, ...]:
        """Compatibility view matching extractors that name findings records."""
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
        """Return audit logging requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Audit Logging Requirements Report"
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
            + (", ".join(f"{key} {type_counts[key]}" for key in sorted(type_counts)) or "none"),
        ]
        if not self.requirements:
            lines.extend(["", "No audit logging requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Type | Scope | Confidence | Missing Details | Evidence |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell(requirement.subject_scope or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.missing_details))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_audit_logging_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAuditLoggingRequirementsReport:
    """Extract audit logging requirement records from SourceBrief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _type_index(requirement.requirement_type),
                requirement.subject_scope or "",
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceAuditLoggingRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_audit_logging_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAuditLoggingRequirementsReport:
    """Compatibility alias for building an audit logging requirements report."""
    return build_source_audit_logging_requirements(source)


def generate_source_audit_logging_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAuditLoggingRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_audit_logging_requirements(source)


def derive_source_audit_logging_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAuditLoggingRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_audit_logging_requirements(source)


def summarize_source_audit_logging_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAuditLoggingRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted audit logging requirements."""
    if isinstance(source_or_result, SourceAuditLoggingRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_audit_logging_requirements(source_or_result).summary


def source_audit_logging_requirements_to_dict(
    report: SourceAuditLoggingRequirementsReport,
) -> dict[str, Any]:
    """Serialize an audit logging requirements report to a plain dictionary."""
    return report.to_dict()


source_audit_logging_requirements_to_dict.__test__ = False


def source_audit_logging_requirements_to_dicts(
    requirements: (
        tuple[SourceAuditLoggingRequirement, ...]
        | list[SourceAuditLoggingRequirement]
        | SourceAuditLoggingRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize audit logging requirement records to dictionaries."""
    if isinstance(requirements, SourceAuditLoggingRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_audit_logging_requirements_to_dicts.__test__ = False


def source_audit_logging_requirements_to_markdown(
    report: SourceAuditLoggingRequirementsReport,
) -> str:
    """Render an audit logging requirements report as Markdown."""
    return report.to_markdown()


source_audit_logging_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    requirement_type: SourceAuditLoggingRequirementType
    subject_scope: str | None
    missing_details: tuple[str, ...]
    confidence: SourceAuditLoggingRequirementConfidence
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
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object
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
                        subject_scope=_subject_scope(segment, source_field),
                        missing_details=_missing_details(requirement_type, segment),
                        confidence=_confidence(requirement_type, segment, source_field),
                        evidence=evidence,
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceAuditLoggingRequirement]:
    grouped: dict[tuple[str | None, SourceAuditLoggingRequirementType, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (
                candidate.source_brief_id,
                candidate.requirement_type,
                _dedupe_key(candidate.subject_scope),
            ),
            [],
        ).append(candidate)

    requirements: list[SourceAuditLoggingRequirement] = []
    for (source_brief_id, requirement_type, _), items in grouped.items():
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        subject_scope = next((item.subject_scope for item in items if item.subject_scope), None)
        common_missing = set(items[0].missing_details)
        for item in items[1:]:
            common_missing.intersection_update(item.missing_details)
        requirements.append(
            SourceAuditLoggingRequirement(
                source_brief_id=source_brief_id,
                requirement_type=requirement_type,
                subject_scope=subject_scope,
                missing_details=tuple(
                    sorted(
                        _dedupe(common_missing),
                        key=str.casefold,
                    )
                ),
                confidence=confidence,
                evidence=tuple(
                    sorted(
                        _dedupe(item.evidence for item in items),
                        key=lambda item: item.casefold(),
                    )
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
        "risks",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "security",
        "compliance",
        "audit",
        "logging",
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
) -> tuple[SourceAuditLoggingRequirementType, ...]:
    if _NEGATED_SCOPE_RE.search(text):
        return ()
    types = [
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(text)
    ]
    field_text = source_field.replace("_", " ").replace("-", " ")
    for requirement_type in _TYPE_ORDER:
        if (
            requirement_type not in types
            and _TYPE_PATTERNS[requirement_type].search(field_text)
            and _audit_context(text, source_field)
        ):
            types.append(requirement_type)
    if types and "audit_trail" not in types and _AUDIT_CONTEXT_RE.search(text):
        types.insert(0, "audit_trail")
    if (
        "exportable_audit_history" in types
        and "audit_trail" in types
        and not re.search(
            r"\b(?:audit trail|activity log(?:s)?|activity history|change history|"
            r"record all changes|track changes|auditability|"
            r"audit logs?.{0,40}\b(?:track|record))\b",
            text,
            re.I,
        )
    ):
        types.remove("audit_trail")
    if "audit_trail" in types and not re.search(
        r"\b(?:audit trail|activity log(?:s)?|activity history|change history|"
        r"record all changes|track changes|auditability|"
        r"audit logs?.{0,80}\b(?:track|record|include|capture)|"
        r"\b(?:track|record|include|capture)\b.{0,80}\baudit logs?)\b",
        text,
        re.I,
    ):
        types.remove("audit_trail")
    if "audit_log_retention" in types and not (
        _TIME_WINDOW_RE.search(text)
        or re.search(r"\b(?:retain|retention|keep|preserve|store)\b", text, re.I)
        or re.search(r"\bretention\b", field_text, re.I)
    ):
        types.remove("audit_log_retention")
    if "actor_action_resource_metadata" in types and not _audit_context(text, source_field):
        types.remove("actor_action_resource_metadata")
    if "actor_identity" in types and not _audit_context(text, source_field):
        types.remove("actor_identity")
    if "timestamping" in types and not _audit_context(text, source_field):
        types.remove("timestamping")
    if "tamper_evidence" in types and not _audit_context(text, source_field):
        types.remove("tamper_evidence")
    if "compliance_review" in types and not _audit_context(text, source_field):
        types.remove("compliance_review")
    return tuple(_dedupe(types))


def _subject_scope(text: str, source_field: str) -> str | None:
    generic_scopes = {
        "audit log",
        "audit logs",
        "audit history",
        "activity log",
        "activity logs",
        "activity history",
        "event log",
        "event logs",
    }
    matches = [
        _clean_scope(match.group(0))
        for match in _SUBJECT_RE.finditer(text)
        if _clean_scope(match.group(0)) not in {"audit", "security", *generic_scopes}
    ]
    if matches:
        return _dedupe(matches)[0]
    field_tail = source_field.rsplit(".", 1)[-1].replace("_", " ").replace("-", " ")
    field_matches = [
        _clean_scope(match.group(0))
        for match in _SUBJECT_RE.finditer(field_tail)
        if _clean_scope(match.group(0)) not in {"audit", "security"}
    ]
    return _dedupe(field_matches)[0] if field_matches else None


def _missing_details(
    requirement_type: SourceAuditLoggingRequirementType, text: str
) -> tuple[str, ...]:
    missing = list(_MISSING_DETAILS[requirement_type])
    if _actor_present(text):
        _remove(missing, "actor identity")
        _remove(missing, "actor identifier source")
    if re.search(
        r"\b(?:action|change|event|operation|changed fields?|before and after)\b", text, re.I
    ):
        _remove(missing, "action taxonomy")
    if re.search(
        r"\b(?:resource|object|entity|target|workspace|tenant|account|user|role|permission)\b",
        text,
        re.I,
    ):
        _remove(missing, "resource identifier schema")
    if _timestamp_present(text):
        _remove(missing, "timestamp source")
        _remove(missing, "timezone standard")
    if _TIME_WINDOW_RE.search(text):
        _remove(missing, "retention period")
    if re.search(r"\b(?:csv|json|api|download|export)\b", text, re.I):
        _remove(missing, "export format")
    if re.search(
        r"\b(?:append[- ]only|immutable|tamper[- ]proof|tamper evident|write once)\b", text, re.I
    ):
        _remove(missing, "immutability mechanism")
        _remove(missing, "integrity proof mechanism")
    if re.search(r"\b(?:review|auditor|compliance|evidence package)\b", text, re.I):
        _remove(missing, "reviewer access")
    return tuple(missing)


def _confidence(
    requirement_type: SourceAuditLoggingRequirementType,
    text: str,
    source_field: str,
) -> SourceAuditLoggingRequirementConfidence:
    structured_field = bool(_STRUCTURED_FIELD_RE.search(source_field))
    has_detail = bool(
        _actor_present(text) or _timestamp_present(text) or _TIME_WINDOW_RE.search(text)
    )
    if _REQUIRED_RE.search(text) or (structured_field and has_detail):
        return "high"
    if structured_field or requirement_type in {
        "actor_action_resource_metadata",
        "actor_identity",
        "timestamping",
        "immutable_logs",
        "tamper_evidence",
        "exportable_audit_history",
        "compliance_review",
    }:
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceAuditLoggingRequirement, ...], source_count: int
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


def _actor_present(text: str) -> bool:
    return bool(_TYPE_PATTERNS["actor_identity"].search(text))


def _timestamp_present(text: str) -> bool:
    return bool(_TYPE_PATTERNS["timestamping"].search(text))


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in _TYPE_PATTERNS.values())


def _type_index(requirement_type: SourceAuditLoggingRequirementType) -> int:
    return _TYPE_ORDER.index(requirement_type)


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
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
        "source_links",
        "acceptance_criteria",
        "implementation_notes",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "definition_of_done",
        "validation_plan",
        "security",
        "compliance",
        "audit",
        "logging",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _clean_scope(text: str) -> str:
    return _SPACE_RE.sub(" ", text.strip(" .,:;")).casefold()


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


def _dedupe_key(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", _clean_text(value).casefold()).strip()


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
    "SourceAuditLoggingRequirement",
    "SourceAuditLoggingRequirementConfidence",
    "SourceAuditLoggingRequirementType",
    "SourceAuditLoggingRequirementsReport",
    "build_source_audit_logging_requirements",
    "extract_source_audit_logging_requirements",
    "generate_source_audit_logging_requirements",
    "derive_source_audit_logging_requirements",
    "source_audit_logging_requirements_to_dict",
    "source_audit_logging_requirements_to_dicts",
    "source_audit_logging_requirements_to_markdown",
    "summarize_source_audit_logging_requirements",
]
