"""Extract source-level privileged access requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


PrivilegedAccessCategory = Literal[
    "approver_workflow",
    "time_bound_elevation",
    "audit_trail",
    "emergency_access",
    "routine_role_elevation",
    "revocation",
    "monitoring",
]
PrivilegedAccessConfidence = Literal["high", "medium", "low"]
PrivilegedAccessSeverity = Literal["critical", "high", "medium", "low"]

_CATEGORY_ORDER: tuple[PrivilegedAccessCategory, ...] = (
    "approver_workflow",
    "time_bound_elevation",
    "audit_trail",
    "emergency_access",
    "routine_role_elevation",
    "revocation",
    "monitoring",
)
_CONFIDENCE_ORDER: dict[PrivilegedAccessConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SEVERITY_ORDER: dict[PrivilegedAccessSeverity, int] = {"critical": 0, "high": 1, "medium": 2, "low": 3}
_SEVERITY_BY_CATEGORY: dict[PrivilegedAccessCategory, PrivilegedAccessSeverity] = {
    "approver_workflow": "high",
    "time_bound_elevation": "high",
    "audit_trail": "high",
    "emergency_access": "critical",
    "routine_role_elevation": "medium",
    "revocation": "high",
    "monitoring": "high",
}
_CAPABILITY_BY_CATEGORY: dict[PrivilegedAccessCategory, str] = {
    "approver_workflow": "Require explicit approval workflow and approver records before privileged access is granted.",
    "time_bound_elevation": "Limit privileged role elevation with duration, expiry, or just-in-time access windows.",
    "audit_trail": "Record privileged access grants, use, approvals, actors, reasons, and timestamps in an audit trail.",
    "emergency_access": "Define break-glass or emergency administration access for incident response with compensating controls.",
    "routine_role_elevation": "Support normal administrative role elevation separately from emergency access paths.",
    "revocation": "Revoke, expire, or disable privileged access when the window, incident, or approval ends.",
    "monitoring": "Monitor privileged sessions, risky admin actions, and elevated access events with alerting or review.",
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_PRIVILEGED_CONTEXT_RE = re.compile(
    r"\b(?:privileged access|privileged admin|admin elevation|elevated access|role elevation|"
    r"permission elevation|temporary admin|just[- ]?in[- ]?time|jit access|break[- ]?glass|"
    r"emergency access|emergency admin|emergency administrator|super admin|root access|"
    r"platform admin|support admin|operator access|privileged session|privileged action|"
    r"admin approval|approver workflow|access approval|access request|access grant|"
    r"least privilege|standing privilege|privilege escalation|privilege revocation|"
    r"admin audit|access audit|security audit|session recording|privileged monitoring|pam)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:privileged[_ -]?access|privilege|admin[_ -]?elevation|role[_ -]?elevation|"
    r"elevated[_ -]?access|break[_ -]?glass|emergency[_ -]?access|emergency[_ -]?admin|"
    r"approval|approver|just[_ -]?in[_ -]?time|jit|temporary[_ -]?admin|audit|monitor|"
    r"revocation|revoke|access[_ -]?request|access[_ -]?grant|security|authorization|"
    r"admin|source[_ -]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|should|needs?|need(?:s)? to|needed|required|requires?|requirement|"
    r"ensure|support|allow|provide|define|enforce|gate|approve|approval|request|grant|"
    r"elevate|elevation|expire|time[- ]?bound|temporary|just[- ]?in[- ]?time|jit|"
    r"break[- ]?glass|emergency|revoke|disable|remove|deactivate|log|audit|record|"
    r"monitor|alert|review|session|done when|acceptance|before launch)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:privileged access|admin elevation|role elevation|break[- ]?glass|emergency access|"
    r"temporary admin|jit access|approvals?|revocation|audit trails?|monitoring)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:privileged access|admin elevation|role elevation|break[- ]?glass|emergency access|"
    r"temporary admin|jit access|approvals?|revocation|audit trails?|monitoring)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"no changes?|non[- ]?goal)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:admin page copy|admin dashboard layout|admin analytics|administrator biography|"
    r"elevated button|high priority banner|monitor size|audit pricing|role description copy)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[PrivilegedAccessCategory, re.Pattern[str]] = {
    "approver_workflow": re.compile(
        r"\b(?:approver workflow|approval workflow|approval chain|manager approval|security approval|"
        r"two[- ]?person approval|dual approval|access request|request approval|approved by|"
        r"approval reason|approval evidence|approver evidence|business justification|ticket approval)\b",
        re.I,
    ),
    "time_bound_elevation": re.compile(
        r"\b(?:time[- ]?bound|temporary|expires?|expiry|expiration|ttl|duration|limited window|"
        r"just[- ]?in[- ]?time|jit access|elevation window|\d+\s*(?:minutes?|hours?|days?|weeks?)|"
        r"auto[- ]?expire|automatic expiry)\b",
        re.I,
    ),
    "audit_trail": re.compile(
        r"\b(?:audit trail|audit logs?|audit event|audited|logged|logging|record actor|"
        r"record timestamp|reason code|approval evidence|access evidence|admin action log|"
        r"who approved|who elevated|actor and timestamp)\b",
        re.I,
    ),
    "emergency_access": re.compile(
        r"\b(?:break[- ]?glass|emergency access|emergency admin|emergency administrator|"
        r"incident response access|production incident|sev[ -]?[01]|pager duty|pagerduty|"
        r"emergency override|crisis access|fallback admin|disaster recovery access)\b",
        re.I,
    ),
    "routine_role_elevation": re.compile(
        r"\b(?:routine role elevation|role elevation|admin elevation|elevated role|elevated access|"
        r"temporary admin|grant admin|grant privileged role|super admin role|platform admin role|"
        r"support admin role|operator role|privileged role|permission elevation)\b",
        re.I,
    ),
    "revocation": re.compile(
        r"\b(?:revocation|revoke|revoked|remove access|disable access|disabled access|deactivate|"
        r"access removal|drop privilege|end elevation|terminate session|kill session|auto[- ]?revoke)\b",
        re.I,
    ),
    "monitoring": re.compile(
        r"\b(?:monitoring|monitor|alert|alerts|alerting|security review|session review|"
        r"session recording|privileged session|risky admin actions?|anomaly detection|"
        r"siem|security operations|soc review|post[- ]?access review)\b",
        re.I,
    ),
}
_FIELD_CATEGORY_PATTERNS: dict[PrivilegedAccessCategory, re.Pattern[str]] = {
    "approver_workflow": re.compile(r"\b(?:approv|request|justification|ticket)\b", re.I),
    "time_bound_elevation": re.compile(r"\b(?:time|temporary|expiry|expiration|duration|jit|window)\b", re.I),
    "audit_trail": re.compile(r"\b(?:audit|log|evidence|timestamp)\b", re.I),
    "emergency_access": re.compile(r"\b(?:break glass|emergency|incident|sev|fallback)\b", re.I),
    "routine_role_elevation": re.compile(r"\b(?:role|elevation|admin|privilege|permission)\b", re.I),
    "revocation": re.compile(r"\b(?:revocation|revoke|remove|disable|deactivate|end)\b", re.I),
    "monitoring": re.compile(r"\b(?:monitor|alert|session|review|siem|soc)\b", re.I),
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
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "security",
    "authorization",
    "identity",
    "access_control",
    "privileged_access",
    "admin_access",
    "admin_elevation",
    "break_glass",
    "emergency_access",
    "audit",
    "monitoring",
    "metadata",
    "brief_metadata",
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
    "status",
}


@dataclass(frozen=True, slots=True)
class SourcePrivilegedAccessRequirement:
    """One source-backed privileged access requirement."""

    source_brief_id: str | None
    category: PrivilegedAccessCategory
    required_capability: str
    requirement_text: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: PrivilegedAccessConfidence = "medium"
    severity: PrivilegedAccessSeverity = "medium"
    source_field: str | None = None
    source_fields: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> PrivilegedAccessCategory:
        """Compatibility alias for callers expecting requirement_category naming."""
        return self.category

    @property
    def requirement_type(self) -> PrivilegedAccessCategory:
        """Compatibility alias for callers expecting requirement_type naming."""
        return self.category

    @property
    def access_dimension(self) -> PrivilegedAccessCategory:
        """Compatibility alias for callers expecting dimension naming."""
        return self.category

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "required_capability": self.required_capability,
            "requirement_text": self.requirement_text,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "severity": self.severity,
            "source_field": self.source_field,
            "source_fields": list(self.source_fields),
            "matched_terms": list(self.matched_terms),
        }


@dataclass(frozen=True, slots=True)
class SourcePrivilegedAccessRequirementsReport:
    """Source-level privileged access requirements report."""

    source_id: str | None = None
    requirements: tuple[SourcePrivilegedAccessRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourcePrivilegedAccessRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourcePrivilegedAccessRequirement, ...]:
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
        """Return privileged access requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Privileged Access Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        severity_counts = self.summary.get("severity_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
            "- Severity counts: "
            + ", ".join(f"{level} {severity_counts.get(level, 0)}" for level in _SEVERITY_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source privileged access requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Severity | Required Capability | Requirement | Confidence | Source Field | Source Fields | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.category)} | "
                f"{_markdown_cell(requirement.severity)} | "
                f"{_markdown_cell(requirement.required_capability)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(', '.join(requirement.source_fields))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_privileged_access_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePrivilegedAccessRequirementsReport:
    """Extract source-level privileged access requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourcePrivilegedAccessRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_privileged_access_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePrivilegedAccessRequirementsReport:
    """Compatibility alias for building a privileged access requirements report."""
    return build_source_privileged_access_requirements(source)


def generate_source_privileged_access_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePrivilegedAccessRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_privileged_access_requirements(source)


def derive_source_privileged_access_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourcePrivilegedAccessRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_privileged_access_requirements(source)


def summarize_source_privileged_access_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourcePrivilegedAccessRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted privileged access requirements."""
    if isinstance(source_or_result, SourcePrivilegedAccessRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_privileged_access_requirements(source_or_result).summary


def source_privileged_access_requirements_to_dict(
    report: SourcePrivilegedAccessRequirementsReport,
) -> dict[str, Any]:
    """Serialize a privileged access requirements report to a plain dictionary."""
    return report.to_dict()


source_privileged_access_requirements_to_dict.__test__ = False


def source_privileged_access_requirements_to_dicts(
    requirements: (
        tuple[SourcePrivilegedAccessRequirement, ...]
        | list[SourcePrivilegedAccessRequirement]
        | SourcePrivilegedAccessRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize privileged access requirement records to dictionaries."""
    if isinstance(requirements, SourcePrivilegedAccessRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_privileged_access_requirements_to_dicts.__test__ = False


def source_privileged_access_requirements_to_markdown(
    report: SourcePrivilegedAccessRequirementsReport,
) -> str:
    """Render a privileged access requirements report as Markdown."""
    return report.to_markdown()


source_privileged_access_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: PrivilegedAccessCategory
    requirement_text: str
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: PrivilegedAccessConfidence
    severity: PrivilegedAccessSeverity


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
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _candidates_for_briefs(brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        if _brief_out_of_scope(payload):
            continue
        for segment in _candidate_segments(payload):
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            if _NEGATED_RE.search(searchable) or _unrelated_only(searchable):
                continue
            for category in _categories(segment):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        requirement_text=_requirement_text(segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        matched_terms=tuple(_matched_terms(_CATEGORY_PATTERNS[category], searchable)),
                        confidence=_confidence(segment, category),
                        severity=_severity(category, segment.text),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourcePrivilegedAccessRequirement]:
    grouped: dict[tuple[str | None, PrivilegedAccessCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.category), []).append(candidate)

    requirements: list[SourcePrivilegedAccessRequirement] = []
    for (source_brief_id, category), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourcePrivilegedAccessRequirement(
                source_brief_id=source_brief_id,
                category=category,
                required_capability=_CAPABILITY_BY_CATEGORY[category],
                requirement_text=best.requirement_text,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                severity=min((item.severity for item in items), key=lambda value: _SEVERITY_ORDER[value]),
                source_field=best.source_field,
                source_fields=tuple(_dedupe(item.source_field for item in items)),
                matched_terms=tuple(
                    _dedupe(
                        term
                        for item in sorted(items, key=lambda candidate: candidate.source_field.casefold())
                        for term in item.matched_terms
                    )
                )[:8],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _CATEGORY_ORDER.index(requirement.category),
            _SEVERITY_ORDER[requirement.severity],
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.source_field or "",
            requirement.requirement_text.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    global_context = _brief_privileged_access_context(payload)
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name], global_context)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key], global_context)
    return segments


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_words = _field_words(source_field)
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(field_words))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            if str(key) in _IGNORED_FIELDS:
                continue
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _PRIVILEGED_CONTEXT_RE.search(key_text)
                or any(pattern.search(key_text) for pattern in _CATEGORY_PATTERNS.values())
            )
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
            section_context = inherited_context or bool(_PRIVILEGED_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            text = _clean_text(part)
            if text and not _NEGATED_RE.search(text):
                segments.append((text, section_context))
    return segments


def _categories(segment: _Segment) -> tuple[PrivilegedAccessCategory, ...]:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    field_words = _field_words(segment.source_field)
    if _unrelated_only(searchable):
        return ()
    has_context = bool(
        _PRIVILEGED_CONTEXT_RE.search(searchable)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(field_words)
    )
    if not has_context:
        return ()
    if not (
        _REQUIREMENT_RE.search(searchable)
        or segment.section_context
        or _STRUCTURED_FIELD_RE.search(field_words)
    ):
        return ()

    field_categories = [
        category
        for category in _CATEGORY_ORDER
        if _FIELD_CATEGORY_PATTERNS[category].search(field_words)
        and (_CATEGORY_PATTERNS[category].search(searchable) or _STRUCTURED_FIELD_RE.search(field_words))
    ]
    text_categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]
    return tuple(_dedupe([*field_categories, *text_categories]))


def _confidence(segment: _Segment, category: PrivilegedAccessCategory) -> PrivilegedAccessConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    has_requirement = bool(_REQUIREMENT_RE.search(searchable))
    has_structured_context = bool(
        segment.section_context
        or any(
            marker in segment.source_field.replace("-", "_").casefold()
            for marker in (
                "acceptance_criteria",
                "definition_of_done",
                "success_criteria",
                "scope",
                "security",
                "authorization",
                "access_control",
                "privileged_access",
                "admin_access",
                "break_glass",
                "emergency_access",
                "source_payload",
            )
        )
    )
    has_detail = _has_detail(category, segment.text)
    if _CATEGORY_PATTERNS[category].search(searchable) and has_requirement and (has_structured_context or has_detail):
        return "high"
    if has_requirement or has_structured_context or has_detail:
        return "medium"
    return "low"


def _has_detail(category: PrivilegedAccessCategory, text: str) -> bool:
    detail_patterns: dict[PrivilegedAccessCategory, re.Pattern[str]] = {
        "approver_workflow": re.compile(r"\b(?:manager|security|dual|two[- ]?person|ticket|justification|approved by)\b", re.I),
        "time_bound_elevation": re.compile(r"\b(?:minutes?|hours?|days?|weeks?|ttl|expiry|expires?|jit|window|temporary)\b", re.I),
        "audit_trail": re.compile(r"\b(?:actor|timestamp|reason|evidence|audit|log|event|who approved)\b", re.I),
        "emergency_access": re.compile(r"\b(?:break[- ]?glass|emergency|incident|sev|pager|override|fallback)\b", re.I),
        "routine_role_elevation": re.compile(r"\b(?:role|admin|operator|support|platform|temporary|grant)\b", re.I),
        "revocation": re.compile(r"\b(?:revoke|remove|disable|deactivate|expire|terminate|session)\b", re.I),
        "monitoring": re.compile(r"\b(?:monitor|alert|session|review|recording|siem|soc|risky)\b", re.I),
    }
    return bool(detail_patterns[category].search(text))


def _severity(category: PrivilegedAccessCategory, text: str) -> PrivilegedAccessSeverity:
    if category == "emergency_access" or re.search(r"\b(?:break[- ]?glass|emergency|sev[ -]?[01]|incident response)\b", text, re.I):
        return "critical"
    return _SEVERITY_BY_CATEGORY[category]


def _summary(requirements: tuple[SourcePrivilegedAccessRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "categories": [requirement.category for requirement in requirements],
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "severity_counts": {
            severity: sum(1 for requirement in requirements if requirement.severity == severity)
            for severity in _SEVERITY_ORDER
        },
        "status": "ready_for_privileged_access_planning" if requirements else "no_privileged_access_language",
    }


def _brief_out_of_scope(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("title", "summary", "scope", "non_goals", "constraints", "source_payload")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_NEGATED_RE.search(scoped_text))


def _brief_privileged_access_context(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("id", "source_id", "source_brief_id", "title", "domain", "summary", "workflow_context", "product_surface")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_PRIVILEGED_CONTEXT_RE.search(scoped_text) and not _NEGATED_RE.search(scoped_text))


def _unrelated_only(text: str) -> bool:
    return bool(_UNRELATED_RE.search(text) and not _PRIVILEGED_CONTEXT_RE.search(text))


def _requirement_text(text: str) -> str:
    return _clean_text(text)[:300]


def _evidence_snippet(source_field: str, text: str) -> str:
    return f"{source_field}: {_clean_text(text)[:240]}"


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, int, str]:
    return (
        3 - _SEVERITY_ORDER[candidate.severity],
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        len(candidate.matched_terms),
        int("acceptance_criteria" in candidate.source_field or "definition_of_done" in candidate.source_field),
        len(candidate.requirement_text),
        candidate.source_field,
    )


def _dedupe_text_key(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", text.casefold()).strip()


def _dedupe_evidence(items: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for item in items:
        if not item:
            continue
        _, _, statement = item.partition(": ")
        key = _dedupe_text_key(statement or item)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _matched_terms(pattern: re.Pattern[str], text: str) -> list[str]:
    return _dedupe(_clean_text(match.group(0)).casefold() for match in pattern.finditer(text))


def _field_words(source_field: str) -> str:
    return _clean_text(re.sub(r"[\[\]._-]+", " ", source_field))


def _object_payload(source: object) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for name in dir(source):
        if name.startswith("_"):
            continue
        try:
            value = getattr(source, name)
        except Exception:
            continue
        if callable(value):
            continue
        payload[name] = value
    return payload


def _strings(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return [text for key in sorted(value, key=lambda item: str(item)) for text in _strings(value[key])]
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        return [text for item in items for text in _strings(item)]
    if text := _optional_text(value):
        return [text]
    return []


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    if isinstance(value, bool):
        return "true" if value else None
    if isinstance(value, (str, int, float)):
        text = _clean_text(str(value))
        return text or None
    return None


def _clean_text(text: str) -> str:
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip(" -:\t")


def _dedupe(items: Iterable[Any]) -> list[Any]:
    seen: set[Any] = set()
    deduped: list[Any] = []
    for item in items:
        if item is None or item == "" or item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    return deduped


def _markdown_cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", "<br>")


__all__ = [
    "PrivilegedAccessCategory",
    "PrivilegedAccessConfidence",
    "PrivilegedAccessSeverity",
    "SourcePrivilegedAccessRequirement",
    "SourcePrivilegedAccessRequirementsReport",
    "build_source_privileged_access_requirements",
    "derive_source_privileged_access_requirements",
    "extract_source_privileged_access_requirements",
    "generate_source_privileged_access_requirements",
    "summarize_source_privileged_access_requirements",
    "source_privileged_access_requirements_to_dict",
    "source_privileged_access_requirements_to_dicts",
    "source_privileged_access_requirements_to_markdown",
]
