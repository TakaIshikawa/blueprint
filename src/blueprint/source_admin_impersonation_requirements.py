"""Extract admin impersonation and support-access requirements from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceAdminImpersonationRequirementType = Literal[
    "eligibility",
    "consent_or_approval",
    "scoped_permissions",
    "session_duration",
    "audit_logging",
    "customer_visibility",
    "break_glass_controls",
    "revocation",
]
SourceAdminImpersonationRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[SourceAdminImpersonationRequirementType, ...] = (
    "eligibility",
    "consent_or_approval",
    "scoped_permissions",
    "session_duration",
    "audit_logging",
    "customer_visibility",
    "break_glass_controls",
    "revocation",
)
_CONFIDENCE_ORDER: dict[SourceAdminImpersonationRequirementConfidence, int] = {
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
    r"\b(?:no|not|without)\s+(?:impersonation|admin access|support access|"
    r"customer impersonation).*?\b(?:in scope|required|needed|changes?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:impersonation|admin|support|access|customer|consent|approval|eligibility|"
    r"permissions?|session|duration|audit|visibility|break[- ]glass|revocation|"
    r"acceptance|requirements?|constraints?|controls?)",
    re.I,
)
_IMPERSONATION_CONTEXT_RE = re.compile(
    r"\b(?:impersonation|impersonate|admin access|support access|customer access|"
    r"act as customer|login as|access as|support login|support session|"
    r"admin support|customer support access|troubleshooting access)\b",
    re.I,
)
_TYPE_PATTERNS: dict[SourceAdminImpersonationRequirementType, re.Pattern[str]] = {
    "eligibility": re.compile(
        r"\b(?:eligibility|eligible|authorized|authorized (?:staff|admin|support)|"
        r"role[- ]based access|permission to impersonate|who can impersonate|"
        r"allowed to impersonate|support staff only|admin only|tier \d+ support|"
        r"impersonation rights?|access rights?)\b",
        re.I,
    ),
    "consent_or_approval": re.compile(
        r"\b(?:consent|customer consent|user consent|approval|customer approval|"
        r"user approval|permission from customer|customer permission|"
        r"request approval|approval workflow|approval required|consent required|"
        r"opt[- ]in|customer opt[- ]in|explicit consent|explicit approval)\b",
        re.I,
    ),
    "scoped_permissions": re.compile(
        r"\b(?:scoped permissions?|limited permissions?|restricted permissions?|"
        r"access scope|permission scope|minimal permissions?|read[- ]only|"
        r"view[- ]only|restricted access|limited access|scope restrictions?|"
        r"permission restrictions?|least privilege|minimal scope)\b",
        re.I,
    ),
    "session_duration": re.compile(
        r"\b(?:session duration|session time|time limit|time[- ]boxed|"
        r"session expiry|session timeout|session expires?|duration limit|"
        r"limited duration|temporary access|timed session|session length|"
        r"max(?:imum)? duration|(?:for|up to|maximum of|no more than)\s+\d+\s+(?:minutes?|hours?|days?))\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit log(?:s|ging)?|audit trail|activity log|access log|"
        r"impersonation log|log impersonation|record impersonation|"
        r"track impersonation|log support access|record support access|"
        r"capture impersonation|log actor|log customer|impersonation event(?:s)?)\b",
        re.I,
    ),
    "customer_visibility": re.compile(
        r"\b(?:customer visibility|customer aware|customer (?:must be )?notified|"
        r"notify customer|user visibility|user aware|user notified|visible to (?:the )?customer|"
        r"visible to (?:the )?user|notification to customer|alert customer|inform customer|"
        r"customer sees?|user sees?|transparency|transparent access|"
        r"customer can see|user can see)\b",
        re.I,
    ),
    "break_glass_controls": re.compile(
        r"\b(?:break[- ]glass|emergency access|urgent access|critical access|"
        r"emergency impersonation|override|emergency override|"
        r"incident response|production incident|urgent troubleshooting|"
        r"emergency procedures?|escalation path)\b",
        re.I,
    ),
    "revocation": re.compile(
        r"\b(?:revoke|revocation|terminate session|end session|terminate access|"
        r"end access|terminate impersonation|end impersonation|stop impersonation|"
        r"session termination|termination events?|access termination|force logout|forced logout|"
        r"kill session|abort session)\b",
        re.I,
    ),
}
_SUBJECT_RE = re.compile(
    r"\b(?:admin(?:istrator)?|support|customer|user|account|tenant|workspace|"
    r"organization|troubleshooting|incident|emergency|session|access|"
    r"impersonation|permissions?)"
    r"(?:[- ](?:access|session|impersonation|support|staff|team|role|rights?|"
    r"permissions?|controls?|logs?))?\b",
    re.I,
)
_TIME_WINDOW_RE = re.compile(
    r"\b(?:(?:for|within|after|at least|minimum of|no less than|up to|maximum of|max)\s+)?"
    r"(?:\d+(?:\.\d+)?|one|two|three|four|five|six|seven|eight|nine|ten|"
    r"fifteen|twenty|thirty|sixty)\s+"
    r"(?:seconds?|mins?|minutes?|hrs?|hours?|days?)\b",
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
_MISSING_DETAILS: dict[SourceAdminImpersonationRequirementType, tuple[str, ...]] = {
    "eligibility": ("role criteria", "department restrictions", "authorization mechanism"),
    "consent_or_approval": ("approval workflow", "consent capture mechanism", "consent storage"),
    "scoped_permissions": ("permission enumeration", "scope enforcement mechanism", "permission deny list"),
    "session_duration": ("duration value", "timeout mechanism", "duration enforcement"),
    "audit_logging": ("log fields", "log retention", "log destination"),
    "customer_visibility": ("notification mechanism", "visibility scope", "transparency UI"),
    "break_glass_controls": ("emergency criteria", "approval override", "post-incident review"),
    "revocation": ("revocation trigger", "termination mechanism", "revocation verification"),
}


@dataclass(frozen=True, slots=True)
class SourceAdminImpersonationRequirement:
    """One source-backed admin impersonation requirement."""

    source_brief_id: str | None
    requirement_type: SourceAdminImpersonationRequirementType
    subject_scope: str | None = None
    missing_details: tuple[str, ...] = field(default_factory=tuple)
    confidence: SourceAdminImpersonationRequirementConfidence = "medium"
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
class SourceAdminImpersonationRequirementsReport:
    """Source-level admin impersonation requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceAdminImpersonationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAdminImpersonationRequirement, ...]:
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
        """Return admin impersonation requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Admin Impersonation Requirements Report"
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
            lines.extend(["", "No admin impersonation requirements were found in the source brief."])
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


def build_source_admin_impersonation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAdminImpersonationRequirementsReport:
    """Extract admin impersonation requirement records from SourceBrief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _TYPE_ORDER.index(requirement.requirement_type),
                _CONFIDENCE_ORDER[requirement.confidence],
                _optional_text(requirement.subject_scope) or "",
            ),
        )
    )
    return SourceAdminImpersonationRequirementsReport(
        source_id=_source_id(brief_payloads),
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_admin_impersonation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAdminImpersonationRequirementsReport:
    """Extract admin impersonation requirements from source briefs (alias)."""
    return build_source_admin_impersonation_requirements(source)


def derive_source_admin_impersonation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAdminImpersonationRequirementsReport:
    """Derive admin impersonation requirements from source briefs (alias)."""
    return build_source_admin_impersonation_requirements(source)


def generate_source_admin_impersonation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceAdminImpersonationRequirementsReport:
    """Generate admin impersonation requirements from source briefs (alias)."""
    return build_source_admin_impersonation_requirements(source)


def summarize_source_admin_impersonation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
        | SourceAdminImpersonationRequirementsReport
    ),
) -> SourceAdminImpersonationRequirementsReport:
    """Summarize admin impersonation requirements (alias)."""
    if isinstance(source, SourceAdminImpersonationRequirementsReport):
        return source
    return build_source_admin_impersonation_requirements(source)


def source_admin_impersonation_requirements_to_dict(
    report: SourceAdminImpersonationRequirementsReport,
) -> dict[str, Any]:
    """Serialize admin impersonation requirements report to a plain dictionary."""
    return report.to_dict()


source_admin_impersonation_requirements_to_dict.__test__ = False


def source_admin_impersonation_requirements_to_dicts(
    report: SourceAdminImpersonationRequirementsReport | Iterable[SourceAdminImpersonationRequirement],
) -> list[dict[str, Any]]:
    """Serialize admin impersonation requirement records to plain dictionaries."""
    if isinstance(report, SourceAdminImpersonationRequirementsReport):
        return report.to_dicts()
    return [requirement.to_dict() for requirement in report]


source_admin_impersonation_requirements_to_dicts.__test__ = False


def source_admin_impersonation_requirements_to_markdown(
    report: SourceAdminImpersonationRequirementsReport,
) -> str:
    """Render admin impersonation requirements report as Markdown."""
    return report.to_markdown()


source_admin_impersonation_requirements_to_markdown.__test__ = False


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> list[dict[str, Any]]:
    if isinstance(source, str):
        text = _optional_text(source)
        if not text:
            return []
        return [{"id": "text-brief", "content": text}]
    if isinstance(source, SourceBrief):
        return [_brief_payload(source)]
    if isinstance(source, ImplementationBrief):
        return [_brief_payload(source)]
    if isinstance(source, Mapping):
        return [_brief_payload(source)]
    if hasattr(source, "__iter__") and not isinstance(source, (str, bytes, bytearray)):
        payloads: list[dict[str, Any]] = []
        for item in source:
            if isinstance(item, (SourceBrief, ImplementationBrief)):
                payloads.append(_brief_payload(item))
            elif isinstance(item, Mapping):
                payloads.append(_brief_payload(item))
        return payloads
    if hasattr(source, "id") or hasattr(source, "source_payload"):
        return [_brief_payload(source)]
    return []


def _brief_payload(brief: SourceBrief | ImplementationBrief | Mapping[str, Any] | object) -> dict[str, Any]:
    if isinstance(brief, (SourceBrief, ImplementationBrief)):
        return brief.model_dump(mode="python")
    if hasattr(brief, "model_dump"):
        value = brief.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    if isinstance(brief, Mapping):
        try:
            validated = SourceBrief.model_validate(brief)
            return validated.model_dump(mode="python")
        except (TypeError, ValueError, ValidationError):
            pass
        try:
            validated = ImplementationBrief.model_validate(brief)
            return validated.model_dump(mode="python")
        except (TypeError, ValueError, ValidationError):
            pass
        return dict(brief)
    payload: dict[str, Any] = {}
    for field in ("id", "title", "summary", "description", "scope", "source_payload", "metadata"):
        if hasattr(brief, field):
            payload[field] = getattr(brief, field)
    return payload


def _candidates_for_briefs(
    brief_payloads: list[dict[str, Any]],
) -> list[SourceAdminImpersonationRequirement]:
    candidates: list[SourceAdminImpersonationRequirement] = []
    for brief in brief_payloads:
        brief_id = _optional_text(brief.get("id"))
        if _NEGATED_SCOPE_RE.search(_searchable_text(brief)):
            continue
        for source_field, segment in _candidate_segments(brief):
            requirement_types = _requirement_types(segment, source_field)
            if not requirement_types:
                continue
            evidence_source = source_field
            for requirement_type in requirement_types:
                subject = _extract_subject(segment)
                missing = _extract_missing_details(requirement_type, segment)
                confidence = _assess_confidence(segment, requirement_type, evidence_source)
                evidence = _build_evidence(evidence_source, segment)
                candidates.append(
                    SourceAdminImpersonationRequirement(
                        source_brief_id=brief_id,
                        requirement_type=requirement_type,
                        subject_scope=subject,
                        missing_details=missing,
                        confidence=confidence,
                        evidence=evidence,
                    )
                )
    return candidates


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    """Extract all text segments with their source field paths."""
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    # Process priority fields first
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "scope",
        "requirements",
        "constraints",
        "risks",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "impersonation",
        "admin",
        "support",
        "access",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    # Process remaining fields
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    """Recursively append text segments from nested structures."""
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            if _any_impersonation_signal(key_text):
                values.append((child_field, key_text))
            _append_value(values, child_field, child)
            if _any_impersonation_signal(key_text) and not isinstance(child, (Mapping, list, tuple, set)):
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
    """Split text into individual sentences and clauses."""
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
) -> tuple[SourceAdminImpersonationRequirementType, ...]:
    """Determine which requirement types match the text segment."""
    if _NEGATED_SCOPE_RE.search(text):
        return ()
    types: list[SourceAdminImpersonationRequirementType] = [
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(text)
    ]
    # Check if field name matches pattern even if text doesn't
    field_text = source_field.replace("_", " ").replace("-", " ")
    for requirement_type in _TYPE_ORDER:
        if (
            requirement_type not in types
            and _TYPE_PATTERNS[requirement_type].search(field_text)
            and _impersonation_context(text, source_field)
        ):
            types.append(requirement_type)
    return tuple(types)


def _any_impersonation_signal(text: str) -> bool:
    """Check if text contains any impersonation-related signal."""
    return bool(_STRUCTURED_FIELD_RE.search(text) or _IMPERSONATION_CONTEXT_RE.search(text))


def _impersonation_context(text: str, source_field: str) -> bool:
    """Check if text has impersonation context."""
    return bool(
        _IMPERSONATION_CONTEXT_RE.search(text)
        or _STRUCTURED_FIELD_RE.search(source_field)
        or "impersonat" in text.lower()
        or "support access" in text.lower()
        or "admin access" in text.lower()
    )


def _clean_text(value: str) -> str:
    """Clean and normalize text."""
    return _SPACE_RE.sub(" ", value).strip()


def _extract_subject(snippet: str) -> str | None:
    match = _SUBJECT_RE.search(snippet)
    if match:
        subject = _optional_text(match.group(0))
        return subject[:100] if subject else None
    return None


def _extract_missing_details(
    requirement_type: SourceAdminImpersonationRequirementType,
    snippet: str,
) -> tuple[str, ...]:
    all_details = _MISSING_DETAILS.get(requirement_type, ())
    missing: list[str] = []
    for detail in all_details:
        normalized_detail = detail.replace(" ", "").replace("-", "").lower()
        normalized_snippet = snippet.replace(" ", "").replace("-", "").lower()
        if normalized_detail not in normalized_snippet:
            missing.append(detail)
    return tuple(missing)


def _assess_confidence(
    snippet: str,
    requirement_type: SourceAdminImpersonationRequirementType,
    evidence_source: str,
) -> SourceAdminImpersonationRequirementConfidence:
    if evidence_source.startswith("source_payload"):
        return "high"
    if _REQUIRED_RE.search(snippet):
        return "high"
    pattern = _TYPE_PATTERNS[requirement_type]
    matches = pattern.findall(snippet)
    if len(matches) >= 2:
        return "medium"
    return "medium"


def _build_evidence(evidence_source: str, snippet: str) -> tuple[str, ...]:
    truncated = snippet[:180] + ("..." if len(snippet) > 180 else "")
    return (f"{evidence_source}: {truncated}",)


def _dedupe_key(value: str | None) -> str:
    """Normalize subject scope for deduplication."""
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", " ", _clean_text(value).casefold()).strip()


def _merge_candidates(
    candidates: list[SourceAdminImpersonationRequirement],
) -> list[SourceAdminImpersonationRequirement]:
    """Merge duplicate candidates with the same brief_id, requirement_type, and similar subject_scope."""
    grouped: dict[tuple[str | None, SourceAdminImpersonationRequirementType, str], list[SourceAdminImpersonationRequirement]] = {}
    for candidate in candidates:
        key = (
            candidate.source_brief_id,
            candidate.requirement_type,
            _dedupe_key(candidate.subject_scope),
        )
        grouped.setdefault(key, []).append(candidate)

    requirements: list[SourceAdminImpersonationRequirement] = []
    for (source_brief_id, requirement_type, _), items in grouped.items():
        # Pick the highest confidence level
        confidence: SourceAdminImpersonationRequirementConfidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        # Pick the first non-empty subject scope
        subject_scope = next((item.subject_scope for item in items if item.subject_scope), None)
        # Find common missing details (intersection)
        common_missing = set(items[0].missing_details)
        for item in items[1:]:
            common_missing.intersection_update(item.missing_details)
        # Flatten and dedupe all evidence
        all_evidence: list[str] = []
        for item in items:
            all_evidence.extend(item.evidence)
        requirements.append(
            SourceAdminImpersonationRequirement(
                source_brief_id=source_brief_id,
                requirement_type=requirement_type,
                subject_scope=subject_scope,
                missing_details=tuple(
                    sorted(
                        common_missing,
                        key=str.casefold,
                    )
                ),
                confidence=confidence,
                evidence=tuple(
                    sorted(
                        _dedupe(all_evidence),
                        key=str.casefold,
                    )
                )[:5],
            )
        )
    return requirements


def _source_id(brief_payloads: list[dict[str, Any]]) -> str | None:
    if len(brief_payloads) == 1:
        return _optional_text(brief_payloads[0].get("id"))
    return None


def _summary(requirements: tuple[SourceAdminImpersonationRequirement, ...]) -> dict[str, Any]:
    type_counts: dict[str, int] = {}
    confidence_counts: dict[str, int] = {"high": 0, "medium": 0, "low": 0}
    for requirement in requirements:
        type_counts[requirement.requirement_type] = type_counts.get(requirement.requirement_type, 0) + 1
        confidence_counts[requirement.confidence] += 1
    return {
        "requirement_count": len(requirements),
        "type_counts": type_counts,
        "confidence_counts": confidence_counts,
    }


def _searchable_text(brief: dict[str, Any]) -> str:
    """Extract all searchable text from a brief for negation checking."""
    parts: list[str] = []
    for _, segment in _candidate_segments(brief):
        parts.append(segment)
    return " ".join(parts)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "SourceAdminImpersonationRequirement",
    "SourceAdminImpersonationRequirementConfidence",
    "SourceAdminImpersonationRequirementType",
    "SourceAdminImpersonationRequirementsReport",
    "build_source_admin_impersonation_requirements",
    "derive_source_admin_impersonation_requirements",
    "extract_source_admin_impersonation_requirements",
    "generate_source_admin_impersonation_requirements",
    "source_admin_impersonation_requirements_to_dict",
    "source_admin_impersonation_requirements_to_dicts",
    "source_admin_impersonation_requirements_to_markdown",
    "summarize_source_admin_impersonation_requirements",
]
