"""Extract source-level seat and license management requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SeatManagementCategory = Literal[
    "seat_allocation",
    "invite_acceptance",
    "role_license_assignment",
    "overage_handling",
    "seat_limits",
    "deprovisioning",
    "reassignment",
    "billing_proration",
    "admin_audit_evidence",
    "notifications",
]
SeatManagementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[SeatManagementCategory, ...] = (
    "seat_allocation",
    "invite_acceptance",
    "role_license_assignment",
    "overage_handling",
    "seat_limits",
    "deprovisioning",
    "reassignment",
    "billing_proration",
    "admin_audit_evidence",
    "notifications",
)
_CONFIDENCE_ORDER: dict[SeatManagementConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SEAT_CONTEXT_RE = re.compile(
    r"\b(?:seat management|license management|licence management|seat allocation|allocate seats?|"
    r"assigned seats?|seat assignment|license assignment|licence assignment|paid seats?|user seats?|"
    r"team seats?|workspace seats?|seat pool|seat inventory|seat count|seat cap|seat limits?|license limits?|"
    r"invite acceptance|accepted invites?|accept invitations?|pending invites?|member invites?|"
    r"roles?|permissions?|admin|owner|member|viewer|license tier|licensed role|"
    r"overage|overages|extra seats?|additional seats?|true[- ]?up|threshold|hard cap|soft cap|"
    r"deprovision(?:ing)?|deactivate|remove users?|remove members?|offboard|suspend|terminated users?|"
    r"reassign|transfer seats?|seat transfer|reclaim seats?|reuse seats?|"
    r"proration|prorated|pro[- ]?rate|billing adjustment|billing credit|billing period|"
    r"audit evidence|audit log|admin audit|seat event|license event|evidence|"
    r"notification|notifications|notify|email|in[- ]?app|slack|webhook)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:seat|seats|license|licence|allocation|assignment|invite|acceptance|role|permission|"
    r"overage|threshold|limit|cap|deprovision|offboard|deactivate|remove|reassign|transfer|"
    r"proration|prorated|billing|audit|evidence|notification|notify|requirements?|"
    r"acceptance|criteria|definition[-_ ]?of[-_ ]?done|metadata|source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|send|email|assign|allocate|reserve|consume|accept|invite|"
    r"cap|limit|enforce|block|charge|bill|invoice|prorate|credit|deprovision|deactivate|"
    r"remove|releases?|reassign|transfer|"
    r"audit|log|record|track|notify|review|approve|escalate|acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:seat management|license management|seat allocation|seat limits?|overages?|"
    r"deprovisioning|reassignment|proration|seat notifications?|seat audit)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:seat management|license management|seat allocation|seat limits?|overages?|"
    r"deprovisioning|reassignment|proration|seat notifications?|seat audit)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_ACCOUNT_RE = re.compile(
    r"\b(?:no seat management|no license management|seat management is out of scope|"
    r"license management is out of scope|without paid seats|no paid seats|single[- ]?user only)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:seats?|licenses?|licences?|users?|members?|admins?|owners?)|"
    r"\$?\d+(?:\.\d{2})?\s*(?:per\s+seat|/seat|per additional seat|overage|overage fee)?|"
    r"\d+%\s*(?:overage|threshold|buffer)?|"
    r"\d+\s*(?:hours?|days?|weeks?|months?)|"
    r"(?:hard cap|soft cap|true[- ]?up|prorated credit|billing credit|invoice adjustment|"
    r"admin|owner|member|viewer|email|in[- ]?app|slack|webhook|audit log|seat event|license event))\b",
    re.I,
)
_DURATION_RE = re.compile(r"\b\d+\s*(?:hours?|days?|weeks?|months?)\b", re.I)
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
    "integration_points",
    "risks",
    "non_goals",
    "assumptions",
    "authentication",
    "auth_requirements",
    "security",
    "seat_management",
    "license_management",
    "license",
    "licensing",
    "billing",
    "notifications",
    "audit",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[SeatManagementCategory, re.Pattern[str]] = {
    "seat_allocation": re.compile(
        r"\b(?:seat allocation|allocate seats?|allocated seats?|seat assignment|assign seats?|"
        r"seat inventory|seat pool|seat management|license management|paid seats?|user seats?|"
        r"workspace seats?|team seats?|consume seats?|reserve seats?)\b",
        re.I,
    ),
    "invite_acceptance": re.compile(
        r"\b(?:invite acceptance|accept invites?|accept invitations?|accepted invites?|accepted invitations?|"
        r"pending invites?|pending invitations?|invited users?|invited members?|join link|invitation link|"
        r"seat consumed on accept|consume(?:s|d)? a seat when accepted)\b",
        re.I,
    ),
    "role_license_assignment": re.compile(
        r"\b(?:role assignment|assign(?:ed|s)? roles?|license assignment|licence assignment|"
        r"assign(?:ed|s)? licenses?|license tier|licensed role|permission set|default role|"
        r"admin role|owner role|member role|viewer role|billing admin)\b",
        re.I,
    ),
    "overage_handling": re.compile(
        r"\b(?:overage|overages|overage handling|overage fee|additional seats?|extra seats?|"
        r"exceed(?:s|ed)? seat (?:limit|cap)|above seat (?:limit|cap)|true[- ]?up|"
        r"grace seats?|overage threshold|threshold before billing)\b",
        re.I,
    ),
    "seat_limits": re.compile(
        r"\b(?:seat limits?|license limits?|licence limits?|seat cap|license cap|hard cap|soft cap|"
        r"maximum seats?|max seats?|up to \d+\s*(?:seats?|users?|members?)|"
        r"\d+\s*(?:seats?|licenses?|licences?|users?|members?))\b",
        re.I,
    ),
    "deprovisioning": re.compile(
        r"\b(?:deprovision|deprovisioning|deactivate users?|remove users?|remove members?|"
        r"offboard|offboarding|suspend users?|terminated users?|disable accounts?|free seats?|"
        r"release seats?|reclaim seats? after removal)\b",
        re.I,
    ),
    "reassignment": re.compile(
        r"\b(?:reassign|reassignment|transfer seats?|seat transfer|transfer licenses?|"
        r"move seats?|replace users?|swap users?|reuse seats?|reallocate seats?)\b",
        re.I,
    ),
    "billing_proration": re.compile(
        r"\b(?:billing proration|proration|prorated|pro[- ]?rate|prorated charges?|prorated credits?|"
        r"billing credit|invoice adjustment|billing adjustment|mid[- ]cycle|billing period|"
        r"remaining term|true[- ]?up window)\b",
        re.I,
    ),
    "admin_audit_evidence": re.compile(
        r"\b(?:admin audit|audit evidence|audit trail|audit log|audited|logged|logging|"
        r"seat events?|license events?|licence events?|record actor|admin report|evidence|"
        r"who changed seats?|seat change history)\b",
        re.I,
    ),
    "notifications": re.compile(
        r"\b(?:notifications?|notify|email|in[- ]?app|slack|webhook|billing alert|seat alert|"
        r"limit warning|overage warning|admin notification|send.{0,50}(?:notice|alert|email))\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[SeatManagementCategory, tuple[str, ...]] = {
    "seat_allocation": ("identity", "billing", "backend"),
    "invite_acceptance": ("identity", "lifecycle_messaging"),
    "role_license_assignment": ("identity", "authorization", "billing"),
    "overage_handling": ("billing", "backend"),
    "seat_limits": ("billing", "authorization"),
    "deprovisioning": ("identity", "authorization"),
    "reassignment": ("identity", "billing"),
    "billing_proration": ("billing", "finance"),
    "admin_audit_evidence": ("security", "compliance"),
    "notifications": ("lifecycle_messaging", "billing"),
}
_PLAN_IMPACTS: dict[SeatManagementCategory, tuple[str, ...]] = {
    "seat_allocation": ("Define when seats are reserved, consumed, released, and reconciled.",),
    "invite_acceptance": ("Specify whether pending invites reserve seats and when acceptance consumes a seat.",),
    "role_license_assignment": ("Map roles, permission sets, and paid license tiers during member changes.",),
    "overage_handling": ("Define overage thresholds, blocking behavior, true-up, and billing treatment.",),
    "seat_limits": ("Enforce plan seat caps and describe hard-cap or soft-cap behavior in APIs and UI.",),
    "deprovisioning": ("Release or preserve seats during user removal, suspension, and offboarding flows.",),
    "reassignment": ("Support seat transfer, replacement, and ownership rules for reassigned licenses.",),
    "billing_proration": ("Model mid-cycle seat changes, prorated charges or credits, and invoice adjustments.",),
    "admin_audit_evidence": ("Record actor, target user, seat delta, role, timestamp, and billing impact evidence.",),
    "notifications": ("Notify admins or billing owners about limit warnings, overages, removals, and billing changes.",),
}


@dataclass(frozen=True, slots=True)
class SourceSeatManagementRequirement:
    """One source-backed seat management requirement."""

    category: SeatManagementCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: SeatManagementConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> SeatManagementCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> SeatManagementCategory:
        """Compatibility view for extractors that expose concern naming."""
        return self.category

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owners": list(self.suggested_owners),
            "suggested_plan_impacts": list(self.suggested_plan_impacts),
        }


@dataclass(frozen=True, slots=True)
class SourceSeatManagementRequirementsReport:
    """Source-level seat and license management requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceSeatManagementRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSeatManagementRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceSeatManagementRequirement, ...]:
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
        """Return seat management requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Seat Management Requirements Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
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
        ]
        if not self.requirements:
            lines.extend(["", "No source seat management requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Value | Confidence | Source Field | Owners | Evidence | Suggested Plan Impacts |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{_markdown_cell(requirement.value or '')} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell(', '.join(requirement.suggested_owners))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell('; '.join(requirement.suggested_plan_impacts))} |"
            )
        return "\n".join(lines)


def build_source_seat_management_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceSeatManagementRequirementsReport:
    """Build a seat management requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    requirements = () if _has_global_no_scope(payload) else tuple(_merge_candidates(_requirement_candidates(payload)))
    return SourceSeatManagementRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def summarize_source_seat_management_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceSeatManagementRequirementsReport
        | str
        | object
    ),
) -> SourceSeatManagementRequirementsReport | dict[str, Any]:
    """Compatibility helper for callers that use summarize_* naming."""
    if isinstance(source, SourceSeatManagementRequirementsReport):
        return dict(source.summary)
    return build_source_seat_management_requirements(source)


def derive_source_seat_management_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceSeatManagementRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_seat_management_requirements(source)


def generate_source_seat_management_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceSeatManagementRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_seat_management_requirements(source)


def extract_source_seat_management_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceSeatManagementRequirement, ...]:
    """Return seat management requirement records from brief-shaped input."""
    return build_source_seat_management_requirements(source).requirements


def source_seat_management_requirements_to_dict(report: SourceSeatManagementRequirementsReport) -> dict[str, Any]:
    """Serialize a seat management requirements report to a plain dictionary."""
    return report.to_dict()


source_seat_management_requirements_to_dict.__test__ = False


def source_seat_management_requirements_to_dicts(
    requirements: (
        tuple[SourceSeatManagementRequirement, ...]
        | list[SourceSeatManagementRequirement]
        | SourceSeatManagementRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize seat management requirement records to dictionaries."""
    if isinstance(requirements, SourceSeatManagementRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_seat_management_requirements_to_dicts.__test__ = False


def source_seat_management_requirements_to_markdown(report: SourceSeatManagementRequirementsReport) -> str:
    """Render a seat management requirements report as Markdown."""
    return report.to_markdown()


source_seat_management_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: SeatManagementCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: SeatManagementConfidence


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
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        if not _is_requirement(segment):
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = [
            category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)
        ]
        for category in _dedupe(categories):
            candidates.append(
                _Candidate(
                    category=category,
                    value=_value(category, segment.text),
                    source_field=segment.source_field,
                    evidence=_evidence_snippet(segment.source_field, segment.text),
                    confidence=_confidence(segment),
                )
            )
    return candidates


def _has_global_no_scope(payload: Mapping[str, Any]) -> bool:
    for segment in _candidate_segments(payload):
        if segment.source_field.split("[", 1)[0].split(".", 1)[0] not in {
            "title",
            "summary",
            "body",
            "description",
            "scope",
            "non_goals",
            "constraints",
            "source_payload",
        }:
            continue
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        if _NO_ACCOUNT_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceSeatManagementRequirement]:
    grouped: dict[SeatManagementCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceSeatManagementRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        source_field = sorted(
            {item.source_field for item in items if item.source_field},
            key=lambda field: (
                min(_CONFIDENCE_ORDER[item.confidence] for item in items if item.source_field == field),
                _field_category_rank(category, field),
                field.casefold(),
            ),
        )[0]
        requirements.append(
            SourceSeatManagementRequirement(
                category=category,
                source_field=source_field,
                evidence=tuple(
                    sorted(
                        _dedupe_evidence(
                            item.evidence
                            for item in sorted(
                                items,
                                key=lambda item: (
                                    _field_category_rank(category, item.source_field),
                                    1 if "same" in item.source_field.casefold() else 0,
                                    item.source_field.casefold(),
                                ),
                            )
                        ),
                        key=str.casefold,
                    )
                )[:5],
                confidence=confidence,
                value=_best_value(items),
                suggested_owners=_OWNER_SUGGESTIONS[category],
                suggested_plan_impacts=_PLAN_IMPACTS[category],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.value or "",
            requirement.source_field.casefold(),
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
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(segments, str(key), payload[key], False)
    return segments


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _SEAT_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
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
            section_context = inherited_context or bool(
                _SEAT_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = (
                [part]
                if _NEGATED_SCOPE_RE.search(part) and _SEAT_CONTEXT_RE.search(part)
                else _CLAUSE_SPLIT_RE.split(part)
            )
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    field_words = _field_words(segment.source_field)
    if _NO_ACCOUNT_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if not (_SEAT_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _SEAT_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:assigned|allocated|accepted|capped|charged|billed|prorated|"
            r"deprovisioned|deactivated|removed|reassigned|transferred|audited|notified)\b",
            segment.text,
            re.I,
        )
    )


def _value(category: SeatManagementCategory, text: str) -> str | None:
    if category in {"seat_allocation", "seat_limits"}:
        if match := re.search(r"\b(?P<value>\d+\s*(?:seats?|licenses?|licences?|users?|members?))\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
        if category == "seat_limits" and (
            match := re.search(r"\b(?P<value>hard cap|soft cap|seat cap|license cap|max(?:imum)? seats?)\b", text, re.I)
        ):
            return _clean_text(match.group("value")).casefold()
    if category == "invite_acceptance":
        if match := re.search(r"\b(?P<value>accept(?:ed|ance)? invites?|accept(?:ed|ance)? invitations?|pending invites?)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "role_license_assignment":
        if match := re.search(
            r"\b(?P<value>admin|owner|member|viewer|billing admin|licensed role|license tier|permission set)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if category == "overage_handling":
        if match := re.search(
            r"(?P<value>\$?\d+(?:\.\d{2})?\s*(?:per\s+seat|/seat|per additional seat|overage fee)?|"
            r"\d+%\s*(?:overage|threshold|buffer)?|overage threshold|true[- ]?up|grace seats?)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if category == "deprovisioning":
        if match := re.search(r"\b(?P<value>deprovision(?:ing)?|deactivate|remove users?|offboard(?:ing)?|release seats?|free seats?)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "reassignment":
        if match := re.search(r"\b(?P<value>reassign(?:ment)?|transfer seats?|seat transfer|reuse seats?|replace users?)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "billing_proration":
        if match := re.search(r"\b(?P<value>\d+\s*(?:hours?|days?|weeks?|months?))\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(
            r"\b(?P<value>prorat(?:e|ed|ion)|"
            r"prorated credit|billing credit|invoice adjustment|mid[- ]cycle|billing period)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if category == "admin_audit_evidence":
        if match := re.search(r"\b(?P<value>audit evidence|audit log|seat events?|license events?|record actor|seat change history)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "notifications":
        if match := re.search(r"\b(?P<value>email|in[- ]?app|slack|webhook|billing alert|seat alert|limit warning|overage warning)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if match := _VALUE_RE.search(text):
        return _clean_text(match.group(0)).casefold()
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    values = sorted(
        {item.value for item in items if item.value},
        key=lambda value: (
            0 if re.search(r"\d", value) else 1,
            0 if _VALUE_RE.search(value) or _DURATION_RE.search(value) else 1,
            len(value),
            value.casefold(),
        ),
    )
    return values[0] if values else None


def _confidence(segment: _Segment) -> SeatManagementConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if _REQUIREMENT_RE.search(segment.text) and (
        segment.section_context
        or any(
            marker in segment.source_field.replace("-", "_").casefold()
            for marker in (
                "acceptance_criteria",
                "definition_of_done",
                "success_criteria",
                "scope",
                "billing",
                "authorization",
                "security",
                "seat",
                "license",
                "licensing",
                "audit",
                "notification",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _SEAT_CONTEXT_RE.search(searchable):
        return "medium"
    if _SEAT_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(requirements: tuple[SourceSeatManagementRequirement, ...]) -> dict[str, Any]:
    return {
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
        "status": "ready_for_planning" if requirements else "no_seat_management_language",
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
        "success_criteria",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "authentication",
        "auth_requirements",
        "security",
        "seat_management",
        "license_management",
        "license",
        "licensing",
        "billing",
        "notifications",
        "audit",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: SeatManagementCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[SeatManagementCategory, tuple[str, ...]] = {
        "seat_allocation": ("allocation", "seat", "inventory", "pool"),
        "invite_acceptance": ("invite", "invitation", "acceptance", "pending"),
        "role_license_assignment": ("role", "permission", "license", "assignment"),
        "overage_handling": ("overage", "threshold", "true up", "extra"),
        "seat_limits": ("limit", "cap", "maximum", "max"),
        "deprovisioning": ("deprovision", "deactivate", "remove", "offboard"),
        "reassignment": ("reassign", "transfer", "reuse", "replace"),
        "billing_proration": ("proration", "prorated", "billing", "invoice"),
        "admin_audit_evidence": ("audit", "evidence", "event", "log"),
        "notifications": ("notification", "notify", "email", "alert"),
    }
    return 0 if any(marker in field_words for marker in markers[category]) else 1


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


__all__ = [
    "SeatManagementCategory",
    "SeatManagementConfidence",
    "SourceSeatManagementRequirement",
    "SourceSeatManagementRequirementsReport",
    "build_source_seat_management_requirements",
    "derive_source_seat_management_requirements",
    "extract_source_seat_management_requirements",
    "generate_source_seat_management_requirements",
    "summarize_source_seat_management_requirements",
    "source_seat_management_requirements_to_dict",
    "source_seat_management_requirements_to_dicts",
    "source_seat_management_requirements_to_markdown",
]
