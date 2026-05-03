"""Extract source-level admin console requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


AdminConsoleRequirementCategory = Literal[
    "admin_roles",
    "permission_scopes",
    "audit_logging",
    "impersonation_constraints",
    "bulk_actions",
    "search_filters",
    "support_workflows",
    "sensitive_data_visibility",
]
AdminConsoleGapCategory = Literal["missing_role_boundaries", "missing_audit_expectations"]
AdminConsoleConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[AdminConsoleRequirementCategory, ...] = (
    "admin_roles",
    "permission_scopes",
    "audit_logging",
    "impersonation_constraints",
    "bulk_actions",
    "search_filters",
    "support_workflows",
    "sensitive_data_visibility",
)
_GAP_ORDER: tuple[AdminConsoleGapCategory, ...] = (
    "missing_role_boundaries",
    "missing_audit_expectations",
)
_CONFIDENCE_ORDER: dict[AdminConsoleConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_OWNER_BY_CATEGORY: dict[AdminConsoleRequirementCategory, tuple[str, ...]] = {
    "admin_roles": ("product", "security"),
    "permission_scopes": ("security", "platform"),
    "audit_logging": ("security", "compliance"),
    "impersonation_constraints": ("support", "security"),
    "bulk_actions": ("product", "operations"),
    "search_filters": ("product", "support"),
    "support_workflows": ("support", "customer_success"),
    "sensitive_data_visibility": ("security", "privacy"),
}
_PLAN_IMPACTS: dict[AdminConsoleRequirementCategory, tuple[str, ...]] = {
    "admin_roles": ("Define admin personas, role boundaries, assignment rules, and escalation paths.",),
    "permission_scopes": ("Map permissions to console actions, resources, tenants, and read/write scopes.",),
    "audit_logging": ("Capture admin actions with actor, target, timestamp, reason, outcome, and export needs.",),
    "impersonation_constraints": ("Constrain login-as access with approvals, ticket linkage, time limits, and customer visibility.",),
    "bulk_actions": ("Specify selection, preview, confirmation, partial failure, rollback, and progress handling.",),
    "search_filters": ("Define searchable fields, filters, saved views, pagination, and permission-aware results.",),
    "support_workflows": ("Connect console actions to cases, queues, escalations, notes, and resolution states.",),
    "sensitive_data_visibility": ("Set masking, reveal approvals, field-level access, redaction, and privacy boundaries.",),
}
_GAP_MESSAGES: dict[AdminConsoleGapCategory, str] = {
    "missing_role_boundaries": "Specify admin roles, role boundaries, and which permissions each role can use.",
    "missing_audit_expectations": "Specify audit logging expectations for admin actions, actor, target, timestamp, and reason.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_ADMIN_CONTEXT_RE = re.compile(
    r"\b(?:admin|administrator|internal|staff|operator|operations|ops|back[- ]?office|"
    r"console|admin console|management console|support tool(?:ing)?|helpdesk|support agent|"
    r"customer success|csr|moderator|super admin|tenant admin|org admin|workspace admin|"
    r"role based|rbac|permission|audit|impersonat|login[- ]?as|bulk|search|filter|"
    r"sensitive data|pii|redact|mask|reveal)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:admin|administrator|internal|staff|operator|ops|operations|back[-_ ]?office|"
    r"console|support|helpdesk|role|roles|rbac|permission|permissions|audit|impersonation|"
    r"bulk|search|filter|visibility|sensitive|pii|privacy|requirements?|acceptance|criteria|"
    r"scope|workflow|security|metadata|source[-_ ]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"support|allow|provide|enable|restrict|limit|prevent|deny|grant|capture|record|log|"
    r"track|show|hide|mask|redact|reveal|approve|confirm|preview|filter|search|export|"
    r"acceptance|done when|before launch|cannot ship)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:admin console|admin tooling|internal console|back[- ]?office|support tooling|"
    r"admin roles?|permissions?|audit logs?|impersonation|bulk actions?)\b|"
    r"\b(?:admin console|admin tooling|internal console|back[- ]?office|support tooling|"
    r"admin roles?|permissions?|audit logs?|impersonation|bulk actions?)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|excluded)\b",
    re.I,
)
_END_USER_ONLY_RE = re.compile(
    r"\b(?:end users?|customers?|members?|shoppers?|visitors?)\b.{0,90}"
    r"\b(?:profile|checkout|feed|onboarding|signup|sign up|settings|landing page|mobile app)\b",
    re.I,
)
_SPECIFIC_ADMIN_RE = re.compile(
    r"\b(?:super admin|tenant admin|org admin|workspace admin|support agent|moderator|operator|"
    r"role boundaries?|rbac|permission scopes?|read[- ]only|write access|audit log|audit trail|"
    r"admin action|actor|timestamp|reason|ticket id|impersonat|login[- ]?as|bulk action|"
    r"bulk update|bulk export|search|filters?|saved views?|support queue|case management|"
    r"sensitive data|pii|mask(?:ed|ing)?|redact(?:ed|ion)?|field[- ]level|data visibility)\b",
    re.I,
)
_VALUE_PATTERNS: dict[AdminConsoleRequirementCategory, re.Pattern[str]] = {
    "admin_roles": re.compile(
        r"\b(?:super admin|tenant admin|org admin|workspace admin|billing admin|support agent|"
        r"moderator|operator|read[- ]only admin|security admin)\b",
        re.I,
    ),
    "permission_scopes": re.compile(
        r"\b(?:rbac|read[- ]only|read/write|write access|manage users?|"
        r"manage billing|approve refunds?|tenant scoped|workspace scoped|least privilege)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit log|audit trail|admin action log|actor|timestamp|reason|target record|"
        r"before and after|exportable logs?|compliance evidence)\b",
        re.I,
    ),
    "impersonation_constraints": re.compile(
        r"\b(?:login[- ]?as|support access|ticket id|timeboxed|"
        r"approval required|customer visible|session limit|reason capture)\b",
        re.I,
    ),
    "bulk_actions": re.compile(
        r"\b(?:bulk action|bulk update|bulk edit|bulk export|bulk delete|batch action|"
        r"select all|preview step|confirmation|partial failure|rollback)\b",
        re.I,
    ),
    "search_filters": re.compile(
        r"\b(?:search|filter|filters|saved view|sort|pagination|tenant id|email lookup|"
        r"status filter|date range|advanced search)\b",
        re.I,
    ),
    "support_workflows": re.compile(
        r"\b(?:support workflow|support queue|helpdesk|case management|support ticket|ticket id|"
        r"escalation|notes|resolution state|customer success|refund review|account recovery)\b",
        re.I,
    ),
    "sensitive_data_visibility": re.compile(
        r"\b(?:sensitive data|pii|personal data|mask(?:ed|ing)?|redact(?:ed|ion)?|"
        r"reveal|field[- ]level access|data visibility|privacy|ssn|tax id|payment details)\b",
        re.I,
    ),
}
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
    "risks",
    "security",
    "privacy",
    "admin",
    "admin_console",
    "operations",
    "support",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[AdminConsoleRequirementCategory, re.Pattern[str]] = {
    "admin_roles": re.compile(
        r"\b(?:admin roles?|administrator roles?|role boundaries?|role assignment|role matrix|"
        r"super admin|tenant admin|org admin|workspace admin|billing admin|support agent|"
        r"moderator|operator|read[- ]only admin|security admin)\b",
        re.I,
    ),
    "permission_scopes": re.compile(
        r"\b(?:permission scopes?|permissions?|rbac|role[- ]based access|least privilege|"
        r"read[- ]only|read/write|write access|manage users?|manage billing|approve refunds?|"
        r"tenant scoped|workspace scoped|resource scoped|access control)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit logging|audit logs?|audit trail|admin action log|operator action log|"
        r"activity log|record admin actions?|log admin actions?|admin actions?.{0,60}(?:actor|timestamp|reason|target)|"
        r"(?:actor|timestamp|reason|target record).{0,60}(?:audit|log|admin action)|"
        r"before and after|compliance evidence|exportable logs?)\b",
        re.I,
    ),
    "impersonation_constraints": re.compile(
        r"\b(?:impersonat(?:e|es|ing|ion)|login[- ]?as|log in as|support access to customer|"
        r"masquerade|assume user|delegated access|ticket id|reason capture|timeboxed|"
        r"customer visible|session limit|approval required)\b",
        re.I,
    ),
    "bulk_actions": re.compile(
        r"\b(?:bulk actions?|bulk update|bulk edit|bulk export|bulk delete|bulk import|"
        r"batch actions?|batch operation|select all|mass update|preview step|confirmation|"
        r"partial failure|rollback|undo)\b",
        re.I,
    ),
    "search_filters": re.compile(
        r"\b(?:(?:admin|console|support|operator).{0,60}(?:search|filter|saved view|sort|pagination)|"
        r"(?:search|filter|saved views?|advanced search|date range|status filter|tenant id|email lookup)"
        r".{0,60}(?:admin|console|support|operator|case|tenant|customer|account))\b",
        re.I,
    ),
    "support_workflows": re.compile(
        r"\b(?:support workflows?|support queue|support tooling|helpdesk|case management|"
        r"support tickets?|ticket id|customer success|csr|escalation|resolution states?|"
        r"account recovery|refund review|support notes?)\b",
        re.I,
    ),
    "sensitive_data_visibility": re.compile(
        r"\b(?:sensitive[- ]data visibility|data visibility|sensitive data|pii|personal data|"
        r"mask(?:ed|ing)?|redact(?:ed|ion)?|reveal sensitive|field[- ]level access|"
        r"privacy boundary|ssn|tax id|payment details)\b",
        re.I,
    ),
}


@dataclass(frozen=True, slots=True)
class SourceAdminConsoleRequirement:
    """One source-backed admin console requirement."""

    category: AdminConsoleRequirementCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: AdminConsoleConfidence = "medium"
    value: str = ""
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    suggested_plan_impacts: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> AdminConsoleRequirementCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> AdminConsoleRequirementCategory:
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
class SourceAdminConsoleEvidenceGap:
    """One missing admin console detail that should be resolved before planning."""

    category: AdminConsoleGapCategory
    message: str
    confidence: AdminConsoleConfidence = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "message": self.message,
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class SourceAdminConsoleRequirementsReport:
    """Source-level admin console requirements report."""

    source_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceAdminConsoleRequirement, ...] = field(default_factory=tuple)
    evidence_gaps: tuple[SourceAdminConsoleEvidenceGap, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceAdminConsoleRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceAdminConsoleRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    @property
    def gaps(self) -> tuple[SourceAdminConsoleEvidenceGap, ...]:
        """Compatibility alias for evidence gaps."""
        return self.evidence_gaps

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "title": self.title,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "evidence_gaps": [gap.to_dict() for gap in self.evidence_gaps],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
            "gaps": [gap.to_dict() for gap in self.gaps],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return admin console requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Admin Console Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Evidence gaps: {self.summary.get('evidence_gap_count', 0)}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if self.requirements:
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
                    f"{_markdown_cell(requirement.value)} | "
                    f"{requirement.confidence} | "
                    f"{_markdown_cell(requirement.source_field)} | "
                    f"{_markdown_cell(', '.join(requirement.suggested_owners))} | "
                    f"{_markdown_cell('; '.join(requirement.evidence))} | "
                    f"{_markdown_cell('; '.join(requirement.suggested_plan_impacts))} |"
                )
        else:
            lines.extend(["", "No admin console requirements were found in the source brief."])
        if self.evidence_gaps:
            lines.extend(["", "## Evidence Gaps", "", "| Gap | Confidence | Message |", "| --- | --- | --- |"])
            for gap in self.evidence_gaps:
                lines.append(f"| {gap.category} | {gap.confidence} | {_markdown_cell(gap.message)} |")
        return "\n".join(lines)


def build_source_admin_console_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceAdminConsoleRequirementsReport:
    """Build an admin console requirements report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    requirements = tuple(_merge_candidates(candidates))
    gaps = tuple(_evidence_gaps(requirements, candidates))
    return SourceAdminConsoleRequirementsReport(
        source_id=source_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        evidence_gaps=gaps,
        summary=_summary(requirements, gaps),
    )


def generate_source_admin_console_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceAdminConsoleRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_admin_console_requirements(source)


def derive_source_admin_console_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceAdminConsoleRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_admin_console_requirements(source)


def extract_source_admin_console_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[SourceAdminConsoleRequirement, ...]:
    """Return admin console requirement records extracted from brief-shaped input."""
    return build_source_admin_console_requirements(source).requirements


def summarize_source_admin_console_requirements(
    source_or_result: (
        str
        | Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceAdminConsoleRequirementsReport
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic admin console requirements summary."""
    if isinstance(source_or_result, SourceAdminConsoleRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_admin_console_requirements(source_or_result).summary


def source_admin_console_requirements_to_dict(
    report: SourceAdminConsoleRequirementsReport,
) -> dict[str, Any]:
    """Serialize an admin console requirements report to a plain dictionary."""
    return report.to_dict()


source_admin_console_requirements_to_dict.__test__ = False


def source_admin_console_requirements_to_dicts(
    requirements: tuple[SourceAdminConsoleRequirement, ...]
    | list[SourceAdminConsoleRequirement]
    | SourceAdminConsoleRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize source admin console requirement records to dictionaries."""
    if isinstance(requirements, SourceAdminConsoleRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_admin_console_requirements_to_dicts.__test__ = False


def source_admin_console_requirements_to_markdown(
    report: SourceAdminConsoleRequirementsReport,
) -> str:
    """Render an admin console requirements report as Markdown."""
    return report.to_markdown()


source_admin_console_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: AdminConsoleRequirementCategory
    confidence: AdminConsoleConfidence
    evidence: str
    source_field: str
    value: str


def _source_payload(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (bytes, bytearray)):
        return None, {}
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
    payload = _object_payload(source)
    return _brief_id(payload), payload


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = [
            category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)
        ]
        if not categories or not _is_requirement(segment):
            continue
        confidence = _confidence(segment)
        evidence = _evidence_snippet(segment.source_field, segment.text)
        for category in _dedupe(categories):
            candidates.append(
                _Candidate(
                    category=category,
                    confidence=confidence,
                    evidence=evidence,
                    source_field=segment.source_field,
                    value=_extract_value(category, segment.text),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceAdminConsoleRequirement]:
    by_category: dict[AdminConsoleRequirementCategory, list[_Candidate]] = {}
    for candidate in candidates:
        by_category.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceAdminConsoleRequirement] = []
    for category in _CATEGORY_ORDER:
        items = by_category.get(category, [])
        if not items:
            continue
        best = min(
            items,
            key=lambda item: (
                _CONFIDENCE_ORDER[item.confidence],
                _field_category_rank(category, item.source_field),
                item.source_field.casefold(),
            ),
        )
        requirements.append(
            SourceAdminConsoleRequirement(
                category=category,
                source_field=best.source_field,
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:5],
                confidence=best.confidence,
                value=_merge_values(item.value for item in items),
                suggested_owners=_OWNER_BY_CATEGORY[category],
                suggested_plan_impacts=_PLAN_IMPACTS[category],
            )
        )
    return requirements


def _evidence_gaps(
    requirements: tuple[SourceAdminConsoleRequirement, ...],
    candidates: list[_Candidate],
) -> list[SourceAdminConsoleEvidenceGap]:
    if not requirements and not candidates:
        return []
    present = {requirement.category for requirement in requirements}
    gaps: list[SourceAdminConsoleEvidenceGap] = []
    if "admin_roles" not in present and "permission_scopes" not in present:
        gaps.append(
            SourceAdminConsoleEvidenceGap(
                category="missing_role_boundaries",
                message=_GAP_MESSAGES["missing_role_boundaries"],
                confidence="medium",
            )
        )
    if "audit_logging" not in present:
        gaps.append(
            SourceAdminConsoleEvidenceGap(
                category="missing_audit_expectations",
                message=_GAP_MESSAGES["missing_audit_expectations"],
                confidence="medium",
            )
        )
    return gaps


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
            if str(key) in _IGNORED_FIELDS:
                continue
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _ADMIN_CONTEXT_RE.search(key_text)
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
                _ADMIN_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
            )
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(segment: _Segment) -> bool:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    if _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _END_USER_ONLY_RE.search(segment.text) and not _ADMIN_CONTEXT_RE.search(searchable):
        return False
    if not (_ADMIN_CONTEXT_RE.search(searchable) or field_context or segment.section_context):
        return False
    if not _SPECIFIC_ADMIN_RE.search(searchable) and not any(
        pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values()
    ):
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if field_context or segment.section_context:
        return True
    return bool(_SPECIFIC_ADMIN_RE.search(searchable))


def _has_global_no_scope(payload: Mapping[str, Any]) -> bool:
    for segment in _candidate_segments(payload):
        root_field = segment.source_field.split("[", 1)[0].split(".", 1)[0]
        if root_field not in {"title", "summary", "body", "description", "scope", "non_goals", "constraints", "source_payload"}:
            continue
        if _NEGATED_SCOPE_RE.search(f"{_field_words(segment.source_field)} {segment.text}"):
            return True
    return False


def _confidence(segment: _Segment) -> AdminConsoleConfidence:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    score = 0
    if _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        score += 1
    if segment.section_context or _ADMIN_CONTEXT_RE.search(searchable):
        score += 1
    if _REQUIREMENT_RE.search(segment.text):
        score += 1
    if _SPECIFIC_ADMIN_RE.search(searchable):
        score += 1
    return "high" if score >= 4 else "medium" if score >= 2 else "low"


def _extract_value(category: AdminConsoleRequirementCategory, text: str) -> str:
    values = _dedupe(_clean_text(match.group(0)) for match in _VALUE_PATTERNS[category].finditer(text))
    return ", ".join(values[:3])


def _summary(
    requirements: tuple[SourceAdminConsoleRequirement, ...],
    gaps: tuple[SourceAdminConsoleEvidenceGap, ...],
) -> dict[str, Any]:
    return {
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "categories": [requirement.category for requirement in requirements],
        "evidence_gap_count": len(gaps),
        "evidence_gaps": [gap.category for gap in gaps],
        "status": _status(requirements, gaps),
    }


def _status(
    requirements: tuple[SourceAdminConsoleRequirement, ...],
    gaps: tuple[SourceAdminConsoleEvidenceGap, ...],
) -> str:
    if not requirements:
        return "no_admin_console_requirements_found"
    if gaps:
        return "needs_admin_console_detail"
    return "ready_for_planning"


def _object_payload(value: object) -> dict[str, Any]:
    fields = ("id", "source_brief_id", "source_id", *_SCANNED_FIELDS)
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: AdminConsoleRequirementCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[AdminConsoleRequirementCategory, tuple[str, ...]] = {
        "admin_roles": ("role", "rbac", "admin"),
        "permission_scopes": ("permission", "scope", "access", "rbac"),
        "audit_logging": ("audit", "log", "event", "evidence"),
        "impersonation_constraints": ("imperson", "login as", "delegated"),
        "bulk_actions": ("bulk", "batch", "mass"),
        "search_filters": ("search", "filter", "view"),
        "support_workflows": ("support", "ticket", "case", "helpdesk"),
        "sensitive_data_visibility": ("sensitive", "visibility", "pii", "privacy", "redact", "mask"),
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


def _merge_values(values: Iterable[str]) -> str:
    parts: list[str] = []
    for value in values:
        parts.extend(item.strip() for item in value.split(",") if item.strip())
    return ", ".join(_dedupe(parts)[:3])


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
    "AdminConsoleRequirementCategory",
    "AdminConsoleGapCategory",
    "AdminConsoleConfidence",
    "SourceAdminConsoleRequirement",
    "SourceAdminConsoleEvidenceGap",
    "SourceAdminConsoleRequirementsReport",
    "build_source_admin_console_requirements",
    "derive_source_admin_console_requirements",
    "extract_source_admin_console_requirements",
    "generate_source_admin_console_requirements",
    "summarize_source_admin_console_requirements",
    "source_admin_console_requirements_to_dict",
    "source_admin_console_requirements_to_dicts",
    "source_admin_console_requirements_to_markdown",
]
