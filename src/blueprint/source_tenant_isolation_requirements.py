"""Extract source-level multi-tenant isolation requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceTenantIsolationRequirementCategory = Literal[
    "tenant_boundary",
    "row_level_security",
    "organization_membership",
    "cross_tenant_search",
    "shared_resource_isolation",
    "admin_override",
    "audit_scope",
    "migration_isolation",
]
SourceTenantIsolationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[SourceTenantIsolationRequirementCategory, ...] = (
    "tenant_boundary",
    "row_level_security",
    "organization_membership",
    "cross_tenant_search",
    "shared_resource_isolation",
    "admin_override",
    "audit_scope",
    "migration_isolation",
)
_CONFIDENCE_ORDER: dict[SourceTenantIsolationConfidence, int] = {
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
    r"acceptance|done when|before launch|cannot ship|policy|guardrail|block|prevent|"
    r"enforce|deny|restrict|isolate|segregate|scope|filter)\b",
    re.I,
)
_TENANT_CONTEXT_RE = re.compile(
    r"\b(?:multi[- ]tenant|multitenant|tenant(?:s)?|tenant[-_ ]id|tenant boundary|"
    r"tenant isolation|tenant scoped|tenant-scoped|workspace(?:s)?|organization(?:s)?|"
    r"org(?:s)?|account(?:s)?|customer(?:s)?|customer boundary|data boundary)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:tenant|multi[-_ ]?tenant|isolation|boundary|workspace|organi[sz]ation|org|"
    r"account|customer|membership|authorization|permission|security|rls|row[-_ ]?level|"
    r"search|index|shared|resource|admin|override|impersonation|audit|migration|"
    r"requirements?|constraints?|acceptance|metadata|source[-_ ]?payload)",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|exclude|excluding|out of scope|non[- ]goals?)\b.{0,100}"
    r"\b(?:tenant|multi[- ]tenant|multitenant|workspace|organi[sz]ation|account|"
    r"customer|rls|row[- ]level|cross[- ]tenant|isolation|boundary)\b.{0,100}"
    r"\b(?:in scope|required|needed|changes?|requirements?|impact|work|support)\b",
    re.I,
)
_PATH_RE = re.compile(
    r"(?:^|[\s`'\"])(?:[./\w-]+/)+(?:[\w.-]*"
    r"(?:tenant|workspace|organi[sz]ation|org|account|membership|rls|search|shared|"
    r"admin|audit|migration|isolation|boundary)[\w.-]*)",
    re.I,
)
_CATEGORY_PATTERNS: dict[SourceTenantIsolationRequirementCategory, re.Pattern[str]] = {
    "tenant_boundary": re.compile(
        r"\b(?:tenant boundary|tenant isolation|isolate tenants?|tenant[- ]scoped|"
        r"tenant[-_ ]id|scope by tenant|filter by tenant|data boundary|customer boundary|"
        r"prevent data leaks?|no cross[- ]tenant access|deny cross[- ]tenant|"
        r"workspace boundary|account boundary)\b",
        re.I,
    ),
    "row_level_security": re.compile(
        r"\b(?:row[- ]level security|row level security|rls|row policies|row policy|"
        r"tenant[_-]?id predicates?|tenant predicates?|tenant filter|policy per tenant|postgres policies?|"
        r"database policies?|tenant_id predicate)\b",
        re.I,
    ),
    "organization_membership": re.compile(
        r"\b(?:organization membership|organisation membership|org membership|"
        r"workspace membership|tenant membership|member of (?:the )?(?:org|organization|workspace)|"
        r"membership check|role membership|organization role|workspace role|"
        r"invite(?:s|d)? users?|team membership)\b",
        re.I,
    ),
    "cross_tenant_search": re.compile(
        r"\b(?:cross[- ]tenant search|global search|search index|search results?|"
        r"index per tenant|tenant-aware search|tenant scoped search|"
        r"search must not leak|search filters?.{0,40}\btenant|"
        r"results?.{0,40}\b(?:tenant|workspace|organization) boundary)\b",
        re.I,
    ),
    "shared_resource_isolation": re.compile(
        r"\b(?:shared resource isolation|shared resources?|shared buckets?|shared queues?|"
        r"shared cache|shared redis|shared topic|shared index|shared storage|"
        r"namespace per tenant|tenant namespace|prefix per tenant|"
        r"object storage.{0,40}\btenant|cache keys?.{0,40}\btenant)\b",
        re.I,
    ),
    "admin_override": re.compile(
        r"\b(?:admin override|administrator override|super admin|superadmin|"
        r"support impersonation|impersonate users?|break[- ]glass|cross[- ]tenant admin|"
        r"staff access|operator access|admin bypass|privileged access)\b",
        re.I,
    ),
    "audit_scope": re.compile(
        r"\b(?:audit scope|tenant audit|audit logs?.{0,50}\btenant|audit events?.{0,50}\btenant|"
        r"tenant id in audit|workspace id in audit|organization id in audit|"
        r"audit trail.{0,50}\b(?:tenant|workspace|organization)|"
        r"log(?:ged)? with tenant|record tenant context)\b",
        re.I,
    ),
    "migration_isolation": re.compile(
        r"\b(?:tenant migration|migrate tenants?|migration isolation|backfill by tenant|"
        r"tenant backfill|per[- ]tenant migration|migration batch.{0,40}\btenant|"
        r"data migration.{0,40}\b(?:tenant|workspace|organization)|"
        r"do not mix tenants?.{0,40}\bmigration)\b",
        re.I,
    ),
}
_CATEGORY_PATH_PATTERNS: dict[SourceTenantIsolationRequirementCategory, re.Pattern[str]] = {
    "tenant_boundary": re.compile(r"(?:tenant|workspace|organi[sz]ation|account).*(?:boundary|isolation|scope)|(?:boundary|isolation).*(?:tenant|workspace|org)", re.I),
    "row_level_security": re.compile(r"(?:rls|row[-_]?level|row[_-]?polic)", re.I),
    "organization_membership": re.compile(r"(?:membership|members?|roles?|invites?)", re.I),
    "cross_tenant_search": re.compile(r"(?:search|index).*(?:tenant|workspace|org)|(?:tenant|workspace|org).*(?:search|index)", re.I),
    "shared_resource_isolation": re.compile(r"(?:shared|bucket|queue|cache|redis|topic|storage|namespace)", re.I),
    "admin_override": re.compile(r"(?:admin|override|impersonat|break[-_]?glass|staff|operator)", re.I),
    "audit_scope": re.compile(r"(?:audit|activity[_-]?log|event[_-]?log)", re.I),
    "migration_isolation": re.compile(r"(?:migration|backfill|import|export)", re.I),
}
_SCOPE_RE = re.compile(
    r"\b(?:tenant|workspace|organization|organisation|org|account|customer|user|team|"
    r"project|record|row|document|file|attachment|invoice|billing|audit|search|"
    r"cache|queue|bucket|index|migration|admin|support)"
    r"(?:[- ](?:data|records?|rows?|files?|uploads?|members?|roles?|permissions?|"
    r"events?|logs?|results?|indexes?|resources?|operations?|access|actions?))*\b",
    re.I,
)
_OWNER_BY_CATEGORY: dict[SourceTenantIsolationRequirementCategory, str] = {
    "tenant_boundary": "backend",
    "row_level_security": "data",
    "organization_membership": "identity",
    "cross_tenant_search": "search",
    "shared_resource_isolation": "platform",
    "admin_override": "security",
    "audit_scope": "security",
    "migration_isolation": "data",
}
_PLANNING_NOTES: dict[SourceTenantIsolationRequirementCategory, str] = {
    "tenant_boundary": "Plan explicit tenant scoping and denial behavior for every affected read, write, and authorization path.",
    "row_level_security": "Carry row-level tenant predicates or database policies into schema, query, migration, and test tasks.",
    "organization_membership": "Plan membership and role checks before granting tenant, workspace, or organization access.",
    "cross_tenant_search": "Plan tenant-aware indexing, filtering, and leakage tests for search and result rendering.",
    "shared_resource_isolation": "Plan tenant namespaces, prefixes, quotas, and cleanup for shared storage, queues, caches, and indexes.",
    "admin_override": "Plan privileged cross-tenant access as an audited, approved exception with narrow authorization.",
    "audit_scope": "Plan audit events to include tenant context and preserve tenant-scoped reviewability.",
    "migration_isolation": "Plan migrations and backfills so tenant batches, validation, rollback, and logs stay isolated.",
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
    "success_criteria",
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
    "security",
    "compliance",
    "privacy",
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
}


@dataclass(frozen=True, slots=True)
class SourceTenantIsolationRequirement:
    """One source-backed tenant isolation requirement category."""

    category: SourceTenantIsolationRequirementCategory
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: SourceTenantIsolationConfidence = "medium"
    suggested_owner: str = ""
    suggested_planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "suggested_owner": self.suggested_owner,
            "suggested_planning_note": self.suggested_planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceTenantIsolationRequirementsReport:
    """Source-level tenant isolation requirements report."""

    source_id: str | None = None
    source_title: str | None = None
    requirements: tuple[SourceTenantIsolationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceTenantIsolationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "source_title": self.source_title,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return tenant isolation requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Tenant Isolation Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        elif self.source_title:
            title = f"{title}: {self.source_title}"
        category_counts = self.summary.get("category_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Confidence counts: "
            + ", ".join(
                f"{confidence} {confidence_counts.get(confidence, 0)}"
                for confidence in _CONFIDENCE_ORDER
            ),
            "- Category counts: "
            + ", ".join(
                f"{category} {category_counts.get(category, 0)}"
                for category in _CATEGORY_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source tenant isolation requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Confidence | Owner | Source Fields | Matched Terms | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.suggested_owner)} | "
                f"{_markdown_cell(', '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell(', '.join(requirement.matched_terms))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.suggested_planning_note)} |"
            )
        return "\n".join(lines)


def build_source_tenant_isolation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceTenantIsolationRequirementsReport:
    """Build a tenant isolation requirements report from brief-shaped input."""
    source_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(source_payloads)),
            key=lambda requirement: (
                _category_index(requirement.category),
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.source_field_paths,
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _, _ in source_payloads if source_id)
    source_titles = _dedupe(source_title for _, source_title, _ in source_payloads if source_title)
    return SourceTenantIsolationRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        source_title=source_titles[0] if len(source_titles) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, source_payloads),
    )


def extract_source_tenant_isolation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceTenantIsolationRequirementsReport:
    """Compatibility alias for building a tenant isolation requirements report."""
    return build_source_tenant_isolation_requirements(source)


def generate_source_tenant_isolation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceTenantIsolationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_tenant_isolation_requirements(source)


def derive_source_tenant_isolation_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceTenantIsolationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_tenant_isolation_requirements(source)


def summarize_source_tenant_isolation_requirements(
    source_or_report: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceTenantIsolationRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for source tenant isolation requirements."""
    if isinstance(source_or_report, SourceTenantIsolationRequirementsReport):
        return dict(source_or_report.summary)
    return build_source_tenant_isolation_requirements(source_or_report).summary


def source_tenant_isolation_requirements_to_dict(
    report: SourceTenantIsolationRequirementsReport,
) -> dict[str, Any]:
    """Serialize a source tenant isolation requirements report to a plain dictionary."""
    return report.to_dict()


source_tenant_isolation_requirements_to_dict.__test__ = False


def source_tenant_isolation_requirements_to_dicts(
    requirements: (
        tuple[SourceTenantIsolationRequirement, ...]
        | list[SourceTenantIsolationRequirement]
        | SourceTenantIsolationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source tenant isolation requirement records to dictionaries."""
    if isinstance(requirements, SourceTenantIsolationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_tenant_isolation_requirements_to_dicts.__test__ = False


def source_tenant_isolation_requirements_to_markdown(
    report: SourceTenantIsolationRequirementsReport,
) -> str:
    """Render a source tenant isolation requirements report as Markdown."""
    return report.to_markdown()


source_tenant_isolation_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: SourceTenantIsolationRequirementCategory
    confidence: SourceTenantIsolationConfidence
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]


def _source_payloads(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> list[tuple[str | None, str | None, dict[str, Any]]]:
    if isinstance(
        source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)
    ) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = dict(source.model_dump(mode="python"))
        return _source_id(payload), _source_title(payload), payload
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), _source_title(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                payload = dict(model.model_validate(source).model_dump(mode="python"))
                return _source_id(payload), _source_title(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), _source_title(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_id(payload), _source_title(payload), payload
    return None, None, {}


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _source_title(payload: Mapping[str, Any]) -> str | None:
    return _optional_text(payload.get("title"))


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for _, _, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            categories = _categories(segment)
            if not categories:
                continue
            evidence = _evidence_snippet(segment.source_field, segment.text)
            for category in categories:
                candidates.append(
                    _Candidate(
                        category=category,
                        confidence=_confidence(category, segment),
                        evidence=evidence,
                        source_field_path=segment.source_field,
                        matched_terms=tuple(_matched_terms(category, segment)),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceTenantIsolationRequirement]:
    grouped: dict[SourceTenantIsolationRequirementCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceTenantIsolationRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        requirements.append(
            SourceTenantIsolationRequirement(
                category=category,
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:6],
                source_field_paths=tuple(
                    sorted(_dedupe(item.source_field_path for item in items), key=str.casefold)
                ),
                matched_terms=tuple(
                    sorted(
                        _dedupe(term for item in items for term in item.matched_terms),
                        key=str.casefold,
                    )
                ),
                confidence=confidence,
                suggested_owner=_OWNER_BY_CATEGORY[category],
                suggested_planning_note=_PLANNING_NOTES[category],
            )
        )
    return requirements


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
    segments: list[_Segment], source_field: str, value: Any, section_context: bool
) -> None:
    field_words = _field_words(source_field)
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(field_words))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _TENANT_CONTEXT_RE.search(key_text)
            )
            if _any_signal(key_text):
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
                _TENANT_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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


def _categories(segment: _Segment) -> tuple[SourceTenantIsolationRequirementCategory, ...]:
    if _NEGATED_SCOPE_RE.search(segment.text) or _non_goal_field(segment.source_field):
        return ()
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    categories = [
        category
        for category in _CATEGORY_ORDER
        if _CATEGORY_PATTERNS[category].search(searchable)
    ]
    if not categories and _PATH_RE.search(segment.text):
        categories = [
            category
            for category in _CATEGORY_ORDER
            if _CATEGORY_PATH_PATTERNS[category].search(searchable)
        ]
    if not categories:
        return ()
    if _is_requirement(segment, categories):
        return tuple(_dedupe(categories))
    return ()


def _is_requirement(
    segment: _Segment,
    categories: Iterable[SourceTenantIsolationRequirementCategory],
) -> bool:
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    has_tenant_context = bool(_TENANT_CONTEXT_RE.search(segment.text) or field_context or segment.section_context)
    has_requirement_language = bool(_REQUIREMENT_RE.search(segment.text))
    has_path = bool(_PATH_RE.search(segment.text))
    if has_requirement_language and has_tenant_context:
        return True
    if field_context and (_TENANT_CONTEXT_RE.search(segment.text) or has_requirement_language):
        return True
    if segment.section_context and (has_requirement_language or _TENANT_CONTEXT_RE.search(segment.text)):
        return True
    if has_path and (field_context or segment.section_context or _TENANT_CONTEXT_RE.search(segment.text)):
        return True
    if any(category in {"row_level_security", "admin_override"} for category in categories) and has_requirement_language:
        return True
    return False


def _confidence(
    category: SourceTenantIsolationRequirementCategory,
    segment: _Segment,
) -> SourceTenantIsolationConfidence:
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    has_requirement_language = bool(_REQUIREMENT_RE.search(segment.text))
    has_tenant_context = bool(_TENANT_CONTEXT_RE.search(segment.text) or field_context or segment.section_context)
    has_path = bool(_PATH_RE.search(segment.text))
    if has_requirement_language and has_tenant_context:
        return "high"
    if field_context and has_tenant_context and category in {"row_level_security", "tenant_boundary"}:
        return "high"
    if has_tenant_context or has_path:
        return "medium"
    return "low"


def _matched_terms(
    category: SourceTenantIsolationRequirementCategory, segment: _Segment
) -> list[str]:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    terms: list[str] = []
    for pattern in (_CATEGORY_PATTERNS[category], _TENANT_CONTEXT_RE, _REQUIREMENT_RE):
        for match in pattern.finditer(searchable):
            terms.append(_clean_text(match.group(0)).casefold())
    if _PATH_RE.search(segment.text):
        terms.extend(_clean_text(match.group(0)).strip("`'\"").casefold() for match in _PATH_RE.finditer(segment.text))
    return _dedupe(terms)


def _summary(
    requirements: tuple[SourceTenantIsolationRequirement, ...],
    source_payloads: list[tuple[str | None, str | None, Mapping[str, Any]]],
) -> dict[str, Any]:
    source_ids = _dedupe(source_id for source_id, _, _ in source_payloads if source_id)
    source_titles = _dedupe(title for _, title, _ in source_payloads if title)
    return {
        "source_count": len(source_payloads),
        "source_id": source_ids[0] if len(source_ids) == 1 else None,
        "source_title": source_titles[0] if len(source_titles) == 1 else None,
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
        "privacy",
        "compliance",
        "security",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _any_signal(text: str) -> bool:
    return bool(
        _TENANT_CONTEXT_RE.search(text)
        or _STRUCTURED_FIELD_RE.search(text)
        or any(pattern.search(text) for pattern in _CATEGORY_PATTERNS.values())
        or any(pattern.search(text) for pattern in _CATEGORY_PATH_PATTERNS.values())
    )


def _category_index(category: SourceTenantIsolationRequirementCategory) -> int:
    return _CATEGORY_ORDER.index(category)


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _non_goal_field(source_field: str) -> bool:
    return bool(re.search(r"(?:^|\.)non_goals?(?:$|\[|\.)", source_field))


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
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe_evidence(values: Iterable[str]) -> list[str]:
    deduped: list[str] = []
    seen: dict[str, int] = {}
    for value in values:
        if not value:
            continue
        _, _, statement = value.partition(": ")
        key = _clean_text(statement or value).casefold()
        if key in seen:
            index = seen[key]
            if _evidence_priority(value) < _evidence_priority(deduped[index]):
                deduped[index] = value
            continue
        deduped.append(value)
        seen[key] = len(deduped) - 1
    return sorted(deduped, key=lambda item: (_evidence_priority(item), item.casefold()))


def _evidence_priority(value: str) -> int:
    source_field, _, _ = value.partition(": ")
    if ".requirements" in source_field or ".constraints" in source_field or ".acceptance" in source_field:
        return 0
    if ".metadata" in source_field or ".brief_metadata" in source_field:
        return 2
    return 1


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
    "SourceTenantIsolationConfidence",
    "SourceTenantIsolationRequirement",
    "SourceTenantIsolationRequirementCategory",
    "SourceTenantIsolationRequirementsReport",
    "build_source_tenant_isolation_requirements",
    "derive_source_tenant_isolation_requirements",
    "extract_source_tenant_isolation_requirements",
    "generate_source_tenant_isolation_requirements",
    "source_tenant_isolation_requirements_to_dict",
    "source_tenant_isolation_requirements_to_dicts",
    "source_tenant_isolation_requirements_to_markdown",
    "summarize_source_tenant_isolation_requirements",
]
