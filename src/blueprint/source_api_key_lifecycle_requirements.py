"""Extract source-level API key lifecycle requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ApiKeyLifecycleCategory = Literal[
    "key_creation",
    "key_scoping",
    "key_expiration",
    "key_rotation",
    "key_revocation",
    "last_used_visibility",
    "audit_logging",
    "recovery_guidance",
]
ApiKeyLifecycleMissingDetail = Literal["missing_key_scopes", "missing_rotation_or_revocation"]
ApiKeyLifecycleConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[ApiKeyLifecycleCategory, ...] = (
    "key_creation",
    "key_scoping",
    "key_expiration",
    "key_rotation",
    "key_revocation",
    "last_used_visibility",
    "audit_logging",
    "recovery_guidance",
)
_MISSING_DETAIL_ORDER: tuple[ApiKeyLifecycleMissingDetail, ...] = (
    "missing_key_scopes",
    "missing_rotation_or_revocation",
)
_CONFIDENCE_ORDER: dict[ApiKeyLifecycleConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_API_KEY_CONTEXT_RE = re.compile(
    r"\b(?:api keys?|access keys?|developer keys?|integration keys?|service keys?|client keys?|"
    r"secret keys?|key lifecycle|credential lifecycle|integration credentials?|personal access tokens?|"
    r"pat tokens?|token lifecycle|scoped keys?|read[- ]?only keys?|write keys?|webhook signing secrets?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:api[_ -]?keys?|access[_ -]?keys?|developer[_ -]?keys?|integration[_ -]?keys?|"
    r"service[_ -]?keys?|secret[_ -]?keys?|credentials?|tokens?|pat|lifecycle|scope|scopes|"
    r"permission|expiration|expiry|ttl|rotation|rotate|revocation|revoke|disable|last[_ -]?used|"
    r"audit|logging|security|integration|recovery|regenerate|source[_ -]?payload|requirements?)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|create|generate|issue|provision|scope|restrict|limit|"
    r"expire|expires?|ttl|rotate|rotation|revoke|revocation|disable|delete|deactivate|"
    r"last[- ]?used|audit|log|record|recover|recovery|regenerate|customer|guidance|"
    r"acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:api keys?|access keys?|developer keys?|integration keys?|secret keys?|personal access tokens?|"
    r"key lifecycle|key rotation|key revocation|key scopes?|last[- ]?used|audit logging|recovery guidance)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:api keys?|access keys?|developer keys?|integration keys?|secret keys?|personal access tokens?|"
    r"key lifecycle|key rotation|key revocation|key scopes?|last[- ]?used|audit logging|recovery guidance)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_API_KEYS_RE = re.compile(
    r"\b(?:no api key|api keys? are out of scope|access keys? are out of scope|"
    r"developer keys? are out of scope|key lifecycle is out of scope|no key lifecycle work)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:keyboard shortcut|translation key|cache key|primary key|foreign key|key value|"
    r"legend key|map key|license key copy|key result|object key|sort key)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:\d+\s*(?:minutes?|hours?|days?|weeks?|months?|years?)|read[- ]?only|write|admin|"
    r"billing|webhook|per[- ]?environment|project|workspace|organization|service account|"
    r"last used|audit log|customer support|regenerate|rotation window|ttl)\b",
    re.I,
)
_DURATION_RE = re.compile(r"\b\d+\s*(?:minutes?|hours?|days?|weeks?|months?|years?)\b", re.I)
_SCOPE_DETAIL_RE = re.compile(
    r"\b(?:scope|scopes|scoped|permission|permissions|read[- ]?only|write|admin|billing|"
    r"least privilege|environment|project|workspace|organization|service account)\b",
    re.I,
)
_ROTATION_REVOKE_DETAIL_RE = re.compile(
    r"\b(?:rotate|rotation|rotated|regenerate|reissue|revoke|revocation|disable|delete|deactivate|compromise)\b",
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
    "status",
    "created_by",
    "updated_by",
    "owner",
    "last_editor",
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
    "authentication",
    "auth_requirements",
    "security",
    "authorization",
    "integrations",
    "api",
    "api_keys",
    "access_keys",
    "developer_keys",
    "integration_keys",
    "credentials",
    "tokens",
    "audit",
    "support",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[ApiKeyLifecycleCategory, re.Pattern[str]] = {
    "key_creation": re.compile(
        r"\b(?:create|creation|generate|generated|issue|issued|provision|mint|new api keys?|"
        r"developer keys?|create keys?|key name|key secret|show secret once)\b",
        re.I,
    ),
    "key_scoping": re.compile(
        r"\b(?:scope|scopes|scoped|permission|permissions|least privilege|read[- ]?only|write|admin|"
        r"billing|environment|project|workspace|organization|service account|restricted keys?)\b",
        re.I,
    ),
    "key_expiration": re.compile(
        r"\b(?:expir(?:e|es|y|ation)|ttl|time[- ]?to[- ]?live|lifetime|valid for|"
        r"\d+\s*(?:minutes?|hours?|days?|weeks?|months?|years?))\b",
        re.I,
    ),
    "key_rotation": re.compile(r"\b(?:rotate|rotation|rotated|regenerate|reissue|roll keys?|key rollover)\b", re.I),
    "key_revocation": re.compile(
        r"\b(?:revoke|revocation|revoked|disable|delete|deactivate|invalidate|compromised key|remove key)\b",
        re.I,
    ),
    "last_used_visibility": re.compile(
        r"\b(?:last[- ]?used|last usage|last seen|usage timestamp|used at|key usage|unused keys?)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit log|audit trail|audited|logged|logging|key events?|record actor|actor and timestamp|"
        r"created by|rotated by|revoked by)\b",
        re.I,
    ),
    "recovery_guidance": re.compile(
        r"\b(?:recovery guidance|customer[- ]?facing recovery|recover|recovery|regenerate|lost secret|"
        r"compromised key guidance|customer support|help docs?|runbook)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[ApiKeyLifecycleCategory, tuple[str, ...]] = {
    "key_creation": ("developer_experience", "security"),
    "key_scoping": ("authorization", "security"),
    "key_expiration": ("security", "developer_experience"),
    "key_rotation": ("security", "integrations"),
    "key_revocation": ("security", "developer_experience"),
    "last_used_visibility": ("developer_experience", "analytics"),
    "audit_logging": ("security", "compliance"),
    "recovery_guidance": ("support", "developer_experience"),
}
_PLANNING_NOTES: dict[ApiKeyLifecycleCategory, tuple[str, ...]] = {
    "key_creation": ("Define who can create API keys, how secrets are displayed, and naming or ownership metadata.",),
    "key_scoping": ("Map key scopes, permissions, resource boundaries, and least-privilege defaults.",),
    "key_expiration": ("Specify optional or required expiration windows, TTL behavior, and expired-key handling.",),
    "key_rotation": ("Describe rotation, regeneration, overlap windows, and customer migration expectations.",),
    "key_revocation": ("Define revoke, disable, delete, and compromised-key invalidation behavior.",),
    "last_used_visibility": ("Expose last-used timestamps or usage visibility for stale-key review.",),
    "audit_logging": ("Record key lifecycle events with actor, key metadata, action, target, and timestamp.",),
    "recovery_guidance": ("Provide customer-facing guidance for lost, leaked, expired, or rotated keys.",),
}
_GAP_MESSAGES: dict[ApiKeyLifecycleMissingDetail, str] = {
    "missing_key_scopes": "Specify API key scopes, permissions, or resource boundaries.",
    "missing_rotation_or_revocation": "Specify API key rotation, regeneration, revocation, or disablement expectations.",
}


@dataclass(frozen=True, slots=True)
class SourceApiKeyLifecycleRequirement:
    """One source-backed API key lifecycle requirement."""

    category: ApiKeyLifecycleCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: ApiKeyLifecycleConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> ApiKeyLifecycleCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> ApiKeyLifecycleCategory:
        """Compatibility view for extractors that expose concern naming."""
        return self.category

    @property
    def suggested_plan_impacts(self) -> tuple[str, ...]:
        """Compatibility view matching adjacent source extractors."""
        return self.planning_notes

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "value": self.value,
            "suggested_owners": list(self.suggested_owners),
            "planning_notes": list(self.planning_notes),
            "gap_messages": list(self.gap_messages),
        }


@dataclass(frozen=True, slots=True)
class SourceApiKeyLifecycleRequirementsReport:
    """Source-level API key lifecycle requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceApiKeyLifecycleRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceApiKeyLifecycleRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceApiKeyLifecycleRequirement, ...]:
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
        """Return API key lifecycle requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Key Lifecycle Requirements Report"
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
            "- Missing detail flags: " + ", ".join(self.summary.get("missing_detail_flags", [])),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source API key lifecycle requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Value | Confidence | Source Field | Owners | Evidence | Planning Notes | Gap Messages |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
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
                f"{_markdown_cell('; '.join(requirement.planning_notes))} | "
                f"{_markdown_cell('; '.join(requirement.gap_messages))} |"
            )
        return "\n".join(lines)


def build_source_api_key_lifecycle_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceApiKeyLifecycleRequirementsReport:
    """Build an API key lifecycle requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    gap_flags = tuple(_missing_detail_flags(candidate.evidence for candidate in candidates))
    requirements = tuple(_merge_candidates(candidates, gap_flags))
    return SourceApiKeyLifecycleRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements, gap_flags if requirements else ()),
    )


def summarize_source_api_key_lifecycle_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceApiKeyLifecycleRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted API key lifecycle requirements."""
    if isinstance(source, SourceApiKeyLifecycleRequirementsReport):
        return dict(source.summary)
    return build_source_api_key_lifecycle_requirements(source).summary


def derive_source_api_key_lifecycle_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceApiKeyLifecycleRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_api_key_lifecycle_requirements(source)


def generate_source_api_key_lifecycle_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceApiKeyLifecycleRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_api_key_lifecycle_requirements(source)


def extract_source_api_key_lifecycle_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceApiKeyLifecycleRequirement, ...]:
    """Return API key lifecycle requirement records from brief-shaped input."""
    return build_source_api_key_lifecycle_requirements(source).requirements


def source_api_key_lifecycle_requirements_to_dict(
    report: SourceApiKeyLifecycleRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API key lifecycle requirements report to a plain dictionary."""
    return report.to_dict()


source_api_key_lifecycle_requirements_to_dict.__test__ = False


def source_api_key_lifecycle_requirements_to_dicts(
    requirements: (
        tuple[SourceApiKeyLifecycleRequirement, ...]
        | list[SourceApiKeyLifecycleRequirement]
        | SourceApiKeyLifecycleRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize API key lifecycle requirement records to dictionaries."""
    if isinstance(requirements, SourceApiKeyLifecycleRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_key_lifecycle_requirements_to_dicts.__test__ = False


def source_api_key_lifecycle_requirements_to_markdown(
    report: SourceApiKeyLifecycleRequirementsReport,
) -> str:
    """Render an API key lifecycle requirements report as Markdown."""
    return report.to_markdown()


source_api_key_lifecycle_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: ApiKeyLifecycleCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: ApiKeyLifecycleConfidence


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
        categories = _categories(searchable)
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
        if _NO_API_KEYS_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(
    candidates: Iterable[_Candidate],
    gap_flags: tuple[ApiKeyLifecycleMissingDetail, ...],
) -> list[SourceApiKeyLifecycleRequirement]:
    grouped: dict[ApiKeyLifecycleCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceApiKeyLifecycleRequirement] = []
    gap_messages = tuple(_GAP_MESSAGES[flag] for flag in gap_flags)
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
            SourceApiKeyLifecycleRequirement(
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
                planning_notes=_PLANNING_NOTES[category],
                gap_messages=gap_messages,
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
            if str(key) in _IGNORED_FIELDS:
                continue
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _API_KEY_CONTEXT_RE.search(key_text)
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
                _API_KEY_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
                if _NEGATED_SCOPE_RE.search(part) and _API_KEY_CONTEXT_RE.search(part)
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
    if _NO_API_KEYS_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _UNRELATED_RE.search(searchable) and not _API_KEY_CONTEXT_RE.search(searchable):
        return False
    if not (_API_KEY_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _API_KEY_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:created|generated|issued|scoped|expires?|rotated|revoked|disabled|logged|visible|recovered)\b",
            segment.text,
            re.I,
        )
    )


def _categories(searchable: str) -> list[ApiKeyLifecycleCategory]:
    categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]
    if "key_rotation" in categories and "recovery_guidance" in categories:
        if re.search(r"\b(?:recovery guidance|customer[- ]?facing recovery|lost secret|customer support|help docs?|runbook)\b", searchable, re.I):
            categories.remove("key_rotation")
    return categories


def _missing_detail_flags(evidence_values: Iterable[str]) -> list[ApiKeyLifecycleMissingDetail]:
    text = " ".join(evidence_values)
    if not text:
        return []
    flags: list[ApiKeyLifecycleMissingDetail] = []
    if not _SCOPE_DETAIL_RE.search(text):
        flags.append("missing_key_scopes")
    if not _ROTATION_REVOKE_DETAIL_RE.search(text):
        flags.append("missing_rotation_or_revocation")
    return [flag for flag in _MISSING_DETAIL_ORDER if flag in flags]


def _value(category: ApiKeyLifecycleCategory, text: str) -> str | None:
    if category == "key_expiration":
        if match := _DURATION_RE.search(text):
            return _clean_text(match.group(0)).casefold()
        if match := re.search(r"\b(?P<value>ttl|time[- ]?to[- ]?live|expiration window|key lifetime)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "key_scoping":
        if match := re.search(
            r"\b(?P<value>read[- ]?only|write|admin|billing|webhook|project|workspace|organization|environment|service account|least privilege)\b",
            text,
            re.I,
        ):
            return _clean_text(match.group("value")).casefold()
    if category in {"key_rotation", "recovery_guidance"}:
        if match := re.search(r"\b(?P<value>rotate|rotation|regenerate|reissue|recovery|customer support)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "key_revocation":
        if match := re.search(r"\b(?P<value>revoke|disable|delete|deactivate|compromised key)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "last_used_visibility":
        if match := re.search(r"\b(?P<value>last[- ]?used|last usage|last seen|usage timestamp|unused keys?)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "audit_logging":
        if match := re.search(r"\b(?P<value>audit log|audit trail|key events?|actor and timestamp)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if match := _VALUE_RE.search(text):
        return _clean_text(match.group(0)).casefold()
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    ranked_values = sorted(
        ((index, item.value) for index, item in enumerate(items) if item.value),
        key=lambda indexed_value: (
            0 if re.search(r"\d", indexed_value[1]) else 1,
            0 if _VALUE_RE.search(indexed_value[1]) or _DURATION_RE.search(indexed_value[1]) else 1,
            indexed_value[0],
            len(indexed_value[1]),
            indexed_value[1].casefold(),
        ),
    )
    values = _dedupe(value for _, value in ranked_values)
    return values[0] if values else None


def _confidence(segment: _Segment) -> ApiKeyLifecycleConfidence:
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
                "authorization",
                "security",
                "integration",
                "api",
                "key",
                "credential",
                "token",
                "audit",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _API_KEY_CONTEXT_RE.search(searchable):
        return "medium"
    if _API_KEY_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(
    requirements: tuple[SourceApiKeyLifecycleRequirement, ...],
    gap_flags: tuple[ApiKeyLifecycleMissingDetail, ...],
) -> dict[str, Any]:
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
        "missing_detail_flags": list(gap_flags),
        "missing_detail_counts": {
            flag: sum(1 for requirement in requirements if _GAP_MESSAGES[flag] in requirement.gap_messages)
            for flag in _MISSING_DETAIL_ORDER
        },
        "gap_messages": [_GAP_MESSAGES[flag] for flag in gap_flags],
        "status": "ready_for_planning" if requirements and not gap_flags else "needs_api_key_lifecycle_details" if requirements else "no_api_key_lifecycle_language",
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
        "authorization",
        "integrations",
        "api",
        "api_keys",
        "access_keys",
        "developer_keys",
        "integration_keys",
        "credentials",
        "tokens",
        "audit",
        "support",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: ApiKeyLifecycleCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[ApiKeyLifecycleCategory, tuple[str, ...]] = {
        "key_creation": ("creation", "create", "generate", "issue", "provision"),
        "key_scoping": ("scope", "permission", "role", "authorization"),
        "key_expiration": ("expiry", "expiration", "ttl", "lifetime"),
        "key_rotation": ("rotation", "rotate", "regenerate", "reissue"),
        "key_revocation": ("revocation", "revoke", "disable", "delete"),
        "last_used_visibility": ("last used", "usage", "stale", "visibility"),
        "audit_logging": ("audit", "log", "event", "timestamp"),
        "recovery_guidance": ("recovery", "guidance", "support", "docs", "runbook"),
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
    "ApiKeyLifecycleCategory",
    "ApiKeyLifecycleConfidence",
    "ApiKeyLifecycleMissingDetail",
    "SourceApiKeyLifecycleRequirement",
    "SourceApiKeyLifecycleRequirementsReport",
    "build_source_api_key_lifecycle_requirements",
    "derive_source_api_key_lifecycle_requirements",
    "extract_source_api_key_lifecycle_requirements",
    "generate_source_api_key_lifecycle_requirements",
    "summarize_source_api_key_lifecycle_requirements",
    "source_api_key_lifecycle_requirements_to_dict",
    "source_api_key_lifecycle_requirements_to_dicts",
    "source_api_key_lifecycle_requirements_to_markdown",
]
