"""Extract source-level API key management requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ApiKeyManagementCategory = Literal[
    "key_creation",
    "one_time_secret_display",
    "scoped_permissions",
    "expiration",
    "rotation",
    "revocation",
    "audit_logging",
    "rate_limit_association",
    "environment_separation",
    "customer_ownership",
    "admin_ownership",
]
ApiKeyManagementConfidence = Literal["high", "medium", "low"]

_CATEGORY_ORDER: tuple[ApiKeyManagementCategory, ...] = (
    "key_creation",
    "one_time_secret_display",
    "scoped_permissions",
    "expiration",
    "rotation",
    "revocation",
    "audit_logging",
    "rate_limit_association",
    "environment_separation",
    "customer_ownership",
    "admin_ownership",
)
_CONFIDENCE_ORDER: dict[ApiKeyManagementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_CAPABILITY_BY_CATEGORY: dict[ApiKeyManagementCategory, str] = {
    "key_creation": "Allow authorized actors to create API keys with durable metadata and ownership context.",
    "one_time_secret_display": "Display newly generated API key secrets only once and require secure copy or regeneration flows.",
    "scoped_permissions": "Bind API keys to explicit scopes, permissions, products, or resources.",
    "expiration": "Support API key expiration dates, TTLs, and expired-key enforcement.",
    "rotation": "Support API key rotation, replacement, overlap windows, and stale key cleanup.",
    "revocation": "Allow API keys to be revoked, disabled, deleted, or deactivated with immediate enforcement.",
    "audit_logging": "Record API key lifecycle events in audit logs with actor, timestamp, and key metadata.",
    "rate_limit_association": "Associate API keys with rate limits, quotas, usage plans, or throttling policy.",
    "environment_separation": "Separate API keys by environment such as test, sandbox, staging, and production.",
    "customer_ownership": "Model customer, tenant, team, workspace, project, or account ownership for API keys.",
    "admin_ownership": "Model admin, operator, support, or internal owner controls for API key lifecycle management.",
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_API_KEY_CONTEXT_RE = re.compile(
    r"\b(?:api[- ]?keys?|developer[- ]?keys?|access[- ]?keys?|client[- ]?keys?|integration[- ]?keys?|secret[- ]?keys?|"
    r"api[- ]?token(?:s)?|developer[- ]?token(?:s)?|personal[- ]access[- ]token(?:s)?|pat(?:s)?|"
    r"key management|credential management|token management|developer credential(?:s)?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:api[_ -]?keys?|developer[_ -]?keys?|access[_ -]?keys?|integration[_ -]?keys?|"
    r"secret[_ -]?keys?|api[_ -]?tokens?|developer[_ -]?tokens?|personal[_ -]?access[_ -]?tokens?|"
    r"credential|token[_ -]?management|key[_ -]?management|scope|permission|secret|expiration|"
    r"expiry|ttl|rotation|rotate|revocation|revoke|audit|rate[_ -]?limit|quota|throttle|"
    r"environment|sandbox|production|customer|tenant|workspace|admin|owner|source[_ -]?payload)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|should|needs?|need(?:s)? to|needed|required|requires?|requirement|ensure|support|"
    r"allow|provide|define|enforce|create|generate|issue|display(?:ed)?|show|reveal|copy|scope|"
    r"permission|expire|rotate|revoke|disable|delete|deactivate|log|audit|record|associate|associated|"
    r"rate limit|quota|throttle|separate|own(?:er|ership)?|admin|customer|tenant|done when|"
    r"acceptance|before launch)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:api keys?|developer keys?|access keys?|api tokens?|personal access tokens?|"
    r"key management|credential management|scopes?|rotation|revocation|audit logs?)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:api keys?|developer keys?|access keys?|api tokens?|personal access tokens?|"
    r"key management|credential management|scopes?|rotation|revocation|audit logs?)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"no changes?|non[- ]?goal)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:keyboard keys?|license keys?|foreign keys?|cache keys?|translation keys?|ssh keys?|"
    r"key value|key-value|encryption keys?|kms keys?|primary keys?)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[ApiKeyManagementCategory, re.Pattern[str]] = {
    "key_creation": re.compile(
        r"\b(?:create|creation|generate|issue|provision|mint|new)\b.{0,80}\b(?:api keys?|developer keys?|access keys?|tokens?|credentials?)\b|"
        r"\b(?:api keys?|developer keys?|access keys?|tokens?|credentials?)\b.{0,80}\b(?:create|creation|generate|issue|provision|mint|new)\b",
        re.I,
    ),
    "one_time_secret_display": re.compile(
        r"\b(?:one[- ]?time|only once|single display|shown once|display once|reveal once|copy once|never shown again|"
        r"secret display|secret value|newly generated secret|plaintext secret)\b",
        re.I,
    ),
    "scoped_permissions": re.compile(
        r"\b(?:scoped keys?|scopes?|permissions?|permissioned|least privilege|read[- ]?only|write scope|"
        r"resource scope|product scope|endpoint scope|role[- ]?based|rbac)\b",
        re.I,
    ),
    "expiration": re.compile(
        r"\b(?:expiration|expiry|expires?|expire_at|expires_at|ttl|time[- ]?to[- ]?live|lifetime|valid until|"
        r"\d+\s*(?:minutes?|hours?|days?|weeks?|months?)\s+(?:ttl|lifetime|expiry|expiration))\b",
        re.I,
    ),
    "rotation": re.compile(
        r"\b(?:rotation|rotate|rotated|rotating|replacement keys?|rollover|overlap window|stale keys?|old keys?)\b",
        re.I,
    ),
    "revocation": re.compile(
        r"\b(?:revocation|revoke|revoked|disable|disabled|delete|deleted|deactivate|deactivated|terminate|kill switch)\b",
        re.I,
    ),
    "audit_logging": re.compile(
        r"\b(?:audit logs?|audit logging|audit trail|audit event|lifecycle logs?|activity logs?|log creation|log revocation|"
        r"logged with|record actor|record timestamp|created_by|revoked_by|rotated_by)\b",
        re.I,
    ),
    "rate_limit_association": re.compile(
        r"\b(?:rate limits?|rate limiting|quota|quotas|usage limits?|usage plans?|throttl(?:e|ing)|burst limit|"
        r"request limits?|per[- ]key limits?)\b",
        re.I,
    ),
    "environment_separation": re.compile(
        r"\b(?:environment separation|separate environments?|sandbox|test keys?|staging keys?|production keys?|"
        r"prod keys?|dev keys?|live keys?|environment[- ]specific|per environment)\b",
        re.I,
    ),
    "customer_ownership": re.compile(
        r"\b(?:customer[- ]owned|customer ownership|customer owner|tenant owner|tenant owned|account owner|"
        r"workspace owner|workspace owned|team owner|project owner|organization owner|org owner|per customer|"
        r"per tenant|customer admins?)\b",
        re.I,
    ),
    "admin_ownership": re.compile(
        r"\b(?:admin owned|admin ownership|admin owner|internal admin|operator|support admin|platform admin|"
        r"super admin|staff user|backoffice|admin console|administrator)\b",
        re.I,
    ),
}
_FIELD_CATEGORY_PATTERNS: dict[ApiKeyManagementCategory, re.Pattern[str]] = {
    "key_creation": re.compile(r"\b(?:create|creation|generate|issue|provision|mint|created)\b", re.I),
    "one_time_secret_display": re.compile(r"\b(?:one time|secret display|secret|display|reveal|copy)\b", re.I),
    "scoped_permissions": re.compile(r"\b(?:scope|scopes|permission|permissions|rbac|role)\b", re.I),
    "expiration": re.compile(r"\b(?:expiration|expiry|expires|ttl|lifetime)\b", re.I),
    "rotation": re.compile(r"\b(?:rotation|rotate|rollover|replacement)\b", re.I),
    "revocation": re.compile(r"\b(?:revocation|revoke|disable|delete|deactivate)\b", re.I),
    "audit_logging": re.compile(r"\b(?:audit|log|logging|activity)\b", re.I),
    "rate_limit_association": re.compile(r"\b(?:rate limit|quota|throttle|usage plan|request limit)\b", re.I),
    "environment_separation": re.compile(r"\b(?:environment|sandbox|staging|production|prod|test|dev|live)\b", re.I),
    "customer_ownership": re.compile(r"\b(?:customer|tenant|account|workspace|team|project|organization|org)\b", re.I),
    "admin_ownership": re.compile(r"\b(?:admin|administrator|operator|support|staff|backoffice)\b", re.I),
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
    "developer_experience",
    "api",
    "api_keys",
    "credentials",
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
class SourceApiKeyManagementRequirement:
    """One source-backed API key management requirement."""

    source_brief_id: str | None
    category: ApiKeyManagementCategory
    required_capability: str
    requirement_text: str
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: ApiKeyManagementConfidence = "medium"
    source_field: str | None = None
    source_fields: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> ApiKeyManagementCategory:
        """Compatibility alias for callers expecting requirement_category naming."""
        return self.category

    @property
    def requirement_type(self) -> ApiKeyManagementCategory:
        """Compatibility alias for callers expecting requirement_type naming."""
        return self.category

    @property
    def lifecycle_dimension(self) -> ApiKeyManagementCategory:
        """Compatibility alias for callers expecting lifecycle_dimension naming."""
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
            "source_field": self.source_field,
            "source_fields": list(self.source_fields),
            "matched_terms": list(self.matched_terms),
        }


@dataclass(frozen=True, slots=True)
class SourceApiKeyManagementRequirementsReport:
    """Source-level API key management requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceApiKeyManagementRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceApiKeyManagementRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceApiKeyManagementRequirement, ...]:
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
        """Return API key management requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Key Management Requirements Report"
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
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source API key management requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Required Capability | Requirement | Confidence | Source Field | Source Fields | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.category)} | "
                f"{_markdown_cell(requirement.required_capability)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(', '.join(requirement.source_fields))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_api_key_management_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceApiKeyManagementRequirementsReport:
    """Extract source-level API key management requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceApiKeyManagementRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_api_key_management_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceApiKeyManagementRequirementsReport:
    """Compatibility alias for building an API key management requirements report."""
    return build_source_api_key_management_requirements(source)


def generate_source_api_key_management_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceApiKeyManagementRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_api_key_management_requirements(source)


def derive_source_api_key_management_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceApiKeyManagementRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_api_key_management_requirements(source)


def summarize_source_api_key_management_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceApiKeyManagementRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted API key management requirements."""
    if isinstance(source_or_result, SourceApiKeyManagementRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_api_key_management_requirements(source_or_result).summary


def source_api_key_management_requirements_to_dict(
    report: SourceApiKeyManagementRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API key management requirements report to a plain dictionary."""
    return report.to_dict()


source_api_key_management_requirements_to_dict.__test__ = False


def source_api_key_management_requirements_to_dicts(
    requirements: (
        tuple[SourceApiKeyManagementRequirement, ...]
        | list[SourceApiKeyManagementRequirement]
        | SourceApiKeyManagementRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize API key management requirement records to dictionaries."""
    if isinstance(requirements, SourceApiKeyManagementRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_key_management_requirements_to_dicts.__test__ = False


def source_api_key_management_requirements_to_markdown(
    report: SourceApiKeyManagementRequirementsReport,
) -> str:
    """Render an API key management requirements report as Markdown."""
    return report.to_markdown()


source_api_key_management_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: ApiKeyManagementCategory
    requirement_text: str
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: ApiKeyManagementConfidence


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
            categories = _categories(segment)
            for category in categories:
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        requirement_text=_requirement_text(segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        matched_terms=tuple(_matched_terms(_CATEGORY_PATTERNS[category], searchable)),
                        confidence=_confidence(segment, category),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceApiKeyManagementRequirement]:
    grouped: dict[tuple[str | None, ApiKeyManagementCategory], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.category), []).append(candidate)

    requirements: list[SourceApiKeyManagementRequirement] = []
    for (source_brief_id, category), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceApiKeyManagementRequirement(
                source_brief_id=source_brief_id,
                category=category,
                required_capability=_CAPABILITY_BY_CATEGORY[category],
                requirement_text=best.requirement_text,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
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
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.source_field or "",
            requirement.requirement_text.casefold(),
            requirement.evidence,
        ),
    )


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    global_context = _brief_api_key_context(payload)
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
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text)
                or _API_KEY_CONTEXT_RE.search(key_text)
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
            section_context = inherited_context or bool(_API_KEY_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
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


def _categories(segment: _Segment) -> tuple[ApiKeyManagementCategory, ...]:
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    field_words = _field_words(segment.source_field)
    if _unrelated_only(searchable):
        return ()
    has_context = bool(
        _API_KEY_CONTEXT_RE.search(searchable)
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
    text_categories = [
        category
        for category in _CATEGORY_ORDER
        if _CATEGORY_PATTERNS[category].search(searchable)
    ]
    return tuple(_dedupe([*field_categories, *text_categories]))


def _confidence(segment: _Segment, category: ApiKeyManagementCategory) -> ApiKeyManagementConfidence:
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
                "developer_experience",
                "api",
                "api_keys",
                "credentials",
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


def _has_detail(category: ApiKeyManagementCategory, text: str) -> bool:
    detail_patterns: dict[ApiKeyManagementCategory, re.Pattern[str]] = {
        "key_creation": re.compile(r"\b(?:create|generate|issue|provision|metadata|name|prefix)\b", re.I),
        "one_time_secret_display": re.compile(r"\b(?:one[- ]?time|only once|copy|secret|never shown again)\b", re.I),
        "scoped_permissions": re.compile(r"\b(?:read|write|scope|permission|resource|endpoint|least privilege)\b", re.I),
        "expiration": re.compile(r"\b(?:ttl|expires?|days?|weeks?|months?|valid until)\b", re.I),
        "rotation": re.compile(r"\b(?:rotate|rotation|overlap|replacement|stale)\b", re.I),
        "revocation": re.compile(r"\b(?:revoke|disable|delete|deactivate|immediate)\b", re.I),
        "audit_logging": re.compile(r"\b(?:actor|timestamp|created_by|revoked_by|audit|log|event)\b", re.I),
        "rate_limit_association": re.compile(r"\b(?:rate|quota|usage|throttle|burst|request)\b", re.I),
        "environment_separation": re.compile(r"\b(?:sandbox|test|staging|production|prod|dev|live)\b", re.I),
        "customer_ownership": re.compile(r"\b(?:customer|tenant|workspace|account|team|project|organization)\b", re.I),
        "admin_ownership": re.compile(r"\b(?:admin|operator|support|staff|backoffice|console)\b", re.I),
    }
    return bool(detail_patterns[category].search(text))


def _summary(requirements: tuple[SourceApiKeyManagementRequirement, ...], source_count: int) -> dict[str, Any]:
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
        "status": "ready_for_api_key_management_planning" if requirements else "no_api_key_management_language",
    }


def _brief_out_of_scope(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("title", "summary", "scope", "non_goals", "constraints", "source_payload")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_NEGATED_RE.search(scoped_text))


def _brief_api_key_context(payload: Mapping[str, Any]) -> bool:
    scoped_text = " ".join(
        text
        for field_name in ("id", "source_id", "source_brief_id", "title", "domain", "summary", "workflow_context", "product_surface")
        if field_name in payload
        for text in _strings(payload.get(field_name))
    )
    return bool(_API_KEY_CONTEXT_RE.search(scoped_text) and not _NEGATED_RE.search(scoped_text))


def _unrelated_only(text: str) -> bool:
    return bool(_UNRELATED_RE.search(text) and not _API_KEY_CONTEXT_RE.search(text))


def _requirement_text(text: str) -> str:
    return _clean_text(text)[:300]


def _evidence_snippet(source_field: str, text: str) -> str:
    return f"{source_field}: {_clean_text(text)[:240]}"


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, str]:
    return (
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
    "ApiKeyManagementCategory",
    "ApiKeyManagementConfidence",
    "SourceApiKeyManagementRequirement",
    "SourceApiKeyManagementRequirementsReport",
    "build_source_api_key_management_requirements",
    "derive_source_api_key_management_requirements",
    "extract_source_api_key_management_requirements",
    "generate_source_api_key_management_requirements",
    "summarize_source_api_key_management_requirements",
    "source_api_key_management_requirements_to_dict",
    "source_api_key_management_requirements_to_dicts",
    "source_api_key_management_requirements_to_markdown",
]
