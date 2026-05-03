"""Extract source-level sandbox and non-production environment requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SandboxEnvironmentRequirementCategory = Literal[
    "sandbox_environment",
    "staging_environment",
    "test_data",
    "refresh_cadence",
    "production_data_restriction",
    "feature_parity",
    "external_integration_sandbox",
]
SandboxEnvironmentRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[SandboxEnvironmentRequirementCategory, ...] = (
    "sandbox_environment",
    "staging_environment",
    "test_data",
    "refresh_cadence",
    "production_data_restriction",
    "feature_parity",
    "external_integration_sandbox",
)
_CONFIDENCE_ORDER: dict[SandboxEnvironmentRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_OWNER_SUGGESTIONS: dict[SandboxEnvironmentRequirementCategory, str] = {
    "sandbox_environment": "platform",
    "staging_environment": "release_engineering",
    "test_data": "qa",
    "refresh_cadence": "platform",
    "production_data_restriction": "security",
    "feature_parity": "engineering",
    "external_integration_sandbox": "integrations",
}
_PLANNING_NOTES: dict[SandboxEnvironmentRequirementCategory, str] = {
    "sandbox_environment": "Provision source-like sandbox access, credentials, isolation, ownership, and reset expectations before execution planning.",
    "staging_environment": "Confirm staging promotion path, deployment gates, seed state, access control, and validation ownership.",
    "test_data": "Define fixture accounts, synthetic datasets, anonymization needs, data seeding, and scenario coverage.",
    "refresh_cadence": "Document refresh schedule, source of refreshed state, blackout windows, and reset procedures.",
    "production_data_restriction": "Confirm production data handling boundaries, masking or synthetic-only rules, approvals, and audit expectations.",
    "feature_parity": "Compare sandbox and staging parity against production behavior, configuration, feature flags, and integrations.",
    "external_integration_sandbox": "Coordinate third-party sandbox tenants, API keys, callbacks, rate limits, and provider-specific test fixtures.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_ENVIRONMENT_CONTEXT_RE = re.compile(
    r"\b(?:sandbox(?:es)?|staging|stage env(?:ironment)?|test env(?:ironment)?|testing environment|"
    r"non[- ]?prod(?:uction)?|pre[- ]?prod(?:uction)?|uat|qa environment|dev environment|"
    r"integration sandbox|provider sandbox|external sandbox|third[- ]party sandbox|test tenant|"
    r"test account|fixture|seed data|synthetic data|masked data|anonymi[sz]ed data|"
    r"production data|prod data|refresh cadence|data refresh|environment refresh|feature parity)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:sandbox|staging|stage|environment|env|non[_ -]?prod|pre[_ -]?prod|uat|qa|test[_ -]?data|"
    r"fixtures?|seed|synthetic|masked|anonymi[sz]ed|refresh|cadence|parity|production[_ -]?data|"
    r"integration|provider|external|acceptance|requirements?|validation|source[_ -]?payload|metadata)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|support|"
    r"provide|provision|create|configure|define|document|maintain|refresh|seed|mask|anonymi[sz]e|"
    r"prohibit|forbid|avoid|cannot|must not|should not|do not|only|available|acceptance|done when)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,120}\b(?:sandbox|staging|non[- ]?prod(?:uction)?|test environment|"
    r"uat|qa environment|integration sandbox|test data|fixture|refresh|feature parity)\b"
    r".{0,120}\b(?:required|needed|in scope|planned|changes?|work|requirements?)\b|"
    r"\b(?:sandbox|staging|non[- ]?prod(?:uction)?|test environment|uat|qa environment|"
    r"integration sandbox|test data|fixture|refresh|feature parity)\b.{0,120}\b(?:not required|"
    r"not needed|out of scope|no changes?|no work|non[- ]?goal|unneeded)\b",
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
    "compliance",
    "operations",
    "qa",
    "testing",
    "environments",
    "sandbox",
    "staging",
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
    "domain",
    "status",
}
_CATEGORY_PATTERNS: dict[SandboxEnvironmentRequirementCategory, re.Pattern[str]] = {
    "sandbox_environment": re.compile(
        r"\b(?:sandbox(?:es)?|source sandbox|isolated sandbox|non[- ]?production sandbox|test sandbox|"
        r"sandbox tenant|sandbox account|sandbox access|sandbox credentials)\b",
        re.I,
    ),
    "staging_environment": re.compile(
        r"\b(?:staging|stage env(?:ironment)?|pre[- ]?prod(?:uction)?|uat|qa environment|"
        r"release candidate environment|validation environment)\b",
        re.I,
    ),
    "test_data": re.compile(
        r"\b(?:test data|fixture(?:s)?|seed data|seeded accounts?|test accounts?|demo accounts?|"
        r"synthetic data|mock data|masked data|anonymi[sz]ed data|sample dataset|golden dataset)\b",
        re.I,
    ),
    "refresh_cadence": re.compile(
        r"\b(?:refresh cadence|refresh schedule|data refresh|environment refresh|refreshed (?:daily|weekly|monthly|nightly)|"
        r"daily refresh|weekly refresh|monthly refresh|nightly refresh|reset cadence|reseed(?:ed|ing)?|"
        r"refresh every \d+|every \d+\s*(?:hours?|days?|weeks?)|on demand refresh)\b",
        re.I,
    ),
    "production_data_restriction": re.compile(
        r"\b(?:no production data|no prod data|do not use production data|must not use production data|"
        r"production data (?:is )?(?:prohibited|forbidden|not allowed|restricted)|prod data (?:is )?(?:prohibited|forbidden|not allowed|restricted)|"
        r"customer data (?:must be )?(?:masked|anonymi[sz]ed|synthetic only)|synthetic data only|"
        r"masked production data|anonymi[sz]ed production data|pii (?:must be )?(?:masked|removed|anonymi[sz]ed))\b",
        re.I,
    ),
    "feature_parity": re.compile(
        r"\b(?:feature parity|production parity|prod parity|parity with production|same as production|"
        r"matches? production|mirror(?:s|ing)? production|production-like|prod-like|staging parity|"
        r"configuration parity|flag parity|integration parity)\b",
        re.I,
    ),
    "external_integration_sandbox": re.compile(
        r"\b(?:external integration sandbox|integration sandbox|provider sandbox|third[- ]party sandbox|"
        r"partner sandbox|vendor sandbox|api sandbox|sandbox api keys?|sandbox credentials|"
        r"stripe sandbox|adyen sandbox|braintree sandbox|paypal sandbox|salesforce sandbox|shopify sandbox|"
        r"webhook sandbox|callback sandbox|test merchant|provider test account)\b",
        re.I,
    ),
}
_FIELD_CATEGORY_PATTERNS: dict[SandboxEnvironmentRequirementCategory, re.Pattern[str]] = {
    category: re.compile(category.replace("_", r"[_ -]?"), re.I) for category in _CATEGORY_ORDER
}
_ENVIRONMENT_TYPE_RE = re.compile(
    r"\b(?P<environment>sandbox(?:es)?|staging|stage env(?:ironment)?|test env(?:ironment)?|testing environment|"
    r"non[- ]?prod(?:uction)?|pre[- ]?prod(?:uction)?|uat|qa environment|dev environment|"
    r"integration sandbox|provider sandbox|partner sandbox|vendor sandbox|third[- ]party sandbox)\b",
    re.I,
)
_REFRESH_CADENCE_RE = re.compile(
    r"\b(?P<cadence>(?:daily|weekly|monthly|nightly|hourly|on[- ]demand) refresh|"
    r"refresh(?:ed)? (?:daily|weekly|monthly|nightly|hourly|on[- ]demand)|"
    r"refresh every \d+\s*(?:hours?|days?|weeks?)|every \d+\s*(?:hours?|days?|weeks?)|"
    r"reset (?:daily|weekly|monthly|nightly)|reseed(?:ed)? (?:daily|weekly|monthly|nightly))\b",
    re.I,
)
_INTEGRATION_RE = re.compile(
    r"\b(?P<integration>stripe|adyen|braintree|paypal|salesforce|shopify|netsuite|zendesk|twilio|sendgrid|"
    r"payment provider|identity provider|idp|crm|erp|webhook|callback|partner|vendor|third[- ]party provider)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class SourceSandboxEnvironmentRequirement:
    """One source-backed sandbox or non-production environment requirement."""

    source_brief_id: str | None
    category: SandboxEnvironmentRequirementCategory
    requirement_text: str
    environment_types: tuple[str, ...] = field(default_factory=tuple)
    production_data_restriction: bool = False
    refresh_cadence: str | None = None
    integration_dependency: str | None = None
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: SandboxEnvironmentRequirementConfidence = "medium"
    owner_suggestion: str = ""
    planning_note: str = ""

    @property
    def requirement_category(self) -> SandboxEnvironmentRequirementCategory:
        """Compatibility alias matching category-oriented reports."""
        return self.category

    @property
    def planning_notes(self) -> tuple[str, ...]:
        """Compatibility alias for callers expecting plural planning notes."""
        return (self.planning_note,)

    @property
    def owner_suggestions(self) -> tuple[str, ...]:
        """Compatibility alias for callers expecting plural owner suggestions."""
        return (self.owner_suggestion,)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "category": self.category,
            "requirement_text": self.requirement_text,
            "environment_types": list(self.environment_types),
            "production_data_restriction": self.production_data_restriction,
            "refresh_cadence": self.refresh_cadence,
            "integration_dependency": self.integration_dependency,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "owner_suggestion": self.owner_suggestion,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceSandboxEnvironmentRequirementsReport:
    """Source-level sandbox environment requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceSandboxEnvironmentRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSandboxEnvironmentRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceSandboxEnvironmentRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as findings."""
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
        """Return sandbox environment requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Sandbox Environment Requirements Report"
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
            lines.extend(["", "No sandbox environment requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Category | Requirement | Environment Types | Production Data Restriction | Refresh Cadence | Integration Dependency | Source Field | Matched Terms | Confidence | Owner | Planning Note | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.category)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(', '.join(requirement.environment_types))} | "
                f"{_markdown_cell(str(requirement.production_data_restriction))} | "
                f"{_markdown_cell(requirement.refresh_cadence or '')} | "
                f"{_markdown_cell(requirement.integration_dependency or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(', '.join(requirement.matched_terms))} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.owner_suggestion)} | "
                f"{_markdown_cell(requirement.planning_note)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_sandbox_environment_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSandboxEnvironmentRequirementsReport:
    """Extract source-level sandbox environment requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceSandboxEnvironmentRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_sandbox_environment_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSandboxEnvironmentRequirementsReport:
    """Compatibility alias for building a sandbox environment requirements report."""
    return build_source_sandbox_environment_requirements(source)


def generate_source_sandbox_environment_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSandboxEnvironmentRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_sandbox_environment_requirements(source)


def derive_source_sandbox_environment_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceSandboxEnvironmentRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_sandbox_environment_requirements(source)


def summarize_source_sandbox_environment_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceSandboxEnvironmentRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted sandbox environment requirements."""
    if isinstance(source_or_result, SourceSandboxEnvironmentRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_sandbox_environment_requirements(source_or_result).summary


def source_sandbox_environment_requirements_to_dict(
    report: SourceSandboxEnvironmentRequirementsReport,
) -> dict[str, Any]:
    """Serialize a sandbox environment requirements report to a plain dictionary."""
    return report.to_dict()


source_sandbox_environment_requirements_to_dict.__test__ = False


def source_sandbox_environment_requirements_to_dicts(
    requirements: (
        tuple[SourceSandboxEnvironmentRequirement, ...]
        | list[SourceSandboxEnvironmentRequirement]
        | SourceSandboxEnvironmentRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize sandbox environment requirement records to dictionaries."""
    if isinstance(requirements, SourceSandboxEnvironmentRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_sandbox_environment_requirements_to_dicts.__test__ = False


def source_sandbox_environment_requirements_to_markdown(
    report: SourceSandboxEnvironmentRequirementsReport,
) -> str:
    """Render a sandbox environment requirements report as Markdown."""
    return report.to_markdown()


source_sandbox_environment_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    category: SandboxEnvironmentRequirementCategory
    requirement_text: str
    environment_types: tuple[str, ...]
    production_data_restriction: bool
    refresh_cadence: str | None
    integration_dependency: str | None
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: SandboxEnvironmentRequirementConfidence


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
    return _optional_text(payload.get("id")) or _optional_text(payload.get("source_brief_id")) or _optional_text(payload.get("source_id"))


def _candidates_for_briefs(brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            searchable = _searchable_text(segment.source_field, segment.text)
            if _NEGATED_SCOPE_RE.search(searchable):
                continue
            for category in _categories(segment):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        category=category,
                        requirement_text=_requirement_text(segment.text),
                        environment_types=_environment_types(searchable),
                        production_data_restriction=bool(_CATEGORY_PATTERNS["production_data_restriction"].search(searchable)),
                        refresh_cadence=_field_value_detail("refresh_cadence", segment.text)
                        or _field_value_detail("cadence", segment.text)
                        or _match_refresh_cadence(segment.text),
                        integration_dependency=_field_value_detail("integration_dependency", segment.text)
                        or _field_value_detail("provider", segment.text)
                        or _match_integration_dependency(segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        matched_terms=_matched_terms(category, searchable),
                        confidence=_confidence(category, segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceSandboxEnvironmentRequirement]:
    grouped: dict[tuple[str | None, SandboxEnvironmentRequirementCategory, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (candidate.source_brief_id, candidate.category, _dedupe_requirement_key(candidate.requirement_text)),
            [],
        ).append(candidate)

    requirements: list[SourceSandboxEnvironmentRequirement] = []
    for (_source_brief_id, category, _requirement_key), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceSandboxEnvironmentRequirement(
                source_brief_id=best.source_brief_id,
                category=category,
                requirement_text=best.requirement_text,
                environment_types=tuple(
                    sorted(_dedupe(environment for item in items for environment in item.environment_types), key=str.casefold)
                ),
                production_data_restriction=any(item.production_data_restriction for item in items),
                refresh_cadence=_first_detail(item.refresh_cadence for item in items),
                integration_dependency=_first_detail(item.integration_dependency for item in items),
                source_field=best.source_field,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                matched_terms=tuple(sorted(_dedupe(term for item in items for term in item.matched_terms), key=str.casefold)),
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                owner_suggestion=_OWNER_SUGGESTIONS[category],
                planning_note=_PLANNING_NOTES[category],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _CATEGORY_ORDER.index(requirement.category),
            _CONFIDENCE_ORDER[requirement.confidence],
            requirement.requirement_text.casefold(),
            requirement.source_field or "",
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


def _append_value(segments: list[_Segment], source_field: str, value: Any, section_context: bool) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        if _has_structured_shape(value):
            for evidence in _structured_segments(value):
                segments.append(_Segment(source_field, evidence, True))
            return
        for key in sorted(value, key=lambda item: str(item)):
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text) or _ENVIRONMENT_CONTEXT_RE.search(key_text))
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
            section_context = inherited_context or bool(_ENVIRONMENT_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_SCOPE_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = [part] if _ENVIRONMENT_CONTEXT_RE.search(part) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text and not _NEGATED_SCOPE_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _categories(segment: _Segment) -> tuple[SandboxEnvironmentRequirementCategory, ...]:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    field_categories = [category for category in _CATEGORY_ORDER if _FIELD_CATEGORY_PATTERNS[category].search(field_words)]
    text_categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(segment.text)]
    has_environment_context = bool(_ENVIRONMENT_CONTEXT_RE.search(searchable))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    has_explicit_category = bool(field_categories or text_categories)
    if not (has_environment_context or has_structured_context or has_explicit_category):
        return ()
    if not (_REQUIREMENT_RE.search(searchable) or has_structured_context):
        return ()
    categories = _dedupe(field_categories + text_categories)
    if "external_integration_sandbox" in categories and "sandbox_environment" in text_categories:
        categories = [category for category in categories if category != "sandbox_environment" or category in field_categories]
    if "production_data_restriction" in categories and "sandbox_environment" in text_categories:
        categories = [category for category in categories if category != "sandbox_environment" or category in field_categories]
    if "production_data_restriction" in categories and "test_data" in categories:
        production_restriction_only = bool(
            re.search(r"\b(?:synthetic data only|customer data (?:must be )?(?:masked|anonymi[sz]ed)|masked production data|anonymi[sz]ed production data)\b", segment.text, re.I)
        )
        if production_restriction_only:
            categories = [category for category in categories if category != "test_data" or category in field_categories]
    return tuple(categories)


def _confidence(category: SandboxEnvironmentRequirementCategory, segment: _Segment) -> SandboxEnvironmentRequirementConfidence:
    field_words = _field_words(segment.source_field)
    has_explicit_requirement = bool(_REQUIREMENT_RE.search(segment.text))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    has_category = bool(_CATEGORY_PATTERNS[category].search(segment.text) or _FIELD_CATEGORY_PATTERNS[category].search(field_words))
    has_detail = bool(_environment_types(segment.text) or _match_refresh_cadence(segment.text) or _match_integration_dependency(segment.text))
    if has_category and has_explicit_requirement and has_structured_context and has_detail:
        return "high"
    if has_category and (has_explicit_requirement or has_structured_context):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceSandboxEnvironmentRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "category_counts": {
            category: sum(1 for requirement in requirements if requirement.category == category)
            for category in _CATEGORY_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "categories": [
            category
            for category in _CATEGORY_ORDER
            if any(requirement.category == category for requirement in requirements)
        ],
        "environment_types": sorted(
            _dedupe(environment for requirement in requirements for environment in requirement.environment_types),
            key=str.casefold,
        ),
        "refresh_cadences": sorted(
            _dedupe(requirement.refresh_cadence for requirement in requirements if requirement.refresh_cadence),
            key=str.casefold,
        ),
        "integration_dependencies": sorted(
            _dedupe(requirement.integration_dependency for requirement in requirements if requirement.integration_dependency),
            key=str.casefold,
        ),
        "requires_production_data_restriction": any(requirement.production_data_restriction for requirement in requirements),
        "requires_test_data": any(requirement.category == "test_data" for requirement in requirements),
        "requires_refresh_cadence": any(requirement.category == "refresh_cadence" for requirement in requirements),
        "requires_feature_parity": any(requirement.category == "feature_parity" for requirement in requirements),
        "requires_external_integration_sandbox": any(
            requirement.category == "external_integration_sandbox" for requirement in requirements
        ),
        "status": "ready_for_sandbox_environment_planning" if requirements else "no_sandbox_environment_language",
    }


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    if any(isinstance(value, (Mapping, list, tuple, set)) for value in item.values()):
        return False
    return bool(
        keys
        & {
            "category",
            "requirement_category",
            "environment_type",
            "environment_types",
            "refresh_cadence",
            "integration_dependency",
            "production_data_restriction",
        }
    )


def _structured_segments(item: Mapping[str, Any]) -> list[str]:
    parts: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = ", ".join(_strings(value)) if isinstance(value, (list, tuple, set)) else _clean_text(value)
        if text:
            parts.append(f"{key}: {text}")
    return ["; ".join(parts)] if parts else []


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "domain",
        "target_user",
        "buyer",
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
        "product_surface",
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
        "compliance",
        "operations",
        "qa",
        "testing",
        "environments",
        "sandbox",
        "staging",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, int, int]:
    return (
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        int("[" in candidate.source_field),
        int(bool(_STRUCTURED_FIELD_RE.search(_field_words(candidate.source_field)))),
        len(candidate.evidence),
        -_source_index(candidate.source_field),
    )


def _matched_terms(category: SandboxEnvironmentRequirementCategory, text: str) -> tuple[str, ...]:
    return tuple(sorted(_dedupe(_clean_text(match.group(0)).casefold() for match in _CATEGORY_PATTERNS[category].finditer(text)), key=str.casefold))


def _environment_types(text: str) -> tuple[str, ...]:
    return tuple(sorted(_dedupe(_normalize_environment(match.group("environment")) for match in _ENVIRONMENT_TYPE_RE.finditer(text)), key=str.casefold))


def _normalize_environment(value: str) -> str:
    text = _clean_text(value).casefold().replace("_", " ").replace("-", " ")
    aliases = {
        "stage env": "staging",
        "stage environment": "staging",
        "testing environment": "test environment",
        "non production": "non-production",
        "non prod": "non-production",
        "pre production": "pre-production",
        "pre prod": "pre-production",
    }
    return aliases.get(text, text)


def _match_refresh_cadence(text: str) -> str | None:
    match = _REFRESH_CADENCE_RE.search(text)
    if not match:
        return None
    return _clean_text(match.group("cadence")).rstrip(".").casefold().replace("-", " ")


def _match_integration_dependency(text: str) -> str | None:
    if not _CATEGORY_PATTERNS["external_integration_sandbox"].search(text):
        return None
    match = _INTEGRATION_RE.search(text)
    if not match:
        return "external integration"
    return _clean_text(match.group("integration")).rstrip(".").casefold()


def _field_value_detail(field_name: str, text: str) -> str | None:
    pattern = re.compile(rf"\b{re.escape(field_name)}:\s*([^;]+)", re.I)
    if not (match := pattern.search(text)):
        return None
    return _clean_text(match.group(1)).rstrip(".").casefold()


def _first_detail(values: Iterable[str | None]) -> str | None:
    for value in values:
        if value:
            return value
    return None


def _source_index(source_field: str) -> int:
    match = re.search(r"\[(\d+)\]", source_field)
    return int(match.group(1)) if match else 0


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _clean_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _clean_text(value)
    return [text] if text else []


def _field_words(source_field: str) -> str:
    value = source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")
    return re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)


def _searchable_text(source_field: str, text: str) -> str:
    return f"{_field_words(source_field)} {text}".replace("_", " ").replace("-", " ")


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


def _requirement_text(value: str) -> str:
    text = _clean_text(value)
    if len(text) > 220:
        text = f"{text[:217].rstrip()}..."
    return text


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


def _dedupe_requirement_key(value: str) -> str:
    text = _clean_text(value).casefold()
    return _SPACE_RE.sub(" ", text).strip()


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
    "SandboxEnvironmentRequirementCategory",
    "SandboxEnvironmentRequirementConfidence",
    "SourceSandboxEnvironmentRequirement",
    "SourceSandboxEnvironmentRequirementsReport",
    "build_source_sandbox_environment_requirements",
    "derive_source_sandbox_environment_requirements",
    "extract_source_sandbox_environment_requirements",
    "generate_source_sandbox_environment_requirements",
    "source_sandbox_environment_requirements_to_dict",
    "source_sandbox_environment_requirements_to_dicts",
    "source_sandbox_environment_requirements_to_markdown",
    "summarize_source_sandbox_environment_requirements",
]
