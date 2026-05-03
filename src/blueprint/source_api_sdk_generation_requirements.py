"""Extract source-level API SDK generation and client library requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceApiSdkGenerationRequirementCategory = Literal[
    "sdk_language_targets",
    "sdk_generation_tooling",
    "sdk_versioning",
    "sdk_package_distribution",
    "sdk_documentation",
    "sdk_authentication_helpers",
    "sdk_retry_logic",
    "sdk_example_code",
]
SourceApiSdkGenerationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[SourceApiSdkGenerationRequirementCategory, ...] = (
    "sdk_language_targets",
    "sdk_generation_tooling",
    "sdk_versioning",
    "sdk_package_distribution",
    "sdk_documentation",
    "sdk_authentication_helpers",
    "sdk_retry_logic",
    "sdk_example_code",
)
_CONFIDENCE_ORDER: dict[SourceApiSdkGenerationConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_PLANNING_NOTES: dict[SourceApiSdkGenerationRequirementCategory, str] = {
    "sdk_language_targets": "Specify target languages (Python, JavaScript, Go, Java, Ruby) for SDK generation and prioritize based on user base.",
    "sdk_generation_tooling": "Configure SDK generation tooling (OpenAPI Generator, Swagger Codegen) with templates and customization options.",
    "sdk_versioning": "Define SDK versioning strategy aligned with API versions, semantic versioning, and backward compatibility.",
    "sdk_package_distribution": "Set up SDK package distribution channels (npm, PyPI, RubyGems, Maven) with publishing workflows and credentials.",
    "sdk_documentation": "Generate SDK documentation from OpenAPI specs, include usage examples, and publish to documentation sites.",
    "sdk_authentication_helpers": "Implement authentication helper methods in SDKs for API keys, bearer tokens, and OAuth flows.",
    "sdk_retry_logic": "Add retry logic with exponential backoff, jitter, and configurable retry policies in SDK clients.",
    "sdk_example_code": "Include SDK example code for common operations, quickstart guides, and integration patterns.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SDK_CONTEXT_RE = re.compile(
    r"\b(?:sdk(?:s)?|client library(?:ies)?|api client(?:s)?|client generation|"
    r"sdk generation|generate(?:d)? sdk(?:s)?|code generation|client code|"
    r"language binding(?:s)?|api binding(?:s)?|openapi generator|swagger codegen|"
    r"npm package|pypi package|rubygems|maven|package distribution|"
    r"sdk versioning|sdk documentation|authentication helper(?:s)?|retry logic|"
    r"retry polic(?:y|ies)|exponential backoff|example code|code sample(?:s)?|"
    r"quickstart(?:s)?|sdk example(?:s)?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:sdk(?:s)?|client(?:s)?|library(?:ies)?|generation|tooling|versioning|"
    r"distribution|package(?:s)?|documentation|api|requirements?|constraints?|"
    r"acceptance|metadata|source[_ -]?payload|implementation[_ -]?notes)",
    re.I,
)
_DIRECTIVE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|enable|configure|provide|document|define|implement|generate|"
    r"publish|distribute|include|add|create|build|"
    r"cannot ship|before launch|done when|acceptance)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|outside scope|non[- ]?goal|defer|deferred)\b"
    r".{0,160}\b(?:sdk(?:s)?|client library(?:ies)?|api client(?:s)?|sdk generation|"
    r"code generation|openapi generator|swagger codegen|package distribution|"
    r"sdk versioning|sdk documentation)\b"
    r".{0,160}\b(?:required|needed|in scope|supported|support|work|changes?|planned|"
    r"requirements?)?\b|"
    r"\b(?:sdk(?:s)?|client library(?:ies)?|api client(?:s)?|sdk generation|"
    r"code generation|openapi generator|swagger codegen|package distribution|"
    r"sdk versioning|sdk documentation)\b"
    r".{0,160}\b(?:out of scope|outside scope|not required|not needed|no support|"
    r"unsupported|no work|non[- ]?goal|deferred)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[SourceApiSdkGenerationRequirementCategory, re.Pattern[str]] = {
    "sdk_language_targets": re.compile(
        r"\b(?:python sdk|javascript sdk|go sdk|java sdk|ruby sdk|php sdk|"
        r"typescript sdk|node\.?js sdk|dotnet sdk|c# sdk|swift sdk|kotlin sdk|"
        r"language target(?:s)?|target language(?:s)?|languages?:\s*(?:python|javascript|go|java|ruby)|"
        r"sdk for (?:python|javascript|go|java|ruby)|"
        r"(?:python|javascript|go|java|ruby|php|typescript|swift|kotlin) (?:client|binding(?:s)?|library(?:ies)?)|"
        r"sdk(?:s)? for (?:python|javascript|typescript|go|java|ruby|php|swift|kotlin)|"
        r"generate(?:d)? (?:python|javascript|typescript|go|java|ruby|php|swift|kotlin) sdk(?:s)?|"
        r"(?:python|javascript|typescript|go|java|ruby|php|swift|kotlin)(?:,|\sand\s).{0,80}(?:sdk(?:s)?|client(?:s)?|language(?:s)?))\b",
        re.I,
    ),
    "sdk_generation_tooling": re.compile(
        r"\b(?:openapi generator|swagger codegen|swagger[- ]?codegen|"
        r"sdk generator|code generator|client generator|generator tool(?:s)?|"
        r"autorest|smithy|protoc|grpc[- ]?tools|api[- ]?generator|"
        r"generate(?:d)? (?:from|using) (?:openapi|swagger|spec)|"
        r"generator template(?:s)?|custom generator|mustache template(?:s)?)\b",
        re.I,
    ),
    "sdk_versioning": re.compile(
        r"\b(?:sdk version(?:ing)?|client version(?:ing)?|version strategy|"
        r"semantic versioning|semver|version alignment|api version alignment|"
        r"version compatibility|backward(?:s)?[- ]?compatible version(?:ing)?|"
        r"version bump|major version|minor version|patch version|"
        r"sdk v\d+|client v\d+|version sync)\b",
        re.I,
    ),
    "sdk_package_distribution": re.compile(
        r"\b(?:npm package|pypi package|rubygems|maven central|nuget|"
        r"package distribution|package publishing|publish to (?:npm|pypi|rubygems|maven)|"
        r"package registry(?:ies)?|package manager|distribution channel(?:s)?|"
        r"publish sdk|publish client|package release|package workflow|"
        r"packagecloud|jfrog|artifactory|github packages|cocoapods|packagist|"
        r"publish (?:python|javascript|go|java|ruby) to (?:npm|pypi|rubygems|maven)|"
        r"(?:npm|pypi|rubygems|maven|nuget|cocoapods|packagist) for (?:python|javascript|go|java|ruby|php|swift)|"
        r"distribution.{0,60}(?:npm|pypi|rubygems|maven|github))\b",
        re.I,
    ),
    "sdk_documentation": re.compile(
        r"\b(?:sdk documentation|client documentation|api documentation|"
        r"generated documentation|auto[- ]?generated doc(?:s)?|docstring(?:s)?|"
        r"jsdoc|pydoc|javadoc|rdoc|godoc|inline documentation|"
        r"documentation site|docs site|readme|api reference|"
        r"usage guide(?:s)?|integration guide(?:s)?)\b",
        re.I,
    ),
    "sdk_authentication_helpers": re.compile(
        r"\b(?:authentication helper(?:s)?|auth helper(?:s)?|auth method(?:s)?|"
        r"api key helper(?:s)?|bearer token helper(?:s)?|oauth helper(?:s)?|"
        r"credential(?:s)? helper(?:s)?|authenticate method|auth configuration|"
        r"authentication wrapper|auth interceptor(?:s)?|credential provider(?:s)?|"
        r"token refresh|authentication flow)\b",
        re.I,
    ),
    "sdk_retry_logic": re.compile(
        r"\b(?:retry logic|retry polic(?:y|ies)|retry mechanism|retry strategy(?:ies)?|"
        r"exponential backoff|backoff strategy(?:ies)?|jitter|retry attempt(?:s)?|"
        r"max(?:imum)? retr(?:y|ies)|retry configuration|automatic retry(?:ies)?|"
        r"transient error retry(?:ies)?|retry on failure|idempotent retry(?:ies)?|"
        r"circuit breaker)\b",
        re.I,
    ),
    "sdk_example_code": re.compile(
        r"\b(?:example code|code example(?:s)?|usage example(?:s)?|sample code|"
        r"code sample(?:s)?|quickstart|getting started|tutorial(?:s)?|"
        r"integration example(?:s)?|example script(?:s)?|demo code|"
        r"sample application(?:s)?|reference implementation(?:s)?|cookbook|"
        r"snippet(?:s)?|code snippet(?:s)?)\b",
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
    "api",
    "sdk",
    "sdks",
    "client",
    "clients",
    "distribution",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)


@dataclass(frozen=True, slots=True)
class SourceApiSdkGenerationRequirement:
    """One source-backed API SDK generation requirement."""

    category: SourceApiSdkGenerationRequirementCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: SourceApiSdkGenerationConfidence = "medium"
    planning_note: str = ""
    unresolved_questions: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> SourceApiSdkGenerationRequirementCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
            "unresolved_questions": list(self.unresolved_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceApiSdkGenerationRequirementsReport:
    """Source-level API SDK generation requirements report."""

    source_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceApiSdkGenerationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceApiSdkGenerationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceApiSdkGenerationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as findings."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "title": self.title,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
            "findings": [finding.to_dict() for finding in self.findings],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return API SDK generation requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API SDK Generation Requirements Report"
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
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Category counts: "
            + ", ".join(f"{category} {category_counts.get(category, 0)}" for category in _CATEGORY_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No source API SDK generation requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Category | Confidence | Source | Evidence | Planning Note | Unresolved Questions |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.category} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell(requirement.source_field)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.planning_note)} | "
                f"{_markdown_cell('; '.join(requirement.unresolved_questions))} |"
            )
        return "\n".join(lines)


def build_source_api_sdk_generation_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceApiSdkGenerationRequirementsReport:
    """Build an API SDK generation requirements report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    requirements = tuple(_merge_candidates(candidates))
    return SourceApiSdkGenerationRequirementsReport(
        source_id=source_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def build_source_api_sdk_generation_requirements_report(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceApiSdkGenerationRequirementsReport:
    """Compatibility helper for callers that use explicit report naming."""
    return build_source_api_sdk_generation_requirements(source)


def generate_source_api_sdk_generation_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceApiSdkGenerationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_api_sdk_generation_requirements(source)


def derive_source_api_sdk_generation_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceApiSdkGenerationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_api_sdk_generation_requirements(source)


def extract_source_api_sdk_generation_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[SourceApiSdkGenerationRequirement, ...]:
    """Return API SDK generation requirement records extracted from brief-shaped input."""
    return build_source_api_sdk_generation_requirements(source).requirements


def summarize_source_api_sdk_generation_requirements(
    source_or_result: (
        str
        | Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceApiSdkGenerationRequirementsReport
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic API SDK generation requirements summary."""
    if isinstance(source_or_result, SourceApiSdkGenerationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_api_sdk_generation_requirements(source_or_result).summary


def source_api_sdk_generation_requirements_to_dict(
    report: SourceApiSdkGenerationRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API SDK generation requirements report to a plain dictionary."""
    return report.to_dict()


source_api_sdk_generation_requirements_to_dict.__test__ = False


def source_api_sdk_generation_requirements_to_dicts(
    requirements: tuple[SourceApiSdkGenerationRequirement, ...]
    | list[SourceApiSdkGenerationRequirement]
    | SourceApiSdkGenerationRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize source API SDK generation requirement records to dictionaries."""
    if isinstance(requirements, SourceApiSdkGenerationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_sdk_generation_requirements_to_dicts.__test__ = False


def source_api_sdk_generation_requirements_to_markdown(
    report: SourceApiSdkGenerationRequirementsReport,
) -> str:
    """Render an API SDK generation requirements report as Markdown."""
    return report.to_markdown()


source_api_sdk_generation_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: SourceApiSdkGenerationRequirementCategory
    source_field: str
    evidence: str
    confidence: SourceApiSdkGenerationConfidence


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
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


def _requirement_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        if _is_out_of_scope(segment):
            continue
        searchable = _searchable_text(segment.source_field, segment.text)
        categories: list[SourceApiSdkGenerationRequirementCategory] = [
            category
            for category in _CATEGORY_ORDER
            if _CATEGORY_PATTERNS[category].search(searchable)
        ]
        if not categories or not _is_requirement(segment, categories):
            continue
        evidence = _evidence_snippet(segment.source_field, segment.text)
        for category in _dedupe(categories):
            candidates.append(
                _Candidate(
                    category=category,
                    source_field=segment.source_field,
                    evidence=evidence,
                    confidence=_confidence(category, segment),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceApiSdkGenerationRequirement]:
    grouped: dict[SourceApiSdkGenerationRequirementCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceApiSdkGenerationRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        best = min(items, key=_candidate_sort_key)
        evidence = tuple(_dedupe_evidence(item.evidence for item in sorted(items, key=_candidate_sort_key)))[:6]
        requirements.append(
            SourceApiSdkGenerationRequirement(
                category=category,
                source_field=best.source_field,
                evidence=evidence,
                confidence=best.confidence,
                planning_note=_PLANNING_NOTES[category],
                unresolved_questions=tuple(_unresolved_questions(category, items)),
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
                _STRUCTURED_FIELD_RE.search(key_text) or _SDK_CONTEXT_RE.search(key_text)
            )
            child = value[key]
            if child_context and not isinstance(child, (Mapping, list, tuple, set)):
                if text := _optional_text(child):
                    for segment_text, segment_context in _segments(f"{key_text}: {text}", child_context):
                        segments.append(_Segment(child_field, segment_text, segment_context))
                continue
            _append_value(segments, child_field, child, child_context)
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
            section_context = inherited_context or bool(_SDK_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = [part] if _NEGATED_SCOPE_RE.search(part) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text:
                    segments.append((text, section_context))
    return segments


def _is_requirement(
    segment: _Segment,
    categories: Iterable[SourceApiSdkGenerationRequirementCategory],
) -> bool:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    if not (_SDK_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    if _DIRECTIVE_RE.search(segment.text) or segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(categories)


def _has_global_no_scope(payload: Mapping[str, Any]) -> bool:
    for segment in _candidate_segments(payload):
        root_field = segment.source_field.split("[", 1)[0].split(".", 1)[0]
        if root_field not in {"title", "summary", "body", "description", "scope", "non_goals", "constraints", "source_payload"}:
            continue
        if _is_out_of_scope(segment):
            return True
    return False


def _is_out_of_scope(segment: _Segment) -> bool:
    searchable = _searchable_text(segment.source_field, segment.text)
    return bool(_NEGATED_SCOPE_RE.search(searchable))


def _confidence(
    category: SourceApiSdkGenerationRequirementCategory,
    segment: _Segment,
) -> SourceApiSdkGenerationConfidence:
    searchable = _searchable_text(segment.source_field, segment.text)
    score = 0
    if _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        score += 1
    if segment.section_context or _SDK_CONTEXT_RE.search(searchable):
        score += 1
    if _DIRECTIVE_RE.search(segment.text):
        score += 1
    if _CATEGORY_PATTERNS[category].search(searchable):
        score += 1
    return "high" if score >= 3 else "medium" if score >= 2 else "low"


def _unresolved_questions(
    category: SourceApiSdkGenerationRequirementCategory,
    items: Iterable[_Candidate],
) -> list[str]:
    item_list = list(items)
    questions: list[str] = []
    if category == "sdk_language_targets" and not any(re.search(r"\b(?:python|javascript|go|java|ruby|php|typescript|swift|kotlin)\b", item.evidence, re.I) for item in item_list):
        questions.append("What specific programming languages should SDKs target (Python, JavaScript, Go, Java, Ruby)?")
    if category == "sdk_generation_tooling" and not any(re.search(r"\b(?:openapi generator|swagger codegen|autorest|smithy)\b", item.evidence, re.I) for item in item_list):
        questions.append("What SDK generation tooling should be used (OpenAPI Generator, Swagger Codegen, custom)?")
    if category == "sdk_versioning" and not any(re.search(r"\b(?:semver|semantic versioning|version strategy|alignment)\b", item.evidence, re.I) for item in item_list):
        questions.append("What SDK versioning strategy should be followed and how should it align with API versions?")
    if category == "sdk_package_distribution" and not any(re.search(r"\b(?:npm|pypi|rubygems|maven|nuget)\b", item.evidence, re.I) for item in item_list):
        questions.append("What package distribution channels should be used (npm, PyPI, RubyGems, Maven)?")
    if category == "sdk_documentation" and not any(re.search(r"\b(?:generated|auto|jsdoc|pydoc|javadoc|rdoc|godoc)\b", item.evidence, re.I) for item in item_list):
        questions.append("What documentation generation tooling should be used for SDK documentation?")
    if category == "sdk_authentication_helpers" and not any(re.search(r"\b(?:api key|bearer|oauth|credential|token)\b", item.evidence, re.I) for item in item_list):
        questions.append("What authentication mechanisms should SDK authentication helpers support?")
    if category == "sdk_retry_logic" and not any(re.search(r"\b(?:exponential|backoff|jitter|max|policy)\b", item.evidence, re.I) for item in item_list):
        questions.append("What retry policy should be implemented (exponential backoff, jitter, max retries)?")
    return questions[:3]


def _summary(requirements: tuple[SourceApiSdkGenerationRequirement, ...]) -> dict[str, Any]:
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
        "status": "ready_for_planning" if requirements else "no_api_sdk_generation_requirements_found",
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = ("id", "source_brief_id", "source_id", *_SCANNED_FIELDS)
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _searchable_text(source_field: str, text: str) -> str:
    return f"{_field_words(source_field)} {text}".replace("_", " ").replace("-", " ")


def _candidate_sort_key(item: _Candidate) -> tuple[int, int, str, str]:
    return (
        _CONFIDENCE_ORDER[item.confidence],
        _field_category_rank(item.category, item.source_field),
        item.source_field.casefold(),
        item.evidence.casefold(),
    )


def _field_category_rank(category: SourceApiSdkGenerationRequirementCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[SourceApiSdkGenerationRequirementCategory, tuple[str, ...]] = {
        "sdk_language_targets": ("sdk", "language", "target", "client"),
        "sdk_generation_tooling": ("generator", "tooling", "openapi", "swagger"),
        "sdk_versioning": ("version", "versioning", "semver"),
        "sdk_package_distribution": ("package", "distribution", "publish", "npm", "pypi"),
        "sdk_documentation": ("documentation", "docs", "jsdoc", "pydoc"),
        "sdk_authentication_helpers": ("authentication", "auth", "helper", "credential"),
        "sdk_retry_logic": ("retry", "backoff", "logic", "policy"),
        "sdk_example_code": ("example", "sample", "quickstart", "tutorial"),
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
    return deduped


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
        key = _clean_text(str(value)).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "SourceApiSdkGenerationConfidence",
    "SourceApiSdkGenerationRequirement",
    "SourceApiSdkGenerationRequirementCategory",
    "SourceApiSdkGenerationRequirementsReport",
    "build_source_api_sdk_generation_requirements",
    "build_source_api_sdk_generation_requirements_report",
    "derive_source_api_sdk_generation_requirements",
    "extract_source_api_sdk_generation_requirements",
    "generate_source_api_sdk_generation_requirements",
    "source_api_sdk_generation_requirements_to_dict",
    "source_api_sdk_generation_requirements_to_dicts",
    "source_api_sdk_generation_requirements_to_markdown",
    "summarize_source_api_sdk_generation_requirements",
]
