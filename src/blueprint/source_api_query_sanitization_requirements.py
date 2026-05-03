"""Extract source-level API query parameter sanitization requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ApiQuerySanitizationCategory = Literal[
    "sql_injection_prevention",
    "nosql_injection_prevention",
    "command_injection_prevention",
    "ldap_injection_prevention",
    "xpath_injection_prevention",
    "parameter_whitelisting",
    "input_encoding",
    "length_type_validation",
    "escaping_strategies",
    "test_coverage",
]
ApiQuerySanitizationMissingDetail = Literal[
    "missing_validation_library",
    "missing_sanitization_strategy",
]
ApiQuerySanitizationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[ApiQuerySanitizationCategory, ...] = (
    "sql_injection_prevention",
    "nosql_injection_prevention",
    "command_injection_prevention",
    "ldap_injection_prevention",
    "xpath_injection_prevention",
    "parameter_whitelisting",
    "input_encoding",
    "length_type_validation",
    "escaping_strategies",
    "test_coverage",
)
_MISSING_DETAIL_ORDER: tuple[ApiQuerySanitizationMissingDetail, ...] = (
    "missing_validation_library",
    "missing_sanitization_strategy",
)
_CONFIDENCE_ORDER: dict[ApiQuerySanitizationConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SANITIZATION_CONTEXT_RE = re.compile(
    r"\b(?:query parameter(?:s)?|query param(?:s)?|url parameter(?:s)?|query string|"
    r"sanitiz(?:e|ation|ing)|validat(?:e|ion|ing)|input validat(?:ion|ing)|"
    r"injection prevention|injection attack(?:s)?|sql injection|nosql injection|"
    r"command injection|ldap injection|xpath injection|xss|cross[- ]?site scripting|"
    r"parameter whitelist(?:ing)?|parameter blacklist(?:ing)?|input encoding|html encoding|"
    r"url encoding|escape|escaping|length validation|length constraint(?:s)?|type validation|"
    r"parameter type(?:s)?|types?|joi|pydantic|validator)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:query|param(?:eter)?s?|sanitiz(?:ation|e)|validat(?:ion|e)|injection|"
    r"security|requirements?|source[_ -]?payload|constraints?|acceptance|api)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirements?|needs?|need to|should|ensure|"
    r"allow|support|provide|enable|validate|sanitize|escape|encode|prevent|"
    r"protect|check|verify|filter|whitelist|blacklist|implement|apply|add|use|"
    r"acceptance|done when|before launch)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|non[- ]?goal)\b.{0,140}"
    r"\b(?:query parameter(?:s)?|sanitization|validation|injection prevention|"
    r"input encoding|escaping|parameter whitelist)\b"
    r".{0,140}\b(?:required|needed|in scope|support|supported|work|planned|changes?|for this release)\b|"
    r"\b(?:query parameter(?:s)?|sanitization|validation|injection prevention|"
    r"input encoding|escaping|parameter whitelist)\b"
    r".{0,140}\b(?:out of scope|not required|not needed|no support|unsupported|no work|"
    r"non[- ]?goal|no changes?|excluded)\b",
    re.I,
)
_NO_SANITIZATION_RE = re.compile(
    r"\b(?:no query sanitization|sanitization is out of scope|"
    r"no sanitization work|no input validation|trust(?:ed)? input)\b",
    re.I,
)
_UNRELATED_RE = re.compile(
    r"\b(?:ui validation|client[- ]?side validation|form validation(?! middleware)|"
    r"browser validation|frontend validation)\b",
    re.I,
)
_VALUE_RE = re.compile(
    r"\b(?:parameterized queries?|prepared statements?|orm|sqlalchemy|mongoose|"
    r"joi|yup|zod|validator|express[- ]?validator|"
    r"whitelist|blacklist|allowlist|denylist|"
    r"html[- ]?encode|url[- ]?encode|base64|utf[- ]?8|"
    r"min[- ]?length|max[- ]?length|regexp?|regex|pattern|"
    r"string|number|integer|boolean|uuid|email|url)\b",
    re.I,
)
_VALIDATION_LIBRARY_RE = re.compile(
    r"\b(?:joi|yup|zod|validator|express[- ]?validator|pydantic|marshmallow|"
    r"cerberus|voluptuous|jsonschema|class[- ]?validator)\b",
    re.I,
)
_SANITIZATION_STRATEGY_RE = re.compile(
    r"\b(?:whitelist|blacklist|allowlist|denylist|escape|encode|sanitize|"
    r"parameterized|prepared statements?|orm|strip|filter|validate)\b",
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
    "security",
    "api",
    "query_params",
    "parameters",
    "validation",
    "sanitization",
    "input_validation",
    "metadata",
    "brief_metadata",
    "source_payload",
)
_CATEGORY_PATTERNS: dict[ApiQuerySanitizationCategory, re.Pattern[str]] = {
    "sql_injection_prevention": re.compile(
        r"\b(?:sql injection|sqli|sql[- ]?i|parameterized queries?|prepared statements?|"
        r"orm(?:\s|$)|sqlalchemy|sequelize|typeorm|query escaping|sql escaping)\b",
        re.I,
    ),
    "nosql_injection_prevention": re.compile(
        r"\b(?:nosql injection|mongodb injection|mongo injection|json injection|"
        r"mongoose|nosql[- ]?i|operator injection|\$where injection)\b",
        re.I,
    ),
    "command_injection_prevention": re.compile(
        r"\b(?:command injection|shell injection|os command injection|exec injection|"
        r"cmd[- ]?i|shell escaping|command escaping|subprocess injection)\b",
        re.I,
    ),
    "ldap_injection_prevention": re.compile(
        r"\b(?:ldap injection|ldap[- ]?i|directory injection|ldap escaping|"
        r"ldap filter injection|ldap query injection)\b",
        re.I,
    ),
    "xpath_injection_prevention": re.compile(
        r"\b(?:xpath injection|xpath[- ]?i|xml injection|xpath escaping|"
        r"xpath query injection)\b",
        re.I,
    ),
    "parameter_whitelisting": re.compile(
        r"\b(?:whitelist(?:ing)?|allowlist(?:ing)?|parameter whitelist(?:ing)?|allowed parameters?|"
        r"permit(?:ted)? parameters?|blacklist(?:ing)?|denylist(?:ing)?|blocked parameters?|"
        r"parameter filtering|filter parameters?)\b",
        re.I,
    ),
    "input_encoding": re.compile(
        r"\b(?:input encoding|html encoding|url encoding|percent encoding|"
        r"uri encoding|base64 encoding|utf[- ]?8 encoding|character encoding|"
        r"encode input|encode parameters?|html encode|url encode|apply.*encoding)\b",
        re.I,
    ),
    "length_type_validation": re.compile(
        r"\b(?:length validation|max[- ]?length|min[- ]?length|string length|length constraint(?:s)?|"
        r"type validation|data type|parameter type(?:s)?|type checking|type enforcement|"
        r"integer validation|string validation|uuid validation|email validation|"
        r"url validation|pattern validation|regexp? validation|regex validation|validate.*types?)\b",
        re.I,
    ),
    "escaping_strategies": re.compile(
        r"\b(?:escape|escaping|escape strategy|escaping strategy|context[- ]?specific escaping|"
        r"html escape|sql escape|shell escape|regex escape|special character(?:s)? escape|"
        r"quote escaping|delimiter escaping)\b",
        re.I,
    ),
    "test_coverage": re.compile(
        r"\b(?:tests?|test coverage|unit tests?|integration tests?|sanitization tests?|"
        r"validation tests?|injection tests?|security tests?|attack tests?|"
        r"xss tests?|sql injection tests?|fuzzing|fuzz tests?|add.*tests?|comprehensive tests?)\b",
        re.I,
    ),
}
_OWNER_SUGGESTIONS: dict[ApiQuerySanitizationCategory, tuple[str, ...]] = {
    "sql_injection_prevention": ("security", "backend"),
    "nosql_injection_prevention": ("security", "backend"),
    "command_injection_prevention": ("security", "infrastructure"),
    "ldap_injection_prevention": ("security", "backend"),
    "xpath_injection_prevention": ("security", "backend"),
    "parameter_whitelisting": ("security", "api_platform"),
    "input_encoding": ("security", "backend"),
    "length_type_validation": ("security", "api_platform"),
    "escaping_strategies": ("security", "backend"),
    "test_coverage": ("qa", "security"),
}
_PLANNING_NOTES: dict[ApiQuerySanitizationCategory, tuple[str, ...]] = {
    "sql_injection_prevention": (
        "Use parameterized queries or ORM to prevent SQL injection attacks on query parameters.",
    ),
    "nosql_injection_prevention": (
        "Sanitize query parameters to prevent NoSQL injection via operator or JSON manipulation.",
    ),
    "command_injection_prevention": (
        "Escape or validate query parameters before using in shell commands or subprocess calls.",
    ),
    "ldap_injection_prevention": (
        "Escape special LDAP characters in query parameters used in directory queries.",
    ),
    "xpath_injection_prevention": (
        "Escape or validate query parameters before constructing XPath queries.",
    ),
    "parameter_whitelisting": (
        "Define explicit allowlists for accepted query parameters and reject unknown parameters.",
    ),
    "input_encoding": (
        "Encode query parameter values appropriately for context (HTML, URL, JSON) before use.",
    ),
    "length_type_validation": (
        "Validate query parameter types, lengths, and formats before processing requests.",
    ),
    "escaping_strategies": (
        "Apply context-specific escaping for special characters in query parameters.",
    ),
    "test_coverage": (
        "Add tests for injection attacks, invalid parameters, encoding edge cases, and sanitization bypasses.",
    ),
}
_GAP_MESSAGES: dict[ApiQuerySanitizationMissingDetail, str] = {
    "missing_validation_library": "Specify validation library or framework for query parameter sanitization.",
    "missing_sanitization_strategy": "Specify sanitization strategy (whitelist, escape, encode, parameterize).",
}


@dataclass(frozen=True, slots=True)
class SourceApiQuerySanitizationRequirement:
    """One source-backed API query parameter sanitization requirement."""

    category: ApiQuerySanitizationCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: ApiQuerySanitizationConfidence = "medium"
    value: str | None = None
    suggested_owners: tuple[str, ...] = field(default_factory=tuple)
    planning_notes: tuple[str, ...] = field(default_factory=tuple)
    gap_messages: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> ApiQuerySanitizationCategory:
        """Compatibility view for extractors that expose requirement_category."""
        return self.category

    @property
    def concern(self) -> ApiQuerySanitizationCategory:
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
class SourceApiQuerySanitizationRequirementsReport:
    """Source-level API query parameter sanitization requirements report."""

    brief_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceApiQuerySanitizationRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceApiQuerySanitizationRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceApiQuerySanitizationRequirement, ...]:
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
        """Return API query sanitization requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Query Sanitization Requirements Report"
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
            lines.extend(["", "No source API query sanitization requirements were inferred."])
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


def build_source_api_query_sanitization_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceApiQuerySanitizationRequirementsReport:
    """Build an API query sanitization requirements report from a brief-shaped payload."""
    brief_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    gap_flags = tuple(_missing_detail_flags(candidate.evidence for candidate in candidates))
    requirements = tuple(_merge_candidates(candidates, gap_flags))
    return SourceApiQuerySanitizationRequirementsReport(
        brief_id=brief_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements, gap_flags if requirements else ()),
    )


def summarize_source_api_query_sanitization_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceApiQuerySanitizationRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted API query sanitization requirements."""
    if isinstance(source, SourceApiQuerySanitizationRequirementsReport):
        return dict(source.summary)
    return build_source_api_query_sanitization_requirements(source).summary


def derive_source_api_query_sanitization_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceApiQuerySanitizationRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_api_query_sanitization_requirements(source)


def generate_source_api_query_sanitization_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceApiQuerySanitizationRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_api_query_sanitization_requirements(source)


def extract_source_api_query_sanitization_requirements(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceApiQuerySanitizationRequirement, ...]:
    """Return API query sanitization requirement records from brief-shaped input."""
    return build_source_api_query_sanitization_requirements(source).requirements


def source_api_query_sanitization_requirements_to_dict(
    report: SourceApiQuerySanitizationRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API query sanitization requirements report to a plain dictionary."""
    return report.to_dict()


source_api_query_sanitization_requirements_to_dict.__test__ = False


def source_api_query_sanitization_requirements_to_dicts(
    requirements: (
        tuple[SourceApiQuerySanitizationRequirement, ...]
        | list[SourceApiQuerySanitizationRequirement]
        | SourceApiQuerySanitizationRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize API query sanitization requirement records to dictionaries."""
    if isinstance(requirements, SourceApiQuerySanitizationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_query_sanitization_requirements_to_dicts.__test__ = False


def source_api_query_sanitization_requirements_to_markdown(
    report: SourceApiQuerySanitizationRequirementsReport,
) -> str:
    """Render an API query sanitization requirements report as Markdown."""
    return report.to_markdown()


source_api_query_sanitization_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: ApiQuerySanitizationCategory
    value: str | None
    source_field: str
    evidence: str
    confidence: ApiQuerySanitizationConfidence


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
        if _NO_SANITIZATION_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
            return True
    return False


def _merge_candidates(
    candidates: Iterable[_Candidate],
    gap_flags: tuple[ApiQuerySanitizationMissingDetail, ...],
) -> list[SourceApiQuerySanitizationRequirement]:
    grouped: dict[ApiQuerySanitizationCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceApiQuerySanitizationRequirement] = []
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
            SourceApiQuerySanitizationRequirement(
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
                _STRUCTURED_FIELD_RE.search(key_text) or _SANITIZATION_CONTEXT_RE.search(key_text)
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
                _SANITIZATION_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title)
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
                if _NEGATED_SCOPE_RE.search(part) and _SANITIZATION_CONTEXT_RE.search(part)
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
    if _NO_SANITIZATION_RE.search(searchable) or _NEGATED_SCOPE_RE.search(searchable):
        return False
    if _UNRELATED_RE.search(searchable) and not _SANITIZATION_CONTEXT_RE.search(searchable):
        return False
    if not (_SANITIZATION_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
        return False
    has_category = any(pattern.search(searchable) for pattern in _CATEGORY_PATTERNS.values())
    if not has_category:
        return False
    if _REQUIREMENT_RE.search(segment.text):
        return True
    if segment.section_context or _STRUCTURED_FIELD_RE.search(field_words):
        return True
    return bool(
        _SANITIZATION_CONTEXT_RE.search(segment.text)
        and re.search(
            r"\b(?:prevented|validated|sanitized|escaped|encoded|filtered|whitelisted|checked)\b",
            segment.text,
            re.I,
        )
    )


def _categories(searchable: str) -> list[ApiQuerySanitizationCategory]:
    return [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]


def _missing_detail_flags(evidence_values: Iterable[str]) -> list[ApiQuerySanitizationMissingDetail]:
    text = " ".join(evidence_values)
    if not text:
        return []
    flags: list[ApiQuerySanitizationMissingDetail] = []
    if not _VALIDATION_LIBRARY_RE.search(text):
        flags.append("missing_validation_library")
    if not _SANITIZATION_STRATEGY_RE.search(text):
        flags.append("missing_sanitization_strategy")
    return [flag for flag in _MISSING_DETAIL_ORDER if flag in flags]


def _value(category: ApiQuerySanitizationCategory, text: str) -> str | None:
    if category == "sql_injection_prevention":
        if match := re.search(r"\b(?P<value>parameterized queries?|prepared statements?|orm|sqlalchemy)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "nosql_injection_prevention":
        if match := re.search(r"\b(?P<value>mongoose|nosql injection)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "parameter_whitelisting":
        if match := re.search(r"\b(?P<value>whitelist|allowlist|blacklist|denylist)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "input_encoding":
        if match := re.search(r"\b(?P<value>html encoding|url encoding|base64|utf[- ]?8)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if category == "length_type_validation":
        if match := re.search(r"\b(?P<value>joi|yup|zod|validator|pydantic|marshmallow)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
        if match := re.search(r"\b(?P<value>string|number|integer|boolean|uuid|email|url)\b", text, re.I):
            return _clean_text(match.group("value")).casefold()
    if match := _VALUE_RE.search(text):
        return _clean_text(match.group(0)).casefold()
    return None


def _best_value(items: Iterable[_Candidate]) -> str | None:
    ranked_values = sorted(
        ((index, item.value) for index, item in enumerate(items) if item.value),
        key=lambda indexed_value: (
            0 if _VALUE_RE.search(indexed_value[1]) else 1,
            indexed_value[0],
            len(indexed_value[1]),
            indexed_value[1].casefold(),
        ),
    )
    values = _dedupe(value for _, value in ranked_values)
    return values[0] if values else None


def _confidence(segment: _Segment) -> ApiQuerySanitizationConfidence:
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
                "security",
                "validation",
                "sanitization",
                "requirements",
                "api",
                "source_payload",
            )
        )
    ):
        return "high"
    if _REQUIREMENT_RE.search(segment.text) and _SANITIZATION_CONTEXT_RE.search(searchable):
        return "medium"
    if _SANITIZATION_CONTEXT_RE.search(searchable):
        return "medium" if segment.section_context else "low"
    return "low"


def _summary(
    requirements: tuple[SourceApiQuerySanitizationRequirement, ...],
    gap_flags: tuple[ApiQuerySanitizationMissingDetail, ...],
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
        "status": (
            "ready_for_planning"
            if requirements and not gap_flags
            else "needs_sanitization_details"
            if requirements
            else "no_sanitization_language"
        ),
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
        "security",
        "api",
        "query_params",
        "parameters",
        "validation",
        "sanitization",
        "input_validation",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _field_category_rank(category: ApiQuerySanitizationCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[ApiQuerySanitizationCategory, tuple[str, ...]] = {
        "sql_injection_prevention": ("sql", "injection", "parameterized", "prepared"),
        "nosql_injection_prevention": ("nosql", "mongodb", "mongo", "mongoose"),
        "command_injection_prevention": ("command", "shell", "exec", "subprocess"),
        "ldap_injection_prevention": ("ldap", "directory"),
        "xpath_injection_prevention": ("xpath", "xml"),
        "parameter_whitelisting": ("whitelist", "allowlist", "blacklist", "denylist"),
        "input_encoding": ("encoding", "encode", "html", "url"),
        "length_type_validation": ("length", "type", "validation", "validate"),
        "escaping_strategies": ("escape", "escaping"),
        "test_coverage": ("test", "tests", "coverage"),
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
    "ApiQuerySanitizationCategory",
    "ApiQuerySanitizationConfidence",
    "ApiQuerySanitizationMissingDetail",
    "SourceApiQuerySanitizationRequirement",
    "SourceApiQuerySanitizationRequirementsReport",
    "build_source_api_query_sanitization_requirements",
    "derive_source_api_query_sanitization_requirements",
    "extract_source_api_query_sanitization_requirements",
    "generate_source_api_query_sanitization_requirements",
    "summarize_source_api_query_sanitization_requirements",
    "source_api_query_sanitization_requirements_to_dict",
    "source_api_query_sanitization_requirements_to_dicts",
    "source_api_query_sanitization_requirements_to_markdown",
]
