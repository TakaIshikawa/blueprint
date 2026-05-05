"""Extract API error handling requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


ErrorHandlingRequirementType = Literal[
    "error_response_format",
    "status_code_mapping",
    "error_message_templates",
    "retry_strategies",
    "fallback_behaviors",
    "error_logging",
    "client_error_guidance",
    "validation_errors",
]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[ErrorHandlingRequirementType, ...] = (
    "error_response_format",
    "status_code_mapping",
    "error_message_templates",
    "retry_strategies",
    "fallback_behaviors",
    "error_logging",
    "client_error_guidance",
    "validation_errors",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "acceptance",
    "acceptance_criteria",
    "integration_points",
    "integrations",
    "constraints",
    "metadata",
)

_TYPE_PATTERNS: dict[ErrorHandlingRequirementType, re.Pattern[str]] = {
    "error_response_format": re.compile(
        r"\b(?:error (?:format|structure|schema|shape|body)|"
        r"error response|response format|json error|"
        r"error payload|error object|error field|"
        r"standard(?:ized)? error|error envelope|"
        r"error code field|error detail(?:s)? field|"
        r"error(?:s)? array|errors? in (?:response|extensions))\b",
        re.I,
    ),
    "status_code_mapping": re.compile(
        r"\b(?:status code|http (?:status|code)|"
        r"4\d{2}|5\d{2}|400|401|403|404|422|429|500|502|503|504|"
        r"bad request|unauthorized|forbidden|not found|"
        r"unprocessable|too many requests|internal server error|"
        r"service unavailable|gateway timeout|"
        r"status code (?:mapping|strategy|convention))\b",
        re.I,
    ),
    "error_message_templates": re.compile(
        r"\b(?:error message|error text|message template|"
        r"error wording|error copy|user[- ]facing error|"
        r"localized error|error translation|"
        r"error description|human[- ]readable error|"
        r"error string|error phrase)\b",
        re.I,
    ),
    "retry_strategies": re.compile(
        r"\b(?:retry|retries|retryable|retry(?:ing)? logic|"
        r"exponential backoff|backoff strategy|"
        r"retry[- ]after|max(?:imum)? retries?|"
        r"retry limit|retry delay|retry interval|"
        r"retry policy|automatic retry|"
        r"idempotent retry|transient (?:error|failure))\b",
        re.I,
    ),
    "fallback_behaviors": re.compile(
        r"\b(?:fallback|graceful degrad(?:ation|e)|"
        r"default (?:value|response|behavior)|"
        r"fail[- ]safe|fail[- ]open|fail[- ]closed|"
        r"circuit break(?:er)?|degraded (?:mode|service)|"
        r"partial (?:response|data)|best[- ]effort|"
        r"cache[d]? fallback|stale data)\b",
        re.I,
    ),
    "error_logging": re.compile(
        r"\b(?:log(?:ging)? (?:error|all error)|error log|"
        r"track(?:ing)? error|error track(?:ing)?|"
        r"error monitor(?:ing)?|error alert|"
        r"error metric|error telemetry|"
        r"error trace|stack trace|error context|"
        r"error aggregation|error dashboard)\b|"
        r"\blog .{0,30}error",
        re.I,
    ),
    "client_error_guidance": re.compile(
        r"\b(?:client guidance|developer guidance|"
        r"api documentation|error documentation|"
        r"troubleshoot(?:ing)?|debug(?:ging)? (?:guide|help)|"
        r"error reference|error code reference|"
        r"remediation steps?|resolution steps?|"
        r"help(?:ful)? error|actionable (?:error|guidance))\b",
        re.I,
    ),
    "validation_errors": re.compile(
        r"\b(?:validation error|input validation|"
        r"field[- ]level (?:error|validation)|parameter error|"
        r"schema validation|constraint violation|"
        r"validation message|invalid (?:input|field|parameter)|"
        r"field error|validation failure|"
        r"validation detail|per[- ]field (?:error|validation)|"
        r"field[- ]level|return .{0,30}validation error)\b",
        re.I,
    ),
}

_BASE_QUESTIONS: dict[ErrorHandlingRequirementType, tuple[str, ...]] = {
    "error_response_format": (
        "What is the standardized error response structure?",
        "What fields should be included in error responses?",
    ),
    "status_code_mapping": (
        "What HTTP status codes should be used for different error types?",
        "How should client errors (4xx) be distinguished from server errors (5xx)?",
    ),
    "error_message_templates": (
        "What format should error messages follow?",
        "Should error messages be localized for different languages?",
    ),
    "retry_strategies": (
        "Which errors are retryable vs non-retryable?",
        "What retry backoff strategy should clients use?",
    ),
    "fallback_behaviors": (
        "What fallback behavior should be used when the API fails?",
        "Should degraded mode return partial data or fail completely?",
    ),
    "error_logging": (
        "What error details should be logged for debugging?",
        "How should errors be tracked and monitored?",
    ),
    "client_error_guidance": (
        "What documentation should be provided to help clients handle errors?",
        "Should error responses include remediation steps?",
    ),
    "validation_errors": (
        "How should field-level validation errors be structured?",
        "Should all validation errors be returned together or fail-fast?",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceApiErrorHandlingRequirement:
    """One source-backed API error handling requirement."""

    requirement_type: ErrorHandlingRequirementType
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    follow_up_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "requirement_type": self.requirement_type,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "follow_up_questions": list(self.follow_up_questions),
        }


@dataclass(frozen=True, slots=True)
class SourceApiErrorHandlingRequirementsReport:
    """Source-level API error handling requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceApiErrorHandlingRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceApiErrorHandlingRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return API error handling requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Error Handling Requirements Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        type_counts = self.summary.get("type_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Source count: {self.summary.get('source_count', 1)}",
            f"- Client experience coverage: {self.summary.get('client_experience_coverage', 0)}%",
            f"- Reliability coverage: {self.summary.get('reliability_coverage', 0)}%",
            f"- Observability coverage: {self.summary.get('observability_coverage', 0)}%",
            "- Requirement type counts: "
            + ", ".join(
                f"{req_type} {type_counts.get(req_type, 0)}" for req_type in _TYPE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source API error handling requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Type | Source Field Paths | Evidence | Follow-up Questions |",
                "| --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{requirement.requirement_type} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell('; '.join(requirement.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(requirement.follow_up_questions) or 'none')} |"
            )
        return "\n".join(lines)


def build_source_api_error_handling_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceApiErrorHandlingRequirementsReport:
    """Extract API error handling requirement signals from a source brief."""
    source_brief_id, payload = _source_payload(source)
    grouped = _group_requirements(payload)
    requirements = _merge_requirements(grouped, source_brief_id)
    return SourceApiErrorHandlingRequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_api_error_handling_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SourceApiErrorHandlingRequirement, ...]:
    """Return API error handling requirement records extracted from brief-shaped input."""
    return build_source_api_error_handling_requirements(source).requirements


def summarize_source_api_error_handling_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | SourceApiErrorHandlingRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic API error handling requirements summary."""
    if isinstance(source_or_result, SourceApiErrorHandlingRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_api_error_handling_requirements(source_or_result).summary


def source_api_error_handling_requirements_to_dict(
    report: SourceApiErrorHandlingRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API error handling requirements report to a plain dictionary."""
    return report.to_dict()


source_api_error_handling_requirements_to_dict.__test__ = False


def source_api_error_handling_requirements_to_dicts(
    requirements: (
        tuple[SourceApiErrorHandlingRequirement, ...]
        | list[SourceApiErrorHandlingRequirement]
        | SourceApiErrorHandlingRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source API error handling requirement records to dictionaries."""
    if isinstance(requirements, SourceApiErrorHandlingRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_error_handling_requirements_to_dicts.__test__ = False


def source_api_error_handling_requirements_to_markdown(
    report: SourceApiErrorHandlingRequirementsReport,
) -> str:
    """Render an API error handling requirements report as Markdown."""
    return report.to_markdown()


source_api_error_handling_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: ErrorHandlingRequirementType
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        payload = source.model_dump(mode="python")
        return _source_brief_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            payload = dict(value)
            return _source_brief_id(payload), payload
    if isinstance(source, Mapping):
        try:
            payload = dict(SourceBrief.model_validate(source).model_dump(mode="python"))
            return _source_brief_id(payload), payload
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
            return _source_brief_id(payload), payload
    if not isinstance(source, (bytes, bytearray)):
        payload = _object_payload(source)
        return _source_brief_id(payload), payload
    return None, {}


def _source_brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _group_requirements(payload: Mapping[str, Any]) -> dict[ErrorHandlingRequirementType, list[_Candidate]]:
    grouped: dict[ErrorHandlingRequirementType, list[_Candidate]] = {}
    for source_field, text in _candidate_texts(payload):
        for segment in _segments(text):
            for requirement_type in _matched_requirement_types(segment):
                candidate = _Candidate(
                    requirement_type=requirement_type,
                    evidence=_evidence_snippet(source_field, segment),
                    source_field_path=source_field,
                    matched_terms=_matched_terms(requirement_type, segment),
                )
                grouped.setdefault(requirement_type, []).append(candidate)
    return grouped


def _merge_requirements(
    grouped: dict[ErrorHandlingRequirementType, list[_Candidate]],
    source_brief_id: str | None,
) -> tuple[SourceApiErrorHandlingRequirement, ...]:
    requirements: list[SourceApiErrorHandlingRequirement] = []
    for requirement_type in _TYPE_ORDER:
        candidates = grouped.get(requirement_type, [])
        if not candidates:
            continue
        evidence = tuple(_dedupe_evidence(item.evidence for item in candidates))[:5]
        source_field_paths = tuple(
            sorted(_dedupe(item.source_field_path for item in candidates), key=str.casefold)
        )
        matched_terms = tuple(
            sorted(
                _dedupe(term for item in candidates for term in item.matched_terms),
                key=str.casefold,
            )
        )
        questions = _follow_up_questions(requirement_type, " ".join(evidence))
        requirements.append(
            SourceApiErrorHandlingRequirement(
                requirement_type=requirement_type,
                evidence=evidence,
                source_field_paths=source_field_paths,
                matched_terms=matched_terms,
                follow_up_questions=questions,
            )
        )
    return tuple(requirements)


def _candidate_texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    for field_name in _SCANNED_FIELDS:
        value = payload.get(field_name)
        if field_name == "metadata":
            texts.extend(_nested_texts(value, field_name))
            continue
        for index, text in enumerate(_strings(value)):
            texts.append((field_name if index == 0 else f"{field_name}[{index}]", text))

    if isinstance(payload.get("source_payload"), Mapping):
        for field_name in _SCANNED_FIELDS:
            if field_name in payload["source_payload"]:
                texts.extend(_nested_texts(payload["source_payload"][field_name], f"source_payload.{field_name}"))
    return texts


def _nested_texts(value: Any, prefix: str) -> list[tuple[str, str]]:
    if isinstance(value, Mapping):
        texts: list[tuple[str, str]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            field = f"{prefix}.{key}"
            if isinstance(child, (Mapping, list, tuple, set)):
                texts.extend(_nested_texts(child, field))
            elif text := _optional_text(child):
                texts.append((field, text))
        return texts
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        texts = []
        for index, item in enumerate(items):
            field = f"{prefix}[{index}]"
            if isinstance(item, (Mapping, list, tuple, set)):
                texts.extend(_nested_texts(item, field))
            elif text := _optional_text(item):
                texts.append((field, text))
        return texts
    text = _optional_text(value)
    return [(prefix, text)] if text else []


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
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
    text = _optional_text(value)
    return [text] if text else []


def _segments(text: str) -> list[str]:
    segments: list[str] = []
    for raw_segment in _SENTENCE_SPLIT_RE.split(text):
        segment = _clean_text(raw_segment)
        if segment:
            segments.append(segment)
    return segments


def _matched_requirement_types(text: str) -> tuple[ErrorHandlingRequirementType, ...]:
    return tuple(
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(text)
    )


def _matched_terms(
    requirement_type: ErrorHandlingRequirementType,
    text: str,
) -> tuple[str, ...]:
    return tuple(
        _dedupe(
            _clean_text(match.group(0)) for match in _TYPE_PATTERNS[requirement_type].finditer(text)
        )
    )


def _follow_up_questions(
    requirement_type: ErrorHandlingRequirementType,
    evidence_text: str,
) -> tuple[str, ...]:
    questions = list(_BASE_QUESTIONS[requirement_type])
    # Reduce questions if evidence already provides specific answers
    if requirement_type == "error_response_format" and re.search(
        r"\b(?:include|contain|field|code|message|detail)\b", evidence_text, re.I
    ):
        questions = []  # Fields/structure specified
    if requirement_type == "status_code_mapping" and re.search(
        r"\b(?:4\d{2}|5\d{2}|400|404|500|status code)\b", evidence_text, re.I
    ):
        questions = []  # Specific codes mentioned
    if requirement_type == "retry_strategies" and re.search(
        r"\b(?:retryable|exponential|backoff|retry limit|max.*retries)\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Strategy mentioned
    return tuple(_dedupe(questions))


def _summary(requirements: tuple[SourceApiErrorHandlingRequirement, ...]) -> dict[str, Any]:
    # Calculate coverage metrics
    client_experience = {"error_message_templates", "client_error_guidance", "validation_errors"}
    reliability = {"retry_strategies", "fallback_behaviors"}
    observability = {"error_logging", "error_response_format"}

    req_types = {req.requirement_type for req in requirements}
    client_experience_coverage = int(100 * len(req_types & client_experience) / len(client_experience)) if client_experience else 0
    reliability_coverage = int(100 * len(req_types & reliability) / len(reliability)) if reliability else 0
    observability_coverage = int(100 * len(req_types & observability) / len(observability)) if observability else 0

    return {
        "requirement_count": len(requirements),
        "source_count": 1,
        "type_counts": {
            req_type: sum(1 for req in requirements if req.requirement_type == req_type)
            for req_type in _TYPE_ORDER
        },
        "requirement_types": [req.requirement_type for req in requirements],
        "follow_up_question_count": sum(
            len(req.follow_up_questions) for req in requirements
        ),
        "client_experience_coverage": client_experience_coverage,
        "reliability_coverage": reliability_coverage,
        "observability_coverage": observability_coverage,
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
        "requirements",
        "acceptance_criteria",
        "acceptance",
        "constraints",
        "integration_points",
        "integrations",
        "metadata",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _BULLET_RE.sub("", text.strip())
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
    "ErrorHandlingRequirementType",
    "SourceApiErrorHandlingRequirement",
    "SourceApiErrorHandlingRequirementsReport",
    "build_source_api_error_handling_requirements",
    "extract_source_api_error_handling_requirements",
    "source_api_error_handling_requirements_to_dict",
    "source_api_error_handling_requirements_to_dicts",
    "source_api_error_handling_requirements_to_markdown",
    "summarize_source_api_error_handling_requirements",
]
