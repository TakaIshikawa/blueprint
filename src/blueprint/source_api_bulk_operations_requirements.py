"""Extract API bulk operations requirements from SourceBrief records."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


BulkOperationRequirementType = Literal[
    "batch_size_limits",
    "partial_success_handling",
    "transaction_semantics",
    "progress_tracking",
    "validation_checks",
    "performance_requirements",
    "batch_pagination",
    "error_reporting",
]
_T = TypeVar("_T")

_TYPE_ORDER: tuple[BulkOperationRequirementType, ...] = (
    "batch_size_limits",
    "partial_success_handling",
    "transaction_semantics",
    "progress_tracking",
    "validation_checks",
    "performance_requirements",
    "batch_pagination",
    "error_reporting",
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

_TYPE_PATTERNS: dict[BulkOperationRequirementType, re.Pattern[str]] = {
    "batch_size_limits": re.compile(
        r"\b(?:batch size|batch limit|max(?:imum)? batch|max(?:imum)? \d+\s*items?|"
        r"\d+\s*items? per (?:batch|bulk|mutation)|records? per batch|bulk size|max(?:imum)? \d+\s*records?|"
        r"chunk size|batch capacity)\b",
        re.I,
    ),
    "partial_success_handling": re.compile(
        r"\b(?:partial (?:success|failure)|partial commit|partial result|"
        r"some (?:succeed|fail)|mixed result|continue on error|"
        r"fail(?:ed)? item|skip(?:ped)? item|individual (?:error|failure)|"
        r"handle partial)\b",
        re.I,
    ),
    "transaction_semantics": re.compile(
        r"\b(?:transaction|transactional|atomic|atomicity|all[- ]or[- ]nothing|"
        r"rollback|commit|two[- ]phase commit|saga|eventual consistency|"
        r"consistency guarantee|ACID)\b",
        re.I,
    ),
    "progress_tracking": re.compile(
        r"\b(?:progress (?:track(?:ing)?|report(?:ing)?|status)|track progress|"
        r"status (?:update|report)|completion (?:status|percent(?:age)?)|"
        r"processed count|remaining item|job (?:status|progress)|"
        r"monitor(?:ing)? progress|progress indicator)\b",
        re.I,
    ),
    "validation_checks": re.compile(
        r"\b(?:pre[- ]?flight|dry[- ]?run|test[- ]?mode|"
        r"validate before|pre[- ]?validation|"
        r"schema validation|input validation)\b.{0,50}\b(?:batch|bulk|processing|import|operation)\b|"
        r"\b(?:batch|bulk|processing|import|operation)\b.{0,50}\b(?:pre[- ]?flight|dry[- ]?run|validation|validate before)\b",
        re.I,
    ),
    "performance_requirements": re.compile(
        r"\b(?:performance|throughput|latency|speed|processing time|"
        r"bulk performance|batch performance|concurrent processing|"
        r"parallel processing|optimization|efficient|fast|quick)\b",
        re.I,
    ),
    "batch_pagination": re.compile(
        r"\b(?:batch pagina(?:tion|te)|paginate batch|large batch|"
        r"split batch|chunks? of \d+|divide batch|batch iteration|"
        r"process in (?:batch(?:es)?|chunk(?:s)?)|iterate (?:batch(?:es)?|over)|"
        r"chunk size|chunking)\b",
        re.I,
    ),
    "error_reporting": re.compile(
        r"\b(?:error (?:report(?:ing)?|detail|message|response)|"
        r"fail(?:ure)? (?:report(?:ing)?|detail|message|response)|"
        r"error handling|error format|detailed error|"
        r"which (?:item|record) fail(?:ed)?|error per (?:item|record)|"
        r"individual error|error (?:for )?each (?:item|record|failed))\b",
        re.I,
    ),
}

_BASE_QUESTIONS: dict[BulkOperationRequirementType, tuple[str, ...]] = {
    "batch_size_limits": (
        "What are the minimum and maximum batch size limits?",
        "How should requests exceeding the maximum batch size be handled?",
    ),
    "partial_success_handling": (
        "Should the operation continue if some items fail?",
        "How are partial successes communicated to the client?",
    ),
    "transaction_semantics": (
        "Should bulk operations be atomic (all-or-nothing)?",
        "What rollback or compensation strategy should be used on failure?",
    ),
    "progress_tracking": (
        "How should progress be tracked and reported to clients?",
        "Should progress updates be real-time or polling-based?",
    ),
    "validation_checks": (
        "Should validation occur before processing (pre-flight)?",
        "What validation failures should block the entire batch?",
    ),
    "performance_requirements": (
        "What are the performance and throughput requirements?",
        "Should processing be parallel or sequential?",
    ),
    "batch_pagination": (
        "How should large batches be split and paginated?",
        "What is the iteration strategy for processing chunks?",
    ),
    "error_reporting": (
        "What error detail level should be provided per item?",
        "How should error responses be structured and formatted?",
    ),
}


@dataclass(frozen=True, slots=True)
class SourceApiBulkOperationsRequirement:
    """One source-backed API bulk operations requirement."""

    requirement_type: BulkOperationRequirementType
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
class SourceApiBulkOperationsRequirementsReport:
    """Source-level API bulk operations requirements report."""

    source_brief_id: str | None = None
    requirements: tuple[SourceApiBulkOperationsRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceApiBulkOperationsRequirement, ...]:
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
        """Return API bulk operations requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source API Bulk Operations Requirements Report"
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
            f"- Reliability coverage: {self.summary.get('reliability_coverage', 0)}%",
            f"- Observability coverage: {self.summary.get('observability_coverage', 0)}%",
            f"- Data integrity coverage: {self.summary.get('data_integrity_coverage', 0)}%",
            "- Requirement type counts: "
            + ", ".join(
                f"{req_type} {type_counts.get(req_type, 0)}" for req_type in _TYPE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source API bulk operations requirements were inferred."])
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


def build_source_api_bulk_operations_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceApiBulkOperationsRequirementsReport:
    """Extract API bulk operations requirement signals from a source brief."""
    source_brief_id, payload = _source_payload(source)
    grouped = _group_requirements(payload)
    requirements = _merge_requirements(grouped, source_brief_id)
    return SourceApiBulkOperationsRequirementsReport(
        source_brief_id=source_brief_id,
        requirements=requirements,
        summary=_summary(requirements),
    )


def extract_source_api_bulk_operations_requirements(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SourceApiBulkOperationsRequirement, ...]:
    """Return API bulk operations requirement records extracted from brief-shaped input."""
    return build_source_api_bulk_operations_requirements(source).requirements


def summarize_source_api_bulk_operations_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | SourceApiBulkOperationsRequirementsReport
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic API bulk operations requirements summary."""
    if isinstance(source_or_result, SourceApiBulkOperationsRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_api_bulk_operations_requirements(source_or_result).summary


def source_api_bulk_operations_requirements_to_dict(
    report: SourceApiBulkOperationsRequirementsReport,
) -> dict[str, Any]:
    """Serialize an API bulk operations requirements report to a plain dictionary."""
    return report.to_dict()


source_api_bulk_operations_requirements_to_dict.__test__ = False


def source_api_bulk_operations_requirements_to_dicts(
    requirements: (
        tuple[SourceApiBulkOperationsRequirement, ...]
        | list[SourceApiBulkOperationsRequirement]
        | SourceApiBulkOperationsRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize source API bulk operations requirement records to dictionaries."""
    if isinstance(requirements, SourceApiBulkOperationsRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_api_bulk_operations_requirements_to_dicts.__test__ = False


def source_api_bulk_operations_requirements_to_markdown(
    report: SourceApiBulkOperationsRequirementsReport,
) -> str:
    """Render an API bulk operations requirements report as Markdown."""
    return report.to_markdown()


source_api_bulk_operations_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    requirement_type: BulkOperationRequirementType
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


def _group_requirements(payload: Mapping[str, Any]) -> dict[BulkOperationRequirementType, list[_Candidate]]:
    grouped: dict[BulkOperationRequirementType, list[_Candidate]] = {}
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
    grouped: dict[BulkOperationRequirementType, list[_Candidate]],
    source_brief_id: str | None,
) -> tuple[SourceApiBulkOperationsRequirement, ...]:
    requirements: list[SourceApiBulkOperationsRequirement] = []
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
            SourceApiBulkOperationsRequirement(
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


def _matched_requirement_types(text: str) -> tuple[BulkOperationRequirementType, ...]:
    return tuple(
        requirement_type
        for requirement_type in _TYPE_ORDER
        if _TYPE_PATTERNS[requirement_type].search(text)
    )


def _matched_terms(
    requirement_type: BulkOperationRequirementType,
    text: str,
) -> tuple[str, ...]:
    return tuple(
        _dedupe(
            _clean_text(match.group(0)) for match in _TYPE_PATTERNS[requirement_type].finditer(text)
        )
    )


def _follow_up_questions(
    requirement_type: BulkOperationRequirementType,
    evidence_text: str,
) -> tuple[str, ...]:
    questions = list(_BASE_QUESTIONS[requirement_type])
    # Reduce questions if evidence already provides specific answers
    if requirement_type == "batch_size_limits" and re.search(
        r"\b(?:\d+\s*(?:items?|records?|batch)|max(?:imum)?\s+\d+)\b", evidence_text, re.I
    ):
        questions = []  # Specific limits provided
    if requirement_type == "transaction_semantics" and re.search(
        r"\b(?:atomic|all[- ]or[- ]nothing|rollback|commit)\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Semantics mentioned
    if requirement_type == "partial_success_handling" and re.search(
        r"\b(?:continue|proceed|skip|individual)\b", evidence_text, re.I
    ):
        questions = questions[1:]  # Handling strategy mentioned
    return tuple(_dedupe(questions))


def _summary(requirements: tuple[SourceApiBulkOperationsRequirement, ...]) -> dict[str, Any]:
    # Calculate coverage metrics
    reliability = {"partial_success_handling", "error_reporting"}
    observability = {"progress_tracking", "error_reporting"}
    data_integrity = {"transaction_semantics", "validation_checks"}

    req_types = {req.requirement_type for req in requirements}
    reliability_coverage = int(100 * len(req_types & reliability) / len(reliability)) if reliability else 0
    observability_coverage = int(100 * len(req_types & observability) / len(observability)) if observability else 0
    data_integrity_coverage = int(100 * len(req_types & data_integrity) / len(data_integrity)) if data_integrity else 0

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
        "reliability_coverage": reliability_coverage,
        "observability_coverage": observability_coverage,
        "data_integrity_coverage": data_integrity_coverage,
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
    "BulkOperationRequirementType",
    "SourceApiBulkOperationsRequirement",
    "SourceApiBulkOperationsRequirementsReport",
    "build_source_api_bulk_operations_requirements",
    "extract_source_api_bulk_operations_requirements",
    "source_api_bulk_operations_requirements_to_dict",
    "source_api_bulk_operations_requirements_to_dicts",
    "source_api_bulk_operations_requirements_to_markdown",
    "summarize_source_api_bulk_operations_requirements",
]
