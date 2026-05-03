"""Extract source-level gRPC service and protobuf requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceGRPCServiceRequirementCategory = Literal[
    "protobuf_schema",
    "service_methods",
    "error_handling",
    "metadata_headers",
    "interceptors",
    "deadline_timeout",
    "load_balancing",
    "service_reflection",
]
SourceGRPCServiceConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[SourceGRPCServiceRequirementCategory, ...] = (
    "protobuf_schema",
    "service_methods",
    "error_handling",
    "metadata_headers",
    "interceptors",
    "deadline_timeout",
    "load_balancing",
    "service_reflection",
)
_CONFIDENCE_ORDER: dict[SourceGRPCServiceConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_PLANNING_NOTES: dict[SourceGRPCServiceRequirementCategory, str] = {
    "protobuf_schema": "Carry protobuf schema structure (.proto files, messages, services) into API implementation tasks.",
    "service_methods": "Plan service method types (unary, server streaming, client streaming, bidirectional) and request/response patterns.",
    "error_handling": "Define gRPC status codes, error handling patterns, and error propagation strategies.",
    "metadata_headers": "Plan metadata/headers for context propagation, authentication, tracing, and request tracking.",
    "interceptors": "Implement interceptors for authentication, logging, metrics, and cross-cutting concerns.",
    "deadline_timeout": "Configure deadline/timeout policies, propagation, and timeout handling strategies.",
    "load_balancing": "Plan load balancing strategies (round-robin, least-request, ring-hash) and service mesh integration.",
    "service_reflection": "Implement service reflection and discovery for debugging and dynamic client generation.",
}

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_GRPC_CONTEXT_RE = re.compile(
    r"\b(?:grpc|g[- ]?rpc|protobuf|proto[- ]?buf|protocol buffer|\.proto|"
    r"service method|unary|streaming|bidirectional|server[- ]?stream|client[- ]?stream|"
    r"status code|metadata|header|interceptor|deadline|timeout|"
    r"load[- ]?balanc|reflection|service discovery)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:grpc|protobuf|proto|service|rpc|api|requirements?|constraints?|acceptance|"
    r"metadata|source[_ -]?payload|implementation[_ -]?notes)",
    re.I,
)
_DIRECTIVE_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"support|enable|configure|provide|document|define|implement|enforce|validate|"
    r"cannot ship|before launch|done when|acceptance)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without|out of scope|outside scope|non[- ]?goal|defer|deferred)\b"
    r".{0,160}\b(?:grpc|g[- ]?rpc|protobuf|proto[- ]?buf|protocol buffer|"
    r"service method|streaming|interceptor|metadata)\b"
    r".{0,160}\b(?:required|needed|in scope|supported|support|work|changes?|planned|"
    r"requirements?)?\b|"
    r"\b(?:grpc|g[- ]?rpc|protobuf|proto[- ]?buf|protocol buffer|"
    r"service method|streaming|interceptor|metadata)\b"
    r".{0,160}\b(?:out of scope|outside scope|not required|not needed|no support|"
    r"unsupported|no work|non[- ]?goal|deferred)\b",
    re.I,
)
_CATEGORY_PATTERNS: dict[SourceGRPCServiceRequirementCategory, re.Pattern[str]] = {
    "protobuf_schema": re.compile(
        r"\b(?:protobuf schema|proto[- ]?buf schema|protocol buffer|\.proto file|"
        r"proto definition|message definition|protobuf message|proto message|"
        r"schema definition|protoc|proto compiler|"
        r"(?:with|support(?:s)?|include(?:s)?|implement(?:s)?)\s+protobuf)\b",
        re.I,
    ),
    "service_methods": re.compile(
        r"\b(?:service method|rpc method|unary|server[- ]?stream(?:ing)?|client[- ]?stream(?:ing)?|"
        r"bidirectional stream(?:ing)?|bidi stream|duplex stream|"
        r"streaming rpc|method type|rpc call|service call)\b",
        re.I,
    ),
    "error_handling": re.compile(
        r"\b(?:grpc status|status code|error code|grpc error|error handling|"
        r"status\.(?:ok|cancelled|unknown|invalid_argument|deadline_exceeded|not_found|"
        r"already_exists|permission_denied|resource_exhausted|failed_precondition|"
        r"aborted|out_of_range|unimplemented|internal|unavailable|data_loss|unauthenticated)|"
        r"error propagation|error detail|status detail)\b",
        re.I,
    ),
    "metadata_headers": re.compile(
        r"\b(?:grpc metadata|metadata header|request metadata|response metadata|"
        r"context propagation|header propagation|tracing header|"
        r"request[- ]?id|trace[- ]?id|correlation[- ]?id|"
        r"metadata key|metadata value|custom header|metadata)\b",
        re.I,
    ),
    "interceptors": re.compile(
        r"\b(?:grpc interceptor|interceptor(?:s)?|server interceptor|client interceptor|"
        r"unary interceptor|stream interceptor|"
        r"auth(?:entication)? interceptor|logging interceptor|metrics interceptor|"
        r"middleware|interceptor chain)\b",
        re.I,
    ),
    "deadline_timeout": re.compile(
        r"\b(?:deadline|timeout|grpc deadline|request timeout|call timeout|"
        r"deadline propagation|timeout policy|timeout handling|"
        r"context deadline|deadline exceeded|timeout exceeded)\b",
        re.I,
    ),
    "load_balancing": re.compile(
        r"\b(?:load[- ]?balanc(?:ing|er|e)|round[- ]?robin|least[- ]?request|"
        r"ring[- ]?hash|consistent hash|grpc[- ]?lb|service mesh|"
        r"client[- ]?side load[- ]?balanc|load distribution)\b",
        re.I,
    ),
    "service_reflection": re.compile(
        r"\b(?:service reflection|grpc reflection|server reflection|"
        r"reflection api|reflection service|service discovery|"
        r"grpcurl|grpc_cli|dynamic client|schema discovery)\b",
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
    "security",
    "api",
    "grpc",
    "protobuf",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)


@dataclass(frozen=True, slots=True)
class SourceGRPCServiceRequirement:
    """One source-backed gRPC service requirement."""

    category: SourceGRPCServiceRequirementCategory
    source_field: str = ""
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: SourceGRPCServiceConfidence = "medium"
    planning_note: str = ""
    unresolved_questions: tuple[str, ...] = field(default_factory=tuple)

    @property
    def requirement_category(self) -> SourceGRPCServiceRequirementCategory:
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
class SourceGRPCServiceRequirementsReport:
    """Source-level gRPC service requirements report."""

    source_id: str | None = None
    title: str | None = None
    requirements: tuple[SourceGRPCServiceRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceGRPCServiceRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceGRPCServiceRequirement, ...]:
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
        """Return gRPC service requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source gRPC Service Requirements Report"
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
            lines.extend(["", "No source gRPC service requirements were inferred."])
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


def build_source_grpc_service_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceGRPCServiceRequirementsReport:
    """Build a gRPC service requirements report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    candidates = [] if _has_global_no_scope(payload) else _requirement_candidates(payload)
    requirements = tuple(_merge_candidates(candidates))
    return SourceGRPCServiceRequirementsReport(
        source_id=source_id,
        title=_optional_text(payload.get("title")),
        requirements=requirements,
        summary=_summary(requirements),
    )


def build_source_grpc_service_requirements_report(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceGRPCServiceRequirementsReport:
    """Compatibility helper for callers that use explicit report naming."""
    return build_source_grpc_service_requirements(source)


def generate_source_grpc_service_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceGRPCServiceRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_grpc_service_requirements(source)


def derive_source_grpc_service_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceGRPCServiceRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_grpc_service_requirements(source)


def extract_source_grpc_service_requirements(
    source: str | Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[SourceGRPCServiceRequirement, ...]:
    """Return gRPC service requirement records extracted from brief-shaped input."""
    return build_source_grpc_service_requirements(source).requirements


def summarize_source_grpc_service_requirements(
    source_or_result: (
        str
        | Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceGRPCServiceRequirementsReport
        | object
    ),
) -> dict[str, Any]:
    """Return the deterministic gRPC service requirements summary."""
    if isinstance(source_or_result, SourceGRPCServiceRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_grpc_service_requirements(source_or_result).summary


def source_grpc_service_requirements_to_dict(
    report: SourceGRPCServiceRequirementsReport,
) -> dict[str, Any]:
    """Serialize a gRPC service requirements report to a plain dictionary."""
    return report.to_dict()


source_grpc_service_requirements_to_dict.__test__ = False


def source_grpc_service_requirements_to_dicts(
    requirements: tuple[SourceGRPCServiceRequirement, ...]
    | list[SourceGRPCServiceRequirement]
    | SourceGRPCServiceRequirementsReport,
) -> list[dict[str, Any]]:
    """Serialize source gRPC service requirement records to dictionaries."""
    if isinstance(requirements, SourceGRPCServiceRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_grpc_service_requirements_to_dicts.__test__ = False


def source_grpc_service_requirements_to_markdown(
    report: SourceGRPCServiceRequirementsReport,
) -> str:
    """Render a gRPC service requirements report as Markdown."""
    return report.to_markdown()


source_grpc_service_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: SourceGRPCServiceRequirementCategory
    source_field: str
    evidence: str
    confidence: SourceGRPCServiceConfidence


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
        categories: list[SourceGRPCServiceRequirementCategory] = [
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


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceGRPCServiceRequirement]:
    grouped: dict[SourceGRPCServiceRequirementCategory, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.category, []).append(candidate)

    requirements: list[SourceGRPCServiceRequirement] = []
    for category in _CATEGORY_ORDER:
        items = grouped.get(category, [])
        if not items:
            continue
        best = min(items, key=_candidate_sort_key)
        evidence = tuple(_dedupe_evidence(item.evidence for item in sorted(items, key=_candidate_sort_key)))[:6]
        requirements.append(
            SourceGRPCServiceRequirement(
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
                _STRUCTURED_FIELD_RE.search(key_text) or _GRPC_CONTEXT_RE.search(key_text)
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
            section_context = inherited_context or bool(_GRPC_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
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
    categories: Iterable[SourceGRPCServiceRequirementCategory],
) -> bool:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    if not (_GRPC_CONTEXT_RE.search(searchable) or _STRUCTURED_FIELD_RE.search(field_words)):
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
    category: SourceGRPCServiceRequirementCategory,
    segment: _Segment,
) -> SourceGRPCServiceConfidence:
    searchable = _searchable_text(segment.source_field, segment.text)
    score = 0
    if _STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)):
        score += 1
    if segment.section_context or _GRPC_CONTEXT_RE.search(searchable):
        score += 1
    if _DIRECTIVE_RE.search(segment.text):
        score += 1
    if _CATEGORY_PATTERNS[category].search(searchable):
        score += 1
    return "high" if score >= 3 else "medium" if score >= 2 else "low"


def _unresolved_questions(
    category: SourceGRPCServiceRequirementCategory,
    items: Iterable[_Candidate],
) -> list[str]:
    item_list = list(items)
    questions: list[str] = []
    if category == "protobuf_schema" and not any(re.search(r"\b(?:\.proto|message|service)\b", item.evidence, re.I) for item in item_list):
        questions.append("What protobuf messages, services, and RPC methods should be defined in the .proto files?")
    if category == "service_methods" and not any(re.search(r"\b(?:unary|streaming|bidirectional)\b", item.evidence, re.I) for item in item_list):
        questions.append("What service method types (unary, server streaming, client streaming, bidirectional) are required?")
    if category == "error_handling" and not any(re.search(r"\b(?:status|code|error)\b", item.evidence, re.I) for item in item_list):
        questions.append("What gRPC status codes and error handling patterns should be implemented?")
    if category == "metadata_headers" and not any(re.search(r"\b(?:metadata|header|propagation)\b", item.evidence, re.I) for item in item_list):
        questions.append("What metadata headers are required for authentication, tracing, and context propagation?")
    if category == "interceptors" and not any(re.search(r"\b(?:auth|logging|metrics|middleware)\b", item.evidence, re.I) for item in item_list):
        questions.append("What interceptors should be implemented for authentication, logging, and metrics?")
    if category == "deadline_timeout" and not any(re.search(r"\b(?:deadline|timeout|policy)\b", item.evidence, re.I) for item in item_list):
        questions.append("What deadline and timeout policies should be configured for gRPC calls?")
    if category == "load_balancing" and not any(re.search(r"\b(?:round-robin|least-request|hash|mesh)\b", item.evidence, re.I) for item in item_list):
        questions.append("What load balancing strategy (round-robin, least-request, ring-hash) should be used?")
    return questions[:3]


def _summary(requirements: tuple[SourceGRPCServiceRequirement, ...]) -> dict[str, Any]:
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
        "status": "ready_for_planning" if requirements else "no_grpc_service_requirements_found",
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


def _field_category_rank(category: SourceGRPCServiceRequirementCategory, source_field: str) -> int:
    field_words = _field_words(source_field).casefold()
    markers: dict[SourceGRPCServiceRequirementCategory, tuple[str, ...]] = {
        "protobuf_schema": ("protobuf", "proto", "schema", "definition"),
        "service_methods": ("service", "method", "rpc", "streaming"),
        "error_handling": ("error", "status", "code", "handling"),
        "metadata_headers": ("metadata", "header", "context", "propagation"),
        "interceptors": ("interceptor", "middleware", "auth", "logging"),
        "deadline_timeout": ("deadline", "timeout", "policy"),
        "load_balancing": ("load", "balancing", "distribution"),
        "service_reflection": ("reflection", "discovery", "service"),
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
    "SourceGRPCServiceConfidence",
    "SourceGRPCServiceRequirement",
    "SourceGRPCServiceRequirementCategory",
    "SourceGRPCServiceRequirementsReport",
    "build_source_grpc_service_requirements",
    "build_source_grpc_service_requirements_report",
    "derive_source_grpc_service_requirements",
    "extract_source_grpc_service_requirements",
    "generate_source_grpc_service_requirements",
    "source_grpc_service_requirements_to_dict",
    "source_grpc_service_requirements_to_dicts",
    "source_grpc_service_requirements_to_markdown",
    "summarize_source_grpc_service_requirements",
]
