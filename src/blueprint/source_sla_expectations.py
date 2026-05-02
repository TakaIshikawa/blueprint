"""Extract SLA and SLO expectations from source and implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SLAExpectationCategory = Literal[
    "availability",
    "latency",
    "support_response",
    "maintenance_window",
    "error_budget",
    "contractual_sla",
]
_T = TypeVar("_T")

_CATEGORY_ORDER: tuple[SLAExpectationCategory, ...] = (
    "availability",
    "latency",
    "support_response",
    "maintenance_window",
    "error_budget",
    "contractual_sla",
)
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_SLA_CONTEXT_RE = re.compile(
    r"\b(?:sla|slo|service level(?: agreement| objective)?|service commitment|"
    r"service commitments|uptime commitment|uptime commitments|operational commitment|"
    r"operational commitments|contract(?:ual)? service level)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:sla|slo|service[-_ ]?level|availability|uptime|latency|response[-_ ]?time|"
    r"support[-_ ]?response|first[-_ ]?response|maintenance|error[-_ ]?budget|"
    r"contract(?:ual)?|credits?|penalt(?:y|ies))",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|need(?:s)?|commit(?:s|ted|ment)?|guarantee(?:d|s)?|"
    r"target|objective|threshold|within|under|below|at least|no more than|no less than|"
    r"before launch|reviewed before)\b",
    re.I,
)
_THRESHOLD_RE = re.compile(
    r"\b(?:\d+(?:\.\d+)?\s*(?:%|ms|milliseconds?|s|sec|seconds?|mins?|minutes?|hours?|hrs?)|"
    r"p\d{2}|four nines|five nines|24[/-]7|business hours?|same day|next business day)(?=$|\W)",
    re.I,
)
_IGNORED_FIELDS = {
    "created_at",
    "updated_at",
    "source_project",
    "source_entity_type",
    "source_links",
}

_CATEGORY_PATTERNS: dict[SLAExpectationCategory, re.Pattern[str]] = {
    "availability": re.compile(
        r"\b(?:availability|available|uptime|downtime|24[/-]7|four nines|five nines|"
        r"99(?:\.\d+)?\s*%|service continuity)\b",
        re.I,
    ),
    "latency": re.compile(
        r"\b(?:latency|p95|p99|response time|responds? in|time to first byte|ttfb|"
        r"timeout|milliseconds?|ms\b|seconds? end[- ]?to[- ]?end)\b",
        re.I,
    ),
    "support_response": re.compile(
        r"\b(?:support response|first response|response commitment|respond within|"
        r"support tickets?|helpdesk|customer support|p[0-4]\s+(?:response|incident)|"
        r"sev(?:erity)?\s*[0-4]|escalat(?:e|ion))\b",
        re.I,
    ),
    "maintenance_window": re.compile(
        r"\b(?:maintenance window|scheduled maintenance|maintenance period|service window|"
        r"change window|planned downtime|downtime window|blackout window)\b",
        re.I,
    ),
    "error_budget": re.compile(
        r"\b(?:error budget|budget burn|burn rate|allowed downtime|availability budget|"
        r"slo burn|error[- ]?budget policy)\b",
        re.I,
    ),
    "contractual_sla": re.compile(
        r"\b(?:contractual sla|contracted sla|service level agreement|contract service level|"
        r"contractual service level|vendor sla|customer sla|sla credits?|service credits?|"
        r"penalt(?:y|ies)|commercial terms?)\b",
        re.I,
    ),
}
_OWNER_BY_CATEGORY: dict[SLAExpectationCategory, str] = {
    "availability": "engineering_oncall",
    "latency": "engineering_oncall",
    "support_response": "support_lead",
    "maintenance_window": "release_owner",
    "error_budget": "sre_owner",
    "contractual_sla": "account_owner",
}
_PLANNING_NOTE_BY_CATEGORY: dict[SLAExpectationCategory, str] = {
    "availability": "Translate the availability commitment into monitoring, alerting, and launch readiness criteria.",
    "latency": "Add latency objectives to performance validation and production monitoring plans.",
    "support_response": "Plan support routing, ownership, and response-time tracking before rollout.",
    "maintenance_window": "Schedule implementation and release work around the stated maintenance window.",
    "error_budget": "Include error-budget tracking and burn-rate escalation in the execution plan.",
    "contractual_sla": "Confirm contractual SLA obligations, credits, and owner sign-off before task generation.",
}


@dataclass(frozen=True, slots=True)
class SourceSLAExpectation:
    """One source-backed SLA or SLO expectation category."""

    category: SLAExpectationCategory
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)
    suggested_owner: str = ""
    suggested_planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "category": self.category,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "suggested_owner": self.suggested_owner,
            "suggested_planning_note": self.suggested_planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceSLAExpectationsReport:
    """Brief-level SLA expectation report before implementation planning."""

    brief_id: str | None = None
    expectations: tuple[SourceSLAExpectation, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSLAExpectation, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.expectations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "expectations": [expectation.to_dict() for expectation in self.expectations],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return SLA expectation records as plain dictionaries."""
        return [expectation.to_dict() for expectation in self.expectations]


def build_source_sla_expectations(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceSLAExpectationsReport:
    """Build an SLA expectation report from brief-shaped input."""
    brief_id, payload = _source_payload(source)
    expectations = tuple(_merge_candidates(_expectation_candidates(payload)))
    return SourceSLAExpectationsReport(
        brief_id=brief_id,
        expectations=expectations,
        summary=_summary(expectations),
    )


def build_source_sla_expectation_report(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> SourceSLAExpectationsReport:
    """Compatibility helper for callers that use singular report naming."""
    return build_source_sla_expectations(source)


def extract_source_sla_expectations(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[SourceSLAExpectation, ...]:
    """Return SLA expectation records extracted from brief-shaped input."""
    return build_source_sla_expectations(source).expectations


def source_sla_expectations_to_dict(report: SourceSLAExpectationsReport) -> dict[str, Any]:
    """Serialize a source SLA expectations report to a plain dictionary."""
    return report.to_dict()


source_sla_expectations_to_dict.__test__ = False


def source_sla_expectations_to_dicts(
    expectations: tuple[SourceSLAExpectation, ...] | list[SourceSLAExpectation] | SourceSLAExpectationsReport,
) -> list[dict[str, Any]]:
    """Serialize source SLA expectation records to dictionaries."""
    if isinstance(expectations, SourceSLAExpectationsReport):
        return expectations.to_dicts()
    return [expectation.to_dict() for expectation in expectations]


source_sla_expectations_to_dicts.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: SLAExpectationCategory
    confidence: float
    evidence: str


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | object,
) -> tuple[str | None, dict[str, Any]]:
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
    if not isinstance(source, (str, bytes, bytearray)):
        payload = _object_payload(source)
        return _brief_id(payload), payload
    return None, {}


def _brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _expectation_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for segment in _candidate_segments(payload):
        searchable = f"{_field_words(segment.source_field)} {segment.text}"
        categories = [category for category in _CATEGORY_ORDER if _CATEGORY_PATTERNS[category].search(searchable)]
        if not categories:
            continue
        if not _is_expectation(segment.text, segment.source_field, segment.section_context, categories):
            continue
        evidence = _evidence_snippet(segment.source_field, segment.text)
        confidence = _confidence(segment.text, segment.source_field, segment.section_context)
        for category in categories:
            candidates.append(_Candidate(category=category, confidence=confidence, evidence=evidence))
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceSLAExpectation]:
    by_category: dict[SLAExpectationCategory, list[_Candidate]] = {}
    for candidate in candidates:
        by_category.setdefault(candidate.category, []).append(candidate)

    expectations: list[SourceSLAExpectation] = []
    for category in _CATEGORY_ORDER:
        items = by_category.get(category, [])
        if not items:
            continue
        expectations.append(
            SourceSLAExpectation(
                category=category,
                confidence=round(max(item.confidence for item in items), 2),
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:5],
                suggested_owner=_OWNER_BY_CATEGORY[category],
                suggested_planning_note=_PLANNING_NOTE_BY_CATEGORY[category],
            )
        )
    return expectations


def _candidate_segments(payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "problem_statement",
        "mvp_goal",
        "workflow_context",
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
        "sla",
        "slo",
        "service_levels",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
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
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text))
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
            section_context = inherited_context or bool(_SLA_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
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


def _is_expectation(
    text: str,
    source_field: str,
    section_context: bool,
    categories: Iterable[SLAExpectationCategory],
) -> bool:
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if field_context or section_context or _SLA_CONTEXT_RE.search(text):
        return True
    if _REQUIREMENT_RE.search(text) and _THRESHOLD_RE.search(text):
        return True
    if _REQUIREMENT_RE.search(text) and any(category in {"maintenance_window", "error_budget"} for category in categories):
        return True
    if "availability" in categories and re.search(r"\b(?:uptime|99(?:\.\d+)?\s*%|24[/-]7)\b", text, re.I):
        return True
    return False


def _confidence(text: str, source_field: str, section_context: bool) -> float:
    score = 0.68
    if _STRUCTURED_FIELD_RE.search(_field_words(source_field)):
        score += 0.08
    if section_context or _SLA_CONTEXT_RE.search(text):
        score += 0.07
    if _REQUIREMENT_RE.search(text):
        score += 0.07
    if _THRESHOLD_RE.search(text):
        score += 0.08
    return round(min(score, 0.95), 2)


def _summary(expectations: tuple[SourceSLAExpectation, ...]) -> dict[str, Any]:
    return {
        "expectation_count": len(expectations),
        "category_counts": {
            category: sum(1 for expectation in expectations if expectation.category == category)
            for category in _CATEGORY_ORDER
        },
        "high_confidence_count": sum(1 for expectation in expectations if expectation.confidence >= 0.85),
        "categories": [expectation.category for expectation in expectations],
        "suggested_owner_counts": {
            owner: sum(1 for expectation in expectations if expectation.suggested_owner == owner)
            for owner in sorted({expectation.suggested_owner for expectation in expectations})
        },
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
        "sla",
        "slo",
        "service_levels",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _any_signal(text: str) -> bool:
    return bool(_SLA_CONTEXT_RE.search(text) or _STRUCTURED_FIELD_RE.search(text)) or any(
        pattern.search(text) for pattern in _CATEGORY_PATTERNS.values()
    )


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = str(value).strip()
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) > 180:
        cleaned = f"{cleaned[:177].rstrip()}..."
    return f"{source_field}: {cleaned}"


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
    "SLAExpectationCategory",
    "SourceSLAExpectation",
    "SourceSLAExpectationsReport",
    "build_source_sla_expectation_report",
    "build_source_sla_expectations",
    "extract_source_sla_expectations",
    "source_sla_expectations_to_dict",
    "source_sla_expectations_to_dicts",
]
