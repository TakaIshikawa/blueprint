"""Extract source-level observability expectations from brief-shaped inputs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ObservabilityExpectationCategory = Literal[
    "logs",
    "metrics",
    "traces",
    "dashboards",
    "alerts",
    "audit_events",
    "slos",
    "anomaly_detection",
    "debug_tooling",
]

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|should|need(?:s)? to|required|requires?|requirement|ensure|"
    r"support|capture|emit|record|track|monitor|alert|page|dashboard|instrument|"
    r"trace|log|audit|measure|debug|diagnos(?:e|is)|done when|acceptance)\b",
    re.I,
)
_MEASURABLE_RE = re.compile(
    r"(?:\b\d+(?:\.\d+)?\s*(?:%|percent|ms|milliseconds?|s|seconds?|minutes?|"
    r"hours?|events?|errors?|requests?|rps|qps|incidents?)\b|[<>]=?\s*\d+|"
    r"\bp\d{2}\b|99\.\d+)",
    re.I,
)
_OBSERVABILITY_SECTION_RE = re.compile(
    r"\b(?:observability|monitoring|telemetry|instrumentation|operations?|debugging|"
    r"diagnostics?|alerting|dashboards?|audit(?:ing)?|slo|slos|service level)\b",
    re.I,
)
_STRUCTURED_OBSERVABILITY_FIELD_RE = re.compile(
    r"(?:observability|monitoring|telemetry|instrumentation|alerting|alerts?|"
    r"dashboards?|metrics?|logs?|logging|traces?|tracing|audit(?:_events?|_logs?)?|"
    r"slos?|service_level|anomal(?:y|ies)|debug|diagnostics?)",
    re.I,
)
_STRUCTURED_CONTEXT_FIELD_RE = re.compile(
    r"(?:acceptance|criteria|definition_of_done|done|risk|risks|constraints?|"
    r"requirements?|nonfunctional|non_functional|metadata|validation|success)",
    re.I,
)

_CATEGORY_ORDER: dict[ObservabilityExpectationCategory, int] = {
    "logs": 0,
    "metrics": 1,
    "traces": 2,
    "dashboards": 3,
    "alerts": 4,
    "audit_events": 5,
    "slos": 6,
    "anomaly_detection": 7,
    "debug_tooling": 8,
}
_CATEGORY_PATTERNS: dict[ObservabilityExpectationCategory, re.Pattern[str]] = {
    "logs": re.compile(r"\b(?:log|logs|logging|structured logs?|logger|log line)\b", re.I),
    "metrics": re.compile(
        r"\b(?:metric|metrics|counter|histogram|gauge|timer|telemetry|measure|"
        r"measurement|instrument(?:ation)?|rate|latency|duration|throughput)\b",
        re.I,
    ),
    "traces": re.compile(
        r"\b(?:trace|traces|tracing|span|spans|distributed tracing|opentelemetry|otel)\b",
        re.I,
    ),
    "dashboards": re.compile(
        r"\b(?:dashboard|dashboards|grafana|datadog|chart|charts|panel|panels|reporting view)\b",
        re.I,
    ),
    "alerts": re.compile(
        r"\b(?:alert|alerts|alerting|page|paging|pagerduty|on-call|oncall|threshold|notify ops)\b",
        re.I,
    ),
    "audit_events": re.compile(
        r"\b(?:audit event|audit events|audit log|audit logs|audit trail|event log|"
        r"compliance event|security event)\b",
        re.I,
    ),
    "slos": re.compile(
        r"\b(?:slo|slos|sla|service level objective|service level objectives|"
        r"service level|error budget|availability target|uptime target)\b",
        re.I,
    ),
    "anomaly_detection": re.compile(
        r"\b(?:anomaly detection|anomaly|anomalies|outlier|outliers|spike detection|"
        r"drift detection|unusual activity|detect regressions?)\b",
        re.I,
    ),
    "debug_tooling": re.compile(
        r"\b(?:debug tool|debug tools|debug tooling|diagnostic(?:s)?|troubleshoot(?:ing)?|"
        r"debug mode|debug panel|debug endpoint|support console|investigation tool)\b",
        re.I,
    ),
}
_PLANNING_NOTES: dict[ObservabilityExpectationCategory, str] = {
    "logs": "Add task acceptance criteria for structured logs, fields, and retention.",
    "metrics": "Add task acceptance criteria for emitted metrics, dimensions, and validation queries.",
    "traces": "Add task acceptance criteria for trace propagation and span coverage across runtime boundaries.",
    "dashboards": "Add task acceptance criteria for dashboard panels and owner review before launch.",
    "alerts": "Add task acceptance criteria for alert thresholds, routing, and paging behavior.",
    "audit_events": "Add task acceptance criteria for audit event schema, actor context, and retention.",
    "slos": "Add task acceptance criteria for SLO targets, measurement windows, and error-budget ownership.",
    "anomaly_detection": "Add task acceptance criteria for anomaly detection rules and investigation workflow.",
    "debug_tooling": "Add task acceptance criteria for diagnostic tooling, access controls, and support handoff.",
}


@dataclass(frozen=True, slots=True)
class SourceObservabilityExpectation:
    """One observability expectation category found in source evidence."""

    source_id: str | None
    category: ObservabilityExpectationCategory
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)
    suggested_planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "category": self.category,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "suggested_planning_note": self.suggested_planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceObservabilityExpectationReport:
    """Source-level observability expectation report."""

    source_id: str | None = None
    expectations: tuple[SourceObservabilityExpectation, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "expectations": [expectation.to_dict() for expectation in self.expectations],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return expectation records as plain dictionaries."""
        return [expectation.to_dict() for expectation in self.expectations]

    @property
    def records(self) -> tuple[SourceObservabilityExpectation, ...]:
        """Compatibility view matching reports that name extracted items records."""
        return self.expectations


def build_source_observability_expectation_report(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> SourceObservabilityExpectationReport:
    """Build an observability expectation report from brief-shaped input."""
    source_id, payload = _source_payload(source)
    expectations = tuple(_merge_candidates(source_id, _expectation_candidates(payload)))
    return SourceObservabilityExpectationReport(
        source_id=source_id,
        expectations=expectations,
        summary=_summary(expectations),
    )


def extract_source_observability_expectations(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[SourceObservabilityExpectation, ...]:
    """Return observability expectation records from brief-shaped input."""
    return build_source_observability_expectation_report(source).expectations


def summarize_source_observability_expectations(
    source_or_expectations: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | str
        | tuple[SourceObservabilityExpectation, ...]
        | list[SourceObservabilityExpectation]
        | object
    ),
) -> SourceObservabilityExpectationReport:
    """Build a source observability report, accepting already extracted records."""
    if _looks_like_expectations(source_or_expectations):
        expectations = tuple(source_or_expectations)  # type: ignore[arg-type]
        source_id = expectations[0].source_id if expectations else None
        return SourceObservabilityExpectationReport(
            source_id=source_id,
            expectations=expectations,
            summary=_summary(expectations),
        )
    return build_source_observability_expectation_report(source_or_expectations)


def source_observability_expectations_to_dicts(
    expectations: tuple[SourceObservabilityExpectation, ...] | list[SourceObservabilityExpectation],
) -> list[dict[str, Any]]:
    """Serialize observability expectation records to dictionaries."""
    return [expectation.to_dict() for expectation in expectations]


def source_observability_expectation_report_to_dict(
    report: SourceObservabilityExpectationReport,
) -> dict[str, Any]:
    """Serialize a source observability expectation report to a plain dictionary."""
    return report.to_dict()


source_observability_expectation_report_to_dict.__test__ = False


def source_observability_expectations_to_dict(
    report: SourceObservabilityExpectationReport,
) -> dict[str, Any]:
    """Serialize a source observability expectation report to a plain dictionary."""
    return report.to_dict()


source_observability_expectations_to_dict.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    category: ObservabilityExpectationCategory
    confidence: float
    evidence: str


def _expectation_candidates(payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_field, value in _brief_values(payload):
        field_context = _field_context(source_field)
        for segment, section_context in _segments(value):
            categories = _categories(segment)
            if not categories:
                continue
            if not _expectation_context(segment, field_context, section_context):
                continue
            confidence = _confidence(segment, field_context, section_context)
            evidence = _evidence_snippet(source_field, segment)
            for category in categories:
                candidates.append(
                    _Candidate(category=category, confidence=confidence, evidence=evidence)
                )
    return candidates


def _merge_candidates(
    source_id: str | None,
    candidates: Iterable[_Candidate],
) -> list[SourceObservabilityExpectation]:
    by_category: dict[ObservabilityExpectationCategory, list[_Candidate]] = {}
    for candidate in candidates:
        by_category.setdefault(candidate.category, []).append(candidate)

    expectations: list[SourceObservabilityExpectation] = []
    for category in _CATEGORY_ORDER:
        category_candidates = by_category.get(category, [])
        if not category_candidates:
            continue
        expectations.append(
            SourceObservabilityExpectation(
                source_id=source_id,
                category=category,
                confidence=round(max(candidate.confidence for candidate in category_candidates), 2),
                evidence=tuple(
                    _dedupe_evidence(candidate.evidence for candidate in category_candidates)
                )[:4],
                suggested_planning_note=_PLANNING_NOTES[category],
            )
        )
    return expectations


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | ImplementationBrief | str | object,
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, (SourceBrief, ImplementationBrief)):
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        for model in (SourceBrief, ImplementationBrief):
            try:
                value = model.model_validate(source).model_dump(mode="python")
                return _source_id(value), dict(value)
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    return None, {}


def _source_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_id"))
        or _optional_text(payload.get("source_brief_id"))
    )


def _brief_values(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "problem",
        "problem_statement",
        "mvp_goal",
        "workflow_context",
        "architecture_notes",
        "data_requirements",
        "validation_plan",
    ):
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for field_name in (
        "goals",
        "scope",
        "constraints",
        "risks",
        "acceptance",
        "acceptance_criteria",
        "criteria",
        "definition_of_done",
        "requirements",
        "nonfunctional_requirements",
        "non_functional_requirements",
    ):
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for field_name in ("source_payload", "metadata"):
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited:
            continue
        _append_value(values, str(key), payload[key])
    return values


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            _append_value(values, f"{source_field}.{key}", value[key])
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if isinstance(value, str):
        if value.strip():
            values.append((source_field, value))
        return
    if text := _optional_text(value):
        values.append((source_field, text))


def _segments(value: str) -> list[tuple[str, bool]]:
    segments: list[tuple[str, bool]] = []
    in_observability_section = False
    for raw_line in value.splitlines() or [value]:
        line = raw_line.strip()
        if not line:
            continue
        heading = _HEADING_RE.match(line)
        if heading:
            title = _clean_text(heading.group("title"))
            in_observability_section = bool(_OBSERVABILITY_SECTION_RE.search(title))
            if title:
                segments.append((title, in_observability_section))
            continue
        text = _clean_text(line)
        parts = (
            [text] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _sentence_parts(text)
        )
        for part in parts:
            if part:
                segments.append((part, in_observability_section))
    if not segments:
        text = _clean_text(value)
        return [(text, False)] if text else []
    return segments


def _sentence_parts(value: str) -> list[str]:
    return [_clean_text(part) for part in _SENTENCE_SPLIT_RE.split(value) if _clean_text(part)]


def _categories(text: str) -> list[ObservabilityExpectationCategory]:
    return [category for category, pattern in _CATEGORY_PATTERNS.items() if pattern.search(text)]


def _field_context(source_field: str) -> tuple[bool, bool]:
    normalized = source_field.replace("-", "_").replace(" ", "_")
    return (
        bool(_STRUCTURED_OBSERVABILITY_FIELD_RE.search(normalized)),
        bool(_STRUCTURED_CONTEXT_FIELD_RE.search(normalized)),
    )


def _expectation_context(
    text: str, field_context: tuple[bool, bool], section_context: bool
) -> bool:
    has_observability_field, has_structured_context = field_context
    return bool(
        has_observability_field
        or section_context
        or _OBSERVABILITY_SECTION_RE.search(text)
        or _REQUIREMENT_RE.search(text)
        or has_structured_context
    )


def _confidence(text: str, field_context: tuple[bool, bool], section_context: bool) -> float:
    has_observability_field, has_structured_context = field_context
    score = 0.58
    if has_observability_field:
        score += 0.18
    if has_structured_context:
        score += 0.12
    if _REQUIREMENT_RE.search(text):
        score += 0.08
    if _MEASURABLE_RE.search(text):
        score += 0.06
    if section_context:
        score += 0.05
    if _OBSERVABILITY_SECTION_RE.search(text):
        score += 0.03
    return round(min(score, 0.95), 2)


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _summary(expectations: tuple[SourceObservabilityExpectation, ...]) -> dict[str, Any]:
    return {
        "expectation_count": len(expectations),
        "high_confidence_count": sum(
            1 for expectation in expectations if expectation.confidence >= 0.85
        ),
        "category_counts": {
            category: sum(1 for expectation in expectations if expectation.category == category)
            for category in _CATEGORY_ORDER
        },
        "categories": [expectation.category for expectation in expectations],
    }


def _clean_text(value: str) -> str:
    text = _CHECKBOX_RE.sub("", value.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


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


def _looks_like_expectations(value: Any) -> bool:
    if not isinstance(value, (list, tuple)):
        return False
    return all(isinstance(item, SourceObservabilityExpectation) for item in value)


__all__ = [
    "ObservabilityExpectationCategory",
    "SourceObservabilityExpectation",
    "SourceObservabilityExpectationReport",
    "build_source_observability_expectation_report",
    "extract_source_observability_expectations",
    "source_observability_expectation_report_to_dict",
    "source_observability_expectations_to_dict",
    "source_observability_expectations_to_dicts",
    "summarize_source_observability_expectations",
]
