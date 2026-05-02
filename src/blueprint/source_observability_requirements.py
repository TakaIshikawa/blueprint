"""Extract source-level observability requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ObservabilityRequirementConcern = Literal[
    "logging",
    "metrics",
    "tracing",
    "alerting",
    "dashboards",
    "slo_monitoring",
    "audit_evidence",
    "incident_diagnostics",
]
ObservabilityRequirementConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_CONCERN_ORDER: tuple[ObservabilityRequirementConcern, ...] = (
    "logging",
    "metrics",
    "tracing",
    "alerting",
    "dashboards",
    "slo_monitoring",
    "audit_evidence",
    "incident_diagnostics",
)
_CONFIDENCE_ORDER: dict[ObservabilityRequirementConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|"
    r"acceptance|done when|before launch|cannot ship|support|capture|emit|record|"
    r"track|monitor|alert|page|dashboard|instrument|trace|log|measure|diagnos(?:e|is))\b",
    re.I,
)
_OBSERVABILITY_CONTEXT_RE = re.compile(
    r"\b(?:observability|monitoring|telemetry|instrumentation|operational visibility|"
    r"runtime visibility|logs?|logging|metrics?|traces?|tracing|spans?|alert(?:ing|s)?|"
    r"pages?|paging|dashboard(?:s)?|slo monitoring|slo burn|burn rate|error budget|"
    r"audit evidence|operational evidence|diagnostics?|debugg(?:ing)?|incident|"
    r"triage|troubleshoot(?:ing)?|root cause|runbook)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:observability|monitoring|telemetry|instrumentation|operations?|requirements?|"
    r"acceptance|criteria|definition_of_done|constraints?|risks?|logs?|logging|metrics?|"
    r"traces?|tracing|alerts?|alerting|dashboards?|slo|audit|evidence|incident|"
    r"diagnostics?|debug)",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:observability|monitoring|telemetry|instrumentation|"
    r"logs?|metrics?|traces?|alerts?|dashboards?|diagnostics?).*?"
    r"\b(?:in scope|required|needed|changes?)\b",
    re.I,
)
_PURE_SLO_TARGET_RE = re.compile(
    r"\b(?:slo|sla|service level objective|availability target|uptime target|uptime|"
    r"availability)\b[^.?!]{0,80}\b(?:\d+(?:\.\d+)?\s*%|nine[s]?|nines|target)\b",
    re.I,
)
_SLO_MONITORING_RE = re.compile(
    r"\b(?:monitor|monitoring|alert|alerts|alerting|dashboard|dashboards|burn rate|"
    r"error budget|slo burn|slo report|service level indicator|sli)\b",
    re.I,
)
_CONCERN_PATTERNS: dict[ObservabilityRequirementConcern, re.Pattern[str]] = {
    "logging": re.compile(
        r"\b(?:log|logs|logging|structured logs?|log fields?|logger|request log|"
        r"application log|correlation id|request id)\b",
        re.I,
    ),
    "metrics": re.compile(
        r"\b(?:metric|metrics|counter|counters|histogram|histograms|gauge|gauges|timer|"
        r"telemetry|measure(?:ment)?|instrument(?:ation)?|latency|duration|throughput|"
        r"error rate|success rate)\b",
        re.I,
    ),
    "tracing": re.compile(
        r"\b(?:trace|traces|tracing|span|spans|distributed tracing|trace propagation|"
        r"opentelemetry|otel)\b",
        re.I,
    ),
    "alerting": re.compile(
        r"\b(?:alert|alerts|alerting|alarm|page|paging|pagerduty|opsgenie|on-call|"
        r"oncall|notify ops|threshold breach)\b",
        re.I,
    ),
    "dashboards": re.compile(
        r"\b(?:dashboard|dashboards|grafana|datadog|chart|charts|panel|panels|"
        r"monitoring view|operational view|reporting view)\b",
        re.I,
    ),
    "slo_monitoring": re.compile(
        r"\b(?:slo monitoring|monitor(?:ing)? slo|slo burn|burn rate|error budget|"
        r"service level indicator|sli|slo dashboard|slo alert|slo report)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit evidence|operational evidence|audit event|audit events|audit logs?|"
        r"compliance evidence|evidence trail|operator action|admin action evidence)\b",
        re.I,
    ),
    "incident_diagnostics": re.compile(
        r"\b(?:incident diagnostics?|diagnostic(?:s)?|debugg(?:ing)?|troubleshoot(?:ing)?|"
        r"root cause|triage|runbook evidence|failure investigation|post[- ]incident)\b",
        re.I,
    ),
}
_IMPLEMENTATION_NOTE_BY_CONCERN: dict[ObservabilityRequirementConcern, str] = {
    "logging": "Define structured log events, required fields, correlation identifiers, and retention expectations.",
    "metrics": "Define metric names, types, dimensions, emission points, and validation queries.",
    "tracing": "Plan trace propagation, span boundaries, sampling, and cross-service context handoff.",
    "alerting": "Specify alert thresholds, routing, severity, suppression, and on-call ownership.",
    "dashboards": "Specify dashboard panels, filters, owners, and pre-launch review criteria.",
    "slo_monitoring": "Connect SLO visibility to SLIs, burn-rate alerts, dashboards, and error-budget review.",
    "audit_evidence": "Capture operational evidence schema, actor context, timestamps, retention, and access controls.",
    "incident_diagnostics": "Plan diagnostic signals, runbook links, failure context, and incident triage workflows.",
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
    "architecture_notes",
    "requirements",
    "constraints",
    "success_criteria",
    "acceptance_criteria",
    "definition_of_done",
    "risks",
    "operations",
    "monitoring",
    "observability",
    "validation_plan",
    "metadata",
    "brief_metadata",
    "implementation_notes",
    "source_payload",
)
_IGNORED_FIELDS = {
    "id",
    "source_brief_id",
    "source_id",
    "source_project",
    "source_entity_type",
    "created_at",
    "updated_at",
    "source_links",
}


@dataclass(frozen=True, slots=True)
class SourceObservabilityRequirement:
    """One source-backed observability requirement."""

    source_brief_id: str | None
    concern: ObservabilityRequirementConcern
    evidence: tuple[str, ...] = field(default_factory=tuple)
    source_field_paths: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: ObservabilityRequirementConfidence = "medium"
    implementation_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "concern": self.concern,
            "evidence": list(self.evidence),
            "source_field_paths": list(self.source_field_paths),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "implementation_note": self.implementation_note,
        }


@dataclass(frozen=True, slots=True)
class SourceObservabilityRequirementsReport:
    """Source-level observability requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceObservabilityRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceObservabilityRequirement, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return observability requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Observability Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        concern_counts = self.summary.get("concern_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            f"- Status: {self.summary.get('status', 'unknown')}",
            "- Concern counts: "
            + ", ".join(
                f"{concern} {concern_counts.get(concern, 0)}" for concern in _CONCERN_ORDER
            ),
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        if not self.requirements:
            lines.extend(["", "No source observability requirements were inferred."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Concern | Confidence | Source Field Paths | Evidence | Implementation Note |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{requirement.concern} | "
                f"{requirement.confidence} | "
                f"{_markdown_cell('; '.join(requirement.source_field_paths))} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} | "
                f"{_markdown_cell(requirement.implementation_note)} |"
            )
        return "\n".join(lines)


def build_source_observability_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceObservabilityRequirementsReport:
    """Extract observability requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda requirement: (
                _optional_text(requirement.source_brief_id) or "",
                _concern_index(requirement.concern),
                _CONFIDENCE_ORDER[requirement.confidence],
                requirement.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceObservabilityRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_observability_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceObservabilityRequirementsReport:
    """Compatibility alias for building an observability requirements report."""
    return build_source_observability_requirements(source)


def generate_source_observability_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceObservabilityRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_observability_requirements(source)


def derive_source_observability_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceObservabilityRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_observability_requirements(source)


def summarize_source_observability_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceObservabilityRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted observability requirements."""
    if isinstance(source_or_result, SourceObservabilityRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_observability_requirements(source_or_result).summary


def source_observability_requirements_to_dict(
    report: SourceObservabilityRequirementsReport,
) -> dict[str, Any]:
    """Serialize an observability requirements report to a plain dictionary."""
    return report.to_dict()


source_observability_requirements_to_dict.__test__ = False


def source_observability_requirements_to_dicts(
    requirements: (
        tuple[SourceObservabilityRequirement, ...]
        | list[SourceObservabilityRequirement]
        | SourceObservabilityRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize observability requirement records to dictionaries."""
    if isinstance(requirements, SourceObservabilityRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_observability_requirements_to_dicts.__test__ = False


def source_observability_requirements_to_markdown(
    report: SourceObservabilityRequirementsReport,
) -> str:
    """Render an observability requirements report as Markdown."""
    return report.to_markdown()


source_observability_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    concern: ObservabilityRequirementConcern
    evidence: str
    source_field_path: str
    matched_terms: tuple[str, ...]
    confidence: ObservabilityRequirementConfidence


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
    if isinstance(
        source, (str, bytes, bytearray, Mapping, SourceBrief, ImplementationBrief)
    ) or hasattr(source, "model_dump"):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


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


def _candidates_for_briefs(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_brief_id, payload in brief_payloads:
        for segment in _candidate_segments(payload):
            if not _is_requirement(segment):
                continue
            searchable = f"{_field_words(segment.source_field)} {segment.text}"
            concerns = [
                concern
                for concern in _CONCERN_ORDER
                if _CONCERN_PATTERNS[concern].search(searchable)
                and not _is_pure_slo_target(segment.text, concern)
            ]
            for concern in _dedupe(concerns):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        concern=concern,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        source_field_path=segment.source_field,
                        matched_terms=_matched_terms(concern, searchable),
                        confidence=_confidence(segment),
                    )
                )
    return candidates


def _merge_candidates(
    candidates: Iterable[_Candidate],
) -> list[SourceObservabilityRequirement]:
    grouped: dict[tuple[str | None, ObservabilityRequirementConcern], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.concern), []).append(candidate)

    requirements: list[SourceObservabilityRequirement] = []
    for (source_brief_id, concern), items in grouped.items():
        evidence = tuple(
            sorted(_dedupe_evidence(item.evidence for item in items), key=str.casefold)
        )[:5]
        source_field_paths = tuple(
            sorted(_dedupe(item.source_field_path for item in items), key=str.casefold)
        )
        matched_terms = tuple(
            sorted(
                _dedupe(term for item in items for term in item.matched_terms),
                key=str.casefold,
            )
        )
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        requirements.append(
            SourceObservabilityRequirement(
                source_brief_id=source_brief_id,
                concern=concern,
                evidence=evidence,
                source_field_paths=source_field_paths,
                matched_terms=matched_terms,
                confidence=confidence,
                implementation_note=_IMPLEMENTATION_NOTE_BY_CONCERN[concern],
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
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(
                _STRUCTURED_FIELD_RE.search(key_text) or _OBSERVABILITY_CONTEXT_RE.search(key_text)
            )
            _append_value(segments, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text in _segments(text):
            segments.append(_Segment(source_field, segment_text, field_context))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for raw_line in value.splitlines() or [value]:
        cleaned = _clean_text(raw_line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(raw_line) or _CHECKBOX_RE.match(raw_line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for part in parts:
            for clause in _CLAUSE_SPLIT_RE.split(part):
                text = _clean_text(clause)
                if text:
                    segments.append(text)
    return segments


def _is_requirement(segment: _Segment) -> bool:
    if _NEGATED_SCOPE_RE.search(segment.text):
        return False
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    if not _OBSERVABILITY_CONTEXT_RE.search(searchable):
        return False
    if _is_pure_slo_target(segment.text, "slo_monitoring"):
        return False
    if segment.section_context:
        return True
    if segment.source_field == "title" and not _REQUIRED_RE.search(segment.text):
        return False
    return bool(_REQUIRED_RE.search(segment.text))


def _is_pure_slo_target(text: str, concern: ObservabilityRequirementConcern) -> bool:
    if concern != "slo_monitoring":
        return False
    return bool(_PURE_SLO_TARGET_RE.search(text) and not _SLO_MONITORING_RE.search(text))


def _matched_terms(
    concern: ObservabilityRequirementConcern,
    text: str,
) -> tuple[str, ...]:
    return tuple(
        _dedupe(_clean_text(match.group(0)) for match in _CONCERN_PATTERNS[concern].finditer(text))
    )


def _confidence(segment: _Segment) -> ObservabilityRequirementConfidence:
    if _REQUIRED_RE.search(segment.text):
        return "high"
    if segment.section_context:
        return "medium"
    return "low"


def _summary(
    requirements: tuple[SourceObservabilityRequirement, ...],
    source_count: int,
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "concern_counts": {
            concern: sum(1 for requirement in requirements if requirement.concern == concern)
            for concern in _CONCERN_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for requirement in requirements if requirement.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "concerns": [requirement.concern for requirement in requirements],
        "status": "ready_for_planning" if requirements else "no_observability_language",
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
        "requirements",
        "acceptance_criteria",
        "definition_of_done",
        "risks",
        "operations",
        "monitoring",
        "observability",
        "metadata",
        "brief_metadata",
        "implementation_notes",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _concern_index(concern: ObservabilityRequirementConcern) -> int:
    return _CONCERN_ORDER.index(concern)


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ")


def _clean_text(value: Any) -> str:
    text = "" if value is None or isinstance(value, (bytes, bytearray)) else str(value)
    text = _CHECKBOX_RE.sub("", text.strip())
    text = _BULLET_RE.sub("", text)
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
    "ObservabilityRequirementConcern",
    "ObservabilityRequirementConfidence",
    "SourceObservabilityRequirement",
    "SourceObservabilityRequirementsReport",
    "build_source_observability_requirements",
    "derive_source_observability_requirements",
    "extract_source_observability_requirements",
    "generate_source_observability_requirements",
    "source_observability_requirements_to_dict",
    "source_observability_requirements_to_dicts",
    "source_observability_requirements_to_markdown",
    "summarize_source_observability_requirements",
]
