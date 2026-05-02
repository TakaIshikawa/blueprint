"""Extract reporting and analytics metric definitions from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


ReportingMetricConfidence = Literal["high", "medium", "low"]

_CONFIDENCE_ORDER: dict[ReportingMetricConfidence, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REPORTING_CONTEXT_RE = re.compile(
    r"\b(?:reporting|analytics?|dashboard|dashboards|report|reports|kpi|kpis|metric|"
    r"metrics|funnel|conversion rate|revenue|mrr|arr|retention|cohort|cohorts|"
    r"drilldown|drill down|operational report|scorecard|business intelligence|bi)\b",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|should|need(?:s)? to|required|requires?|requirement|ensure|"
    r"support|show|display|track|measure|report|include|break(?:s)? down|filter|"
    r"segment|define|defines|definition|acceptance|done when|owner)\b",
    re.I,
)
_SURFACE_RE = re.compile(
    r"\b(?P<surface>[A-Za-z][A-Za-z0-9 /_-]{0,80}?"
    r"(?:cohort retention drilldown|retention drilldown|dashboard|report|kpi|metric|"
    r"funnel|conversion rate|revenue metric|cohort|retention|drilldown|drill down|"
    r"scorecard))\b",
    re.I,
)
_METRIC_WORD_RE = re.compile(
    r"\b(?:activation|adoption|signup|onboarding|checkout|purchase|trial[- ]to[- ]paid|"
    r"conversion rate|conversion|funnel|retention|churn|cohort|revenue|mrr|arr|"
    r"net revenue retention|nrr|gross revenue retention|grr|ltv|arpu|sla breaches?|"
    r"tickets?|incidents?|backlog|throughput|usage|engagement|kpi|metric)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:reporting|analytics?|dashboards?|reports?|kpis?|metrics?|funnel|conversion|"
    r"revenue|retention|cohorts?|drilldowns?|scorecards?|acceptance|criteria|metadata|"
    r"owner|audience|freshness)",
    re.I,
)
_METRIC_KEY_RE = re.compile(r"\b(?:metric|name|surface|report|dashboard|kpi|definition)\b", re.I)
_NUMERATOR_KEY_RE = re.compile(r"\bnumerator\b", re.I)
_DENOMINATOR_KEY_RE = re.compile(r"\bdenominator\b", re.I)
_WINDOW_KEY_RE = re.compile(r"\b(?:window|period|aggregation|grain|granularity|timeframe|cadence)\b", re.I)
_DIMENSION_KEY_RE = re.compile(r"\b(?:dimension|dimensions|breakdown|breakdowns|group by|segment|segments)\b", re.I)
_FILTER_KEY_RE = re.compile(r"\b(?:filter|filters|where|scope|cohort criteria)\b", re.I)
_FRESHNESS_KEY_RE = re.compile(r"\b(?:freshness|latency|refresh|updated|update cadence|sla)\b", re.I)
_AUDIENCE_KEY_RE = re.compile(r"\b(?:owner|audience|dri|team|stakeholder|consumer|for)\b", re.I)
_RATE_RE = re.compile(r"\b(?:conversion rate|rate|retention|churn|funnel|nrr|grr|arpu|ltv)\b", re.I)
_NUMERATOR_RE = re.compile(r"\bnumerator\s*[:=]?\s*([^.;,\n]+)", re.I)
_DENOMINATOR_RE = re.compile(r"\bdenominator\s*[:=]?\s*([^.;,\n]+)", re.I)
_FROM_TO_RE = re.compile(r"\bfrom\s+([^.;,\n]+?)\s+to\s+([^.;,\n]+?)(?=\s+(?:over|by|for|within|broken|filtered|for)|[.;,\n]|$)", re.I)
_WINDOW_RE = re.compile(
    r"\b(?:daily|weekly|monthly|quarterly|annual|annually|hourly|real[- ]?time|"
    r"same day|near real[- ]?time)\b|"
    r"\b(?:over|within|for|last|past|rolling)\s+([^.;,\n]*(?:hour|day|week|month|quarter|year|days|weeks|months|quarters|years)[^.;,\n]*)",
    re.I,
)
_DIMENSION_RE = re.compile(
    r"\b(?:dimensions?|breakdowns?|segments?|group(?:ed)? by|broken down by|breakdown by|segmented by|by|per)\s*[:=]?\s+([^.;\n]+)",
    re.I,
)
_FILTER_RE = re.compile(r"\b(?:filters?|filtered to|filter(?:ed)? by|where|for only)\s*[:=]?\s+([^.;,\n]+)", re.I)
_FRESHNESS_RE = re.compile(
    r"\b(?:refresh(?:es|ed)?|updated|freshness|latency|available|data)\s+(?:within|every|daily|hourly|by)?\s*([^.;,\n]*(?:minute|hour|day|real[- ]?time|daily|hourly)[^.;,\n]*)",
    re.I,
)
_AUDIENCE_RE = re.compile(
    r"\b(?:for|available to|owner|owned by|audience|dri|team|stakeholder)\b\s*[:=]?\s*"
    r"(@?[A-Za-z][A-Za-z0-9 _./-]{1,60})",
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
    "acceptance",
    "acceptance_criteria",
    "success_criteria",
    "definition_of_done",
    "validation_plan",
    "data_requirements",
    "risks",
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
}
_MISSING_DETAIL_FIELDS = (
    "missing_numerator_denominator",
    "missing_time_window",
    "missing_dimension_filter",
    "missing_owner",
)


@dataclass(frozen=True, slots=True)
class SourceReportingMetricDefinition:
    """One source-backed reporting metric definition candidate."""

    source_brief_id: str | None
    metric_surface: str
    aggregation_or_window: str | None = None
    dimensions: tuple[str, ...] = field(default_factory=tuple)
    filters: tuple[str, ...] = field(default_factory=tuple)
    freshness_or_latency: str | None = None
    audience_hint: str | None = None
    numerator_hint: str | None = None
    denominator_hint: str | None = None
    missing_numerator_denominator: bool = False
    missing_time_window: bool = False
    missing_dimension_filter: bool = False
    missing_owner: bool = False
    confidence: ReportingMetricConfidence = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "metric_surface": self.metric_surface,
            "aggregation_or_window": self.aggregation_or_window,
            "dimensions": list(self.dimensions),
            "filters": list(self.filters),
            "freshness_or_latency": self.freshness_or_latency,
            "audience_hint": self.audience_hint,
            "numerator_hint": self.numerator_hint,
            "denominator_hint": self.denominator_hint,
            "missing_numerator_denominator": self.missing_numerator_denominator,
            "missing_time_window": self.missing_time_window,
            "missing_dimension_filter": self.missing_dimension_filter,
            "missing_owner": self.missing_owner,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class SourceReportingMetricDefinitionsReport:
    """Source-level reporting metric definitions report."""

    source_id: str | None = None
    metric_definitions: tuple[SourceReportingMetricDefinition, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceReportingMetricDefinition, ...]:
        """Compatibility view matching reports that name extracted items records."""
        return self.metric_definitions

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "metric_definitions": [definition.to_dict() for definition in self.metric_definitions],
            "summary": dict(self.summary),
            "records": [definition.to_dict() for definition in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return reporting metric definition records as plain dictionaries."""
        return [definition.to_dict() for definition in self.metric_definitions]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Reporting Metric Definitions Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Metric definitions found: {self.summary.get('metric_definition_count', 0)}",
            "- Confidence counts: "
            + ", ".join(
                f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER
            ),
        ]
        for flag in _MISSING_DETAIL_FIELDS:
            lines.append(
                f"- {flag.replace('_', ' ').title()}: "
                f"{self.summary.get('missing_detail_counts', {}).get(flag, 0)}"
            )
        if not self.metric_definitions:
            lines.extend(["", "No reporting metric definitions were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Metric Definitions",
                "",
                "| Source Brief | Metric Surface | Window | Dimensions | Filters | Audience | Flags | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for definition in self.metric_definitions:
            lines.append(
                "| "
                f"{_markdown_cell(definition.source_brief_id or '')} | "
                f"{_markdown_cell(definition.metric_surface)} | "
                f"{_markdown_cell(definition.aggregation_or_window or '')} | "
                f"{_markdown_cell(', '.join(definition.dimensions))} | "
                f"{_markdown_cell(', '.join(definition.filters))} | "
                f"{_markdown_cell(definition.audience_hint or '')} | "
                f"{_markdown_cell(', '.join(_flags(definition)) or 'none')} | "
                f"{_markdown_cell('; '.join(definition.evidence))} |"
            )
        return "\n".join(lines)


def build_source_reporting_metric_definitions(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceReportingMetricDefinitionsReport:
    """Extract reporting metric definition records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    definitions = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda definition: (
                _optional_text(definition.source_brief_id) or "",
                definition.metric_surface.casefold(),
                _CONFIDENCE_ORDER[definition.confidence],
                definition.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceReportingMetricDefinitionsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        metric_definitions=definitions,
        summary=_summary(definitions, len(brief_payloads)),
    )


def extract_source_reporting_metric_definitions(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceReportingMetricDefinitionsReport:
    """Compatibility alias for building a reporting metric definitions report."""
    return build_source_reporting_metric_definitions(source)


def generate_source_reporting_metric_definitions(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceReportingMetricDefinitionsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_reporting_metric_definitions(source)


def derive_source_reporting_metric_definitions(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceReportingMetricDefinitionsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_reporting_metric_definitions(source)


def summarize_source_reporting_metric_definitions(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceReportingMetricDefinitionsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted reporting metric definitions."""
    if isinstance(source_or_result, SourceReportingMetricDefinitionsReport):
        return dict(source_or_result.summary)
    return build_source_reporting_metric_definitions(source_or_result).summary


def source_reporting_metric_definitions_to_dict(
    report: SourceReportingMetricDefinitionsReport,
) -> dict[str, Any]:
    """Serialize a reporting metric definitions report to a plain dictionary."""
    return report.to_dict()


source_reporting_metric_definitions_to_dict.__test__ = False


def source_reporting_metric_definitions_to_dicts(
    definitions: (
        tuple[SourceReportingMetricDefinition, ...]
        | list[SourceReportingMetricDefinition]
        | SourceReportingMetricDefinitionsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize reporting metric definition records to dictionaries."""
    if isinstance(definitions, SourceReportingMetricDefinitionsReport):
        return definitions.to_dicts()
    return [definition.to_dict() for definition in definitions]


source_reporting_metric_definitions_to_dicts.__test__ = False


def source_reporting_metric_definitions_to_markdown(
    report: SourceReportingMetricDefinitionsReport,
) -> str:
    """Render a reporting metric definitions report as Markdown."""
    return report.to_markdown()


source_reporting_metric_definitions_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    metric_surface: str
    aggregation_or_window: str | None
    dimensions: tuple[str, ...]
    filters: tuple[str, ...]
    freshness_or_latency: str | None
    audience_hint: str | None
    numerator_hint: str | None
    denominator_hint: str | None
    evidence: str
    confidence: ReportingMetricConfidence


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
                value = model.model_validate(source).model_dump(mode="python")
                payload = dict(value)
                return _source_id(payload), payload
            except (TypeError, ValueError, ValidationError):
                continue
        payload = dict(source)
        return _source_id(payload), payload
    return None, _object_payload(source)


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
    metadata = _metadata_contexts(brief_payloads)
    for source_brief_id, payload in brief_payloads:
        context = metadata.get(source_brief_id, {})
        for source_field, segment in _candidate_segments(payload):
            if not _has_reporting_signal(segment, source_field):
                continue
            candidate = _candidate_from_text(source_brief_id, source_field, segment, context)
            if candidate:
                candidates.append(candidate)
    return candidates


def _candidate_from_text(
    source_brief_id: str | None,
    source_field: str,
    text: str,
    context: Mapping[str, str],
) -> _Candidate | None:
    if _context_only_field(source_field) and not _explicit_metric_definition(text):
        return None
    surface = _metric_surface(text, source_field)
    if not surface:
        return None
    numerator = _detail_match(_NUMERATOR_RE, text) or context.get("numerator_hint")
    denominator = _detail_match(_DENOMINATOR_RE, text) or context.get("denominator_hint")
    if not numerator or not denominator:
        from_to = _from_to(text)
        numerator = numerator or from_to[1]
        denominator = denominator or from_to[0]
    dimensions = _items(_detail_match(_DIMENSION_RE, text)) or _context_items(context.get("dimensions"))
    filters = _items(_detail_match(_FILTER_RE, text)) or _context_items(context.get("filters"))
    window = _window(text) or context.get("aggregation_or_window")
    audience = _audience(text) or context.get("audience_hint")
    freshness = _freshness(text) or context.get("freshness_or_latency")
    evidence = _evidence_snippet(source_field, text)
    return _build_candidate(
        source_brief_id=source_brief_id,
        metric_surface=surface,
        aggregation_or_window=window,
        dimensions=dimensions,
        filters=filters,
        freshness_or_latency=freshness,
        audience_hint=audience,
        numerator_hint=numerator,
        denominator_hint=denominator,
        evidence=evidence,
        source_field=source_field,
        text=text,
    )


def _build_candidate(
    *,
    source_brief_id: str | None,
    metric_surface: str,
    aggregation_or_window: str | None,
    dimensions: tuple[str, ...],
    filters: tuple[str, ...],
    freshness_or_latency: str | None,
    audience_hint: str | None,
    numerator_hint: str | None,
    denominator_hint: str | None,
    evidence: str,
    source_field: str,
    text: str,
) -> _Candidate:
    confidence = _confidence(
        metric_surface=metric_surface,
        aggregation_or_window=aggregation_or_window,
        dimensions=dimensions,
        filters=filters,
        audience_hint=audience_hint,
        numerator_hint=numerator_hint,
        denominator_hint=denominator_hint,
        source_field=source_field,
        text=text,
    )
    return _Candidate(
        source_brief_id=source_brief_id,
        metric_surface=metric_surface,
        aggregation_or_window=aggregation_or_window,
        dimensions=dimensions,
        filters=filters,
        freshness_or_latency=freshness_or_latency,
        audience_hint=audience_hint,
        numerator_hint=numerator_hint,
        denominator_hint=denominator_hint,
        evidence=evidence,
        confidence=confidence,
    )


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceReportingMetricDefinition]:
    grouped: dict[tuple[str | None, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (candidate.source_brief_id, _dedupe_text_key(candidate.metric_surface)),
            [],
        ).append(candidate)

    definitions: list[SourceReportingMetricDefinition] = []
    for (source_brief_id, _surface_key), items in grouped.items():
        best = max(items, key=_candidate_score)
        numerator = best.numerator_hint
        denominator = best.denominator_hint
        window = best.aggregation_or_window
        dimensions = tuple(_dedupe(item for candidate in items for item in candidate.dimensions))
        filters = tuple(_dedupe(item for candidate in items for item in candidate.filters))
        audience = best.audience_hint
        freshness = best.freshness_or_latency
        rate_like = any(
            _RATE_RE.search(item.metric_surface) or _RATE_RE.search(item.evidence)
            for item in items
        )
        missing_numerator_denominator = rate_like and not (numerator and denominator)
        missing_time_window = window is None
        missing_dimension_filter = not (dimensions or filters)
        missing_owner = audience is None
        definitions.append(
            SourceReportingMetricDefinition(
                source_brief_id=source_brief_id,
                metric_surface=best.metric_surface,
                aggregation_or_window=window,
                dimensions=dimensions,
                filters=filters,
                freshness_or_latency=freshness,
                audience_hint=audience,
                numerator_hint=numerator,
                denominator_hint=denominator,
                missing_numerator_denominator=missing_numerator_denominator,
                missing_time_window=missing_time_window,
                missing_dimension_filter=missing_dimension_filter,
                missing_owner=missing_owner,
                confidence=_merged_confidence(
                    best.confidence,
                    missing_numerator_denominator,
                    missing_time_window,
                    missing_dimension_filter,
                    missing_owner,
                ),
                evidence=tuple(
                    sorted(
                        _dedupe(candidate.evidence for candidate in items),
                        key=lambda item: item.casefold(),
                    )
                )[:5],
            )
        )
    return definitions


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited and str(key) not in _IGNORED_FIELDS:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        if _has_structured_metric_shape(value):
            values.append((source_field, _structured_evidence(value)))
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            if _has_reporting_signal(key_text, child_field) and not isinstance(
                child, (Mapping, list, tuple, set)
            ):
                if text := _optional_text(child):
                    values.append((child_field, _clean_text(f"{key_text}: {text}")))
                continue
            _append_value(values, child_field, child)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        values.extend((source_field, segment) for segment in _segments(text))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for line in value.splitlines() or [value]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        parts = (
            [cleaned]
            if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line)
            else _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for sentence in parts:
            segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _has_reporting_signal(text: str, source_field: str) -> bool:
    if _generic_reporting_statement(text):
        return False
    searchable = _searchable_text(source_field, text)
    return bool(
        _REPORTING_CONTEXT_RE.search(searchable)
        and (
            _REQUIREMENT_RE.search(searchable)
            or _STRUCTURED_FIELD_RE.search(_field_words(source_field))
            or _SURFACE_RE.search(searchable)
        )
    )


def _metric_surface(text: str, source_field: str) -> str | None:
    if match := re.search(r"\b(?:metric|surface|name|dashboard|report|kpi)\s*[:=]\s*([^;]+)", text, re.I):
        candidate = _clean_surface(match.group(1))
        if candidate:
            return candidate
    if match := re.search(r"\b(?P<surface>[A-Za-z][A-Za-z0-9 /_-]{0,70}?(?:dashboard|report|drilldown|drill down|kpi|funnel))\s+(?:must|should|shall|needs?|need to|defines?|tracks?|shows?|includes?|displays?|is)\b", text, re.I):
        surface = _clean_surface(match.group("surface"))
        if surface.casefold() in {"dashboard", "report"} and re.search(r"\bconversion rate\b", text, re.I):
            return "conversion rate"
        return surface
    if match := re.search(r"\b(?P<surface>[A-Za-z][A-Za-z0-9 /_-]{0,70}?\s+conversion rate)\b", text, re.I):
        raw_surface = match.group("surface")
        surface = _clean_surface(raw_surface)
        if (
            surface
            and not surface.casefold().startswith(("include ", "show ", "track "))
            and not re.search(r"\b(?:must|should|shall|include|show|track|display)\b", raw_surface, re.I)
        ):
            return surface
    if re.search(r"\b(?:dashboard|report)\s+should\s+include\s+conversion rate\b", text, re.I):
        return "conversion rate"
    surface_matches = [match.group("surface") for match in _SURFACE_RE.finditer(text)]
    if surface_matches:
        surface = _clean_surface(surface_matches[0])
        if surface.casefold() in {"dashboard", "report"}:
            if match := _METRIC_WORD_RE.search(text):
                return _clean_surface(match.group(0))
        return surface
    if match := _METRIC_WORD_RE.search(text):
        return _clean_surface(match.group(0))
    return None


def _window(text: str) -> str | None:
    if match := re.search(r"\b(?:window|period|aggregation|grain|granularity|timeframe|cadence)\s*[:=]\s*([^;,\n]+)", text, re.I):
        return _detail(match.group(1))
    if match := _WINDOW_RE.search(text):
        return _detail(match.group(1) or match.group(0))
    return None


def _freshness(text: str) -> str | None:
    return _detail_match(_FRESHNESS_RE, text)


def _audience(text: str) -> str | None:
    for match in _AUDIENCE_RE.finditer(text):
        value = re.sub(
            r"\b(?:must|should|needs?|show|include|track|display|refreshes|within|by|filtered)\b.*$",
            "",
            match.group(1),
            flags=re.I,
        )
        value = _detail(value)
        if value and re.search(
            r"\b(?:team|leadership|finance|support|success|manager|managers|ops|operations|"
            r"product|growth|executive|executives|stakeholder|stakeholders|analyst|analysts)\b",
            value,
            re.I,
        ):
            return value
    return None


def _from_to(text: str) -> tuple[str | None, str | None]:
    if match := _FROM_TO_RE.search(text):
        return _detail(match.group(1)), _detail(match.group(2))
    return None, None


def _detail_match(pattern: re.Pattern[str], text: str) -> str | None:
    if not (match := pattern.search(text)):
        return None
    return _detail(match.group(1))


def _items(value: str | None) -> tuple[str, ...]:
    if not value:
        return ()
    value = re.sub(
        r"\b(?:filtered to|filter(?:ed)? by|where|for|owner|audience|refresh(?:es)?|within|over)\b.*$",
        "",
        value,
        flags=re.I,
    )
    parts = re.split(r"\s*(?:,|;|\band\b|/)\s*", value)
    return tuple(_dedupe(_detail(part) for part in parts if _detail(part)))


def _context_items(value: str | None) -> tuple[str, ...]:
    return _items(value)


def _metadata_contexts(
    brief_payloads: Iterable[tuple[str | None, Mapping[str, Any]]],
) -> dict[str | None, dict[str, str]]:
    contexts: dict[str | None, dict[str, str]] = {}
    for source_brief_id, payload in brief_payloads:
        context: dict[str, str] = {}
        for field, value in _candidate_segments({"metadata": payload.get("metadata", {})}):
            _merge_context(context, field, value)
        for field, value in _candidate_segments({"source_payload": payload.get("source_payload", {})}):
            if re.search(r"(?:metadata|owner|audience|freshness)", field, re.I):
                _merge_context(context, field, value)
        contexts[source_brief_id] = context
    return contexts


def _merge_context(context: dict[str, str], field: str, text: str) -> None:
    key_text = _field_words(field)
    if _AUDIENCE_KEY_RE.search(key_text) or re.search(r"\b(?:owner|audience)\b", text, re.I):
        value = _audience(text)
        if not value and _AUDIENCE_KEY_RE.search(key_text):
            value = _detail(text.rsplit(":", 1)[-1])
        if value:
            context.setdefault("audience_hint", value)
    if _FRESHNESS_KEY_RE.search(key_text) or _FRESHNESS_KEY_RE.search(text):
        if value := _freshness(text) or _detail(text.partition(":")[2] or text):
            context.setdefault("freshness_or_latency", value)
    if _WINDOW_KEY_RE.search(key_text):
        context.setdefault("aggregation_or_window", _detail(text.partition(":")[2] or text) or "")
    if _DIMENSION_KEY_RE.search(key_text):
        context.setdefault("dimensions", _detail(text.partition(":")[2] or text) or "")
    if _FILTER_KEY_RE.search(key_text):
        context.setdefault("filters", _detail(text.partition(":")[2] or text) or "")


def _context_only_field(source_field: str) -> bool:
    return bool(
        re.search(
            r"(?:metadata|freshness|latency|owner|audience|analytics_owner)",
            source_field,
            re.I,
        )
    )


def _explicit_metric_definition(text: str) -> bool:
    return bool(_METRIC_KEY_RE.search(text) and _REPORTING_CONTEXT_RE.search(text))


def _generic_reporting_statement(text: str) -> bool:
    return bool(
        re.fullmatch(
            r"(?:general\s+)?(?:reporting|analytics?)\s+(?:metric\s+)?requirements?\.?|"
            r"improve\s+metric\s+visibility\.?",
            _clean_text(text),
            re.I,
        )
    )


def _has_structured_metric_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    return bool(
        keys
        & {
            "metric",
            "metric_name",
            "metric_surface",
            "report_metric",
            "dashboard_metric",
            "kpi",
            "kpi_name",
            "definition",
        }
    )


def _structured_evidence(item: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = ", ".join(_strings(value)) if isinstance(value, (list, tuple, set)) else _clean_text(str(value))
        if text:
            parts.append(f"{key}: {text}")
    return "; ".join(parts) or _clean_text(str(item))


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
    text = _clean_text(str(value))
    return [text] if text else []


def _confidence(
    *,
    metric_surface: str,
    aggregation_or_window: str | None,
    dimensions: tuple[str, ...],
    filters: tuple[str, ...],
    audience_hint: str | None,
    numerator_hint: str | None,
    denominator_hint: str | None,
    source_field: str,
    text: str,
) -> ReportingMetricConfidence:
    details = sum(
        bool(value)
        for value in (
            aggregation_or_window,
            dimensions or filters,
            audience_hint,
            numerator_hint and denominator_hint,
        )
    )
    field_text = _field_words(source_field)
    if details >= 3 and (
        _REQUIREMENT_RE.search(text) or _STRUCTURED_FIELD_RE.search(field_text)
    ):
        return "high"
    if details >= 2 or _REQUIREMENT_RE.search(text) or _STRUCTURED_FIELD_RE.search(field_text):
        return "medium"
    if metric_surface:
        return "low"
    return "low"


def _merged_confidence(
    base: ReportingMetricConfidence,
    missing_numerator_denominator: bool,
    missing_time_window: bool,
    missing_dimension_filter: bool,
    missing_owner: bool,
) -> ReportingMetricConfidence:
    missing_count = sum(
        (
            missing_numerator_denominator,
            missing_time_window,
            missing_dimension_filter,
            missing_owner,
        )
    )
    if missing_count == 0:
        return base
    if missing_count >= 3:
        return "low"
    if _CONFIDENCE_ORDER[base] <= _CONFIDENCE_ORDER["medium"]:
        return "medium"
    return base


def _summary(
    definitions: tuple[SourceReportingMetricDefinition, ...], source_count: int
) -> dict[str, Any]:
    surfaces = [definition.metric_surface for definition in definitions]
    return {
        "source_count": source_count,
        "metric_definition_count": len(definitions),
        "surface_counts": {
            surface: sum(1 for definition in definitions if definition.metric_surface == surface)
            for surface in sorted(set(surfaces), key=lambda item: item.casefold())
        },
        "confidence_counts": {
            confidence: sum(
                1 for definition in definitions if definition.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
        "missing_detail_counts": {
            field: sum(1 for definition in definitions if getattr(definition, field))
            for field in _MISSING_DETAIL_FIELDS
        },
        "metric_surfaces": surfaces,
    }


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
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "data_requirements",
        "risks",
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


def _candidate_score(candidate: _Candidate) -> tuple[int, int, int, str]:
    details = sum(
        bool(value)
        for value in (
            candidate.aggregation_or_window,
            candidate.dimensions or candidate.filters,
            candidate.freshness_or_latency,
            candidate.audience_hint,
            candidate.numerator_hint and candidate.denominator_hint,
        )
    )
    return (
        details,
        2 - _CONFIDENCE_ORDER[candidate.confidence],
        len(candidate.evidence),
        candidate.evidence,
    )


def _flags(definition: SourceReportingMetricDefinition) -> tuple[str, ...]:
    flags = []
    if definition.missing_numerator_denominator:
        flags.append("missing numerator denominator")
    if definition.missing_time_window:
        flags.append("missing time window")
    if definition.missing_dimension_filter:
        flags.append("missing dimension filter")
    if definition.missing_owner:
        flags.append("missing owner")
    return tuple(flags)


def _clean_surface(value: str) -> str:
    text = _clean_text(value)
    text = re.sub(r"^(?:source payload|metadata|acceptance criteria|reporting)\s+", "", text, flags=re.I)
    text = re.sub(r"\s+(?:must|should|shall|needs?|need to|required|requires?|show|include|track|display|defines?).*$", "", text, flags=re.I)
    text = re.sub(r"\b(?:the|a|an|our|their)\b", "", text, flags=re.I)
    text = _SPACE_RE.sub(" ", text).strip(" :;-")
    return text[:80].rstrip() or "metric"


def _detail(value: Any) -> str | None:
    text = _clean_text(str(value)) if value is not None else ""
    text = text.strip("`'\" ;,.")
    if not text:
        return None
    return text[:120].rstrip()


def _clean_text(value: str) -> str:
    text = _CHECKBOX_RE.sub("", _BULLET_RE.sub("", value.strip()))
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _field_words(source_field: str) -> str:
    return source_field.replace("_", " ").replace("-", " ").replace(".", " ").replace("/", " ")


def _searchable_text(source_field: str, text: str) -> str:
    value = f"{_field_words(source_field)} {text}"
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", value)
    return value.replace("/", " ").replace("_", " ").replace("-", " ")


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[Any]) -> list[Any]:
    seen: set[str] = set()
    result: list[Any] = []
    for value in values:
        key = _dedupe_text_key(value)
        if not key or key in seen:
            continue
        seen.add(key)
        result.append(value)
    return result


def _dedupe_text_key(value: Any) -> str:
    return _clean_text(str(value)).casefold() if value is not None else ""


__all__ = [
    "ReportingMetricConfidence",
    "SourceReportingMetricDefinition",
    "SourceReportingMetricDefinitionsReport",
    "build_source_reporting_metric_definitions",
    "derive_source_reporting_metric_definitions",
    "extract_source_reporting_metric_definitions",
    "generate_source_reporting_metric_definitions",
    "source_reporting_metric_definitions_to_dict",
    "source_reporting_metric_definitions_to_dicts",
    "source_reporting_metric_definitions_to_markdown",
    "summarize_source_reporting_metric_definitions",
]
