"""Extract source-level usage metering requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


UsageMeteringDimension = Literal[
    "metered_event",
    "counter",
    "billing_period",
    "aggregation_window",
    "quota_unit",
    "overage_behavior",
    "auditability",
    "customer_visible_reporting",
]
UsageMeteringConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_DIMENSION_ORDER: tuple[UsageMeteringDimension, ...] = (
    "metered_event",
    "counter",
    "billing_period",
    "aggregation_window",
    "quota_unit",
    "overage_behavior",
    "auditability",
    "customer_visible_reporting",
)
_CONFIDENCE_ORDER: dict[UsageMeteringConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_PLANNING_NOTES: dict[UsageMeteringDimension, str] = {
    "metered_event": "Define metered event names, emission points, idempotency keys, and delivery guarantees.",
    "counter": "Plan counter storage, increment semantics, correction paths, and reconciliation behavior.",
    "billing_period": "Specify billing period boundaries, reset timing, timezone behavior, and invoice alignment.",
    "aggregation_window": "Define aggregation windows, rollups, late-arriving events, and recomputation rules.",
    "quota_unit": "Capture quota units, included allowances, unit conversions, and limit enforcement.",
    "overage_behavior": "Plan overage charging, throttling, notifications, grace periods, and invoice presentation.",
    "auditability": "Persist audit evidence for usage events, adjustments, exports, and billing disputes.",
    "customer_visible_reporting": "Provide customer-visible usage reporting, breakdowns, freshness, and export access.",
}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_METERING_CONTEXT_RE = re.compile(
    r"\b(?:usage[- ]?based|usage metering|meter(?:ed|ing)?|billable usage|usage events?|"
    r"consumption|pay as you go|pay[- ]?per[- ]?use|quota|allowance|overage|over limit|"
    r"billing period|aggregation window|counter|usage dashboard|usage reporting|"
    r"usage export|usage audit|usage ledger|usage units?|usage adjustments?|audit trail)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:usage|meter|metered|metering|billable|consumption|event|counter|billing[_ -]?period|"
    r"aggregation|window|quota|allowance|unit|overage|audit|ledger|reporting|dashboard|export)",
    re.I,
)
_REQUIREMENT_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|support|"
    r"allow|provide|define|document|record|track|persist|emit|count|aggregate|roll ?up|"
    r"reset(?:s)?|enforce|charge|bill|throttle|cap|notify|export|display|show|reconcile|audit|records?|"
    r"done when|acceptance|cannot ship)\b",
    re.I,
)
_NEGATED_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,140}\b(?:usage[- ]?based|usage metering|meter(?:ed|ing)?|"
    r"billable usage|quota|overage|usage reporting|usage dashboard|usage events?)\b"
    r".{0,140}\b(?:required|needed|in scope|planned|changes?|impact|work|requirements?)\b|"
    r"\b(?:usage[- ]?based|usage metering|meter(?:ed|ing)?|billable usage|quota|overage|"
    r"usage reporting|usage dashboard|usage events?)\b.{0,140}\b(?:not required|not needed|"
    r"out of scope|no changes?|no work|non[- ]?goal)\b",
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
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "billing",
    "pricing",
    "usage",
    "usage_metering",
    "metering",
    "quotas",
    "limits",
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
    "domain",
    "status",
}
_DIMENSION_PATTERNS: dict[UsageMeteringDimension, re.Pattern[str]] = {
    "metered_event": re.compile(
        r"\b(?:metered events?|usage events?|billable events?|event names?|emit(?:ted)? usage|"
        r"capture usage|track usage|idempotency keys?)\b",
        re.I,
    ),
    "counter": re.compile(
        r"\b(?:counter|counters|count(?:s|ed|ing)?|increment|decrement|usage balance|"
        r"running total|dedupe|reconciliation|correction)\b",
        re.I,
    ),
    "billing_period": re.compile(
        r"\b(?:billing periods?|monthly|annual|annually|calendar month|invoice period|"
        r"period boundaries|reset(?:s)? (?:monthly|annually|per billing period)|renewal)\b",
        re.I,
    ),
    "aggregation_window": re.compile(
        r"\b(?:aggregation windows?|aggregate|aggregated|roll ?ups?|hourly window|daily window|"
        r"real[- ]?time window|late[- ]arriving events?|recompute|bucket)\b",
        re.I,
    ),
    "quota_unit": re.compile(
        r"\b(?:quota units?|units?|included allowance|allowance|limits?|entitlement|"
        r"api calls?|seats?|messages?|gb|gigabytes?|tokens?|credits?)\b",
        re.I,
    ),
    "overage_behavior": re.compile(
        r"\b(?:overage|overages|over limit|exceed(?:s|ed)? quota|extra usage|"
        r"charge overages?|throttle|hard cap|soft cap|grace period|limit reached)\b",
        re.I,
    ),
    "auditability": re.compile(
        r"\b(?:audit(?:able|ability)?|audit trail|usage ledger|evidence|immutable log|"
        r"adjustments?|disputes?|reconcile|reconciliation)\b",
        re.I,
    ),
    "customer_visible_reporting": re.compile(
        r"\b(?:customer[- ]visible usage|usage dashboard|usage reporting|usage report|"
        r"usage page|usage breakdown|customer portal|show usage|display usage|export usage|csv export)\b",
        re.I,
    ),
}
_FIELD_DIMENSION_PATTERNS: dict[UsageMeteringDimension, re.Pattern[str]] = {
    dimension: re.compile(dimension.replace("_", r"[_ -]?"), re.I)
    for dimension in _DIMENSION_ORDER
}
_EVENT_DETAIL_RE = re.compile(r"\b(?:event names?|metered events?|usage events?)\s*:?\s*([^.;\n]+)", re.I)
_COUNTER_DETAIL_RE = re.compile(r"\b(?:counters?|counting|increment)\b\s*:?\s*([^.;\n]+)", re.I)
_BILLING_PERIOD_DETAIL_RE = re.compile(r"\b(?:billing periods?|period|resets?)\s*:?\s*([^.;\n]*(?:monthly|annual|calendar month|invoice|renewal|period)[^.;\n]*)", re.I)
_AGGREGATION_DETAIL_RE = re.compile(r"\b(?:aggregation windows?|aggregate|roll ?ups?)\s*:?\s*([^.;\n]+)", re.I)
_QUOTA_DETAIL_RE = re.compile(r"\b(?:quota units?|included allowance|allowance|limits?|unit)\s*:?\s*([^.;\n]+)", re.I)
_OVERAGE_DETAIL_RE = re.compile(r"\b(?:overages?|exceed(?:s|ed)? quota|over limit|throttle|cap)\b\s*:?\s*([^.;\n]+)", re.I)
_AUDIT_DETAIL_RE = re.compile(r"\b(?:audit trail|auditability|usage ledger|audit|evidence)\s*:?\s*([^.;\n]+)", re.I)
_REPORTING_DETAIL_RE = re.compile(r"\b(?:usage dashboard|usage reporting|usage reports?|usage page|export usage|csv export)\b\s*:?\s*([^.;\n]+)", re.I)


@dataclass(frozen=True, slots=True)
class SourceUsageMeteringRequirement:
    """One source-backed usage metering requirement."""

    source_brief_id: str | None
    dimension: UsageMeteringDimension
    requirement_text: str
    metered_event: str | None = None
    counter: str | None = None
    billing_period: str | None = None
    aggregation_window: str | None = None
    quota_unit: str | None = None
    overage_behavior: str | None = None
    auditability: str | None = None
    customer_visible_reporting: str | None = None
    source_field: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    matched_terms: tuple[str, ...] = field(default_factory=tuple)
    confidence: UsageMeteringConfidence = "medium"
    planning_note: str = ""

    @property
    def requirement_category(self) -> UsageMeteringDimension:
        """Compatibility alias matching category-oriented reports."""
        return self.dimension

    @property
    def planning_notes(self) -> tuple[str, ...]:
        """Compatibility alias for callers expecting plural planning notes."""
        return (self.planning_note,)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "dimension": self.dimension,
            "requirement_text": self.requirement_text,
            "metered_event": self.metered_event,
            "counter": self.counter,
            "billing_period": self.billing_period,
            "aggregation_window": self.aggregation_window,
            "quota_unit": self.quota_unit,
            "overage_behavior": self.overage_behavior,
            "auditability": self.auditability,
            "customer_visible_reporting": self.customer_visible_reporting,
            "source_field": self.source_field,
            "evidence": list(self.evidence),
            "matched_terms": list(self.matched_terms),
            "confidence": self.confidence,
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceUsageMeteringRequirementsReport:
    """Source-level usage metering requirements report."""

    source_id: str | None = None
    requirements: tuple[SourceUsageMeteringRequirement, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceUsageMeteringRequirement, ...]:
        """Compatibility view matching reports that expose extracted items as records."""
        return self.requirements

    @property
    def findings(self) -> tuple[SourceUsageMeteringRequirement, ...]:
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
        """Return usage metering requirement records as plain dictionaries."""
        return [requirement.to_dict() for requirement in self.requirements]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Usage Metering Requirements Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        dimension_counts = self.summary.get("dimension_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Requirements found: {self.summary.get('requirement_count', 0)}",
            "- Dimension counts: "
            + ", ".join(f"{dimension} {dimension_counts.get(dimension, 0)}" for dimension in _DIMENSION_ORDER),
            "- Confidence counts: "
            + ", ".join(f"{level} {confidence_counts.get(level, 0)}" for level in _CONFIDENCE_ORDER),
        ]
        if not self.requirements:
            lines.extend(["", "No usage metering requirements were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Requirements",
                "",
                "| Source Brief | Dimension | Requirement | Event | Counter | Billing Period | Aggregation Window | Quota Unit | Overage | Auditability | Customer Reporting | Source Field | Matched Terms | Confidence | Planning Note | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for requirement in self.requirements:
            lines.append(
                "| "
                f"{_markdown_cell(requirement.source_brief_id or '')} | "
                f"{_markdown_cell(requirement.dimension)} | "
                f"{_markdown_cell(requirement.requirement_text)} | "
                f"{_markdown_cell(requirement.metered_event or '')} | "
                f"{_markdown_cell(requirement.counter or '')} | "
                f"{_markdown_cell(requirement.billing_period or '')} | "
                f"{_markdown_cell(requirement.aggregation_window or '')} | "
                f"{_markdown_cell(requirement.quota_unit or '')} | "
                f"{_markdown_cell(requirement.overage_behavior or '')} | "
                f"{_markdown_cell(requirement.auditability or '')} | "
                f"{_markdown_cell(requirement.customer_visible_reporting or '')} | "
                f"{_markdown_cell(requirement.source_field or '')} | "
                f"{_markdown_cell(', '.join(requirement.matched_terms))} | "
                f"{_markdown_cell(requirement.confidence)} | "
                f"{_markdown_cell(requirement.planning_note)} | "
                f"{_markdown_cell('; '.join(requirement.evidence))} |"
            )
        return "\n".join(lines)


def build_source_usage_metering_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceUsageMeteringRequirementsReport:
    """Extract source-level usage metering requirement records from brief-shaped input."""
    brief_payloads = _source_payloads(source)
    requirements = tuple(_merge_candidates(_candidates_for_briefs(brief_payloads)))
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceUsageMeteringRequirementsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        requirements=requirements,
        summary=_summary(requirements, len(brief_payloads)),
    )


def extract_source_usage_metering_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceUsageMeteringRequirementsReport:
    """Compatibility alias for building a usage metering requirements report."""
    return build_source_usage_metering_requirements(source)


def generate_source_usage_metering_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceUsageMeteringRequirementsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_usage_metering_requirements(source)


def derive_source_usage_metering_requirements(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceUsageMeteringRequirementsReport:
    """Compatibility helper for callers that use derive_* naming."""
    return build_source_usage_metering_requirements(source)


def summarize_source_usage_metering_requirements(
    source_or_result: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceUsageMeteringRequirementsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for extracted usage metering requirements."""
    if isinstance(source_or_result, SourceUsageMeteringRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_usage_metering_requirements(source_or_result).summary


def source_usage_metering_requirements_to_dict(report: SourceUsageMeteringRequirementsReport) -> dict[str, Any]:
    """Serialize a usage metering requirements report to a plain dictionary."""
    return report.to_dict()


source_usage_metering_requirements_to_dict.__test__ = False


def source_usage_metering_requirements_to_dicts(
    requirements: (
        tuple[SourceUsageMeteringRequirement, ...]
        | list[SourceUsageMeteringRequirement]
        | SourceUsageMeteringRequirementsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize usage metering requirement records to dictionaries."""
    if isinstance(requirements, SourceUsageMeteringRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_usage_metering_requirements_to_dicts.__test__ = False


def source_usage_metering_requirements_to_markdown(report: SourceUsageMeteringRequirementsReport) -> str:
    """Render a usage metering requirements report as Markdown."""
    return report.to_markdown()


source_usage_metering_requirements_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    dimension: UsageMeteringDimension
    requirement_text: str
    metered_event: str | None
    counter: str | None
    billing_period: str | None
    aggregation_window: str | None
    quota_unit: str | None
    overage_behavior: str | None
    auditability: str | None
    customer_visible_reporting: str | None
    source_field: str
    evidence: str
    matched_terms: tuple[str, ...]
    confidence: UsageMeteringConfidence


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
            if _NEGATED_RE.search(searchable):
                continue
            for dimension in _dimensions(segment):
                candidates.append(
                    _Candidate(
                        source_brief_id=source_brief_id,
                        dimension=dimension,
                        requirement_text=_requirement_text(segment.text),
                        metered_event=_field_value_detail("metered_event", segment.text)
                        or _field_value_detail("event", segment.text)
                        or _match_detail(_EVENT_DETAIL_RE, segment.text),
                        counter=_field_value_detail("counter", segment.text)
                        or _match_detail(_COUNTER_DETAIL_RE, segment.text),
                        billing_period=_field_value_detail("billing_period", segment.text)
                        or _match_detail(_BILLING_PERIOD_DETAIL_RE, segment.text),
                        aggregation_window=_field_value_detail("aggregation_window", segment.text)
                        or _field_value_detail("window", segment.text)
                        or _match_detail(_AGGREGATION_DETAIL_RE, segment.text),
                        quota_unit=_field_value_detail("quota_unit", segment.text)
                        or _field_value_detail("unit", segment.text)
                        or _match_detail(_QUOTA_DETAIL_RE, segment.text),
                        overage_behavior=_field_value_detail("overage_behavior", segment.text)
                        or _field_value_detail("overage", segment.text)
                        or _match_detail(_OVERAGE_DETAIL_RE, segment.text),
                        auditability=_field_value_detail("auditability", segment.text)
                        or _field_value_detail("audit", segment.text)
                        or _match_detail(_AUDIT_DETAIL_RE, segment.text),
                        customer_visible_reporting=_field_value_detail("customer_visible_reporting", segment.text)
                        or _field_value_detail("reporting", segment.text)
                        or _match_detail(_REPORTING_DETAIL_RE, segment.text),
                        source_field=segment.source_field,
                        evidence=_evidence_snippet(segment.source_field, segment.text),
                        matched_terms=_matched_terms(dimension, searchable),
                        confidence=_confidence(dimension, segment),
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceUsageMeteringRequirement]:
    grouped: dict[tuple[str | None, UsageMeteringDimension, str], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(
            (candidate.source_brief_id, candidate.dimension, _dedupe_requirement_key(candidate.requirement_text)),
            [],
        ).append(candidate)

    requirements: list[SourceUsageMeteringRequirement] = []
    for (_source_brief_id, dimension, _requirement_key), items in grouped.items():
        best = max(items, key=_candidate_score)
        requirements.append(
            SourceUsageMeteringRequirement(
                source_brief_id=best.source_brief_id,
                dimension=dimension,
                requirement_text=best.requirement_text,
                metered_event=_first_detail(item.metered_event for item in items),
                counter=_first_detail(item.counter for item in items),
                billing_period=_first_detail(item.billing_period for item in items),
                aggregation_window=_first_detail(item.aggregation_window for item in items),
                quota_unit=_first_detail(item.quota_unit for item in items),
                overage_behavior=_first_detail(item.overage_behavior for item in items),
                auditability=_first_detail(item.auditability for item in items),
                customer_visible_reporting=_first_detail(item.customer_visible_reporting for item in items),
                source_field=best.source_field,
                evidence=tuple(_dedupe_evidence([best.evidence, *(item.evidence for item in items)]))[:5],
                matched_terms=tuple(sorted(_dedupe(term for item in items for term in item.matched_terms), key=str.casefold)),
                confidence=min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]),
                planning_note=_PLANNING_NOTES[dimension],
            )
        )
    return sorted(
        requirements,
        key=lambda requirement: (
            _optional_text(requirement.source_brief_id) or "",
            _DIMENSION_ORDER.index(requirement.dimension),
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
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text) or _METERING_CONTEXT_RE.search(key_text))
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
            section_context = inherited_context or bool(_METERING_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
            if title:
                segments.append((title, section_context))
            continue
        cleaned = _clean_text(line)
        if not cleaned or _NEGATED_RE.search(cleaned):
            continue
        parts = [cleaned] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else _SENTENCE_SPLIT_RE.split(cleaned)
        for part in parts:
            clauses = [part] if _METERING_CONTEXT_RE.search(part) else _CLAUSE_SPLIT_RE.split(part)
            for clause in clauses:
                text = _clean_text(clause)
                if text and not _NEGATED_RE.search(text):
                    segments.append((text, section_context))
    return segments


def _dimensions(segment: _Segment) -> tuple[UsageMeteringDimension, ...]:
    searchable = _searchable_text(segment.source_field, segment.text)
    field_words = _field_words(segment.source_field)
    has_metering_context = bool(_METERING_CONTEXT_RE.search(searchable))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    if not (has_metering_context or has_structured_context):
        return ()
    if not (_REQUIREMENT_RE.search(searchable) or has_structured_context):
        return ()
    field_dimensions = [dimension for dimension in _DIMENSION_ORDER if _FIELD_DIMENSION_PATTERNS[dimension].search(field_words)]
    text_dimensions = [dimension for dimension in _DIMENSION_ORDER if _DIMENSION_PATTERNS[dimension].search(segment.text)]
    if (
        _METERING_CONTEXT_RE.search(segment.text)
        and _REQUIREMENT_RE.search(segment.text)
        and not text_dimensions
        and not field_dimensions
    ):
        text_dimensions.append("metered_event")
    return tuple(_dedupe(field_dimensions + text_dimensions))


def _confidence(dimension: UsageMeteringDimension, segment: _Segment) -> UsageMeteringConfidence:
    field_words = _field_words(segment.source_field)
    has_explicit_requirement = bool(_REQUIREMENT_RE.search(segment.text))
    has_structured_context = bool(segment.section_context or _STRUCTURED_FIELD_RE.search(field_words))
    has_dimension = bool(_DIMENSION_PATTERNS[dimension].search(segment.text) or _FIELD_DIMENSION_PATTERNS[dimension].search(field_words))
    detail_count = sum(
        1
        for value in (
            _match_detail(_EVENT_DETAIL_RE, segment.text),
            _match_detail(_BILLING_PERIOD_DETAIL_RE, segment.text),
            _match_detail(_AGGREGATION_DETAIL_RE, segment.text),
            _match_detail(_QUOTA_DETAIL_RE, segment.text),
            _match_detail(_OVERAGE_DETAIL_RE, segment.text),
            _match_detail(_AUDIT_DETAIL_RE, segment.text),
            _match_detail(_REPORTING_DETAIL_RE, segment.text),
        )
        if value
    )
    if has_dimension and has_explicit_requirement and has_structured_context and detail_count >= 1:
        return "high"
    if has_dimension and (has_explicit_requirement or has_structured_context):
        return "medium"
    return "low"


def _summary(requirements: tuple[SourceUsageMeteringRequirement, ...], source_count: int) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "requirement_count": len(requirements),
        "dimension_counts": {
            dimension: sum(1 for requirement in requirements if requirement.dimension == dimension)
            for dimension in _DIMENSION_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for requirement in requirements if requirement.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "dimensions": [
            dimension
            for dimension in _DIMENSION_ORDER
            if any(requirement.dimension == dimension for requirement in requirements)
        ],
        "requires_metered_events": any(requirement.metered_event for requirement in requirements),
        "requires_counters": any(requirement.counter for requirement in requirements),
        "requires_billing_periods": any(requirement.billing_period for requirement in requirements),
        "requires_aggregation_windows": any(requirement.aggregation_window for requirement in requirements),
        "requires_quota_units": any(requirement.quota_unit for requirement in requirements),
        "requires_overage_behavior": any(requirement.overage_behavior for requirement in requirements),
        "requires_auditability": any(requirement.auditability for requirement in requirements),
        "requires_customer_visible_reporting": any(requirement.customer_visible_reporting for requirement in requirements),
        "status": "ready_for_usage_metering_planning" if requirements else "no_usage_metering_language",
    }


def _has_structured_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    if any(isinstance(value, (Mapping, list, tuple, set)) for value in item.values()):
        return False
    return bool(
        keys
        & {
            "dimension",
            "requirement_category",
            "metered_event",
            "event",
            "event_name",
            "counter",
            "billing_period",
            "aggregation_window",
            "quota_unit",
            "unit",
            "allowance",
            "overage",
            "overage_behavior",
            "audit",
            "auditability",
            "reporting",
            "customer_visible_reporting",
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
        "acceptance",
        "acceptance_criteria",
        "success_criteria",
        "definition_of_done",
        "validation_plan",
        "architecture_notes",
        "data_requirements",
        "integration_points",
        "risks",
        "billing",
        "pricing",
        "usage",
        "usage_metering",
        "metering",
        "quotas",
        "limits",
        "metadata",
        "brief_metadata",
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


def _matched_terms(dimension: UsageMeteringDimension, text: str) -> tuple[str, ...]:
    return tuple(sorted(_dedupe(_clean_text(match.group(0)).casefold() for match in _DIMENSION_PATTERNS[dimension].finditer(text)), key=str.casefold))


def _match_detail(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    if not match:
        return None
    return _clean_text(match.group(1)).rstrip(".").casefold()


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
    "UsageMeteringConfidence",
    "UsageMeteringDimension",
    "SourceUsageMeteringRequirement",
    "SourceUsageMeteringRequirementsReport",
    "build_source_usage_metering_requirements",
    "derive_source_usage_metering_requirements",
    "extract_source_usage_metering_requirements",
    "generate_source_usage_metering_requirements",
    "source_usage_metering_requirements_to_dict",
    "source_usage_metering_requirements_to_dicts",
    "source_usage_metering_requirements_to_markdown",
    "summarize_source_usage_metering_requirements",
]
