"""Extract source-level data quality expectations from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief, SourceBrief


SourceDataQualityDimension = Literal[
    "completeness",
    "accuracy",
    "deduplication",
    "validation",
    "freshness_threshold",
    "consistency",
    "null_handling",
    "schema_conformance",
    "reconciliation",
]
SourceDataQualityConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_DIMENSION_ORDER: tuple[SourceDataQualityDimension, ...] = (
    "completeness",
    "accuracy",
    "deduplication",
    "validation",
    "freshness_threshold",
    "consistency",
    "null_handling",
    "schema_conformance",
    "reconciliation",
)
_CONFIDENCE_ORDER: dict[SourceDataQualityConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(?P<title>.+?)\s*#*\s*$")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_QUALITY_CONTEXT_RE = re.compile(
    r"\b(?:data quality|quality|dq|validation|validate|completeness|complete|accuracy|accurate|"
    r"dedup(?:e|lication)?|de[- ]?duplicat(?:e|ion)|duplicates?|consistency|consistent|"
    r"null|blank|missing values?|schema|schema conformance|contract|reconciliation|reconcile|"
    r"source of truth|audit totals?|quality gate|data checks?)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:data[-_ ]?quality|quality|dq|validation|validat|reconcil|schema|contract|"
    r"complete|accuracy|accurate|dedup|duplicate|null|blank|consisten|fresh|stale|"
    r"data[-_ ]?requirements|requirements?|constraints?|acceptance|definition[-_ ]?of[-_ ]?done|metadata)",
    re.I,
)
_EXPECTATION_RE = re.compile(
    r"\b(?:must|shall|required|requires?|requirement|needs?|need to|should|ensure|verify|"
    r"validate|check|reject|block|flag|alert|acceptance|done when|before launch|cannot ship|"
    r"pass|prove|enforce|monitor|reconcile)\b",
    re.I,
)
_NEGATED_SCOPE_RE = re.compile(
    r"\b(?:no|not|without)\s+(?:data\s+)?(?:quality|validation|reconciliation|schema|"
    r"freshness|completeness|accuracy|deduplication|null).*?"
    r"\b(?:in scope|required|needed|changes?|impact|expectations?)\b",
    re.I,
)
_THRESHOLD_RE = re.compile(
    r"\b(?:no older than|not older than|at most|maximum age|max age|max(?:imum)? staleness|"
    r"freshness (?:sla|target|threshold)(?: of)?|stale after|within)\s+"
    r"(?:is\s+|of\s+)?"
    r"(\d+(?:\.\d+)?\s*(?:seconds?|secs?|minutes?|mins?|hours?|hrs?|days?|weeks?))\b",
    re.I,
)
_DIMENSION_PATTERNS: dict[SourceDataQualityDimension, re.Pattern[str]] = {
    "completeness": re.compile(
        r"\b(?:completeness|complete records?|all required fields?|all fields?|every record|"
        r"missing records?|missing fields?|required fields? present|coverage of required)\b",
        re.I,
    ),
    "accuracy": re.compile(
        r"\b(?:accuracy|accurate|correct(?:ness)?|source of truth|truth data|verified against|"
        r"match(?:es)? source|wrong values?|calculation accuracy)\b",
        re.I,
    ),
    "deduplication": re.compile(
        r"\b(?:dedup(?:e|ed|ing|lication)?|de[- ]?duplicat(?:e|ed|ion|ing)|duplicates?|"
        r"unique records?|uniqueness|duplicate suppression|idempotent import)\b",
        re.I,
    ),
    "validation": re.compile(
        r"\b(?:validation|validate|validated|validating|invalid|reject invalid|quality checks?|"
        r"data checks?|field checks?|sanity checks?|quality gate)\b",
        re.I,
    ),
    "freshness_threshold": re.compile(
        r"\b(?:freshness (?:sla|target|threshold|requirement)|staleness threshold|stale after|"
        r"no older than|not older than|maximum age|max age|within \d+(?:\.\d+)?\s*"
        r"(?:seconds?|secs?|minutes?|mins?|hours?|hrs?|days?|weeks?))\b",
        re.I,
    ),
    "consistency": re.compile(
        r"\b(?:consistency|consistent|same value|same totals?|align(?:ed|ment)? across|"
        r"cross[- ]system consistency|referential integrity|consistent across)\b",
        re.I,
    ),
    "null_handling": re.compile(
        r"\b(?:null handling|nulls?|blank values?|empty values?|missing values?|unknown values?|"
        r"default values?|coalesce|nullable|non[- ]null|not null)\b",
        re.I,
    ),
    "schema_conformance": re.compile(
        r"\b(?:schema conformance|schema compliance|schema validation|schema|json schema|"
        r"contract conformance|contract validation|field types?|type checks?|required properties|"
        r"payload shape)\b",
        re.I,
    ),
    "reconciliation": re.compile(
        r"\b(?:reconciliation|reconcile|reconciled|tie[- ]out|tie out|match totals?|"
        r"ledger match|balance checks?|control totals?|audit totals?|source totals?)\b",
        re.I,
    ),
}
_SUBJECT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("customer records", re.compile(r"\bcustomer records?\b|\bcustomer data\b", re.I)),
    ("orders", re.compile(r"\borders?\b", re.I)),
    ("invoices", re.compile(r"\binvoices?\b", re.I)),
    ("ledger entries", re.compile(r"\bledger entries?\b|\bledger\b", re.I)),
    ("payments", re.compile(r"\bpayments?\b", re.I)),
    ("inventory", re.compile(r"\binventory\b", re.I)),
    ("events", re.compile(r"\bevents?\b", re.I)),
    ("imports", re.compile(r"\bimports?\b|\bimported rows?\b", re.I)),
    ("exports", re.compile(r"\bexports?\b", re.I)),
    ("source data", re.compile(r"\bsource data\b", re.I)),
    ("payloads", re.compile(r"\bpayloads?\b", re.I)),
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
    "success_criteria",
    "acceptance",
    "acceptance_criteria",
    "definition_of_done",
    "validation_plan",
    "architecture_notes",
    "data_requirements",
    "risks",
    "scope",
    "assumptions",
    "integration_points",
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


@dataclass(frozen=True, slots=True)
class SourceDataQualityExpectation:
    """One source-backed data quality expectation."""

    source_id: str | None
    dimension: SourceDataQualityDimension
    data_subject: str = "source data"
    threshold: str | None = None
    confidence: SourceDataQualityConfidence = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)
    planning_note: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "dimension": self.dimension,
            "data_subject": self.data_subject,
            "threshold": self.threshold,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "planning_note": self.planning_note,
        }


@dataclass(frozen=True, slots=True)
class SourceDataQualityExpectationsReport:
    """Source-level data quality expectations report."""

    source_id: str | None = None
    expectations: tuple[SourceDataQualityExpectation, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceDataQualityExpectation, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.expectations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "expectations": [expectation.to_dict() for expectation in self.expectations],
            "summary": dict(self.summary),
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return data quality expectation records as plain dictionaries."""
        return [expectation.to_dict() for expectation in self.expectations]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Data Quality Expectations Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        dimension_counts = self.summary.get("dimension_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        source_counts = self.summary.get("source_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Expectations found: {self.summary.get('expectation_count', 0)}",
            "- Dimension counts: "
            + ", ".join(f"{dimension} {dimension_counts.get(dimension, 0)}" for dimension in _DIMENSION_ORDER),
            "- Source counts: "
            + ", ".join(f"{source} {source_counts.get(source, 0)}" for source in sorted(source_counts, key=str.casefold)),
            "- Confidence counts: "
            + ", ".join(f"{confidence} {confidence_counts.get(confidence, 0)}" for confidence in _CONFIDENCE_ORDER),
        ]
        if not self.expectations:
            lines.extend(["", "No source data quality expectations were found in the brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Expectations",
                "",
                "| Source | Dimension | Data Subject | Threshold | Confidence | Evidence | Planning Note |",
                "| --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for expectation in self.expectations:
            lines.append(
                "| "
                f"{_markdown_cell(expectation.source_id or '')} | "
                f"{expectation.dimension} | "
                f"{_markdown_cell(expectation.data_subject)} | "
                f"{_markdown_cell(expectation.threshold or '')} | "
                f"{expectation.confidence} | "
                f"{_markdown_cell('; '.join(expectation.evidence))} | "
                f"{_markdown_cell(expectation.planning_note)} |"
            )
        return "\n".join(lines)


def build_source_data_quality_expectations(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceDataQualityExpectationsReport:
    """Extract data quality expectations from brief-shaped input."""
    source_payloads = _source_payloads(source)
    expectations = tuple(
        sorted(
            _merge_candidates(_expectation_candidates(source_payloads)),
            key=lambda expectation: (
                _optional_text(expectation.source_id) or "",
                _dimension_index(expectation.dimension),
                expectation.data_subject.casefold(),
                expectation.threshold or "",
                _CONFIDENCE_ORDER[expectation.confidence],
                expectation.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in source_payloads if source_id)
    return SourceDataQualityExpectationsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        expectations=expectations,
        summary=_summary(expectations, len(source_payloads)),
    )


def build_source_data_quality_expectations_report(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceDataQualityExpectationsReport:
    """Compatibility helper for callers that use explicit report naming."""
    return build_source_data_quality_expectations(source)


def generate_source_data_quality_expectations(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> SourceDataQualityExpectationsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_data_quality_expectations(source)


def extract_source_data_quality_expectations(
    source: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> tuple[SourceDataQualityExpectation, ...]:
    """Return data quality expectation records from brief-shaped input."""
    return build_source_data_quality_expectations(source).expectations


def summarize_source_data_quality_expectations(
    source_or_report: (
        Mapping[str, Any]
        | SourceBrief
        | ImplementationBrief
        | SourceDataQualityExpectationsReport
        | Iterable[Mapping[str, Any] | SourceBrief | ImplementationBrief]
        | str
        | object
    ),
) -> dict[str, Any]:
    """Return deterministic counts for source data quality expectations."""
    if isinstance(source_or_report, SourceDataQualityExpectationsReport):
        return dict(source_or_report.summary)
    return build_source_data_quality_expectations(source_or_report).summary


def source_data_quality_expectations_to_dict(report: SourceDataQualityExpectationsReport) -> dict[str, Any]:
    """Serialize a data quality expectations report to a plain dictionary."""
    return report.to_dict()


source_data_quality_expectations_to_dict.__test__ = False


def source_data_quality_expectations_to_dicts(
    expectations: (
        tuple[SourceDataQualityExpectation, ...]
        | list[SourceDataQualityExpectation]
        | SourceDataQualityExpectationsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize data quality expectation records to dictionaries."""
    if isinstance(expectations, SourceDataQualityExpectationsReport):
        return expectations.to_dicts()
    return [expectation.to_dict() for expectation in expectations]


source_data_quality_expectations_to_dicts.__test__ = False


def source_data_quality_expectations_to_markdown(report: SourceDataQualityExpectationsReport) -> str:
    """Render a data quality expectations report as Markdown."""
    return report.to_markdown()


source_data_quality_expectations_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Segment:
    source_id: str | None
    source_field: str
    text: str
    section_context: bool


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_id: str | None
    dimension: SourceDataQualityDimension
    data_subject: str
    threshold: str | None
    confidence: SourceDataQualityConfidence
    evidence: str


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
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _expectation_candidates(source_payloads: Iterable[tuple[str | None, Mapping[str, Any]]]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_id, payload in source_payloads:
        for segment in _candidate_segments(source_id, payload):
            dimensions = _dimensions(segment)
            if not dimensions:
                continue
            evidence = _evidence_snippet(segment.source_field, segment.text)
            for dimension in dimensions:
                candidates.append(
                    _Candidate(
                        source_id=segment.source_id,
                        dimension=dimension,
                        data_subject=_data_subject(segment.text, segment.source_field),
                        threshold=_threshold(segment.text) if dimension == "freshness_threshold" else None,
                        confidence=_confidence(dimension, segment),
                        evidence=evidence,
                    )
                )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceDataQualityExpectation]:
    grouped: dict[tuple[str | None, SourceDataQualityDimension], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_id, candidate.dimension), []).append(candidate)

    expectations: list[SourceDataQualityExpectation] = []
    for (source_id, dimension), items in grouped.items():
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        expectations.append(
            SourceDataQualityExpectation(
                source_id=source_id,
                dimension=dimension,
                data_subject=next((item.data_subject for item in items if item.data_subject != "source data"), "source data"),
                threshold=next((item.threshold for item in items if item.threshold), None),
                confidence=confidence,
                evidence=tuple(_dedupe_evidence(item.evidence for item in items))[:6],
                planning_note=_planning_note(dimension),
            )
        )
    return expectations


def _candidate_segments(source_id: str | None, payload: Mapping[str, Any]) -> list[_Segment]:
    segments: list[_Segment] = []
    visited: set[str] = set()
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(segments, source_id, field_name, payload[field_name], False)
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, source_id, str(key), payload[key], False)
    return segments


def _append_value(
    segments: list[_Segment],
    source_id: str | None,
    source_field: str,
    value: Any,
    section_context: bool,
) -> None:
    field_context = section_context or bool(_STRUCTURED_FIELD_RE.search(_field_words(source_field)))
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " ").replace("-", " "))
            child_context = field_context or bool(_STRUCTURED_FIELD_RE.search(key_text))
            _append_value(segments, source_id, child_field, value[key], child_context)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, source_id, f"{source_field}[{index}]", item, field_context)
        return
    if text := _optional_text(value):
        for segment_text, segment_context in _segments(text, field_context):
            segments.append(_Segment(source_id, source_field, segment_text, segment_context))


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
            section_context = inherited_context or bool(_QUALITY_CONTEXT_RE.search(title) or _STRUCTURED_FIELD_RE.search(title))
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


def _dimensions(segment: _Segment) -> tuple[SourceDataQualityDimension, ...]:
    if _NEGATED_SCOPE_RE.search(segment.text):
        return ()
    searchable = f"{_field_words(segment.source_field)} {segment.text}"
    dimensions: list[SourceDataQualityDimension] = []
    for dimension in _DIMENSION_ORDER:
        dimension_searchable = segment.text
        if dimension != "validation" or not segment.source_field.endswith("validation_plan"):
            dimension_searchable = searchable
        if _DIMENSION_PATTERNS[dimension].search(dimension_searchable):
            dimensions.append(dimension)
    if "freshness_threshold" in dimensions and not _broader_quality_context(segment, dimensions):
        dimensions.remove("freshness_threshold")
    if not dimensions or not _is_expectation(segment, dimensions):
        return ()
    return tuple(_dedupe(dimensions))


def _broader_quality_context(segment: _Segment, dimensions: Iterable[SourceDataQualityDimension]) -> bool:
    non_freshness_dimensions = [dimension for dimension in dimensions if dimension != "freshness_threshold"]
    if non_freshness_dimensions:
        return True
    field_words = _field_words(segment.source_field)
    if _STRUCTURED_FIELD_RE.search(field_words) and not re.fullmatch(r".*fresh(?:ness)?(?: threshold| sla| target)?", field_words, re.I):
        return True
    quality_text = _QUALITY_CONTEXT_RE.search(segment.text)
    return bool(quality_text and not re.fullmatch(r"(?:freshness|stale|staleness)", quality_text.group(0), re.I))


def _is_expectation(segment: _Segment, dimensions: Iterable[SourceDataQualityDimension]) -> bool:
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    if field_context or segment.section_context:
        return True
    if _EXPECTATION_RE.search(segment.text) and (_QUALITY_CONTEXT_RE.search(segment.text) or len(tuple(dimensions)) > 1):
        return True
    if any(dimension in {"schema_conformance", "reconciliation"} for dimension in dimensions):
        return True
    return False


def _confidence(dimension: SourceDataQualityDimension, segment: _Segment) -> SourceDataQualityConfidence:
    field_context = bool(_STRUCTURED_FIELD_RE.search(_field_words(segment.source_field)))
    has_threshold = bool(_threshold(segment.text))
    if _EXPECTATION_RE.search(segment.text) and (field_context or segment.section_context or has_threshold):
        return "high"
    if dimension in {"validation", "schema_conformance", "reconciliation", "freshness_threshold"} and (
        field_context or has_threshold
    ):
        return "high"
    if field_context or segment.section_context or _EXPECTATION_RE.search(segment.text):
        return "medium"
    return "low"


def _threshold(text: str) -> str | None:
    match = _THRESHOLD_RE.search(text)
    if not match:
        return None
    return _clean_text(match.group(1))


def _data_subject(text: str, source_field: str) -> str:
    searchable = f"{_field_words(source_field)} {text}"
    for subject, pattern in _SUBJECT_PATTERNS:
        if pattern.search(searchable):
            return subject
    field_parts = [
        part
        for part in re.split(r"[.\[\]_\-\s]+", source_field)
        if part and not part.isdigit() and part not in {"source", "payload", "metadata", "requirements", "quality", "data"}
    ]
    if field_parts and _STRUCTURED_FIELD_RE.search(source_field):
        return _clean_text(" ".join(field_parts[-2:])).casefold()
    return "source data"


def _summary(expectations: tuple[SourceDataQualityExpectation, ...], input_count: int) -> dict[str, Any]:
    source_ids = sorted({expectation.source_id for expectation in expectations if expectation.source_id}, key=str.casefold)
    return {
        "expectation_count": len(expectations),
        "input_count": input_count,
        "dimension_counts": {
            dimension: sum(1 for expectation in expectations if expectation.dimension == dimension)
            for dimension in _DIMENSION_ORDER
        },
        "source_counts": {
            source_id: sum(1 for expectation in expectations if expectation.source_id == source_id)
            for source_id in source_ids
        },
        "confidence_counts": {
            confidence: sum(1 for expectation in expectations if expectation.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "dimensions": [expectation.dimension for expectation in expectations],
        "source_ids": source_ids,
    }


def _planning_note(dimension: SourceDataQualityDimension) -> str:
    notes: dict[SourceDataQualityDimension, str] = {
        "completeness": "Plan completeness checks and missing-record handling before downstream use.",
        "accuracy": "Plan source-of-truth comparison and correction evidence for accuracy-sensitive data.",
        "deduplication": "Plan duplicate detection, uniqueness rules, and idempotent import behavior.",
        "validation": "Plan explicit validation gates, rejected-record handling, and test fixtures.",
        "freshness_threshold": "Plan freshness validation as a data quality threshold alongside other quality checks.",
        "consistency": "Plan cross-field or cross-system consistency checks and diagnostics.",
        "null_handling": "Plan null, blank, and default-value behavior in ingestion, storage, and exports.",
        "schema_conformance": "Plan schema or contract conformance checks with versioned fixtures.",
        "reconciliation": "Plan reconciliation, control totals, and exception reporting.",
    }
    return notes[dimension]


def _dimension_index(dimension: SourceDataQualityDimension) -> int:
    return _DIMENSION_ORDER.index(dimension)


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
        "assumptions",
        "integration_points",
        "metadata",
        "brief_metadata",
        "source_payload",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


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
        key = str(value).casefold()
        if key in seen:
            continue
        deduped.append(value)
        seen.add(key)
    return deduped


__all__ = [
    "SourceDataQualityConfidence",
    "SourceDataQualityDimension",
    "SourceDataQualityExpectation",
    "SourceDataQualityExpectationsReport",
    "build_source_data_quality_expectations",
    "build_source_data_quality_expectations_report",
    "extract_source_data_quality_expectations",
    "generate_source_data_quality_expectations",
    "source_data_quality_expectations_to_dict",
    "source_data_quality_expectations_to_dicts",
    "source_data_quality_expectations_to_markdown",
    "summarize_source_data_quality_expectations",
]
