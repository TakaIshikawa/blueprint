"""Extract data freshness, staleness, and sync cadence expectations from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


FreshnessExpectationMode = Literal[
    "real_time",
    "near_real_time",
    "batch",
    "polling",
    "cache_age",
    "webhook",
    "sync_cadence",
    "manual_refresh",
    "last_updated",
    "staleness_tolerance",
]
FreshnessExpectationConfidence = Literal["high", "medium", "low"]
FreshnessMissingDetailFlag = Literal[
    "missing_cadence_or_max_age",
    "missing_update_mechanism",
    "missing_user_visible_impact",
]
_T = TypeVar("_T")

_MODE_ORDER: tuple[FreshnessExpectationMode, ...] = (
    "real_time",
    "near_real_time",
    "batch",
    "polling",
    "cache_age",
    "webhook",
    "sync_cadence",
    "manual_refresh",
    "last_updated",
    "staleness_tolerance",
)
_CONFIDENCE_ORDER: dict[FreshnessExpectationConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|need(?:s)?|support|ensure|acceptance|"
    r"done when|before launch|sla|service level|no older than|at most|within)\b",
    re.I,
)
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:fresh(?:ness)?|stale|staleness|sync|cadence|latency|real[-_ ]?time|"
    r"batch|poll(?:ing)?|cache|webhook|refresh|last[-_ ]?updated|import|sla)",
    re.I,
)
_IGNORED_FIELDS = {
    "id",
    "source_id",
    "source_brief_id",
    "source_project",
    "source_entity_type",
    "created_at",
    "updated_at",
}

_MODE_PATTERNS: dict[FreshnessExpectationMode, re.Pattern[str]] = {
    "real_time": re.compile(r"\b(?:real[- ]?time|live updates?|instant(?:ly)?|immediate(?:ly)?)\b", re.I),
    "near_real_time": re.compile(r"\b(?:near[- ]?real[- ]?time|near live|low[- ]latency|within seconds?)\b", re.I),
    "batch": re.compile(
        r"\b(?:(?:daily|hourly|nightly|weekly|monthly)\s+(?:batch|import|export|job|load)|"
        r"batch(?:ed)?\s+(?:job|import|export|load|process|update|refresh)|etl|bulk import)\b",
        re.I,
    ),
    "polling": re.compile(r"\b(?:poll(?:ing)?|polls?|periodic(?:ally)? check|scheduled check)\b", re.I),
    "cache_age": re.compile(
        r"\b(?:cache age|cache ttl|ttl|cached (?:for|data)|max(?:imum)? cache|cache expires?)\b",
        re.I,
    ),
    "webhook": re.compile(r"\b(?:webhook|web hook|callback update|event push|pushed update)\b", re.I),
    "sync_cadence": re.compile(r"\b(?:sync every|syncs? every|synchroni[sz]e every|sync cadence|cadence|delta sync|scheduled sync)\b", re.I),
    "manual_refresh": re.compile(r"\b(?:refresh|manual refresh|refresh button|pull to refresh|refresh required)\b", re.I),
    "last_updated": re.compile(r"\b(?:last updated|last refreshed|last synced|updated at|as of timestamp)\b", re.I),
    "staleness_tolerance": re.compile(
        r"\b(?:stale|staleness|out[- ]of[- ]date|freshness sla|freshness target|freshness requirement|"
        r"no older than|at most \d|maximum age|max age)\b",
        re.I,
    ),
}
_SURFACE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("inventory", re.compile(r"\binventory\b", re.I)),
    ("orders", re.compile(r"\borders?\b", re.I)),
    ("dashboard", re.compile(r"\bdashboards?\b", re.I)),
    ("reports", re.compile(r"\breports?\b", re.I)),
    ("metrics", re.compile(r"\bmetrics?\b", re.I)),
    ("search index", re.compile(r"\bsearch index\b|\bsearch results?\b", re.I)),
    ("customer data", re.compile(r"\bcustomer data\b|\bcustomers?\b", re.I)),
    ("catalog", re.compile(r"\bcatalog\b", re.I)),
    ("profile", re.compile(r"\bprofiles?\b", re.I)),
    ("price data", re.compile(r"\bprices?\b|\bpricing\b", re.I)),
    ("source data", re.compile(r"\bsource data\b", re.I)),
)
_USER_IMPACT_RE = re.compile(
    r"\b(?:user(?:s)?|customer(?:s)?|admin(?:s)?|operator(?:s)?|viewer(?:s)?|agent(?:s)?|"
    r"show|display|visible|warn|warning|banner|badge|indicator|block|prevent|validation|decision)\b",
    re.I,
)
_CADENCE_RE = re.compile(
    r"\b(?:(?:sync|refresh|poll|import|update|run|batch|load)s?\s+)?"
    r"(?:every|each|per)\s+(\d+(?:\.\d+)?\s*(?:seconds?|secs?|minutes?|mins?|hours?|hrs?|days?|weeks?))\b|"
    r"\b((?:hourly|daily|nightly|weekly|monthly)(?:\s+(?:batch|import|sync|refresh|job|load))?)\b|"
    r"\b((?:once|twice)\s+(?:per|a)\s+(?:hour|day|week|month))\b",
    re.I,
)
_MAX_AGE_RE = re.compile(
    r"\b(?:no older than|not older than|at most|maximum age|max age|max(?:imum)? cache age|"
    r"cache age|cache ttl|ttl|stale after|freshness sla(?: of)?|freshness target(?: of)?|within)\s+"
    r"(?:is\s+|of\s+)?"
    r"(\d+(?:\.\d+)?\s*(?:seconds?|secs?|minutes?|mins?|hours?|hrs?|days?|weeks?))\b",
    re.I,
)
_LATENCY_RE = re.compile(
    r"\b(?:update latency|latency|propagat(?:e|ion)|webhook update|reflect(?:ed)?|visible)\s+"
    r"(?:within|under|below|in)\s+(\d+(?:\.\d+)?\s*(?:seconds?|secs?|minutes?|mins?|hours?|hrs?))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class SourceDataFreshnessExpectation:
    """One source-backed freshness or update-latency expectation."""

    source_brief_id: str | None
    freshness_surface: str
    expectation_mode: FreshnessExpectationMode
    expected_cadence: str | None = None
    max_age: str | None = None
    user_visible_impact: str | None = None
    missing_detail_flags: tuple[FreshnessMissingDetailFlag, ...] = field(default_factory=tuple)
    confidence: FreshnessExpectationConfidence = "medium"
    evidence: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "freshness_surface": self.freshness_surface,
            "expectation_mode": self.expectation_mode,
            "expected_cadence": self.expected_cadence,
            "max_age": self.max_age,
            "user_visible_impact": self.user_visible_impact,
            "missing_detail_flags": list(self.missing_detail_flags),
            "confidence": self.confidence,
            "evidence": list(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class SourceDataFreshnessExpectationsReport:
    """Brief-level data freshness expectation report."""

    source_brief_id: str | None = None
    title: str | None = None
    expectations: tuple[SourceDataFreshnessExpectation, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceDataFreshnessExpectation, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.expectations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "expectations": [expectation.to_dict() for expectation in self.expectations],
            "records": [record.to_dict() for record in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return freshness expectation records as plain dictionaries."""
        return [expectation.to_dict() for expectation in self.expectations]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Data Freshness Expectations Report"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        mode_counts = self.summary.get("mode_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Expectations found: {self.summary.get('expectation_count', 0)}",
            "- Confidence counts: " + ", ".join(
                f"{confidence} {confidence_counts.get(confidence, 0)}" for confidence in _CONFIDENCE_ORDER
            ),
            "- Mode counts: " + ", ".join(f"{mode} {mode_counts.get(mode, 0)}" for mode in _MODE_ORDER),
        ]
        if not self.expectations:
            lines.extend(["", "No source data freshness expectations were found in the brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Expectations",
                "",
                "| Surface | Mode | Cadence | Max Age | Impact | Missing Details | Confidence | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for expectation in self.expectations:
            lines.append(
                "| "
                f"{_markdown_cell(expectation.freshness_surface)} | "
                f"{expectation.expectation_mode} | "
                f"{_markdown_cell(expectation.expected_cadence or 'unspecified')} | "
                f"{_markdown_cell(expectation.max_age or 'unspecified')} | "
                f"{_markdown_cell(expectation.user_visible_impact or 'unspecified')} | "
                f"{_markdown_cell(', '.join(expectation.missing_detail_flags) or 'none')} | "
                f"{expectation.confidence} | "
                f"{_markdown_cell('; '.join(expectation.evidence))} |"
            )
        return "\n".join(lines)


def build_source_data_freshness_expectations(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceDataFreshnessExpectationsReport:
    """Extract data freshness expectations from a source brief."""
    source_brief_id, payload = _source_payload(source)
    expectations = tuple(
        sorted(
            _merge_candidates(_expectation_candidates(source_brief_id, payload)),
            key=lambda expectation: (
                _CONFIDENCE_ORDER[expectation.confidence],
                _mode_index(expectation.expectation_mode),
                expectation.freshness_surface.casefold(),
                expectation.expected_cadence or "",
                expectation.max_age or "",
                expectation.evidence,
            ),
        )
    )
    return SourceDataFreshnessExpectationsReport(
        source_brief_id=source_brief_id,
        title=_optional_text(payload.get("title")),
        expectations=expectations,
        summary=_summary(expectations),
    )


def generate_source_data_freshness_expectations(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceDataFreshnessExpectationsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_data_freshness_expectations(source)


def extract_source_data_freshness_expectations(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SourceDataFreshnessExpectation, ...]:
    """Return data freshness expectation records from brief-shaped input."""
    return build_source_data_freshness_expectations(source).expectations


def source_data_freshness_expectations_to_dict(
    report: SourceDataFreshnessExpectationsReport,
) -> dict[str, Any]:
    """Serialize a data freshness expectations report to a plain dictionary."""
    return report.to_dict()


source_data_freshness_expectations_to_dict.__test__ = False


def source_data_freshness_expectations_to_dicts(
    expectations: (
        tuple[SourceDataFreshnessExpectation, ...]
        | list[SourceDataFreshnessExpectation]
        | SourceDataFreshnessExpectationsReport
    ),
) -> list[dict[str, Any]]:
    """Serialize data freshness expectation records to dictionaries."""
    if isinstance(expectations, SourceDataFreshnessExpectationsReport):
        return expectations.to_dicts()
    return [expectation.to_dict() for expectation in expectations]


source_data_freshness_expectations_to_dicts.__test__ = False


def source_data_freshness_expectations_to_markdown(
    report: SourceDataFreshnessExpectationsReport,
) -> str:
    """Render a data freshness expectations report as Markdown."""
    return report.to_markdown()


source_data_freshness_expectations_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    freshness_surface: str
    expectation_mode: FreshnessExpectationMode
    expected_cadence: str | None
    max_age: str | None
    user_visible_impact: str | None
    evidence: str
    confidence: FreshnessExpectationConfidence


def _source_payload(source: Mapping[str, Any] | SourceBrief | str | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        payload = source.model_dump(mode="python")
        return _source_brief_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_brief_id(payload), payload
    if isinstance(source, Mapping):
        try:
            payload = dict(SourceBrief.model_validate(source).model_dump(mode="python"))
        except (TypeError, ValueError, ValidationError):
            payload = dict(source)
        return _source_brief_id(payload), payload
    payload = _object_payload(source)
    return _source_brief_id(payload), payload


def _source_brief_id(payload: Mapping[str, Any]) -> str | None:
    return (
        _optional_text(payload.get("id"))
        or _optional_text(payload.get("source_brief_id"))
        or _optional_text(payload.get("source_id"))
    )


def _expectation_candidates(source_brief_id: str | None, payload: Mapping[str, Any]) -> list[_Candidate]:
    candidates: list[_Candidate] = []
    for source_field, segment in _candidate_segments(payload):
        modes = _expectation_modes(segment, source_field)
        if not modes:
            continue
        expected_cadence = _expected_cadence(segment)
        max_age = _max_age(segment)
        user_visible_impact = _user_visible_impact(segment)
        evidence = _evidence_snippet(source_field, segment)
        for mode in modes:
            candidates.append(
                _Candidate(
                    source_brief_id=source_brief_id,
                    freshness_surface=_freshness_surface(segment, source_field),
                    expectation_mode=mode,
                    expected_cadence=expected_cadence,
                    max_age=max_age,
                    user_visible_impact=user_visible_impact,
                    evidence=evidence,
                    confidence=_confidence(segment, source_field, expected_cadence, max_age),
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceDataFreshnessExpectation]:
    grouped: dict[
        tuple[str | None, str, FreshnessExpectationMode, str | None, str | None],
        list[_Candidate],
    ] = {}
    for candidate in candidates:
        grouped.setdefault(
            (
                candidate.source_brief_id,
                candidate.freshness_surface,
                candidate.expectation_mode,
                candidate.expected_cadence,
                candidate.max_age,
            ),
            [],
        ).append(candidate)

    expectations: list[SourceDataFreshnessExpectation] = []
    for (source_brief_id, surface, mode, cadence, max_age), items in grouped.items():
        impact = _first_present(item.user_visible_impact for item in items)
        flags = _missing_detail_flags(mode, cadence, max_age, impact)
        confidence = min((item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value])
        if flags and confidence == "high":
            confidence = "medium"
        expectations.append(
            SourceDataFreshnessExpectation(
                source_brief_id=source_brief_id,
                freshness_surface=surface,
                expectation_mode=mode,
                expected_cadence=cadence,
                max_age=max_age,
                user_visible_impact=impact,
                missing_detail_flags=flags,
                confidence=confidence,
                evidence=tuple(sorted(_dedupe(item.evidence for item in items), key=str.casefold))[:6],
            )
        )
    return expectations


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    segments: list[tuple[str, str]] = []
    visited: set[str] = set()
    for field_name in (
        "title",
        "summary",
        "body",
        "description",
        "requirements",
        "constraints",
        "risks",
        "acceptance",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "data_requirements",
        "freshness",
        "sync",
        "cache",
        "metadata",
        "brief_metadata",
        "architecture_notes",
        "implementation_notes",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(segments, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key in visited or str(key) in _IGNORED_FIELDS:
            continue
        _append_value(segments, str(key), payload[key])
    return [(field, segment) for field, segment in segments if segment]


def _append_value(segments: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            if _any_signal(key_text):
                segments.append((child_field, key_text))
            if isinstance(child, (Mapping, list, tuple, set)):
                _append_value(segments, child_field, child)
            elif text := _optional_text(child):
                for segment in _segments(text):
                    segments.append((child_field, segment))
                if _any_signal(key_text):
                    for segment in _segments(f"{key_text}: {text}"):
                        segments.append((child_field, segment))
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(segments, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        for segment in _segments(text):
            segments.append((source_field, segment))


def _segments(value: str) -> list[str]:
    parts: list[str] = []
    for line in value.splitlines() or [value]:
        cleaned = _clean_text(line)
        if not cleaned:
            continue
        sentence_parts = [_clean_text(cleaned)] if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line) else (
            _SENTENCE_SPLIT_RE.split(cleaned)
        )
        for sentence in sentence_parts:
            parts.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in parts if _clean_text(part)]


def _expectation_modes(text: str, source_field: str) -> tuple[FreshnessExpectationMode, ...]:
    searchable = f"{source_field.replace('_', ' ').replace('-', ' ')} {text}"
    modes = [mode for mode in _MODE_ORDER if _MODE_PATTERNS[mode].search(searchable)]
    if _MAX_AGE_RE.search(text) and "staleness_tolerance" not in modes:
        modes.append("staleness_tolerance")
    if _CADENCE_RE.search(text) and "sync_cadence" not in modes:
        modes.append("sync_cadence")
    return tuple(modes)


def _expected_cadence(text: str) -> str | None:
    match = _CADENCE_RE.search(text)
    if not match:
        return None
    return _clean_text(next(group for group in match.groups() if group))


def _max_age(text: str) -> str | None:
    match = _MAX_AGE_RE.search(text) or _LATENCY_RE.search(text)
    if not match:
        return None
    return _clean_text(match.group(1))


def _freshness_surface(text: str, source_field: str) -> str:
    searchable = f"{source_field.replace('_', ' ').replace('-', ' ')} {text}"
    for surface, pattern in _SURFACE_PATTERNS:
        if pattern.search(searchable):
            return surface
    field_parts = [
        part
        for part in re.split(r"[.\[\]_\-\s]+", source_field)
        if part and not part.isdigit() and part not in {"source", "payload", "metadata", "requirements"}
    ]
    if field_parts and _STRUCTURED_FIELD_RE.search(source_field):
        return _clean_text(" ".join(field_parts[-2:]))
    return "source data"


def _user_visible_impact(text: str) -> str | None:
    if not _USER_IMPACT_RE.search(text):
        return None
    return _clean_text(text)


def _missing_detail_flags(
    mode: FreshnessExpectationMode,
    cadence: str | None,
    max_age: str | None,
    impact: str | None,
) -> tuple[FreshnessMissingDetailFlag, ...]:
    flags: list[FreshnessMissingDetailFlag] = []
    if mode in {"batch", "polling", "cache_age", "sync_cadence", "near_real_time", "staleness_tolerance"} and not (
        cadence or max_age
    ):
        flags.append("missing_cadence_or_max_age")
    if mode in {"real_time", "near_real_time", "staleness_tolerance"} and not impact:
        flags.append("missing_user_visible_impact")
    if mode in {"last_updated", "staleness_tolerance"} and not (
        cadence or max_age or mode == "last_updated"
    ):
        flags.append("missing_update_mechanism")
    return tuple(flags)


def _confidence(
    text: str,
    source_field: str,
    cadence: str | None,
    max_age: str | None,
) -> FreshnessExpectationConfidence:
    structured_field = bool(_STRUCTURED_FIELD_RE.search(source_field))
    mode_count = len(_expectation_modes(text, source_field))
    if (_REQUIRED_RE.search(text) or structured_field) and (cadence or max_age or mode_count > 1):
        return "high"
    if cadence or max_age or _REQUIRED_RE.search(text) or mode_count > 1:
        return "medium"
    return "low"


def _summary(expectations: tuple[SourceDataFreshnessExpectation, ...]) -> dict[str, Any]:
    surfaces = sorted({expectation.freshness_surface for expectation in expectations}, key=str.casefold)
    return {
        "expectation_count": len(expectations),
        "mode_counts": {
            mode: sum(1 for expectation in expectations if expectation.expectation_mode == mode)
            for mode in _MODE_ORDER
        },
        "confidence_counts": {
            confidence: sum(1 for expectation in expectations if expectation.confidence == confidence)
            for confidence in _CONFIDENCE_ORDER
        },
        "missing_detail_counts": {
            flag: sum(1 for expectation in expectations if flag in expectation.missing_detail_flags)
            for flag in (
                "missing_cadence_or_max_age",
                "missing_update_mechanism",
                "missing_user_visible_impact",
            )
        },
        "freshness_surfaces": surfaces,
    }


def _mode_index(mode: FreshnessExpectationMode) -> int:
    return _MODE_ORDER.index(mode)


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
        "constraints",
        "risks",
        "metadata",
        "brief_metadata",
        "source_payload",
        "source_links",
        "acceptance_criteria",
        "definition_of_done",
        "validation_plan",
        "implementation_notes",
        "data_requirements",
        "architecture_notes",
        "freshness",
        "sync",
        "cache",
    )
    return {field_name: getattr(value, field_name) for field_name in fields if hasattr(value, field_name)}


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in _MODE_PATTERNS.values()) or bool(
        _CADENCE_RE.search(text) or _MAX_AGE_RE.search(text) or _STRUCTURED_FIELD_RE.search(text)
    )


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


def _dedupe(values: Iterable[_T]) -> list[_T]:
    result: list[_T] = []
    seen: set[Any] = set()
    for value in values:
        key = value.casefold() if isinstance(value, str) else value
        if not value or key in seen:
            continue
        result.append(value)
        seen.add(key)
    return result


def _first_present(values: Iterable[str | None]) -> str | None:
    for value in values:
        if value:
            return value
    return None


__all__ = [
    "FreshnessExpectationConfidence",
    "FreshnessExpectationMode",
    "FreshnessMissingDetailFlag",
    "SourceDataFreshnessExpectation",
    "SourceDataFreshnessExpectationsReport",
    "build_source_data_freshness_expectations",
    "extract_source_data_freshness_expectations",
    "generate_source_data_freshness_expectations",
    "source_data_freshness_expectations_to_dict",
    "source_data_freshness_expectations_to_dicts",
    "source_data_freshness_expectations_to_markdown",
]
