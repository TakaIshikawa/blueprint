"""Extract offline, intermittent connectivity, and sync expectations from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


OfflineExpectationType = Literal[
    "offline_mode",
    "intermittent_connectivity",
    "sync",
    "conflict_resolution",
    "local_cache",
    "outbox",
    "retry_when_online",
    "stale_data",
]
OfflineExpectationConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_EXPECTATION_ORDER: tuple[OfflineExpectationType, ...] = (
    "offline_mode",
    "intermittent_connectivity",
    "sync",
    "conflict_resolution",
    "local_cache",
    "outbox",
    "retry_when_online",
    "stale_data",
)
_CONFIDENCE_ORDER: dict[OfflineExpectationConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but)\s+", re.I)
_REQUIRED_RE = re.compile(
    r"\b(?:must|shall|required|requires?|need(?:s)?|support|ensure|handle|prevent|"
    r"done when|acceptance|before launch|cannot lose)\b",
    re.I,
)

_SIGNAL_PATTERNS: dict[OfflineExpectationType, re.Pattern[str]] = {
    "offline_mode": re.compile(
        r"\b(?:offline mode|offline[- ]first|work offline|offline access|without network|"
        r"without connectivity|no connectivity|no connection|no internet|airplane mode|"
        r"network unavailable)\b",
        re.I,
    ),
    "intermittent_connectivity": re.compile(
        r"\b(?:intermittent (?:connection|connectivity|network)|flaky (?:connection|network)|"
        r"spotty (?:connection|connectivity)|poor connectivity|dropped connection|network drops|"
        r"connection drops|goes offline|comes back online)\b",
        re.I,
    ),
    "sync": re.compile(
        r"\b(?:sync|syncs|synced|syncing|synchroni[sz]e|synchroni[sz]ation|"
        r"background sync|delta sync|merge remote changes)\b",
        re.I,
    ),
    "conflict_resolution": re.compile(
        r"\b(?:conflict resolution|conflict handling|sync conflict|merge conflict|"
        r"write conflict|concurrent edits?|last[- ]write[- ]wins|version conflict)\b",
        re.I,
    ),
    "local_cache": re.compile(
        r"\b(?:local cache|cached locally|cache for offline|offline cache|local storage|"
        r"indexeddb|sqlite|persist locally|local persistence|cached data)\b",
        re.I,
    ),
    "outbox": re.compile(
        r"\b(?:outbox|queued changes?|queued writes?|pending changes?|pending writes?|"
        r"queue submissions?|store and forward|deferred upload)\b",
        re.I,
    ),
    "retry_when_online": re.compile(
        r"\b(?:retry when online|retry once online|retry on reconnect|retry after reconnect|"
        r"retry when connectivity returns|resume when online|send when online|flush when online|"
        r"when back online|after reconnect(?:ion)?)\b",
        re.I,
    ),
    "stale_data": re.compile(
        r"\b(?:stale data|staleness|stale warning|out[- ]of[- ]date|outdated data|"
        r"data freshness|last synced|last updated|refresh required)\b",
        re.I,
    ),
}
_STRUCTURED_FIELD_RE = re.compile(
    r"(?:offline|connectivity|connection|sync|synchroni[sz]e|cache|outbox|queue|"
    r"retry|stale|freshness|conflict)",
    re.I,
)
_PLANNING_IMPLICATIONS: dict[OfflineExpectationType, str] = {
    "offline_mode": "Plan offline-capable storage, validation, and user-visible availability boundaries.",
    "intermittent_connectivity": "Plan resilient network state handling and transitions between offline and online operation.",
    "sync": "Plan sync protocol ownership, idempotent writes, server reconciliation, and verification paths.",
    "conflict_resolution": "Plan conflict detection, merge policy, user resolution flows, and auditability of chosen values.",
    "local_cache": "Plan cache schema, invalidation, encryption or retention needs, and cache warming behavior.",
    "outbox": "Plan durable queued-change storage, ordering, replay semantics, and failed-item visibility.",
    "retry_when_online": "Plan retry backoff, reconnect triggers, idempotency keys, and permanent-failure handling.",
    "stale_data": "Plan freshness indicators, stale-read limits, refresh triggers, and validation against outdated data.",
}


@dataclass(frozen=True, slots=True)
class SourceOfflineExpectation:
    """One offline or sync expectation found in source evidence."""

    source_brief_id: str | None
    expectation_type: OfflineExpectationType
    detected_signals: tuple[str, ...] = field(default_factory=tuple)
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: OfflineExpectationConfidence = "medium"
    planning_implications: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "expectation_type": self.expectation_type,
            "detected_signals": list(self.detected_signals),
            "evidence": list(self.evidence),
            "confidence": self.confidence,
            "planning_implications": list(self.planning_implications),
        }


@dataclass(frozen=True, slots=True)
class SourceOfflineExpectationsReport:
    """Source-level offline expectation report."""

    source_id: str | None = None
    expectations: tuple[SourceOfflineExpectation, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceOfflineExpectation, ...]:
        """Compatibility view matching reports that expose rows as records."""
        return self.expectations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_id": self.source_id,
            "expectations": [expectation.to_dict() for expectation in self.expectations],
            "summary": dict(self.summary),
            "records": [expectation.to_dict() for expectation in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return expectation records as plain dictionaries."""
        return [expectation.to_dict() for expectation in self.expectations]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Offline Expectations Report"
        if self.source_id:
            title = f"{title}: {self.source_id}"
        type_counts = self.summary.get("expectation_type_counts", {})
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Expectations found: {self.summary.get('expectation_count', 0)}",
            "- Confidence counts: "
            f"high {confidence_counts.get('high', 0)}, "
            f"medium {confidence_counts.get('medium', 0)}, "
            f"low {confidence_counts.get('low', 0)}",
            "- Expectation type counts: "
            + (", ".join(f"{key} {type_counts[key]}" for key in sorted(type_counts)) or "none"),
        ]
        if not self.expectations:
            lines.extend(["", "No offline or sync expectations were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Expectations",
                "",
                "| Source Brief | Type | Confidence | Signals | Evidence | Planning Implications |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for expectation in self.expectations:
            lines.append(
                "| "
                f"{_markdown_cell(expectation.source_brief_id or '')} | "
                f"{expectation.expectation_type} | "
                f"{expectation.confidence} | "
                f"{_markdown_cell('; '.join(expectation.detected_signals))} | "
                f"{_markdown_cell('; '.join(expectation.evidence))} | "
                f"{_markdown_cell('; '.join(expectation.planning_implications))} |"
            )
        return "\n".join(lines)


def build_source_offline_expectations(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> SourceOfflineExpectationsReport:
    """Extract offline, connectivity, and sync expectation records from source briefs."""
    brief_payloads = _source_payloads(source)
    expectations = tuple(
        sorted(
            _merge_candidates(_candidates_for_briefs(brief_payloads)),
            key=lambda expectation: (
                _optional_text(expectation.source_brief_id) or "",
                _expectation_index(expectation.expectation_type),
                _CONFIDENCE_ORDER[expectation.confidence],
                expectation.evidence,
            ),
        )
    )
    source_ids = _dedupe(source_id for source_id, _ in brief_payloads if source_id)
    return SourceOfflineExpectationsReport(
        source_id=source_ids[0] if len(source_ids) == 1 else None,
        expectations=expectations,
        summary=_summary(expectations, len(brief_payloads)),
    )


def generate_source_offline_expectations(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> SourceOfflineExpectationsReport:
    """Compatibility helper for callers that use generate_* naming."""
    return build_source_offline_expectations(source)


def extract_source_offline_expectations(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> tuple[SourceOfflineExpectation, ...]:
    """Return offline expectation records from brief-shaped input."""
    return build_source_offline_expectations(source).expectations


def source_offline_expectations_to_dict(report: SourceOfflineExpectationsReport) -> dict[str, Any]:
    """Serialize an offline expectations report to a plain dictionary."""
    return report.to_dict()


source_offline_expectations_to_dict.__test__ = False


def source_offline_expectations_to_dicts(
    expectations: tuple[SourceOfflineExpectation, ...] | list[SourceOfflineExpectation],
) -> list[dict[str, Any]]:
    """Serialize offline expectation records to dictionaries."""
    return [expectation.to_dict() for expectation in expectations]


source_offline_expectations_to_dicts.__test__ = False


def source_offline_expectations_to_markdown(report: SourceOfflineExpectationsReport) -> str:
    """Render an offline expectations report as Markdown."""
    return report.to_markdown()


source_offline_expectations_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    source_brief_id: str | None
    expectation_type: OfflineExpectationType
    detected_signal: str
    evidence: str
    confidence: OfflineExpectationConfidence


def _source_payloads(
    source: (
        Mapping[str, Any] | SourceBrief | Iterable[Mapping[str, Any] | SourceBrief] | str | object
    ),
) -> list[tuple[str | None, dict[str, Any]]]:
    if isinstance(source, (str, bytes, bytearray, Mapping, SourceBrief)) or hasattr(
        source, "model_dump"
    ):
        return [_source_payload(source)]
    if isinstance(source, Iterable):
        return [_source_payload(item) for item in source]
    return [_source_payload(source)]


def _source_payload(
    source: Mapping[str, Any] | SourceBrief | str | object
) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        payload = source.model_dump(mode="python")
        return _source_id(payload), dict(payload)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
        return _source_id(payload), payload
    if isinstance(source, Mapping):
        try:
            value = SourceBrief.model_validate(source).model_dump(mode="python")
            payload = dict(value)
        except (TypeError, ValueError, ValidationError):
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
    for source_brief_id, payload in brief_payloads:
        for source_field, segment in _candidate_segments(payload):
            expectation_types = _expectation_types(segment)
            if not expectation_types:
                continue
            confidence = _confidence(segment, source_field)
            evidence = _evidence_snippet(source_field, segment)
            for expectation_type in expectation_types:
                for signal in _detected_signals(expectation_type, segment):
                    candidates.append(
                        _Candidate(
                            source_brief_id=source_brief_id,
                            expectation_type=expectation_type,
                            detected_signal=signal,
                            evidence=evidence,
                            confidence=confidence,
                        )
                    )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceOfflineExpectation]:
    grouped: dict[tuple[str | None, OfflineExpectationType], list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault((candidate.source_brief_id, candidate.expectation_type), []).append(
            candidate
        )

    expectations: list[SourceOfflineExpectation] = []
    for (source_brief_id, expectation_type), items in grouped.items():
        confidence = min(
            (item.confidence for item in items), key=lambda value: _CONFIDENCE_ORDER[value]
        )
        evidence = tuple(
            sorted(_dedupe(item.evidence for item in items), key=lambda item: item.casefold())
        )
        expectations.append(
            SourceOfflineExpectation(
                source_brief_id=source_brief_id,
                expectation_type=expectation_type,
                detected_signals=tuple(
                    sorted(
                        _dedupe(item.detected_signal for item in items),
                        key=lambda item: item.casefold(),
                    )
                ),
                evidence=evidence,
                confidence=confidence,
                planning_implications=(_PLANNING_IMPLICATIONS[expectation_type],),
            )
        )
    return expectations


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
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
        "architecture_notes",
        "implementation_notes",
        "metadata",
        "brief_metadata",
        "source_payload",
    ):
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
            visited.add(field_name)
    for key in sorted(payload, key=lambda item: str(item)):
        if key not in visited:
            _append_value(values, str(key), payload[key])
    return [(field, segment) for field, segment in values if segment]


def _append_value(values: list[tuple[str, str]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            child_field = f"{source_field}.{key}"
            key_text = _clean_text(str(key).replace("_", " "))
            if _any_signal(key_text):
                values.append((child_field, key_text))
            _append_value(values, child_field, child)
        return
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append_value(values, f"{source_field}[{index}]", item)
        return
    if text := _optional_text(value):
        for segment in _segments(text):
            values.append((source_field, segment))


def _segments(value: str) -> list[str]:
    segments: list[str] = []
    for sentence in _SENTENCE_SPLIT_RE.split(value):
        segments.extend(_CLAUSE_SPLIT_RE.split(sentence))
    return [_clean_text(part) for part in segments if _clean_text(part)]


def _expectation_types(text: str) -> tuple[OfflineExpectationType, ...]:
    return tuple(
        expectation_type
        for expectation_type in _EXPECTATION_ORDER
        if _SIGNAL_PATTERNS[expectation_type].search(text)
    )


def _detected_signals(expectation_type: OfflineExpectationType, text: str) -> tuple[str, ...]:
    pattern = _SIGNAL_PATTERNS[expectation_type]
    return tuple(_dedupe(match.group(0).casefold() for match in pattern.finditer(text)))


def _confidence(text: str, source_field: str) -> OfflineExpectationConfidence:
    structured_field = bool(_STRUCTURED_FIELD_RE.search(source_field))
    expectation_count = len(_expectation_types(text))
    if _REQUIRED_RE.search(text) or structured_field or expectation_count > 1:
        return "high"
    if expectation_count == 1:
        return "medium"
    return "low"


def _summary(
    expectations: tuple[SourceOfflineExpectation, ...], source_count: int
) -> dict[str, Any]:
    return {
        "source_count": source_count,
        "expectation_count": len(expectations),
        "expectation_type_counts": {
            expectation_type: sum(
                1
                for expectation in expectations
                if expectation.expectation_type == expectation_type
            )
            for expectation_type in _EXPECTATION_ORDER
        },
        "confidence_counts": {
            confidence: sum(
                1 for expectation in expectations if expectation.confidence == confidence
            )
            for confidence in _CONFIDENCE_ORDER
        },
    }


def _expectation_index(expectation_type: OfflineExpectationType) -> int:
    return _EXPECTATION_ORDER.index(expectation_type)


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
        "implementation_notes",
        "validation_plan",
        "data_requirements",
        "architecture_notes",
    )
    payload = {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }
    return payload


def _any_signal(text: str) -> bool:
    return any(pattern.search(text) for pattern in _SIGNAL_PATTERNS.values())


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    return _SPACE_RE.sub(" ", text).strip()


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


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


__all__ = [
    "OfflineExpectationConfidence",
    "OfflineExpectationType",
    "SourceOfflineExpectation",
    "SourceOfflineExpectationsReport",
    "build_source_offline_expectations",
    "extract_source_offline_expectations",
    "generate_source_offline_expectations",
    "source_offline_expectations_to_dict",
    "source_offline_expectations_to_dicts",
    "source_offline_expectations_to_markdown",
]
