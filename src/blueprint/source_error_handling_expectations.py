"""Extract source brief error-handling expectations."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


SourceErrorHandlingExpectationType = Literal[
    "error_handling",
    "fallback",
    "retry",
    "timeout",
    "degraded_mode",
    "validation_error",
    "partial_failure",
    "user_facing_failure",
]
SourceErrorHandlingExpectationTuple = tuple[
    SourceErrorHandlingExpectationType, float, tuple[str, ...]
]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_SPLIT_RE = re.compile(r",\s+(?:and|but|or)\s+", re.I)
_MAX_EVIDENCE_PER_EXPECTATION = 4
_SCANNED_FIELDS: tuple[str, ...] = (
    "title",
    "summary",
    "problem_statement",
    "context",
    "constraints",
    "risks",
    "acceptance_criteria",
    "metadata",
)
_EXPECTATION_ORDER: dict[SourceErrorHandlingExpectationType, int] = {
    "error_handling": 0,
    "fallback": 1,
    "retry": 2,
    "timeout": 3,
    "degraded_mode": 4,
    "validation_error": 5,
    "partial_failure": 6,
    "user_facing_failure": 7,
}
_EXPECTATION_PATTERNS: dict[
    SourceErrorHandlingExpectationType, tuple[re.Pattern[str], ...]
] = {
    "error_handling": (
        re.compile(
            r"\b(?:error handling|handle errors?|handles? failures?|failure behavior|"
            r"failure path|exception handling|resilience requirements?|recover(?:y|able))\b",
            re.I,
        ),
    ),
    "fallback": (
        re.compile(
            r"\b(?:fallback|fall back|backup option|backup path|alternate path|"
            r"secondary provider|default response|safe default)\b",
            re.I,
        ),
    ),
    "retry": (
        re.compile(
            r"\b(?:retry|retries|retried|retrying|backoff|exponential backoff|"
            r"retry budget|idempotent retry|attempts?)\b",
            re.I,
        ),
    ),
    "timeout": (
        re.compile(
            r"\b(?:timeout|timeouts|time out|times out|deadline|request limit|"
            r"response limit|after \d+(?:\.\d+)?\s*(?:ms|s|sec|seconds?|minutes?))\b",
            re.I,
        ),
    ),
    "degraded_mode": (
        re.compile(
            r"\b(?:degraded mode|degrade gracefully|graceful degradation|reduced functionality|"
            r"limited mode|read[- ]only mode|offline mode|best effort|partial service)\b",
            re.I,
        ),
    ),
    "validation_error": (
        re.compile(
            r"\b(?:validation errors?|invalid input|invalid fields?|field errors?|"
            r"input errors?|form errors?|schema errors?|bad request|malformed)\b",
            re.I,
        ),
    ),
    "partial_failure": (
        re.compile(
            r"\b(?:partial failure|partial failures|partial success|partially fail|"
            r"some items fail|failed items|per[- ]item failures?|batch failures?|"
            r"continue processing|skip failed)\b",
            re.I,
        ),
    ),
    "user_facing_failure": (
        re.compile(
            r"\b(?:error message|failure message|user[- ]facing error|friendly error|"
            r"clear error|notify users?|tell users?|user notification|explain the failure)\b",
            re.I,
        ),
    ),
}
_RECOMMENDED_CONTROLS: dict[SourceErrorHandlingExpectationType, tuple[str, ...]] = {
    "error_handling": (
        "Define expected failure paths, logging, ownership, and recovery behavior before implementation.",
    ),
    "fallback": (
        "Document fallback eligibility, activation conditions, and parity limits for the fallback path.",
    ),
    "retry": (
        "Use bounded retries with backoff, jitter where appropriate, and idempotency protections.",
    ),
    "timeout": (
        "Set explicit client and server deadlines with observable timeout metrics and cancellation handling.",
    ),
    "degraded_mode": (
        "Specify the reduced experience, entry and exit conditions, and user or operator communication.",
    ),
    "validation_error": (
        "Return actionable validation feedback while preserving server-side validation and audit visibility.",
    ),
    "partial_failure": (
        "Report per-item outcomes, preserve successful work, and define reconciliation or replay behavior.",
    ),
    "user_facing_failure": (
        "Provide clear user-facing failure states, recovery actions, and support escalation guidance.",
    ),
}
_REVIEW_QUESTIONS: dict[SourceErrorHandlingExpectationType, tuple[str, ...]] = {
    "error_handling": ("Which failures must be handled explicitly, and who owns recovery?",),
    "fallback": ("What conditions activate the fallback, and how is fallback health verified?",),
    "retry": ("Which operations are safe to retry, and what retry budget prevents amplification?",),
    "timeout": ("What deadlines apply at each boundary, and what happens when they expire?",),
    "degraded_mode": ("Which capabilities remain available in degraded mode, and how is recovery detected?",),
    "validation_error": ("Which validation failures need field-level responses or remediation guidance?",),
    "partial_failure": ("How are successful and failed items surfaced, reconciled, or replayed?",),
    "user_facing_failure": ("What should users see and do when the failure occurs?",),
}


@dataclass(frozen=True, slots=True)
class SourceErrorHandlingExpectation:
    """One error-handling expectation found in source brief evidence."""

    expectation_type: SourceErrorHandlingExpectationType
    confidence: float
    evidence: tuple[str, ...] = field(default_factory=tuple)
    recommended_controls: tuple[str, ...] = field(default_factory=tuple)
    review_questions: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "expectation_type": self.expectation_type,
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "recommended_controls": list(self.recommended_controls),
            "review_questions": list(self.review_questions),
        }

    def to_tuple(self) -> SourceErrorHandlingExpectationTuple:
        """Return a compact compatibility tuple."""
        return (self.expectation_type, self.confidence, self.evidence)


@dataclass(frozen=True, slots=True)
class SourceErrorHandlingExpectationsReport:
    """Source-level error-handling expectations report."""

    source_brief_id: str | None = None
    title: str | None = None
    expectations: tuple[SourceErrorHandlingExpectation, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceErrorHandlingExpectation, ...]:
        """Compatibility view matching reports that expose extracted rows as records."""
        return self.expectations

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_brief_id": self.source_brief_id,
            "title": self.title,
            "summary": dict(self.summary),
            "expectations": [expectation.to_dict() for expectation in self.expectations],
            "records": [expectation.to_dict() for expectation in self.records],
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return expectation records as plain dictionaries."""
        return [expectation.to_dict() for expectation in self.expectations]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Error Handling Expectations"
        if self.source_brief_id:
            title = f"{title}: {self.source_brief_id}"
        type_counts = self.summary.get("expectation_type_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Expectations found: {self.summary.get('expectation_count', 0)}",
            f"- Highest confidence: {self.summary.get('highest_confidence', 0.0):.2f}",
            "- Expectation type counts: "
            + (", ".join(f"{key} {type_counts[key]}" for key in sorted(type_counts)) or "none"),
        ]
        if not self.expectations:
            lines.extend(["", "No source error-handling expectations were found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Expectations",
                "",
                "| Expectation Type | Confidence | Evidence | Recommended Controls | Review Questions |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for expectation in self.expectations:
            lines.append(
                "| "
                f"{expectation.expectation_type} | "
                f"{expectation.confidence:.2f} | "
                f"{_markdown_cell('; '.join(expectation.evidence) or 'none')} | "
                f"{_markdown_cell('; '.join(expectation.recommended_controls) or 'none')} | "
                f"{_markdown_cell('; '.join(expectation.review_questions) or 'none')} |"
            )
        return "\n".join(lines)


def build_source_error_handling_expectations_report(
    source: Mapping[str, Any] | SourceBrief | object,
) -> SourceErrorHandlingExpectationsReport:
    """Build an error-handling expectations report from a source brief-like payload."""
    source_brief_id, payload = _source_payload(source)
    candidates = _expectation_candidates(source_brief_id, payload)
    expectations = tuple(_merge_candidates(candidates))
    return SourceErrorHandlingExpectationsReport(
        source_brief_id=source_brief_id,
        title=_optional_text(payload.get("title")),
        expectations=expectations,
        summary=_summary(expectations),
    )


def extract_source_error_handling_expectation_records(
    source: Mapping[str, Any] | SourceBrief | object,
) -> tuple[SourceErrorHandlingExpectation, ...]:
    """Return full error-handling expectation records from brief-shaped input."""
    return build_source_error_handling_expectations_report(source).expectations


def extract_source_error_handling_expectations(
    source: Mapping[str, Any] | SourceBrief | object,
) -> tuple[SourceErrorHandlingExpectationTuple, ...]:
    """Return compact tuple records for callers that only need extracted expectations."""
    return tuple(
        expectation.to_tuple()
        for expectation in build_source_error_handling_expectations_report(source).expectations
    )


def source_error_handling_expectations_report_to_dict(
    report: SourceErrorHandlingExpectationsReport,
) -> dict[str, Any]:
    """Serialize an error-handling expectations report to a plain dictionary."""
    return report.to_dict()


source_error_handling_expectations_report_to_dict.__test__ = False


def source_error_handling_expectations_to_dicts(
    expectations: (
        tuple[SourceErrorHandlingExpectation, ...] | list[SourceErrorHandlingExpectation]
    ),
) -> list[dict[str, Any]]:
    """Serialize error-handling expectation records to dictionaries."""
    return [expectation.to_dict() for expectation in expectations]


source_error_handling_expectations_to_dicts.__test__ = False


def source_error_handling_expectations_report_to_markdown(
    report: SourceErrorHandlingExpectationsReport,
) -> str:
    """Render an error-handling expectations report as Markdown."""
    return report.to_markdown()


source_error_handling_expectations_report_to_markdown.__test__ = False


@dataclass(frozen=True, slots=True)
class _Candidate:
    expectation_type: SourceErrorHandlingExpectationType
    confidence: float
    evidence: str


def _source_payload(source: Mapping[str, Any] | SourceBrief | object) -> tuple[str | None, dict[str, Any]]:
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


def _expectation_candidates(
    source_brief_id: str | None, payload: Mapping[str, Any]
) -> list[_Candidate]:
    del source_brief_id
    candidates: list[_Candidate] = []
    for source_field, segment in _candidate_segments(payload):
        expectation_types = _expectation_types(segment)
        if not expectation_types:
            continue
        evidence = _evidence_snippet(source_field, segment)
        for expectation_type in expectation_types:
            candidates.append(
                _Candidate(
                    expectation_type=expectation_type,
                    confidence=_confidence(segment, source_field),
                    evidence=evidence,
                )
            )
    return candidates


def _merge_candidates(candidates: Iterable[_Candidate]) -> list[SourceErrorHandlingExpectation]:
    grouped: dict[SourceErrorHandlingExpectationType, list[_Candidate]] = {}
    for candidate in candidates:
        grouped.setdefault(candidate.expectation_type, []).append(candidate)

    expectations: list[SourceErrorHandlingExpectation] = []
    for expectation_type in _EXPECTATION_ORDER:
        items = grouped.get(expectation_type, [])
        if not items:
            continue
        evidence = tuple(_dedupe_evidence(item.evidence for item in items))[
            :_MAX_EVIDENCE_PER_EXPECTATION
        ]
        expectations.append(
            SourceErrorHandlingExpectation(
                expectation_type=expectation_type,
                confidence=round(max(item.confidence for item in items), 2),
                evidence=evidence,
                recommended_controls=_RECOMMENDED_CONTROLS[expectation_type],
                review_questions=_REVIEW_QUESTIONS[expectation_type],
            )
        )
    return expectations


def _candidate_segments(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    for field_name in _SCANNED_FIELDS:
        if field_name in payload:
            _append_value(values, field_name, payload[field_name])
    if isinstance(payload.get("source_payload"), Mapping):
        source_payload = payload["source_payload"]
        for field_name in _SCANNED_FIELDS:
            if field_name in source_payload:
                _append_value(values, f"source_payload.{field_name}", source_payload[field_name])
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


def _expectation_types(text: str) -> list[SourceErrorHandlingExpectationType]:
    return [
        expectation_type
        for expectation_type, patterns in _EXPECTATION_PATTERNS.items()
        if any(pattern.search(text) for pattern in patterns)
    ]


def _confidence(text: str, source_field: str) -> float:
    score = 0.62
    normalized_field = source_field.replace("-", "_").casefold()
    if any(
        marker in normalized_field
        for marker in ("acceptance_criteria", "constraint", "risk", "metadata")
    ):
        score += 0.08
    if re.search(r"\b(?:must|shall|required|needs?|ensure|acceptance|done when)\b", text, re.I):
        score += 0.12
    if re.search(r"\b(?:if|when|on|after|during|unless)\b", text, re.I):
        score += 0.04
    if re.search(r"\d", text):
        score += 0.04
    return round(min(score, 0.95), 2)


def _summary(expectations: tuple[SourceErrorHandlingExpectation, ...]) -> dict[str, Any]:
    return {
        "expectation_count": len(expectations),
        "highest_confidence": max(
            (expectation.confidence for expectation in expectations), default=0.0
        ),
        "expectation_types": [expectation.expectation_type for expectation in expectations],
        "expectation_type_counts": {
            expectation_type: sum(
                1
                for expectation in expectations
                if expectation.expectation_type == expectation_type
            )
            for expectation_type in _EXPECTATION_ORDER
        },
    }


def _object_payload(value: object) -> dict[str, Any]:
    fields = (
        "id",
        "source_brief_id",
        "source_id",
        "title",
        "summary",
        "problem_statement",
        "context",
        "constraints",
        "risks",
        "acceptance_criteria",
        "metadata",
        "source_payload",
    )
    return {
        field_name: getattr(value, field_name)
        for field_name in fields
        if hasattr(value, field_name)
    }


def _any_signal(text: str) -> bool:
    return any(
        pattern.search(text)
        for patterns in _EXPECTATION_PATTERNS.values()
        for pattern in patterns
    )


def _optional_text(value: Any) -> str | None:
    if value is None or isinstance(value, (bytes, bytearray)):
        return None
    text = _clean_text(str(value))
    return text or None


def _clean_text(value: str) -> str:
    text = _BULLET_RE.sub("", value.strip())
    return _SPACE_RE.sub(" ", text).strip()


def _evidence_snippet(source_field: str, text: str) -> str:
    value = _clean_text(text)
    if len(value) > 180:
        value = f"{value[:177].rstrip()}..."
    return f"{source_field}: {value}"


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
    return sorted(deduped, key=lambda item: item.casefold())


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
    "SourceErrorHandlingExpectation",
    "SourceErrorHandlingExpectationTuple",
    "SourceErrorHandlingExpectationType",
    "SourceErrorHandlingExpectationsReport",
    "build_source_error_handling_expectations_report",
    "extract_source_error_handling_expectation_records",
    "extract_source_error_handling_expectations",
    "source_error_handling_expectations_report_to_dict",
    "source_error_handling_expectations_report_to_markdown",
    "source_error_handling_expectations_to_dicts",
]
