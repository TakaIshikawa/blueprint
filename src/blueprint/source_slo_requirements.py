"""Extract source-level service-level objective requirements from briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping


SloRequirementSignal = Literal[
    "availability",
    "latency",
    "error_budget",
    "measurement_window",
    "alerting",
    "owner",
    "exclusions",
]

_SIGNALS: tuple[SloRequirementSignal, ...] = (
    "availability",
    "latency",
    "error_budget",
    "measurement_window",
    "alerting",
    "owner",
    "exclusions",
)
_SPACE_RE = re.compile(r"\s+")
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_PATTERNS: dict[SloRequirementSignal, re.Pattern[str]] = {
    "availability": re.compile(
        r"\b(?:slo|service level objective|availability|uptime)\b.{0,80}"
        r"(?:99(?:\.\d+)?\s*%|\d+(?:\.\d+)?\s*percent)",
        re.I,
    ),
    "latency": re.compile(
        r"\b(?:latency|response time|p95|p99|percentile)\b.{0,80}"
        r"(?:\d+(?:\.\d+)?\s*(?:ms|milliseconds?|s|sec(?:onds?)?))",
        re.I,
    ),
    "error_budget": re.compile(r"\b(?:error budget|burn rate|budget burn|remaining budget)\b", re.I),
    "measurement_window": re.compile(
        r"\b(?:measurement|rolling|calendar|monthly|weekly|quarterly|window|over)\b.{0,80}"
        r"(?:\d+\s*(?:day|week|month|hour)s?|month|quarter|week)",
        re.I,
    ),
    "alerting": re.compile(r"\b(?:alert|alerting|page|pagerduty|on-call|notify|threshold)\b", re.I),
    "owner": re.compile(
        r"\b(?:owner|owned by|accountable owner|responsible team|owning team|team owns)\b.{0,80}\b\w+",
        re.I,
    ),
    "exclusions": re.compile(
        r"\b(?:exclude|excludes|excluding|exclusion|not counted|do not count|maintenance|third[- ]party|force majeure)\b",
        re.I,
    ),
}
_VALUE_PATTERNS: dict[SloRequirementSignal, re.Pattern[str]] = {
    "availability": re.compile(r"(99(?:\.\d+)?\s*%|\d+(?:\.\d+)?\s*percent)", re.I),
    "latency": re.compile(
        r"((?:p9[059]\s*)?(?:(?:must|shall|should)\s+be\s+)?(?:<=?|under|below|within|exceeds?)?\s*\d+(?:\.\d+)?\s*(?:ms|milliseconds?|s|sec(?:onds?)?))",
        re.I,
    ),
    "measurement_window": re.compile(
        r"((?:rolling|calendar)?\s*\d+\s*(?:day|week|month|hour)s?|monthly|weekly|quarterly)",
        re.I,
    ),
}
_WEAK_RE = re.compile(r"\b(?:fast|reliable|high availability|soon|quickly|monitor|slo)\b", re.I)
_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "requirements",
    "constraints",
    "acceptance_criteria",
    "definition_of_done",
    "metadata",
    "source_payload",
)


@dataclass(frozen=True, slots=True)
class SourceSloRequirement:
    """One extracted SLO requirement signal."""

    signal: SloRequirementSignal
    value: str | None = None
    evidence: tuple[str, ...] = field(default_factory=tuple)
    confidence: Literal["high", "medium"] = "high"

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal": self.signal,
            "value": self.value,
            "evidence": list(self.evidence),
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class SourceSloRequirementsReport:
    """Structured SLO requirement findings and gaps."""

    source_id: str | None = None
    requirements: tuple[SourceSloRequirement, ...] = field(default_factory=tuple)
    missing_signals: tuple[SloRequirementSignal, ...] = field(default_factory=tuple)
    weak_signals: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    @property
    def records(self) -> tuple[SourceSloRequirement, ...]:
        return self.requirements

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_id": self.source_id,
            "requirements": [requirement.to_dict() for requirement in self.requirements],
            "records": [requirement.to_dict() for requirement in self.records],
            "missing_signals": list(self.missing_signals),
            "weak_signals": list(self.weak_signals),
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        return [requirement.to_dict() for requirement in self.requirements]


def build_source_slo_requirements(source: Mapping[str, Any] | str | object) -> SourceSloRequirementsReport:
    """Build a deterministic SLO requirements report from brief-like input."""
    source_id, payload = _payload(source)
    found: dict[SloRequirementSignal, list[str]] = {signal: [] for signal in _SIGNALS}
    values: dict[SloRequirementSignal, str] = {}
    weak: list[str] = []

    for field, text in _texts(payload):
        for signal, pattern in _PATTERNS.items():
            if pattern.search(text):
                found[signal].append(f"{field}: {text}")
                if signal in _VALUE_PATTERNS and signal not in values:
                    if match := _VALUE_PATTERNS[signal].search(text):
                        values[signal] = _clean(match.group(1))
        if (
            field not in {"id", "source_id", "title"}
            and _WEAK_RE.search(text)
            and not any(pattern.search(text) for pattern in _PATTERNS.values())
        ):
            weak.append(f"{field}: clarify measurable SLO target for '{text}'")

    requirements = tuple(
        SourceSloRequirement(signal=signal, value=values.get(signal), evidence=tuple(_dedupe(found[signal]))[:4])
        for signal in _SIGNALS
        if found[signal]
    )
    missing = tuple(signal for signal in _SIGNALS if not found[signal])
    return SourceSloRequirementsReport(
        source_id=source_id,
        requirements=requirements,
        missing_signals=missing,
        weak_signals=tuple(_dedupe(weak)),
        summary={
            "requirement_count": len(requirements),
            "missing_count": len(missing),
            "weak_count": len(tuple(_dedupe(weak))),
            "signals": [requirement.signal for requirement in requirements],
            "signal_counts": {signal: int(bool(found[signal])) for signal in _SIGNALS},
        },
    )


def extract_source_slo_requirements(source: Mapping[str, Any] | str | object) -> tuple[SourceSloRequirement, ...]:
    """Return extracted SLO requirement records."""
    return build_source_slo_requirements(source).requirements


def source_slo_requirements_to_dict(report: SourceSloRequirementsReport) -> dict[str, Any]:
    """Serialize an SLO requirements report."""
    return report.to_dict()


source_slo_requirements_to_dict.__test__ = False


def _payload(source: Mapping[str, Any] | str | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
    elif isinstance(source, Mapping):
        payload = dict(source)
    else:
        payload = {
            key: getattr(source, key)
            for key in dir(source)
            if not key.startswith("_") and not callable(getattr(source, key))
        }
    source_id = _optional(payload.get("id")) or _optional(payload.get("source_id"))
    return source_id, payload


def _texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    texts: list[tuple[str, str]] = []
    seen: set[str] = set()
    for field in _FIELDS:
        if field in payload:
            _append(texts, field, payload[field])
            seen.add(field)
    for field in sorted(payload):
        if field not in seen:
            _append(texts, str(field), payload[field])
    return texts


def _append(texts: list[tuple[str, str]], field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value):
            _append(texts, f"{field}.{key}", value[key])
    elif isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append(texts, f"{field}[{index}]", item)
    elif text := _optional(value):
        for segment in _SENTENCE_RE.split(text):
            cleaned = _clean(segment)
            if cleaned:
                texts.append((field, cleaned))


def _optional(value: Any) -> str | None:
    if value is None:
        return None
    text = _clean(str(value))
    return text or None


def _clean(value: str) -> str:
    return _SPACE_RE.sub(" ", value).strip(" -\t\r\n.")


def _dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = value.casefold()
        if key not in seen:
            seen.add(key)
            result.append(value)
    return result
