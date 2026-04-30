"""Extract measurable success metric candidates from implementation briefs."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ImplementationBrief


SuccessMetricType = Literal[
    "threshold",
    "date",
    "count",
    "performance",
    "reliability",
    "vague",
]
MetricConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_FIELD_NAMES = (
    "definition_of_done",
    "validation_plan",
    "scope",
    "risks",
    "architecture_notes",
    "metadata",
    "mvp_goal",
)
_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|(?:\r?\n|;)+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_SPACE_RE = re.compile(r"\s+")
_TOKEN_RE = re.compile(r"[a-z0-9]+")
_NUMBER_RE = re.compile(r"\b\d+(?:\.\d+)?\s*(?:%|ms|s|sec|seconds?|minutes?|hours?)?")
_COMPARATOR_RE = re.compile(
    r"\b(?:at\s+least|at\s+most|under|below|above|over|within|by|before|after|"
    r"no\s+more\s+than|less\s+than|greater\s+than|fewer\s+than|minimum|maximum)\b|[<>]=?",
    re.IGNORECASE,
)
_DATE_RE = re.compile(
    r"\b(?:20\d{2}-\d{2}-\d{2}|"
    r"(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?\s+\d{1,2}(?:,\s*20\d{2})?)\b",
    re.IGNORECASE,
)
_PERCENT_RE = re.compile(r"\b\d+(?:\.\d+)?\s*%")
_PERFORMANCE_RE = re.compile(
    r"\b(?:latency|response\s+time|load\s+time|startup|throughput|p50|p90|p95|p99|"
    r"requests?\s+per\s+second|rps|ms|milliseconds?)\b",
    re.IGNORECASE,
)
_RELIABILITY_RE = re.compile(
    r"\b(?:uptime|availability|error\s+rate|failure\s+rate|crash(?:es)?|incident(?:s)?|"
    r"retries|retry|durable|reliability|slo|sla)\b",
    re.IGNORECASE,
)
_COUNT_RE = re.compile(
    r"\b\d+(?:\.\d+)?\s*(?:[a-z]+\s+){0,2}(?:users?|records?|items?|events?|requests?|tasks?|steps?|"
    r"files?|seconds?|minutes?|hours?|days?)\b",
    re.IGNORECASE,
)
_VAGUE_RE = re.compile(
    r"\b(?:better|fast|faster|easy|easier|simple|seamless|robust|intuitive|delightful|"
    r"improve|improved|optimize|optimized|good|great|high\s+quality|user[-\s]?friendly|"
    r"production[-\s]?ready|scalable)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class BriefSuccessMetricCandidate:
    """One success metric candidate extracted from an implementation brief."""

    source_field: str
    normalized_label: str
    metric_type: SuccessMetricType
    measurement_hint: str
    confidence: MetricConfidence
    vague_or_unverifiable: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "source_field": self.source_field,
            "normalized_label": self.normalized_label,
            "metric_type": self.metric_type,
            "measurement_hint": self.measurement_hint,
            "confidence": self.confidence,
            "vague_or_unverifiable": self.vague_or_unverifiable,
        }


def extract_brief_success_metrics(
    brief: Mapping[str, Any] | ImplementationBrief,
) -> tuple[BriefSuccessMetricCandidate, ...]:
    """Extract deterministic success metric candidates from a brief-like object."""
    payload = _brief_payload(brief)
    candidates: list[BriefSuccessMetricCandidate] = []
    seen: set[str] = set()

    for source_field, text in _candidate_texts(payload):
        metric_type = _metric_type(text)
        vague = metric_type == "vague"
        if metric_type is None:
            continue

        normalized_label = _normalized_label(text)
        if not normalized_label:
            continue

        dedupe_key = _dedupe_key(normalized_label, metric_type)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        candidates.append(
            BriefSuccessMetricCandidate(
                source_field=source_field,
                normalized_label=normalized_label,
                metric_type=metric_type,
                measurement_hint=_measurement_hint(text, metric_type),
                confidence=_confidence(text, metric_type),
                vague_or_unverifiable=vague,
            )
        )

    return tuple(candidates)


def brief_success_metrics_to_dicts(
    metrics: tuple[BriefSuccessMetricCandidate, ...] | list[BriefSuccessMetricCandidate],
) -> list[dict[str, Any]]:
    """Serialize success metric candidates to plain dictionaries."""
    return [metric.to_dict() for metric in metrics]


def _brief_payload(brief: Mapping[str, Any] | ImplementationBrief) -> dict[str, Any]:
    if hasattr(brief, "model_dump"):
        return brief.model_dump(mode="python")
    try:
        return ImplementationBrief.model_validate(brief).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        if isinstance(brief, Mapping):
            return dict(brief)
    return {}


def _candidate_texts(payload: Mapping[str, Any]) -> list[tuple[str, str]]:
    candidates: list[tuple[str, str]] = []
    for field_name in _FIELD_NAMES:
        value = payload.get(field_name)
        if field_name == "metadata":
            _append_metadata(candidates, value, "metadata")
        else:
            _append_value(candidates, value, field_name)
    return candidates


def _append_value(candidates: list[tuple[str, str]], value: Any, source_field: str) -> None:
    if isinstance(value, str):
        for segment in _segments(value):
            candidates.append((source_field, segment))
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            if isinstance(item, str):
                for segment in _segments(item):
                    candidates.append((f"{source_field}[{index}]", segment))


def _append_metadata(candidates: list[tuple[str, str]], value: Any, source_field: str) -> None:
    if not isinstance(value, Mapping):
        return
    for key, item in value.items():
        child_field = f"{source_field}.{key}"
        if _success_metric_key(key):
            _append_metadata_metric(candidates, item, child_field)
        else:
            _append_metadata(candidates, item, child_field)


def _append_metadata_metric(
    candidates: list[tuple[str, str]], value: Any, source_field: str
) -> None:
    if isinstance(value, str):
        for segment in _segments(value):
            candidates.append((source_field, segment))
        return
    if isinstance(value, Mapping):
        text = _metadata_metric_text(value)
        if text:
            candidates.append((source_field, text))
        else:
            _append_metadata(candidates, value, source_field)
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _append_metadata_metric(candidates, item, f"{source_field}[{index}]")


def _metadata_metric_text(value: Mapping[str, Any]) -> str | None:
    label = _clean_text(value.get("label")) or _clean_text(value.get("name"))
    metric = _clean_text(value.get("metric"))
    target = (
        _clean_text(value.get("target"))
        or _clean_text(value.get("threshold"))
        or _clean_text(value.get("measurement"))
    )
    pieces = [piece for piece in (label, metric, target) if piece]
    if pieces:
        return " ".join(pieces)
    return None


def _success_metric_key(key: Any) -> bool:
    if not isinstance(key, str):
        return False
    normalized = "_".join(_TOKEN_RE.findall(key.casefold()))
    return normalized in {
        "success_metric",
        "success_metrics",
        "metric",
        "metrics",
        "outcome",
        "outcomes",
        "kpi",
        "kpis",
        "target",
        "targets",
    }


def _segments(value: str) -> list[str]:
    return [
        text
        for part in _SPLIT_RE.split(value)
        if (text := _clean_text(_BULLET_RE.sub("", part)))
    ]


def _metric_type(text: str) -> SuccessMetricType | None:
    if _PERFORMANCE_RE.search(text):
        return "performance"
    if _RELIABILITY_RE.search(text):
        return "reliability"
    if _DATE_RE.search(text):
        return "date"
    if _COMPARATOR_RE.search(text) and _NUMBER_RE.search(text):
        return "threshold"
    if _PERCENT_RE.search(text):
        return "threshold"
    if _COUNT_RE.search(text):
        return "count"
    if _VAGUE_RE.search(text):
        return "vague"
    return None


def _measurement_hint(text: str, metric_type: SuccessMetricType) -> str:
    if metric_type == "vague":
        return "Needs a concrete observable threshold."

    if metric_type == "date":
        date = _first_match(_DATE_RE, text)
        if date:
            return f"Target date: {date}"
    threshold = _threshold_hint(text)
    if threshold:
        return threshold
    count = _first_match(_COUNT_RE, text) or _first_match(_NUMBER_RE, text)
    if count:
        return f"Track value: {count}"
    return "Track observable outcome."


def _threshold_hint(text: str) -> str | None:
    comparator = _first_match(_COMPARATOR_RE, text)
    number = _first_match(_NUMBER_RE, text)
    if comparator and number:
        return f"Threshold: {comparator} {number}"
    percent = _first_match(_PERCENT_RE, text)
    if percent:
        return f"Threshold: {percent}"
    return None


def _confidence(text: str, metric_type: SuccessMetricType) -> MetricConfidence:
    if metric_type == "vague":
        return "low"
    measurable_signals = sum(
        1
        for pattern in (_COMPARATOR_RE, _NUMBER_RE, _DATE_RE, _PERFORMANCE_RE, _RELIABILITY_RE)
        if pattern.search(text)
    )
    if measurable_signals >= 2:
        return "high"
    return "medium"


def _normalized_label(text: str) -> str:
    text = re.sub(
        r"\b(\d+(?:\.\d+)?)\s+(milliseconds?|ms)\b",
        r"\1ms",
        text,
        flags=re.IGNORECASE,
    )
    tokens = _TOKEN_RE.findall(text.casefold())
    stop_words = {"a", "an", "the", "is", "are", "be", "to", "for", "with", "and"}
    normalized_tokens = [
        _normalize_token(token) for token in tokens if token not in stop_words
    ]
    return " ".join(normalized_tokens)


def _normalize_token(token: str) -> str:
    synonyms = {
        "less": "under",
        "below": "under",
        "fewer": "under",
        "milliseconds": "ms",
        "millisecond": "ms",
        "seconds": "second",
        "users": "user",
        "errors": "error",
        "requests": "request",
    }
    return synonyms.get(token, token)


def _dedupe_key(normalized_label: str, metric_type: SuccessMetricType) -> str:
    tokens = [
        token
        for token in normalized_label.split()
        if token not in {"at", "least", "most", "under", "than", "no", "more"}
    ]
    return f"{metric_type}:{' '.join(tokens)}"


def _first_match(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    if match is None:
        return None
    return _clean_text(match.group(0))


def _clean_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = _SPACE_RE.sub(" ", value).strip()
    return text or None


def _dedupe(values: list[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "BriefSuccessMetricCandidate",
    "MetricConfidence",
    "SuccessMetricType",
    "brief_success_metrics_to_dicts",
    "extract_brief_success_metrics",
]
