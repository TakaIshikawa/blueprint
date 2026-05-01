"""Extract measurable product and engineering outcomes from source briefs."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import SourceBrief


OutcomeConfidence = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_BULLET_RE = re.compile(r"^\s*(?:[-*+]|\d+[.)])\s+")
_CHECKBOX_RE = re.compile(r"^\s*(?:[-*+]\s*)?\[[ xX]\]\s+")
_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CONFIDENCE_ORDER: dict[OutcomeConfidence, int] = {"high": 0, "medium": 1, "low": 2}
_MISSING_DETAIL_ORDER = (
    "missing_baseline",
    "missing_target",
    "missing_measurement_window",
    "ambiguous_metric_definition",
)
_TOP_LEVEL_FIELDS = (
    "title",
    "summary",
    "body",
    "description",
    "markdown",
    "problem",
    "problem_statement",
    "goals",
    "success_metrics",
    "success_criteria",
    "acceptance_criteria",
    "constraints",
)
_SOURCE_PAYLOAD_FIELDS = (
    "outcomes",
    "measurable_outcomes",
    "success_metrics",
    "success_criteria",
    "kpis",
    "metrics",
    "goals",
    "acceptance_criteria",
    "requirements",
    "body",
    "description",
    "markdown",
)
_STRUCTURED_OUTCOME_KEYS = {
    "outcome",
    "outcomes",
    "measurable_outcome",
    "measurable_outcomes",
    "success_metric",
    "success_metrics",
    "success_criteria",
    "metric",
    "metrics",
    "kpi",
    "kpis",
    "measure",
    "measurement",
}
_METRIC_KEY_RE = re.compile(r"\b(?:metric|kpi|measure|outcome|success criteria|name)\b", re.I)
_BASELINE_KEY_RE = re.compile(r"\b(?:baseline|current|today|before|from|starting)\b", re.I)
_TARGET_KEY_RE = re.compile(r"\b(?:target|goal|desired|to|after|increase|decrease|reduce)\b", re.I)
_THRESHOLD_KEY_RE = re.compile(r"\b(?:threshold|limit|minimum|maximum|min|max|sla|slo)\b", re.I)
_WINDOW_KEY_RE = re.compile(r"\b(?:window|timeframe|period|by|within|over|weekly|monthly)\b", re.I)
_OWNER_KEY_RE = re.compile(r"\b(?:owner|dri|responsible|team|squad|lead)\b", re.I)
_VALUE_RE = re.compile(
    r"(?:[$€£]\s?\d[\d,.]*(?:\s?[kmb])?|\b\d+(?:\.\d+)?\s?"
    r"(?:percent|pp|bps|ms|milliseconds?|s|sec(?:onds?)?|minutes?|hours?|days?|weeks?|"
    r"months?|users?|customers?|requests?|rps|qps|errors?|tickets?|incidents?|signups?|"
    r"conversions?|sessions?|points?|stars?|x)\b|[<>]=?\s?\d+(?:\.\d+)?|"
    r"\b\d+(?:\.\d+)?\s?%|\b\d+(?:\.\d+)?\s?(?:k|m|b)\b)",
    re.I,
)
_METRIC_WORD_RE = re.compile(
    r"\b(?:activation|adoption|conversion|retention|churn|abandonment|latency|response time|"
    r"load time|error rate|failure rate|success rate|uptime|availability|nps|csat|ces|"
    r"satisfaction|revenue|cost|spend|tickets?|incidents?|defects?|bugs?|throughput|"
    r"engagement|usage|quality|performance|reliability|sla|slo|p95|p99)\b",
    re.I,
)
_FUZZY_METRIC_RE = re.compile(
    r"\b(?:engagement|usage|quality|performance|reliability|experience|better|faster|"
    r"improve|improved|successful|healthy|good)\b",
    re.I,
)
_TARGET_RE = re.compile(
    r"\b(?:target|goal|to|increase(?:d)? to|decrease(?:d)? to|reduce(?:d)? to|lower(?:ed)? to|"
    r"raise(?:d)? to|at least|at most|no more than|under|below|above|over|less than|"
    r"greater than|hit|reach|achieve|maintain|keep)\s*[:=]?\s*([^.;,\n]+)",
    re.I,
)
_BASELINE_RE = re.compile(
    r"\b(?:baseline|current(?:ly)?|today|as-is|before|from|starting at)\s*[:=]?\s*"
    r"([^.;,\n]+)",
    re.I,
)
_THRESHOLD_RE = re.compile(
    r"\b(?:threshold|limit|minimum|maximum|min|max|sla|slo)\s*[:=]?\s*([^.;,\n]+)",
    re.I,
)
_WINDOW_RE = re.compile(
    r"\b(?:within|over|during|for|in the first|by|before|after)\s+"
    r"([^.;,\n]*(?:day|week|month|quarter|q[1-4]|year|launch|release|rollout|sprint)[^.;,\n]*)|"
    r"\b(?:daily|weekly|monthly|quarterly|annually|per day|per week|per month)\b",
    re.I,
)
_OWNER_RE = re.compile(
    r"\b(?:owned by|owner|dri|responsible team|team|squad|lead)\s*[:=]?\s*"
    r"(@?[A-Za-z][A-Za-z0-9 _./-]{1,60})",
    re.I,
)
_METRIC_FROM_VERB_RE = re.compile(
    r"\b(?:increase|decrease|reduce|lower|raise|improve|cut|keep|maintain|achieve|hit|"
    r"reach)\s+(?P<metric>[A-Za-z][A-Za-z0-9 /_-]{2,80}?)(?=\s+(?:from|to|by|under|"
    r"below|above|over|within|during|for|owned by|owner|dri)\b|[:=]|$)",
    re.I,
)
_METRIC_BEFORE_MODAL_RE = re.compile(
    r"(?P<metric>[A-Za-z][A-Za-z0-9 /_-]{2,80}?)\s+(?:must|should|shall|needs? to|will)\s+"
    r"(?:stay|be|remain|reach|hit|improve|drop|fall|rise)",
    re.I,
)


@dataclass(frozen=True, slots=True)
class SourceMeasurableOutcome:
    """One source-backed measurable outcome candidate."""

    metric_name: str
    source_field: str
    evidence: str
    baseline: str | None = None
    target: str | None = None
    threshold: str | None = None
    measurement_window: str | None = None
    owner_hint: str | None = None
    missing_baseline: bool = False
    missing_target: bool = False
    missing_measurement_window: bool = False
    ambiguous_metric_definition: bool = False
    confidence: OutcomeConfidence = "medium"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "metric_name": self.metric_name,
            "source_field": self.source_field,
            "evidence": self.evidence,
            "baseline": self.baseline,
            "target": self.target,
            "threshold": self.threshold,
            "measurement_window": self.measurement_window,
            "owner_hint": self.owner_hint,
            "missing_baseline": self.missing_baseline,
            "missing_target": self.missing_target,
            "missing_measurement_window": self.missing_measurement_window,
            "ambiguous_metric_definition": self.ambiguous_metric_definition,
            "confidence": self.confidence,
        }


@dataclass(frozen=True, slots=True)
class SourceMeasurableOutcomeReport:
    """Brief-level measurable outcome extraction report."""

    brief_id: str | None = None
    outcomes: tuple[SourceMeasurableOutcome, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "brief_id": self.brief_id,
            "outcomes": [outcome.to_dict() for outcome in self.outcomes],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return measurable outcome records as plain dictionaries."""
        return [outcome.to_dict() for outcome in self.outcomes]

    def to_markdown(self) -> str:
        """Render the report as deterministic Markdown."""
        title = "# Source Measurable Outcomes Report"
        if self.brief_id:
            title = f"{title}: {self.brief_id}"
        confidence_counts = self.summary.get("confidence_counts", {})
        lines = [
            title,
            "",
            "## Summary",
            "",
            f"- Outcomes found: {self.summary.get('outcome_count', 0)}",
            "- Confidence counts: "
            f"high {confidence_counts.get('high', 0)}, "
            f"medium {confidence_counts.get('medium', 0)}, "
            f"low {confidence_counts.get('low', 0)}",
            f"- Missing baselines: {self.summary.get('missing_baseline_count', 0)}",
            f"- Missing targets: {self.summary.get('missing_target_count', 0)}",
            f"- Missing measurement windows: {self.summary.get('missing_measurement_window_count', 0)}",
            f"- Ambiguous metric definitions: {self.summary.get('ambiguous_metric_count', 0)}",
        ]
        if not self.outcomes:
            lines.extend(["", "No measurable outcomes were found in the source brief."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "## Outcomes",
                "",
                "| Metric | Source | Baseline | Target | Threshold | Window | Owner | Flags | Evidence |",
                "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for outcome in self.outcomes:
            lines.append(
                "| "
                f"{_markdown_cell(outcome.metric_name)} | "
                f"{_markdown_cell(outcome.source_field)} | "
                f"{_markdown_cell(outcome.baseline or 'missing')} | "
                f"{_markdown_cell(outcome.target or 'missing')} | "
                f"{_markdown_cell(outcome.threshold or 'none')} | "
                f"{_markdown_cell(outcome.measurement_window or 'missing')} | "
                f"{_markdown_cell(outcome.owner_hint or 'none')} | "
                f"{_markdown_cell(', '.join(_flags(outcome)) or 'none')} | "
                f"{_markdown_cell(outcome.evidence)} |"
            )
        return "\n".join(lines)


def build_source_measurable_outcomes(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> SourceMeasurableOutcomeReport:
    """Extract source-brief measurable outcomes and measurement detail gaps."""
    brief_id, payload = _source_payload(source)
    candidates: list[SourceMeasurableOutcome] = []
    for source_field, value in _candidate_values(payload):
        candidates.extend(_structured_outcomes(value, source_field))
        if isinstance(value, str):
            candidates.extend(_text_outcomes(value, source_field))

    outcomes = tuple(
        sorted(
            _dedupe_outcomes(candidates),
            key=lambda outcome: (
                _CONFIDENCE_ORDER[outcome.confidence],
                outcome.metric_name.casefold(),
                outcome.source_field,
                outcome.evidence.casefold(),
            ),
        )
    )
    return SourceMeasurableOutcomeReport(
        brief_id=brief_id,
        outcomes=outcomes,
        summary=_summary(outcomes),
    )


def extract_source_measurable_outcomes(
    source: Mapping[str, Any] | SourceBrief | str | object,
) -> tuple[SourceMeasurableOutcome, ...]:
    """Return measurable outcome records from source brief objects, dictionaries, or text."""
    return build_source_measurable_outcomes(source).outcomes


def summarize_source_measurable_outcomes(
    source_or_outcomes: (
        Mapping[str, Any]
        | SourceBrief
        | str
        | tuple[SourceMeasurableOutcome, ...]
        | list[SourceMeasurableOutcome]
        | object
    ),
) -> SourceMeasurableOutcomeReport:
    """Build a measurable outcome report, accepting records as an already extracted source."""
    if _looks_like_outcomes(source_or_outcomes):
        outcomes = tuple(source_or_outcomes)  # type: ignore[arg-type]
        return SourceMeasurableOutcomeReport(outcomes=outcomes, summary=_summary(outcomes))
    return build_source_measurable_outcomes(source_or_outcomes)


def source_measurable_outcomes_to_dicts(
    outcomes: tuple[SourceMeasurableOutcome, ...] | list[SourceMeasurableOutcome],
) -> list[dict[str, Any]]:
    """Serialize measurable outcome records to dictionaries."""
    return [outcome.to_dict() for outcome in outcomes]


def source_measurable_outcomes_report_to_dict(
    report: SourceMeasurableOutcomeReport,
) -> dict[str, Any]:
    """Serialize a measurable outcomes report to a plain dictionary."""
    return report.to_dict()


source_measurable_outcomes_report_to_dict.__test__ = False


def source_measurable_outcomes_to_dict(
    report: SourceMeasurableOutcomeReport,
) -> dict[str, Any]:
    """Serialize a measurable outcomes report to a plain dictionary."""
    return report.to_dict()


source_measurable_outcomes_to_dict.__test__ = False


def source_measurable_outcomes_to_markdown(
    report: SourceMeasurableOutcomeReport,
) -> str:
    """Render a measurable outcomes report as Markdown."""
    return report.to_markdown()


source_measurable_outcomes_to_markdown.__test__ = False


def source_measurable_outcomes_report_to_markdown(
    report: SourceMeasurableOutcomeReport,
) -> str:
    """Render a measurable outcomes report as Markdown."""
    return report.to_markdown()


source_measurable_outcomes_report_to_markdown.__test__ = False


def _structured_outcomes(value: Any, source_field: str) -> list[SourceMeasurableOutcome]:
    outcomes: list[SourceMeasurableOutcome] = []
    if isinstance(value, Mapping):
        if _has_outcome_shape(value):
            outcome = _structured_outcome(value, source_field)
            if outcome:
                outcomes.append(outcome)
        for key in sorted(value, key=lambda item: str(item)):
            child_field = f"{source_field}.{key}"
            outcomes.extend(_structured_outcomes(value[key], child_field))
        return outcomes
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            outcomes.extend(_structured_outcomes(item, f"{source_field}[{index}]"))
    return outcomes


def _structured_outcome(item: Mapping[str, Any], source_field: str) -> SourceMeasurableOutcome | None:
    metric = _first_key_text(item, _METRIC_KEY_RE)
    baseline = _first_key_text(item, _BASELINE_KEY_RE)
    target = _first_key_text(item, _TARGET_KEY_RE)
    threshold = _first_key_text(item, _THRESHOLD_KEY_RE)
    window = _first_key_text(item, _WINDOW_KEY_RE)
    owner = _first_key_text(item, _OWNER_KEY_RE)
    evidence = _structured_evidence(item)
    if not metric:
        metric = _metric_name(evidence)
    if not metric or not (_VALUE_RE.search(evidence) or target or baseline or threshold):
        return None
    return _outcome(
        metric_name=metric,
        source_field=source_field,
        evidence=evidence,
        baseline=baseline or _baseline(evidence),
        target=target or (None if threshold else _target(evidence)),
        threshold=threshold or _threshold(evidence),
        measurement_window=window or _measurement_window(evidence),
        owner_hint=owner or _owner(evidence),
    )


def _text_outcomes(value: str, source_field: str) -> list[SourceMeasurableOutcome]:
    outcomes: list[SourceMeasurableOutcome] = []
    for segment in _segments(value):
        if not _looks_measurable(segment):
            continue
        metric = _metric_name(segment)
        if not metric:
            continue
        outcomes.append(
            _outcome(
                metric_name=metric,
                source_field=source_field,
                evidence=segment,
                baseline=_baseline(segment),
                target=_target_without_threshold(segment),
                threshold=_threshold(segment),
                measurement_window=_measurement_window(segment),
                owner_hint=_owner(segment),
            )
        )
    return outcomes


def _outcome(
    *,
    metric_name: str,
    source_field: str,
    evidence: str,
    baseline: str | None,
    target: str | None,
    threshold: str | None,
    measurement_window: str | None,
    owner_hint: str | None,
) -> SourceMeasurableOutcome:
    metric = _clean_metric_name(metric_name)
    baseline = _detail(baseline)
    target = _detail(target)
    threshold = _detail(threshold)
    window = _detail(measurement_window)
    owner = _detail(owner_hint)
    missing_baseline = baseline is None
    missing_target = target is None and threshold is None
    missing_window = window is None
    ambiguous = _ambiguous_metric(metric, evidence)
    return SourceMeasurableOutcome(
        metric_name=metric,
        source_field=source_field,
        evidence=_snippet(evidence),
        baseline=baseline,
        target=target,
        threshold=threshold,
        measurement_window=window,
        owner_hint=owner,
        missing_baseline=missing_baseline,
        missing_target=missing_target,
        missing_measurement_window=missing_window,
        ambiguous_metric_definition=ambiguous,
        confidence=_confidence(missing_baseline, missing_target, missing_window, ambiguous),
    )


def _candidate_values(payload: Mapping[str, Any]) -> list[tuple[str, Any]]:
    candidates: list[tuple[str, Any]] = []
    for field_name in _TOP_LEVEL_FIELDS:
        if field_name in payload:
            _append_value(candidates, field_name, payload[field_name])

    source_payload = payload.get("source_payload")
    if isinstance(source_payload, Mapping):
        visited: set[str] = set()
        for field_name in _SOURCE_PAYLOAD_FIELDS:
            if field_name in source_payload:
                source_field = f"source_payload.{field_name}"
                _append_value(candidates, source_field, source_payload[field_name])
                visited.add(source_field)
        for source_field, value in _flatten_payload(source_payload, prefix="source_payload"):
            if _is_under_visited_field(source_field, visited):
                continue
            _append_value(candidates, source_field, value)
    elif payload and not candidates:
        for source_field, value in _flatten_payload(payload, prefix="source"):
            _append_value(candidates, source_field, value)
    return candidates


def _append_value(candidates: list[tuple[str, Any]], source_field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        candidates.append((source_field, value))
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _append_value(candidates, f"{source_field}[{index}]", item)
        return
    if isinstance(value, str) and value.strip():
        candidates.append((source_field, value))


def _flatten_payload(value: Any, *, prefix: str) -> list[tuple[str, Any]]:
    flattened: list[tuple[str, Any]] = []

    def append(current: Any, path: str) -> None:
        if isinstance(current, Mapping):
            for key in sorted(current, key=lambda item: str(item)):
                append(current[key], f"{path}.{key}")
            return
        if isinstance(current, (list, tuple)):
            for index, item in enumerate(current):
                append(item, f"{path}[{index}]")
            return
        flattened.append((path, current))

    append(value, prefix)
    return flattened


def _segments(value: str) -> tuple[str, ...]:
    segments: list[str] = []
    for line in value.splitlines():
        line_text = _clean_text(line)
        if not line_text:
            continue
        if _BULLET_RE.match(line) or _CHECKBOX_RE.match(line):
            segments.append(line_text)
            continue
        for part in _SENTENCE_SPLIT_RE.split(line):
            text = _clean_text(part)
            if text:
                segments.append(text)
    return tuple(segments)


def _looks_measurable(text: str) -> bool:
    return bool(_METRIC_WORD_RE.search(text) and (_VALUE_RE.search(text) or _FUZZY_METRIC_RE.search(text)))


def _metric_name(text: str) -> str | None:
    if match := _METRIC_FROM_VERB_RE.search(text):
        return match.group("metric")
    if match := _METRIC_BEFORE_MODAL_RE.search(text):
        return match.group("metric")
    if match := _METRIC_WORD_RE.search(text):
        return match.group(0)
    return None


def _baseline(text: str) -> str | None:
    return _detail_match(_BASELINE_RE, text)


def _target(text: str) -> str | None:
    return _detail_match(_TARGET_RE, text)


def _target_without_threshold(text: str) -> str | None:
    if _threshold(text):
        return None
    return _target(text)


def _threshold(text: str) -> str | None:
    if match := re.search(
        r"\b(?:keep|stay|remain|maintain)\b.*?\b"
        r"((?:under|below|above|over|at least|at most|no more than|less than|greater than)\s+"
        r"[$€£]?\s?\d+(?:\.\d+)?\s?(?:%|percent|ms|milliseconds?|s|sec(?:onds?)?|"
        r"minutes?|hours?|days?|weeks?|months?|x)?)",
        text,
        re.I,
    ):
        return _detail(match.group(1))
    return _detail_match(_THRESHOLD_RE, text)


def _measurement_window(text: str) -> str | None:
    if match := _WINDOW_RE.search(text):
        return _clean_text(match.group(1) or match.group(0))
    return None


def _owner(text: str) -> str | None:
    return _detail_match(_OWNER_RE, text)


def _detail_match(pattern: re.Pattern[str], text: str) -> str | None:
    if not (match := pattern.search(text)):
        return None
    value = _clean_text(match.group(1))
    value = re.sub(
        r"\b(?:to|by|within|over|during|for|owned by|owner|dri)\b.*$",
        "",
        value,
        flags=re.I,
    )
    return _detail(value)


def _detail(value: Any) -> str | None:
    text = _clean_text(str(value)) if value is not None else ""
    text = text.strip("`'\" ;,.")
    if not text:
        return None
    return text[:120].rstrip()


def _has_outcome_shape(item: Mapping[str, Any]) -> bool:
    keys = {str(key).casefold().replace("-", "_").replace(" ", "_") for key in item}
    if keys & _STRUCTURED_OUTCOME_KEYS:
        return True
    key_text = " ".join(keys).replace("_", " ")
    return bool(
        _METRIC_KEY_RE.search(key_text)
        and (_TARGET_KEY_RE.search(key_text) or _BASELINE_KEY_RE.search(key_text))
    )


def _first_key_text(item: Mapping[str, Any], pattern: re.Pattern[str]) -> str | None:
    for key in sorted(item, key=lambda value: str(value)):
        key_text = str(key).replace("_", " ").replace("-", " ")
        if pattern.search(key_text):
            if text := _value_text(item[key]):
                return text
    return None


def _value_text(value: Any) -> str | None:
    if isinstance(value, Mapping):
        return None
    if isinstance(value, (list, tuple, set)):
        text = "; ".join(_strings(value))
    else:
        text = _clean_text(str(value)) if value is not None else ""
    return text or None


def _structured_evidence(item: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for key in sorted(item, key=lambda value: str(value)):
        value = item[key]
        if isinstance(value, Mapping):
            continue
        text = "; ".join(_strings(value)) if isinstance(value, (list, tuple, set)) else _clean_text(str(value))
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


def _source_payload(source: Mapping[str, Any] | SourceBrief | str | object) -> tuple[str | None, dict[str, Any]]:
    if isinstance(source, str):
        return None, {"body": source}
    if isinstance(source, SourceBrief):
        value = source.model_dump(mode="python")
        return _optional_text(value.get("id")), dict(value)
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        if isinstance(value, Mapping):
            return _optional_text(value.get("id")), dict(value)
        return None, {}
    try:
        value = SourceBrief.model_validate(source).model_dump(mode="python")
        return _optional_text(value.get("id")), dict(value)
    except (TypeError, ValueError, ValidationError):
        if isinstance(source, Mapping):
            return _optional_text(source.get("id")), dict(source)
    return None, {}


def _summary(outcomes: tuple[SourceMeasurableOutcome, ...]) -> dict[str, Any]:
    confidence_counts = {
        confidence: sum(1 for outcome in outcomes if outcome.confidence == confidence)
        for confidence in _CONFIDENCE_ORDER
    }
    return {
        "outcome_count": len(outcomes),
        "confidence_counts": confidence_counts,
        "missing_baseline_count": sum(1 for outcome in outcomes if outcome.missing_baseline),
        "missing_target_count": sum(1 for outcome in outcomes if outcome.missing_target),
        "missing_measurement_window_count": sum(
            1 for outcome in outcomes if outcome.missing_measurement_window
        ),
        "ambiguous_metric_count": sum(
            1 for outcome in outcomes if outcome.ambiguous_metric_definition
        ),
        "owner_hint_count": sum(1 for outcome in outcomes if outcome.owner_hint),
        "metrics": [outcome.metric_name for outcome in outcomes],
        "missing_details": {
            detail: sum(1 for outcome in outcomes if getattr(outcome, detail))
            for detail in _MISSING_DETAIL_ORDER
        },
    }


def _confidence(
    missing_baseline: bool,
    missing_target: bool,
    missing_window: bool,
    ambiguous: bool,
) -> OutcomeConfidence:
    missing_count = sum((missing_baseline, missing_target, missing_window, ambiguous))
    if missing_count == 0:
        return "high"
    if missing_count <= 2 and not ambiguous:
        return "medium"
    return "low"


def _ambiguous_metric(metric_name: str, evidence: str) -> bool:
    metric_words = re.findall(r"[A-Za-z0-9]+", metric_name)
    if len(metric_words) < 2 and _FUZZY_METRIC_RE.search(metric_name):
        return True
    if _FUZZY_METRIC_RE.fullmatch(metric_name.casefold()):
        return True
    return bool(_FUZZY_METRIC_RE.search(evidence) and not _VALUE_RE.search(evidence))


def _dedupe_outcomes(outcomes: Iterable[SourceMeasurableOutcome]) -> list[SourceMeasurableOutcome]:
    selected: dict[str, SourceMeasurableOutcome] = {}
    for outcome in outcomes:
        key = _dedupe_key(outcome)
        current = selected.get(key)
        if current is None or _record_score(outcome) > _record_score(current):
            selected[key] = outcome
    return list(selected.values())


def _dedupe_key(outcome: SourceMeasurableOutcome) -> str:
    target = outcome.target or outcome.threshold or ""
    return re.sub(r"[^a-z0-9]+", " ", f"{outcome.metric_name} {target}".casefold()).strip()


def _record_score(outcome: SourceMeasurableOutcome) -> tuple[int, int, str]:
    completeness = sum(
        1
        for value in (
            outcome.baseline,
            outcome.target,
            outcome.threshold,
            outcome.measurement_window,
            outcome.owner_hint,
        )
        if value
    )
    confidence_score = 2 - _CONFIDENCE_ORDER[outcome.confidence]
    return (completeness, confidence_score, outcome.source_field)


def _flags(outcome: SourceMeasurableOutcome) -> tuple[str, ...]:
    flags = []
    if outcome.missing_baseline:
        flags.append("missing baseline")
    if outcome.missing_target:
        flags.append("missing target")
    if outcome.missing_measurement_window:
        flags.append("missing measurement window")
    if outcome.ambiguous_metric_definition:
        flags.append("ambiguous metric definition")
    return tuple(flags)


def _clean_metric_name(value: str) -> str:
    text = _clean_text(value)
    text = re.sub(r"\b(?:the|a|an|our|their)\b", "", text, flags=re.I)
    text = re.sub(r"\s+", " ", text).strip(" :;-")
    return text[:80].rstrip() or "metric"


def _clean_text(value: str) -> str:
    text = _CHECKBOX_RE.sub("", value.strip())
    text = _BULLET_RE.sub("", text)
    text = re.sub(r"^#+\s*", "", text)
    return _SPACE_RE.sub(" ", text).strip()


def _snippet(text: str) -> str:
    cleaned = _clean_text(text)
    if len(cleaned) <= 200:
        return cleaned
    return f"{cleaned[:197].rstrip()}..."


def _optional_text(value: Any) -> str | None:
    text = _clean_text(str(value)) if value is not None else ""
    return text or None


def _markdown_cell(value: str) -> str:
    return _clean_text(value).replace("|", "\\|").replace("\n", " ")


def _is_under_visited_field(source_field: str, visited_fields: set[str]) -> bool:
    return any(
        source_field == visited
        or source_field.startswith(f"{visited}.")
        or source_field.startswith(f"{visited}[")
        for visited in visited_fields
    )


def _looks_like_outcomes(value: Any) -> bool:
    if not isinstance(value, (list, tuple)):
        return False
    return all(isinstance(item, SourceMeasurableOutcome) for item in value)


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "OutcomeConfidence",
    "SourceMeasurableOutcome",
    "SourceMeasurableOutcomeReport",
    "build_source_measurable_outcomes",
    "extract_source_measurable_outcomes",
    "source_measurable_outcomes_report_to_dict",
    "source_measurable_outcomes_report_to_markdown",
    "source_measurable_outcomes_to_dict",
    "source_measurable_outcomes_to_dicts",
    "source_measurable_outcomes_to_markdown",
    "summarize_source_measurable_outcomes",
]
