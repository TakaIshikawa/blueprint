"""Evaluate observability dashboard readiness for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal, Mapping


DashboardReadinessCriterion = Literal[
    "audience",
    "metrics",
    "filters_dimensions",
    "data_source",
    "freshness_slo",
    "alert_linkage",
    "ownership",
    "validation_evidence",
]

_CRITERIA: tuple[DashboardReadinessCriterion, ...] = (
    "audience",
    "metrics",
    "filters_dimensions",
    "data_source",
    "freshness_slo",
    "alert_linkage",
    "ownership",
    "validation_evidence",
)
_SPACE_RE = re.compile(r"\s+")
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_PATTERNS: dict[DashboardReadinessCriterion, re.Pattern[str]] = {
    "audience": re.compile(r"\b(?:audience|for\s+(?:sre|ops|support|product|executives?|on-call|engineers?|team))\b", re.I),
    "metrics": re.compile(r"\b(?:metrics?|latency|error rate|traffic|throughput|saturation|availability|slo|p95|p99|apdex)\b", re.I),
    "filters_dimensions": re.compile(r"\b(?:filters?|dimensions?|group by|breakdown|slice by|service|region|tenant|environment|cohort)\b", re.I),
    "data_source": re.compile(r"\b(?:data source|prometheus|grafana|datadog|new relic|cloudwatch|bigquery|loki|opensearch|table)\b", re.I),
    "freshness_slo": re.compile(r"\b(?:freshness|refresh|updated every|lag|delay|within \d+\s*(?:m|min|minute|s|sec|second)|slo)\b", re.I),
    "alert_linkage": re.compile(r"\b(?:alert|alerting|pagerduty|opsgenie|linked alert|runbook|threshold|page)\b", re.I),
    "ownership": re.compile(r"\b(?:owner|owned by|responsible team|maintainer|on-call team|accountable)\b", re.I),
    "validation_evidence": re.compile(r"\b(?:validate|validation|verify|evidence|screenshot|review|sign[- ]off|test|backfill check)\b", re.I),
}
_GAPS: dict[DashboardReadinessCriterion, str] = {
    "audience": "Name the dashboard audience and primary operational workflow.",
    "metrics": "List the key metrics and expected chart semantics.",
    "filters_dimensions": "Define filters or dimensions such as service, region, tenant, or environment.",
    "data_source": "Identify the telemetry data source or query backend.",
    "freshness_slo": "Set data freshness, refresh cadence, or dashboard SLO expectations.",
    "alert_linkage": "Link dashboard panels to alerts, thresholds, or incident response paths.",
    "ownership": "Assign an owner for dashboard correctness and maintenance.",
    "validation_evidence": "Describe validation evidence such as screenshots, query review, or post-deploy checks.",
}
_FIELDS = (
    "title",
    "description",
    "summary",
    "acceptance_criteria",
    "definition_of_done",
    "validation_plan",
    "notes",
    "metadata",
)


@dataclass(frozen=True, slots=True)
class TaskObservabilityDashboardReadiness:
    """Criterion-level readiness for observability dashboard work."""

    readiness_level: Literal["ready", "partial", "missing"]
    satisfied_criteria: tuple[DashboardReadinessCriterion, ...] = field(default_factory=tuple)
    missing_criteria: tuple[DashboardReadinessCriterion, ...] = field(default_factory=tuple)
    gaps: tuple[str, ...] = field(default_factory=tuple)
    evidence: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "readiness_level": self.readiness_level,
            "satisfied_criteria": list(self.satisfied_criteria),
            "missing_criteria": list(self.missing_criteria),
            "gaps": list(self.gaps),
            "evidence": {key: list(value) for key, value in self.evidence.items()},
        }


def evaluate_task_observability_dashboard_readiness(
    source: Mapping[str, Any] | str | object,
) -> TaskObservabilityDashboardReadiness:
    """Evaluate dashboard task text for expected observability readiness criteria."""
    evidence: dict[DashboardReadinessCriterion, list[str]] = {criterion: [] for criterion in _CRITERIA}
    for field, text in _texts(source):
        for criterion, pattern in _PATTERNS.items():
            if pattern.search(text):
                evidence[criterion].append(f"{field}: {_clean(text)}")

    satisfied = tuple(criterion for criterion in _CRITERIA if evidence[criterion])
    missing = tuple(criterion for criterion in _CRITERIA if not evidence[criterion])
    if not satisfied:
        level: Literal["ready", "partial", "missing"] = "missing"
    elif missing:
        level = "partial"
    else:
        level = "ready"
    return TaskObservabilityDashboardReadiness(
        readiness_level=level,
        satisfied_criteria=satisfied,
        missing_criteria=missing,
        gaps=tuple(_GAPS[criterion] for criterion in missing),
        evidence={criterion: tuple(_dedupe(values)) for criterion, values in evidence.items() if values},
    )


def summarize_task_observability_dashboard_readiness(
    source: Mapping[str, Any] | str | object,
) -> TaskObservabilityDashboardReadiness:
    """Compatibility alias for dashboard readiness evaluation."""
    return evaluate_task_observability_dashboard_readiness(source)


def task_observability_dashboard_readiness_to_dict(
    result: TaskObservabilityDashboardReadiness,
) -> dict[str, Any]:
    """Serialize dashboard readiness to a plain dictionary."""
    return result.to_dict()


task_observability_dashboard_readiness_to_dict.__test__ = False


def _texts(source: Mapping[str, Any] | str | object) -> list[tuple[str, str]]:
    if isinstance(source, str):
        return [("body", source)]
    if hasattr(source, "model_dump"):
        value = source.model_dump(mode="python")
        payload = dict(value) if isinstance(value, Mapping) else {}
    elif isinstance(source, Mapping):
        payload = dict(source)
    else:
        payload = {}
    values: list[tuple[str, str]] = []
    seen: set[str] = set()
    for field in _FIELDS:
        if field in payload:
            _append(values, field, payload[field])
            seen.add(field)
    for field in sorted(payload):
        if field not in seen:
            _append(values, str(field), payload[field])
    return values


def _append(values: list[tuple[str, str]], field: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for key in sorted(value):
            _append(values, f"{field}.{key}", value[key])
    elif isinstance(value, (list, tuple, set)):
        items = sorted(value, key=str) if isinstance(value, set) else value
        for index, item in enumerate(items):
            _append(values, f"{field}[{index}]", item)
    elif value is not None:
        for segment in _SENTENCE_RE.split(str(value)):
            text = _clean(segment)
            if text:
                values.append((field, text))


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
