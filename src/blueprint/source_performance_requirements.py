"""Extract performance requirements from source brief data."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for performance requirements concepts
_RESPONSE_TIME_RE = re.compile(
    r"\b(?:response[_\s]+time(?:s)?[_\s]+(?:target(?:s)?|requirement(?:s)?|objective(?:s)?|limit(?:s)?|budget)|"
    r"(?:target|maximum|max)[_\s]+response[_\s]+time|"
    r"latency[_\s]+(?:target|requirement|budget|limit)|"
    r"(?:p95|p99|percentile)[_\s]+(?:latency|response[_\s]+time)|"
    r"(?:server|api|page)[_\s]+response[_\s]+time|"
    r"time[_\s]+to[_\s]+first[_\s]+byte|ttfb)\b",
    re.I,
)
_THROUGHPUT_RE = re.compile(
    r"\b(?:throughput[_\s]+(?:target(?:s)?|requirement(?:s)?|objective(?:s)?)|"
    r"requests?[_\s]+per[_\s]+second|rps|qps|"
    r"queries?[_\s]+per[_\s]+second|"
    r"transactions?[_\s]+per[_\s]+(?:second|minute)|tps|tpm|"
    r"(?:maximum|peak|sustained)[_\s]+(?:throughput|load|traffic))\b",
    re.I,
)
_CONCURRENCY_RE = re.compile(
    r"\b(?:concurrency[_\s]+(?:level(?:s)?|requirement(?:s)?|target(?:s)?)|"
    r"concurrent[_\s]+(?:users?|requests?|connections?|sessions?)|"
    r"(?:maximum|peak|simultaneous)[_\s]+(?:users?|connections?|requests?)|"
    r"active[_\s]+users?[_\s]+(?:count|limit)|"
    r"connection[_\s]+pool[_\s]+size)\b",
    re.I,
)
_RESOURCE_UTILIZATION_RE = re.compile(
    r"\b(?:resource[_\s]+(?:utilization|usage|consumption|limit(?:s)?)|"
    r"(?:cpu|memory|disk)[_\s]+(?:usage|utilization|limit|budget)|"
    r"(?:maximum|peak)[_\s]+(?:cpu|memory|resource)[_\s]+usage|"
    r"resource[_\s]+constraint(?:s)?|"
    r"(?:memory|cpu)[_\s]+footprint|heap[_\s]+size)\b",
    re.I,
)
_LOAD_TESTING_RE = re.compile(
    r"\b(?:load[_\s]+test(?:ing)?[_\s]+(?:strategy|plan|requirement(?:s)?|approach)|"
    r"performance[_\s]+test(?:ing)?|stress[_\s]+test(?:ing)?|"
    r"capacity[_\s]+test(?:ing)?|soak[_\s]+test(?:ing)?|"
    r"spike[_\s]+test(?:ing)?|endurance[_\s]+test(?:ing)?|"
    r"benchmark(?:ing)?[_\s]+(?:suite|test(?:s)?|plan))\b",
    re.I,
)
_PERFORMANCE_BUDGETS_RE = re.compile(
    r"\b(?:performance[_\s]+budget(?:s)?|"
    r"(?:latency|response[_\s]+time)[_\s]+budget(?:s)?|"
    r"resource[_\s]+budget(?:s)?|"
    r"performance[_\s]+threshold(?:s)?|"
    r"performance[_\s]+(?:target(?:s)?|goal(?:s)?|objective(?:s)?))\b",
    re.I,
)
_OPTIMIZATION_PRIORITIES_RE = re.compile(
    r"\b(?:optimization[_\s]+(?:priorit(?:y|ies)|target(?:s)?|area(?:s)?)|"
    r"optimize[_\s]+(?:for|priority)|"
    r"performance[_\s]+optimization|"
    r"(?:prioritize|focus[_\s]+on)[_\s]+(?:latency|throughput|resource[_\s]+usage)|"
    r"critical[_\s]+performance[_\s]+path)\b",
    re.I,
)
_SCALABILITY_TARGETS_RE = re.compile(
    r"\b(?:scalability[_\s]+(?:target(?:s)?|requirement(?:s)?|goal(?:s)?)|"
    r"scale[_\s]+to[_\s]+(?:\d+|millions?|thousands?)|"
    r"(?:horizontal|vertical)[_\s]+scal(?:e|ing|ability)|"
    r"auto[_\s-]*scal(?:e|ing)|elastic[_\s]+scal(?:e|ing)|"
    r"scale[_\s]+(?:up|down|out|in)[_\s]+(?:strategy|capability))\b",
    re.I,
)
_METRIC_CLARITY_RE = re.compile(
    r"\b(?:performance[_\s]+metric(?:s)?|"
    r"(?:measure|track|monitor)[_\s]+(?:latency|throughput|response[_\s]+time)|"
    r"(?:key[_\s]+)?performance[_\s]+indicator(?:s)?|kpi(?:s)?|"
    r"metric(?:s)?[_\s]+(?:definition|specification|collection)|"
    r"sli(?:s)?|service[_\s]+level[_\s]+indicator(?:s)?)\b",
    re.I,
)
_SLO_ALIGNMENT_RE = re.compile(
    r"\b(?:slo(?:s)?|service[_\s]+level[_\s]+objective(?:s)?|"
    r"sla(?:s)?|service[_\s]+level[_\s]+agreement(?:s)?|"
    r"availability[_\s]+(?:target|requirement|objective)|"
    r"uptime[_\s]+(?:target|requirement|objective)|"
    r"reliability[_\s]+(?:target|requirement|objective)|"
    r"performance[_\s]+slo)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class PerformanceRequirements:
    """Performance requirements extracted from source brief."""

    response_time_targets_specified: bool = False
    throughput_requirements_defined: bool = False
    concurrency_levels_identified: bool = False
    resource_utilization_limits_set: bool = False
    load_testing_strategy_planned: bool = False
    performance_budgets_established: bool = False
    optimization_priorities_defined: bool = False
    scalability_targets_specified: bool = False
    metric_clarity_ensured: bool = False
    slo_alignment_addressed: bool = False

    @property
    def completeness_score(self) -> float:
        """Calculate completeness score (0.0 to 1.0)."""
        total_checks = 10
        passed_checks = sum([
            self.response_time_targets_specified,
            self.throughput_requirements_defined,
            self.concurrency_levels_identified,
            self.resource_utilization_limits_set,
            self.load_testing_strategy_planned,
            self.performance_budgets_established,
            self.optimization_priorities_defined,
            self.scalability_targets_specified,
            self.metric_clarity_ensured,
            self.slo_alignment_addressed,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "response_time_targets_specified": self.response_time_targets_specified,
            "throughput_requirements_defined": self.throughput_requirements_defined,
            "concurrency_levels_identified": self.concurrency_levels_identified,
            "resource_utilization_limits_set": self.resource_utilization_limits_set,
            "load_testing_strategy_planned": self.load_testing_strategy_planned,
            "performance_budgets_established": self.performance_budgets_established,
            "optimization_priorities_defined": self.optimization_priorities_defined,
            "scalability_targets_specified": self.scalability_targets_specified,
            "metric_clarity_ensured": self.metric_clarity_ensured,
            "slo_alignment_addressed": self.slo_alignment_addressed,
            "completeness_score": self.completeness_score,
        }


def extract_performance_requirements(source_data: Mapping[str, Any]) -> PerformanceRequirements:
    """
    Extract performance requirements from source brief data.

    Args:
        source_data: A mapping containing source brief information with fields like
                    'title', 'description', 'requirements', etc.

    Returns:
        PerformanceRequirements with boolean flags for each aspect and overall score.
    """
    if not isinstance(source_data, Mapping):
        return PerformanceRequirements()

    searchable_text = _extract_searchable_text(source_data)

    return PerformanceRequirements(
        response_time_targets_specified=bool(_RESPONSE_TIME_RE.search(searchable_text)),
        throughput_requirements_defined=bool(_THROUGHPUT_RE.search(searchable_text)),
        concurrency_levels_identified=bool(_CONCURRENCY_RE.search(searchable_text)),
        resource_utilization_limits_set=bool(_RESOURCE_UTILIZATION_RE.search(searchable_text)),
        load_testing_strategy_planned=bool(_LOAD_TESTING_RE.search(searchable_text)),
        performance_budgets_established=bool(_PERFORMANCE_BUDGETS_RE.search(searchable_text)),
        optimization_priorities_defined=bool(_OPTIMIZATION_PRIORITIES_RE.search(searchable_text)),
        scalability_targets_specified=bool(_SCALABILITY_TARGETS_RE.search(searchable_text)),
        metric_clarity_ensured=bool(_METRIC_CLARITY_RE.search(searchable_text)),
        slo_alignment_addressed=bool(_SLO_ALIGNMENT_RE.search(searchable_text)),
    )


def _extract_searchable_text(source_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the source data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale", "context"):
        value = source_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("requirements", "acceptance_criteria", "notes", "constraints", "objectives"):
        value = source_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "PerformanceRequirements",
    "extract_performance_requirements",
]
