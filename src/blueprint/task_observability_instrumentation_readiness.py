"""Analyze observability instrumentation readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for observability instrumentation concepts
_METRICS_COLLECTION_RE = re.compile(
    r"\b(?:metrics?[_\s]+(?:collection|tracking|recording|emission)|"
    r"collect[_\s]+metrics?|emit[_\s]+metrics?|"
    r"prometheus|statsd|counter[_\s]+metric|"
    r"gauge|histogram|meter[_\s]+metric|"
    r"track[_\s]+(?:metrics?|performance|latency)|"
    r"custom[_\s]+metrics?|business[_\s]+metrics?)\b",
    re.I,
)
_DISTRIBUTED_TRACING_RE = re.compile(
    r"\b(?:distributed[_\s]+tracing|trace[_\s]+(?:propagation|context)|"
    r"opentelemetry|otel|jaeger|zipkin|"
    r"trace[_\s]+(?:id|span)|span[_\s]+(?:context|propagation)|"
    r"tracing[_\s]+(?:enabled|configured|instrumentation)|"
    r"trace[_\s]+header|request[_\s]+tracing|"
    r"end[_\s-]*to[_\s-]*end[_\s]+tracing)\b",
    re.I,
)
_STRUCTURED_LOGGING_RE = re.compile(
    r"\b(?:structured[_\s]+log(?:ging)?|log[_\s]+(?:structure|format)|"
    r"json[_\s]+log(?:ging)?|contextual[_\s]+log(?:ging)?|"
    r"log[_\s]+(?:context|metadata|fields)|correlation[_\s]+id|"
    r"log[_\s]+aggregation|centralized[_\s]+logging|"
    r"log[_\s]+(?:level|severity)|semantic[_\s]+logging)\b",
    re.I,
)
_ALERTING_RULES_RE = re.compile(
    r"\b(?:alerting|alert(?:ing)?[_\s]+(?:rules?|policy|threshold|strategy|configured)|"
    r"define[_\s]+alert|configure[_\s]+alert|"
    r"alert[_\s]+(?:condition|trigger|notification)|"
    r"pagerduty|oncall|incident[_\s]+alert|"
    r"threshold[_\s]+(?:alert|monitoring)|error[_\s]+alert|"
    r"anomaly[_\s]+detection|alert(?:ing)?[_\s]*rules?[_\s]+needed)\b",
    re.I,
)
_SLI_SLO_RE = re.compile(
    r"\b(?:slis?|slos?|service[_\s]+level[_\s]+(?:indicators?|objectives?)|"
    r"availability[_\s]+(?:target|slo)|latency[_\s]+slo|"
    r"error[_\s]+(?:budget|rate[_\s]+slo)|"
    r"performance[_\s]+(?:target|objective|slo)|"
    r"uptime[_\s]+(?:target|slo)|reliability[_\s]+target|"
    r"slo[_\s]+defined)\b",
    re.I,
)
_TRACE_COVERAGE_RE = re.compile(
    r"\b(?:trace[_\s]+coverage|tracing[_\s]+(?:span|coverage)|"
    r"instrument(?:ation)?[_\s]+(?:code|function|endpoints?|service|all)|"
    r"trace[_\s]+(?:all|critical)[_\s]+(?:path|operation)|"
    r"span[_\s]+(?:creation|instrumentation)|"
    r"complete[_\s]+(?:trace|tracing)|full[_\s]+trace[_\s]+coverage)\b",
    re.I,
)
_LOG_AGGREGATION_RE = re.compile(
    r"\b(?:log[_\s]+aggregation|aggregate[_\s]+logs?|"
    r"log[_\s]+(?:collector|shipper|forwarding)|"
    r"elasticsearch|elk[_\s]+stack|splunk|datadog[_\s]+logs?|"
    r"cloudwatch[_\s]+logs?|fluentd|logstash|"
    r"centralized[_\s]+log(?:ging)?|log[_\s]+pipeline)\b",
    re.I,
)
_DASHBOARD_RE = re.compile(
    r"\b(?:dashboard|grafana|kibana|datadog[_\s]+dashboard|"
    r"metrics?[_\s]+dashboard|monitoring[_\s]+dashboard|"
    r"observability[_\s]+dashboard|visualization|"
    r"metrics?[_\s]+visualization|create[_\s]+dashboard)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class ObservabilityInstrumentationReadiness:
    """Observability instrumentation readiness analysis for a task."""

    metrics_collection_defined: bool = False
    distributed_tracing_enabled: bool = False
    structured_logging_configured: bool = False
    alerting_rules_specified: bool = False
    sli_slo_defined: bool = False
    trace_coverage_complete: bool = False
    log_aggregation_configured: bool = False
    dashboard_requirements_specified: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        total_checks = 8
        passed_checks = sum([
            self.metrics_collection_defined,
            self.distributed_tracing_enabled,
            self.structured_logging_configured,
            self.alerting_rules_specified,
            self.sli_slo_defined,
            self.trace_coverage_complete,
            self.log_aggregation_configured,
            self.dashboard_requirements_specified,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "metrics_collection_defined": self.metrics_collection_defined,
            "distributed_tracing_enabled": self.distributed_tracing_enabled,
            "structured_logging_configured": self.structured_logging_configured,
            "alerting_rules_specified": self.alerting_rules_specified,
            "sli_slo_defined": self.sli_slo_defined,
            "trace_coverage_complete": self.trace_coverage_complete,
            "log_aggregation_configured": self.log_aggregation_configured,
            "dashboard_requirements_specified": self.dashboard_requirements_specified,
            "readiness_score": self.readiness_score,
        }


def analyze_observability_instrumentation_readiness(task_data: Mapping[str, Any]) -> ObservabilityInstrumentationReadiness:
    """
    Analyze observability instrumentation readiness from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        ObservabilityInstrumentationReadiness with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return ObservabilityInstrumentationReadiness()

    searchable_text = _extract_searchable_text(task_data)

    return ObservabilityInstrumentationReadiness(
        metrics_collection_defined=bool(_METRICS_COLLECTION_RE.search(searchable_text)),
        distributed_tracing_enabled=bool(_DISTRIBUTED_TRACING_RE.search(searchable_text)),
        structured_logging_configured=bool(_STRUCTURED_LOGGING_RE.search(searchable_text)),
        alerting_rules_specified=bool(_ALERTING_RULES_RE.search(searchable_text)),
        sli_slo_defined=bool(_SLI_SLO_RE.search(searchable_text)),
        trace_coverage_complete=bool(_TRACE_COVERAGE_RE.search(searchable_text)),
        log_aggregation_configured=bool(_LOG_AGGREGATION_RE.search(searchable_text)),
        dashboard_requirements_specified=bool(_DASHBOARD_RE.search(searchable_text)),
    )


def _extract_searchable_text(task_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the task data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = task_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("acceptance_criteria", "requirements", "notes", "risks", "definition_of_done"):
        value = task_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Extract validation commands
    validation = task_data.get("validation_command") or task_data.get("validation_commands")
    if isinstance(validation, str):
        parts.append(validation)
    elif isinstance(validation, (list, tuple)):
        parts.extend(str(cmd) for cmd in validation if cmd)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "ObservabilityInstrumentationReadiness",
    "analyze_observability_instrumentation_readiness",
]
