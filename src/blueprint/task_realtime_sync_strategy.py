"""Analyze realtime synchronization strategy readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for realtime sync concepts
_SYNC_PROTOCOL_RE = re.compile(
    r"\b(?:websocket|ws|wss|server[- ]sent\s+event|sse|"
    r"polling|long[- ]polling|realtime\s+(?:sync|protocol|communication)|"
    r"sync\s+protocol|bidirectional\s+communication)\b",
    re.I,
)
_CONFLICT_RESOLUTION_RE = re.compile(
    r"\b(?:conflict\s+resolution|resolve\s+conflict|"
    r"operational\s+transform(?:ation)?|ot\s+(?:algorithm|strategy)|"
    r"crdt|conflict[- ]free\s+replicated\s+data\s+type|"
    r"last[- ]write[- ]wins|lww|merge\s+strateg(?:y|ies)|"
    r"concurrent\s+edit)\b",
    re.I,
)
_OFFLINE_SUPPORT_RE = re.compile(
    r"\b(?:offline\s+(?:support|mode|capability|first)|"
    r"work\s+offline|offline[- ]first|"
    r"local[- ]first|optimistic\s+(?:update|sync)|"
    r"sync\s+when\s+online|background\s+sync)\b",
    re.I,
)
_DELTA_UPDATES_RE = re.compile(
    r"\b(?:delta\s+(?:updates?|sync|change)|"
    r"incremental\s+(?:updates?|sync)|"
    r"partial\s+(?:updates?|sync)|patch|diff\s+(?:updates?|sync)|"
    r"change(?:sets?|s)(?:\s+(?:sync|propagation|incrementally))?|"
    r"(?:sync|propagate)\s+change(?:sets?|s)(?:\s+incrementally)?)\b",
    re.I,
)
_CONNECTION_RESILIENCE_RE = re.compile(
    r"\b(?:connection\s+resilience|reconnect(?:ion)?(?:\s+logic)?|"
    r"auto[- ]reconnect|retry\s+(?:logic|strategy)|"
    r"exponential\s+backoff|connection\s+(?:recovery|handling)|"
    r"network\s+resilience|handle\s+disconnect)\b",
    re.I,
)
_STATE_RECONCILIATION_RE = re.compile(
    r"\b(?:state\s+reconciliation|reconcile\s+state|"
    r"sync\s+reconciliation|merge\s+state|"
    r"state\s+(?:merge|synchronization)|consistency\s+check)\b",
    re.I,
)
_BANDWIDTH_OPTIMIZATION_RE = re.compile(
    r"\b(?:bandwidth\s+optimization|optimize\s+bandwidth|"
    r"compress(?:ion)?|debounce|throttle|"
    r"(?:batch(?:ing)?|prevent|avoid)\s+(?:update|sync|storm)|minimize\s+(?:bandwidth|traffic|payload)|"
    r"sync\s+storm)\b",
    re.I,
)
_LATENCY_REQUIREMENTS_RE = re.compile(
    r"\b(?:latency\s+(?:requirements?|target|budget|constraint|not\s+met)|"
    r"low[- ]latency|real[- ]time\s+(?:requirements?|constraint)|"
    r"response\s+time|sync\s+latency|propagation\s+delay)\b",
    re.I,
)
_SCALABILITY_RE = re.compile(
    r"\b(?:scalabilit(?:y|ies)|horizontal\s+scal(?:e|ing)|"
    r"load\s+balanc(?:e|ing)|distributed\s+sync|"
    r"scale\s+(?:sync|websocket)|cluster|shard)\b",
    re.I,
)
_MONITORING_COVERAGE_RE = re.compile(
    r"\b(?:monitor(?:ing)?\s+(?:sync|websocket|connection)|"
    r"metric|telemetry|observability|"
    r"track\s+(?:sync|latency|connection)|log\s+sync|"
    r"sync\s+(?:metric|telemetry))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class RealtimeSyncStrategyReadiness:
    """Realtime synchronization strategy readiness analysis for a change brief."""

    sync_protocol_defined: bool = False
    conflict_resolution_addressed: bool = False
    offline_support_implemented: bool = False
    delta_updates_configured: bool = False
    connection_resilience_implemented: bool = False
    state_reconciliation_planned: bool = False
    bandwidth_optimization_included: bool = False
    latency_requirements_specified: bool = False
    scalability_considered: bool = False
    monitoring_coverage_planned: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "sync_protocol_defined": self.sync_protocol_defined,
            "conflict_resolution_addressed": self.conflict_resolution_addressed,
            "offline_support_implemented": self.offline_support_implemented,
            "delta_updates_configured": self.delta_updates_configured,
            "connection_resilience_implemented": self.connection_resilience_implemented,
            "state_reconciliation_planned": self.state_reconciliation_planned,
            "bandwidth_optimization_included": self.bandwidth_optimization_included,
            "latency_requirements_specified": self.latency_requirements_specified,
            "scalability_considered": self.scalability_considered,
            "monitoring_coverage_planned": self.monitoring_coverage_planned,
        }


def analyze_realtime_sync_strategy(change_brief: Mapping[str, Any]) -> RealtimeSyncStrategyReadiness:
    """
    Analyze realtime synchronization strategy readiness from a change brief.

    Args:
        change_brief: A mapping containing change information with fields like
                     'title', 'description', 'acceptance_criteria', etc.

    Returns:
        RealtimeSyncStrategyReadiness with boolean flags for each sync aspect.
    """
    if not isinstance(change_brief, Mapping):
        return RealtimeSyncStrategyReadiness()

    searchable_text = _extract_searchable_text(change_brief)

    return RealtimeSyncStrategyReadiness(
        sync_protocol_defined=bool(_SYNC_PROTOCOL_RE.search(searchable_text)),
        conflict_resolution_addressed=bool(_CONFLICT_RESOLUTION_RE.search(searchable_text)),
        offline_support_implemented=bool(_OFFLINE_SUPPORT_RE.search(searchable_text)),
        delta_updates_configured=bool(_DELTA_UPDATES_RE.search(searchable_text)),
        connection_resilience_implemented=bool(_CONNECTION_RESILIENCE_RE.search(searchable_text)),
        state_reconciliation_planned=bool(_STATE_RECONCILIATION_RE.search(searchable_text)),
        bandwidth_optimization_included=bool(_BANDWIDTH_OPTIMIZATION_RE.search(searchable_text)),
        latency_requirements_specified=bool(_LATENCY_REQUIREMENTS_RE.search(searchable_text)),
        scalability_considered=bool(_SCALABILITY_RE.search(searchable_text)),
        monitoring_coverage_planned=bool(_MONITORING_COVERAGE_RE.search(searchable_text)),
    )


def _extract_searchable_text(change_brief: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the change brief for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = change_brief.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("acceptance_criteria", "requirements", "notes", "risks", "definition_of_done"):
        value = change_brief.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Extract validation commands
    validation = change_brief.get("validation_command") or change_brief.get("validation_commands")
    if isinstance(validation, str):
        parts.append(validation)
    elif isinstance(validation, (list, tuple)):
        parts.extend(str(cmd) for cmd in validation if cmd)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "RealtimeSyncStrategyReadiness",
    "analyze_realtime_sync_strategy",
]
