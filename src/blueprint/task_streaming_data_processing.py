"""Analyze streaming data processing readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for streaming data processing concepts
_EVENT_SCHEMA_RE = re.compile(
    r"\b(?:event[_\s-]+schema|message[_\s-]+schema|stream[_\s-]+schema|"
    r"event[_\s-]+structure|payload[_\s-]+schema|avro[_\s-]+schema|"
    r"protobuf[_\s-]+schema|json[_\s-]+schema|event[_\s-]+format|"
    r"test[_\s-]+event[_\s-]+schema)\b",
    re.I,
)
_PROCESSING_GUARANTEES_RE = re.compile(
    r"\b(?:exactly[_\s-]*once|at[_\s-]*least[_\s-]*once|at[_\s-]*most[_\s-]*once|"
    r"processing[_\s-]+guarantee[s]?|delivery[_\s-]+guarantee[s]?|"
    r"idempotent[_\s-]+processing|deduplication|"
    r"test[_\s-]+processing[_\s-]+guarantee)\b",
    re.I,
)
_WINDOWING_STRATEGY_RE = re.compile(
    r"\b(?:windowing[_\s-]*(?:strategy|strateg(?:y|ies))?|tumbling[_\s-]+window[s]?|"
    r"sliding[_\s-]+window[s]?|session[_\s-]+window[s]?|"
    r"hopping[_\s-]+window[s]?|fixed[_\s-]+window[s]?|"
    r"time[_\s-]+window[s]?|count[_\s-]+window[s]?|"
    r"test[_\s-]+(?:windowing|window))\b",
    re.I,
)
_STATE_MANAGEMENT_RE = re.compile(
    r"\b(?:state[_\s-]+management|stateful[_\s-]+processing|"
    r"state[_\s-]+store|rocksdb|state[_\s-]+backend|"
    r"key[_\s-]+value[_\s-]+store|state[_\s-]+checkpoint|"
    r"stateful[_\s-]+operator|state[_\s-]+recovery|"
    r"test[_\s-]+state[_\s-]+management)\b",
    re.I,
)
_BACKPRESSURE_HANDLING_RE = re.compile(
    r"\b(?:backpressure|back[_\s-]*pressure|flow[_\s-]+control|"
    r"rate[_\s-]+limit(?:ing|er)?|throttl(?:e|ing)|"
    r"buffer[_\s-]+(?:overflow|limit|management)|"
    r"congestion[_\s-]+control|reactive[_\s-]+streams|"
    r"test[_\s-]+backpressure)\b",
    re.I,
)
_OUT_OF_ORDER_EVENTS_RE = re.compile(
    r"\b(?:out[_\s-]*of[_\s-]*order[_\s-]*event[s]?|"
    r"late[_\s-]+arrival[s]?|late[_\s-]+data|late[_\s-]+event[s]?|"
    r"event[_\s-]+ordering|event[_\s-]+time[_\s-]+ordering|"
    r"allowed[_\s-]+lateness|lateness[_\s-]+handling|"
    r"test[_\s-]+(?:out[_\s-]*of[_\s-]*order|late[_\s-]+arrival))\b",
    re.I,
)
_WATERMARKS_RE = re.compile(
    r"\b(?:watermark[s]?|event[_\s-]+time[_\s-]+watermark[s]?|"
    r"low[_\s-]+watermark|high[_\s-]+watermark|"
    r"watermark[_\s-]+strateg(?:y|ies)|watermark[_\s-]+generation|"
    r"test[_\s-]+watermark)\b",
    re.I,
)
_CHECKPOINT_RECOVERY_RE = re.compile(
    r"\b(?:checkpoint[_\s-]*(?:ing)?|snapshot[_\s-]*(?:ing)?|"
    r"state[_\s-]+snapshot|checkpoint[_\s-]+recovery|"
    r"failure[_\s-]+recovery|fault[_\s-]+tolerance|"
    r"savepoint[s]?|recovery[_\s-]+point[s]?|"
    r"crash[_\s-]+recovery|test[_\s-]+(?:checkpoint|recovery))\b",
    re.I,
)
_EVENT_TIME_RE = re.compile(
    r"\b(?:event[_\s-]+time|processing[_\s-]+time|ingestion[_\s-]+time|"
    r"event[_\s-]+timestamp|time[_\s-]+semantic[s]?|"
    r"event[_\s-]+time[_\s-]+vs[_\s-]+processing[_\s-]+time|"
    r"test[_\s-]+event[_\s-]+time)\b",
    re.I,
)
_MONITORING_RE = re.compile(
    r"\b(?:stream[_\s-]+(?:monitoring|metric[s]?|observability|throughput)|"
    r"lag[_\s-]+monitoring|consumer[_\s-]+lag|processing[_\s-]+latency|"
    r"throughput[_\s-]+monitoring|backlog[_\s-]+monitoring|"
    r"stream[_\s-]+health|test[_\s-]+stream[_\s-]+monitoring)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class StreamingDataProcessingReadiness:
    """Streaming data processing readiness analysis for a task."""

    event_schema_defined: bool = False
    processing_guarantees_specified: bool = False
    windowing_strategy_defined: bool = False
    state_management_addressed: bool = False
    backpressure_handling_implemented: bool = False
    out_of_order_events_handled: bool = False
    watermarks_configured: bool = False
    checkpoint_recovery_enabled: bool = False
    event_time_semantics_used: bool = False
    monitoring_configured: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        # Core streaming requirements (critical for reliability)
        core_checks = [
            self.event_schema_defined,
            self.processing_guarantees_specified,
            self.checkpoint_recovery_enabled,
        ]

        # Consistency guarantees (critical for correctness)
        consistency_checks = [
            self.windowing_strategy_defined,
            self.state_management_addressed,
            self.out_of_order_events_handled,
            self.watermarks_configured,
            self.event_time_semantics_used,
        ]

        # Performance and operations (important for production)
        operations_checks = [
            self.backpressure_handling_implemented,
            self.monitoring_configured,
        ]

        # Weight: core=40%, consistency=40%, operations=20%
        core_score = sum(core_checks) / len(core_checks) * 0.4
        consistency_score = sum(consistency_checks) / len(consistency_checks) * 0.4
        operations_score = sum(operations_checks) / len(operations_checks) * 0.2

        return core_score + consistency_score + operations_score

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "event_schema_defined": self.event_schema_defined,
            "processing_guarantees_specified": self.processing_guarantees_specified,
            "windowing_strategy_defined": self.windowing_strategy_defined,
            "state_management_addressed": self.state_management_addressed,
            "backpressure_handling_implemented": self.backpressure_handling_implemented,
            "out_of_order_events_handled": self.out_of_order_events_handled,
            "watermarks_configured": self.watermarks_configured,
            "checkpoint_recovery_enabled": self.checkpoint_recovery_enabled,
            "event_time_semantics_used": self.event_time_semantics_used,
            "monitoring_configured": self.monitoring_configured,
            "readiness_score": self.readiness_score,
        }


def analyze_streaming_data_processing_readiness(task_data: Mapping[str, Any]) -> StreamingDataProcessingReadiness:
    """
    Analyze streaming data processing readiness from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        StreamingDataProcessingReadiness with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return StreamingDataProcessingReadiness()

    searchable_text = _extract_searchable_text(task_data)

    return StreamingDataProcessingReadiness(
        event_schema_defined=bool(_EVENT_SCHEMA_RE.search(searchable_text)),
        processing_guarantees_specified=bool(_PROCESSING_GUARANTEES_RE.search(searchable_text)),
        windowing_strategy_defined=bool(_WINDOWING_STRATEGY_RE.search(searchable_text)),
        state_management_addressed=bool(_STATE_MANAGEMENT_RE.search(searchable_text)),
        backpressure_handling_implemented=bool(_BACKPRESSURE_HANDLING_RE.search(searchable_text)),
        out_of_order_events_handled=bool(_OUT_OF_ORDER_EVENTS_RE.search(searchable_text)),
        watermarks_configured=bool(_WATERMARKS_RE.search(searchable_text)),
        checkpoint_recovery_enabled=bool(_CHECKPOINT_RECOVERY_RE.search(searchable_text)),
        event_time_semantics_used=bool(_EVENT_TIME_RE.search(searchable_text)),
        monitoring_configured=bool(_MONITORING_RE.search(searchable_text)),
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
    "StreamingDataProcessingReadiness",
    "analyze_streaming_data_processing_readiness",
]
