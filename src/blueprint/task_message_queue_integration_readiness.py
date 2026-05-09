"""Analyze message queue integration readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for message queue integration concepts
_MESSAGE_SCHEMA_RE = re.compile(
    r"\b(?:message[_\s]+schema|schema[_\s]+(?:validation|definition)|"
    r"payload[_\s]+(?:schema|structure|validation|format)|"
    r"message[_\s]+(?:format|structure|validation)|"
    r"schema[_\s]+(?:registry|validation)|avro[_\s]+schema|"
    r"protobuf[_\s]+schema|json[_\s]+schema|message[_\s]+contract|"
    r"validate[_\s]+(?:message|payload)|schema[_\s]+evolution|"
    r"test[_\s]+(?:message[_\s]+)?schema(?:[_\s]+validation)?)\b",
    re.I,
)
_DELIVERY_GUARANTEES_RE = re.compile(
    r"\b(?:delivery[_\s]+guarantees?|at[_\s-]*(?:least|most)[_\s-]*once|"
    r"exactly[_\s-]*once|message[_\s]+delivery|"
    r"delivery[_\s]+semantic|guaranteed[_\s]+delivery|"
    r"message[_\s]+acknowledgment|ack[_\s]+(?:mode|strategy)|"
    r"delivery[_\s]+(?:confirmation|reliability)|message[_\s]+durability)\b",
    re.I,
)
_RETRY_POLICY_RE = re.compile(
    r"\b(?:retry[_\s]+(?:policy|strategy|logic|mechanism)|"
    r"exponential[_\s]+backoff|backoff[_\s]+strategy|"
    r"max[_\s]+retr(?:y|ies)|retry[_\s]+attempt|"
    r"retry[_\s]+(?:count|limit|interval)|reprocess(?:ing)?[_\s]+(?:strategy|logic)|"
    r"failure[_\s]+retry|message[_\s]+retry|test[_\s]+retry[_\s]+mechanism)\b",
    re.I,
)
_DEAD_LETTER_QUEUE_RE = re.compile(
    r"\b(?:dead[_\s-]*letter[_\s]+queue|dlq|"
    r"failed[_\s]+message[_\s]+(?:queue|handling)|"
    r"poison[_\s]+message[_\s]+(?:queue|handling)|"
    r"undeliverable[_\s]+message|error[_\s]+queue|"
    r"failure[_\s]+queue|message[_\s]+graveyard)\b",
    re.I,
)
_IDEMPOTENCY_RE = re.compile(
    r"\b(?:idempoten(?:t|cy)|idempotent[_\s]+(?:consumer|processing|handler)|"
    r"duplicate[_\s]+(?:detection|prevention|handling)|"
    r"deduplicat(?:e|ion)|message[_\s]+(?:id|identifier)|"
    r"exactly[_\s-]*once[_\s]+processing|prevent[_\s]+(?:duplicate|reprocess)|"
    r"unique[_\s]+message[_\s]+id|test[_\s]+idempotency)\b",
    re.I,
)
_MESSAGE_ORDERING_RE = re.compile(
    r"\b(?:message[_\s]+order(?:ing)?|order[_\s]+guarantee|"
    r"fifo[_\s]+(?:queue|ordering)|sequential[_\s]+processing|"
    r"partition[_\s]+(?:key|ordering)|maintain[_\s]+order|"
    r"ordered[_\s]+(?:delivery|processing)|sequence[_\s]+number)\b",
    re.I,
)
_BACKPRESSURE_RE = re.compile(
    r"\b(?:backpressure|back[_\s-]*pressure|"
    r"flow[_\s]+control|rate[_\s]+limit(?:ing)?|"
    r"throttl(?:e|ing)|consumer[_\s]+(?:lag|scaling)|"
    r"prefetch[_\s]+(?:count|limit)|message[_\s]+(?:rate|throughput)|"
    r"load[_\s]+shedding|queue[_\s]+depth)\b",
    re.I,
)
_MONITORING_RE = re.compile(
    r"\b(?:queue[_\s]+(?:monitoring|metrics|observability|depth)|"
    r"message[_\s]+(?:monitoring|metrics|tracking)|"
    r"consumer[_\s]+(?:lag|metrics|monitoring|scaling)|"
    r"queue[_\s]+depth|processing[_\s]+(?:time|metrics)|"
    r"dlq[_\s]+monitoring|alert(?:ing)?|monitoring|"
    r"message[_\s]+(?:latency|throughput)[_\s]*(?:metrics)?|"
    r"monitor(?:ing)?[_\s]+(?:dlq|queue))\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class MessageQueueIntegrationReadiness:
    """Message queue integration readiness analysis for a task."""

    message_schema_defined: bool = False
    delivery_guarantees_specified: bool = False
    retry_policy_defined: bool = False
    dead_letter_queue_configured: bool = False
    idempotency_handled: bool = False
    message_ordering_addressed: bool = False
    backpressure_managed: bool = False
    monitoring_configured: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        total_checks = 8
        passed_checks = sum([
            self.message_schema_defined,
            self.delivery_guarantees_specified,
            self.retry_policy_defined,
            self.dead_letter_queue_configured,
            self.idempotency_handled,
            self.message_ordering_addressed,
            self.backpressure_managed,
            self.monitoring_configured,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "message_schema_defined": self.message_schema_defined,
            "delivery_guarantees_specified": self.delivery_guarantees_specified,
            "retry_policy_defined": self.retry_policy_defined,
            "dead_letter_queue_configured": self.dead_letter_queue_configured,
            "idempotency_handled": self.idempotency_handled,
            "message_ordering_addressed": self.message_ordering_addressed,
            "backpressure_managed": self.backpressure_managed,
            "monitoring_configured": self.monitoring_configured,
            "readiness_score": self.readiness_score,
        }


def analyze_message_queue_integration_readiness(task_data: Mapping[str, Any]) -> MessageQueueIntegrationReadiness:
    """
    Analyze message queue integration readiness from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        MessageQueueIntegrationReadiness with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return MessageQueueIntegrationReadiness()

    searchable_text = _extract_searchable_text(task_data)

    return MessageQueueIntegrationReadiness(
        message_schema_defined=bool(_MESSAGE_SCHEMA_RE.search(searchable_text)),
        delivery_guarantees_specified=bool(_DELIVERY_GUARANTEES_RE.search(searchable_text)),
        retry_policy_defined=bool(_RETRY_POLICY_RE.search(searchable_text)),
        dead_letter_queue_configured=bool(_DEAD_LETTER_QUEUE_RE.search(searchable_text)),
        idempotency_handled=bool(_IDEMPOTENCY_RE.search(searchable_text)),
        message_ordering_addressed=bool(_MESSAGE_ORDERING_RE.search(searchable_text)),
        backpressure_managed=bool(_BACKPRESSURE_RE.search(searchable_text)),
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
    "MessageQueueIntegrationReadiness",
    "analyze_message_queue_integration_readiness",
]
