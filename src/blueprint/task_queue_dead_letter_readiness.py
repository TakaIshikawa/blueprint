"""Assess readiness for queue dead-letter handling tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan

TaskQueueDeadLetterReadinessPlan = SimpleReadinessPlan
TaskQueueDeadLetterReadinessRecord = SimpleReadinessRecord

_SIGNALS = {
    "dead_letter_queue": re.compile(r"\b(?:dead[- ]?letter queue|dead[- ]?letter topic|dead[- ]?letter handling|dlq)\b", re.I),
    "poison_message_handling": re.compile(r"\b(?:poison messages?|failed message quarantine|quarantine failed messages?|retry exhaustion)\b", re.I),
    "failure_routing": re.compile(r"\b(?:queue failure routing|failure routing|message replay|replay messages?|redrive)\b", re.I),
}
_PATH_SIGNALS = {
    "dead_letter_queue": re.compile(r"(?:dlq|dead[_-]?letter)", re.I),
    "poison_message_handling": re.compile(r"(?:poison|failed[_-]?messages?|quarantine)", re.I),
    "failure_routing": re.compile(r"(?:replay|redrive|failure[_-]?routing|retry[_-]?exhaust)", re.I),
}
_CRITERIA = {
    "routing_conditions": re.compile(r"\b(?:routing conditions?|route to dlq|dead[- ]?letter routing|failure routing|retry exhaustion|after retries)\b", re.I),
    "retention_policy": re.compile(r"\b(?:retention policy|retention window|message ttl|ttl|expire failed messages?|purge policy)\b", re.I),
    "inspection_tooling": re.compile(r"\b(?:inspection tooling|inspect messages?|message inspection|dlq browser|queue dashboard|diagnostics)\b", re.I),
    "replay_or_discard_workflow": re.compile(r"\b(?:replay|redrive|discard workflow|replay workflow|manual discard|quarantine release)\b", re.I),
    "alerting_ownership": re.compile(r"\b(?:alerting|alerts?|owner|ownership|on[- ]?call|escalation|runbook)\b", re.I),
    "idempotency_safeguards": re.compile(r"\b(?:idempotency|idempotent|deduplication|duplicate protection|safe replay)\b", re.I),
    "validation_coverage": re.compile(r"\b(?:validation coverage|tests?|unit tests?|integration tests?|pytest|replay tests?|dlq tests?)\b", re.I),
}
_GUIDANCE = {
    "routing_conditions": "Define routing conditions for retry exhaustion, failure routing, or when messages are routed to the DLQ.",
    "retention_policy": "Specify retention policy, retention window, TTL, expiration, or purge behavior for failed messages.",
    "inspection_tooling": "Add inspection tooling such as message inspection, a DLQ browser, queue dashboard, or diagnostics.",
    "replay_or_discard_workflow": "Document replay, redrive, discard, or quarantine release workflow.",
    "alerting_ownership": "Assign alerting, ownership, on-call escalation, or runbook guidance for DLQ growth and failures.",
    "idempotency_safeguards": "Describe idempotency safeguards, deduplication, duplicate protection, or safe replay behavior.",
    "validation_coverage": "Add validation coverage with unit, integration, pytest, replay, or DLQ tests.",
}
_NO_IMPACT = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:dlq|dead[- ]?letter|poison message|failed messages?)\b.{0,80}\b(?:impact|changes?|planned|scope|required|needed)\b", re.I)


def build_task_queue_dead_letter_readiness_plan(source: Any) -> TaskQueueDeadLetterReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(source, title="Task Queue Dead Letter Readiness", signal_patterns=_SIGNALS, path_signal_patterns=_PATH_SIGNALS, criteria_patterns=_CRITERIA, criterion_guidance=_GUIDANCE, no_impact_pattern=_NO_IMPACT)


analyze_task_queue_dead_letter_readiness = build_task_queue_dead_letter_readiness_plan
extract_task_queue_dead_letter_readiness = build_task_queue_dead_letter_readiness_plan
generate_task_queue_dead_letter_readiness = build_task_queue_dead_letter_readiness_plan
derive_task_queue_dead_letter_readiness = build_task_queue_dead_letter_readiness_plan
summarize_task_queue_dead_letter_readiness = build_task_queue_dead_letter_readiness_plan
summarize_task_queue_dead_letter_readiness_plan = build_task_queue_dead_letter_readiness_plan


def recommend_task_queue_dead_letter_readiness(source: Any) -> tuple[TaskQueueDeadLetterReadinessRecord, ...]:
    return build_task_queue_dead_letter_readiness_plan(source).records


def task_queue_dead_letter_readiness_plan_to_dict(result: TaskQueueDeadLetterReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_queue_dead_letter_readiness_plan_to_dict.__test__ = False


def task_queue_dead_letter_readiness_plan_to_dicts(result: TaskQueueDeadLetterReadinessPlan | Iterable[TaskQueueDeadLetterReadinessRecord]) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_queue_dead_letter_readiness_plan_to_dicts.__test__ = False
task_queue_dead_letter_readiness_to_dicts = task_queue_dead_letter_readiness_plan_to_dicts


def task_queue_dead_letter_readiness_plan_to_markdown(result: TaskQueueDeadLetterReadinessPlan) -> str:
    return result.to_markdown()


task_queue_dead_letter_readiness_plan_to_markdown.__test__ = False
