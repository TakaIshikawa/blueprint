"""Analyze queue consumer scaling readiness for execution-plan tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, build_simple_readiness_plan


_SIGNAL_PATTERNS = {
    "queue_consumer_scaling": re.compile(
        r"\b(?:queue|topic|stream)[_\s-]+(?:consumer|worker|processor)[_\s-]+(?:scaling|scale|increase|rebalance|tuning)\b|"
        r"\b(?:scale|increase|tune|rebalance|autoscale).{0,100}\b(?:consumer|worker|queue|topic|stream)\b",
        re.I,
    ),
    "worker_concurrency": re.compile(r"\b(?:worker|consumer)[_\s-]+(?:concurrency|parallelism|pool[_\s-]+size|threads?|replicas?)\b|\bconcurrency.{0,80}\b(?:worker|consumer)\b", re.I),
    "autoscaling": re.compile(r"\b(?:autoscal(?:e|ing)|auto[_\s-]+scal(?:e|ing)|hpa|horizontal[_\s-]+pod[_\s-]+autoscaler|scale[_\s-]+out|scale[_\s-]+in)\b", re.I),
    "partition_rebalancing": re.compile(r"\b(?:partition|shard|topic)[_\s-]+(?:rebalance|rebalancing|assignment|reassignment|redistribution)\b|\brebalance.{0,80}\b(?:partition|shard|topic)\b", re.I),
    "throughput_increase": re.compile(r"\b(?:throughput|qps|rps|messages?[_\s-]+per[_\s-]+second|processing[_\s-]+rate)[_\s-]+(?:increase|target|raise|improvement)\b|\bincrease.{0,80}\bthroughput\b", re.I),
    "lag_reduction": re.compile(r"\b(?:lag|backlog|queue[_\s-]+depth)[_\s-]+(?:reduction|reduce|target|drain|lower)\b|\breduce.{0,80}\b(?:lag|backlog|queue[_\s-]+depth)\b", re.I),
    "backpressure": re.compile(r"\b(?:backpressure|back[_\s-]+pressure|load[_\s-]+shedding|throttle|throttling|rate[_\s-]+limit)\b", re.I),
}
_PATH_SIGNAL_PATTERNS = {
    "queue_consumer_scaling": re.compile(r"(?:queue|topic|stream).*(?:consumer|worker).*(?:scal|rebalance|tuning)|(?:scal|rebalance|tuning).*(?:queue|topic|stream).*(?:consumer|worker)", re.I),
    "worker_concurrency": re.compile(r"(?:worker|consumer).*(?:concurrency|parallel|pool|replica|thread)|(?:concurrency|parallel|pool|replica|thread).*(?:worker|consumer)", re.I),
    "autoscaling": re.compile(r"autoscal|auto[_-]?scal|hpa|scale[_-]?(?:out|in)", re.I),
    "partition_rebalancing": re.compile(r"(?:partition|shard|topic).*(?:rebalance|assignment|reassign|redistribution)", re.I),
    "throughput_increase": re.compile(r"throughput|qps|rps|messages?[_-]?per[_-]?second|processing[_-]?rate", re.I),
    "lag_reduction": re.compile(r"(?:lag|backlog|queue[_-]?depth).*(?:reduc|target|drain)|(?:reduc|drain).*(?:lag|backlog|queue[_-]?depth)", re.I),
    "backpressure": re.compile(r"back[_-]?pressure|load[_-]?shedding|throttl|rate[_-]?limit", re.I),
}
_CRITERIA_PATTERNS = {
    "idempotency": re.compile(r"\b(?:idempotent|idempotency|dedupe|deduplication|duplicate[_\s-]+handling|safe[_\s-]+to[_\s-]+replay|replay[_\s-]+safe|exactly[_\s-]+once|job[_\s-]+key|message[_\s-]+key)\b", re.I),
    "ordering": re.compile(r"\b(?:ordering|ordered|message[_\s-]+order|per[_\s-]+key[_\s-]+order|partition[_\s-]+order|fifo|sequence|out[_\s-]+of[_\s-]+order)\b", re.I),
    "retry_behavior": re.compile(r"\b(?:retry|retries|retry[_\s-]+policy|backoff|exponential[_\s-]+backoff|jitter|max[_\s-]+attempts?|poison[_\s-]+message)\b", re.I),
    "dead_letter_monitoring": re.compile(r"\b(?:dead[_\s-]+letter|dlq|dlt|failure[_\s-]+queue|parking[_\s-]+lot)[_\s-]*(?:monitoring|alerts?|metrics?|dashboard)?\b|\b(?:monitor|alert).{0,80}\b(?:dlq|dead[_\s-]+letter)\b", re.I),
    "capacity_limits": re.compile(r"\b(?:capacity[_\s-]+limits?|concurrency[_\s-]+limit|replica[_\s-]+limit|max[_\s-]+workers?|rate[_\s-]+limit|throttle|quota|cpu|memory|broker[_\s-]+capacity|database[_\s-]+capacity)\b", re.I),
    "rollback_concurrency_reduction": re.compile(r"\b(?:rollback|roll[_\s-]+back|revert|scale[_\s-]+down|reduce[_\s-]+concurrency|lower[_\s-]+concurrency|disable[_\s-]+autoscaling|restore[_\s-]+previous[_\s-]+workers?)\b", re.I),
    "metrics": re.compile(r"\b(?:metrics?|dashboard|monitor(?:ing)?|alerts?|consumer[_\s-]+lag|queue[_\s-]+depth|throughput|processing[_\s-]+rate|latency|error[_\s-]+rate|saturation)\b", re.I),
    "validation": re.compile(r"\b(?:validation|validate|verification|load[_\s-]+test|soak[_\s-]+test|canary|dry[_\s-]+run|smoke[_\s-]+test|validation[_\s-]+command|pytest|replay[_\s-]+test)\b", re.I),
}
_GUIDANCE = {
    "idempotency": "Document idempotency, deduplication, replay safety, or unique message keys before increasing consumers.",
    "ordering": "State ordering guarantees, partition ordering, FIFO constraints, and out-of-order handling.",
    "retry_behavior": "Specify retry limits, backoff, jitter, poison-message handling, and max attempts.",
    "dead_letter_monitoring": "Add dead-letter queue metrics, alerts, dashboards, and ownership for failed messages.",
    "capacity_limits": "Set capacity limits for worker concurrency, replicas, broker load, downstream systems, CPU, and memory.",
    "rollback_concurrency_reduction": "Define rollback, scale-down, autoscaling disable, or concurrency reduction steps.",
    "metrics": "Name metrics and alerts for lag, queue depth, throughput, latency, saturation, and errors.",
    "validation": "Add validation commands, load tests, soak tests, canaries, dry runs, or replay tests.",
}


def build_task_queue_consumer_scaling_readiness_plan(source: Any) -> SimpleReadinessPlan:
    """Build queue consumer scaling readiness records from task-shaped input."""
    return build_simple_readiness_plan(
        source,
        title="Task Queue Consumer Scaling Readiness",
        signal_patterns=_SIGNAL_PATTERNS,
        path_signal_patterns=_PATH_SIGNAL_PATTERNS,
        criteria_patterns=_CRITERIA_PATTERNS,
        criterion_guidance=_GUIDANCE,
    )


analyze_task_queue_consumer_scaling_readiness = build_task_queue_consumer_scaling_readiness_plan
summarize_task_queue_consumer_scaling_readiness = build_task_queue_consumer_scaling_readiness_plan
generate_task_queue_consumer_scaling_readiness = build_task_queue_consumer_scaling_readiness_plan
extract_task_queue_consumer_scaling_readiness = build_task_queue_consumer_scaling_readiness_plan
recommend_task_queue_consumer_scaling_readiness = build_task_queue_consumer_scaling_readiness_plan


def task_queue_consumer_scaling_readiness_plan_to_dict(plan: SimpleReadinessPlan) -> dict[str, Any]:
    """Serialize a queue consumer scaling readiness plan."""
    return plan.to_dict()


def task_queue_consumer_scaling_readiness_plan_to_dicts(plan: SimpleReadinessPlan) -> list[dict[str, Any]]:
    """Serialize queue consumer scaling readiness records."""
    return plan.to_dicts()


def task_queue_consumer_scaling_readiness_plan_to_markdown(plan: SimpleReadinessPlan) -> str:
    """Render queue consumer scaling readiness as Markdown."""
    return plan.to_markdown()


__all__ = [
    "SimpleReadinessPlan",
    "analyze_task_queue_consumer_scaling_readiness",
    "build_task_queue_consumer_scaling_readiness_plan",
    "extract_task_queue_consumer_scaling_readiness",
    "generate_task_queue_consumer_scaling_readiness",
    "recommend_task_queue_consumer_scaling_readiness",
    "summarize_task_queue_consumer_scaling_readiness",
    "task_queue_consumer_scaling_readiness_plan_to_dict",
    "task_queue_consumer_scaling_readiness_plan_to_dicts",
    "task_queue_consumer_scaling_readiness_plan_to_markdown",
]
