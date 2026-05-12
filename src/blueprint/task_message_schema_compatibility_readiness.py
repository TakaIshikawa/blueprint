"""Assess readiness for task plans that change message schema compatibility."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import (
    SimpleReadinessPlan,
    SimpleReadinessRecord,
    build_simple_readiness_plan,
)


MessageSchemaCompatibilityReadinessFinding = SimpleReadinessRecord
MessageSchemaCompatibilityReadinessPlan = SimpleReadinessPlan

_SIGNAL_PATTERNS = {
    "queue_event": re.compile(r"\b(?:queue event|queued event|message queue|async event|event bus|domain event)\b", re.I),
    "pubsub_topic": re.compile(r"\b(?:pub/sub|pubsub|topic|subscription|subscriber|publisher)\b", re.I),
    "kafka_message": re.compile(r"\b(?:kafka|kafka message|kafka topic|consumer group|partition)\b", re.I),
    "json_schema": re.compile(r"\b(?:json schema|schema\.json|json payload|message schema)\b", re.I),
    "protobuf": re.compile(r"\b(?:protobuf|proto3?|\.proto|grpc message)\b", re.I),
    "avro": re.compile(r"\b(?:avro|schema registry|avsc)\b", re.I),
    "producer_consumer": re.compile(r"\b(?:producer|consumer|publisher|subscriber|downstream consumer|upstream producer)\b", re.I),
}
_PATH_SIGNAL_PATTERNS = {
    "queue_event": re.compile(r"queues?|events?|messages?", re.I),
    "pubsub_topic": re.compile(r"pubsub|pub-sub|topics?|subscriptions?", re.I),
    "kafka_message": re.compile(r"kafka|consumer[-_]?groups?|partitions?", re.I),
    "json_schema": re.compile(r"json[-_]?schema|schemas?|payloads?", re.I),
    "protobuf": re.compile(r"proto|protobuf", re.I),
    "avro": re.compile(r"avro|avsc|schema[-_]?registry", re.I),
    "producer_consumer": re.compile(r"producers?|consumers?|publishers?|subscribers?", re.I),
}
_CRITERIA_PATTERNS = {
    "producer_consumer_inventory": re.compile(r"\b(?:producer.?consumer inventory|inventory producers?|inventory consumers?|affected consumers?|affected producers?|consumer list|producer list)\b", re.I),
    "compatibility_mode": re.compile(r"\b(?:backward compatible|backwards compatible|forward compatible|full compatibility|compatibility mode|breaking change|schema evolution|transitive compatibility)\b", re.I),
    "versioning": re.compile(r"\b(?:schema version|versioned schema|message version|contract version|v\d+|versioning|deprecation)\b", re.I),
    "fixture_tests": re.compile(r"\b(?:fixture tests?|contract fixtures?|golden messages?|sample payloads?|consumer fixtures?|producer fixtures?|schema tests?|compatibility tests?)\b", re.I),
    "replay_backfill": re.compile(r"\b(?:replay|backfill|reprocess|historical messages?|migration replay|event replay)\b", re.I),
    "dead_letter_handling": re.compile(r"\b(?:dead[- ]letter|dlq|poison message|failed message|error queue|undeliverable)\b", re.I),
    "monitoring": re.compile(r"\b(?:monitoring|metrics|alerts?|consumer lag|schema error rate|decode failures?|deserialization failures?|dashboard)\b", re.I),
}
_GUIDANCE = {
    "producer_consumer_inventory": "Inventory affected producers, consumers, topics, and queues before changing the message schema.",
    "compatibility_mode": "Define compatibility mode and breaking-change handling for the schema change.",
    "versioning": "Specify schema or message versioning, deprecation, and rollout rules.",
    "fixture_tests": "Add producer and consumer fixture or compatibility tests for the new message contract.",
    "replay_backfill": "Plan replay, backfill, or reprocessing behavior for historical messages.",
    "dead_letter_handling": "Define dead-letter handling for messages that cannot be decoded or migrated.",
    "monitoring": "Add monitoring and alerts for schema validation, deserialization errors, and consumer lag.",
}
_NO_IMPACT_RE = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:message schema|event schema|topic schema|queue event|protobuf|avro|json schema)\b"
    r".{0,100}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


def build_task_message_schema_compatibility_readiness_plan(source: Any) -> MessageSchemaCompatibilityReadinessPlan:
    """Build message schema compatibility readiness findings for relevant tasks."""
    return build_simple_readiness_plan(
        source,
        title="Task Message Schema Compatibility Readiness",
        signal_patterns=_SIGNAL_PATTERNS,
        path_signal_patterns=_PATH_SIGNAL_PATTERNS,
        criteria_patterns=_CRITERIA_PATTERNS,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT_RE,
    )


def analyze_task_message_schema_compatibility_readiness(source: Any) -> MessageSchemaCompatibilityReadinessPlan:
    return build_task_message_schema_compatibility_readiness_plan(source)


def extract_task_message_schema_compatibility_readiness(source: Any) -> MessageSchemaCompatibilityReadinessPlan:
    return build_task_message_schema_compatibility_readiness_plan(source)


def generate_task_message_schema_compatibility_readiness(source: Any) -> MessageSchemaCompatibilityReadinessPlan:
    return build_task_message_schema_compatibility_readiness_plan(source)


def recommend_task_message_schema_compatibility_readiness(source: Any) -> MessageSchemaCompatibilityReadinessPlan:
    return build_task_message_schema_compatibility_readiness_plan(source)


def summarize_task_message_schema_compatibility_readiness(source: Any) -> MessageSchemaCompatibilityReadinessPlan:
    return build_task_message_schema_compatibility_readiness_plan(source)


def task_message_schema_compatibility_readiness_plan_to_dict(report: MessageSchemaCompatibilityReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_message_schema_compatibility_readiness_plan_to_dict.__test__ = False


def task_message_schema_compatibility_readiness_plan_to_dicts(report: MessageSchemaCompatibilityReadinessPlan) -> list[dict[str, Any]]:
    return report.to_dicts()


task_message_schema_compatibility_readiness_plan_to_dicts.__test__ = False


def task_message_schema_compatibility_readiness_plan_to_markdown(report: MessageSchemaCompatibilityReadinessPlan) -> str:
    return report.to_markdown()


task_message_schema_compatibility_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "MessageSchemaCompatibilityReadinessFinding",
    "MessageSchemaCompatibilityReadinessPlan",
    "analyze_task_message_schema_compatibility_readiness",
    "build_task_message_schema_compatibility_readiness_plan",
    "extract_task_message_schema_compatibility_readiness",
    "generate_task_message_schema_compatibility_readiness",
    "recommend_task_message_schema_compatibility_readiness",
    "summarize_task_message_schema_compatibility_readiness",
    "task_message_schema_compatibility_readiness_plan_to_dict",
    "task_message_schema_compatibility_readiness_plan_to_dicts",
    "task_message_schema_compatibility_readiness_plan_to_markdown",
]
