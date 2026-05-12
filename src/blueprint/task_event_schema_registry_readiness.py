"""Assess readiness for task plans that change event schema registry contracts."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskEventSchemaRegistryReadinessFinding = SimpleReadinessRecord
TaskEventSchemaRegistryReadinessPlan = SimpleReadinessPlan

_SIGNALS = {
    "event_schema": re.compile(r"\b(?:event schema|schema for events?|event payload schema|domain event schema|integration event schema)\b", re.I),
    "schema_registry": re.compile(r"\b(?:schema registry|registry schema|confluent registry|avro registry|schema catalog)\b", re.I),
    "producer": re.compile(r"\b(?:producer|publisher|emits? events?|publishes? events?|upstream service)\b", re.I),
    "consumer": re.compile(r"\b(?:consumer|subscriber|downstream consumer|consumer group|downstream service)\b", re.I),
    "event_contract": re.compile(r"\b(?:event contract|message contract|topic contract|stream contract|producer contract|consumer contract)\b", re.I),
}
_PATH_SIGNALS = {
    "event_schema": re.compile(r"events?|schemas?|payloads?", re.I),
    "schema_registry": re.compile(r"schema[-_]?registry|registry|avro|avsc|proto", re.I),
    "producer": re.compile(r"producers?|publishers?", re.I),
    "consumer": re.compile(r"consumers?|subscribers?|consumer[-_]?groups?", re.I),
    "event_contract": re.compile(r"contracts?|topics?|streams?|messages?", re.I),
}
_CRITERIA = {
    "schema_ownership": re.compile(r"\b(?:schema owner|schema ownership|contract owner|owning team|owner team|domain owner|schema steward|approval owner)\b", re.I),
    "compatibility_policy": re.compile(r"\b(?:compatibility policy|compatibility mode|backward compatible|backwards compatible|forward compatible|full compatibility|breaking change|non[- ]breaking|schema evolution)\b", re.I),
    "versioning": re.compile(r"\b(?:schema version|versioned schema|event version|contract version|v\d+|versioning|deprecation|evolution rule)\b", re.I),
    "validation_tests": re.compile(r"\b(?:validation tests?|schema tests?|contract tests?|compatibility tests?|producer tests?|consumer tests?|fixture tests?|sample payloads?|golden events?)\b", re.I),
    "consumer_communication": re.compile(r"\b(?:consumer communication|consumer comms?|notify consumers?|consumer notification|subscriber notification|migration notice|release notes?|announce to consumers?|downstream communication)\b", re.I),
}
_GUIDANCE = {
    "schema_ownership": "Name the schema owner or owning team responsible for registry approval.",
    "compatibility_policy": "Define the schema registry compatibility policy and breaking-change handling.",
    "versioning": "Specify event schema versioning, deprecation, and evolution rules.",
    "validation_tests": "Add schema validation, compatibility, and producer/consumer contract tests.",
    "consumer_communication": "Plan communication to affected consumers with timing and migration action.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:event schema|schema registry|event contract|producer|consumer)\b"
    r".{0,100}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


def build_task_event_schema_registry_readiness_plan(source: Any) -> TaskEventSchemaRegistryReadinessPlan:
    return build_simple_readiness_plan(
        source,
        title="Task Event Schema Registry Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_event_schema_registry_readiness(source: Any) -> TaskEventSchemaRegistryReadinessPlan:
    return build_task_event_schema_registry_readiness_plan(source)


def extract_task_event_schema_registry_readiness(source: Any) -> TaskEventSchemaRegistryReadinessPlan:
    return build_task_event_schema_registry_readiness_plan(source)


def generate_task_event_schema_registry_readiness(source: Any) -> TaskEventSchemaRegistryReadinessPlan:
    return build_task_event_schema_registry_readiness_plan(source)


def derive_task_event_schema_registry_readiness(source: Any) -> TaskEventSchemaRegistryReadinessPlan:
    return build_task_event_schema_registry_readiness_plan(source)


def summarize_task_event_schema_registry_readiness(source: Any) -> TaskEventSchemaRegistryReadinessPlan:
    return build_task_event_schema_registry_readiness_plan(source)


def recommend_task_event_schema_registry_readiness(source: Any) -> TaskEventSchemaRegistryReadinessPlan:
    return build_task_event_schema_registry_readiness_plan(source)


def task_event_schema_registry_readiness_plan_to_dict(report: TaskEventSchemaRegistryReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_event_schema_registry_readiness_plan_to_dict.__test__ = False


def task_event_schema_registry_readiness_plan_to_dicts(report: TaskEventSchemaRegistryReadinessPlan) -> list[dict[str, Any]]:
    return report.to_dicts()


task_event_schema_registry_readiness_plan_to_dicts.__test__ = False


def task_event_schema_registry_readiness_plan_to_markdown(report: TaskEventSchemaRegistryReadinessPlan) -> str:
    return report.to_markdown()


task_event_schema_registry_readiness_plan_to_markdown.__test__ = False


__all__ = [
    "TaskEventSchemaRegistryReadinessFinding",
    "TaskEventSchemaRegistryReadinessPlan",
    "analyze_task_event_schema_registry_readiness",
    "build_task_event_schema_registry_readiness_plan",
    "derive_task_event_schema_registry_readiness",
    "extract_task_event_schema_registry_readiness",
    "generate_task_event_schema_registry_readiness",
    "recommend_task_event_schema_registry_readiness",
    "summarize_task_event_schema_registry_readiness",
    "task_event_schema_registry_readiness_plan_to_dict",
    "task_event_schema_registry_readiness_plan_to_dicts",
    "task_event_schema_registry_readiness_plan_to_markdown",
]
