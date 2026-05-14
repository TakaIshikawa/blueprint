"""Assess readiness for webhook payload validation tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan

TaskWebhookPayloadValidationReadinessPlan = SimpleReadinessPlan
TaskWebhookPayloadValidationReadinessRecord = SimpleReadinessRecord

_SIGNALS = {
    "webhook_payload_validation": re.compile(r"\b(?:webhook payload validation|payload validation|invalid payload rejection|validate webhook payloads?)\b", re.I),
    "schema_contract": re.compile(r"\b(?:schema validation|payload schema|event contract|webhook contract|required fields?|payload shape)\b", re.I),
    "versioned_payload": re.compile(r"\b(?:versioned payloads?|payload versions?|event type validation|event versions?)\b", re.I),
}
_PATH_SIGNALS = {
    "webhook_payload_validation": re.compile(r"(?:webhooks?|payload|fixtures?)", re.I),
    "schema_contract": re.compile(r"(?:schema|contract|event[_-]?contract)", re.I),
    "versioned_payload": re.compile(r"(?:versioned|v[0-9]+|event[_-]?types?)", re.I),
}
_CRITERIA = {
    "schema_source_of_truth": re.compile(r"\b(?:schema source|source of truth|canonical schema|json schema|openapi|schema registry|payload schema)\b", re.I),
    "required_field_checks": re.compile(r"\b(?:required fields?|required field checks?|mandatory fields?|field validation|payload shape)\b", re.I),
    "version_handling": re.compile(r"\b(?:version handling|versioned payloads?|payload version|event version|backward compatibility)\b", re.I),
    "failure_response": re.compile(r"\b(?:failure response|invalid payload rejection|reject invalid|400|422|bad request|error response)\b", re.I),
    "logging_auditability": re.compile(r"\b(?:logging|audit|auditability|event log|validation log|traceability)\b", re.I),
    "replay_fixture_coverage": re.compile(r"\b(?:replay|fixtures?|golden payloads?|sample payloads?|contract fixtures?)\b", re.I),
    "validation_tests": re.compile(r"\b(?:validation tests?|contract tests?|unit tests?|integration tests?|pytest|webhook tests?)\b", re.I),
}
_GUIDANCE = {
    "schema_source_of_truth": "Identify the schema source of truth such as JSON Schema, OpenAPI, schema registry, or canonical payload schema.",
    "required_field_checks": "Define required field checks, mandatory fields, field validation, or payload shape rules.",
    "version_handling": "Document version handling for versioned payloads, event versions, and compatibility.",
    "failure_response": "Specify failure response behavior for invalid payload rejection, 400, 422, or structured errors.",
    "logging_auditability": "Add logging, auditability, event logs, validation logs, or traceability.",
    "replay_fixture_coverage": "Add replay, fixture, golden payload, sample payload, or contract fixture coverage.",
    "validation_tests": "Add validation, contract, unit, integration, pytest, or webhook tests.",
}
_NO_IMPACT = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:webhook payload validation|payload validation|webhook schema)\b.{0,80}\b(?:impact|changes?|planned|scope|required|needed)\b", re.I)


def build_task_webhook_payload_validation_readiness_plan(source: Any) -> TaskWebhookPayloadValidationReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(source, title="Task Webhook Payload Validation Readiness", signal_patterns=_SIGNALS, path_signal_patterns=_PATH_SIGNALS, criteria_patterns=_CRITERIA, criterion_guidance=_GUIDANCE, no_impact_pattern=_NO_IMPACT)


analyze_task_webhook_payload_validation_readiness = build_task_webhook_payload_validation_readiness_plan
extract_task_webhook_payload_validation_readiness = build_task_webhook_payload_validation_readiness_plan
generate_task_webhook_payload_validation_readiness = build_task_webhook_payload_validation_readiness_plan
derive_task_webhook_payload_validation_readiness = build_task_webhook_payload_validation_readiness_plan
summarize_task_webhook_payload_validation_readiness = build_task_webhook_payload_validation_readiness_plan
summarize_task_webhook_payload_validation_readiness_plan = build_task_webhook_payload_validation_readiness_plan


def recommend_task_webhook_payload_validation_readiness(source: Any) -> tuple[TaskWebhookPayloadValidationReadinessRecord, ...]:
    return build_task_webhook_payload_validation_readiness_plan(source).records


def task_webhook_payload_validation_readiness_plan_to_dict(result: TaskWebhookPayloadValidationReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_webhook_payload_validation_readiness_plan_to_dict.__test__ = False


def task_webhook_payload_validation_readiness_plan_to_dicts(result: TaskWebhookPayloadValidationReadinessPlan | Iterable[TaskWebhookPayloadValidationReadinessRecord]) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_webhook_payload_validation_readiness_plan_to_dicts.__test__ = False
task_webhook_payload_validation_readiness_to_dicts = task_webhook_payload_validation_readiness_plan_to_dicts


def task_webhook_payload_validation_readiness_plan_to_markdown(result: TaskWebhookPayloadValidationReadinessPlan) -> str:
    return result.to_markdown()


task_webhook_payload_validation_readiness_plan_to_markdown.__test__ = False
