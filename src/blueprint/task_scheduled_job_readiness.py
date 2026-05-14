"""Assess readiness for scheduled job implementation tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan

TaskScheduledJobReadinessPlan = SimpleReadinessPlan
TaskScheduledJobReadinessRecord = SimpleReadinessRecord
TaskScheduledJobReadinessFinding = SimpleReadinessRecord
TaskScheduledJobReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "scheduled_job": re.compile(r"\b(?:scheduled jobs?|job scheduler|scheduler|cron jobs?|cron|recurring jobs?|interval jobs?|timer|batch schedule|calendar[- ]triggered)\b", re.I),
    "recurrence": re.compile(r"\b(?:recurring|recurrence|interval|every \d+|daily|hourly|weekly|monthly|calendar trigger)\b", re.I),
    "batch_schedule": re.compile(r"\b(?:batch schedule|scheduled batch|nightly batch|background schedule)\b", re.I),
}
_PATH_SIGNALS = {
    "scheduled_job": re.compile(r"(?:cron|scheduler|scheduled[_-]?jobs?|jobs?/scheduled|recurring[_-]?jobs?)", re.I),
    "recurrence": re.compile(r"(?:recurring|interval|timer|calendar[_-]?trigger)", re.I),
    "batch_schedule": re.compile(r"(?:batch[_-]?schedule|nightly[_-]?batch)", re.I),
}
_CRITERIA = {
    "schedule_definition": re.compile(r"\b(?:schedule definition|cron expression|cron schedule|interval|recurrence|timezone|time zone|calendar trigger|daily|hourly|weekly)\b", re.I),
    "idempotency": re.compile(r"\b(?:idempotent|idempotency|safe to rerun|dedupe|de-?duplicate|unique key|upsert|exactly once)\b", re.I),
    "concurrency_overlap_handling": re.compile(r"\b(?:concurrency|overlap|lock|mutex|single flight|lease|advisory lock|skip if running|parallel runs?)\b", re.I),
    "retry_failure_behavior": re.compile(r"\b(?:retry|retries|failure behavior|failure handling|backoff|dead letter|dlq|timeout|poison)\b", re.I),
    "observability": re.compile(r"\b(?:observability|monitoring|metrics?|logs?|tracing|alerts?|dashboard)\b", re.I),
    "owner_runbook": re.compile(r"\b(?:owner|dri|responsible team|runbook|on-call|operator|maintainer)\b", re.I),
    "validation_coverage": re.compile(r"\b(?:validation|tests?|pytest|unit tests?|integration tests?|scheduler tests?|cron tests?|acceptance tests?)\b", re.I),
}
_GUIDANCE = {
    "schedule_definition": "Define the schedule with cron expression, interval, recurrence, timezone, or calendar trigger details.",
    "idempotency": "Make the job idempotent and safe to rerun with dedupe, unique keys, upserts, or equivalent safeguards.",
    "concurrency_overlap_handling": "Document concurrency and overlap handling such as locks, leases, mutexes, or skip-if-running behavior.",
    "retry_failure_behavior": "Specify retry, backoff, timeout, failure handling, dead-letter, or poison-message behavior.",
    "observability": "Add observability with logs, metrics, tracing, dashboards, monitoring, or alerts.",
    "owner_runbook": "Name the owner, DRI, responsible team, on-call, maintainer, or operational runbook.",
    "validation_coverage": "Add validation coverage with unit, integration, scheduler, cron, pytest, or acceptance tests.",
}
_NO_IMPACT = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:scheduled jobs?|cron|scheduler|recurring jobs?)\b.{0,80}\b(?:impact|changes?|planned|scope|required|needed)\b", re.I)


def build_task_scheduled_job_readiness_plan(source: Any) -> TaskScheduledJobReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Scheduled Job Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_scheduled_job_readiness = build_task_scheduled_job_readiness_plan
extract_task_scheduled_job_readiness = build_task_scheduled_job_readiness_plan
generate_task_scheduled_job_readiness = build_task_scheduled_job_readiness_plan
derive_task_scheduled_job_readiness = build_task_scheduled_job_readiness_plan
summarize_task_scheduled_job_readiness = build_task_scheduled_job_readiness_plan
summarize_task_scheduled_job_readiness_plan = build_task_scheduled_job_readiness_plan


def recommend_task_scheduled_job_readiness(source: Any) -> tuple[TaskScheduledJobReadinessRecord, ...]:
    return build_task_scheduled_job_readiness_plan(source).records


def task_scheduled_job_readiness_plan_to_dict(result: TaskScheduledJobReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_scheduled_job_readiness_plan_to_dict.__test__ = False


def task_scheduled_job_readiness_plan_to_dicts(
    result: TaskScheduledJobReadinessPlan | Iterable[TaskScheduledJobReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_scheduled_job_readiness_plan_to_dicts.__test__ = False
task_scheduled_job_readiness_to_dicts = task_scheduled_job_readiness_plan_to_dicts
task_scheduled_job_readiness_to_dicts.__test__ = False


def task_scheduled_job_readiness_plan_to_markdown(result: TaskScheduledJobReadinessPlan) -> str:
    return result.to_markdown()


task_scheduled_job_readiness_plan_to_markdown.__test__ = False
