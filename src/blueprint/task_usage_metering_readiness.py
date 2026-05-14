"""Assess readiness for usage metering execution tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskUsageMeteringReadinessPlan = SimpleReadinessPlan
TaskUsageMeteringReadinessRecord = SimpleReadinessRecord
TaskUsageMeteringReadinessFinding = SimpleReadinessRecord
TaskUsageMeteringReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "usage_metering": re.compile(r"\b(?:usage metering|metered usage|metering pipeline|usage[- ]based billing|consumption billing|usage billing|metered billing)\b", re.I),
    "usage_events": re.compile(r"\b(?:usage events?|metering events?|consumption events?|usage ingestion|usage capture|event source)\b", re.I),
    "billable_counters": re.compile(r"\b(?:billable counters?|billing counters?|metered counters?|billable units?|usage counters?|seat counters?|api call counters?)\b", re.I),
    "quota_consumption_tracking": re.compile(r"\b(?:quotas?|quota enforcement|quota usage|consumption tracking|usage tracking|remaining usage|usage limit|entitlement usage)\b", re.I),
}
_PATH_SIGNALS = {
    "usage_metering": re.compile(r"(?:usage[_-]?meter|metering|metered[_-]?usage|usage[_-]?billing|consumption[_-]?billing)", re.I),
    "usage_events": re.compile(r"(?:usage[_-]?events?|metering[_-]?events?|consumption[_-]?events?|event[_-]?source|usage[_-]?ingest)", re.I),
    "billable_counters": re.compile(r"(?:billable[_-]?counters?|billing[_-]?counters?|usage[_-]?counters?|billable[_-]?units?)", re.I),
    "quota_consumption_tracking": re.compile(r"(?:quota|consumption[_-]?tracking|usage[_-]?tracking|usage[_-]?limit|entitlement[_-]?usage)", re.I),
}
_CRITERIA = {
    "event_source": re.compile(r"\b(?:event source|source event|usage event source|metering source|producer|source system|authoritative source|capture point)\b", re.I),
    "idempotency_deduplication": re.compile(r"\b(?:idempotenc\w*|deduplicat\w*|dedupe|duplicate event|event id|idempotency key|exactly once|at[- ]least once)\b", re.I),
    "aggregation_window": re.compile(r"\b(?:aggregation window|billing window|metering window|rollup window|daily rollup|hourly rollup|monthly rollup|period boundary|window boundary)\b", re.I),
    "billing_reconciliation": re.compile(r"\b(?:billing reconciliation|invoice reconciliation|ledger reconciliation|invoice match|billing audit|charge reconciliation|reconcile usage)\b", re.I),
    "quota_enforcement": re.compile(r"\b(?:quota enforcement|enforce quota|usage limit enforcement|limit enforcement|hard limit|soft limit|quota gate|entitlement check)\b", re.I),
    "backfill_replay_behavior": re.compile(r"\b(?:backfill|replay|reprocess|late events?|missed events?|historical usage|catch[- ]up|recompute usage)\b", re.I),
    "observability": re.compile(r"\b(?:observability|metrics?|dashboard|alerts?|logs?|tracing|monitoring|audit trail|usage anomaly)\b", re.I),
    "tests": re.compile(r"\b(?:tests?|pytest|unit tests?|integration tests?|contract tests?|reconciliation tests?|quota tests?|replay tests?|metering tests?)\b", re.I),
}
_GUIDANCE = {
    "event_source": "Identify the authoritative event source, producer, source system, or capture point for usage events.",
    "idempotency_deduplication": "Define idempotency, deduplication, event IDs, or duplicate-event handling.",
    "aggregation_window": "Specify aggregation, rollup, billing, metering, or period-boundary windows.",
    "billing_reconciliation": "Add billing, invoice, ledger, charge, or usage reconciliation checks.",
    "quota_enforcement": "Define quota, entitlement, hard-limit, soft-limit, or usage-limit enforcement behavior.",
    "backfill_replay_behavior": "Document backfill, replay, late-event, missed-event, or historical-usage behavior.",
    "observability": "Add metrics, dashboards, alerts, logs, tracing, monitoring, or anomaly detection.",
    "tests": "Add unit, integration, contract, reconciliation, quota, replay, or metering tests.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:usage metering|metered usage|usage events?|billable counters?|quotas?|consumption tracking|usage billing)\b.{0,80}\b(?:impact|changes?|planned|scope|required|needed)\b",
    re.I,
)


def build_task_usage_metering_readiness_plan(source: Any) -> TaskUsageMeteringReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Usage Metering Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_usage_metering_readiness = build_task_usage_metering_readiness_plan
extract_task_usage_metering_readiness = build_task_usage_metering_readiness_plan
generate_task_usage_metering_readiness = build_task_usage_metering_readiness_plan
derive_task_usage_metering_readiness = build_task_usage_metering_readiness_plan
summarize_task_usage_metering_readiness = build_task_usage_metering_readiness_plan
summarize_task_usage_metering_readiness_plan = build_task_usage_metering_readiness_plan


def recommend_task_usage_metering_readiness(source: Any) -> tuple[TaskUsageMeteringReadinessRecord, ...]:
    return build_task_usage_metering_readiness_plan(source).records


def task_usage_metering_readiness_plan_to_dict(plan: TaskUsageMeteringReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


task_usage_metering_readiness_plan_to_dict.__test__ = False


def task_usage_metering_readiness_plan_to_dicts(
    plan: TaskUsageMeteringReadinessPlan | Iterable[TaskUsageMeteringReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(plan, SimpleReadinessPlan):
        return plan.to_dicts()
    return [record.to_dict() for record in plan]


task_usage_metering_readiness_plan_to_dicts.__test__ = False
task_usage_metering_readiness_to_dicts = task_usage_metering_readiness_plan_to_dicts
task_usage_metering_readiness_to_dicts.__test__ = False


def task_usage_metering_readiness_plan_to_markdown(plan: TaskUsageMeteringReadinessPlan) -> str:
    return plan.to_markdown()


task_usage_metering_readiness_plan_to_markdown.__test__ = False
