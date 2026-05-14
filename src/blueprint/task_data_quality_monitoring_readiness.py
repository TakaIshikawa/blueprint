"""Assess readiness for task-level data quality monitoring work."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskDataQualityMonitoringReadinessPlan = SimpleReadinessPlan
TaskDataQualityMonitoringReadinessRecord = SimpleReadinessRecord
TaskDataQualityMonitoringReadinessFinding = SimpleReadinessRecord
TaskDataQualityMonitoringReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "data_quality_monitoring": re.compile(r"\b(?:data quality monitoring|data quality checks?|dq monitoring|quality monitors?|data validation monitors?)\b", re.I),
    "freshness_completeness": re.compile(r"\b(?:freshness|staleness|completeness|missing values?|null rate|required field coverage)\b", re.I),
    "reconciliation_anomaly_schema": re.compile(r"\b(?:duplicate detection|reconciliation|anomaly alerts?|schema drift|data validation)\b", re.I),
}
_PATH_SIGNALS = {
    "data_quality_monitoring": re.compile(r"data[_-]?quality|dq|quality[_-]?monitor|validation[_-]?monitor", re.I),
    "freshness_completeness": re.compile(r"freshness|completeness|null[_-]?rate|coverage", re.I),
    "reconciliation_anomaly_schema": re.compile(r"reconciliation|anomaly|schema[_-]?drift|duplicates?", re.I),
}
_CRITERIA = {
    "metric_ownership": re.compile(r"\b(?:metric ownership|owner|owned by|responsible team|dri|data steward|on-call|accountable)\b", re.I),
    "thresholds": re.compile(r"\b(?:thresholds?|sla|freshness limit|completeness target|null rate|alert threshold|\d+\s*(?:%|percent|minutes?|hours?))\b", re.I),
    "scan_scope": re.compile(r"\b(?:sampling|sampled|full scan|full-scan|scan scope|all rows|partition scope|table scope|dataset scope)\b", re.I),
    "alert_routing": re.compile(r"\b(?:alert routing|alert route|pagerduty|slack|email alert|notification channel|page on-call|ticket routing)\b", re.I),
    "remediation_runbook": re.compile(r"\b(?:remediation runbook|runbook|playbook|repair workflow|triage steps|incident steps|remediation steps)\b", re.I),
    "backfill_strategy": re.compile(r"\b(?:backfill strategy|backfill|reprocess|replay|repair historical|correction job|rerun pipeline)\b", re.I),
}
_GUIDANCE = {
    "metric_ownership": "Name the metric owner, responsible team, DRI, steward, or on-call path.",
    "thresholds": "Define freshness, completeness, duplicate, reconciliation, anomaly, or schema-drift thresholds.",
    "scan_scope": "Specify sampling, full-scan, partition, table, dataset, or row scan scope.",
    "alert_routing": "Route alerts to PagerDuty, Slack, email, tickets, notifications, or an on-call path.",
    "remediation_runbook": "Add a remediation runbook, playbook, triage steps, or repair workflow.",
    "backfill_strategy": "Define backfill, reprocess, replay, correction, or historical repair strategy.",
}
_NO_IMPACT = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:data quality|dq|freshness|completeness|validation)\b.{0,80}\b(?:monitoring|checks?|changes?|required|planned|impact)\b", re.I)


def build_task_data_quality_monitoring_readiness_plan(source: Any) -> TaskDataQualityMonitoringReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Data Quality Monitoring Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_data_quality_monitoring_readiness = build_task_data_quality_monitoring_readiness_plan
extract_task_data_quality_monitoring_readiness = build_task_data_quality_monitoring_readiness_plan
generate_task_data_quality_monitoring_readiness = build_task_data_quality_monitoring_readiness_plan
derive_task_data_quality_monitoring_readiness = build_task_data_quality_monitoring_readiness_plan
summarize_task_data_quality_monitoring_readiness = build_task_data_quality_monitoring_readiness_plan
summarize_task_data_quality_monitoring_readiness_plan = build_task_data_quality_monitoring_readiness_plan


def recommend_task_data_quality_monitoring_readiness(source: Any) -> tuple[TaskDataQualityMonitoringReadinessRecord, ...]:
    return build_task_data_quality_monitoring_readiness_plan(source).records


def task_data_quality_monitoring_readiness_plan_to_dict(plan: TaskDataQualityMonitoringReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


task_data_quality_monitoring_readiness_plan_to_dict.__test__ = False


def task_data_quality_monitoring_readiness_plan_to_dicts(plan: TaskDataQualityMonitoringReadinessPlan | Iterable[TaskDataQualityMonitoringReadinessRecord]) -> list[dict[str, Any]]:
    if isinstance(plan, SimpleReadinessPlan):
        return plan.to_dicts()
    return [record.to_dict() for record in plan]


task_data_quality_monitoring_readiness_plan_to_dicts.__test__ = False
task_data_quality_monitoring_readiness_to_dicts = task_data_quality_monitoring_readiness_plan_to_dicts
task_data_quality_monitoring_readiness_to_dicts.__test__ = False


def task_data_quality_monitoring_readiness_plan_to_markdown(plan: TaskDataQualityMonitoringReadinessPlan) -> str:
    return plan.to_markdown()


task_data_quality_monitoring_readiness_plan_to_markdown.__test__ = False

