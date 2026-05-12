"""Assess readiness for shadow traffic and dark-read rollout tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskShadowTrafficReadinessPlan = SimpleReadinessPlan
TaskShadowTrafficReadinessRecord = SimpleReadinessRecord
TaskShadowTrafficReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "shadow_traffic": re.compile(r"\b(?:shadow traffic|shadow mode|shadow request|shadow read|shadow write|dark read|dark launch|mirrored traffic|traffic mirroring|mirror production|request replay|replay traffic|parallel run)\b", re.I),
    "mirrored_reads": re.compile(r"\b(?:mirror|mirrored|duplicate|fan[- ]?out|tee)\b.{0,80}\b(?:read|request|traffic|query)\b|\b(?:dark reads?|shadow reads?)\b", re.I),
    "replay_rollout": re.compile(r"\b(?:replay|backfill replay|production replay|recorded traffic)\b", re.I),
}
_PATH_SIGNALS = {
    "shadow_traffic": re.compile(r"(?:shadow|dark read|traffic mirror|mirror|replay|parallel run)", re.I),
}
_CRITERIA = {
    "mirroring_scope": re.compile(r"\b(?:scope|cohort|percentage|sample|route|endpoint|tenant|allowlist|traffic slice|read path)\b", re.I),
    "data_isolation": re.compile(r"\b(?:read[- ]?only|no writes|suppress writes|data isolation|isolated|sandbox|side effect|do not persist|write block)\b", re.I),
    "comparison_metrics": re.compile(r"\b(?:compare|comparison|parity|diff|mismatch|accuracy|latency|error rate|metric)\b", re.I),
    "kill_switch": re.compile(r"\b(?:kill switch|disable|feature flag|rollback|turn off|abort|emergency stop)\b", re.I),
    "observability": re.compile(r"\b(?:observability|monitor|alert|dashboard|log|trace|telemetry|span)\b", re.I),
}
_GUIDANCE = {
    "mirroring_scope": "Define the mirrored traffic scope by route, cohort, tenant, percentage, or sample.",
    "data_isolation": "State how shadow traffic is isolated from writes and side effects.",
    "comparison_metrics": "Add comparison metrics for old vs new behavior.",
    "kill_switch": "Add a kill switch or rollback trigger for stopping shadow traffic.",
    "observability": "Add dashboards, logs, traces, or alerts for the shadow run.",
}
_NO_IMPACT = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:shadow|dark read|mirrored traffic|replay)\b.{0,80}\b(?:required|needed|impact|changes?)\b", re.I)


def build_task_shadow_traffic_readiness_plan(source: Any) -> TaskShadowTrafficReadinessPlan:
    return build_simple_readiness_plan(
        source,
        title="Task Shadow Traffic Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_shadow_traffic_readiness(source: Any) -> TaskShadowTrafficReadinessPlan:
    return build_task_shadow_traffic_readiness_plan(source)


def extract_task_shadow_traffic_readiness(source: Any) -> TaskShadowTrafficReadinessPlan:
    return build_task_shadow_traffic_readiness_plan(source)


def generate_task_shadow_traffic_readiness(source: Any) -> TaskShadowTrafficReadinessPlan:
    return build_task_shadow_traffic_readiness_plan(source)


def derive_task_shadow_traffic_readiness(source: Any) -> TaskShadowTrafficReadinessPlan:
    return build_task_shadow_traffic_readiness_plan(source)


def summarize_task_shadow_traffic_readiness(source: Any) -> TaskShadowTrafficReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_task_shadow_traffic_readiness_plan(source)


def recommend_task_shadow_traffic_readiness(source: Any) -> tuple[TaskShadowTrafficReadinessRecord, ...]:
    return build_task_shadow_traffic_readiness_plan(source).records


def task_shadow_traffic_readiness_plan_to_dict(result: TaskShadowTrafficReadinessPlan) -> dict[str, Any]:
    return result.to_dict()


task_shadow_traffic_readiness_plan_to_dict.__test__ = False


def task_shadow_traffic_readiness_plan_to_dicts(
    result: TaskShadowTrafficReadinessPlan | Iterable[TaskShadowTrafficReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [record.to_dict() for record in result]


task_shadow_traffic_readiness_plan_to_dicts.__test__ = False
task_shadow_traffic_readiness_to_dicts = task_shadow_traffic_readiness_plan_to_dicts
task_shadow_traffic_readiness_to_dicts.__test__ = False


def task_shadow_traffic_readiness_plan_to_markdown(result: TaskShadowTrafficReadinessPlan) -> str:
    return result.to_markdown()


task_shadow_traffic_readiness_plan_to_markdown.__test__ = False

