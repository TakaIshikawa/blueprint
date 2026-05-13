"""Assess blue-green deployment readiness for execution tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import (
    SimpleReadinessPlan,
    SimpleReadinessRecord,
    build_simple_readiness_plan,
)


TaskBlueGreenDeploymentReadinessPlan = SimpleReadinessPlan
TaskBlueGreenDeploymentReadinessRecord = SimpleReadinessRecord
TaskBlueGreenDeploymentReadinessFinding = SimpleReadinessRecord

_SIGNALS = {
    "blue_green_deployment": re.compile(
        r"\b(?:blue[- ]green|blue/green|green environment|blue environment|parallel environment|"
        r"deployment cutover|traffic switch|traffic shifting|switch traffic)\b",
        re.I,
    ),
    "release_cutover": re.compile(
        r"\b(?:cutover|rollout|release deployment|load balancer|service mesh|route traffic|dns switch)\b",
        re.I,
    ),
}
_PATH_SIGNALS = {
    "blue_green_deployment": re.compile(r"blue|green|deployment|cutover", re.I),
    "release_cutover": re.compile(r"traffic|routing|loadbalancer|lb|service.?mesh|dns|deploy", re.I),
}
_CRITERIA = {
    "parallel_environment_provisioning": re.compile(
        r"\b(?:(?:provision|equivalent|parallel|standby|duplicate).{0,80}(?:environment|stack|cluster|infra)|"
        r"(?:environment|stack|cluster|infra).{0,80}(?:provision|equivalent|parallel|standby|duplicate)|"
        r"provision.{0,40}(?:green|parallel|second)|blue[- ]green environment)\b",
        re.I,
    ),
    "traffic_switch_strategy": re.compile(
        r"\b(?:(?:traffic|routing|load balancer|service mesh|dns).{0,80}(?:switch|shift|route|cutover|percent|weight)|"
        r"(?:switch|shift|route|cutover|percent|weight).{0,80}(?:traffic|routing|load balancer|service mesh|dns)|"
        r"weighted routing|traffic percentage|endpoint switch)\b",
        re.I,
    ),
    "health_checks": re.compile(
        r"\b(?:(?:health|readiness|liveness).{0,80}(?:checks?|probes?|endpoint|validation|gate|verify)|"
        r"(?:checks?|probes?|endpoint|validation|gate|verify).{0,80}(?:health|readiness|liveness)|"
        r"service health|synthetic health)\b",
        re.I,
    ),
    "data_compatibility": re.compile(
        r"\b(?:(?:data|database|schema|migration).{0,80}(?:compatib|backward|forward|dual[- ]write|version|expand)|"
        r"(?:compatib|backward|forward|dual[- ]write|version|expand).{0,80}(?:data|database|schema|migration)|"
        r"expand[- ]contract|backward compatible schema)\b",
        re.I,
    ),
    "rollback_trigger": re.compile(
        r"\b(?:(?:rollback|roll back|revert|failback|switch back).{0,80}(?:trigger|threshold|condition|plan|step|automation)|"
        r"(?:trigger|threshold|condition|plan|step|automation).{0,80}(?:rollback|roll back|revert|failback|switch back)|"
        r"backout criteria|abort criteria)\b",
        re.I,
    ),
    "observability": re.compile(
        r"\b(?:(?:monitoring|observability|dashboard|metric|alert|log|trace).{0,80}(?:deployment|release|cutover|traffic|error|latency)|"
        r"(?:deployment|release|cutover|traffic|error|latency).{0,80}(?:monitoring|observability|dashboard|metric|alert|log|trace)|"
        r"error rate|latency dashboard|slo alert)\b",
        re.I,
    ),
    "ownership": re.compile(
        r"\b(?:(?:owner|dri|sre|ops|operator|on[- ]call|release manager).{0,80}(?:approval|handoff|responsib|runbook|sign[- ]off)|"
        r"(?:approval|handoff|responsib|runbook|sign[- ]off).{0,80}(?:owner|dri|sre|ops|operator|on[- ]call|release manager)|"
        r"owning team|deployment owner)\b",
        re.I,
    ),
    "validation_evidence": re.compile(
        r"\b(?:(?:smoke|synthetic|validation|test|qa|evidence).{0,80}(?:green|deployment|release|cutover|pass|artifact|sign[- ]off)|"
        r"(?:green|deployment|release|cutover).{0,80}(?:smoke|synthetic|validation|test|qa|evidence|sign[- ]off)|"
        r"post[- ]deployment test|validation artifact|test evidence|qa sign[- ]off)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "parallel_environment_provisioning": "Describe how the parallel blue and green environments, stacks, or clusters will be provisioned and kept equivalent.",
    "traffic_switch_strategy": "Define the traffic switch strategy, including routing mechanism, cutover order, and percentage or weighted shift behavior.",
    "health_checks": "Add health, readiness, liveness, or synthetic checks that must pass before and after traffic moves.",
    "data_compatibility": "Document data and schema compatibility, including backward-compatible migrations, dual writes, or version constraints.",
    "rollback_trigger": "Define rollback triggers, thresholds, ownership, and exact steps to switch traffic back.",
    "observability": "Specify dashboards, metrics, alerts, logs, or traces used to observe the deployment and traffic switch.",
    "ownership": "Name the deployment owner, approver, on-call role, or operating team responsible for the cutover.",
    "validation_evidence": "Capture validation evidence such as smoke test results, QA sign-off, synthetic checks, or release artifacts.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:blue[- ]green|blue/green|deployment cutover|traffic switch)\b"
    r".{0,80}\b(?:scope|impact|changes?|required|needed|involved)\b",
    re.I,
)


def build_task_blue_green_deployment_readiness_plan(source: Any) -> TaskBlueGreenDeploymentReadinessPlan:
    """Build criterion-level readiness findings for blue-green deployment tasks."""
    return build_simple_readiness_plan(
        source,
        title="Task Blue Green Deployment Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_blue_green_deployment_readiness(source: Any) -> TaskBlueGreenDeploymentReadinessPlan:
    return build_task_blue_green_deployment_readiness_plan(source)


def summarize_task_blue_green_deployment_readiness(source: Any) -> TaskBlueGreenDeploymentReadinessPlan:
    return build_task_blue_green_deployment_readiness_plan(source)


def extract_task_blue_green_deployment_readiness(source: Any) -> TaskBlueGreenDeploymentReadinessPlan:
    return build_task_blue_green_deployment_readiness_plan(source)


def generate_task_blue_green_deployment_readiness(source: Any) -> TaskBlueGreenDeploymentReadinessPlan:
    return build_task_blue_green_deployment_readiness_plan(source)


def recommend_task_blue_green_deployment_readiness(source: Any) -> TaskBlueGreenDeploymentReadinessPlan:
    return build_task_blue_green_deployment_readiness_plan(source)


def plan_task_blue_green_deployment_readiness(source: Any) -> TaskBlueGreenDeploymentReadinessPlan:
    return build_task_blue_green_deployment_readiness_plan(source)


def task_blue_green_deployment_readiness_plan_to_dict(
    result: TaskBlueGreenDeploymentReadinessPlan,
) -> dict[str, Any]:
    return result.to_dict()


task_blue_green_deployment_readiness_plan_to_dict.__test__ = False


def task_blue_green_deployment_readiness_to_dict(
    result: TaskBlueGreenDeploymentReadinessPlan,
) -> dict[str, Any]:
    return result.to_dict()


task_blue_green_deployment_readiness_to_dict.__test__ = False


def task_blue_green_deployment_readiness_plan_to_dicts(result: Any) -> list[dict[str, Any]]:
    if isinstance(result, SimpleReadinessPlan):
        return result.to_dicts()
    return [item.to_dict() for item in result]


task_blue_green_deployment_readiness_plan_to_dicts.__test__ = False


def task_blue_green_deployment_readiness_plan_to_markdown(
    result: TaskBlueGreenDeploymentReadinessPlan,
) -> str:
    return result.to_markdown()


task_blue_green_deployment_readiness_plan_to_markdown.__test__ = False
