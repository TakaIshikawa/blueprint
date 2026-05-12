"""Assess readiness for SLO error-budget tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskSloErrorBudgetReadinessFinding = SimpleReadinessRecord
TaskSloErrorBudgetReadinessPlan = SimpleReadinessPlan

_SIGNALS = {
    "error_budget": re.compile(r"\b(?:error budget|reliability budget|availability budget|budget burn|remaining budget)\b", re.I),
    "burn_rate": re.compile(r"\b(?:burn rate|burn-rate|fast burn|slow burn|multi[- ]window burn|budget exhaustion)\b", re.I),
    "slo_gate": re.compile(r"\b(?:slo gate|release gate|deployment gate|error budget gate|freeze releases?|block deploy)\b", re.I),
    "slo_policy": re.compile(r"\b(?:slo policy|service level objective policy|reliability policy|slo governance)\b", re.I),
}
_PATH_SIGNALS = {
    "error_budget": re.compile(r"error[-_]?budget|reliability[-_]?budget", re.I),
    "burn_rate": re.compile(r"burn[-_]?rate|budget[-_]?burn", re.I),
    "slo_gate": re.compile(r"slo[-_]?gate|release[-_]?gate|deployment[-_]?gate", re.I),
    "slo_policy": re.compile(r"slo|reliability[-_]?policy", re.I),
}
_CRITERIA = {
    "sli_slo_definition": re.compile(r"\b(?:sli|service level indicator|slo|service level objective|availability target|latency target|success rate)\b", re.I),
    "burn_rate_thresholds": re.compile(r"\b(?:burn rate threshold|burn-rate threshold|fast burn|slow burn|threshold|2x|5x|10x|multi[- ]window)\b", re.I),
    "alert_routing": re.compile(r"\b(?:alert routing|alerts?|pagerduty|on[- ]call|notify|notification channel|slack channel|route to)\b", re.I),
    "release_gate_behavior": re.compile(r"\b(?:release gate|deployment gate|slo gate|block deploy|freeze release|halt release|approval to ship|override gate)\b", re.I),
    "owner_escalation": re.compile(r"\b(?:owner|owning team|service owner|accountable|escalation|escalate|incident commander|business owner)\b", re.I),
    "reporting_cadence": re.compile(r"\b(?:reporting cadence|weekly report|monthly report|review cadence|service review|quarterly review|status report)\b", re.I),
    "exception_process": re.compile(r"\b(?:exception process|exception|waiver|override|temporary exemption|risk acceptance|approval exception)\b", re.I),
}
_GUIDANCE = {
    "sli_slo_definition": "Define the SLI, SLO target, measurement window, and excluded traffic.",
    "burn_rate_thresholds": "Set burn-rate thresholds for fast and slow budget consumption.",
    "alert_routing": "Route alerts to the right on-call team and notification channels.",
    "release_gate_behavior": "Specify how error-budget state blocks, pauses, or permits releases.",
    "owner_escalation": "Assign service ownership and escalation paths for budget breaches.",
    "reporting_cadence": "Define weekly, monthly, or service-review reporting cadence.",
    "exception_process": "Document exception, waiver, override, and risk-acceptance handling.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,100}\b(?:error budget|burn rate|slo gate|reliability budget)\b"
    r".{0,100}\b(?:scope|impact|changes?|required|needed)\b",
    re.I,
)


def build_task_slo_error_budget_readiness_plan(source: Any) -> TaskSloErrorBudgetReadinessPlan:
    return build_simple_readiness_plan(
        source,
        title="Task SLO Error Budget Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


def analyze_task_slo_error_budget_readiness(source: Any) -> TaskSloErrorBudgetReadinessPlan:
    return build_task_slo_error_budget_readiness_plan(source)


def extract_task_slo_error_budget_readiness(source: Any) -> TaskSloErrorBudgetReadinessPlan:
    return build_task_slo_error_budget_readiness_plan(source)


def generate_task_slo_error_budget_readiness(source: Any) -> TaskSloErrorBudgetReadinessPlan:
    return build_task_slo_error_budget_readiness_plan(source)


def derive_task_slo_error_budget_readiness(source: Any) -> TaskSloErrorBudgetReadinessPlan:
    return build_task_slo_error_budget_readiness_plan(source)


def summarize_task_slo_error_budget_readiness(source: Any) -> TaskSloErrorBudgetReadinessPlan:
    return build_task_slo_error_budget_readiness_plan(source)


def recommend_task_slo_error_budget_readiness(source: Any) -> TaskSloErrorBudgetReadinessPlan:
    return build_task_slo_error_budget_readiness_plan(source)


def task_slo_error_budget_readiness_plan_to_dict(report: TaskSloErrorBudgetReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_slo_error_budget_readiness_plan_to_dict.__test__ = False


def task_slo_error_budget_readiness_plan_to_dicts(report: TaskSloErrorBudgetReadinessPlan) -> list[dict[str, Any]]:
    return report.to_dicts()


task_slo_error_budget_readiness_plan_to_dicts.__test__ = False


def task_slo_error_budget_readiness_plan_to_markdown(report: TaskSloErrorBudgetReadinessPlan) -> str:
    return report.to_markdown()


task_slo_error_budget_readiness_plan_to_markdown.__test__ = False
