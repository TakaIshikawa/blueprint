"""Assess task-level readiness for subscription proration implementation work."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._task_safeguard_readiness import (
    TaskSafeguardReadinessPlan,
    TaskSafeguardReadinessRecord,
    build_task_safeguard_readiness_plan,
)


TaskSubscriptionProrationReadinessRecord = TaskSafeguardReadinessRecord
TaskSubscriptionProrationReadinessPlan = TaskSafeguardReadinessPlan

_SIGNALS = {
    "subscription_proration": re.compile(r"\b(?:subscription proration|prorat(?:e|ion|ed)|prorated billing)\b", re.I),
    "plan_change": re.compile(r"\b(?:plan changes?|change plans?|upgrade|downgrade|seat change|tier change)\b", re.I),
    "mid_cycle_change": re.compile(r"\b(?:mid[- ]cycle|billing cycle|cycle anchor|partial period|remaining period)\b", re.I),
    "invoice_preview": re.compile(r"\b(?:invoice preview|preview invoice|upcoming invoice|billing preview)\b", re.I),
    "credit_calculation": re.compile(r"\b(?:credit calculation|proration credit|unused time credit|account credit|credit note)\b", re.I),
    "tax_interaction": re.compile(r"\b(?:tax|vat|gst|sales tax|tax calculation|tax inclusive)\b", re.I),
    "provider_sync": re.compile(r"\b(?:stripe|adyen|billing provider|provider sync|external billing|payment provider)\b", re.I),
    "customer_notification": re.compile(r"\b(?:customer notification|notify customer|billing email|receipt|price change notice)\b", re.I),
    "reconciliation": re.compile(r"\b(?:reconciliation|reconcile|ledger|settlement|billing variance|audit)\b", re.I),
}
_PATH_SIGNALS = {
    "subscription_proration": re.compile(r"prorat|subscription", re.I),
    "plan_change": re.compile(r"plan|upgrade|downgrade|tier|seat", re.I),
    "mid_cycle_change": re.compile(r"cycle|period|anchor", re.I),
    "invoice_preview": re.compile(r"invoice.*preview|preview.*invoice|upcoming[_-]?invoice", re.I),
    "credit_calculation": re.compile(r"credit|adjustment", re.I),
    "tax_interaction": re.compile(r"tax|vat|gst", re.I),
    "provider_sync": re.compile(r"stripe|adyen|provider|billing", re.I),
    "customer_notification": re.compile(r"notification|email|receipt", re.I),
    "reconciliation": re.compile(r"reconcil|ledger|settlement|variance|audit", re.I),
}
_SAFEGUARDS = {
    "invoice_preview": re.compile(r"\b(?:invoice preview|preview invoice|upcoming invoice|billing preview|preview tests?)\b", re.I),
    "tax_handling": re.compile(r"\b(?:tax handling|tax calculation|tax tests?|vat|gst|sales tax|tax rounding)\b", re.I),
    "provider_reconciliation": re.compile(
        r"\b(?:provider reconciliation|billing provider reconciliation|stripe reconciliation|reconcile provider|provider sync checks?)\b",
        re.I,
    ),
    "customer_communication": re.compile(
        r"\b(?:customer communication|customer notification|notify customer|billing email|price change notice|receipt)\b",
        re.I,
    ),
    "rollback": re.compile(r"\b(?:rollback|roll back|reversal|compensating credit|manual adjustment|backout)\b", re.I),
    "credit_calculation_tests": re.compile(
        r"\b(?:credit calculation tests?|proration credit tests?|unused time credit|rounding tests?|partial period tests?)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "invoice_preview": "Require invoice previews before applying mid-cycle subscription plan changes.",
    "tax_handling": "Cover tax, VAT, GST, sales-tax, and rounding interactions for prorated credits and charges.",
    "provider_reconciliation": "Reconcile internal proration amounts against the billing provider and ledger records.",
    "customer_communication": "Notify customers about prorated charges, credits, effective dates, and invoice changes.",
    "rollback": "Define rollback or compensating adjustment behavior for incorrect prorations.",
    "credit_calculation_tests": "Test credit calculations for upgrades, downgrades, seat changes, partial periods, and rounding.",
}
_HIGH_IMPACT = {"subscription_proration", "plan_change", "mid_cycle_change", "provider_sync", "tax_interaction"}


def build_task_subscription_proration_readiness_plan(source: Any) -> TaskSubscriptionProrationReadinessPlan:
    return build_task_safeguard_readiness_plan(
        source,
        title="Task Subscription Proration Readiness",
        task_count_label="proration_task_count",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        safeguard_patterns=_SAFEGUARDS,
        safeguard_guidance=_GUIDANCE,
        high_impact_signals=_HIGH_IMPACT,
    )


def analyze_task_subscription_proration_readiness(source: Any) -> TaskSubscriptionProrationReadinessPlan:
    return build_task_subscription_proration_readiness_plan(source)


def extract_task_subscription_proration_readiness(source: Any) -> TaskSubscriptionProrationReadinessPlan:
    return build_task_subscription_proration_readiness_plan(source)


def generate_task_subscription_proration_readiness(source: Any) -> TaskSubscriptionProrationReadinessPlan:
    return build_task_subscription_proration_readiness_plan(source)


def derive_task_subscription_proration_readiness(source: Any) -> TaskSubscriptionProrationReadinessPlan:
    return build_task_subscription_proration_readiness_plan(source)


def summarize_task_subscription_proration_readiness(source: Any) -> TaskSubscriptionProrationReadinessPlan:
    return build_task_subscription_proration_readiness_plan(source)


def recommend_task_subscription_proration_readiness(source: Any) -> TaskSubscriptionProrationReadinessPlan:
    return build_task_subscription_proration_readiness_plan(source)


def task_subscription_proration_readiness_plan_to_dict(report: TaskSubscriptionProrationReadinessPlan) -> dict[str, Any]:
    return report.to_dict()


task_subscription_proration_readiness_plan_to_dict.__test__ = False


def task_subscription_proration_readiness_plan_to_dicts(
    report: TaskSubscriptionProrationReadinessPlan | Iterable[TaskSubscriptionProrationReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(report, TaskSafeguardReadinessPlan):
        return report.to_dicts()
    return [record.to_dict() for record in report]


task_subscription_proration_readiness_plan_to_dicts.__test__ = False
task_subscription_proration_readiness_to_dicts = task_subscription_proration_readiness_plan_to_dicts
task_subscription_proration_readiness_to_dicts.__test__ = False


def task_subscription_proration_readiness_plan_to_markdown(report: TaskSubscriptionProrationReadinessPlan) -> str:
    return report.to_markdown()


task_subscription_proration_readiness_plan_to_markdown.__test__ = False
