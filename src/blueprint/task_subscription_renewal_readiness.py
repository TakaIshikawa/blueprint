"""Assess readiness for subscription renewal execution tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskSubscriptionRenewalReadinessPlan = SimpleReadinessPlan
TaskSubscriptionRenewalReadinessRecord = SimpleReadinessRecord
TaskSubscriptionRenewalReadinessFinding = SimpleReadinessRecord
TaskSubscriptionRenewalReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "subscription_renewal": re.compile(r"\b(?:subscription renewal|renewal workflow|renewal terms|renewal cycle|renewal event)\b", re.I),
    "auto_renewal": re.compile(r"\b(?:auto[- ]?renewal|auto[- ]?renew|automatic renewal|recurring renewal)\b", re.I),
    "renewal_reminders": re.compile(r"\b(?:renewal reminders?|renewal notices?|renewal notification|renewal email|customer notice)\b", re.I),
    "renewal_billing": re.compile(r"\b(?:renewal invoices?|renewal billing|renewal charge|renewal payment|renewal grace period|cancellation window|opt[- ]?out window)\b", re.I),
}
_PATH_SIGNALS = {
    "subscription_renewal": re.compile(r"(?:subscription|renewal|renewals)", re.I),
    "auto_renewal": re.compile(r"(?:auto[_-]?renew|automatic[_-]?renewal|recurring[_-]?renewal)", re.I),
    "renewal_reminders": re.compile(r"(?:renewal[_-]?(?:reminder|notice|notification|email)|customer[_-]?notice)", re.I),
    "renewal_billing": re.compile(r"(?:billing|invoice|grace[_-]?period|cancellation[_-]?window|opt[_-]?out)", re.I),
}
_CRITERIA = {
    "renewal_trigger": re.compile(r"\b(?:renewal trigger|triggering event|trigger condition|renewal date|term end|billing anniversary|renewal job|renewal schedule)\b", re.I),
    "customer_notice_timing": re.compile(r"\b(?:notice timing|customer notice|advance notice|renewal reminder|notice window|reminder cadence|days? before renewal)\b", re.I),
    "billing_invoice_behavior": re.compile(r"\b(?:renewal invoice|invoice generation|billing behavior|renewal charge|payment collection|proration|tax calculation)\b", re.I),
    "payment_failure_handling": re.compile(r"\b(?:payment failure|failed payment|dunning|retry schedule|card decline|past due|collection retry)\b", re.I),
    "cancellation_opt_out_window": re.compile(r"\b(?:cancellation window|cancel before|opt[- ]?out window|renewal cancellation|non[- ]renewal|turn off auto[- ]?renew)\b", re.I),
    "entitlement_continuity": re.compile(r"\b(?:entitlement continuity|continued access|service continuity|access continuity|grace period|entitlement extension|subscription access)\b", re.I),
    "audit_support_visibility": re.compile(r"\b(?:audit trail|support visibility|support dashboard|admin view|renewal history|event log|customer support)\b", re.I),
    "tests": re.compile(r"\b(?:tests?|pytest|unit tests?|integration tests?|contract tests?|renewal tests?|billing tests?|invoice tests?)\b", re.I),
}
_GUIDANCE = {
    "renewal_trigger": "Define the renewal trigger, renewal date, term end, billing anniversary, schedule, or job.",
    "customer_notice_timing": "Specify customer notice timing, advance reminders, notice windows, or reminder cadence.",
    "billing_invoice_behavior": "Document renewal invoice generation, billing behavior, charges, proration, or tax handling.",
    "payment_failure_handling": "Add payment failure, dunning, retry, decline, or past-due handling.",
    "cancellation_opt_out_window": "Define cancellation, non-renewal, or opt-out windows before renewal.",
    "entitlement_continuity": "Clarify entitlement continuity, continued access, service continuity, or grace-period behavior.",
    "audit_support_visibility": "Add audit trail, renewal history, support visibility, admin views, or event logs.",
    "tests": "Add unit, integration, contract, renewal, billing, or invoice tests.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:subscription renewal|auto[- ]?renewal|renewal reminders?|renewal invoices?|grace periods?|cancellation windows?)\b.{0,80}\b(?:impact|changes?|planned|scope|required|needed)\b",
    re.I,
)


def build_task_subscription_renewal_readiness_plan(source: Any) -> TaskSubscriptionRenewalReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Subscription Renewal Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_subscription_renewal_readiness = build_task_subscription_renewal_readiness_plan
extract_task_subscription_renewal_readiness = build_task_subscription_renewal_readiness_plan
generate_task_subscription_renewal_readiness = build_task_subscription_renewal_readiness_plan
derive_task_subscription_renewal_readiness = build_task_subscription_renewal_readiness_plan
summarize_task_subscription_renewal_readiness = build_task_subscription_renewal_readiness_plan
summarize_task_subscription_renewal_readiness_plan = build_task_subscription_renewal_readiness_plan


def recommend_task_subscription_renewal_readiness(source: Any) -> tuple[TaskSubscriptionRenewalReadinessRecord, ...]:
    return build_task_subscription_renewal_readiness_plan(source).records


def task_subscription_renewal_readiness_plan_to_dict(plan: TaskSubscriptionRenewalReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


task_subscription_renewal_readiness_plan_to_dict.__test__ = False


def task_subscription_renewal_readiness_plan_to_dicts(
    plan: TaskSubscriptionRenewalReadinessPlan | Iterable[TaskSubscriptionRenewalReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(plan, SimpleReadinessPlan):
        return plan.to_dicts()
    return [record.to_dict() for record in plan]


task_subscription_renewal_readiness_plan_to_dicts.__test__ = False
task_subscription_renewal_readiness_to_dicts = task_subscription_renewal_readiness_plan_to_dicts
task_subscription_renewal_readiness_to_dicts.__test__ = False


def task_subscription_renewal_readiness_plan_to_markdown(plan: TaskSubscriptionRenewalReadinessPlan) -> str:
    return plan.to_markdown()


task_subscription_renewal_readiness_plan_to_markdown.__test__ = False
