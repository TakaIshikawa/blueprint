"""Analyze support escalation readiness for execution-plan tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, build_simple_readiness_plan


_SIGNAL_PATTERNS = {
    "escalation": re.compile(r"\b(?:support[_\s-]+escalation|escalat(?:e|ion)|tier[_\s-]+handoff|tier[_\s-]+\d|sla[_\s-]+breach|breach[_\s-]+escalation)\b", re.I),
    "routing": re.compile(r"\b(?:support|ticket|case|customer).{0,80}\b(?:routing|route|handoff|queue|triage|owner)\b|\b(?:routing|route|handoff|queue|triage).{0,80}\b(?:support|ticket|case|customer)\b", re.I),
}
_PATH_SIGNAL_PATTERNS = {
    "support_escalation_path": re.compile(r"\b(?:support|tickets?|cases?|customers?).*(?:escalation|routing|handoff|sla|tier)\b|\b(?:escalation|routing|handoff|sla|tier).*(?:support|tickets?|cases?|customers?)\b", re.I),
}
_CRITERIA_PATTERNS = {
    "escalation_trigger": re.compile(r"\b(?:escalation[_\s-]+trigger|trigger|sla[_\s-]+breach|priority[_\s-]+change|severity|age[_\s-]+threshold|unassigned[_\s-]+timeout)\b", re.I),
    "target_tier_owner": re.compile(r"\b(?:target[_\s-]+tier|tier[_\s-]+\d|owner|assignee|queue[_\s-]+owner|dri|on[_\s-]+call|specialist[_\s-]+team)\b", re.I),
    "routing_rules": re.compile(r"\b(?:routing[_\s-]+rules?|route[_\s-]+to|assignment[_\s-]+rules?|triage[_\s-]+rules?|queue[_\s-]+mapping|skill[_\s-]+based)\b", re.I),
    "customer_context": re.compile(r"\b(?:customer[_\s-]+context|account[_\s-]+context|account[_\s-]+plan|customer[_\s-]+tier|entitlement|case[_\s-]+history|customer[_\s-]+history|case[_\s-]+summary|conversation[_\s-]+history)\b", re.I),
    "sla_timing": re.compile(r"\b(?:sla|service[_\s-]+level|response[_\s-]+time|resolution[_\s-]+time|deadline|timer|breach[_\s-]+window|business[_\s-]+hours)\b", re.I),
    "audit_trail": re.compile(r"\b(?:audit[_\s-]+trail|audit[_\s-]+log|history|timeline|handoff[_\s-]+record|actor|timestamp|reason[_\s-]+code)\b", re.I),
    "notification_template": re.compile(r"\b(?:notification[_\s-]+template|email[_\s-]+template|slack[_\s-]+template|message[_\s-]+template|agent[_\s-]+notification|customer[_\s-]+notice)\b", re.I),
    "tests": re.compile(r"\b(?:tests?|unit[_\s-]+tests?|integration[_\s-]+tests?|e2e|fixture|coverage|verification)\b", re.I),
}
_GUIDANCE = {
    "escalation_trigger": "Define escalation triggers such as SLA breach, severity, age threshold, or manual handoff.",
    "target_tier_owner": "Name target tier, owner, queue, DRI, on-call, or specialist team.",
    "routing_rules": "Document routing, assignment, triage, skill-based, and queue mapping rules.",
    "customer_context": "Include customer/account context, plan, history, entitlement, and case summary in the handoff.",
    "sla_timing": "Specify SLA timing, response/resolution deadlines, timers, and business-hours behavior.",
    "audit_trail": "Record audit trail with actor, timestamp, reason, previous owner, target owner, and outcome.",
    "notification_template": "Add agent/customer notification templates for escalation and handoff events.",
    "tests": "Cover triggers, routing, owner assignment, context, SLA timing, audit trail, notifications, and edge cases in tests.",
}


def build_task_support_escalation_readiness_plan(source: Any) -> SimpleReadinessPlan:
    """Build support escalation readiness records from task-shaped input."""
    return build_simple_readiness_plan(
        source,
        title="Task Support Escalation Readiness",
        signal_patterns=_SIGNAL_PATTERNS,
        path_signal_patterns=_PATH_SIGNAL_PATTERNS,
        criteria_patterns=_CRITERIA_PATTERNS,
        criterion_guidance=_GUIDANCE,
    )


analyze_task_support_escalation_readiness = build_task_support_escalation_readiness_plan
summarize_task_support_escalation_readiness = build_task_support_escalation_readiness_plan
generate_task_support_escalation_readiness = build_task_support_escalation_readiness_plan
extract_task_support_escalation_readiness = build_task_support_escalation_readiness_plan
recommend_task_support_escalation_readiness = build_task_support_escalation_readiness_plan


def task_support_escalation_readiness_plan_to_dict(plan: SimpleReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


def task_support_escalation_readiness_plan_to_dicts(plan: SimpleReadinessPlan) -> list[dict[str, Any]]:
    return plan.to_dicts()


def task_support_escalation_readiness_plan_to_markdown(plan: SimpleReadinessPlan) -> str:
    return plan.to_markdown()


__all__ = [
    "SimpleReadinessPlan",
    "analyze_task_support_escalation_readiness",
    "build_task_support_escalation_readiness_plan",
    "extract_task_support_escalation_readiness",
    "generate_task_support_escalation_readiness",
    "recommend_task_support_escalation_readiness",
    "summarize_task_support_escalation_readiness",
    "task_support_escalation_readiness_plan_to_dict",
    "task_support_escalation_readiness_plan_to_dicts",
    "task_support_escalation_readiness_plan_to_markdown",
]
