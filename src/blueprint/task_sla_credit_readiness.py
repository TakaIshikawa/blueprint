"""Assess readiness for SLA credit execution tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskSlaCreditReadinessPlan = SimpleReadinessPlan
TaskSlaCreditReadinessRecord = SimpleReadinessRecord
TaskSlaCreditReadinessFinding = SimpleReadinessRecord
TaskSlaCreditReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "sla_credit": re.compile(r"\b(?:sla credits?|service credits?|uptime credits?|availability credits?)\b", re.I),
    "availability_penalty": re.compile(r"\b(?:availability penalt(?:y|ies)|uptime penalt(?:y|ies)|sla penalt(?:y|ies)|downtime compensation)\b", re.I),
    "credit_eligibility": re.compile(r"\b(?:credit eligibility|eligible credits?|credit claim|service credit claim)\b", re.I),
    "customer_compensation": re.compile(r"\b(?:customer compensation|downtime compensation|bill credit|account credit|billing credit)\b", re.I),
}
_PATH_SIGNALS = {
    "sla_credit": re.compile(r"(?:sla|service[_-]?credit|uptime[_-]?credit|availability[_-]?credit)", re.I),
    "availability_penalty": re.compile(r"(?:availability[_-]?penalt|uptime[_-]?penalt|downtime[_-]?compensation)", re.I),
    "credit_eligibility": re.compile(r"(?:credit[_-]?eligibility|credit[_-]?claim|claim[_-]?approval)", re.I),
    "customer_compensation": re.compile(r"(?:customer[_-]?compensation|bill[_-]?credit|account[_-]?credit|billing[_-]?credit)", re.I),
}
_CRITERIA = {
    "eligibility_rules": re.compile(r"\b(?:eligibility rules?|eligible customers?|credit eligibility|qualifying outage|excluded outage|sla threshold|credit policy)\b", re.I),
    "outage_measurement_source": re.compile(r"\b(?:outage measurement source|measurement source|uptime source|availability source|monitoring source|status source|source of truth)\b", re.I),
    "credit_calculation": re.compile(r"\b(?:credit calculation|calculate credits?|credit amount|credit percentage|monthly fee|service credit formula|proration)\b", re.I),
    "claim_approval_workflow": re.compile(r"\b(?:claim workflow|claim approval|approval workflow|approver|support claim|customer claim|manual review)\b", re.I),
    "billing_application": re.compile(r"\b(?:billing application|apply credit|invoice credit|billing credit|account credit|next invoice|ledger adjustment)\b", re.I),
    "customer_communication": re.compile(r"\b(?:customer communication|customer notice|credit notice|notification|email|support response|terms disclosure)\b", re.I),
    "audit_evidence": re.compile(r"\b(?:audit evidence|audit trail|evidence record|calculation evidence|outage evidence|approval log|ledger audit)\b", re.I),
    "tests": re.compile(r"\b(?:tests?|pytest|unit tests?|integration tests?|credit tests?|billing tests?|sla tests?|claim tests?)\b", re.I),
}
_GUIDANCE = {
    "eligibility_rules": "Define eligibility rules, qualifying outages, exclusions, SLA thresholds, or credit policy.",
    "outage_measurement_source": "Identify the outage, uptime, availability, monitoring, status, or source-of-truth measurement source.",
    "credit_calculation": "Specify credit calculation, credit amount, percentage, formula, monthly fee, or proration.",
    "claim_approval_workflow": "Document claim workflow, claim approval, approver, support claim, or manual review.",
    "billing_application": "Define how credits apply to invoices, accounts, billing ledgers, or the next invoice.",
    "customer_communication": "Add customer communication, notices, notifications, email, support response, or terms disclosure.",
    "audit_evidence": "Capture audit evidence, audit trail, evidence records, outage evidence, approval logs, or ledger audit.",
    "tests": "Add unit, integration, credit, billing, SLA, or claim tests.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:sla credits?|service credits?|uptime credits?|availability penalties|credit eligibility|customer compensation)\b.{0,80}\b(?:impact|changes?|planned|scope|required|needed)\b",
    re.I,
)


def build_task_sla_credit_readiness_plan(source: Any) -> TaskSlaCreditReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task SLA Credit Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_sla_credit_readiness = build_task_sla_credit_readiness_plan
extract_task_sla_credit_readiness = build_task_sla_credit_readiness_plan
generate_task_sla_credit_readiness = build_task_sla_credit_readiness_plan
derive_task_sla_credit_readiness = build_task_sla_credit_readiness_plan
summarize_task_sla_credit_readiness = build_task_sla_credit_readiness_plan
summarize_task_sla_credit_readiness_plan = build_task_sla_credit_readiness_plan


def recommend_task_sla_credit_readiness(source: Any) -> tuple[TaskSlaCreditReadinessRecord, ...]:
    return build_task_sla_credit_readiness_plan(source).records


def task_sla_credit_readiness_plan_to_dict(plan: TaskSlaCreditReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


task_sla_credit_readiness_plan_to_dict.__test__ = False


def task_sla_credit_readiness_plan_to_dicts(
    plan: TaskSlaCreditReadinessPlan | Iterable[TaskSlaCreditReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(plan, SimpleReadinessPlan):
        return plan.to_dicts()
    return [record.to_dict() for record in plan]


task_sla_credit_readiness_plan_to_dicts.__test__ = False
task_sla_credit_readiness_to_dicts = task_sla_credit_readiness_plan_to_dicts
task_sla_credit_readiness_to_dicts.__test__ = False


def task_sla_credit_readiness_plan_to_markdown(plan: TaskSlaCreditReadinessPlan) -> str:
    return plan.to_markdown()


task_sla_credit_readiness_plan_to_markdown.__test__ = False
