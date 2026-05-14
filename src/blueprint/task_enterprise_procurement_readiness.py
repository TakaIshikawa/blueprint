"""Assess readiness for enterprise procurement execution tasks."""

from __future__ import annotations

import re
from typing import Any, Iterable

from blueprint._simple_task_readiness import SimpleReadinessPlan, SimpleReadinessRecord, build_simple_readiness_plan


TaskEnterpriseProcurementReadinessPlan = SimpleReadinessPlan
TaskEnterpriseProcurementReadinessRecord = SimpleReadinessRecord
TaskEnterpriseProcurementReadinessFinding = SimpleReadinessRecord
TaskEnterpriseProcurementReadinessRecommendation = SimpleReadinessRecord

_SIGNALS = {
    "enterprise_procurement": re.compile(r"\b(?:enterprise procurement|vendor procurement|procurement approval|contract onboarding|procurement workflow)\b", re.I),
    "security_questionnaire": re.compile(r"\b(?:security questionnaires?|vendor security review|security review questionnaire|security artifacts?)\b", re.I),
    "legal_review": re.compile(r"\b(?:legal review|contract review|msa review|dpa review|legal artifacts?)\b", re.I),
    "purchase_order": re.compile(r"\b(?:purchase orders?|po process|procurement invoice|invoice path|vendor onboarding)\b", re.I),
}
_PATH_SIGNALS = {
    "enterprise_procurement": re.compile(r"(?:enterprise[_-]?procurement|vendor[_-]?procurement|procurement|contract[_-]?onboarding)", re.I),
    "security_questionnaire": re.compile(r"(?:security[_-]?questionnaire|security[_-]?review|security[_-]?artifact)", re.I),
    "legal_review": re.compile(r"(?:legal[_-]?review|contract[_-]?review|legal|contract|msa|dpa)", re.I),
    "purchase_order": re.compile(r"(?:purchase[_-]?order|po[_-]?process|invoice|vendor)", re.I),
}
_CRITERIA = {
    "buyer_admin_workflow": re.compile(r"\b(?:buyer workflow|admin workflow|buyer admin|procurement workflow|requester workflow|purchasing admin|vendor admin)\b", re.I),
    "approval_authority": re.compile(r"\b(?:approval authority|approver|approval chain|procurement approval|budget owner|legal approver|security approver|sign[- ]off)\b", re.I),
    "security_legal_artifacts": re.compile(r"\b(?:security artifacts?|legal artifacts?|security questionnaire|soc 2|iso 27001|dpa|msa|contract|terms)\b", re.I),
    "purchase_order_invoicing_path": re.compile(r"\b(?:purchase order|po process|po number|invoicing path|invoice routing|billing contact|procurement invoice)\b", re.I),
    "provisioning_handoff": re.compile(r"\b(?:provisioning handoff|provisioning workflow|account provisioning|tenant provisioning|implementation handoff|customer success handoff)\b", re.I),
    "audit_trail": re.compile(r"\b(?:audit trail|approval log|procurement record|contract record|decision log|evidence log)\b", re.I),
    "timeline_sla": re.compile(r"\b(?:timeline|sla|service level|turnaround time|due date|procurement deadline|response time)\b", re.I),
    "tests": re.compile(r"\b(?:tests?|pytest|unit tests?|integration tests?|workflow tests?|procurement tests?|contract tests?|approval tests?)\b", re.I),
}
_GUIDANCE = {
    "buyer_admin_workflow": "Define buyer, admin, requester, purchasing admin, vendor admin, or procurement workflow.",
    "approval_authority": "Name approval authority, approvers, approval chain, budget owner, legal/security approver, or sign-off.",
    "security_legal_artifacts": "Capture security and legal artifacts such as questionnaires, SOC 2, ISO 27001, DPA, MSA, contracts, or terms.",
    "purchase_order_invoicing_path": "Document purchase order, PO number, invoicing path, invoice routing, billing contact, or procurement invoice flow.",
    "provisioning_handoff": "Add provisioning, account, tenant, implementation, or customer-success handoff details.",
    "audit_trail": "Add audit trail, approval log, procurement record, contract record, decision log, or evidence log.",
    "timeline_sla": "Specify timeline, SLA, service level, turnaround time, due date, deadline, or response time.",
    "tests": "Add unit, integration, workflow, procurement, contract, or approval tests.",
}
_NO_IMPACT = re.compile(
    r"\b(?:no|not|without)\b.{0,80}\b(?:enterprise procurement|vendor procurement|security questionnaires?|legal review|purchase orders?|contract onboarding)\b.{0,80}\b(?:impact|changes?|planned|scope|required|needed)\b",
    re.I,
)


def build_task_enterprise_procurement_readiness_plan(source: Any) -> TaskEnterpriseProcurementReadinessPlan:
    if isinstance(source, SimpleReadinessPlan):
        return source
    return build_simple_readiness_plan(
        source,
        title="Task Enterprise Procurement Readiness",
        signal_patterns=_SIGNALS,
        path_signal_patterns=_PATH_SIGNALS,
        criteria_patterns=_CRITERIA,
        criterion_guidance=_GUIDANCE,
        no_impact_pattern=_NO_IMPACT,
    )


analyze_task_enterprise_procurement_readiness = build_task_enterprise_procurement_readiness_plan
extract_task_enterprise_procurement_readiness = build_task_enterprise_procurement_readiness_plan
generate_task_enterprise_procurement_readiness = build_task_enterprise_procurement_readiness_plan
derive_task_enterprise_procurement_readiness = build_task_enterprise_procurement_readiness_plan
summarize_task_enterprise_procurement_readiness = build_task_enterprise_procurement_readiness_plan
summarize_task_enterprise_procurement_readiness_plan = build_task_enterprise_procurement_readiness_plan


def recommend_task_enterprise_procurement_readiness(source: Any) -> tuple[TaskEnterpriseProcurementReadinessRecord, ...]:
    return build_task_enterprise_procurement_readiness_plan(source).records


def task_enterprise_procurement_readiness_plan_to_dict(plan: TaskEnterpriseProcurementReadinessPlan) -> dict[str, Any]:
    return plan.to_dict()


task_enterprise_procurement_readiness_plan_to_dict.__test__ = False


def task_enterprise_procurement_readiness_plan_to_dicts(
    plan: TaskEnterpriseProcurementReadinessPlan | Iterable[TaskEnterpriseProcurementReadinessRecord],
) -> list[dict[str, Any]]:
    if isinstance(plan, SimpleReadinessPlan):
        return plan.to_dicts()
    return [record.to_dict() for record in plan]


task_enterprise_procurement_readiness_plan_to_dicts.__test__ = False
task_enterprise_procurement_readiness_to_dicts = task_enterprise_procurement_readiness_plan_to_dicts
task_enterprise_procurement_readiness_to_dicts.__test__ = False


def task_enterprise_procurement_readiness_plan_to_markdown(plan: TaskEnterpriseProcurementReadinessPlan) -> str:
    return plan.to_markdown()


task_enterprise_procurement_readiness_plan_to_markdown.__test__ = False
