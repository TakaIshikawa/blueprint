"""Analyze tenant migration readiness for execution-plan tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, build_simple_readiness_plan


_SIGNAL_PATTERNS = {
    "tenant_migration": re.compile(
        r"\b(?:tenant|account|workspace|organization|org)[_\s-]+(?:migration|move|relocation|transfer|cutover|rehome|re-home)\b|"
        r"\b(?:migrate|move|relocat(?:e|ion)|transfer|rehome|re-home).{0,100}\b(?:tenant|account|workspace|organization|org)\b",
        re.I,
    ),
    "workspace_move": re.compile(r"\bworkspace[_\s-]+(?:move|migration|relocation|transfer|cutover)\b", re.I),
    "account_transfer": re.compile(r"\baccount[_\s-]+(?:transfer|move|migration|relocation|reassignment)\b", re.I),
    "shard_relocation": re.compile(r"\b(?:shard|partition|cell)[_\s-]+(?:relocation|move|migration|rebalance|rehome|re-home)\b", re.I),
    "region_move": re.compile(r"\b(?:region|regional)[_\s-]+(?:move|migration|relocation|transfer|cutover)\b|\b(?:move|migrate).{0,80}\b(?:region|us|eu|emea|apac)\b", re.I),
    "cutover_cohort": re.compile(r"\b(?:cutover[_\s-]+cohort|cohort[_\s-]+cutover|migration[_\s-]+cohort|pilot[_\s-]+cohort|wave[_\s-]+cutover)\b", re.I),
    "tenant_rollback": re.compile(r"\b(?:tenant|account|workspace|organization|org)[_\s-]+(?:rollback|roll[_\s-]+back|restore|fallback|revert)\b|\brollback.{0,80}\b(?:tenant|account|workspace|organization|org)\b", re.I),
}
_PATH_SIGNAL_PATTERNS = {
    "tenant_migration": re.compile(r"(?:tenant|account|workspace|organi[sz]ation|org).*(?:migration|move|transfer|relocation|rehome)|(?:migration|move|transfer|relocation|rehome).*(?:tenant|account|workspace|organi[sz]ation|org)", re.I),
    "workspace_move": re.compile(r"workspace.*(?:move|migration|relocation|transfer)", re.I),
    "account_transfer": re.compile(r"account.*(?:transfer|move|migration|relocation)", re.I),
    "shard_relocation": re.compile(r"(?:shard|partition|cell).*(?:relocation|move|migration|rebalance|rehome)", re.I),
    "region_move": re.compile(r"(?:region|regional|us[_-]?to[_-]?eu|eu[_-]?to[_-]?us).*(?:move|migration|relocation|cutover)|(?:move|migration|relocation|cutover).*(?:region|regional)", re.I),
    "cutover_cohort": re.compile(r"(?:cutover|migration).*(?:cohort|wave|pilot)|(?:cohort|wave|pilot).*cutover", re.I),
    "tenant_rollback": re.compile(r"(?:tenant|account|workspace|org).*(?:rollback|restore|fallback|revert)|(?:rollback|restore|fallback|revert).*(?:tenant|account|workspace|org)", re.I),
}
_CRITERIA_PATTERNS = {
    "owner": re.compile(r"\b(?:owner|owned[_\s-]+by|responsible[_\s-]+team|dri|on[_\s-]+call|migration[_\s-]+lead|accountable)\b", re.I),
    "tenant_selection": re.compile(r"\b(?:tenant[_\s-]+selection|tenant[_\s-]+scope|affected[_\s-]+tenants?|eligible[_\s-]+tenants?|account[_\s-]+selection|workspace[_\s-]+selection|cohort|allowlist|exclusion|pilot[_\s-]+tenants?)\b", re.I),
    "validation": re.compile(r"\b(?:validation|verify|verification|dry[_\s-]+run|reconciliation|checksum|row[_\s-]+count|record[_\s-]+count|smoke[_\s-]+test|integrity[_\s-]+check|validation[_\s-]+command)\b", re.I),
    "downtime_plan": re.compile(r"\b(?:downtime|no[_\s-]+downtime|zero[_\s-]+downtime|maintenance[_\s-]+window|read[_\s-]+only|freeze[_\s-]+window|dual[_\s-]+write|traffic[_\s-]+drain|cutover[_\s-]+window)\b", re.I),
    "rollback_path": re.compile(r"\b(?:rollback|roll[_\s-]+back|revert|restore|fallback|abort|reverse[_\s-]+migration|failback|tenant[_\s-]+rollback)\b", re.I),
    "communication": re.compile(r"\b(?:communication|notify|notification|customer[_\s-]+notice|tenant[_\s-]+notice|workspace[_\s-]+admin|support[_\s-]+runbook|release[_\s-]+notes?|status[_\s-]+page)\b", re.I),
    "post_migration_verification": re.compile(r"\b(?:post[_\s-]+migration[_\s-]+verification|post[_\s-]+migration[_\s-]+checks?|post[_\s-]+cutover[_\s-]+checks?|after[_\s-]+migration[_\s-]+verification|monitor(?:ing)?[_\s-]+after|success[_\s-]+criteria|migration[_\s-]+report)\b", re.I),
}
_GUIDANCE = {
    "owner": "Name the owner, DRI, responsible team, or on-call path for the tenant migration.",
    "tenant_selection": "Define tenant, account, workspace, or organization selection, exclusions, and cutover cohorts.",
    "validation": "Add validation such as dry runs, reconciliation, checksums, smoke tests, or validation commands.",
    "downtime_plan": "State the downtime, no-downtime, maintenance-window, freeze, or traffic-drain plan.",
    "rollback_path": "Define tenant rollback, restore, fallback, abort, or reverse-migration steps.",
    "communication": "Prepare tenant, customer, admin, support, status-page, or release-note communication.",
    "post_migration_verification": "Specify post-migration verification, monitoring, success criteria, and migration reports.",
}


def build_task_tenant_migration_readiness_plan(source: Any) -> SimpleReadinessPlan:
    """Build tenant migration readiness records from task-shaped input."""
    return build_simple_readiness_plan(
        source,
        title="Task Tenant Migration Readiness",
        signal_patterns=_SIGNAL_PATTERNS,
        path_signal_patterns=_PATH_SIGNAL_PATTERNS,
        criteria_patterns=_CRITERIA_PATTERNS,
        criterion_guidance=_GUIDANCE,
    )


analyze_task_tenant_migration_readiness = build_task_tenant_migration_readiness_plan
summarize_task_tenant_migration_readiness = build_task_tenant_migration_readiness_plan
generate_task_tenant_migration_readiness = build_task_tenant_migration_readiness_plan
extract_task_tenant_migration_readiness = build_task_tenant_migration_readiness_plan
recommend_task_tenant_migration_readiness = build_task_tenant_migration_readiness_plan


def task_tenant_migration_readiness_plan_to_dict(plan: SimpleReadinessPlan) -> dict[str, Any]:
    """Serialize a tenant migration readiness plan."""
    return plan.to_dict()


def task_tenant_migration_readiness_plan_to_dicts(plan: SimpleReadinessPlan) -> list[dict[str, Any]]:
    """Serialize tenant migration readiness records."""
    return plan.to_dicts()


def task_tenant_migration_readiness_plan_to_markdown(plan: SimpleReadinessPlan) -> str:
    """Render tenant migration readiness as Markdown."""
    return plan.to_markdown()


__all__ = [
    "SimpleReadinessPlan",
    "analyze_task_tenant_migration_readiness",
    "build_task_tenant_migration_readiness_plan",
    "extract_task_tenant_migration_readiness",
    "generate_task_tenant_migration_readiness",
    "recommend_task_tenant_migration_readiness",
    "summarize_task_tenant_migration_readiness",
    "task_tenant_migration_readiness_plan_to_dict",
    "task_tenant_migration_readiness_plan_to_dicts",
    "task_tenant_migration_readiness_plan_to_markdown",
]
