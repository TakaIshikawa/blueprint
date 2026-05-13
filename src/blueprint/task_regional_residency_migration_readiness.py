"""Analyze regional residency migration readiness for execution-plan tasks."""

from __future__ import annotations

import re
from typing import Any

from blueprint._simple_task_readiness import SimpleReadinessPlan, build_simple_readiness_plan


_SIGNAL_PATTERNS = {
    "regional_residency_migration": re.compile(
        r"\b(?:regional|region|data|tenant|customer)[_\s-]+(?:residency|sovereignty)[_\s-]+(?:migration|move|relocation|cutover|enforcement)\b|"
        r"\b(?:migrate|move|relocat(?:e|ion)|cut(?:\s|-)?over|enforce).{0,100}\b(?:regional|region|data)[_\s-]+(?:residency|sovereignty)\b",
        re.I,
    ),
    "tenant_data_relocation": re.compile(
        r"\b(?:migrate|move|relocat(?:e|ion)|transfer|backfill|re-home|rehome).{0,120}\b(?:tenant|customer|account|workspace)[_\s-]+(?:data|records?|profiles?|content)\b|"
        r"\b(?:tenant|customer|account|workspace)[_\s-]+(?:data|records?|profiles?|content).{0,120}\b(?:migrate|move|relocat(?:e|ion)|transfer|backfill|re-home|rehome)\b",
        re.I,
    ),
    "source_target_region": re.compile(
        r"\b(?:source|from|origin)[_\s-]+region\b.{0,120}\b(?:target|to|destination)[_\s-]+region\b|"
        r"\b(?:target|to|destination)[_\s-]+region\b.{0,120}\b(?:source|from|origin)[_\s-]+region\b|"
        r"\b(?:eu|eea|us|usa|uk|canada|australia|japan|singapore|apac|emea)[_\s-]+(?:to|->|into)[_\s-]+(?:eu|eea|us|usa|uk|canada|australia|japan|singapore|apac|emea)\b",
        re.I,
    ),
    "residency_enforcement": re.compile(
        r"\b(?:enforce|enforcing|enforced|guarantee|lock|pin|restrict).{0,100}\b(?:tenant|customer|data)?[_\s-]*(?:region|residency|sovereignty|jurisdiction)\b|"
        r"\b(?:region|residency|sovereignty|jurisdiction)[_\s-]+(?:lock|pinning|restriction|enforcement)\b",
        re.I,
    ),
}
_PATH_SIGNAL_PATTERNS = {
    "regional_residency_migration": re.compile(
        r"(?:regional[_-]?residency|data[_-]?residency|sovereignty).*(?:migration|relocation|cutover|enforce)|"
        r"(?:migration|relocation|cutover|enforce).*(?:regional[_-]?residency|data[_-]?residency|sovereignty)",
        re.I,
    ),
    "tenant_data_relocation": re.compile(
        r"(?:tenant|customer|account|workspace).*(?:data[_-]?)?(?:relocation|migration|rehome|move)|"
        r"(?:relocation|migration|rehome|move).*(?:tenant|customer|account|workspace)",
        re.I,
    ),
    "source_target_region": re.compile(r"(?:source|from|origin).*region.*(?:target|to|destination).*region|(?:eu|us|uk|ca|jp|sg)[_-]?to[_-]?(?:eu|us|uk|ca|jp|sg)", re.I),
    "residency_enforcement": re.compile(r"(?:region|residency|sovereignty)[_-]?(?:lock|pin|enforce|restriction)", re.I),
}
_CRITERIA_PATTERNS = {
    "region_mapping": re.compile(
        r"\b(?:source[_\s-]+region|origin[_\s-]+region|from[_\s-]+region|target[_\s-]+region|destination[_\s-]+region|to[_\s-]+region|region[_\s-]+mapping|region[_\s-]+map|old[_\s-]+to[_\s-]+new[_\s-]+region)\b",
        re.I,
    ),
    "tenant_scope": re.compile(
        r"\b(?:tenant[_\s-]+selection|tenant[_\s-]+scope|customer[_\s-]+selection|customer[_\s-]+scope|eligible[_\s-]+tenants?|affected[_\s-]+tenants?|account[_\s-]+allowlist|cohort|pilot tenants?)\b",
        re.I,
    ),
    "data_classes": re.compile(
        r"\b(?:data[_\s-]+classes?|record[_\s-]+classes?|data[_\s-]+types?|pii|personal[_\s-]+data|customer[_\s-]+data|tenant[_\s-]+data|derived[_\s-]+data|backups?|snapshots?|audit[_\s-]+logs?|attachments?|files?)\b",
        re.I,
    ),
    "migration_sequence": re.compile(
        r"\b(?:migration[_\s-]+sequence|sequencing|phases?|phase[_\s-]+\d+|order[_\s-]+of[_\s-]+operations|batch(?:es|ed)?|wave(?:s)?|pre[_\s-]+cutover|cutover[_\s-]+step|freeze[_\s-]+window)\b",
        re.I,
    ),
    "validation": re.compile(
        r"\b(?:validation|verify|verification|reconciliation|checksum|row[_\s-]+count|record[_\s-]+count|sampling|dry[_\s-]+run|post[_\s-]+migration[_\s-]+checks?|residency[_\s-]+test)\b",
        re.I,
    ),
    "rollback": re.compile(
        r"\b(?:rollback|roll[_\s-]+back|revert|restore|fallback|abort|halt|undo|source[_\s-]+region[_\s-]+restore|reverse[_\s-]+migration)\b",
        re.I,
    ),
    "customer_communication": re.compile(
        r"\b(?:customer[_\s-]+communication|customer[_\s-]+notice|notify[_\s-]+customers?|tenant[_\s-]+notice|admin[_\s-]+notice|support[_\s-]+runbook|release[_\s-]+notes?|maintenance[_\s-]+window|communication[_\s-]+plan)\b",
        re.I,
    ),
    "audit_evidence": re.compile(
        r"\b(?:audit[_\s-]+evidence|compliance[_\s-]+evidence|evidence[_\s-]+pack|attestation|audit[_\s-]+trail|migration[_\s-]+report|chain[_\s-]+of[_\s-]+custody|ropa|dpia|gdpr|soc[_\s-]*2|iso[_\s-]*27001)\b",
        re.I,
    ),
}
_GUIDANCE = {
    "region_mapping": "Document source and target regions, including unsupported or blocked region pairs.",
    "tenant_scope": "Define tenant or customer selection, eligibility, exclusions, and batching cohorts.",
    "data_classes": "Inventory every migrated data class, including derived data, backups, files, and audit records.",
    "migration_sequence": "Specify migration sequencing, cutover steps, ordering constraints, and freeze windows.",
    "validation": "Add validation for counts, checksums, residency placement, reconciliation, and post-migration checks.",
    "rollback": "Define rollback, abort, restore, or reverse-migration steps for failed region moves.",
    "customer_communication": "Prepare customer, admin, support, release-note, or maintenance-window communication.",
    "audit_evidence": "Produce compliance and audit evidence proving what moved, when, by whom, and into which region.",
}


def build_task_regional_residency_migration_readiness_plan(source: Any) -> SimpleReadinessPlan:
    """Build regional residency migration readiness records from task-shaped input."""
    return build_simple_readiness_plan(
        source,
        title="Task Regional Residency Migration Readiness",
        signal_patterns=_SIGNAL_PATTERNS,
        path_signal_patterns=_PATH_SIGNAL_PATTERNS,
        criteria_patterns=_CRITERIA_PATTERNS,
        criterion_guidance=_GUIDANCE,
    )


analyze_task_regional_residency_migration_readiness = build_task_regional_residency_migration_readiness_plan
summarize_task_regional_residency_migration_readiness = build_task_regional_residency_migration_readiness_plan
generate_task_regional_residency_migration_readiness = build_task_regional_residency_migration_readiness_plan
extract_task_regional_residency_migration_readiness = build_task_regional_residency_migration_readiness_plan
recommend_task_regional_residency_migration_readiness = build_task_regional_residency_migration_readiness_plan


def task_regional_residency_migration_readiness_plan_to_dict(plan: SimpleReadinessPlan) -> dict[str, Any]:
    """Serialize a regional residency migration readiness plan."""
    return plan.to_dict()


def task_regional_residency_migration_readiness_plan_to_dicts(plan: SimpleReadinessPlan) -> list[dict[str, Any]]:
    """Serialize regional residency migration readiness records."""
    return plan.to_dicts()


def task_regional_residency_migration_readiness_plan_to_markdown(plan: SimpleReadinessPlan) -> str:
    """Render regional residency migration readiness as Markdown."""
    return plan.to_markdown()


__all__ = [
    "SimpleReadinessPlan",
    "analyze_task_regional_residency_migration_readiness",
    "build_task_regional_residency_migration_readiness_plan",
    "extract_task_regional_residency_migration_readiness",
    "generate_task_regional_residency_migration_readiness",
    "recommend_task_regional_residency_migration_readiness",
    "summarize_task_regional_residency_migration_readiness",
    "task_regional_residency_migration_readiness_plan_to_dict",
    "task_regional_residency_migration_readiness_plan_to_dicts",
    "task_regional_residency_migration_readiness_plan_to_markdown",
]
