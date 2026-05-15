"""Extract source-level entitlement migration requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import (
    KeywordRequirement as SourceEntitlementMigrationRequirement,
    KeywordRequirementSpec,
    KeywordRequirementsReport as SourceEntitlementMigrationRequirementsReport,
    build_keyword_requirements_report,
)


_SPECS = (
    KeywordRequirementSpec("entitlement_inventory", re.compile(r"\b(?:entitlement inventory|inventory of entitlements|current entitlements?|legacy entitlements?|existing grants?)\b", re.I), ("entitlement inventory",), {"entitlement inventory": re.compile(r"\b(?:plan|feature|permission|grant|license|seat|sku|current|legacy|existing)\b", re.I)}),
    KeywordRequirementSpec("migration_mapping", re.compile(r"\b(?:migration mapping|entitlement mapping|mapping rules?|source[- ]to[- ]target|legacy[- ]to[- ]new|map entitlements?)\b", re.I), ("migration mapping",), {"migration mapping": re.compile(r"\b(?:source|target|legacy|new|sku|plan|tier|feature|table|rules?)\b", re.I)}),
    KeywordRequirementSpec("grandfathered_accounts", re.compile(r"\b(?:grandfathered accounts?|grandfathered entitlements?|grandfathered plans?|legacy accounts?|legacy plan protection)\b", re.I), ("grandfathered accounts",), {"grandfathered accounts": re.compile(r"\b(?:grandfather|legacy|protected|exempt|until renewal|renewal|expiration|account)\b", re.I)}),
    KeywordRequirementSpec("downgrade_behavior", re.compile(r"\b(?:downgrade behavior|downgrade handling|plan downgrade|entitlement downgrade|reduced access|lost access)\b", re.I), ("downgrade behavior",), {"downgrade behavior": re.compile(r"\b(?:downgrade|grace|revoke|remove|reduced|read[- ]?only|lost access|notification)\b", re.I)}),
    KeywordRequirementSpec("override_policy", re.compile(r"\b(?:override policy|manual override|entitlement override|support override|admin override|exception policy)\b", re.I), ("override policy",), {"override policy": re.compile(r"\b(?:approval|admin|support|reason|expiry|expiration|audit|exception|manual)\b", re.I)}),
    KeywordRequirementSpec("validation_backfill", re.compile(r"\b(?:validation/backfill|validation and backfill|backfill validation|entitlement backfill|backfill job|validate migrated entitlements?|reconciliation backfill)\b", re.I), ("validation/backfill",), {"validation/backfill": re.compile(r"\b(?:reconcile|reconciliation|counts?|diff|checksum|dry run|replay|job|migrated entitlements?)\b", re.I)}),
    KeywordRequirementSpec("rollout_phases", re.compile(r"\b(?:rollout phases?|phased rollout|migration phases?|pilot phase|canary phase|wave rollout|cohort rollout)\b", re.I), ("rollout phases",), {"rollout phases": re.compile(r"\b(?:phase|pilot|canary|wave|cohort|percentage|%|beta|general availability|ga)\b", re.I)}),
    KeywordRequirementSpec("audit_trail", re.compile(r"\b(?:audit trail|audit log|entitlement audit|migration audit|audit evidence|change history)\b", re.I), ("audit trail",), {"audit trail": re.compile(r"\b(?:actor|timestamp|before|after|reason|audit|history|evidence|log)\b", re.I)}),
    KeywordRequirementSpec("support_remediation", re.compile(r"\b(?:support remediation|remediation workflow|support playbook|customer remediation|support escalation|fix incorrect entitlements?)\b", re.I), ("support remediation",), {"support remediation": re.compile(r"\b(?:tickets?|playbook|escalation|runbook|customer|rollback|correction|incorrect entitlements?)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:entitlement migration|migrate entitlements?|entitlement conversion|entitlement migration planning|plan migration|feature entitlement migration)\b", re.I)
_STRUCTURED = re.compile(r"(?:entitlement|migration|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,100}\b(?:entitlement migration|migrate entitlements?|entitlement conversion|feature entitlement migration)\b.{0,100}\b(?:scope|required|needed|planned|changes?|work)\b|\b(?:entitlement migration|migrate entitlements?|entitlement conversion|feature entitlement migration)\b.{0,100}\b(?:out of scope|not required|not needed|non[- ]?goal|no changes?|no work)\b", re.I)
_FLAGS = {
    "missing_migration_mapping": ("migration mapping",),
    "missing_validation_backfill": ("validation/backfill",),
    "missing_support_remediation": ("support remediation",),
}


def build_source_entitlement_migration_requirements(source: Any) -> SourceEntitlementMigrationRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Entitlement Migration Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_entitlement_migration_requirements(source: Any) -> SourceEntitlementMigrationRequirementsReport:
    return build_source_entitlement_migration_requirements(source)


def generate_source_entitlement_migration_requirements(source: Any) -> SourceEntitlementMigrationRequirementsReport:
    return build_source_entitlement_migration_requirements(source)


def derive_source_entitlement_migration_requirements(source: Any) -> SourceEntitlementMigrationRequirementsReport:
    return build_source_entitlement_migration_requirements(source)


def summarize_source_entitlement_migration_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceEntitlementMigrationRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_entitlement_migration_requirements(source_or_result).summary


def source_entitlement_migration_requirements_to_dict(report: SourceEntitlementMigrationRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_entitlement_migration_requirements_to_dict.__test__ = False


def source_entitlement_migration_requirements_to_dicts(requirements: SourceEntitlementMigrationRequirementsReport | list[SourceEntitlementMigrationRequirement] | tuple[SourceEntitlementMigrationRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceEntitlementMigrationRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_entitlement_migration_requirements_to_dicts.__test__ = False


def source_entitlement_migration_requirements_to_markdown(report: SourceEntitlementMigrationRequirementsReport) -> str:
    return report.to_markdown()


source_entitlement_migration_requirements_to_markdown.__test__ = False
