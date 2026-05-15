"""Extract source-level tenant quota requirements from briefs."""

from __future__ import annotations

import re
from typing import Any

from blueprint._keyword_source_requirements import KeywordRequirement as SourceTenantQuotaRequirement, KeywordRequirementSpec, KeywordRequirementsReport as SourceTenantQuotaRequirementsReport, build_keyword_requirements_report


_SPECS = (
    KeywordRequirementSpec("quota_dimension", re.compile(r"\b(?:quota dimension|quota metric|quota unit|limit dimension|metered dimension)\b", re.I), ("quota dimension",), {"quota dimension": re.compile(r"\b(?:users?|seats?|storage|api calls?|projects?|workspaces?|gb|records?)\b", re.I)}),
    KeywordRequirementSpec("default_limit", re.compile(r"\b(?:default limit|base limit|default quota|standard limit|initial limit)\b", re.I), ("default limit",), {"default limit": re.compile(r"\b(?:limit|quota|default|\d+|gb|seats?|calls?)\b", re.I)}),
    KeywordRequirementSpec("plan_overrides", re.compile(r"\b(?:plan overrides?|plan limits?|tier overrides?|enterprise override|pricing tier limits?)\b", re.I), ("plan overrides",), {"plan overrides": re.compile(r"\b(?:free|pro|enterprise|tier|plan|override|custom)\b", re.I)}),
    KeywordRequirementSpec("enforcement_behavior", re.compile(r"\b(?:enforcement behavior|quota enforcement|limit enforcement|block behavior|throttle behavior)\b", re.I), ("enforcement behavior",), {"enforcement behavior": re.compile(r"\b(?:block|throttle|reject|read-only|soft limit|hard limit)\b", re.I)}),
    KeywordRequirementSpec("warning_threshold", re.compile(r"\b(?:warning threshold|usage warning|quota warning|threshold notification|near limit warning)\b", re.I), ("warning threshold",), {"warning threshold": re.compile(r"\b(?:percent|%|80|90|threshold|email|warning|near limit)\b", re.I)}),
    KeywordRequirementSpec("overage_workflow", re.compile(r"\b(?:overage workflow|overage process|quota overage|overage handling|limit increase workflow)\b", re.I), ("overage workflow",), {"overage workflow": re.compile(r"\b(?:upgrade|purchase|approval|request|sales|limit increase)\b", re.I)}),
    KeywordRequirementSpec("admin_override", re.compile(r"\b(?:admin override|manual override|quota override|temporary override|support override)\b", re.I), ("admin override",), {"admin override": re.compile(r"\b(?:admin|support|override|temporary|expires?|approval)\b", re.I)}),
    KeywordRequirementSpec("telemetry_reporting", re.compile(r"\b(?:telemetry reporting|quota reporting|usage telemetry|quota dashboard|usage report)\b", re.I), ("telemetry reporting",), {"telemetry reporting": re.compile(r"\b(?:telemetry|dashboard|report|metric|usage|export)\b", re.I)}),
    KeywordRequirementSpec("migration_backfill", re.compile(r"\b(?:migration backfill|quota backfill|backfill migration|existing tenants?|legacy migration)\b", re.I), ("migration backfill",), {"migration backfill": re.compile(r"\b(?:backfill|migration|existing|legacy|tenants?|seed|recalculate)\b", re.I)}),
)
_CONTEXT = re.compile(r"\b(?:tenant quota|quota planning|tenant limits?|quota management|tenant limit)\b", re.I)
_STRUCTURED = re.compile(r"(?:tenant|quota|limit|requirements?|acceptance|source_payload)", re.I)
_NEGATED = re.compile(r"\b(?:no|not|without)\b.{0,80}\b(?:tenant quota|quota planning|tenant limits?|quota management)\b.{0,80}\b(?:scope|required|needed|planned|changes?)\b|\b(?:tenant quota|quota planning|tenant limits?|quota management)\b.{0,80}\b(?:out of scope|not required|not needed|non[- ]?goal)\b", re.I)
_FLAGS = {"missing_quota_dimension": ("quota dimension",), "missing_enforcement_behavior": ("enforcement behavior",), "missing_overage_workflow": ("overage workflow",)}


def build_source_tenant_quota_requirements(source: Any) -> SourceTenantQuotaRequirementsReport:
    return build_keyword_requirements_report(source, title="Source Tenant Quota Requirements Report", specs=_SPECS, context_pattern=_CONTEXT, structured_field_pattern=_STRUCTURED, negated_pattern=_NEGATED, summary_flag_groups=_FLAGS)


def extract_source_tenant_quota_requirements(source: Any) -> SourceTenantQuotaRequirementsReport:
    return build_source_tenant_quota_requirements(source)


def generate_source_tenant_quota_requirements(source: Any) -> SourceTenantQuotaRequirementsReport:
    return build_source_tenant_quota_requirements(source)


def derive_source_tenant_quota_requirements(source: Any) -> SourceTenantQuotaRequirementsReport:
    return build_source_tenant_quota_requirements(source)


def summarize_source_tenant_quota_requirements(source_or_result: Any) -> dict[str, Any]:
    if isinstance(source_or_result, SourceTenantQuotaRequirementsReport):
        return dict(source_or_result.summary)
    return build_source_tenant_quota_requirements(source_or_result).summary


def source_tenant_quota_requirements_to_dict(report: SourceTenantQuotaRequirementsReport) -> dict[str, Any]:
    return report.to_dict()


source_tenant_quota_requirements_to_dict.__test__ = False


def source_tenant_quota_requirements_to_dicts(requirements: SourceTenantQuotaRequirementsReport | list[SourceTenantQuotaRequirement] | tuple[SourceTenantQuotaRequirement, ...]) -> list[dict[str, Any]]:
    if isinstance(requirements, SourceTenantQuotaRequirementsReport):
        return requirements.to_dicts()
    return [requirement.to_dict() for requirement in requirements]


source_tenant_quota_requirements_to_dicts.__test__ = False


def source_tenant_quota_requirements_to_markdown(report: SourceTenantQuotaRequirementsReport) -> str:
    return report.to_markdown()


source_tenant_quota_requirements_to_markdown.__test__ = False
