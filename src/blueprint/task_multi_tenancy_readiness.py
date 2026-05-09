"""Analyze multi-tenancy readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for multi-tenancy concepts
_ISOLATION_MODEL_RE = re.compile(
    r"\b(?:isolation[_\s]+model|tenant[_\s]+isolation|"
    r"data[_\s]+isolation|database[_\s]+isolation|"
    r"schema[_\s]+isolation|shared[_\s]+(?:schema|database|instance)|"
    r"separate[_\s]+(?:schema|database|instance)|"
    r"dedicated[_\s]+(?:schema|database|instance)|"
    r"pool(?:ed)?[_\s]+(?:tenancy|tenant)|"
    r"silo(?:ed)?[_\s]+(?:tenancy|tenant)|bridge[_\s]+model|"
    r"multi[_\s-]*tenant[_\s]+architecture|"
    r"test[_\s]+tenant[_\s]+isolation|test_tenant_isolation)\b",
    re.I,
)
_DATA_PARTITIONING_RE = re.compile(
    r"\b(?:data[_\s]+partition(?:ing)?|tenant[_\s]+partition(?:ing)?|"
    r"partition[_\s]+(?:strategy|key|scheme|by)|"
    r"tenant[_\s]+(?:id|identifier|key)[_\s]+(?:column|field)|"
    r"row[_\s-]*level[_\s]+(?:security|isolation)|"
    r"rls|partition[_\s]+by[_\s]+tenant|"
    r"shard(?:ing)?[_\s]+(?:by[_\s]+tenant|strategy)|"
    r"tenant[_\s-]*specific[_\s]+(?:data|tables?)|"
    r"test[_\s]+data[_\s]+partitioning|test_data_partitioning)\b",
    re.I,
)
_TENANT_IDENTIFICATION_RE = re.compile(
    r"\b(?:tenant[_\s]+(?:identification|detection)|"
    r"identify[_\s]+tenant|detect[_\s]+tenant|"
    r"tenant[_\s]+(?:context|resolution|lookup)|"
    r"resolve[_\s]+tenant|extract[_\s]+tenant|"
    r"tenant[_\s]+(?:from[_\s]+)?(?:header|subdomain|domain|path|token|claim)|"
    r"current[_\s]+tenant|active[_\s]+tenant|"
    r"tenant[_\s]+middleware|tenant[_\s]+interceptor)\b",
    re.I,
)
_RESOURCE_QUOTAS_RE = re.compile(
    r"\b(?:resource[_\s]+(?:quota|limit|allocation|constraint)|"
    r"tenant[_\s]+(?:quota|limit|allocation)|"
    r"per[_\s-]*tenant[_\s]+(?:quota|limit|resource)|"
    r"usage[_\s]+(?:limits?|quota|cap|consumption)|"
    r"rate[_\s]+limit(?:ing)?(?:[_\s]+(?:per[_\s]+tenant|by[_\s]+tenant))?|"
    r"storage[_\s]+(?:quota|limit)|api[_\s]+(?:quota|rate[_\s]+limit)|"
    r"throttl(?:e|ing)(?:[_\s]+(?:per[_\s]+tenant|by[_\s]+tenant|api|requests?))?|"
    r"(?:set|enforce|configure)[_\s]+usage[_\s]+limits?|"
    r"(?:usage|resource)[_\s]+limits?[_\s]+(?:needed|required|configured)|"
    r"fair[_\s]+(?:use|usage)[_\s]+policy)\b",
    re.I,
)
_CROSS_TENANT_LEAKAGE_RE = re.compile(
    r"\b(?:cross[_\s-]*tenant[_\s]+(?:access|leakage|data|security|isolation|prevention)|"
    r"tenant[_\s]+data[_\s]+(?:leakage|leak|breach)|"
    r"(?:prevent|prevention)[_\s]+(?:cross[_\s-]*tenant|data[_\s]+leakage)|"
    r"(?:no|without)[_\s]+(?:cross[_\s-]*tenant|data[_\s]+leakage)[_\s]+prevention|"
    r"tenant[_\s]+(?:boundary|separation)|"
    r"isolat(?:e|ion)[_\s]+(?:tenant[_\s]+)?data|"
    r"tenant[_\s]+security[_\s]+(?:boundary|isolation)|"
    r"unauthorized[_\s]+(?:cross[_\s-]*tenant|tenant)[_\s]+access|"
    r"data[_\s]+segregation)\b",
    re.I,
)
_TENANT_CUSTOMIZATION_RE = re.compile(
    r"\b(?:tenant[_\s-]*specific[_\s]+(?:customization|configuration|setting|feature)|"
    r"per[_\s-]*tenant[_\s]+(?:customization|configuration|setting|feature|settings?|preferences?)|"
    r"tenant[_\s]+(?:customization|branding|theme|preference|preferences|override|extension|extensions)|"
    r"custom(?:izable)?[_\s]+(?:per[_\s]+tenant|by[_\s]+tenant|features?)|"
    r"(?:store|configure)[_\s]+tenant[_\s]+preferences?|"
    r"white[_\s-]*label(?:ing)?|multi[_\s-]*tenant[_\s]+customization)\b",
    re.I,
)
_TENANT_LIFECYCLE_RE = re.compile(
    r"\b(?:tenant[_\s]+(?:onboarding|provisioning|creation|registration|setup)|"
    r"(?:onboard|provision|create|register)[_\s]+tenant|"
    r"tenant[_\s]+(?:offboarding|deprovisioning|removal|deletion|cleanup)|"
    r"(?:offboard|deprovision|remove|delete)[_\s]+tenant|"
    r"tenant[_\s]+(?:lifecycle|migration|suspension|activation)|"
    r"(?:migrate|suspend|activate)[_\s]+tenant|"
    r"(?:handle|perform|implement)[_\s]+tenant[_\s]+(?:activation|cleanup|data[_\s]+cleanup)|"
    r"test[_\s]+tenant[_\s]+provisioning|test_tenant_provisioning)\b",
    re.I,
)
_TENANT_ADMINISTRATION_RE = re.compile(
    r"\b(?:tenant[_\s]+(?:administration|management|admin|portal)|"
    r"(?:manage|administer)[_\s]+tenant|"
    r"tenant[_\s]+(?:dashboard|console)|"
    r"tenant[_\s]+(?:user[_\s]+management|role|permission)|"
    r"tenant[_\s-]*level[_\s]+(?:admin|user|role)|"
    r"tenant[_\s]+(?:audit|monitoring|analytics)|"
    r"admin[_\s]+portal)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class MultiTenancyReadiness:
    """Multi-tenancy readiness analysis for a task."""

    isolation_model_defined: bool = False
    data_partitioning_specified: bool = False
    tenant_identification_configured: bool = False
    resource_quotas_planned: bool = False
    cross_tenant_leakage_prevented: bool = False
    tenant_customization_supported: bool = False
    tenant_lifecycle_managed: bool = False
    tenant_administration_included: bool = False

    @property
    def readiness_score(self) -> float:
        """Calculate readiness score (0.0 to 1.0)."""
        total_checks = 8
        passed_checks = sum([
            self.isolation_model_defined,
            self.data_partitioning_specified,
            self.tenant_identification_configured,
            self.resource_quotas_planned,
            self.cross_tenant_leakage_prevented,
            self.tenant_customization_supported,
            self.tenant_lifecycle_managed,
            self.tenant_administration_included,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "isolation_model_defined": self.isolation_model_defined,
            "data_partitioning_specified": self.data_partitioning_specified,
            "tenant_identification_configured": self.tenant_identification_configured,
            "resource_quotas_planned": self.resource_quotas_planned,
            "cross_tenant_leakage_prevented": self.cross_tenant_leakage_prevented,
            "tenant_customization_supported": self.tenant_customization_supported,
            "tenant_lifecycle_managed": self.tenant_lifecycle_managed,
            "tenant_administration_included": self.tenant_administration_included,
            "readiness_score": self.readiness_score,
        }


def analyze_multi_tenancy_readiness(task_data: Mapping[str, Any]) -> MultiTenancyReadiness:
    """
    Analyze multi-tenancy readiness from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        MultiTenancyReadiness with boolean flags for each aspect and overall score.
    """
    if not isinstance(task_data, Mapping):
        return MultiTenancyReadiness()

    searchable_text = _extract_searchable_text(task_data)

    return MultiTenancyReadiness(
        isolation_model_defined=bool(_ISOLATION_MODEL_RE.search(searchable_text)),
        data_partitioning_specified=bool(_DATA_PARTITIONING_RE.search(searchable_text)),
        tenant_identification_configured=bool(_TENANT_IDENTIFICATION_RE.search(searchable_text)),
        resource_quotas_planned=bool(_RESOURCE_QUOTAS_RE.search(searchable_text)),
        cross_tenant_leakage_prevented=bool(_CROSS_TENANT_LEAKAGE_RE.search(searchable_text)),
        tenant_customization_supported=bool(_TENANT_CUSTOMIZATION_RE.search(searchable_text)),
        tenant_lifecycle_managed=bool(_TENANT_LIFECYCLE_RE.search(searchable_text)),
        tenant_administration_included=bool(_TENANT_ADMINISTRATION_RE.search(searchable_text)),
    )


def _extract_searchable_text(task_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the task data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = task_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("acceptance_criteria", "requirements", "notes", "risks", "definition_of_done"):
        value = task_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Extract validation commands
    validation = task_data.get("validation_command") or task_data.get("validation_commands")
    if isinstance(validation, str):
        parts.append(validation)
    elif isinstance(validation, (list, tuple)):
        parts.extend(str(cmd) for cmd in validation if cmd)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "MultiTenancyReadiness",
    "analyze_multi_tenancy_readiness",
]
