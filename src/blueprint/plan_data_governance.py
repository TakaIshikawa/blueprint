"""Generate data governance matrices for execution plans."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for data governance concepts
_DATA_CLASSIFICATION_RE = re.compile(
    r"(?:data[_\s]+classification|classify[_\s]+data|"
    r"(?:public|internal|confidential|restricted|sensitive)[_\s]+data|"
    r"data[_\s]+(?:sensitivity|category|tier)|"
    r"(?:pii|phi|pci)[_\s]+data|"
    r"classification[_\s]+(?:levels?|scheme|framework))",
    re.I,
)
_ACCESS_CONTROLS_RE = re.compile(
    r"(?:access[_\s]+(?:controls?|policies|management|restrictions?)|"
    r"(?:rbac|abac|dac)[_\s]+(?:controls?|policies)?|"
    r"role[_\s-]*based[_\s]+access|"
    r"(?:permission|authorization)[_\s]+(?:controls?|management)|"
    r"data[_\s]+access[_\s]+(?:controls?|policies|restrictions?)|"
    r"(?:restrict|control|manage)[_\s]+(?:data[_\s]+)?access)",
    re.I,
)
_DATA_LINEAGE_RE = re.compile(
    r"(?:data[_\s]+lineage|lineage[_\s]+(?:tracking|documentation)?|"
    r"(?:track|document|trace)[_\s]+(?:data[_\s]+)?(?:lineage|flow|origin)|"
    r"data[_\s]+(?:provenance|trail|history)|"
    r"(?:source|origin)[_\s]+(?:of|tracking|for)[_\s]+data|"
    r"data[_\s]+transformation[_\s]+(?:tracking|lineage))",
    re.I,
)
_QUALITY_RULES_RE = re.compile(
    r"(?:(?:data[_\s]+)?quality[_\s]+(?:rules?|standards?|checks?|validation)|"
    r"data[_\s]+quality[_\s]+(?:framework|requirements?|metrics?)|"
    r"(?:define|establish|enforce)[_\s]+(?:data[_\s]+)?quality|"
    r"quality[_\s]+(?:assurance|control)|"
    r"data[_\s]+(?:validation|verification)[_\s]+rules?)",
    re.I,
)
_RETENTION_POLICIES_RE = re.compile(
    r"(?:retention[_\s]+(?:policies|policy|rules?|requirements?)|"
    r"data[_\s]+retention|retain[_\s]+data|"
    r"(?:archival|archive)[_\s]+(?:policies|policy|requirements?)|"
    r"retention[_\s]+(?:period|duration|schedule)|"
    r"(?:delete|purge)[_\s]+(?:old|expired)[_\s]+data|"
    r"data[_\s]+lifecycle[_\s]+(?:management|policies))",
    re.I,
)
_POLICY_COVERAGE_RE = re.compile(
    r"(?:policy[_\s]+coverage|governance[_\s]+(?:policies|coverage)|"
    r"(?:comprehensive|complete)[_\s]+(?:governance[_\s]+)?policies|"
    r"policy[_\s]+(?:framework|completeness)|"
    r"(?:cover|address)[_\s]+(?:all[_\s]+)?(?:data[_\s]+)?governance|"
    r"governance[_\s]+(?:framework|standards?))",
    re.I,
)
_AUTOMATION_LEVEL_RE = re.compile(
    r"(?:(?:automation|automated?)[_\s]+(?:governance|policies|enforcement|controls?)|"
    r"(?:automate|automated?)[_\s]+(?:data[_\s]+)?governance|"
    r"(?:automatic|automated?)[_\s]+(?:compliance|policy[_\s]+enforcement)|"
    r"governance[_\s]+automation|"
    r"policy[_\s]+automation|"
    r"automated[_\s]+(?:data[_\s]+)?(?:quality|lineage|classification))",
    re.I,
)
_AUDIT_TRAIL_RE = re.compile(
    r"(?:audit[_\s]+(?:trail|log|logging|tracking)|"
    r"(?:track|log|record)[_\s]+(?:data[_\s]+)?(?:access|changes?|modifications?)|"
    r"(?:data[_\s]+)?(?:access|change)[_\s]+(?:logging|tracking|audit)|"
    r"audit[_\s]+(?:history|records?)|"
    r"(?:maintain|keep)[_\s]+audit[_\s]+(?:trail|log))",
    re.I,
)
_COMPLIANCE_ALIGNMENT_RE = re.compile(
    r"(?:compliance[_\s]+(?:alignment|requirements?|standards?|framework)|"
    r"(?:gdpr|hipaa|sox|pci[_\s]*dss|ccpa)[_\s]+compliance|"
    r"(?:align|comply)[_\s]+with[_\s]+(?:regulations?|standards?|requirements?)|"
    r"regulatory[_\s]+compliance|"
    r"compliance[_\s]+(?:mandate(?:s)?|obligation(?:s)?)|"
    r"(?:meet|satisfy)[_\s]+compliance[_\s]+requirements?)",
    re.I,
)
_DATA_OWNERSHIP_RE = re.compile(
    r"(?:data[_\s]+(?:owner(?:ship)?|steward(?:ship)?|custodian(?:ship)?)|"
    r"(?:assign|define|establish)[_\s]+(?:data[_\s]+)?(?:owner(?:s)?|steward(?:s)?)|"
    r"(?:owner|steward|custodian)[_\s]+(?:of|for)[_\s]+data|"
    r"data[_\s]+(?:accountability|responsibility)|"
    r"governance[_\s]+(?:roles?|ownership))",
    re.I,
)


@dataclass(frozen=True, slots=True)
class DataGovernanceMatrix:
    """Data governance matrix generated from plan data."""

    data_classification_defined: bool = False
    access_controls_established: bool = False
    data_lineage_tracked: bool = False
    quality_rules_defined: bool = False
    retention_policies_set: bool = False
    policy_coverage_adequate: bool = False
    automation_level_planned: bool = False
    audit_trail_maintained: bool = False
    compliance_alignment_verified: bool = False
    data_ownership_assigned: bool = False

    @property
    def governance_maturity_score(self) -> float:
        """Calculate governance maturity score (0.0 to 1.0)."""
        total_checks = 10
        passed_checks = sum([
            self.data_classification_defined,
            self.access_controls_established,
            self.data_lineage_tracked,
            self.quality_rules_defined,
            self.retention_policies_set,
            self.policy_coverage_adequate,
            self.automation_level_planned,
            self.audit_trail_maintained,
            self.compliance_alignment_verified,
            self.data_ownership_assigned,
        ])
        return passed_checks / total_checks

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "data_classification_defined": self.data_classification_defined,
            "access_controls_established": self.access_controls_established,
            "data_lineage_tracked": self.data_lineage_tracked,
            "quality_rules_defined": self.quality_rules_defined,
            "retention_policies_set": self.retention_policies_set,
            "policy_coverage_adequate": self.policy_coverage_adequate,
            "automation_level_planned": self.automation_level_planned,
            "audit_trail_maintained": self.audit_trail_maintained,
            "compliance_alignment_verified": self.compliance_alignment_verified,
            "data_ownership_assigned": self.data_ownership_assigned,
            "governance_maturity_score": self.governance_maturity_score,
        }


def generate_data_governance_matrix(plan_data: Mapping[str, Any]) -> DataGovernanceMatrix:
    """
    Generate data governance matrix from plan data.

    Args:
        plan_data: A mapping containing plan information with fields like
                  'title', 'description', 'requirements', etc.

    Returns:
        DataGovernanceMatrix with boolean flags for each aspect and overall score.
    """
    if not isinstance(plan_data, Mapping):
        return DataGovernanceMatrix()

    searchable_text = _extract_searchable_text(plan_data)

    return DataGovernanceMatrix(
        data_classification_defined=bool(_DATA_CLASSIFICATION_RE.search(searchable_text)),
        access_controls_established=bool(_ACCESS_CONTROLS_RE.search(searchable_text)),
        data_lineage_tracked=bool(_DATA_LINEAGE_RE.search(searchable_text)),
        quality_rules_defined=bool(_QUALITY_RULES_RE.search(searchable_text)),
        retention_policies_set=bool(_RETENTION_POLICIES_RE.search(searchable_text)),
        policy_coverage_adequate=bool(_POLICY_COVERAGE_RE.search(searchable_text)),
        automation_level_planned=bool(_AUTOMATION_LEVEL_RE.search(searchable_text)),
        audit_trail_maintained=bool(_AUDIT_TRAIL_RE.search(searchable_text)),
        compliance_alignment_verified=bool(_COMPLIANCE_ALIGNMENT_RE.search(searchable_text)),
        data_ownership_assigned=bool(_DATA_OWNERSHIP_RE.search(searchable_text)),
    )


def _extract_searchable_text(plan_data: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the plan data for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale", "context", "handoff_prompt"):
        value = plan_data.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("requirements", "acceptance_criteria", "notes", "milestones", "objectives"):
        value = plan_data.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "DataGovernanceMatrix",
    "generate_data_governance_matrix",
]
