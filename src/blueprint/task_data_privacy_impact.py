"""Analyze data privacy impact for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for data privacy concepts
_PII_COLLECTION_RE = re.compile(
    r"\b(?:pii[_\s]+collection|collect[_\s]+pii|personal[_\s]+(?:data|information)[_\s]+collection|"
    r"gather[_\s]+(?:personal[_\s]+)?(?:data|information)|user[_\s]+data[_\s]+collection|"
    r"collect[_\s]+(?:name|email|phone|address|ssn|social[_\s]+security)|"
    r"store[_\s]+(?:personal[_\s]+)?(?:data|information)|"
    r"test[_\s]+pii[_\s]+collection)\b",
    re.I,
)
_DATA_SHARING_RE = re.compile(
    r"\b(?:(?:data|user)[_\s]+shar(?:e|ing)|share[_\s]+(?:user[_\s]+)?data|"
    r"share[_\s]+(?:with[_\s]+)?third[_\s]+part(?:y|ies)|"
    r"third[_\s]+party[_\s]+(?:integration|access|sharing|analytics)|"
    r"external[_\s]+(?:data[_\s]+)?sharing|data[_\s]+transfer[_\s]+to|"
    r"vendor[_\s]+(?:access|integration)|partner[_\s]+data[_\s]+access|"
    r"test[_\s]+data[_\s]+shar(?:e|ing))\b",
    re.I,
)
_CONSENT_REQUIREMENT_RE = re.compile(
    r"\b(?:user[_\s]+consent|consent[_\s]+(?:mechanism|requirement|management|flow)|"
    r"obtain[_\s]+consent|explicit[_\s]+consent|opt[_\s-]*in|opt[_\s-]*out|"
    r"consent[_\s]+banner|cookie[_\s]+consent|privacy[_\s]+preferences|"
    r"test[_\s]+consent)\b",
    re.I,
)
_RETENTION_POLICY_RE = re.compile(
    r"\b(?:(?:data[_\s]+)?retention[_\s]+(?:policy|period|schedule)|"
    r"retain[_\s]+(?:data|information|records)|data[_\s]+lifecycle|"
    r"(?:automatic[_\s]+)?data[_\s]+deletion|purge[_\s]+(?:policy|schedule)|"
    r"data[_\s]+expiration|archive[_\s]+policy|"
    r"test[_\s]+(?:data[_\s]+)?retention)\b",
    re.I,
)
_CROSS_BORDER_TRANSFER_RE = re.compile(
    r"\b(?:cross[_\s-]*border[_\s]+transfer|international[_\s]+(?:data[_\s]+)?transfer|"
    r"data[_\s]+transfer[_\s]+(?:across|between)[_\s]+(?:countries|regions|jurisdictions)|"
    r"(?:eu|european)[_\s]+(?:data[_\s]+)?transfer|cross[_\s-]*border[_\s]+data|"
    r"standard[_\s]+contractual[_\s]+clauses|scc|adequacy[_\s]+decision|"
    r"test[_\s]+cross[_\s-]*border)\b",
    re.I,
)
_GDPR_COMPLIANCE_RE = re.compile(
    r"\b(?:gdpr[_\s]+(?:compliance|compliant|requirement|regulation)|"
    r"general[_\s]+data[_\s]+protection[_\s]+regulation|"
    r"article[_\s]+(?:6|7|13|14|15|16|17|20|25|32)|gdpr\b|"
    r"data[_\s]+protection[_\s]+officer|dpo|"
    r"privacy[_\s]+by[_\s]+design|data[_\s]+protection[_\s]+impact[_\s]+assessment|dpia|"
    r"test[_\s]+gdpr)\b",
    re.I,
)
_CCPA_COMPLIANCE_RE = re.compile(
    r"\b(?:ccpa[_\s]+(?:compliance|compliant|requirement|regulation)|"
    r"california[_\s]+consumer[_\s]+privacy[_\s]+act|"
    r"do[_\s]+not[_\s]+sell|consumer[_\s]+(?:privacy[_\s]+)?rights|"
    r"california[_\s]+privacy[_\s]+rights[_\s]+act|cpra|"
    r"test[_\s]+ccpa)\b",
    re.I,
)
_DATA_MINIMIZATION_RE = re.compile(
    r"\b(?:data[_\s]+minimization|minimal[_\s]+data[_\s]+collection|"
    r"collect[_\s]+(?:only[_\s]+)?necessary[_\s]+data|purpose[_\s]+limitation|"
    r"(?:limit|minimize)[_\s]+data[_\s]+collection|minimal[_\s]+pii|"
    r"minimization[_\s]+principles|"
    r"test[_\s]+data[_\s]+minimization)\b",
    re.I,
)
_RIGHT_TO_DELETION_RE = re.compile(
    r"\b(?:right[_\s]+to[_\s]+(?:be[_\s]+)?(?:deletion|erasure|forgotten)|"
    r"delete[_\s]+(?:user[_\s]+)?(?:data|account)|data[_\s]+deletion[_\s]+request|"
    r"user[_\s]+data[_\s]+removal|erasure[_\s]+request|forget[_\s]+(?:me|user)|"
    r"test[_\s]+(?:right[_\s]+to[_\s]+)?deletion)\b",
    re.I,
)
_BREACH_NOTIFICATION_RE = re.compile(
    r"\b(?:breach[_\s]+notification|data[_\s]+breach[_\s]+(?:protocol|procedure|response)|"
    r"security[_\s]+incident[_\s]+notification|notify[_\s]+(?:of[_\s]+)?breach|"
    r"breach[_\s]+disclosure|incident[_\s]+response[_\s]+plan|"
    r"test[_\s]+breach[_\s]+notification)\b",
    re.I,
)
_CHILDREN_DATA_RE = re.compile(
    r"\b(?:children[_\s']*s?[_\s]+(?:data|privacy|information)|coppa[_\s]+compliance|"
    r"parental[_\s]+consent|age[_\s]+verification|minor[_\s']*s?[_\s]+data|"
    r"under[_\s]+(?:13|16|18)|child[_\s]+(?:user|account)|"
    r"test[_\s]+children[_\s']*s?[_\s]+data)\b",
    re.I,
)
_SENSITIVE_CATEGORIES_RE = re.compile(
    r"\b(?:sensitive[_\s]+(?:personal[_\s]+)?(?:data|information|categories)|"
    r"special[_\s]+categories[_\s]+(?:of[_\s]+)?data|"
    r"health[_\s]+(?:data|information)|medical[_\s]+(?:records|data)|"
    r"biometric[_\s]+data|genetic[_\s]+data|racial[_\s]+(?:or[_\s]+)?ethnic[_\s]+origin|"
    r"political[_\s]+opinions|religious[_\s]+beliefs|trade[_\s]+union|"
    r"sexual[_\s]+orientation|financial[_\s]+data|"
    r"test[_\s]+sensitive[_\s]+(?:data|categories))\b",
    re.I,
)
_ANONYMIZATION_RE = re.compile(
    r"\b(?:anonymi[zs](?:e|ation|ed)|anonymous[_\s]+data|de[_\s-]*identif(?:y|ication|ied)|"
    r"remove[_\s]+(?:personal[_\s]+)?identif(?:iers|ying[_\s]+information)|"
    r"data[_\s]+masking|redact(?:ion|ed)?|"
    r"test[_\s]+anonymi[zs]ation)\b",
    re.I,
)
_PSEUDONYMIZATION_RE = re.compile(
    r"\b(?:pseudonimi[zs](?:e|ation|ed)|pseudonym(?:s|ous)?[_\s]+data|"
    r"tokeniz(?:e|ation|ed)|hash(?:ed|ing)?[_\s]+(?:data|identifiers)|hashing[_\s]+identifiers|"
    r"obfuscat(?:e|ion|ed)|"
    r"test[_\s]+pseudonimi[zs]ation)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class DataPrivacyImpact:
    """Data privacy impact analysis for a task."""

    pii_collection_identified: bool = False
    data_sharing_detected: bool = False
    consent_requirements_present: bool = False
    retention_policy_defined: bool = False
    cross_border_transfers_flagged: bool = False
    gdpr_compliance_addressed: bool = False
    ccpa_compliance_addressed: bool = False
    data_minimization_practiced: bool = False
    right_to_deletion_implemented: bool = False
    breach_notification_planned: bool = False
    children_data_handled: bool = False
    sensitive_categories_processed: bool = False
    anonymization_applied: bool = False
    pseudonymization_applied: bool = False

    @property
    def privacy_risk_score(self) -> float:
        """
        Calculate privacy risk score (0.0 to 1.0).

        Higher score indicates better privacy protection.
        Score is based on: data processing scope, compliance coverage, and user control mechanisms.
        """
        # Data sensitivity indicators (presence increases risk if not mitigated)
        sensitivity_flags = [
            self.pii_collection_identified,
            self.data_sharing_detected,
            self.cross_border_transfers_flagged,
            self.children_data_handled,
            self.sensitive_categories_processed,
        ]
        sensitivity_count = sum(sensitivity_flags)

        # Compliance and protection measures (presence reduces risk)
        protection_measures = [
            self.consent_requirements_present,
            self.retention_policy_defined,
            self.gdpr_compliance_addressed,
            self.ccpa_compliance_addressed,
            self.data_minimization_practiced,
            self.right_to_deletion_implemented,
            self.breach_notification_planned,
            self.anonymization_applied,
            self.pseudonymization_applied,
        ]
        protection_count = sum(protection_measures)

        # If no sensitive data is processed, high baseline score
        if sensitivity_count == 0:
            return 0.95

        # Calculate protection coverage relative to sensitivity
        # More sensitive operations require more protection measures
        required_protections = min(sensitivity_count + 2, len(protection_measures))
        protection_coverage = min(protection_count / required_protections, 1.0) if required_protections > 0 else 0.0

        # Scale score: high sensitivity with low protection = low score
        # high sensitivity with high protection = high score
        base_score = protection_coverage * 0.8

        # Bonus for critical protections when handling sensitive data
        critical_protections = [
            self.consent_requirements_present,
            self.data_minimization_practiced,
            self.right_to_deletion_implemented,
        ]
        critical_score = sum(critical_protections) / len(critical_protections) * 0.2

        return base_score + critical_score

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "pii_collection_identified": self.pii_collection_identified,
            "data_sharing_detected": self.data_sharing_detected,
            "consent_requirements_present": self.consent_requirements_present,
            "retention_policy_defined": self.retention_policy_defined,
            "cross_border_transfers_flagged": self.cross_border_transfers_flagged,
            "gdpr_compliance_addressed": self.gdpr_compliance_addressed,
            "ccpa_compliance_addressed": self.ccpa_compliance_addressed,
            "data_minimization_practiced": self.data_minimization_practiced,
            "right_to_deletion_implemented": self.right_to_deletion_implemented,
            "breach_notification_planned": self.breach_notification_planned,
            "children_data_handled": self.children_data_handled,
            "sensitive_categories_processed": self.sensitive_categories_processed,
            "anonymization_applied": self.anonymization_applied,
            "pseudonymization_applied": self.pseudonymization_applied,
            "privacy_risk_score": self.privacy_risk_score,
        }


def analyze_data_privacy_impact(task_data: Mapping[str, Any]) -> DataPrivacyImpact:
    """
    Analyze data privacy impact from task data.

    Args:
        task_data: A mapping containing task information with fields like
                  'title', 'description', 'acceptance_criteria', etc.

    Returns:
        DataPrivacyImpact with boolean flags for each aspect and overall risk score.
    """
    if not isinstance(task_data, Mapping):
        return DataPrivacyImpact()

    searchable_text = _extract_searchable_text(task_data)

    return DataPrivacyImpact(
        pii_collection_identified=bool(_PII_COLLECTION_RE.search(searchable_text)),
        data_sharing_detected=bool(_DATA_SHARING_RE.search(searchable_text)),
        consent_requirements_present=bool(_CONSENT_REQUIREMENT_RE.search(searchable_text)),
        retention_policy_defined=bool(_RETENTION_POLICY_RE.search(searchable_text)),
        cross_border_transfers_flagged=bool(_CROSS_BORDER_TRANSFER_RE.search(searchable_text)),
        gdpr_compliance_addressed=bool(_GDPR_COMPLIANCE_RE.search(searchable_text)),
        ccpa_compliance_addressed=bool(_CCPA_COMPLIANCE_RE.search(searchable_text)),
        data_minimization_practiced=bool(_DATA_MINIMIZATION_RE.search(searchable_text)),
        right_to_deletion_implemented=bool(_RIGHT_TO_DELETION_RE.search(searchable_text)),
        breach_notification_planned=bool(_BREACH_NOTIFICATION_RE.search(searchable_text)),
        children_data_handled=bool(_CHILDREN_DATA_RE.search(searchable_text)),
        sensitive_categories_processed=bool(_SENSITIVE_CATEGORIES_RE.search(searchable_text)),
        anonymization_applied=bool(_ANONYMIZATION_RE.search(searchable_text)),
        pseudonymization_applied=bool(_PSEUDONYMIZATION_RE.search(searchable_text)),
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
    "DataPrivacyImpact",
    "analyze_data_privacy_impact",
]
