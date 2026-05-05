"""Analyze disaster recovery readiness for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping

_SPACE_RE = re.compile(r"\s+")

# Pattern matching for disaster recovery concepts
_BACKUP_STRATEGY_RE = re.compile(
    r"\b(?:backup[_\s]+strateg(?:y|ies)|backup[_\s]+plan|backup[_\s]+approach|backup[_\s]+configuration|"
    r"backup[_\s]+setup|snapshot[_\s]+strateg(?:y|ies)|replication[_\s]+strateg(?:y|ies)|"
    r"test[_\s]+backup[_\s]+strategy)\b",
    re.I,
)
_RTO_RE = re.compile(
    r"\b(?:recovery\s+time\s+objective|"
    r"rto\s+(?:of|is|:|and|,|documented|defined|target|targets)|"
    r"document(?:ed|ing|s)?\s+(?:the\s+)?rto|define(?:d|s)?\s+(?:the\s+)?rto|set\s+(?:the\s+)?rto|"
    r"time\s+to\s+recover|restore\s+time|downtime\s+tolerance|maximum\s+outage)\b",
    re.I,
)
_RPO_RE = re.compile(
    r"\b(?:recovery\s+point\s+objective|"
    r"rpo\s+(?:of|is|:|and|,|documented|defined|target|targets)|"
    r"document(?:ed|ing|s)?\s+(?:the\s+)?rpo|define(?:d|s)?\s+(?:the\s+)?rpo|set\s+(?:the\s+)?rpo|"
    r"(?:and|,)\s+rpo\s+|"
    r"data\s+loss\s+tolerance|acceptable\s+data\s+loss|"
    r"point[- ]in[- ]time|pitr)\b",
    re.I,
)
_FAILOVER_TEST_RE = re.compile(
    r"(?:\bfailover[_\s]+(?:test(?:ing|ed)?|drill|validation)\b|"
    r"\btest(?:ing|ed)?[_\s]+failover\b|"
    r"\bdisaster[_\s]+recovery[_\s]+(?:test|drill)\b|"
    r"\bdr[_\s]+(?:test|drill)\b|"
    r"\btest(?:ing|ed)?[_\s]+(?:disaster[_\s]+recovery|dr)\b|"
    r"test_[a-z_]*(?:failover|disaster_recovery|dr)[a-z_]*(?:test|drill|testing))",
    re.I,
)
_DATA_REPLICATION_RE = re.compile(
    r"\b(?:data[_\s]+replication|replicat(?:e|ion|ed)|cross[_\s-]+region[_\s]+replication|"
    r"multi[_\s-]+region|geo[_\s-]+replication|sync(?:hronization|ed)?|standby|replica|"
    r"verify[_\s]+(?:data[_\s]+)?replication)\b",
    re.I,
)
_RECOVERY_AUTOMATION_RE = re.compile(
    r"\b(?:automat(?:ed|ic)?\s+recovery|recovery\s+automat(?:ed|ion)|automated\s+restore|"
    r"automat(?:ed|ic)?\s+failover|self[- ]healing|recovery\s+script|recovery\s+procedure|"
    r"runbook|playbook)\b",
    re.I,
)
_ROLLBACK_PLAN_RE = re.compile(
    r"\b(?:rollback\s+plan|rollback\s+strateg(?:y|ies)|revert\s+plan|contingency\s+plan|"
    r"back[- ]?out\s+plan|recovery\s+plan|abort\s+procedure)\b",
    re.I,
)


@dataclass(frozen=True, slots=True)
class DisasterRecoveryReadiness:
    """Disaster recovery readiness analysis for a change brief."""

    backup_strategy_defined: bool = False
    rto_documented: bool = False
    rpo_documented: bool = False
    failover_tested: bool = False
    data_replication_verified: bool = False
    recovery_procedures_automated: bool = False
    rollback_plan_exists: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "backup_strategy_defined": self.backup_strategy_defined,
            "rto_documented": self.rto_documented,
            "rpo_documented": self.rpo_documented,
            "failover_tested": self.failover_tested,
            "data_replication_verified": self.data_replication_verified,
            "recovery_procedures_automated": self.recovery_procedures_automated,
            "rollback_plan_exists": self.rollback_plan_exists,
        }


def analyze_disaster_recovery_readiness(
    change_brief: Mapping[str, Any] | str | list[Any] | None,
) -> DisasterRecoveryReadiness:
    """
    Analyze disaster recovery readiness from a change brief.

    Args:
        change_brief: A mapping containing change information with fields like
                     'title', 'description', 'acceptance_criteria', etc.

    Returns:
        DisasterRecoveryReadiness with boolean flags for each DR aspect.
    """
    if not isinstance(change_brief, Mapping):
        return DisasterRecoveryReadiness()

    searchable_text = _extract_searchable_text(change_brief)

    return DisasterRecoveryReadiness(
        backup_strategy_defined=bool(_BACKUP_STRATEGY_RE.search(searchable_text)),
        rto_documented=bool(_RTO_RE.search(searchable_text)),
        rpo_documented=bool(_RPO_RE.search(searchable_text)),
        failover_tested=bool(_FAILOVER_TEST_RE.search(searchable_text)),
        data_replication_verified=bool(_DATA_REPLICATION_RE.search(searchable_text)),
        recovery_procedures_automated=bool(_RECOVERY_AUTOMATION_RE.search(searchable_text)),
        rollback_plan_exists=bool(_ROLLBACK_PLAN_RE.search(searchable_text)),
    )


def _extract_searchable_text(change_brief: Mapping[str, Any]) -> str:
    """Extract all relevant text fields from the change brief for pattern matching."""
    parts: list[str] = []

    # Extract standard text fields
    for field in ("title", "description", "body", "prompt", "rationale"):
        value = change_brief.get(field)
        if isinstance(value, str):
            parts.append(value)

    # Extract list-based fields
    for field in ("acceptance_criteria", "requirements", "notes", "risks", "definition_of_done"):
        value = change_brief.get(field)
        if isinstance(value, (list, tuple)):
            parts.extend(str(item) for item in value if item)
        elif isinstance(value, str):
            parts.append(value)

    # Extract validation commands
    validation = change_brief.get("validation_command") or change_brief.get("validation_commands")
    if isinstance(validation, str):
        parts.append(validation)
    elif isinstance(validation, (list, tuple)):
        parts.extend(str(cmd) for cmd in validation if cmd)

    # Combine all parts
    combined_text = " ".join(parts)
    return _SPACE_RE.sub(" ", combined_text).strip()


__all__ = [
    "DisasterRecoveryReadiness",
    "analyze_disaster_recovery_readiness",
]
