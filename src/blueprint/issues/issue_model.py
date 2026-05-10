"""Issue and blocker tracking data models."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class IssueType(str, Enum):
    BLOCKER = "blocker"
    RISK = "risk"
    QUESTION = "question"
    DECISION_NEEDED = "decision_needed"
    DEPENDENCY = "dependency"


class IssueSeverity(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class IssueStatus(str, Enum):
    OPEN = "open"
    INVESTIGATING = "investigating"
    RESOLVED = "resolved"
    CLOSED = "closed"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gen_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


# Default SLA hours per severity
DEFAULT_SLA_HOURS: dict[IssueSeverity, int] = {
    IssueSeverity.CRITICAL: 4,
    IssueSeverity.HIGH: 24,
    IssueSeverity.MEDIUM: 72,
    IssueSeverity.LOW: 168,
}


@dataclass(frozen=True, slots=True)
class SLAConfig:
    severity: IssueSeverity
    response_hours: int
    resolution_hours: int


@dataclass(frozen=True, slots=True)
class Issue:
    issue_id: str
    title: str
    description: str = ""
    issue_type: IssueType = IssueType.BLOCKER
    severity: IssueSeverity = IssueSeverity.MEDIUM
    status: IssueStatus = IssueStatus.OPEN
    owner: str = ""
    assignee: str = ""
    related_tasks: list[str] = field(default_factory=list)
    plan_id: str = ""
    root_cause: str = ""
    resolution: str = ""
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)
    resolved_at: str | None = None
    escalated: bool = False
    escalation_level: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class IssueTransition:
    transition_id: str
    issue_id: str
    from_status: IssueStatus
    to_status: IssueStatus
    transitioned_by: str = ""
    timestamp: str = field(default_factory=_now_iso)
    comment: str = ""


__all__ = [
    "DEFAULT_SLA_HOURS",
    "Issue",
    "IssueSeverity",
    "IssueStatus",
    "IssueTransition",
    "IssueType",
    "SLAConfig",
    "_gen_id",
    "_now_iso",
]
