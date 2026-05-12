"""Team workspace data models."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class WorkspaceRole(str, Enum):
    OWNER = "owner"
    ADMIN = "admin"
    MEMBER = "member"
    VIEWER = "viewer"


class ApprovalWorkflow(str, Enum):
    NONE = "none"
    SINGLE_APPROVER = "single_approver"
    MULTI_APPROVER = "multi_approver"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gen_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


@dataclass(frozen=True, slots=True)
class TeamMember:
    member_id: str
    user_id: str
    display_name: str
    email: str = ""
    role: WorkspaceRole = WorkspaceRole.MEMBER
    joined_at: str = field(default_factory=_now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SharedResource:
    resource_id: str
    name: str
    resource_type: str
    total_capacity: float = 0.0
    allocated: float = 0.0
    unit: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ResourceAllocation:
    allocation_id: str
    resource_id: str
    plan_id: str
    amount: float
    reason: str = ""
    created_by: str = ""
    created_at: str = field(default_factory=_now_iso)


@dataclass(frozen=True, slots=True)
class WorkspaceInvitation:
    invitation_id: str
    workspace_id: str
    email: str
    role: WorkspaceRole
    status: str = "pending"
    invited_by: str = ""
    created_at: str = field(default_factory=_now_iso)
    accepted_at: str | None = None
    declined_at: str | None = None


@dataclass(frozen=True, slots=True)
class WorkspacePolicyFinding:
    code: str
    severity: str
    message: str
    entity_id: str = ""


@dataclass(frozen=True, slots=True)
class WorkspaceTemplate:
    template_id: str
    name: str
    description: str = ""
    template_data: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CustomField:
    field_id: str
    name: str
    field_type: str = "text"
    required: bool = False
    default_value: Any = None
    options: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class WorkspaceSettings:
    working_hours_start: str = "09:00"
    working_hours_end: str = "17:00"
    timezone: str = "UTC"
    holidays: list[str] = field(default_factory=list)
    approval_workflow: ApprovalWorkflow = ApprovalWorkflow.NONE
    slack_channel: str = ""
    email_domain: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ActivityEvent:
    event_id: str
    workspace_id: str
    user_id: str
    action: str
    entity_type: str
    entity_id: str
    description: str = ""
    timestamp: str = field(default_factory=_now_iso)


@dataclass(frozen=True, slots=True)
class WorkspaceActivityDigest:
    workspace_id: str | None
    total_event_count: int = 0
    counts_by_action: dict[str, int] = field(default_factory=dict)
    counts_by_actor: dict[str, int] = field(default_factory=dict)
    counts_by_target_type: dict[str, int] = field(default_factory=dict)
    latest_activity_timestamp: str | None = None
    window_start: str | None = None
    window_end: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "total_event_count": self.total_event_count,
            "counts_by_action": dict(self.counts_by_action),
            "counts_by_actor": dict(self.counts_by_actor),
            "counts_by_target_type": dict(self.counts_by_target_type),
            "latest_activity_timestamp": self.latest_activity_timestamp,
            "window_start": self.window_start,
            "window_end": self.window_end,
        }


@dataclass(frozen=True, slots=True)
class WorkspaceMemberCapacity:
    member_id: str
    user_id: str
    display_name: str
    email: str = ""
    role: str = ""
    capacity_metadata: dict[str, Any] = field(default_factory=dict)
    calendar_event_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "member_id": self.member_id,
            "user_id": self.user_id,
            "display_name": self.display_name,
            "email": self.email,
            "role": self.role,
            "capacity_metadata": dict(self.capacity_metadata),
            "calendar_event_count": self.calendar_event_count,
        }


@dataclass(frozen=True, slots=True)
class WorkspaceResourceCapacity:
    resource_id: str
    name: str
    resource_type: str
    total_capacity: float = 0.0
    allocated: float = 0.0
    unit: str = ""
    utilization_metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource_id": self.resource_id,
            "name": self.name,
            "resource_type": self.resource_type,
            "total_capacity": self.total_capacity,
            "allocated": self.allocated,
            "unit": self.unit,
            "utilization_metadata": dict(self.utilization_metadata),
        }


@dataclass(frozen=True, slots=True)
class WorkspaceMemberCapacityReport:
    workspace_id: str
    members: tuple[WorkspaceMemberCapacity, ...] = field(default_factory=tuple)
    resources: tuple[WorkspaceResourceCapacity, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "members": [member.to_dict() for member in self.members],
            "resources": [resource.to_dict() for resource in self.resources],
        }


@dataclass(frozen=True, slots=True)
class CalendarEvent:
    event_id: str
    plan_id: str
    title: str
    start_date: str
    end_date: str = ""
    event_type: str = "milestone"


@dataclass(frozen=True, slots=True)
class Workspace:
    workspace_id: str
    name: str
    description: str = ""
    owner_id: str = ""
    members: list[TeamMember] = field(default_factory=list)
    settings: WorkspaceSettings = field(default_factory=WorkspaceSettings)
    resources: list[SharedResource] = field(default_factory=list)
    templates: list[WorkspaceTemplate] = field(default_factory=list)
    custom_fields: list[CustomField] = field(default_factory=list)
    plan_ids: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=_now_iso)
    updated_at: str = field(default_factory=_now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "workspace_id": self.workspace_id,
            "name": self.name,
            "description": self.description,
            "owner_id": self.owner_id,
            "members": [
                {"member_id": m.member_id, "user_id": m.user_id, "role": m.role.value}
                for m in self.members
            ],
            "plan_ids": self.plan_ids,
            "created_at": self.created_at,
        }


__all__ = [
    "ActivityEvent",
    "ApprovalWorkflow",
    "CalendarEvent",
    "CustomField",
    "ResourceAllocation",
    "SharedResource",
    "TeamMember",
    "Workspace",
    "WorkspaceActivityDigest",
    "WorkspaceMemberCapacity",
    "WorkspaceMemberCapacityReport",
    "WorkspaceResourceCapacity",
    "WorkspaceInvitation",
    "WorkspacePolicyFinding",
    "WorkspaceRole",
    "WorkspaceSettings",
    "WorkspaceTemplate",
    "_gen_id",
    "_now_iso",
]
