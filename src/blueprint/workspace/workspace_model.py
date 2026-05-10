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
    "SharedResource",
    "TeamMember",
    "Workspace",
    "WorkspaceRole",
    "WorkspaceSettings",
    "WorkspaceTemplate",
    "_gen_id",
    "_now_iso",
]
