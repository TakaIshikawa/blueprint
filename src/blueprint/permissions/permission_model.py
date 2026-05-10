"""Permission and RBAC data models."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class BuiltInRole(str, Enum):
    VIEWER = "viewer"
    CONTRIBUTOR = "contributor"
    EDITOR = "editor"
    ADMIN = "admin"


class ResourceType(str, Enum):
    PLAN = "plan"
    TASK = "task"
    MILESTONE = "milestone"
    COMMENT = "comment"
    PORTFOLIO = "portfolio"
    WORKSPACE = "workspace"


class Operation(str, Enum):
    READ = "read"
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _gen_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


# Default permission sets per built-in role
ROLE_DEFAULTS: dict[BuiltInRole, set[Operation]] = {
    BuiltInRole.VIEWER: {Operation.READ},
    BuiltInRole.CONTRIBUTOR: {Operation.READ, Operation.CREATE},
    BuiltInRole.EDITOR: {Operation.READ, Operation.CREATE, Operation.UPDATE},
    BuiltInRole.ADMIN: {Operation.READ, Operation.CREATE, Operation.UPDATE, Operation.DELETE},
}


@dataclass(frozen=True, slots=True)
class Permission:
    permission_id: str
    resource_type: ResourceType
    resource_id: str
    user_id: str
    role: str
    operations: frozenset[Operation] = frozenset()
    granted_at: str = field(default_factory=_now_iso)
    granted_by: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CustomRole:
    role_id: str
    name: str
    description: str = ""
    operations: frozenset[Operation] = frozenset()
    created_at: str = field(default_factory=_now_iso)


@dataclass(frozen=True, slots=True)
class PermissionAuditEntry:
    entry_id: str
    user_id: str
    resource_type: ResourceType
    resource_id: str
    role: str
    operations: frozenset[Operation] = frozenset()
    timestamp: str = field(default_factory=_now_iso)


@dataclass(frozen=True, slots=True)
class ResourceHierarchy:
    """Defines parent-child for permission inheritance."""
    resource_type: ResourceType
    resource_id: str
    parent_type: ResourceType | None = None
    parent_id: str | None = None


__all__ = [
    "BuiltInRole",
    "CustomRole",
    "Operation",
    "Permission",
    "PermissionAuditEntry",
    "ROLE_DEFAULTS",
    "ResourceHierarchy",
    "ResourceType",
    "_gen_id",
    "_now_iso",
]
