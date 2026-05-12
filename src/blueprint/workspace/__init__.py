"""Team workspace for organizing plans by team."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.workspace.team_workspace import TeamWorkspace, workspace_configuration_snapshot
    from blueprint.workspace.workspace_model import (
        ActivityEvent,
        ApprovalWorkflow,
        CalendarEvent,
        CustomField,
        ResourceAllocation,
        SharedResource,
        TeamMember,
        Workspace,
        WorkspaceActivityDigest,
        WorkspaceInvitation,
        WorkspaceMemberCapacity,
        WorkspaceMemberCapacityReport,
        WorkspaceResourceCapacity,
        WorkspacePolicyFinding,
        WorkspaceRole,
        WorkspaceSettings,
        WorkspaceTemplate,
    )

_MANAGER_MODULE = "blueprint.workspace.team_workspace"
_MODEL_MODULE = "blueprint.workspace.workspace_model"

_EXPORTS = {
    "TeamWorkspace": _MANAGER_MODULE,
    "workspace_configuration_snapshot": _MANAGER_MODULE,
    "ActivityEvent": _MODEL_MODULE,
    "ApprovalWorkflow": _MODEL_MODULE,
    "CalendarEvent": _MODEL_MODULE,
    "CustomField": _MODEL_MODULE,
    "ResourceAllocation": _MODEL_MODULE,
    "SharedResource": _MODEL_MODULE,
    "TeamMember": _MODEL_MODULE,
    "Workspace": _MODEL_MODULE,
    "WorkspaceActivityDigest": _MODEL_MODULE,
    "WorkspaceInvitation": _MODEL_MODULE,
    "WorkspaceMemberCapacity": _MODEL_MODULE,
    "WorkspaceMemberCapacityReport": _MODEL_MODULE,
    "WorkspaceResourceCapacity": _MODEL_MODULE,
    "WorkspacePolicyFinding": _MODEL_MODULE,
    "WorkspaceRole": _MODEL_MODULE,
    "WorkspaceSettings": _MODEL_MODULE,
    "WorkspaceTemplate": _MODEL_MODULE,
}


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
