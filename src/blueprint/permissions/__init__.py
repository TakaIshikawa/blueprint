"""Role-based access control with granular permissions."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.permissions.permission_manager import PermissionManager
    from blueprint.permissions.permission_model import (
        BuiltInRole,
        CustomRole,
        Operation,
        Permission,
        PermissionAuditEntry,
        ResourceHierarchy,
        ResourceType,
    )

_MANAGER_MODULE = "blueprint.permissions.permission_manager"
_MODEL_MODULE = "blueprint.permissions.permission_model"

_EXPORTS = {
    "PermissionManager": _MANAGER_MODULE,
    "BuiltInRole": _MODEL_MODULE,
    "CustomRole": _MODEL_MODULE,
    "Operation": _MODEL_MODULE,
    "Permission": _MODEL_MODULE,
    "PermissionAuditEntry": _MODEL_MODULE,
    "ResourceHierarchy": _MODEL_MODULE,
    "ResourceType": _MODEL_MODULE,
}


def __getattr__(name: str) -> Any:
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
