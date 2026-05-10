"""Plan versioning system with semantic version control."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.versioning.plan_version import PlanVersion
    from blueprint.versioning.version_manager import VersionManager

_VERSION_MODULE = "blueprint.versioning.plan_version"
_MANAGER_MODULE = "blueprint.versioning.version_manager"

_EXPORTS = {
    "PlanVersion": _VERSION_MODULE,
    "VersionManager": _MANAGER_MODULE,
}


def __getattr__(name: str) -> Any:
    """Load versioning classes on demand."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
