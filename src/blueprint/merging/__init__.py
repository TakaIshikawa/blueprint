"""Plan merge functionality for combining multiple plans."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.merging.plan_merger import (
        Conflict,
        ConflictType,
        MergeConfig,
        MergeDataStore,
        MergePreview,
        MergeReport,
        MergeResult,
        MergeStrategy,
        PlanMerger,
        ResolutionStrategy,
    )

_MODULE = "blueprint.merging.plan_merger"

_EXPORTS = {
    "Conflict": _MODULE,
    "ConflictType": _MODULE,
    "MergeConfig": _MODULE,
    "MergeDataStore": _MODULE,
    "MergePreview": _MODULE,
    "MergeReport": _MODULE,
    "MergeResult": _MODULE,
    "MergeStrategy": _MODULE,
    "PlanMerger": _MODULE,
    "ResolutionStrategy": _MODULE,
}


def __getattr__(name: str) -> Any:
    """Load merge classes on demand."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
