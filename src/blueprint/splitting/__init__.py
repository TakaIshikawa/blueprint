"""Plan split functionality for decomposing large plans."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.splitting.plan_splitter import (
        ExternalDependency,
        PlanSplitter,
        SplitConfig,
        SplitDataStore,
        SplitPreview,
        SplitReport,
        SplitResult,
        SplitStrategy,
        SplitSuggestion,
        ValidationResult,
    )

_MODULE = "blueprint.splitting.plan_splitter"

_EXPORTS = {
    "ExternalDependency": _MODULE,
    "PlanSplitter": _MODULE,
    "SplitConfig": _MODULE,
    "SplitDataStore": _MODULE,
    "SplitPreview": _MODULE,
    "SplitReport": _MODULE,
    "SplitResult": _MODULE,
    "SplitStrategy": _MODULE,
    "SplitSuggestion": _MODULE,
    "ValidationResult": _MODULE,
}


def __getattr__(name: str) -> Any:
    """Load split classes on demand."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
