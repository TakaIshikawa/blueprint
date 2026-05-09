"""Plan archival system with configurable retention policies."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.archival.archive_manager import (
        ArchivalResult,
        ArchivalTrigger,
        ArchiveDataStore,
        ArchiveFilters,
        ArchiveManager,
        ArchiveReport,
        ArchiveReason,
        ArchiveStatus,
        ArchiveTriggerConfig,
        ArchivedPlan,
        ColdStorageBackend,
        CompletionDelay,
        RetentionPeriod,
        RetentionPolicy,
    )

_MODULE = "blueprint.archival.archive_manager"

_EXPORTS = {
    "ArchivalResult": _MODULE,
    "ArchivalTrigger": _MODULE,
    "ArchiveDataStore": _MODULE,
    "ArchiveFilters": _MODULE,
    "ArchiveManager": _MODULE,
    "ArchiveReport": _MODULE,
    "ArchiveReason": _MODULE,
    "ArchiveStatus": _MODULE,
    "ArchiveTriggerConfig": _MODULE,
    "ArchivedPlan": _MODULE,
    "ColdStorageBackend": _MODULE,
    "CompletionDelay": _MODULE,
    "RetentionPeriod": _MODULE,
    "RetentionPolicy": _MODULE,
}


def __getattr__(name: str) -> Any:
    """Load archival classes on demand."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
