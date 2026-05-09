"""Automated backup system with point-in-time recovery."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.backup.backup_manager import (
        Backup,
        BackupDataStore,
        BackupManager,
        BackupSchedule,
        BackupScope,
        BackupStatus,
        BackupType,
        CleanupResult,
        RestoreResult,
        RestoreStatus,
        RetentionPolicy,
        StorageBackend,
        VerificationResult,
    )

_MODULE = "blueprint.backup.backup_manager"

_EXPORTS = {
    "Backup": _MODULE,
    "BackupDataStore": _MODULE,
    "BackupManager": _MODULE,
    "BackupSchedule": _MODULE,
    "BackupScope": _MODULE,
    "BackupStatus": _MODULE,
    "BackupType": _MODULE,
    "CleanupResult": _MODULE,
    "RestoreResult": _MODULE,
    "RestoreStatus": _MODULE,
    "RetentionPolicy": _MODULE,
    "StorageBackend": _MODULE,
    "VerificationResult": _MODULE,
}


def __getattr__(name: str) -> Any:
    """Load backup classes on demand."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
