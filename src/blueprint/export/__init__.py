"""Comprehensive data export API for migrations and integrations."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.export.data_exporter import (
        DataExporter,
        DataExportFormat,
        ExportFilters,
        ExportJob,
        ExportJobStatus,
        ExportManifest,
        ExportManifestValidationResult,
        ExportOptions,
        ExportProgress,
        ExportResult,
        ExportScheduleConfig,
        ExportScope,
        InMemoryDataStore,
        UserDataExport,
    )

_MODULE = "blueprint.export.data_exporter"

_EXPORTS = {
    "DataExporter": _MODULE,
    "DataExportFormat": _MODULE,
    "ExportFilters": _MODULE,
    "ExportJob": _MODULE,
    "ExportJobStatus": _MODULE,
    "ExportManifest": _MODULE,
    "ExportManifestValidationResult": _MODULE,
    "ExportOptions": _MODULE,
    "ExportProgress": _MODULE,
    "ExportResult": _MODULE,
    "ExportScheduleConfig": _MODULE,
    "ExportScope": _MODULE,
    "InMemoryDataStore": _MODULE,
    "UserDataExport": _MODULE,
}


def __getattr__(name: str) -> Any:
    """Load export classes on demand to avoid dependency side effects."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
