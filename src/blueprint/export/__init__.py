"""Comprehensive data export API for migrations and integrations."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from blueprint.export.data_exporter import (
        DataExporter,
        ExportCompression,
        ExportCompressionOption,
        DataExportFormat,
        ExportFilters,
        ExportFilterPreset,
        ExportJob,
        ExportJobStatus,
        ExportManifest,
        ExportManifestValidationResult,
        ExportOptions,
        ExportProgress,
        ExportRedactionPolicy,
        ExportResult,
        ExportScheduleConfig,
        ExportScope,
        InMemoryDataStore,
        UserDataExport,
        build_export_checksum_summary,
        build_export_manifest,
        get_export_compression_options,
        normalize_export_compression,
        parse_export_bundle,
        summarize_delta,
    )

_MODULE = "blueprint.export.data_exporter"

_EXPORTS = {
    "DataExporter": _MODULE,
    "ExportCompression": _MODULE,
    "ExportCompressionOption": _MODULE,
    "DataExportFormat": _MODULE,
    "ExportFilters": _MODULE,
    "ExportFilterPreset": _MODULE,
    "ExportJob": _MODULE,
    "ExportJobStatus": _MODULE,
    "ExportManifest": _MODULE,
    "ExportManifestValidationResult": _MODULE,
    "ExportOptions": _MODULE,
    "ExportProgress": _MODULE,
    "ExportRedactionPolicy": _MODULE,
    "ExportResult": _MODULE,
    "ExportScheduleConfig": _MODULE,
    "ExportScope": _MODULE,
    "InMemoryDataStore": _MODULE,
    "UserDataExport": _MODULE,
    "build_export_checksum_summary": _MODULE,
    "build_export_manifest": _MODULE,
    "get_export_compression_options": _MODULE,
    "normalize_export_compression": _MODULE,
    "parse_export_bundle": _MODULE,
    "summarize_delta": _MODULE,
}


def __getattr__(name: str) -> Any:
    """Load export classes on demand to avoid dependency side effects."""
    if name not in _EXPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(_EXPORTS[name]), name)
    globals()[name] = value
    return value


__all__ = list(_EXPORTS.keys())
