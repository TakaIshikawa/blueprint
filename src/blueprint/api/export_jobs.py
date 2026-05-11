"""API-facing facade for Blueprint data exports."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from blueprint.export.data_exporter import (
    DataExporter,
    DataExportFormat,
    ExportFilters,
    ExportJobStatus,
    ExportOptions,
    ExportResult,
    ExportScope,
    InMemoryDataStore,
)


class ExportJobAPIValidationError(ValueError):
    """Raised when API-facing export input cannot be translated."""


class ExportFiltersRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workspace_id: str | None = None
    plan_ids: list[str] | None = None
    status: list[str] | None = None
    tags: list[str] | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    custom_query: dict[str, Any] | None = None

    def to_export_filters(self) -> ExportFilters:
        return ExportFilters(**self.model_dump())


class ExportOptionsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include_metadata: bool = True
    include_relationships: bool = True
    include_attachments: bool = False
    anonymize: bool = False
    schema_version: str = "1.0.0"

    def to_export_options(self) -> ExportOptions:
        return ExportOptions(**self.model_dump())


class CreateExportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    format: str = "json"
    scope: str = "all"
    destination: str = ""
    filters: ExportFiltersRequest | None = None
    options: ExportOptionsRequest = Field(default_factory=ExportOptionsRequest)

    @field_validator("format")
    @classmethod
    def validate_format(cls, value: str) -> str:
        if value not in {item.value for item in DataExportFormat}:
            raise ValueError(f"Unsupported export format: {value}")
        return value

    @field_validator("scope")
    @classmethod
    def validate_scope(cls, value: str) -> str:
        if value not in {item.value for item in ExportScope}:
            raise ValueError(f"Unsupported export scope: {value}")
        return value


class ExportManifestSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    export_id: str
    format: str
    scope: str
    record_counts: dict[str, int]
    checksums: dict[str, str]
    timestamp: str


class ExportJobResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    export_id: str
    status: str
    destination: str
    manifest: ExportManifestSummary
    data_size_bytes: int = Field(ge=0)


class ExportJobFacade:
    """Small facade that translates API request models into exporter calls."""

    def __init__(self, store: InMemoryDataStore | None = None) -> None:
        self.exporter = DataExporter(store=store or InMemoryDataStore())
        self._results: dict[str, ExportResult] = {}

    def create_export(self, request: CreateExportRequest | dict[str, Any]) -> ExportJobResponse:
        req = request if isinstance(request, CreateExportRequest) else CreateExportRequest(**request)
        fmt = DataExportFormat(req.format)
        options = req.options.to_export_options()
        filters = req.filters.to_export_filters() if req.filters else None
        if req.scope == ExportScope.FILTERED.value or filters is not None:
            result = self.exporter.export_all_data(fmt=fmt, filters=filters, options=options)
        elif req.scope == ExportScope.ALL.value:
            result = self.exporter.export_all_data(fmt=fmt, options=options)
        elif req.scope == ExportScope.WORKSPACE.value:
            if not req.filters or not req.filters.workspace_id:
                raise ExportJobAPIValidationError("workspace scope requires filters.workspace_id")
            data = self.exporter.export_workspace(req.filters.workspace_id, fmt=fmt, options=options)
            result = self.exporter._build_result(  # noqa: SLF001 - facade over existing exporter internals
                {"workspace": {"data": data.decode(errors="replace")}},
                fmt,
                ExportScope.WORKSPACE,
                options,
                filters,
            )
        else:
            if not req.filters or not req.filters.plan_ids:
                raise ExportJobAPIValidationError("plan_tree scope requires filters.plan_ids")
            plan = self.exporter.export_plan_with_dependencies(req.filters.plan_ids[0], options=options)
            result = self.exporter._build_result(  # noqa: SLF001
                {"plans": plan.get("plans", {})},
                fmt,
                ExportScope.PLAN_TREE,
                options,
                filters,
            )
        self._results[result.export_id] = result
        return ExportJobResponse(
            export_id=result.export_id,
            status=ExportJobStatus.COMPLETED.value,
            destination=req.destination,
            manifest=manifest_summary(result),
            data_size_bytes=len(result.data),
        )

    def get_status(self, export_id: str) -> dict[str, Any]:
        result = self._results.get(export_id)
        if result is None:
            return {"export_id": export_id, "status": "not_found"}
        return {"export_id": export_id, "status": ExportJobStatus.COMPLETED.value}

    def get_manifest_summary(self, export_id: str) -> ExportManifestSummary | None:
        result = self._results.get(export_id)
        return manifest_summary(result) if result else None


def manifest_summary(result: ExportResult) -> ExportManifestSummary:
    manifest = result.manifest
    return ExportManifestSummary(
        export_id=manifest.export_id,
        format=manifest.format,
        scope=manifest.scope,
        record_counts=manifest.record_counts,
        checksums=manifest.checksums,
        timestamp=manifest.timestamp,
    )


def validation_error_payload(exc: Exception) -> dict[str, Any]:
    return {
        "code": "validation_error",
        "message": str(exc),
        "details": {},
    }

