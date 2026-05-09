"""Comprehensive data export API for migrations and integrations.

Supports multiple export formats (JSON, CSV, SQL, Parquet, Protobuf),
scopes (all, workspace, plan tree, filtered), incremental exports,
streaming, and background job scheduling.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import struct
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterator

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCHEMA_VERSION = "1.0.0"
DEFAULT_CHUNK_SIZE = 500


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class DataExportFormat(str, Enum):
    """Supported export formats."""

    JSON = "json"
    CSV = "csv"
    SQL = "sql"
    PARQUET = "parquet"
    PROTOBUF = "protobuf"


class ExportScope(str, Enum):
    """Supported export scopes."""

    ALL = "all"
    WORKSPACE = "workspace"
    PLAN_TREE = "plan_tree"
    FILTERED = "filtered"


class ExportJobStatus(str, Enum):
    """Status of a background export job."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ---------------------------------------------------------------------------
# Configuration / options dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ExportOptions:
    """Controls what metadata and relations to include in an export."""

    include_metadata: bool = True
    include_relationships: bool = True
    include_attachments: bool = False
    anonymize: bool = False
    schema_version: str = SCHEMA_VERSION


@dataclass(frozen=True, slots=True)
class ExportFilters:
    """Filters for scoped or incremental exports."""

    workspace_id: str | None = None
    plan_ids: list[str] | None = None
    status: list[str] | None = None
    tags: list[str] | None = None
    date_from: datetime | None = None
    date_to: datetime | None = None
    custom_query: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ExportScheduleConfig:
    """Configuration for a scheduled export job."""

    format: DataExportFormat = DataExportFormat.JSON
    scope: ExportScope = ExportScope.ALL
    filters: ExportFilters | None = None
    options: ExportOptions | None = None
    destination: str = ""


# ---------------------------------------------------------------------------
# Result models
# ---------------------------------------------------------------------------


class ExportManifest(BaseModel):
    """Metadata about an export for verification and re-import."""

    model_config = ConfigDict(extra="forbid")

    export_id: str = Field(min_length=1)
    schema_version: str = SCHEMA_VERSION
    format: str
    scope: str
    timestamp: str
    record_counts: dict[str, int] = Field(default_factory=dict)
    checksums: dict[str, str] = Field(default_factory=dict)
    filters_applied: dict[str, Any] = Field(default_factory=dict)


class ExportResult(BaseModel):
    """Result of a completed export operation."""

    model_config = ConfigDict(extra="forbid")

    export_id: str = Field(min_length=1)
    format: str
    scope: str
    data: bytes
    manifest: ExportManifest
    record_count: int = Field(ge=0)
    created_at: str


class UserDataExport(BaseModel):
    """GDPR-compliant user data export."""

    model_config = ConfigDict(extra="forbid")

    export_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    plans: list[dict[str, Any]] = Field(default_factory=list)
    tasks: list[dict[str, Any]] = Field(default_factory=list)
    activity: list[dict[str, Any]] = Field(default_factory=list)
    exported_at: str
    anonymized: bool = False


class ExportJob(BaseModel):
    """Represents a scheduled or background export job."""

    model_config = ConfigDict(extra="forbid")

    job_id: str = Field(min_length=1)
    schedule: str = Field(min_length=1)
    config: dict[str, Any] = Field(default_factory=dict)
    status: str = ExportJobStatus.PENDING.value
    created_at: str
    next_run: str | None = None
    last_run: str | None = None
    result: dict[str, Any] | None = None


class ExportProgress(BaseModel):
    """Tracks progress of a running export."""

    model_config = ConfigDict(extra="forbid")

    export_id: str = Field(min_length=1)
    total_records: int = Field(ge=0)
    processed_records: int = Field(ge=0)
    status: str = ExportJobStatus.RUNNING.value
    started_at: str
    last_chunk_at: str | None = None


# ---------------------------------------------------------------------------
# Data store protocol
# ---------------------------------------------------------------------------


@dataclass
class InMemoryDataStore:
    """Simple in-memory data store for export operations.

    Production implementations would back this with SQLAlchemy / database
    queries.  This default enables zero-dependency usage and testing.
    """

    workspaces: dict[str, dict[str, Any]] = field(default_factory=dict)
    plans: dict[str, dict[str, Any]] = field(default_factory=dict)
    tasks: dict[str, dict[str, Any]] = field(default_factory=dict)
    users: dict[str, dict[str, Any]] = field(default_factory=dict)
    settings: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    attachments: dict[str, bytes] = field(default_factory=dict)

    def get_all_data(self) -> dict[str, Any]:
        """Return all stored data as a dict."""
        return {
            "workspaces": dict(self.workspaces),
            "plans": dict(self.plans),
            "tasks": dict(self.tasks),
            "users": dict(self.users),
            "settings": dict(self.settings),
            "events": list(self.events),
        }

    def get_workspace_data(self, workspace_id: str) -> dict[str, Any]:
        """Return data scoped to a single workspace."""
        workspace = self.workspaces.get(workspace_id, {})
        ws_plan_ids = workspace.get("plan_ids", [])
        plans = {pid: self.plans[pid] for pid in ws_plan_ids if pid in self.plans}
        task_ids: list[str] = []
        for plan in plans.values():
            task_ids.extend(t["id"] for t in plan.get("tasks", []))
        tasks = {tid: self.tasks[tid] for tid in task_ids if tid in self.tasks}
        user_ids: set[str] = set()
        for plan in plans.values():
            user_ids.update(plan.get("user_ids", []))
        users = {uid: self.users[uid] for uid in user_ids if uid in self.users}
        return {
            "workspace": workspace,
            "plans": plans,
            "tasks": tasks,
            "users": users,
        }

    def get_plan_tree(self, plan_id: str, depth: int = -1) -> dict[str, Any]:
        """Return a plan and its recursive dependencies."""
        visited: set[str] = set()
        result: dict[str, dict[str, Any]] = {}
        self._collect_plan(plan_id, depth, 0, visited, result)
        return result

    def _collect_plan(
        self,
        plan_id: str,
        max_depth: int,
        current_depth: int,
        visited: set[str],
        result: dict[str, dict[str, Any]],
    ) -> None:
        if plan_id in visited:
            return
        if max_depth >= 0 and current_depth > max_depth:
            return
        visited.add(plan_id)
        plan = self.plans.get(plan_id)
        if plan is None:
            return
        result[plan_id] = plan
        for task in plan.get("tasks", []):
            for dep_id in task.get("depends_on", []):
                # Dependency IDs may reference tasks in other plans; look up
                # the parent plan of the dependency.
                dep_plan_id = self.tasks.get(dep_id, {}).get("execution_plan_id")
                if dep_plan_id and dep_plan_id != plan_id:
                    self._collect_plan(
                        dep_plan_id, max_depth, current_depth + 1, visited, result,
                    )

    def get_user_data(self, user_id: str) -> dict[str, Any]:
        """Return all data associated with a specific user."""
        user = self.users.get(user_id, {})
        user_plans = {
            pid: p
            for pid, p in self.plans.items()
            if user_id in p.get("user_ids", [])
        }
        user_tasks = {
            tid: t
            for tid, t in self.tasks.items()
            if t.get("owner_id") == user_id
        }
        user_events = [
            e for e in self.events if e.get("user_id") == user_id
        ]
        return {
            "user": user,
            "plans": user_plans,
            "tasks": user_tasks,
            "activity": user_events,
        }

    def get_filtered_data(self, filters: ExportFilters) -> dict[str, Any]:
        """Return data matching the given filters."""
        plans = dict(self.plans)
        if filters.plan_ids:
            plans = {pid: p for pid, p in plans.items() if pid in filters.plan_ids}
        if filters.status:
            plans = {
                pid: p
                for pid, p in plans.items()
                if p.get("status") in filters.status
            }
        if filters.tags:
            tag_set = set(filters.tags)
            plans = {
                pid: p
                for pid, p in plans.items()
                if tag_set.intersection(p.get("tags", []))
            }
        if filters.date_from:
            plans = {
                pid: p
                for pid, p in plans.items()
                if _parse_dt(p.get("created_at")) >= filters.date_from
            }
        if filters.date_to:
            plans = {
                pid: p
                for pid, p in plans.items()
                if _parse_dt(p.get("created_at")) <= filters.date_to
            }
        task_ids: list[str] = []
        for plan in plans.values():
            task_ids.extend(t["id"] for t in plan.get("tasks", []))
        tasks = {tid: self.tasks[tid] for tid in task_ids if tid in self.tasks}
        return {"plans": plans, "tasks": tasks}

    def get_changes_since(self, since: datetime) -> dict[str, Any]:
        """Return records modified after *since* (incremental export)."""
        plans = {
            pid: p
            for pid, p in self.plans.items()
            if _parse_dt(p.get("updated_at")) >= since
        }
        tasks = {
            tid: t
            for tid, t in self.tasks.items()
            if _parse_dt(t.get("updated_at")) >= since
        }
        events = [
            e for e in self.events if _parse_dt(e.get("created_at")) >= since
        ]
        return {"plans": plans, "tasks": tasks, "events": events}

    def get_changes_between(
        self, start: datetime, end: datetime,
    ) -> dict[str, Any]:
        """Return records modified between two timestamps (delta export)."""
        plans = {
            pid: p
            for pid, p in self.plans.items()
            if start <= _parse_dt(p.get("updated_at")) <= end
        }
        tasks = {
            tid: t
            for tid, t in self.tasks.items()
            if start <= _parse_dt(t.get("updated_at")) <= end
        }
        events = [
            e
            for e in self.events
            if start <= _parse_dt(e.get("created_at")) <= end
        ]
        return {"plans": plans, "tasks": tasks, "events": events}


# ---------------------------------------------------------------------------
# Format serializers
# ---------------------------------------------------------------------------


def _serialize_json(data: dict[str, Any], options: ExportOptions) -> bytes:
    """Serialize data to JSON with schema version."""
    payload = {
        "schema_version": options.schema_version,
        "exported_at": _now_iso(),
        "data": _apply_options(data, options),
    }
    return json.dumps(payload, indent=2, default=str).encode("utf-8")


def _serialize_csv(data: dict[str, Any], options: ExportOptions) -> bytes:
    """Serialize data to CSV — one table per entity type, concatenated."""
    buf = io.StringIO()
    processed = _apply_options(data, options)

    for section_name, section in processed.items():
        if isinstance(section, dict):
            rows = list(section.values())
        elif isinstance(section, list):
            rows = section
        else:
            continue

        if not rows:
            continue

        # Flatten nested dicts for CSV
        flat_rows = [_flatten_dict(r) if isinstance(r, dict) else {"value": r} for r in rows]
        fieldnames = list(flat_rows[0].keys())

        buf.write(f"# {section_name}\n")
        writer = csv.DictWriter(buf, fieldnames=fieldnames)
        writer.writeheader()
        for row in flat_rows:
            writer.writerow({k: _csv_value(row.get(k, "")) for k in fieldnames})
        buf.write("\n")

    return buf.getvalue().encode("utf-8")


def _serialize_sql(data: dict[str, Any], options: ExportOptions) -> bytes:
    """Serialize data to SQL INSERT statements."""
    lines: list[str] = [
        f"-- Blueprint Data Export v{options.schema_version}",
        f"-- Exported at {_now_iso()}",
        "",
    ]
    processed = _apply_options(data, options)

    for table_name, section in processed.items():
        if isinstance(section, dict):
            rows = list(section.values())
        elif isinstance(section, list):
            rows = section
        else:
            continue

        if not rows:
            continue

        flat_rows = [_flatten_dict(r) if isinstance(r, dict) else {"value": r} for r in rows]
        columns = list(flat_rows[0].keys())
        col_defs = ", ".join(columns)

        for row in flat_rows:
            vals = ", ".join(_sql_escape(row.get(c, "")) for c in columns)
            lines.append(f"INSERT INTO {table_name} ({col_defs}) VALUES ({vals});")

        lines.append("")

    return "\n".join(lines).encode("utf-8")


def _serialize_parquet(data: dict[str, Any], options: ExportOptions) -> bytes:
    """Serialize data to a simplified columnar binary format.

    This is a lightweight columnar representation rather than full Apache
    Parquet (which would require ``pyarrow``).  The format stores a JSON
    header with schema information followed by column-oriented binary data,
    suitable for analytics pipelines that can ingest structured binary.
    """
    processed = _apply_options(data, options)
    header = {
        "schema_version": options.schema_version,
        "format": "blueprint-columnar-v1",
        "exported_at": _now_iso(),
        "tables": {},
    }
    column_data: list[bytes] = []
    offset = 0

    for table_name, section in processed.items():
        if isinstance(section, dict):
            rows = list(section.values())
        elif isinstance(section, list):
            rows = section
        else:
            continue
        if not rows:
            continue

        flat_rows = [_flatten_dict(r) if isinstance(r, dict) else {"value": r} for r in rows]
        columns = list(flat_rows[0].keys())

        table_meta: dict[str, Any] = {"columns": columns, "row_count": len(flat_rows), "column_offsets": {}}

        for col in columns:
            col_values = json.dumps([str(row.get(col, "")) for row in flat_rows]).encode("utf-8")
            length = len(col_values)
            table_meta["column_offsets"][col] = {"offset": offset, "length": length}
            column_data.append(col_values)
            offset += length

        header["tables"][table_name] = table_meta

    header_bytes = json.dumps(header, default=str).encode("utf-8")
    # Format: 4-byte header length (big-endian) + header + column data
    result = struct.pack(">I", len(header_bytes)) + header_bytes
    for chunk in column_data:
        result += chunk
    return result


def _serialize_protobuf(data: dict[str, Any], options: ExportOptions) -> bytes:
    """Serialize data to a self-describing binary format.

    Uses a TLV (type-length-value) encoding rather than real Protocol
    Buffers to avoid depending on ``protobuf`` / ``grpcio``.  The wire
    format is:
        magic (4 bytes) + schema_version_len (2) + schema_version +
        for each table:
            table_name_len (2) + table_name +
            row_count (4) +
            for each row: data_len (4) + json_bytes
    """
    processed = _apply_options(data, options)
    buf = io.BytesIO()
    buf.write(b"BPEX")  # magic bytes
    sv = options.schema_version.encode("utf-8")
    buf.write(struct.pack(">H", len(sv)))
    buf.write(sv)

    for table_name, section in processed.items():
        if isinstance(section, dict):
            rows = list(section.values())
        elif isinstance(section, list):
            rows = section
        else:
            continue
        if not rows:
            continue

        tn = table_name.encode("utf-8")
        buf.write(struct.pack(">H", len(tn)))
        buf.write(tn)
        buf.write(struct.pack(">I", len(rows)))

        for row in rows:
            row_bytes = json.dumps(row, default=str).encode("utf-8")
            buf.write(struct.pack(">I", len(row_bytes)))
            buf.write(row_bytes)

    return buf.getvalue()


# ---------------------------------------------------------------------------
# Format dispatcher
# ---------------------------------------------------------------------------

_SERIALIZERS: dict[DataExportFormat, Any] = {
    DataExportFormat.JSON: _serialize_json,
    DataExportFormat.CSV: _serialize_csv,
    DataExportFormat.SQL: _serialize_sql,
    DataExportFormat.PARQUET: _serialize_parquet,
    DataExportFormat.PROTOBUF: _serialize_protobuf,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_id() -> str:
    return f"exp-{uuid.uuid4().hex[:12]}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _parse_dt(value: Any) -> datetime:
    """Parse a datetime from string or return a minimal datetime."""
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            pass
    return datetime.min.replace(tzinfo=timezone.utc)


def _flatten_dict(d: dict[str, Any], prefix: str = "") -> dict[str, str]:
    """Flatten a nested dict into a single-level dict with dotted keys."""
    items: dict[str, str] = {}
    for k, v in d.items():
        key = f"{prefix}{k}" if not prefix else f"{prefix}.{k}"
        if isinstance(v, dict):
            items.update(_flatten_dict(v, key))
        elif isinstance(v, list):
            items[key] = "; ".join(str(i) for i in v)
        else:
            items[key] = str(v) if v is not None else ""
    return items


def _csv_value(v: Any) -> str:
    if isinstance(v, list):
        return "; ".join(str(i) for i in v)
    return str(v) if v is not None else ""


def _sql_escape(v: Any) -> str:
    if v is None or v == "":
        return "NULL"
    s = str(v).replace("'", "''")
    return f"'{s}'"


def _apply_options(data: dict[str, Any], options: ExportOptions) -> dict[str, Any]:
    """Apply export options (anonymization, metadata stripping) to data."""
    result = dict(data)

    if options.anonymize:
        result = _anonymize_data(result)

    if not options.include_metadata:
        result = _strip_metadata(result)

    if not options.include_relationships:
        result = _strip_relationships(result)

    return result


def _anonymize_data(data: dict[str, Any]) -> dict[str, Any]:
    """Replace PII fields with anonymized placeholders."""
    sensitive_keys = {"email", "name", "display_name", "assignee", "reporter", "owner_id", "user_id"}
    return _walk_anonymize(data, sensitive_keys)


def _walk_anonymize(obj: Any, sensitive: set[str]) -> Any:
    if isinstance(obj, dict):
        return {
            k: (f"anon-{hashlib.sha256(str(v).encode()).hexdigest()[:8]}" if k in sensitive and v else _walk_anonymize(v, sensitive))
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_walk_anonymize(item, sensitive) for item in obj]
    return obj


def _strip_metadata(data: dict[str, Any]) -> dict[str, Any]:
    """Remove timestamp and version metadata fields."""
    meta_keys = {"created_at", "updated_at", "exported_at", "generation_model", "generation_tokens", "generation_prompt"}
    return _walk_strip(data, meta_keys)


def _strip_relationships(data: dict[str, Any]) -> dict[str, Any]:
    """Remove relationship / dependency fields."""
    rel_keys = {"depends_on", "user_ids", "plan_ids", "execution_plan_id", "implementation_brief_id"}
    return _walk_strip(data, rel_keys)


def _walk_strip(obj: Any, keys_to_strip: set[str]) -> Any:
    if isinstance(obj, dict):
        return {k: _walk_strip(v, keys_to_strip) for k, v in obj.items() if k not in keys_to_strip}
    if isinstance(obj, list):
        return [_walk_strip(item, keys_to_strip) for item in obj]
    return obj


# ---------------------------------------------------------------------------
# DataExporter
# ---------------------------------------------------------------------------


class DataExporter:
    """Comprehensive data export API for migrations and integrations.

    Provides methods for exporting data in multiple formats and scopes,
    with support for incremental exports, streaming, and background jobs.
    """

    def __init__(self, store: InMemoryDataStore | None = None) -> None:
        self._store = store or InMemoryDataStore()
        self._jobs: dict[str, ExportJob] = {}
        self._progress: dict[str, ExportProgress] = {}

    # -- public API --------------------------------------------------------

    def export_all_data(
        self,
        fmt: DataExportFormat = DataExportFormat.JSON,
        filters: ExportFilters | None = None,
        options: ExportOptions | None = None,
    ) -> ExportResult:
        """Export all data (or filtered subset) in the requested format."""
        opts = options or ExportOptions()
        if filters:
            raw = self._store.get_filtered_data(filters)
            scope = ExportScope.FILTERED
        else:
            raw = self._store.get_all_data()
            scope = ExportScope.ALL

        return self._build_result(raw, fmt, scope, opts, filters)

    def export_workspace(
        self,
        workspace_id: str,
        fmt: DataExportFormat = DataExportFormat.JSON,
        options: ExportOptions | None = None,
    ) -> bytes:
        """Export all data within a workspace, returning raw bytes."""
        opts = options or ExportOptions()
        raw = self._store.get_workspace_data(workspace_id)
        serializer = _SERIALIZERS[fmt]
        return serializer(raw, opts)

    def export_plan_with_dependencies(
        self,
        plan_id: str,
        depth: int = -1,
        options: ExportOptions | None = None,
    ) -> dict[str, Any]:
        """Export a plan and its transitive dependencies as a dict."""
        opts = options or ExportOptions()
        tree = self._store.get_plan_tree(plan_id, depth)
        processed = _apply_options({"plans": tree}, opts)
        return {
            "schema_version": opts.schema_version,
            "scope": ExportScope.PLAN_TREE.value,
            "plan_id": plan_id,
            "depth": depth,
            "exported_at": _now_iso(),
            **processed,
        }

    def export_user_data(
        self,
        user_id: str,
        anonymize: bool = False,
    ) -> UserDataExport:
        """Export all data for a specific user (GDPR data portability)."""
        raw = self._store.get_user_data(user_id)
        if anonymize:
            raw = _anonymize_data(raw)
        return UserDataExport(
            export_id=_generate_id(),
            user_id=user_id,
            plans=list(raw.get("plans", {}).values()),
            tasks=list(raw.get("tasks", {}).values()),
            activity=raw.get("activity", []),
            exported_at=_now_iso(),
            anonymized=anonymize,
        )

    def schedule_export(
        self,
        schedule: str,
        config: ExportScheduleConfig | None = None,
    ) -> ExportJob:
        """Schedule a recurring export job.

        Args:
            schedule: A cron-like schedule string (e.g. ``"daily"``,
                ``"0 2 * * *"``).
            config: Export configuration to use for each run.
        """
        cfg = config or ExportScheduleConfig()
        job = ExportJob(
            job_id=_generate_id(),
            schedule=schedule,
            config={
                "format": cfg.format.value,
                "scope": cfg.scope.value,
                "destination": cfg.destination,
            },
            status=ExportJobStatus.PENDING.value,
            created_at=_now_iso(),
            next_run=_now_iso(),
        )
        self._jobs[job.job_id] = job
        return job

    def stream_export(
        self,
        filters: ExportFilters | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        fmt: DataExportFormat = DataExportFormat.JSON,
        options: ExportOptions | None = None,
    ) -> Iterator[bytes]:
        """Stream an export as chunked byte sequences.

        Yields one chunk per entity section (plans, tasks, etc.) to keep
        memory usage bounded for large datasets.
        """
        opts = options or ExportOptions()
        if filters:
            raw = self._store.get_filtered_data(filters)
        else:
            raw = self._store.get_all_data()

        processed = _apply_options(raw, opts)

        export_id = _generate_id()
        total = sum(
            len(v) if isinstance(v, (dict, list)) else 0
            for v in processed.values()
        )
        progress = ExportProgress(
            export_id=export_id,
            total_records=total,
            processed_records=0,
            status=ExportJobStatus.RUNNING.value,
            started_at=_now_iso(),
        )
        self._progress[export_id] = progress

        serializer = _SERIALIZERS[fmt]
        records_done = 0

        for section_name, section in processed.items():
            if isinstance(section, dict):
                items = list(section.values())
            elif isinstance(section, list):
                items = section
            else:
                continue

            # Yield in pages of chunk_size
            for i in range(0, max(len(items), 1), max(chunk_size, 1)):
                page = items[i : i + chunk_size]
                chunk_data = {section_name: page}
                chunk_bytes = serializer(chunk_data, opts)
                records_done += len(page)
                self._progress[export_id] = ExportProgress(
                    export_id=export_id,
                    total_records=total,
                    processed_records=records_done,
                    status=ExportJobStatus.RUNNING.value,
                    started_at=progress.started_at,
                    last_chunk_at=_now_iso(),
                )
                yield chunk_bytes

        self._progress[export_id] = ExportProgress(
            export_id=export_id,
            total_records=total,
            processed_records=records_done,
            status=ExportJobStatus.COMPLETED.value,
            started_at=progress.started_at,
            last_chunk_at=_now_iso(),
        )

    # -- incremental exports -----------------------------------------------

    def export_changes_since(
        self,
        since: datetime,
        fmt: DataExportFormat = DataExportFormat.JSON,
        options: ExportOptions | None = None,
    ) -> ExportResult:
        """Export only records modified after *since*."""
        opts = options or ExportOptions()
        raw = self._store.get_changes_since(since)
        return self._build_result(
            raw, fmt, ExportScope.FILTERED, opts,
            ExportFilters(date_from=since),
        )

    def export_delta(
        self,
        start: datetime,
        end: datetime,
        fmt: DataExportFormat = DataExportFormat.JSON,
        options: ExportOptions | None = None,
    ) -> ExportResult:
        """Export records modified between two timestamps."""
        opts = options or ExportOptions()
        raw = self._store.get_changes_between(start, end)
        return self._build_result(
            raw, fmt, ExportScope.FILTERED, opts,
            ExportFilters(date_from=start, date_to=end),
        )

    # -- job management ----------------------------------------------------

    def get_job(self, job_id: str) -> ExportJob | None:
        return self._jobs.get(job_id)

    def cancel_job(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if job is None:
            return False
        self._jobs[job_id] = job.model_copy(
            update={"status": ExportJobStatus.CANCELLED.value},
        )
        return True

    def get_progress(self, export_id: str) -> ExportProgress | None:
        return self._progress.get(export_id)

    # -- private helpers ---------------------------------------------------

    def _build_result(
        self,
        raw: dict[str, Any],
        fmt: DataExportFormat,
        scope: ExportScope,
        options: ExportOptions,
        filters: ExportFilters | None = None,
    ) -> ExportResult:
        serializer = _SERIALIZERS[fmt]
        data = serializer(raw, options)
        export_id = _generate_id()

        record_count = sum(
            len(v) if isinstance(v, (dict, list)) else 0
            for v in raw.values()
        )

        manifest = ExportManifest(
            export_id=export_id,
            schema_version=options.schema_version,
            format=fmt.value,
            scope=scope.value,
            timestamp=_now_iso(),
            record_counts=_count_records(raw),
            checksums={"sha256": _checksum(data)},
            filters_applied=_filters_to_dict(filters) if filters else {},
        )

        return ExportResult(
            export_id=export_id,
            format=fmt.value,
            scope=scope.value,
            data=data,
            manifest=manifest,
            record_count=record_count,
            created_at=_now_iso(),
        )


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


def _count_records(data: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for k, v in data.items():
        if isinstance(v, dict):
            counts[k] = len(v)
        elif isinstance(v, list):
            counts[k] = len(v)
    return counts


def _filters_to_dict(filters: ExportFilters) -> dict[str, Any]:
    result: dict[str, Any] = {}
    if filters.workspace_id:
        result["workspace_id"] = filters.workspace_id
    if filters.plan_ids:
        result["plan_ids"] = filters.plan_ids
    if filters.status:
        result["status"] = filters.status
    if filters.tags:
        result["tags"] = filters.tags
    if filters.date_from:
        result["date_from"] = filters.date_from.isoformat()
    if filters.date_to:
        result["date_to"] = filters.date_to.isoformat()
    if filters.custom_query:
        result["custom_query"] = filters.custom_query
    return result
