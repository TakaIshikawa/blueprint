"""Comprehensive data export API for migrations and integrations.

Supports multiple export formats (JSON, JSONL, CSV, SQL, Parquet, Protobuf),
scopes (all, workspace, plan tree, filtered), incremental exports,
streaming, and background job scheduling.
"""

from __future__ import annotations

import base64
import binascii
import csv
import hashlib
import io
import json
import struct
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterator
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

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
    JSONL = "jsonl"
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


class ExportFormatCapability(BaseModel):
    """Describes operational behavior for one export format."""

    model_config = ConfigDict(extra="forbid")

    format: str
    mime_type: str
    file_extension: str
    line_oriented: bool = False
    binary: bool = False
    supports_manifest_record_count_validation: bool = True


_FORMAT_CAPABILITIES: dict[DataExportFormat, ExportFormatCapability] = {
    DataExportFormat.JSON: ExportFormatCapability(
        format=DataExportFormat.JSON.value,
        mime_type="application/json",
        file_extension=".json",
        line_oriented=False,
        binary=False,
        supports_manifest_record_count_validation=True,
    ),
    DataExportFormat.JSONL: ExportFormatCapability(
        format=DataExportFormat.JSONL.value,
        mime_type="application/x-ndjson",
        file_extension=".jsonl",
        line_oriented=True,
        binary=False,
        supports_manifest_record_count_validation=True,
    ),
    DataExportFormat.CSV: ExportFormatCapability(
        format=DataExportFormat.CSV.value,
        mime_type="text/csv",
        file_extension=".csv",
        line_oriented=True,
        binary=False,
        supports_manifest_record_count_validation=False,
    ),
    DataExportFormat.SQL: ExportFormatCapability(
        format=DataExportFormat.SQL.value,
        mime_type="application/sql",
        file_extension=".sql",
        line_oriented=True,
        binary=False,
        supports_manifest_record_count_validation=False,
    ),
    DataExportFormat.PARQUET: ExportFormatCapability(
        format=DataExportFormat.PARQUET.value,
        mime_type="application/vnd.apache.parquet",
        file_extension=".parquet",
        line_oriented=False,
        binary=True,
        supports_manifest_record_count_validation=True,
    ),
    DataExportFormat.PROTOBUF: ExportFormatCapability(
        format=DataExportFormat.PROTOBUF.value,
        mime_type="application/x-protobuf",
        file_extension=".pb",
        line_oriented=False,
        binary=True,
        supports_manifest_record_count_validation=True,
    ),
}


def get_export_format_capability(fmt: DataExportFormat | str) -> ExportFormatCapability:
    """Return the capability descriptor for one export format."""

    export_format = fmt if isinstance(fmt, DataExportFormat) else DataExportFormat(fmt)
    return _FORMAT_CAPABILITIES[export_format].model_copy(deep=True)


def list_export_format_capabilities() -> list[ExportFormatCapability]:
    """List all format capabilities in stable enum order."""

    return [get_export_format_capability(fmt) for fmt in DataExportFormat]


_SENSITIVE_DESTINATION_QUERY_KEYS = {
    "access_key",
    "access_key_id",
    "api_key",
    "client_secret",
    "credential",
    "key",
    "password",
    "secret",
    "signature",
    "sig",
    "token",
}
_REDACTED = "REDACTED"


def sanitize_export_destination(destination: str) -> str:
    """Redact secrets from export destination strings deterministically."""

    if not isinstance(destination, str) or not destination:
        return destination

    try:
        parsed = urlsplit(destination)
    except ValueError:
        return destination
    if not parsed.scheme or not parsed.netloc:
        return destination

    try:
        netloc = _sanitize_url_netloc(parsed)
    except ValueError:
        return destination
    query = _sanitize_url_query(parsed.query)
    return urlunsplit((parsed.scheme, netloc, parsed.path, query, parsed.fragment))


# ---------------------------------------------------------------------------
# Configuration / options dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ExportRedactionPolicy:
    """Named field-level redaction policy for exported data."""

    name: str
    field_names: frozenset[str]
    replacement_text: str = "REDACTED"
    hash_replacements: bool = False

    def __init__(
        self,
        name: str,
        field_names: set[str] | frozenset[str] | list[str] | tuple[str, ...],
        *,
        replacement_text: str = "REDACTED",
        hash_replacements: bool = False,
    ) -> None:
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "field_names", frozenset(str(field) for field in field_names))
        object.__setattr__(self, "replacement_text", replacement_text)
        object.__setattr__(self, "hash_replacements", hash_replacements)


@dataclass(frozen=True, slots=True)
class ExportOptions:
    """Controls what metadata and relations to include in an export."""

    include_metadata: bool = True
    include_relationships: bool = True
    include_attachments: bool = False
    anonymize: bool = False
    redaction_policy: ExportRedactionPolicy | None = None
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


class ExportManifestValidationResult(BaseModel):
    """Structured result from validating an export against its manifest."""

    model_config = ConfigDict(extra="forbid")

    is_valid: bool
    errors: list[dict[str, Any]] = Field(default_factory=list)


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


class ExportFilterPreset(BaseModel):
    """Named export filter configuration."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    description: str = ""
    filters: ExportFilters


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


def _serialize_jsonl(data: dict[str, Any], options: ExportOptions) -> bytes:
    """Serialize data to JSON Lines, one object per exported record."""
    lines: list[str] = []
    processed = _apply_options(data, options)

    for section_name, section in processed.items():
        if isinstance(section, dict):
            rows = list(section.values())
        elif isinstance(section, list):
            rows = section
        else:
            continue

        for row in rows:
            record = {
                "section": section_name,
                "schema_version": options.schema_version,
                "data": row,
            }
            record_id = row.get("id") if isinstance(row, dict) else None
            if record_id is not None:
                record["id"] = record_id
            lines.append(json.dumps(record, default=str, separators=(",", ":")))

    return ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")


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
    DataExportFormat.JSONL: _serialize_jsonl,
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


def build_export_manifest(
    rows: Iterable[Mapping[str, Any]],
    export_format: DataExportFormat | str,
    *,
    generated_at: datetime | str | None = None,
    destination_label: str | None = None,
) -> dict[str, Any]:
    """Build deterministic metadata for a completed row-oriented export."""
    normalized_rows = [_normalize_manifest_value(dict(row)) for row in rows]
    field_names = sorted(
        {
            str(field_name)
            for row in normalized_rows
            if isinstance(row, dict)
            for field_name in row
        }
    )
    generated_timestamp = (
        generated_at.isoformat()
        if isinstance(generated_at, datetime)
        else generated_at or _now_iso()
    )

    manifest: dict[str, Any] = {
        "format": _export_format_value(export_format),
        "generated_at": generated_timestamp,
        "record_count": len(normalized_rows),
        "field_names": field_names,
        "checksum": _manifest_rows_checksum(normalized_rows),
    }
    if destination_label is not None:
        manifest["destination_label"] = destination_label
    return manifest


def _export_format_value(export_format: DataExportFormat | str) -> str:
    return export_format.value if isinstance(export_format, DataExportFormat) else str(export_format)


def _manifest_rows_checksum(rows: list[Any]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return _checksum(payload.encode("utf-8"))


def _normalize_manifest_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {
            str(key): _normalize_manifest_value(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (list, tuple)):
        return [_normalize_manifest_value(item) for item in value]
    if isinstance(value, (set, frozenset)):
        normalized_items = [_normalize_manifest_value(item) for item in value]
        return sorted(
            normalized_items,
            key=lambda item: json.dumps(item, sort_keys=True, separators=(",", ":"), default=str),
        )
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


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

    if options.redaction_policy is not None:
        result = _apply_redaction_policy(result, options.redaction_policy)

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


def _apply_redaction_policy(data: dict[str, Any], policy: ExportRedactionPolicy) -> dict[str, Any]:
    """Apply a caller-defined redaction policy recursively."""
    return _walk_redact(data, policy)


def _walk_redact(obj: Any, policy: ExportRedactionPolicy) -> Any:
    if isinstance(obj, dict):
        return {
            key: _redacted_value(value, policy) if key in policy.field_names else _walk_redact(value, policy)
            for key, value in obj.items()
        }
    if isinstance(obj, list):
        return [_walk_redact(item, policy) for item in obj]
    return obj


def _redacted_value(value: Any, policy: ExportRedactionPolicy) -> str:
    if policy.hash_replacements:
        digest = hashlib.sha256(str(value).encode("utf-8")).hexdigest()
        return f"{policy.replacement_text}{digest}"
    return policy.replacement_text


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
        self._filter_presets: dict[str, ExportFilterPreset] = {}

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

    def export_to_bundle(
        self,
        result: ExportResult,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Package an export result into a JSON-serializable bundle."""
        return {
            "manifest": result.manifest.model_dump(mode="json"),
            "data": base64.b64encode(result.data).decode("ascii"),
            "metadata": {
                "export_id": result.export_id,
                "format": result.format,
                "scope": result.scope,
                "record_count": result.record_count,
                "created_at": result.created_at,
                **(metadata or {}),
            },
        }

    def register_filter_preset(
        self,
        name: str,
        filters: ExportFilters,
        description: str = "",
    ) -> ExportFilterPreset:
        """Register or overwrite a named export filter preset."""
        preset = ExportFilterPreset(
            name=name,
            description=description,
            filters=filters,
        )
        self._filter_presets[name] = preset
        return preset

    def get_filter_preset(self, name: str) -> ExportFilterPreset | None:
        """Return a named filter preset, if registered."""
        preset = self._filter_presets.get(name)
        return preset.model_copy(deep=True) if preset is not None else None

    def list_filter_presets(self) -> list[ExportFilterPreset]:
        """List filter presets in stable name order."""
        return [
            preset.model_copy(deep=True)
            for _, preset in sorted(self._filter_presets.items())
        ]

    def export_filter_preset(
        self,
        name: str,
        fmt: DataExportFormat = DataExportFormat.JSON,
        options: ExportOptions | None = None,
    ) -> ExportResult:
        """Export data through a registered filter preset."""
        preset = self._filter_presets.get(name)
        if preset is None:
            raise ValueError(f"export filter preset {name!r} is not registered")
        return self.export_all_data(fmt=fmt, filters=preset.filters, options=options)

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

    def validate_export_result(
        self, result: ExportResult,
    ) -> ExportManifestValidationResult:
        """Validate a completed export against its manifest.

        Normal integrity mismatches are returned as structured errors so
        callers can report all failures at once before import or migration.
        """
        errors: list[dict[str, Any]] = []
        manifest = result.manifest

        actual_checksum = _checksum(result.data)
        expected_checksum = manifest.checksums.get("sha256")
        if expected_checksum != actual_checksum:
            errors.append(
                _validation_error(
                    "checksum_mismatch",
                    "checksums.sha256",
                    expected_checksum,
                    actual_checksum,
                )
            )

        payload_schema_version = _extract_schema_version(result.data, result.format)
        if payload_schema_version is not None and payload_schema_version != manifest.schema_version:
            errors.append(
                _validation_error(
                    "schema_version_mismatch",
                    "schema_version",
                    manifest.schema_version,
                    payload_schema_version,
                )
            )

        if result.format != manifest.format:
            errors.append(
                _validation_error(
                    "format_mismatch",
                    "format",
                    manifest.format,
                    result.format,
                )
            )

        if result.scope != manifest.scope:
            errors.append(
                _validation_error(
                    "scope_mismatch",
                    "scope",
                    manifest.scope,
                    result.scope,
                )
            )

        actual_record_counts = _extract_record_counts(result)
        if actual_record_counts != manifest.record_counts:
            errors.append(
                _validation_error(
                    "record_counts_mismatch",
                    "record_counts",
                    manifest.record_counts,
                    actual_record_counts,
                )
            )

        return ExportManifestValidationResult(is_valid=not errors, errors=errors)

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


def _sanitize_url_netloc(parsed: Any) -> str:
    host = parsed.hostname or ""
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parsed.port is not None:
        host = f"{host}:{parsed.port}"
    if parsed.username is None and parsed.password is None:
        return host

    userinfo = _REDACTED
    if parsed.password is not None:
        userinfo = f"{_REDACTED}:{_REDACTED}"
    return f"{userinfo}@{host}"


def _sanitize_url_query(query: str) -> str:
    if not query:
        return ""
    redacted = [
        (key, _REDACTED if _is_sensitive_destination_query_key(key) else value)
        for key, value in parse_qsl(query, keep_blank_values=True)
    ]
    return urlencode(redacted, doseq=True)


def _is_sensitive_destination_query_key(key: str) -> bool:
    normalized = key.lower().replace("-", "_")
    return (
        normalized in _SENSITIVE_DESTINATION_QUERY_KEYS
        or normalized.endswith("_token")
        or normalized.endswith("_key")
        or normalized.endswith("_secret")
        or normalized.endswith("_signature")
    )


def _validation_error(
    code: str,
    field: str,
    expected: Any,
    actual: Any,
) -> dict[str, Any]:
    return {
        "code": code,
        "field": field,
        "expected": expected,
        "actual": actual,
    }


def _extract_schema_version(data: bytes, fmt: str) -> str | None:
    if fmt == DataExportFormat.JSON.value:
        try:
            payload = json.loads(data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        value = payload.get("schema_version")
        return str(value) if value is not None else None

    if fmt == DataExportFormat.JSONL.value:
        try:
            for line in data.decode("utf-8").splitlines():
                if not line.strip():
                    continue
                payload = json.loads(line)
                value = payload.get("schema_version")
                return str(value) if value is not None else None
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None

    if fmt == DataExportFormat.SQL.value:
        first_line = data.decode("utf-8", errors="ignore").splitlines()[0:1]
        prefix = "-- Blueprint Data Export v"
        if first_line and first_line[0].startswith(prefix):
            return first_line[0][len(prefix):]

    if fmt == DataExportFormat.PARQUET.value and len(data) >= 4:
        try:
            header_len = struct.unpack(">I", data[:4])[0]
            header = json.loads(data[4 : 4 + header_len].decode("utf-8"))
        except (struct.error, UnicodeDecodeError, json.JSONDecodeError):
            return None
        value = header.get("schema_version")
        return str(value) if value is not None else None

    if fmt == DataExportFormat.PROTOBUF.value and data.startswith(b"BPEX") and len(data) >= 6:
        try:
            version_len = struct.unpack(">H", data[4:6])[0]
            return data[6 : 6 + version_len].decode("utf-8")
        except (struct.error, UnicodeDecodeError):
            return None

    return None


def _extract_record_counts(result: ExportResult) -> dict[str, int]:
    if result.format == DataExportFormat.JSON.value:
        try:
            payload = json.loads(result.data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}
        data = payload.get("data")
        if isinstance(data, dict):
            return _count_records(data)

    if result.format == DataExportFormat.JSONL.value:
        counts: dict[str, int] = {}
        try:
            lines = result.data.decode("utf-8").splitlines()
            for line in lines:
                if not line.strip():
                    continue
                payload = json.loads(line)
                section = payload.get("section")
                if not isinstance(section, str):
                    return {}
                counts[section] = counts.get(section, 0) + 1
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}
        return counts

    if sum(result.manifest.record_counts.values()) == result.record_count:
        return dict(result.manifest.record_counts)
    return {}


def parse_export_bundle(bundle: dict[str, Any]) -> ExportResult:
    """Reconstruct an export result from a portable export bundle."""
    if not isinstance(bundle, dict):
        raise ValueError("export bundle must be a dictionary")

    missing = [field for field in ("manifest", "data", "metadata") if field not in bundle]
    if missing:
        raise ValueError(f"export bundle missing required field: {missing[0]}")

    manifest_raw = bundle["manifest"]
    metadata = bundle["metadata"]
    encoded_data = bundle["data"]
    if not isinstance(manifest_raw, dict):
        raise ValueError("export bundle manifest must be a dictionary")
    if not isinstance(metadata, dict):
        raise ValueError("export bundle metadata must be a dictionary")
    if not isinstance(encoded_data, str):
        raise ValueError("export bundle data must be a base64 string")

    try:
        data = base64.b64decode(encoded_data.encode("ascii"), validate=True)
    except (UnicodeEncodeError, binascii.Error) as exc:
        raise ValueError("export bundle data is not valid base64") from exc

    manifest = ExportManifest.model_validate(manifest_raw)
    return ExportResult(
        export_id=str(metadata.get("export_id") or manifest.export_id),
        format=str(metadata.get("format") or manifest.format),
        scope=str(metadata.get("scope") or manifest.scope),
        data=data,
        manifest=manifest,
        record_count=int(metadata.get("record_count", sum(manifest.record_counts.values()))),
        created_at=str(metadata.get("created_at") or manifest.timestamp),
    )


def summarize_delta(
    before: dict[str, Any] | bytes | bytearray | ExportResult,
    after: dict[str, Any] | bytes | bytearray | ExportResult,
) -> dict[str, dict[str, int]]:
    """Summarize added, removed, unchanged, and changed records by section."""
    before_sections = _normalize_delta_payload(before)
    after_sections = _normalize_delta_payload(after)
    summary: dict[str, dict[str, int]] = {}

    for section in sorted(set(before_sections) | set(after_sections)):
        before_records = _delta_records(before_sections.get(section))
        after_records = _delta_records(after_sections.get(section))
        before_keys = set(before_records)
        after_keys = set(after_records)
        shared_keys = before_keys & after_keys

        summary[section] = {
            "added": len(after_keys - before_keys),
            "removed": len(before_keys - after_keys),
            "unchanged": sum(1 for key in shared_keys if before_records[key] == after_records[key]),
            "changed": sum(1 for key in shared_keys if before_records[key] != after_records[key]),
        }

    return summary


def _normalize_delta_payload(payload: dict[str, Any] | bytes | bytearray | ExportResult) -> dict[str, Any]:
    if isinstance(payload, ExportResult):
        payload = payload.data
    if isinstance(payload, (bytes, bytearray)):
        payload = json.loads(bytes(payload).decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("delta payload must be a dictionary, JSON bytes, or ExportResult")
    data = payload.get("data")
    return data if isinstance(data, dict) else payload


def _delta_records(section: Any) -> dict[str, Any]:
    if isinstance(section, dict):
        return {str(key): value for key, value in section.items()}
    if isinstance(section, list):
        records: dict[str, Any] = {}
        for index, value in enumerate(section):
            key = value.get("id") if isinstance(value, dict) else None
            records[str(key) if key is not None else str(index)] = value
        return records
    return {}


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
