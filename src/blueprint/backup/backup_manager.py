"""Automated backup system with point-in-time recovery capabilities.

Supports full, incremental, plan-specific, and workspace scoped backups.
Backup types include logical (JSON export), physical (snapshot), and
continuous (transaction log) with scheduling, verification, and retention
policy enforcement.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class BackupScope(str, Enum):
    """Scope of a backup operation."""

    FULL = "full"
    INCREMENTAL = "incremental"
    PLAN = "plan"
    WORKSPACE = "workspace"


class BackupType(str, Enum):
    """Type of backup mechanism."""

    LOGICAL = "logical"
    PHYSICAL = "physical"
    CONTINUOUS = "continuous"


class BackupStatus(str, Enum):
    """Status of a backup."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    VERIFIED = "verified"
    EXPIRED = "expired"


class RestoreStatus(str, Enum):
    """Status of a restore operation."""

    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


class StorageBackend(str, Enum):
    """Where backups are stored."""

    LOCAL = "local"
    S3 = "s3"
    AZURE_BLOB = "azure_blob"
    GCS = "gcs"
    SFTP = "sftp"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Backup:
    """Represents a completed or in-progress backup."""

    backup_id: str
    scope: BackupScope
    backup_type: BackupType
    status: BackupStatus
    created_at: str
    data: bytes
    checksum: str
    record_count: int
    metadata: dict[str, Any] = field(default_factory=dict)
    plan_id: str | None = None
    workspace_id: str | None = None
    parent_backup_id: str | None = None
    size_bytes: int = 0


@dataclass(frozen=True, slots=True)
class BackupSchedule:
    """Configuration for a scheduled backup."""

    schedule_id: str
    schedule: str
    scope: BackupScope
    backup_type: BackupType
    storage_backend: StorageBackend
    storage_path: str
    created_at: str
    next_run: str | None = None
    enabled: bool = True
    retention_days: int = 30


@dataclass(frozen=True, slots=True)
class RestoreResult:
    """Result of a restore operation."""

    restore_id: str
    backup_id: str
    status: RestoreStatus
    restored_at: str
    records_restored: int
    message: str
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class VerificationResult:
    """Result of backup integrity verification."""

    backup_id: str
    verified: bool
    verified_at: str
    checksum_valid: bool
    data_readable: bool
    record_count_match: bool
    message: str


@dataclass(frozen=True, slots=True)
class CleanupResult:
    """Result of backup retention cleanup."""

    cleaned_at: str
    backups_removed: int
    bytes_freed: int
    remaining_backups: int
    removed_ids: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """Defines how long backups are retained."""

    max_age_days: int = 30
    max_count: int = 100
    keep_daily: int = 7
    keep_weekly: int = 4
    keep_monthly: int = 12


# ---------------------------------------------------------------------------
# In-memory data store for backups
# ---------------------------------------------------------------------------


@dataclass
class BackupDataStore:
    """Simple in-memory store that BackupManager operates against.

    In production this would be backed by SQLAlchemy / a real database.
    """

    workspaces: dict[str, dict[str, Any]] = field(default_factory=dict)
    plans: dict[str, dict[str, Any]] = field(default_factory=dict)
    tasks: dict[str, dict[str, Any]] = field(default_factory=dict)
    users: dict[str, dict[str, Any]] = field(default_factory=dict)
    settings: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    transaction_log: list[dict[str, Any]] = field(default_factory=list)

    def snapshot(self) -> dict[str, Any]:
        """Full snapshot of all data."""
        return {
            "workspaces": dict(self.workspaces),
            "plans": dict(self.plans),
            "tasks": dict(self.tasks),
            "users": dict(self.users),
            "settings": dict(self.settings),
            "events": list(self.events),
        }

    def workspace_snapshot(self, workspace_id: str) -> dict[str, Any]:
        """Snapshot scoped to a single workspace."""
        ws = self.workspaces.get(workspace_id, {})
        plan_ids = ws.get("plan_ids", [])
        plans = {pid: self.plans[pid] for pid in plan_ids if pid in self.plans}
        task_ids: list[str] = []
        for plan in plans.values():
            task_ids.extend(t["id"] for t in plan.get("tasks", []))
        tasks = {tid: self.tasks[tid] for tid in task_ids if tid in self.tasks}
        return {"workspace": ws, "plans": plans, "tasks": tasks}

    def plan_snapshot(self, plan_id: str) -> dict[str, Any]:
        """Snapshot of a single plan with its tasks."""
        plan = self.plans.get(plan_id, {})
        task_ids = [t["id"] for t in plan.get("tasks", [])]
        tasks = {tid: self.tasks[tid] for tid in task_ids if tid in self.tasks}
        return {"plan": plan, "tasks": tasks}

    def changes_since(self, since: datetime) -> dict[str, Any]:
        """Return data modified after *since* for incremental backups."""
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

    def restore_from(self, data: dict[str, Any]) -> int:
        """Restore data from a backup snapshot, returning record count."""
        count = 0
        if "workspaces" in data:
            self.workspaces.update(data["workspaces"])
            count += len(data["workspaces"])
        if "workspace" in data and data["workspace"]:
            ws = data["workspace"]
            self.workspaces[ws.get("id", "unknown")] = ws
            count += 1
        if "plans" in data:
            self.plans.update(data["plans"])
            count += len(data["plans"])
        if "plan" in data and data["plan"]:
            plan = data["plan"]
            self.plans[plan.get("id", "unknown")] = plan
            count += 1
        if "tasks" in data:
            self.tasks.update(data["tasks"])
            count += len(data["tasks"])
        if "users" in data:
            self.users.update(data["users"])
            count += len(data["users"])
        if "settings" in data:
            self.settings.update(data["settings"])
            count += len(data["settings"])
        if "events" in data:
            self.events.extend(data["events"])
            count += len(data["events"])
        return count

    def get_transaction_log_since(self, since: datetime) -> list[dict[str, Any]]:
        """Return transaction log entries since a timestamp."""
        return [
            entry for entry in self.transaction_log
            if _parse_dt(entry.get("timestamp")) >= since
        ]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_id(prefix: str = "bak") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _checksum(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except (ValueError, TypeError):
            pass
    return datetime.min.replace(tzinfo=timezone.utc)


def _count_records(data: dict[str, Any]) -> int:
    total = 0
    for v in data.values():
        if isinstance(v, dict):
            total += len(v)
        elif isinstance(v, list):
            total += len(v)
    return total


# ---------------------------------------------------------------------------
# BackupManager
# ---------------------------------------------------------------------------


class BackupManager:
    """Automated backup system with scheduling, verification, and recovery.

    Provides methods for creating, restoring, verifying, and managing
    backups with support for multiple scopes, types, and storage backends.
    """

    def __init__(self, store: BackupDataStore | None = None) -> None:
        self._store = store or BackupDataStore()
        self._backups: dict[str, Backup] = {}
        self._schedules: dict[str, BackupSchedule] = {}
        self._last_backup_at: datetime | None = None

    # -- backup creation ---------------------------------------------------

    def create_backup(
        self,
        scope: BackupScope = BackupScope.FULL,
        backup_type: BackupType = BackupType.LOGICAL,
        plan_id: str | None = None,
        workspace_id: str | None = None,
    ) -> Backup:
        """Create a backup with the given scope and type.

        Args:
            scope: What to back up (full, incremental, plan, workspace).
            backup_type: How to back up (logical, physical, continuous).
            plan_id: Required when ``scope`` is ``PLAN``.
            workspace_id: Required when ``scope`` is ``WORKSPACE``.
        """
        raw = self._collect_data(scope, plan_id, workspace_id)
        data = self._serialize(raw, backup_type)
        bid = _generate_id()
        cs = _checksum(data)
        now = _now_iso()

        backup = Backup(
            backup_id=bid,
            scope=scope,
            backup_type=backup_type,
            status=BackupStatus.COMPLETED,
            created_at=now,
            data=data,
            checksum=cs,
            record_count=_count_records(raw),
            metadata={
                "scope": scope.value,
                "backup_type": backup_type.value,
                "record_count": _count_records(raw),
            },
            plan_id=plan_id,
            workspace_id=workspace_id,
            parent_backup_id=self._latest_backup_id(),
            size_bytes=len(data),
        )

        self._backups[bid] = backup
        self._last_backup_at = datetime.now(timezone.utc)
        return backup

    # -- scheduling --------------------------------------------------------

    def schedule_backup(
        self,
        schedule: str,
        scope: BackupScope = BackupScope.FULL,
        backup_type: BackupType = BackupType.LOGICAL,
        storage_backend: StorageBackend = StorageBackend.LOCAL,
        storage_path: str = "/tmp/backups",
        retention_days: int = 30,
    ) -> BackupSchedule:
        """Register a recurring backup schedule."""
        sid = _generate_id("sch")
        now = _now_iso()
        sched = BackupSchedule(
            schedule_id=sid,
            schedule=schedule,
            scope=scope,
            backup_type=backup_type,
            storage_backend=storage_backend,
            storage_path=storage_path,
            created_at=now,
            next_run=now,
            retention_days=retention_days,
        )
        self._schedules[sid] = sched
        return sched

    # -- restore -----------------------------------------------------------

    def restore_backup(
        self,
        backup_id: str,
        dry_run: bool = False,
        target_time: datetime | None = None,
    ) -> RestoreResult:
        """Restore data from a previously created backup.

        Args:
            backup_id: ID of the backup to restore.
            dry_run: If ``True``, validate without modifying data.
            target_time: For continuous backups, restore to this point.
        """
        backup = self._backups.get(backup_id)
        if backup is None:
            return RestoreResult(
                restore_id=_generate_id("rst"),
                backup_id=backup_id,
                status=RestoreStatus.FAILED,
                restored_at=_now_iso(),
                records_restored=0,
                message=f"Backup {backup_id} not found",
            )

        try:
            data = self._deserialize(backup.data, backup.backup_type)
        except Exception as exc:
            return RestoreResult(
                restore_id=_generate_id("rst"),
                backup_id=backup_id,
                status=RestoreStatus.FAILED,
                restored_at=_now_iso(),
                records_restored=0,
                message=f"Failed to deserialize backup: {exc}",
            )

        if target_time and backup.backup_type == BackupType.CONTINUOUS:
            data = self._apply_transaction_log(data, backup.created_at, target_time)

        if dry_run:
            return RestoreResult(
                restore_id=_generate_id("rst"),
                backup_id=backup_id,
                status=RestoreStatus.SUCCESS,
                restored_at=_now_iso(),
                records_restored=_count_records(data),
                message="Dry run — no changes applied",
            )

        count = self._store.restore_from(data)
        return RestoreResult(
            restore_id=_generate_id("rst"),
            backup_id=backup_id,
            status=RestoreStatus.SUCCESS,
            restored_at=_now_iso(),
            records_restored=count,
            message="Restore completed successfully",
        )

    # -- verification ------------------------------------------------------

    def verify_backup(self, backup_id: str) -> VerificationResult:
        """Verify backup integrity (checksum, readability, counts)."""
        backup = self._backups.get(backup_id)
        if backup is None:
            return VerificationResult(
                backup_id=backup_id,
                verified=False,
                verified_at=_now_iso(),
                checksum_valid=False,
                data_readable=False,
                record_count_match=False,
                message=f"Backup {backup_id} not found",
            )

        checksum_valid = _checksum(backup.data) == backup.checksum

        data_readable = True
        record_count_match = False
        try:
            data = self._deserialize(backup.data, backup.backup_type)
            actual_count = _count_records(data)
            record_count_match = actual_count == backup.record_count
        except Exception:
            data_readable = False

        verified = checksum_valid and data_readable and record_count_match

        if verified:
            # Promote status to VERIFIED
            self._backups[backup_id] = Backup(
                backup_id=backup.backup_id,
                scope=backup.scope,
                backup_type=backup.backup_type,
                status=BackupStatus.VERIFIED,
                created_at=backup.created_at,
                data=backup.data,
                checksum=backup.checksum,
                record_count=backup.record_count,
                metadata=backup.metadata,
                plan_id=backup.plan_id,
                workspace_id=backup.workspace_id,
                parent_backup_id=backup.parent_backup_id,
                size_bytes=backup.size_bytes,
            )

        return VerificationResult(
            backup_id=backup_id,
            verified=verified,
            verified_at=_now_iso(),
            checksum_valid=checksum_valid,
            data_readable=data_readable,
            record_count_match=record_count_match,
            message="Backup verified successfully" if verified else "Verification failed",
        )

    # -- listing -----------------------------------------------------------

    def list_backups(
        self,
        scope: BackupScope | None = None,
        status: BackupStatus | None = None,
        limit: int = 50,
    ) -> list[Backup]:
        """List backups, optionally filtered by scope or status."""
        results = list(self._backups.values())
        if scope:
            results = [b for b in results if b.scope == scope]
        if status:
            results = [b for b in results if b.status == status]
        # Sort by creation time descending
        results.sort(key=lambda b: b.created_at, reverse=True)
        return results[:limit]

    # -- cleanup -----------------------------------------------------------

    def cleanup_old_backups(
        self,
        retention: RetentionPolicy | None = None,
    ) -> CleanupResult:
        """Remove backups that exceed the retention policy."""
        policy = retention or RetentionPolicy()
        now = datetime.now(timezone.utc)
        to_remove: list[str] = []
        bytes_freed = 0

        # Sort oldest first
        sorted_backups = sorted(self._backups.values(), key=lambda b: b.created_at)

        for backup in sorted_backups:
            age = now - _parse_dt(backup.created_at)
            if age.days > policy.max_age_days:
                to_remove.append(backup.backup_id)
                bytes_freed += backup.size_bytes

        # Also enforce max count
        remaining = [b for b in sorted_backups if b.backup_id not in to_remove]
        while len(remaining) > policy.max_count:
            oldest = remaining.pop(0)
            to_remove.append(oldest.backup_id)
            bytes_freed += oldest.size_bytes

        for bid in to_remove:
            del self._backups[bid]

        return CleanupResult(
            cleaned_at=_now_iso(),
            backups_removed=len(to_remove),
            bytes_freed=bytes_freed,
            remaining_backups=len(self._backups),
            removed_ids=to_remove,
        )

    # -- schedule management -----------------------------------------------

    def get_schedule(self, schedule_id: str) -> BackupSchedule | None:
        return self._schedules.get(schedule_id)

    def list_schedules(self) -> list[BackupSchedule]:
        return list(self._schedules.values())

    def disable_schedule(self, schedule_id: str) -> bool:
        sched = self._schedules.get(schedule_id)
        if sched is None:
            return False
        self._schedules[schedule_id] = BackupSchedule(
            schedule_id=sched.schedule_id,
            schedule=sched.schedule,
            scope=sched.scope,
            backup_type=sched.backup_type,
            storage_backend=sched.storage_backend,
            storage_path=sched.storage_path,
            created_at=sched.created_at,
            next_run=sched.next_run,
            enabled=False,
            retention_days=sched.retention_days,
        )
        return True

    # -- private helpers ---------------------------------------------------

    def _collect_data(
        self,
        scope: BackupScope,
        plan_id: str | None,
        workspace_id: str | None,
    ) -> dict[str, Any]:
        if scope == BackupScope.FULL:
            return self._store.snapshot()
        if scope == BackupScope.WORKSPACE:
            return self._store.workspace_snapshot(workspace_id or "")
        if scope == BackupScope.PLAN:
            return self._store.plan_snapshot(plan_id or "")
        # BackupScope.INCREMENTAL
        since = self._last_backup_at or datetime.min.replace(tzinfo=timezone.utc)
        return self._store.changes_since(since)

    def _serialize(self, data: dict[str, Any], backup_type: BackupType) -> bytes:
        if backup_type == BackupType.PHYSICAL:
            # Physical backup: compact binary JSON
            return json.dumps(data, separators=(",", ":"), default=str).encode("utf-8")
        if backup_type == BackupType.CONTINUOUS:
            # Continuous: include transaction log marker
            payload = {"data": data, "type": "continuous", "timestamp": _now_iso()}
            return json.dumps(payload, default=str).encode("utf-8")
        # Logical: pretty-printed JSON
        return json.dumps(data, indent=2, default=str).encode("utf-8")

    def _deserialize(self, data: bytes, backup_type: BackupType) -> dict[str, Any]:
        parsed = json.loads(data)
        if backup_type == BackupType.CONTINUOUS:
            return parsed.get("data", parsed)
        return parsed

    def _apply_transaction_log(
        self,
        data: dict[str, Any],
        backup_time_str: str,
        target_time: datetime,
    ) -> dict[str, Any]:
        """Apply transaction log entries up to target_time for PITR."""
        backup_time = _parse_dt(backup_time_str)
        entries = self._store.get_transaction_log_since(backup_time)
        for entry in entries:
            entry_time = _parse_dt(entry.get("timestamp"))
            if entry_time > target_time:
                break
            # Apply the entry's operation to the data
            op = entry.get("operation")
            if op == "upsert_plan":
                data.setdefault("plans", {})[entry["plan_id"]] = entry["payload"]
            elif op == "upsert_task":
                data.setdefault("tasks", {})[entry["task_id"]] = entry["payload"]
            elif op == "delete_plan":
                data.get("plans", {}).pop(entry["plan_id"], None)
        return data

    def _latest_backup_id(self) -> str | None:
        if not self._backups:
            return None
        latest = max(self._backups.values(), key=lambda b: b.created_at)
        return latest.backup_id
