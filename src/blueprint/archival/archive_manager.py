"""Plan archival system with configurable retention policies.

Supports manual and automatic archiving of completed or inactive plans,
retention policy enforcement, compression, cold storage export, and
full lifecycle tracking from archival through deletion.
"""

from __future__ import annotations

import gzip
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class ArchiveStatus(str, Enum):
    """Lifecycle status of an archived plan."""

    ARCHIVED = "archived"
    COMPRESSED = "compressed"
    EXPORTED = "exported"
    SCHEDULED_FOR_DELETION = "scheduled_for_deletion"
    DELETED = "deleted"


class ArchiveReason(str, Enum):
    """Reason a plan was archived."""

    MANUAL = "manual"
    COMPLETED = "completed"
    INACTIVE = "inactive"
    BULK = "bulk"


class RetentionPeriod(str, Enum):
    """Pre-defined retention periods."""

    INDEFINITE = "indefinite"
    ONE_YEAR = "1_year"
    TWO_YEARS = "2_years"
    SEVEN_YEARS = "7_years"


class ColdStorageBackend(str, Enum):
    """Supported cold storage backends for archive export."""

    S3_GLACIER = "s3_glacier"
    AZURE_ARCHIVE = "azure_archive"
    LOCAL = "local"


class ArchivalTrigger(str, Enum):
    """What triggered the archival."""

    MANUAL = "manual"
    COMPLETION = "completion"
    INACTIVITY = "inactivity"
    BULK_CRITERIA = "bulk_criteria"


class CompletionDelay(str, Enum):
    """Delay after plan completion before auto-archiving."""

    IMMEDIATE = "immediate"
    THIRTY_DAYS = "30_days"
    NINETY_DAYS = "90_days"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ArchivedPlan:
    """Represents a plan that has been archived."""

    archive_id: str
    plan_id: str
    plan_data: bytes
    status: ArchiveStatus
    archived_at: str
    archived_by: str
    reason: ArchiveReason
    trigger: ArchivalTrigger
    original_size: int
    compressed_size: int
    checksum: str
    retention_policy_id: str | None = None
    tags: list[str] = field(default_factory=list)
    plan_title: str = ""
    plan_status: str = ""
    plan_owner: str = ""
    completion_date: str | None = None
    scheduled_deletion_date: str | None = None
    cold_storage_backend: ColdStorageBackend | None = None
    cold_storage_path: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class RetentionPolicy:
    """Configurable retention policy for archived plans."""

    policy_id: str
    name: str
    period: RetentionPeriod
    retention_days: int | None = None
    compress: bool = True
    cold_storage_backend: ColdStorageBackend | None = None
    auto_delete: bool = False
    description: str = ""


@dataclass(frozen=True, slots=True)
class ArchivalResult:
    """Result of applying a retention policy."""

    policy_id: str
    applied_at: str
    plans_affected: int
    plans_compressed: int
    plans_exported: int
    plans_deleted: int
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class ArchiveFilters:
    """Filters for querying archived plans."""

    date_from: datetime | None = None
    date_to: datetime | None = None
    owner: str | None = None
    tags: list[str] | None = None
    completion_date_from: datetime | None = None
    completion_date_to: datetime | None = None
    status: ArchiveStatus | None = None
    reason: ArchiveReason | None = None


@dataclass(frozen=True, slots=True)
class ArchiveReport:
    """Report on archive storage and activity."""

    generated_at: str
    total_archives: int
    total_original_bytes: int
    total_compressed_bytes: int
    compression_ratio: float
    archives_by_status: dict[str, int] = field(default_factory=dict)
    archives_by_reason: dict[str, int] = field(default_factory=dict)
    archives_per_period: dict[str, int] = field(default_factory=dict)
    retention_compliance: dict[str, bool] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ArchiveTriggerConfig:
    """Configuration for automatic archival triggers."""

    auto_archive_completed: bool = False
    completion_delay: CompletionDelay = CompletionDelay.THIRTY_DAYS
    auto_archive_inactive: bool = False
    inactivity_days: int = 90
    default_retention_policy_id: str | None = None


# ---------------------------------------------------------------------------
# In-memory data store for archival
# ---------------------------------------------------------------------------


@dataclass
class ArchiveDataStore:
    """In-memory store providing plan data for archival operations.

    In production this would be backed by a real database.
    """

    plans: dict[str, dict[str, Any]] = field(default_factory=dict)
    tasks: dict[str, dict[str, Any]] = field(default_factory=dict)
    users: dict[str, dict[str, Any]] = field(default_factory=dict)

    def get_plan(self, plan_id: str) -> dict[str, Any] | None:
        """Return a plan by ID, or None."""
        return self.plans.get(plan_id)

    def get_plan_with_tasks(self, plan_id: str) -> dict[str, Any]:
        """Return a plan with its associated tasks."""
        plan = self.plans.get(plan_id, {})
        task_ids = [t["id"] for t in plan.get("tasks", [])]
        tasks = {tid: self.tasks[tid] for tid in task_ids if tid in self.tasks}
        return {"plan": dict(plan), "tasks": tasks}

    def remove_plan(self, plan_id: str) -> dict[str, Any] | None:
        """Remove a plan from the active store and return it."""
        return self.plans.pop(plan_id, None)

    def restore_plan(self, plan_data: dict[str, Any]) -> None:
        """Restore a plan and its tasks back to the active store."""
        plan = plan_data.get("plan", {})
        if plan and plan.get("id"):
            self.plans[plan["id"]] = plan
        for tid, task in plan_data.get("tasks", {}).items():
            self.tasks[tid] = task

    def get_completed_plans(self) -> list[dict[str, Any]]:
        """Return all plans with status 'completed'."""
        return [p for p in self.plans.values() if p.get("status") == "completed"]

    def get_inactive_plans(self, days: int) -> list[dict[str, Any]]:
        """Return plans not updated in the given number of days."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        result = []
        for plan in self.plans.values():
            updated = _parse_dt(plan.get("updated_at"))
            if updated < cutoff:
                result.append(plan)
        return result

    def get_plans_by_criteria(
        self,
        status: list[str] | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        tags: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return plans matching the given criteria."""
        results = list(self.plans.values())
        if status:
            results = [p for p in results if p.get("status") in status]
        if date_from:
            results = [
                p for p in results
                if _parse_dt(p.get("created_at")) >= date_from
            ]
        if date_to:
            results = [
                p for p in results
                if _parse_dt(p.get("created_at")) <= date_to
            ]
        if tags:
            tag_set = set(tags)
            results = [
                p for p in results
                if tag_set.intersection(p.get("tags", []))
            ]
        return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_id(prefix: str = "arc") -> str:
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


def _compress(data: bytes) -> bytes:
    """Compress data using gzip."""
    return gzip.compress(data)


def _decompress(data: bytes) -> bytes:
    """Decompress gzip data."""
    return gzip.decompress(data)


def _retention_days(period: RetentionPeriod) -> int | None:
    """Convert retention period enum to number of days."""
    mapping = {
        RetentionPeriod.INDEFINITE: None,
        RetentionPeriod.ONE_YEAR: 365,
        RetentionPeriod.TWO_YEARS: 730,
        RetentionPeriod.SEVEN_YEARS: 2555,
    }
    return mapping.get(period)


# ---------------------------------------------------------------------------
# ArchiveManager
# ---------------------------------------------------------------------------


class ArchiveManager:
    """Manages plan archival lifecycle with configurable retention policies.

    Provides methods for archiving, restoring, deleting, searching, and
    generating reports on archived plans.  Supports manual and automatic
    archival triggers, compression, and cold storage export.
    """

    def __init__(
        self,
        store: ArchiveDataStore | None = None,
        trigger_config: ArchiveTriggerConfig | None = None,
    ) -> None:
        self._store = store or ArchiveDataStore()
        self._archives: dict[str, ArchivedPlan] = {}
        self._policies: dict[str, RetentionPolicy] = {}
        self._trigger_config = trigger_config or ArchiveTriggerConfig()
        self._cold_storage: dict[str, bytes] = {}  # simulated cold storage

    # -- archival ----------------------------------------------------------

    def archive_plan(
        self,
        plan_id: str,
        reason: ArchiveReason = ArchiveReason.MANUAL,
        archived_by: str = "system",
        trigger: ArchivalTrigger = ArchivalTrigger.MANUAL,
        retention_policy_id: str | None = None,
        compress: bool = True,
    ) -> ArchivedPlan:
        """Archive a plan, removing it from the active store.

        Args:
            plan_id: ID of the plan to archive.
            reason: Why the plan is being archived.
            archived_by: User or system performing the archival.
            trigger: What triggered the archival.
            retention_policy_id: Optional retention policy to apply.
            compress: Whether to compress the archived data.
        """
        plan_data = self._store.get_plan_with_tasks(plan_id)
        plan = plan_data.get("plan", {})
        raw = json.dumps(plan_data, indent=2, default=str).encode("utf-8")
        original_size = len(raw)

        if compress:
            archived_data = _compress(raw)
            compressed_size = len(archived_data)
            status = ArchiveStatus.COMPRESSED
        else:
            archived_data = raw
            compressed_size = original_size
            status = ArchiveStatus.ARCHIVED

        archive_id = _generate_id()
        now = _now_iso()

        # Determine scheduled deletion date from policy
        deletion_date = None
        if retention_policy_id and retention_policy_id in self._policies:
            policy = self._policies[retention_policy_id]
            days = policy.retention_days or _retention_days(policy.period)
            if days is not None:
                deletion_dt = datetime.now(timezone.utc) + timedelta(days=days)
                deletion_date = deletion_dt.isoformat()

        archived = ArchivedPlan(
            archive_id=archive_id,
            plan_id=plan_id,
            plan_data=archived_data,
            status=status,
            archived_at=now,
            archived_by=archived_by,
            reason=reason,
            trigger=trigger,
            original_size=original_size,
            compressed_size=compressed_size,
            checksum=_checksum(raw),
            retention_policy_id=retention_policy_id,
            tags=plan.get("tags", []),
            plan_title=plan.get("title", ""),
            plan_status=plan.get("status", ""),
            plan_owner=plan.get("user_ids", [""])[0] if plan.get("user_ids") else "",
            completion_date=plan.get("completed_at"),
            scheduled_deletion_date=deletion_date,
            metadata={"trigger": trigger.value, "reason": reason.value},
        )

        self._archives[archive_id] = archived
        # Remove from active store
        self._store.remove_plan(plan_id)
        return archived

    # -- restoration -------------------------------------------------------

    def restore_plan(
        self,
        archive_id: str,
        as_new: bool = False,
        tasks_only: bool = False,
    ) -> dict[str, Any]:
        """Restore an archived plan to the active store.

        Args:
            archive_id: ID of the archive to restore.
            as_new: If True, assign a new plan ID.
            tasks_only: If True, restore only tasks without the plan shell.

        Returns:
            The restored plan data dict.
        """
        archived = self._archives.get(archive_id)
        if archived is None:
            raise KeyError(f"Archive {archive_id} not found")

        # Decompress if needed
        if archived.status in (ArchiveStatus.COMPRESSED, ArchiveStatus.EXPORTED):
            raw = _decompress(archived.plan_data)
        else:
            raw = archived.plan_data

        plan_data = json.loads(raw)

        if as_new:
            new_id = _generate_id("plan")
            plan_data["plan"]["id"] = new_id
            plan_data["plan"]["status"] = "draft"

        if tasks_only:
            plan_data.pop("plan", None)

        self._store.restore_plan(plan_data)

        # Remove from archives
        del self._archives[archive_id]
        return plan_data

    # -- deletion ----------------------------------------------------------

    def delete_archived_plan(self, archive_id: str, confirm: bool = True) -> None:
        """Permanently delete an archived plan.

        Args:
            archive_id: ID of the archive to delete.
            confirm: Must be True to proceed (safety check).
        """
        if not confirm:
            raise ValueError("Deletion must be confirmed")
        if archive_id not in self._archives:
            raise KeyError(f"Archive {archive_id} not found")
        del self._archives[archive_id]
        # Also remove from cold storage if present
        self._cold_storage.pop(archive_id, None)

    # -- retention policies ------------------------------------------------

    def add_retention_policy(self, policy: RetentionPolicy) -> RetentionPolicy:
        """Register a retention policy."""
        self._policies[policy.policy_id] = policy
        return policy

    def get_retention_policy(self, policy_id: str) -> RetentionPolicy | None:
        """Return a retention policy by ID."""
        return self._policies.get(policy_id)

    def apply_retention_policy(self, policy_id: str) -> ArchivalResult:
        """Apply a retention policy to all archives associated with it.

        Handles compression, cold storage export, and deletion of expired
        archives according to the policy configuration.
        """
        policy = self._policies.get(policy_id)
        if policy is None:
            return ArchivalResult(
                policy_id=policy_id,
                applied_at=_now_iso(),
                plans_affected=0,
                plans_compressed=0,
                plans_exported=0,
                plans_deleted=0,
                errors=[f"Policy {policy_id} not found"],
            )

        affected = 0
        compressed = 0
        exported = 0
        deleted = 0
        errors: list[str] = []

        # Find archives using this policy
        targets = [
            a for a in self._archives.values()
            if a.retention_policy_id == policy_id
        ]

        now = datetime.now(timezone.utc)
        retention_days = policy.retention_days or _retention_days(policy.period)

        for archive in targets:
            affected += 1

            # Compress if policy requires it and not already compressed
            if policy.compress and archive.status == ArchiveStatus.ARCHIVED:
                try:
                    compressed_data = _compress(archive.plan_data)
                    self._archives[archive.archive_id] = ArchivedPlan(
                        archive_id=archive.archive_id,
                        plan_id=archive.plan_id,
                        plan_data=compressed_data,
                        status=ArchiveStatus.COMPRESSED,
                        archived_at=archive.archived_at,
                        archived_by=archive.archived_by,
                        reason=archive.reason,
                        trigger=archive.trigger,
                        original_size=archive.original_size,
                        compressed_size=len(compressed_data),
                        checksum=archive.checksum,
                        retention_policy_id=archive.retention_policy_id,
                        tags=archive.tags,
                        plan_title=archive.plan_title,
                        plan_status=archive.plan_status,
                        plan_owner=archive.plan_owner,
                        completion_date=archive.completion_date,
                        scheduled_deletion_date=archive.scheduled_deletion_date,
                        cold_storage_backend=archive.cold_storage_backend,
                        cold_storage_path=archive.cold_storage_path,
                        metadata=archive.metadata,
                    )
                    compressed += 1
                    archive = self._archives[archive.archive_id]
                except Exception as exc:
                    errors.append(f"Compression failed for {archive.archive_id}: {exc}")

            # Export to cold storage if configured
            if policy.cold_storage_backend and archive.status != ArchiveStatus.EXPORTED:
                try:
                    path = f"{policy.cold_storage_backend.value}/{archive.archive_id}"
                    self._cold_storage[archive.archive_id] = archive.plan_data
                    self._archives[archive.archive_id] = ArchivedPlan(
                        archive_id=archive.archive_id,
                        plan_id=archive.plan_id,
                        plan_data=archive.plan_data,
                        status=ArchiveStatus.EXPORTED,
                        archived_at=archive.archived_at,
                        archived_by=archive.archived_by,
                        reason=archive.reason,
                        trigger=archive.trigger,
                        original_size=archive.original_size,
                        compressed_size=archive.compressed_size,
                        checksum=archive.checksum,
                        retention_policy_id=archive.retention_policy_id,
                        tags=archive.tags,
                        plan_title=archive.plan_title,
                        plan_status=archive.plan_status,
                        plan_owner=archive.plan_owner,
                        completion_date=archive.completion_date,
                        scheduled_deletion_date=archive.scheduled_deletion_date,
                        cold_storage_backend=policy.cold_storage_backend,
                        cold_storage_path=path,
                        metadata=archive.metadata,
                    )
                    exported += 1
                    archive = self._archives[archive.archive_id]
                except Exception as exc:
                    errors.append(f"Export failed for {archive.archive_id}: {exc}")

            # Delete if past retention period and auto_delete is enabled
            if policy.auto_delete and retention_days is not None:
                archive_dt = _parse_dt(archive.archived_at)
                age = (now - archive_dt).days
                if age >= retention_days:
                    del self._archives[archive.archive_id]
                    self._cold_storage.pop(archive.archive_id, None)
                    deleted += 1

        return ArchivalResult(
            policy_id=policy_id,
            applied_at=_now_iso(),
            plans_affected=affected,
            plans_compressed=compressed,
            plans_exported=exported,
            plans_deleted=deleted,
            errors=errors,
        )

    # -- querying ----------------------------------------------------------

    def get_archived_plans(
        self,
        filters: ArchiveFilters | None = None,
    ) -> list[ArchivedPlan]:
        """Return archived plans, optionally filtered."""
        results = list(self._archives.values())
        if filters is None:
            return sorted(results, key=lambda a: a.archived_at, reverse=True)

        if filters.status:
            results = [a for a in results if a.status == filters.status]
        if filters.reason:
            results = [a for a in results if a.reason == filters.reason]
        if filters.owner:
            results = [a for a in results if a.plan_owner == filters.owner]
        if filters.tags:
            tag_set = set(filters.tags)
            results = [
                a for a in results
                if tag_set.intersection(a.tags)
            ]
        if filters.date_from:
            results = [
                a for a in results
                if _parse_dt(a.archived_at) >= filters.date_from
            ]
        if filters.date_to:
            results = [
                a for a in results
                if _parse_dt(a.archived_at) <= filters.date_to
            ]
        if filters.completion_date_from:
            results = [
                a for a in results
                if a.completion_date and _parse_dt(a.completion_date) >= filters.completion_date_from
            ]
        if filters.completion_date_to:
            results = [
                a for a in results
                if a.completion_date and _parse_dt(a.completion_date) <= filters.completion_date_to
            ]

        return sorted(results, key=lambda a: a.archived_at, reverse=True)

    def get_archive(self, archive_id: str) -> ArchivedPlan | None:
        """Return a single archive by ID."""
        return self._archives.get(archive_id)

    # -- preview -----------------------------------------------------------

    def preview_archive(self, archive_id: str) -> dict[str, Any]:
        """Preview archived plan data without full restoration.

        Returns metadata and a summary of the plan without modifying
        the archive or the active store.
        """
        archived = self._archives.get(archive_id)
        if archived is None:
            raise KeyError(f"Archive {archive_id} not found")

        # Decompress for preview
        if archived.status in (ArchiveStatus.COMPRESSED, ArchiveStatus.EXPORTED):
            raw = _decompress(archived.plan_data)
        else:
            raw = archived.plan_data

        plan_data = json.loads(raw)
        plan = plan_data.get("plan", {})
        tasks = plan_data.get("tasks", {})

        return {
            "archive_id": archived.archive_id,
            "plan_id": archived.plan_id,
            "plan_title": plan.get("title", ""),
            "plan_status": plan.get("status", ""),
            "task_count": len(tasks),
            "archived_at": archived.archived_at,
            "archived_by": archived.archived_by,
            "reason": archived.reason.value,
            "original_size": archived.original_size,
            "compressed_size": archived.compressed_size,
            "tags": archived.tags,
        }

    # -- export to cold storage --------------------------------------------

    def export_archive(
        self,
        archive_id: str,
        backend: ColdStorageBackend = ColdStorageBackend.S3_GLACIER,
    ) -> bytes:
        """Export an archive to cold storage and return the serialized data.

        Args:
            archive_id: ID of the archive to export.
            backend: Target cold storage backend.

        Returns:
            The raw archive bytes (compressed or uncompressed).
        """
        archived = self._archives.get(archive_id)
        if archived is None:
            raise KeyError(f"Archive {archive_id} not found")

        data = archived.plan_data
        path = f"{backend.value}/{archive_id}"

        # Store in simulated cold storage
        self._cold_storage[archive_id] = data

        # Update archive status
        self._archives[archive_id] = ArchivedPlan(
            archive_id=archived.archive_id,
            plan_id=archived.plan_id,
            plan_data=archived.plan_data,
            status=ArchiveStatus.EXPORTED,
            archived_at=archived.archived_at,
            archived_by=archived.archived_by,
            reason=archived.reason,
            trigger=archived.trigger,
            original_size=archived.original_size,
            compressed_size=archived.compressed_size,
            checksum=archived.checksum,
            retention_policy_id=archived.retention_policy_id,
            tags=archived.tags,
            plan_title=archived.plan_title,
            plan_status=archived.plan_status,
            plan_owner=archived.plan_owner,
            completion_date=archived.completion_date,
            scheduled_deletion_date=archived.scheduled_deletion_date,
            cold_storage_backend=backend,
            cold_storage_path=path,
            metadata=archived.metadata,
        )

        return data

    # -- automatic triggers ------------------------------------------------

    def auto_archive_completed(
        self,
        archived_by: str = "system",
    ) -> list[ArchivedPlan]:
        """Automatically archive completed plans respecting delay config."""
        if not self._trigger_config.auto_archive_completed:
            return []

        delay_map = {
            CompletionDelay.IMMEDIATE: 0,
            CompletionDelay.THIRTY_DAYS: 30,
            CompletionDelay.NINETY_DAYS: 90,
        }
        delay_days = delay_map[self._trigger_config.completion_delay]
        cutoff = datetime.now(timezone.utc) - timedelta(days=delay_days)

        completed = self._store.get_completed_plans()
        archived: list[ArchivedPlan] = []

        for plan in completed:
            completed_at = _parse_dt(plan.get("completed_at") or plan.get("updated_at"))
            if completed_at <= cutoff:
                result = self.archive_plan(
                    plan["id"],
                    reason=ArchiveReason.COMPLETED,
                    archived_by=archived_by,
                    trigger=ArchivalTrigger.COMPLETION,
                    retention_policy_id=self._trigger_config.default_retention_policy_id,
                )
                archived.append(result)

        return archived

    def auto_archive_inactive(
        self,
        archived_by: str = "system",
    ) -> list[ArchivedPlan]:
        """Automatically archive plans inactive for configured duration."""
        if not self._trigger_config.auto_archive_inactive:
            return []

        inactive = self._store.get_inactive_plans(self._trigger_config.inactivity_days)
        archived: list[ArchivedPlan] = []

        for plan in inactive:
            result = self.archive_plan(
                plan["id"],
                reason=ArchiveReason.INACTIVE,
                archived_by=archived_by,
                trigger=ArchivalTrigger.INACTIVITY,
                retention_policy_id=self._trigger_config.default_retention_policy_id,
            )
            archived.append(result)

        return archived

    def bulk_archive(
        self,
        status: list[str] | None = None,
        date_from: datetime | None = None,
        date_to: datetime | None = None,
        tags: list[str] | None = None,
        archived_by: str = "system",
        retention_policy_id: str | None = None,
    ) -> list[ArchivedPlan]:
        """Archive multiple plans matching criteria."""
        plans = self._store.get_plans_by_criteria(
            status=status,
            date_from=date_from,
            date_to=date_to,
            tags=tags,
        )
        archived: list[ArchivedPlan] = []
        for plan in plans:
            result = self.archive_plan(
                plan["id"],
                reason=ArchiveReason.BULK,
                archived_by=archived_by,
                trigger=ArchivalTrigger.BULK_CRITERIA,
                retention_policy_id=retention_policy_id,
            )
            archived.append(result)
        return archived

    # -- reports -----------------------------------------------------------

    def generate_report(self) -> ArchiveReport:
        """Generate a report on archive storage and activity."""
        archives = list(self._archives.values())
        total_original = sum(a.original_size for a in archives)
        total_compressed = sum(a.compressed_size for a in archives)

        by_status: dict[str, int] = {}
        by_reason: dict[str, int] = {}
        per_period: dict[str, int] = {}

        for a in archives:
            by_status[a.status.value] = by_status.get(a.status.value, 0) + 1
            by_reason[a.reason.value] = by_reason.get(a.reason.value, 0) + 1
            month = _parse_dt(a.archived_at).strftime("%Y-%m")
            per_period[month] = per_period.get(month, 0) + 1

        # Check retention compliance
        compliance: dict[str, bool] = {}
        for pid, policy in self._policies.items():
            retention_days = policy.retention_days or _retention_days(policy.period)
            if retention_days is None:
                compliance[pid] = True
                continue
            now = datetime.now(timezone.utc)
            policy_archives = [
                a for a in archives if a.retention_policy_id == pid
            ]
            compliant = all(
                (now - _parse_dt(a.archived_at)).days <= retention_days
                or a.status == ArchiveStatus.DELETED
                for a in policy_archives
            )
            compliance[pid] = compliant

        return ArchiveReport(
            generated_at=_now_iso(),
            total_archives=len(archives),
            total_original_bytes=total_original,
            total_compressed_bytes=total_compressed,
            compression_ratio=total_compressed / total_original if total_original > 0 else 0.0,
            archives_by_status=by_status,
            archives_by_reason=by_reason,
            archives_per_period=per_period,
            retention_compliance=compliance,
        )
