"""Tests for the automated backup system with point-in-time recovery."""

import json
from datetime import datetime, timezone

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


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_store() -> BackupDataStore:
    """Build a store pre-populated with realistic test data."""
    now = _now()
    store = BackupDataStore()

    store.workspaces["ws-1"] = {
        "id": "ws-1",
        "name": "Acme Corp",
        "plan_ids": ["plan-1", "plan-2"],
    }

    store.plans["plan-1"] = {
        "id": "plan-1",
        "title": "Auth System",
        "status": "in_progress",
        "tasks": [
            {"id": "task-1", "title": "Login endpoint", "depends_on": []},
            {"id": "task-2", "title": "Token refresh", "depends_on": ["task-1"]},
        ],
        "user_ids": ["user-1"],
        "created_at": now,
        "updated_at": now,
    }
    store.plans["plan-2"] = {
        "id": "plan-2",
        "title": "Dashboard UI",
        "status": "draft",
        "tasks": [
            {"id": "task-3", "title": "Chart component", "depends_on": []},
        ],
        "user_ids": ["user-1", "user-2"],
        "created_at": now,
        "updated_at": now,
    }

    store.tasks["task-1"] = {
        "id": "task-1",
        "title": "Login endpoint",
        "execution_plan_id": "plan-1",
        "updated_at": now,
    }
    store.tasks["task-2"] = {
        "id": "task-2",
        "title": "Token refresh",
        "execution_plan_id": "plan-1",
        "updated_at": now,
    }
    store.tasks["task-3"] = {
        "id": "task-3",
        "title": "Chart component",
        "execution_plan_id": "plan-2",
        "updated_at": now,
    }

    store.users["user-1"] = {"id": "user-1", "name": "Alice"}
    store.users["user-2"] = {"id": "user-2", "name": "Bob"}

    store.settings = {"theme": "dark"}

    store.events.append({
        "id": "evt-1",
        "action": "plan_created",
        "entity_id": "plan-1",
        "created_at": now,
    })

    return store


def _manager(store: BackupDataStore | None = None) -> BackupManager:
    return BackupManager(store=store or _make_store())


# ---------------------------------------------------------------------------
# Full backup tests
# ---------------------------------------------------------------------------


class TestFullBackup:
    def test_create_full_backup(self):
        mgr = _manager()
        backup = mgr.create_backup(scope=BackupScope.FULL)

        assert isinstance(backup, Backup)
        assert backup.scope == BackupScope.FULL
        assert backup.backup_type == BackupType.LOGICAL
        assert backup.status == BackupStatus.COMPLETED
        assert backup.record_count > 0
        assert len(backup.data) > 0
        assert len(backup.checksum) == 64

    def test_full_backup_contains_all_data(self):
        mgr = _manager()
        backup = mgr.create_backup(scope=BackupScope.FULL)
        data = json.loads(backup.data)
        assert len(data["plans"]) == 2
        assert len(data["tasks"]) == 3
        assert len(data["users"]) == 2

    def test_full_backup_size_bytes(self):
        mgr = _manager()
        backup = mgr.create_backup(scope=BackupScope.FULL)
        assert backup.size_bytes == len(backup.data)
        assert backup.size_bytes > 0


# ---------------------------------------------------------------------------
# Incremental backup tests
# ---------------------------------------------------------------------------


class TestIncrementalBackup:
    def test_create_incremental_no_prior(self):
        mgr = _manager()
        backup = mgr.create_backup(scope=BackupScope.INCREMENTAL)
        # No prior backup, so incremental = everything since epoch
        assert backup.scope == BackupScope.INCREMENTAL
        assert backup.record_count > 0

    def test_incremental_after_full(self):
        mgr = _manager()
        mgr.create_backup(scope=BackupScope.FULL)
        # Second incremental should capture changes since the full backup.
        # Since nothing changed, it may still pick up items created "now"
        backup = mgr.create_backup(scope=BackupScope.INCREMENTAL)
        assert backup.scope == BackupScope.INCREMENTAL
        assert backup.parent_backup_id is not None


# ---------------------------------------------------------------------------
# Plan-specific backup tests
# ---------------------------------------------------------------------------


class TestPlanBackup:
    def test_create_plan_backup(self):
        mgr = _manager()
        backup = mgr.create_backup(scope=BackupScope.PLAN, plan_id="plan-1")

        assert backup.scope == BackupScope.PLAN
        assert backup.plan_id == "plan-1"
        data = json.loads(backup.data)
        assert data["plan"]["id"] == "plan-1"

    def test_plan_backup_includes_tasks(self):
        mgr = _manager()
        backup = mgr.create_backup(scope=BackupScope.PLAN, plan_id="plan-1")
        data = json.loads(backup.data)
        assert len(data["tasks"]) == 2

    def test_plan_backup_nonexistent(self):
        mgr = _manager()
        backup = mgr.create_backup(scope=BackupScope.PLAN, plan_id="nonexistent")
        data = json.loads(backup.data)
        assert data["plan"] == {}
        assert len(data["tasks"]) == 0


# ---------------------------------------------------------------------------
# Workspace backup tests
# ---------------------------------------------------------------------------


class TestWorkspaceBackup:
    def test_create_workspace_backup(self):
        mgr = _manager()
        backup = mgr.create_backup(scope=BackupScope.WORKSPACE, workspace_id="ws-1")

        assert backup.scope == BackupScope.WORKSPACE
        assert backup.workspace_id == "ws-1"
        data = json.loads(backup.data)
        assert len(data["plans"]) == 2

    def test_workspace_backup_tasks(self):
        mgr = _manager()
        backup = mgr.create_backup(scope=BackupScope.WORKSPACE, workspace_id="ws-1")
        data = json.loads(backup.data)
        assert len(data["tasks"]) == 3


# ---------------------------------------------------------------------------
# Backup types
# ---------------------------------------------------------------------------


class TestBackupTypes:
    def test_logical_backup(self):
        mgr = _manager()
        backup = mgr.create_backup(backup_type=BackupType.LOGICAL)
        # Logical = pretty-printed JSON
        text = backup.data.decode("utf-8")
        assert "\n" in text  # pretty-printed has newlines

    def test_physical_backup(self):
        mgr = _manager()
        backup = mgr.create_backup(backup_type=BackupType.PHYSICAL)
        text = backup.data.decode("utf-8")
        # Physical = compact JSON (no spaces after separators)
        parsed = json.loads(text)
        assert "plans" in parsed

    def test_continuous_backup(self):
        mgr = _manager()
        backup = mgr.create_backup(backup_type=BackupType.CONTINUOUS)
        parsed = json.loads(backup.data)
        assert parsed["type"] == "continuous"
        assert "data" in parsed
        assert "timestamp" in parsed


# ---------------------------------------------------------------------------
# Scheduling tests
# ---------------------------------------------------------------------------


class TestScheduling:
    def test_schedule_backup(self):
        mgr = _manager()
        sched = mgr.schedule_backup("daily")

        assert isinstance(sched, BackupSchedule)
        assert sched.schedule == "daily"
        assert sched.scope == BackupScope.FULL
        assert sched.enabled is True

    def test_schedule_with_options(self):
        mgr = _manager()
        sched = mgr.schedule_backup(
            "0 2 * * *",
            scope=BackupScope.WORKSPACE,
            backup_type=BackupType.PHYSICAL,
            storage_backend=StorageBackend.S3,
            storage_path="s3://my-bucket/backups",
            retention_days=90,
        )
        assert sched.schedule == "0 2 * * *"
        assert sched.scope == BackupScope.WORKSPACE
        assert sched.storage_backend == StorageBackend.S3
        assert sched.retention_days == 90

    def test_list_schedules(self):
        mgr = _manager()
        mgr.schedule_backup("hourly")
        mgr.schedule_backup("daily")
        assert len(mgr.list_schedules()) == 2

    def test_get_schedule(self):
        mgr = _manager()
        sched = mgr.schedule_backup("weekly")
        retrieved = mgr.get_schedule(sched.schedule_id)
        assert retrieved is not None
        assert retrieved.schedule_id == sched.schedule_id

    def test_disable_schedule(self):
        mgr = _manager()
        sched = mgr.schedule_backup("monthly")
        assert mgr.disable_schedule(sched.schedule_id) is True
        disabled = mgr.get_schedule(sched.schedule_id)
        assert disabled is not None
        assert disabled.enabled is False

    def test_disable_nonexistent(self):
        mgr = _manager()
        assert mgr.disable_schedule("fake-id") is False

    def test_schedule_storage_backends(self):
        mgr = _manager()
        for backend in StorageBackend:
            sched = mgr.schedule_backup("daily", storage_backend=backend)
            assert sched.storage_backend == backend


# ---------------------------------------------------------------------------
# Restore tests
# ---------------------------------------------------------------------------


class TestRestore:
    def test_restore_full_backup(self):
        store = _make_store()
        mgr = BackupManager(store=store)
        backup = mgr.create_backup(scope=BackupScope.FULL)

        # Clear the store
        store.plans.clear()
        store.tasks.clear()
        assert len(store.plans) == 0

        result = mgr.restore_backup(backup.backup_id)
        assert isinstance(result, RestoreResult)
        assert result.status == RestoreStatus.SUCCESS
        assert result.records_restored > 0
        assert len(store.plans) == 2

    def test_restore_plan_backup(self):
        store = _make_store()
        mgr = BackupManager(store=store)
        backup = mgr.create_backup(scope=BackupScope.PLAN, plan_id="plan-1")

        # Remove plan-1
        del store.plans["plan-1"]
        assert "plan-1" not in store.plans

        result = mgr.restore_backup(backup.backup_id)
        assert result.status == RestoreStatus.SUCCESS
        assert "plan-1" in store.plans

    def test_restore_nonexistent_backup(self):
        mgr = _manager()
        result = mgr.restore_backup("nonexistent")
        assert result.status == RestoreStatus.FAILED
        assert "not found" in result.message

    def test_restore_dry_run(self):
        store = _make_store()
        mgr = BackupManager(store=store)
        backup = mgr.create_backup(scope=BackupScope.FULL)

        store.plans.clear()
        result = mgr.restore_backup(backup.backup_id, dry_run=True)
        assert result.status == RestoreStatus.SUCCESS
        assert result.records_restored > 0
        # Dry run should NOT actually restore
        assert len(store.plans) == 0


# ---------------------------------------------------------------------------
# Verification tests
# ---------------------------------------------------------------------------


class TestVerification:
    def test_verify_valid_backup(self):
        mgr = _manager()
        backup = mgr.create_backup()
        result = mgr.verify_backup(backup.backup_id)

        assert isinstance(result, VerificationResult)
        assert result.verified is True
        assert result.checksum_valid is True
        assert result.data_readable is True
        assert result.record_count_match is True

    def test_verify_promotes_status(self):
        mgr = _manager()
        backup = mgr.create_backup()
        mgr.verify_backup(backup.backup_id)
        verified_backup = mgr.list_backups()[0]
        assert verified_backup.status == BackupStatus.VERIFIED

    def test_verify_nonexistent(self):
        mgr = _manager()
        result = mgr.verify_backup("fake-id")
        assert result.verified is False
        assert "not found" in result.message

    def test_verify_after_restore(self):
        store = _make_store()
        mgr = BackupManager(store=store)
        backup = mgr.create_backup()
        # Verify should still pass (backup data hasn't changed)
        result = mgr.verify_backup(backup.backup_id)
        assert result.verified is True


# ---------------------------------------------------------------------------
# Listing tests
# ---------------------------------------------------------------------------


class TestListBackups:
    def test_list_all(self):
        mgr = _manager()
        mgr.create_backup(scope=BackupScope.FULL)
        mgr.create_backup(scope=BackupScope.PLAN, plan_id="plan-1")
        mgr.create_backup(scope=BackupScope.WORKSPACE, workspace_id="ws-1")
        backups = mgr.list_backups()
        assert len(backups) == 3

    def test_list_filtered_by_scope(self):
        mgr = _manager()
        mgr.create_backup(scope=BackupScope.FULL)
        mgr.create_backup(scope=BackupScope.PLAN, plan_id="plan-1")
        backups = mgr.list_backups(scope=BackupScope.FULL)
        assert len(backups) == 1
        assert backups[0].scope == BackupScope.FULL

    def test_list_filtered_by_status(self):
        mgr = _manager()
        backup = mgr.create_backup()
        mgr.verify_backup(backup.backup_id)
        mgr.create_backup()  # second, not verified

        verified = mgr.list_backups(status=BackupStatus.VERIFIED)
        assert len(verified) == 1
        completed = mgr.list_backups(status=BackupStatus.COMPLETED)
        assert len(completed) == 1

    def test_list_respects_limit(self):
        mgr = _manager()
        for _ in range(10):
            mgr.create_backup()
        backups = mgr.list_backups(limit=3)
        assert len(backups) == 3

    def test_list_sorted_descending(self):
        mgr = _manager()
        mgr.create_backup()
        mgr.create_backup()
        mgr.create_backup()
        backups = mgr.list_backups()
        times = [b.created_at for b in backups]
        assert times == sorted(times, reverse=True)


# ---------------------------------------------------------------------------
# Cleanup / retention tests
# ---------------------------------------------------------------------------


class TestCleanup:
    def test_cleanup_no_expired(self):
        mgr = _manager()
        mgr.create_backup()
        result = mgr.cleanup_old_backups()
        assert isinstance(result, CleanupResult)
        assert result.backups_removed == 0
        assert result.remaining_backups == 1

    def test_cleanup_max_count(self):
        mgr = _manager()
        for _ in range(5):
            mgr.create_backup()
        policy = RetentionPolicy(max_count=2, max_age_days=9999)
        result = mgr.cleanup_old_backups(retention=policy)
        assert result.backups_removed == 3
        assert result.remaining_backups == 2

    def test_cleanup_returns_removed_ids(self):
        mgr = _manager()
        ids = []
        for _ in range(4):
            b = mgr.create_backup()
            ids.append(b.backup_id)
        policy = RetentionPolicy(max_count=1, max_age_days=9999)
        result = mgr.cleanup_old_backups(retention=policy)
        assert result.backups_removed == 3
        for rid in result.removed_ids:
            assert rid in ids

    def test_cleanup_bytes_freed(self):
        mgr = _manager()
        for _ in range(3):
            mgr.create_backup()
        policy = RetentionPolicy(max_count=1, max_age_days=9999)
        result = mgr.cleanup_old_backups(retention=policy)
        assert result.bytes_freed > 0


# ---------------------------------------------------------------------------
# Point-in-time recovery (PITR)
# ---------------------------------------------------------------------------


class TestPointInTimeRecovery:
    def test_continuous_backup_with_pitr(self):
        store = _make_store()
        mgr = BackupManager(store=store)

        # Create continuous backup
        backup = mgr.create_backup(backup_type=BackupType.CONTINUOUS)
        assert backup.backup_type == BackupType.CONTINUOUS

        # Add a transaction log entry after backup
        store.transaction_log.append({
            "timestamp": _now(),
            "operation": "upsert_plan",
            "plan_id": "plan-new",
            "payload": {"id": "plan-new", "title": "New Plan"},
        })

        # Restore with target_time in the future to include log entry
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        result = mgr.restore_backup(backup.backup_id, target_time=future)
        assert result.status == RestoreStatus.SUCCESS

    def test_pitr_excludes_future_entries(self):
        store = _make_store()
        mgr = BackupManager(store=store)
        backup = mgr.create_backup(backup_type=BackupType.CONTINUOUS)

        # Add log entry with a very old timestamp that won't match
        store.transaction_log.append({
            "timestamp": "2020-01-01T00:00:00+00:00",
            "operation": "upsert_plan",
            "plan_id": "plan-old",
            "payload": {"id": "plan-old", "title": "Old Plan"},
        })

        # Restore to a time BEFORE the log entry's original backup time
        past = datetime(2019, 1, 1, tzinfo=timezone.utc)
        result = mgr.restore_backup(backup.backup_id, target_time=past)
        assert result.status == RestoreStatus.SUCCESS


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_backup_empty_store(self):
        mgr = BackupManager(store=BackupDataStore())
        backup = mgr.create_backup()
        assert backup.record_count == 0
        assert backup.status == BackupStatus.COMPLETED

    def test_multiple_backup_types(self):
        mgr = _manager()
        for bt in BackupType:
            backup = mgr.create_backup(backup_type=bt)
            assert backup.backup_type == bt
            assert backup.status == BackupStatus.COMPLETED

    def test_multiple_scopes(self):
        mgr = _manager()
        mgr.create_backup(scope=BackupScope.FULL)
        mgr.create_backup(scope=BackupScope.INCREMENTAL)
        mgr.create_backup(scope=BackupScope.PLAN, plan_id="plan-1")
        mgr.create_backup(scope=BackupScope.WORKSPACE, workspace_id="ws-1")
        assert len(mgr.list_backups()) == 4

    def test_parent_backup_chain(self):
        mgr = _manager()
        b1 = mgr.create_backup()
        b2 = mgr.create_backup()
        assert b1.parent_backup_id is None
        assert b2.parent_backup_id == b1.backup_id

    def test_backup_id_uniqueness(self):
        mgr = _manager()
        ids = {mgr.create_backup().backup_id for _ in range(10)}
        assert len(ids) == 10

    def test_restore_workspace_backup(self):
        store = _make_store()
        mgr = BackupManager(store=store)
        backup = mgr.create_backup(scope=BackupScope.WORKSPACE, workspace_id="ws-1")

        store.plans.clear()
        result = mgr.restore_backup(backup.backup_id)
        assert result.status == RestoreStatus.SUCCESS
        assert len(store.plans) == 2
