"""Tests for the plan archival system with retention policies."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from blueprint.archival.archive_manager import (
    ArchivalResult,
    ArchivalTrigger,
    ArchiveDataStore,
    ArchiveFilters,
    ArchiveManager,
    ArchiveReason,
    ArchiveReport,
    ArchiveStatus,
    ArchiveTriggerConfig,
    ArchivedPlan,
    ColdStorageBackend,
    CompletionDelay,
    RetentionPeriod,
    RetentionPolicy,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _past(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def _make_store() -> ArchiveDataStore:
    """Build a store pre-populated with realistic test data."""
    now = _now()
    store = ArchiveDataStore()

    store.plans["plan-1"] = {
        "id": "plan-1",
        "title": "Auth System",
        "status": "in_progress",
        "tags": ["backend", "security"],
        "user_ids": ["user-1"],
        "tasks": [
            {"id": "task-1", "title": "Login endpoint", "depends_on": []},
            {"id": "task-2", "title": "Token refresh", "depends_on": ["task-1"]},
        ],
        "created_at": now,
        "updated_at": now,
    }
    store.plans["plan-2"] = {
        "id": "plan-2",
        "title": "Dashboard UI",
        "status": "completed",
        "tags": ["frontend"],
        "user_ids": ["user-1", "user-2"],
        "tasks": [
            {"id": "task-3", "title": "Chart component", "depends_on": []},
        ],
        "created_at": now,
        "updated_at": now,
        "completed_at": now,
    }
    store.plans["plan-3"] = {
        "id": "plan-3",
        "title": "Billing Integration",
        "status": "completed",
        "tags": ["backend", "billing"],
        "user_ids": ["user-2"],
        "tasks": [
            {"id": "task-4", "title": "Stripe setup", "depends_on": []},
        ],
        "created_at": _past(100),
        "updated_at": _past(100),
        "completed_at": _past(100),
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
    store.tasks["task-4"] = {
        "id": "task-4",
        "title": "Stripe setup",
        "execution_plan_id": "plan-3",
        "updated_at": _past(100),
    }

    store.users["user-1"] = {"id": "user-1", "name": "Alice"}
    store.users["user-2"] = {"id": "user-2", "name": "Bob"}

    return store


def _manager(store: ArchiveDataStore | None = None) -> ArchiveManager:
    return ArchiveManager(store=store or _make_store())


# ---------------------------------------------------------------------------
# Manual archival tests
# ---------------------------------------------------------------------------


class TestManualArchival:
    def test_archive_plan(self):
        mgr = _manager()
        result = mgr.archive_plan("plan-1", reason=ArchiveReason.MANUAL)

        assert isinstance(result, ArchivedPlan)
        assert result.plan_id == "plan-1"
        assert result.reason == ArchiveReason.MANUAL
        assert result.trigger == ArchivalTrigger.MANUAL
        assert result.status in (ArchiveStatus.ARCHIVED, ArchiveStatus.COMPRESSED)
        assert result.original_size > 0
        assert len(result.checksum) == 64

    def test_archive_removes_from_store(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        mgr.archive_plan("plan-1")
        assert "plan-1" not in store.plans

    def test_archive_with_compression(self):
        mgr = _manager()
        result = mgr.archive_plan("plan-1", compress=True)
        assert result.status == ArchiveStatus.COMPRESSED
        assert result.compressed_size <= result.original_size

    def test_archive_without_compression(self):
        mgr = _manager()
        result = mgr.archive_plan("plan-1", compress=False)
        assert result.status == ArchiveStatus.ARCHIVED
        assert result.compressed_size == result.original_size

    def test_archive_preserves_metadata(self):
        mgr = _manager()
        result = mgr.archive_plan("plan-1", archived_by="alice")
        assert result.archived_by == "alice"
        assert result.plan_title == "Auth System"
        assert result.plan_status == "in_progress"
        assert "backend" in result.tags
        assert "security" in result.tags

    def test_archive_id_uniqueness(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        ids = set()
        for pid in list(store.plans.keys()):
            a = mgr.archive_plan(pid)
            ids.add(a.archive_id)
        assert len(ids) == 3


# ---------------------------------------------------------------------------
# Archival trigger tests
# ---------------------------------------------------------------------------


class TestArchivalTriggers:
    def test_auto_archive_completed_immediate(self):
        store = _make_store()
        config = ArchiveTriggerConfig(
            auto_archive_completed=True,
            completion_delay=CompletionDelay.IMMEDIATE,
        )
        mgr = ArchiveManager(store=store, trigger_config=config)
        results = mgr.auto_archive_completed()
        # plan-2 and plan-3 are completed
        assert len(results) == 2
        for r in results:
            assert r.reason == ArchiveReason.COMPLETED
            assert r.trigger == ArchivalTrigger.COMPLETION

    def test_auto_archive_completed_with_delay(self):
        store = _make_store()
        # plan-2 completed "now", plan-3 completed 100 days ago
        config = ArchiveTriggerConfig(
            auto_archive_completed=True,
            completion_delay=CompletionDelay.THIRTY_DAYS,
        )
        mgr = ArchiveManager(store=store, trigger_config=config)
        results = mgr.auto_archive_completed()
        # Only plan-3 (100 days ago) should be archived
        assert len(results) == 1
        assert results[0].plan_id == "plan-3"

    def test_auto_archive_disabled(self):
        store = _make_store()
        config = ArchiveTriggerConfig(auto_archive_completed=False)
        mgr = ArchiveManager(store=store, trigger_config=config)
        results = mgr.auto_archive_completed()
        assert len(results) == 0

    def test_auto_archive_inactive(self):
        store = _make_store()
        config = ArchiveTriggerConfig(
            auto_archive_inactive=True,
            inactivity_days=50,
        )
        mgr = ArchiveManager(store=store, trigger_config=config)
        results = mgr.auto_archive_inactive()
        # plan-3 was updated 100 days ago
        assert len(results) == 1
        assert results[0].plan_id == "plan-3"
        assert results[0].reason == ArchiveReason.INACTIVE

    def test_auto_archive_inactive_disabled(self):
        store = _make_store()
        config = ArchiveTriggerConfig(auto_archive_inactive=False)
        mgr = ArchiveManager(store=store, trigger_config=config)
        results = mgr.auto_archive_inactive()
        assert len(results) == 0

    def test_bulk_archive_by_status(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        results = mgr.bulk_archive(status=["completed"])
        assert len(results) == 2
        for r in results:
            assert r.reason == ArchiveReason.BULK
            assert r.trigger == ArchivalTrigger.BULK_CRITERIA

    def test_bulk_archive_by_tags(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        results = mgr.bulk_archive(tags=["frontend"])
        assert len(results) == 1
        assert results[0].plan_id == "plan-2"

    def test_bulk_archive_by_date_range(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        old = datetime.now(timezone.utc) - timedelta(days=200)
        recent = datetime.now(timezone.utc) - timedelta(days=50)
        results = mgr.bulk_archive(date_from=old, date_to=recent)
        # plan-3 created 100 days ago, between 200 and 50 days ago
        assert len(results) == 1
        assert results[0].plan_id == "plan-3"


# ---------------------------------------------------------------------------
# Retention policy tests
# ---------------------------------------------------------------------------


class TestRetentionPolicies:
    def test_add_retention_policy(self):
        mgr = _manager()
        policy = RetentionPolicy(
            policy_id="pol-1",
            name="Standard",
            period=RetentionPeriod.ONE_YEAR,
            compress=True,
        )
        result = mgr.add_retention_policy(policy)
        assert result.policy_id == "pol-1"
        assert mgr.get_retention_policy("pol-1") is not None

    def test_indefinite_retention(self):
        mgr = _manager()
        policy = RetentionPolicy(
            policy_id="pol-indef",
            name="Keep Forever",
            period=RetentionPeriod.INDEFINITE,
        )
        mgr.add_retention_policy(policy)
        mgr.archive_plan("plan-1", retention_policy_id="pol-indef")
        result = mgr.apply_retention_policy("pol-indef")
        assert isinstance(result, ArchivalResult)
        assert result.plans_deleted == 0

    def test_time_based_retention(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        policy = RetentionPolicy(
            policy_id="pol-short",
            name="Short Retention",
            period=RetentionPeriod.ONE_YEAR,
            retention_days=1,  # Override to 1 day for testing
            auto_delete=True,
        )
        mgr.add_retention_policy(policy)

        # Archive a plan, then manually backdate it
        archived = mgr.archive_plan("plan-1", retention_policy_id="pol-short")
        # Backdate the archived_at to 2 days ago
        old_date = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
        mgr._archives[archived.archive_id] = ArchivedPlan(
            archive_id=archived.archive_id,
            plan_id=archived.plan_id,
            plan_data=archived.plan_data,
            status=archived.status,
            archived_at=old_date,
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
        )

        result = mgr.apply_retention_policy("pol-short")
        assert result.plans_deleted == 1

    def test_compliance_retention_seven_years(self):
        mgr = _manager()
        policy = RetentionPolicy(
            policy_id="pol-compliance",
            name="7-Year Compliance",
            period=RetentionPeriod.SEVEN_YEARS,
            compress=True,
        )
        mgr.add_retention_policy(policy)
        mgr.archive_plan("plan-1", retention_policy_id="pol-compliance")
        result = mgr.apply_retention_policy("pol-compliance")
        # Should not delete (plan is brand new, well within 7 years)
        assert result.plans_deleted == 0
        assert result.plans_affected == 1

    def test_policy_compression(self):
        mgr = _manager()
        policy = RetentionPolicy(
            policy_id="pol-compress",
            name="Compress Only",
            period=RetentionPeriod.INDEFINITE,
            compress=True,
        )
        mgr.add_retention_policy(policy)
        mgr.archive_plan("plan-1", retention_policy_id="pol-compress", compress=False)

        # Archive should start as ARCHIVED (uncompressed)
        archives = mgr.get_archived_plans()
        assert archives[0].status == ArchiveStatus.ARCHIVED

        result = mgr.apply_retention_policy("pol-compress")
        assert result.plans_compressed == 1

        # After policy application, should be COMPRESSED
        archives = mgr.get_archived_plans()
        assert archives[0].status == ArchiveStatus.COMPRESSED

    def test_policy_not_found(self):
        mgr = _manager()
        result = mgr.apply_retention_policy("nonexistent")
        assert result.plans_affected == 0
        assert len(result.errors) == 1


# ---------------------------------------------------------------------------
# Compression tests
# ---------------------------------------------------------------------------


class TestCompression:
    def test_compressed_data_smaller(self):
        mgr = _manager()
        result = mgr.archive_plan("plan-1", compress=True)
        assert result.compressed_size < result.original_size

    def test_uncompressed_data_preserved(self):
        mgr = _manager()
        result = mgr.archive_plan("plan-1", compress=False)
        # Raw data should be valid JSON
        data = json.loads(result.plan_data)
        assert data["plan"]["id"] == "plan-1"

    def test_compression_ratio(self):
        mgr = _manager()
        result = mgr.archive_plan("plan-1", compress=True)
        ratio = result.compressed_size / result.original_size
        assert ratio < 1.0


# ---------------------------------------------------------------------------
# Cold storage export tests
# ---------------------------------------------------------------------------


class TestColdStorageExport:
    def test_export_to_s3_glacier(self):
        mgr = _manager()
        archived = mgr.archive_plan("plan-1")
        data = mgr.export_archive(archived.archive_id, backend=ColdStorageBackend.S3_GLACIER)
        assert isinstance(data, bytes)
        assert len(data) > 0

    def test_export_to_azure_archive(self):
        mgr = _manager()
        archived = mgr.archive_plan("plan-1")
        data = mgr.export_archive(archived.archive_id, backend=ColdStorageBackend.AZURE_ARCHIVE)
        assert isinstance(data, bytes)

    def test_export_updates_status(self):
        mgr = _manager()
        archived = mgr.archive_plan("plan-1")
        mgr.export_archive(archived.archive_id)
        updated = mgr.get_archive(archived.archive_id)
        assert updated is not None
        assert updated.status == ArchiveStatus.EXPORTED
        assert updated.cold_storage_backend == ColdStorageBackend.S3_GLACIER

    def test_export_sets_path(self):
        mgr = _manager()
        archived = mgr.archive_plan("plan-1")
        mgr.export_archive(archived.archive_id, backend=ColdStorageBackend.AZURE_ARCHIVE)
        updated = mgr.get_archive(archived.archive_id)
        assert updated is not None
        assert updated.cold_storage_path is not None
        assert "azure_archive" in updated.cold_storage_path

    def test_export_nonexistent_raises(self):
        mgr = _manager()
        with pytest.raises(KeyError):
            mgr.export_archive("fake-id")

    def test_policy_cold_storage_export(self):
        mgr = _manager()
        policy = RetentionPolicy(
            policy_id="pol-cold",
            name="Cold Storage",
            period=RetentionPeriod.INDEFINITE,
            cold_storage_backend=ColdStorageBackend.S3_GLACIER,
        )
        mgr.add_retention_policy(policy)
        mgr.archive_plan("plan-1", retention_policy_id="pol-cold")
        result = mgr.apply_retention_policy("pol-cold")
        assert result.plans_exported == 1


# ---------------------------------------------------------------------------
# Restoration tests
# ---------------------------------------------------------------------------


class TestRestoration:
    def test_restore_plan(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        archived = mgr.archive_plan("plan-1")
        assert "plan-1" not in store.plans

        plan_data = mgr.restore_plan(archived.archive_id)
        assert "plan-1" in store.plans
        assert plan_data["plan"]["id"] == "plan-1"

    def test_restore_as_new(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        archived = mgr.archive_plan("plan-1")
        plan_data = mgr.restore_plan(archived.archive_id, as_new=True)
        # New ID should be assigned
        assert plan_data["plan"]["id"] != "plan-1"
        assert plan_data["plan"]["id"].startswith("plan-")
        assert plan_data["plan"]["status"] == "draft"

    def test_restore_tasks_only(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        archived = mgr.archive_plan("plan-1")
        plan_data = mgr.restore_plan(archived.archive_id, tasks_only=True)
        assert "plan" not in plan_data
        assert "tasks" in plan_data

    def test_restore_removes_from_archives(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        archived = mgr.archive_plan("plan-1")
        mgr.restore_plan(archived.archive_id)
        assert mgr.get_archive(archived.archive_id) is None

    def test_restore_nonexistent_raises(self):
        mgr = _manager()
        with pytest.raises(KeyError):
            mgr.restore_plan("fake-id")

    def test_restore_compressed(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        archived = mgr.archive_plan("plan-1", compress=True)
        assert archived.status == ArchiveStatus.COMPRESSED

        plan_data = mgr.restore_plan(archived.archive_id)
        assert plan_data["plan"]["id"] == "plan-1"
        assert "plan-1" in store.plans


# ---------------------------------------------------------------------------
# Deletion tests
# ---------------------------------------------------------------------------


class TestDeletion:
    def test_delete_archived_plan(self):
        mgr = _manager()
        archived = mgr.archive_plan("plan-1")
        mgr.delete_archived_plan(archived.archive_id)
        assert mgr.get_archive(archived.archive_id) is None

    def test_delete_without_confirmation_raises(self):
        mgr = _manager()
        archived = mgr.archive_plan("plan-1")
        with pytest.raises(ValueError):
            mgr.delete_archived_plan(archived.archive_id, confirm=False)

    def test_delete_nonexistent_raises(self):
        mgr = _manager()
        with pytest.raises(KeyError):
            mgr.delete_archived_plan("fake-id")

    def test_delete_removes_cold_storage(self):
        mgr = _manager()
        archived = mgr.archive_plan("plan-1")
        mgr.export_archive(archived.archive_id)
        assert archived.archive_id in mgr._cold_storage
        mgr.delete_archived_plan(archived.archive_id)
        assert archived.archive_id not in mgr._cold_storage


# ---------------------------------------------------------------------------
# Search and filtering tests
# ---------------------------------------------------------------------------


class TestSearchAndFiltering:
    def test_get_all_archived_plans(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        mgr.archive_plan("plan-1")
        mgr.archive_plan("plan-2")
        results = mgr.get_archived_plans()
        assert len(results) == 2

    def test_filter_by_status(self):
        mgr = _manager()
        mgr.archive_plan("plan-1", compress=True)
        mgr.archive_plan("plan-2", compress=False)
        results = mgr.get_archived_plans(
            filters=ArchiveFilters(status=ArchiveStatus.COMPRESSED)
        )
        assert len(results) == 1
        assert results[0].status == ArchiveStatus.COMPRESSED

    def test_filter_by_reason(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        mgr.archive_plan("plan-1", reason=ArchiveReason.MANUAL)
        mgr.archive_plan("plan-2", reason=ArchiveReason.COMPLETED)
        results = mgr.get_archived_plans(
            filters=ArchiveFilters(reason=ArchiveReason.MANUAL)
        )
        assert len(results) == 1

    def test_filter_by_owner(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        mgr.archive_plan("plan-1")  # owner: user-1
        mgr.archive_plan("plan-2")  # owner: user-1
        mgr.archive_plan("plan-3")  # owner: user-2
        results = mgr.get_archived_plans(
            filters=ArchiveFilters(owner="user-2")
        )
        assert len(results) == 1

    def test_filter_by_tags(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        mgr.archive_plan("plan-1")  # tags: backend, security
        mgr.archive_plan("plan-2")  # tags: frontend
        results = mgr.get_archived_plans(
            filters=ArchiveFilters(tags=["frontend"])
        )
        assert len(results) == 1
        assert results[0].plan_id == "plan-2"

    def test_filter_by_date_range(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        mgr.archive_plan("plan-1")
        results = mgr.get_archived_plans(
            filters=ArchiveFilters(
                date_from=datetime.now(timezone.utc) - timedelta(hours=1),
                date_to=datetime.now(timezone.utc) + timedelta(hours=1),
            )
        )
        assert len(results) == 1

    def test_sorted_descending(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        mgr.archive_plan("plan-1")
        mgr.archive_plan("plan-2")
        mgr.archive_plan("plan-3")
        results = mgr.get_archived_plans()
        times = [a.archived_at for a in results]
        assert times == sorted(times, reverse=True)


# ---------------------------------------------------------------------------
# Preview tests
# ---------------------------------------------------------------------------


class TestPreview:
    def test_preview_archive(self):
        mgr = _manager()
        archived = mgr.archive_plan("plan-1")
        preview = mgr.preview_archive(archived.archive_id)
        assert preview["plan_id"] == "plan-1"
        assert preview["plan_title"] == "Auth System"
        assert preview["task_count"] == 2
        assert preview["archived_by"] == "system"
        assert "backend" in preview["tags"]

    def test_preview_nonexistent_raises(self):
        mgr = _manager()
        with pytest.raises(KeyError):
            mgr.preview_archive("fake-id")

    def test_preview_does_not_restore(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        archived = mgr.archive_plan("plan-1")
        mgr.preview_archive(archived.archive_id)
        # Plan should still be archived, not in active store
        assert "plan-1" not in store.plans
        assert mgr.get_archive(archived.archive_id) is not None


# ---------------------------------------------------------------------------
# Lifecycle tracking tests
# ---------------------------------------------------------------------------


class TestLifecycleTracking:
    def test_lifecycle_archived(self):
        mgr = _manager()
        archived = mgr.archive_plan("plan-1", compress=False)
        assert archived.status == ArchiveStatus.ARCHIVED

    def test_lifecycle_compressed(self):
        mgr = _manager()
        archived = mgr.archive_plan("plan-1", compress=True)
        assert archived.status == ArchiveStatus.COMPRESSED

    def test_lifecycle_exported(self):
        mgr = _manager()
        archived = mgr.archive_plan("plan-1")
        mgr.export_archive(archived.archive_id)
        updated = mgr.get_archive(archived.archive_id)
        assert updated is not None
        assert updated.status == ArchiveStatus.EXPORTED

    def test_lifecycle_scheduled_for_deletion(self):
        mgr = _manager()
        policy = RetentionPolicy(
            policy_id="pol-del",
            name="Delete After 1 Day",
            period=RetentionPeriod.ONE_YEAR,
            retention_days=30,
            auto_delete=True,
        )
        mgr.add_retention_policy(policy)
        archived = mgr.archive_plan("plan-1", retention_policy_id="pol-del")
        assert archived.scheduled_deletion_date is not None

    def test_lifecycle_deleted(self):
        mgr = _manager()
        archived = mgr.archive_plan("plan-1")
        mgr.delete_archived_plan(archived.archive_id)
        assert mgr.get_archive(archived.archive_id) is None


# ---------------------------------------------------------------------------
# Report tests
# ---------------------------------------------------------------------------


class TestReports:
    def test_generate_report(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        mgr.archive_plan("plan-1", reason=ArchiveReason.MANUAL)
        mgr.archive_plan("plan-2", reason=ArchiveReason.COMPLETED)
        report = mgr.generate_report()

        assert isinstance(report, ArchiveReport)
        assert report.total_archives == 2
        assert report.total_original_bytes > 0
        assert report.total_compressed_bytes > 0
        assert report.compression_ratio > 0

    def test_report_by_status(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        mgr.archive_plan("plan-1", compress=True)
        mgr.archive_plan("plan-2", compress=False)
        report = mgr.generate_report()
        assert ArchiveStatus.COMPRESSED.value in report.archives_by_status
        assert ArchiveStatus.ARCHIVED.value in report.archives_by_status

    def test_report_by_reason(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        mgr.archive_plan("plan-1", reason=ArchiveReason.MANUAL)
        mgr.archive_plan("plan-2", reason=ArchiveReason.COMPLETED)
        report = mgr.generate_report()
        assert report.archives_by_reason[ArchiveReason.MANUAL.value] == 1
        assert report.archives_by_reason[ArchiveReason.COMPLETED.value] == 1

    def test_report_per_period(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        mgr.archive_plan("plan-1")
        report = mgr.generate_report()
        assert len(report.archives_per_period) >= 1

    def test_report_retention_compliance(self):
        mgr = _manager()
        policy = RetentionPolicy(
            policy_id="pol-check",
            name="Check Policy",
            period=RetentionPeriod.ONE_YEAR,
        )
        mgr.add_retention_policy(policy)
        mgr.archive_plan("plan-1", retention_policy_id="pol-check")
        report = mgr.generate_report()
        assert report.retention_compliance["pol-check"] is True

    def test_empty_report(self):
        mgr = _manager()
        report = mgr.generate_report()
        assert report.total_archives == 0
        assert report.compression_ratio == 0.0


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_archive_empty_plan(self):
        store = ArchiveDataStore()
        store.plans["empty"] = {
            "id": "empty",
            "title": "Empty Plan",
            "status": "draft",
            "tags": [],
            "user_ids": [],
            "tasks": [],
            "created_at": _now(),
            "updated_at": _now(),
        }
        mgr = ArchiveManager(store=store)
        result = mgr.archive_plan("empty")
        assert result.plan_id == "empty"
        assert result.original_size > 0

    def test_archive_nonexistent_plan(self):
        mgr = _manager()
        # Archiving a nonexistent plan still works (empty data)
        result = mgr.archive_plan("nonexistent")
        assert result.plan_id == "nonexistent"
        assert result.plan_title == ""

    def test_multiple_archive_restore_cycles(self):
        store = _make_store()
        mgr = ArchiveManager(store=store)
        # Archive and restore plan-1 multiple times
        for _ in range(3):
            archived = mgr.archive_plan("plan-1")
            assert "plan-1" not in store.plans
            mgr.restore_plan(archived.archive_id)
            assert "plan-1" in store.plans

    def test_archive_with_retention_policy(self):
        mgr = _manager()
        policy = RetentionPolicy(
            policy_id="pol-test",
            name="Test Policy",
            period=RetentionPeriod.TWO_YEARS,
        )
        mgr.add_retention_policy(policy)
        result = mgr.archive_plan("plan-1", retention_policy_id="pol-test")
        assert result.retention_policy_id == "pol-test"
        assert result.scheduled_deletion_date is not None
