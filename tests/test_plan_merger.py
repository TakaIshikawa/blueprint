"""Tests for plan merge functionality."""

import json
from datetime import datetime, timezone

from blueprint.merging.plan_merger import (
    Conflict,
    ConflictType,
    MergeConfig,
    MergeDataStore,
    MergePreview,
    MergeReport,
    MergeResult,
    MergeStrategy,
    PlanMerger,
    ResolutionStrategy,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_store() -> MergeDataStore:
    """Build a store with plans for merging."""
    now = _now()
    store = MergeDataStore()

    store.plans["plan-a"] = {
        "id": "plan-a",
        "title": "Auth System",
        "status": "in_progress",
        "tags": ["backend", "security"],
        "user_ids": ["user-1"],
        "tasks": [
            {"id": "t-1", "title": "Login endpoint", "depends_on": [], "status": "completed"},
            {"id": "t-2", "title": "Token refresh", "depends_on": ["t-1"], "status": "pending"},
        ],
        "created_at": now,
        "updated_at": now,
    }

    store.plans["plan-b"] = {
        "id": "plan-b",
        "title": "Dashboard UI",
        "status": "draft",
        "tags": ["frontend"],
        "user_ids": ["user-2"],
        "tasks": [
            {"id": "t-3", "title": "Chart component", "depends_on": [], "status": "pending"},
            {"id": "t-4", "title": "Dashboard layout", "depends_on": ["t-3"], "status": "pending"},
        ],
        "created_at": now,
        "updated_at": now,
    }

    store.plans["plan-c"] = {
        "id": "plan-c",
        "title": "Shared Tasks",
        "status": "draft",
        "tags": ["backend"],
        "user_ids": ["user-1", "user-2"],
        "tasks": [
            {"id": "t-1", "title": "Login endpoint", "depends_on": [], "status": "completed"},
            {"id": "t-5", "title": "API docs", "depends_on": [], "status": "pending"},
        ],
        "created_at": now,
        "updated_at": now,
    }

    return store


def _merger(store: MergeDataStore | None = None) -> PlanMerger:
    return PlanMerger(store=store or _make_store())


# ---------------------------------------------------------------------------
# Union merge tests
# ---------------------------------------------------------------------------


class TestUnionMerge:
    def test_union_combines_all_tasks(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "plan-b"], strategy=MergeStrategy.UNION)
        assert isinstance(result, MergeResult)
        assert result.tasks_merged == 4  # 2 + 2

    def test_union_combines_tags(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "plan-b"], strategy=MergeStrategy.UNION)
        tags = result.merged_plan.get("tags", [])
        assert "backend" in tags
        assert "frontend" in tags
        assert "security" in tags

    def test_union_combines_user_ids(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "plan-b"], strategy=MergeStrategy.UNION)
        user_ids = result.merged_plan.get("user_ids", [])
        assert "user-1" in user_ids
        assert "user-2" in user_ids

    def test_union_creates_new_plan_id(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "plan-b"], strategy=MergeStrategy.UNION)
        assert result.merged_plan["id"].startswith("plan-")

    def test_union_records_source_plans(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "plan-b"], strategy=MergeStrategy.UNION)
        assert "plan-a" in result.merged_plan["metadata"]["source_plans"]
        assert "plan-b" in result.merged_plan["metadata"]["source_plans"]


# ---------------------------------------------------------------------------
# Intersection merge tests
# ---------------------------------------------------------------------------


class TestIntersectionMerge:
    def test_intersection_keeps_common_tasks(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "plan-c"], strategy=MergeStrategy.INTERSECTION)
        # "Login endpoint" is common
        titles = [t["title"] for t in result.merged_plan.get("tasks", [])]
        assert "Login endpoint" in titles

    def test_intersection_excludes_unique_tasks(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "plan-c"], strategy=MergeStrategy.INTERSECTION)
        titles = [t["title"] for t in result.merged_plan.get("tasks", [])]
        assert "Token refresh" not in titles
        assert "API docs" not in titles

    def test_intersection_no_common_tasks(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "plan-b"], strategy=MergeStrategy.INTERSECTION)
        assert len(result.merged_plan.get("tasks", [])) == 0


# ---------------------------------------------------------------------------
# Append merge tests
# ---------------------------------------------------------------------------


class TestAppendMerge:
    def test_append_adds_sequentially(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "plan-b"], strategy=MergeStrategy.APPEND)
        assert result.tasks_merged == 4

    def test_append_preserves_order(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "plan-b"], strategy=MergeStrategy.APPEND)
        titles = [t["title"] for t in result.merged_plan.get("tasks", [])]
        # Plan-a tasks come first, then plan-b
        assert titles[0] == "Login endpoint"
        assert titles[1] == "Token refresh"
        assert titles[2] == "Chart component"
        assert titles[3] == "Dashboard layout"


# ---------------------------------------------------------------------------
# Interleave merge tests
# ---------------------------------------------------------------------------


class TestInterleaveMerge:
    def test_interleave_alternates_tasks(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "plan-b"], strategy=MergeStrategy.INTERLEAVE)
        assert result.tasks_merged == 4
        titles = [t["title"] for t in result.merged_plan.get("tasks", [])]
        # Should alternate: plan-a[0], plan-b[0], plan-a[1], plan-b[1]
        assert titles[0] == "Login endpoint"
        assert titles[1] == "Chart component"
        assert titles[2] == "Token refresh"
        assert titles[3] == "Dashboard layout"


# ---------------------------------------------------------------------------
# Conflict detection tests
# ---------------------------------------------------------------------------


class TestConflictDetection:
    def test_detect_duplicate_ids(self):
        merger = _merger()
        conflicts = merger.detect_conflicts(["plan-a", "plan-c"])
        id_conflicts = [c for c in conflicts if c.conflict_type == ConflictType.DUPLICATE_ID]
        assert len(id_conflicts) >= 1

    def test_detect_resource_conflicts(self):
        merger = _merger()
        conflicts = merger.detect_conflicts(["plan-a", "plan-c"])
        resource_conflicts = [c for c in conflicts if c.conflict_type == ConflictType.RESOURCE_CONFLICT]
        assert len(resource_conflicts) >= 1

    def test_detect_metadata_conflicts(self):
        merger = _merger()
        conflicts = merger.detect_conflicts(["plan-a", "plan-b"])
        metadata_conflicts = [c for c in conflicts if c.conflict_type == ConflictType.METADATA_CONFLICT]
        assert len(metadata_conflicts) >= 1

    def test_no_conflicts_same_plan(self):
        store = MergeDataStore()
        store.plans["solo"] = {
            "id": "solo",
            "title": "Solo",
            "status": "draft",
            "tags": [],
            "user_ids": [],
            "tasks": [{"id": "t-1", "title": "Task 1", "depends_on": []}],
            "created_at": _now(),
        }
        merger = PlanMerger(store=store)
        conflicts = merger.detect_conflicts(["solo"])
        assert len(conflicts) == 0


# ---------------------------------------------------------------------------
# Conflict resolution tests
# ---------------------------------------------------------------------------


class TestConflictResolution:
    def test_auto_resolve_prefer_first(self):
        merger = _merger()
        config = MergeConfig(resolution_strategy=ResolutionStrategy.PREFER_FIRST)
        result = merger.merge_plans(["plan-a", "plan-c"], config=config)
        assert result.conflicts_resolved > 0

    def test_auto_resolve_skip(self):
        merger = _merger()
        config = MergeConfig(resolution_strategy=ResolutionStrategy.SKIP)
        result = merger.merge_plans(["plan-a", "plan-c"], config=config)
        assert result.conflicts_resolved > 0

    def test_manual_resolve(self):
        merger = _merger()
        conflicts = merger.detect_conflicts(["plan-a", "plan-c"])
        assert len(conflicts) > 0
        merger.resolve_conflict(conflicts[0].conflict_id, ResolutionStrategy.PREFER_FIRST)


# ---------------------------------------------------------------------------
# Merge options tests
# ---------------------------------------------------------------------------


class TestMergeOptions:
    def test_preserve_ids(self):
        merger = _merger()
        config = MergeConfig(preserve_ids=True)
        result = merger.merge_plans(["plan-a", "plan-b"], config=config)
        task_ids = [t["id"] for t in result.merged_plan.get("tasks", [])]
        assert "t-1" in task_ids
        assert "t-3" in task_ids

    def test_merge_teams(self):
        merger = _merger()
        config = MergeConfig(merge_teams=True)
        result = merger.merge_plans(["plan-a", "plan-b"], config=config)
        assert len(result.merged_plan["user_ids"]) == 2


# ---------------------------------------------------------------------------
# Preview and report tests
# ---------------------------------------------------------------------------


class TestPreviewAndReport:
    def test_preview_merge(self):
        merger = _merger()
        preview = merger.preview_merge(["plan-a", "plan-b"])
        assert isinstance(preview, MergePreview)
        assert preview.total_tasks == 4
        assert len(preview.plan_ids) == 2

    def test_preview_shows_conflicts(self):
        merger = _merger()
        preview = merger.preview_merge(["plan-a", "plan-c"])
        assert len(preview.conflicts) > 0

    def test_generate_report(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "plan-b"])
        report = merger.generate_merge_report(result)
        assert isinstance(report, MergeReport)
        assert len(report.data) > 0
        data = json.loads(report.data)
        assert data["merge_id"] == result.merge_id


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_merge_single_plan(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a"])
        assert result.tasks_merged == 2

    def test_merge_empty_plans(self):
        store = MergeDataStore()
        store.plans["empty-1"] = {"id": "empty-1", "title": "E1", "status": "draft", "tags": [], "user_ids": [], "tasks": [], "created_at": _now()}
        store.plans["empty-2"] = {"id": "empty-2", "title": "E2", "status": "draft", "tags": [], "user_ids": [], "tasks": [], "created_at": _now()}
        merger = PlanMerger(store=store)
        result = merger.merge_plans(["empty-1", "empty-2"])
        assert result.tasks_merged == 0

    def test_merge_nonexistent_plan(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "nonexistent"])
        assert len(result.plan_ids) == 1

    def test_merge_three_plans(self):
        merger = _merger()
        result = merger.merge_plans(["plan-a", "plan-b", "plan-c"])
        assert result.tasks_merged >= 4  # At least tasks from a + b

    def test_merge_id_unique(self):
        merger = _merger()
        r1 = merger.merge_plans(["plan-a", "plan-b"])
        r2 = merger.merge_plans(["plan-a", "plan-b"])
        assert r1.merge_id != r2.merge_id
