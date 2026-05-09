"""Tests for plan split functionality."""

from datetime import datetime, timezone

from blueprint.splitting.plan_splitter import (
    PlanSplitter,
    SplitConfig,
    SplitDataStore,
    SplitPreview,
    SplitResult,
    SplitStrategy,
    SplitSuggestion,
    ValidationResult,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _make_store() -> SplitDataStore:
    """Build a store with a plan suitable for splitting."""
    now = _now()
    store = SplitDataStore()

    store.plans["plan-large"] = {
        "id": "plan-large",
        "title": "Large Project",
        "status": "in_progress",
        "tags": ["backend", "frontend"],
        "user_ids": ["user-1", "user-2", "user-3"],
        "tasks": [
            {"id": "t-1", "title": "API design", "depends_on": [], "tags": ["backend"], "assignee": "user-1"},
            {"id": "t-2", "title": "API implementation", "depends_on": ["t-1"], "tags": ["backend"], "assignee": "user-1"},
            {"id": "t-3", "title": "UI wireframes", "depends_on": [], "tags": ["frontend"], "assignee": "user-2"},
            {"id": "t-4", "title": "UI implementation", "depends_on": ["t-3"], "tags": ["frontend"], "assignee": "user-2"},
            {"id": "t-5", "title": "Integration testing", "depends_on": ["t-2", "t-4"], "tags": ["testing"], "assignee": "user-3"},
            {"id": "t-6", "title": "Documentation", "depends_on": ["t-2"], "tags": ["docs"], "assignee": "user-3"},
        ],
        "created_at": now,
        "updated_at": now,
    }

    store.plans["plan-small"] = {
        "id": "plan-small",
        "title": "Small Project",
        "status": "draft",
        "tags": ["backend"],
        "user_ids": ["user-1"],
        "tasks": [
            {"id": "ts-1", "title": "Single task", "depends_on": [], "tags": ["backend"], "assignee": "user-1"},
        ],
        "created_at": now,
        "updated_at": now,
    }

    return store


def _splitter(store: SplitDataStore | None = None) -> PlanSplitter:
    return PlanSplitter(store=store or _make_store())


# ---------------------------------------------------------------------------
# Split by tags tests
# ---------------------------------------------------------------------------


class TestSplitByTags:
    def test_split_by_tags(self):
        splitter = _splitter()
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_TAGS)
        assert len(result) > 1  # Should split into multiple plans

    def test_split_by_tags_preserves_all_tasks(self):
        splitter = _splitter()
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_TAGS)
        total_tasks = sum(len(p.get("tasks", [])) for p in result)
        assert total_tasks == 6

    def test_split_by_tags_creates_titled_plans(self):
        splitter = _splitter()
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_TAGS)
        for plan in result:
            assert "Large Project" in plan["title"]

    def test_split_by_tags_sets_metadata(self):
        splitter = _splitter()
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_TAGS)
        for plan in result:
            assert plan["metadata"]["split_strategy"] == "by_tags"
            assert plan["metadata"]["source_plan_id"] == "plan-large"


# ---------------------------------------------------------------------------
# Split by assignee tests
# ---------------------------------------------------------------------------


class TestSplitByAssignee:
    def test_split_by_assignee(self):
        splitter = _splitter()
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_ASSIGNEE)
        assert len(result) == 3  # 3 assignees

    def test_split_by_assignee_groups_correctly(self):
        splitter = _splitter()
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_ASSIGNEE)
        for plan in result:
            assignees = set()
            for t in plan.get("tasks", []):
                if t.get("assignee"):
                    assignees.add(t["assignee"])
            # Each sub-plan should have tasks from at most one assignee
            assert len(assignees) <= 1


# ---------------------------------------------------------------------------
# Split by dependency cluster tests
# ---------------------------------------------------------------------------


class TestSplitByDependencyCluster:
    def test_split_by_dependency_cluster(self):
        splitter = _splitter()
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_DEPENDENCY_CLUSTER)
        # All tasks are connected via t-5 depending on t-2 and t-4
        # So it should be one cluster
        assert len(result) >= 1

    def test_independent_clusters_split(self):
        store = SplitDataStore()
        store.plans["disconnected"] = {
            "id": "disconnected",
            "title": "Disconnected",
            "status": "draft",
            "tags": [],
            "user_ids": [],
            "tasks": [
                {"id": "a1", "title": "A1", "depends_on": [], "tags": []},
                {"id": "a2", "title": "A2", "depends_on": ["a1"], "tags": []},
                {"id": "b1", "title": "B1", "depends_on": [], "tags": []},
                {"id": "b2", "title": "B2", "depends_on": ["b1"], "tags": []},
            ],
            "created_at": _now(),
            "updated_at": _now(),
        }
        splitter = PlanSplitter(store=store)
        result = splitter.split_plan("disconnected", strategy=SplitStrategy.BY_DEPENDENCY_CLUSTER)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Split by phase tests
# ---------------------------------------------------------------------------


class TestSplitByPhase:
    def test_split_by_phase_auto(self):
        splitter = _splitter()
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_PHASE)
        assert len(result) >= 2

    def test_split_by_phase_named(self):
        splitter = _splitter()
        config = SplitConfig(phases=["API", "UI"])
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_PHASE, config=config)
        assert len(result) >= 1

    def test_split_by_phase_preserves_tasks(self):
        splitter = _splitter()
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_PHASE)
        total = sum(len(p.get("tasks", [])) for p in result)
        assert total == 6


# ---------------------------------------------------------------------------
# Split by timeline tests
# ---------------------------------------------------------------------------


class TestSplitByTimeline:
    def test_split_by_timeline(self):
        splitter = _splitter()
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_TIMELINE)
        assert len(result) >= 2

    def test_split_by_timeline_preserves_tasks(self):
        splitter = _splitter()
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_TIMELINE)
        total = sum(len(p.get("tasks", [])) for p in result)
        assert total == 6


# ---------------------------------------------------------------------------
# Suggest split points tests
# ---------------------------------------------------------------------------


class TestSplitSuggestions:
    def test_suggest_split_points(self):
        splitter = _splitter()
        suggestions = splitter.suggest_split_points("plan-large")
        assert len(suggestions) > 0
        assert all(isinstance(s, SplitSuggestion) for s in suggestions)

    def test_suggestions_sorted_by_score(self):
        splitter = _splitter()
        suggestions = splitter.suggest_split_points("plan-large")
        scores = [s.score for s in suggestions]
        assert scores == sorted(scores, reverse=True)

    def test_suggest_by_tags(self):
        splitter = _splitter()
        suggestions = splitter.suggest_split_points("plan-large")
        tag_suggestions = [s for s in suggestions if s.strategy == SplitStrategy.BY_TAGS]
        assert len(tag_suggestions) >= 1

    def test_suggest_by_assignee(self):
        splitter = _splitter()
        suggestions = splitter.suggest_split_points("plan-large")
        assignee_suggestions = [s for s in suggestions if s.strategy == SplitStrategy.BY_ASSIGNEE]
        assert len(assignee_suggestions) >= 1

    def test_no_suggestions_for_missing_plan(self):
        splitter = _splitter()
        suggestions = splitter.suggest_split_points("nonexistent")
        assert len(suggestions) == 0


# ---------------------------------------------------------------------------
# Preview and validation tests
# ---------------------------------------------------------------------------


class TestPreviewAndValidation:
    def test_preview_split(self):
        splitter = _splitter()
        preview = splitter.preview_split("plan-large", strategy=SplitStrategy.BY_TAGS)
        assert isinstance(preview, SplitPreview)
        assert preview.result_count > 0
        assert len(preview.plan_summaries) > 0

    def test_validate_valid_config(self):
        splitter = _splitter()
        config = SplitConfig()
        result = splitter.validate_split("plan-large", config)
        assert isinstance(result, ValidationResult)
        assert result.valid is True

    def test_validate_missing_plan(self):
        splitter = _splitter()
        config = SplitConfig()
        result = splitter.validate_split("nonexistent", config)
        assert result.valid is False
        assert len(result.errors) > 0

    def test_validate_small_plan_warning(self):
        splitter = _splitter()
        config = SplitConfig()
        result = splitter.validate_split("plan-small", config)
        assert result.valid is True
        assert len(result.warnings) > 0


# ---------------------------------------------------------------------------
# Split options tests
# ---------------------------------------------------------------------------


class TestSplitOptions:
    def test_preserve_ids(self):
        splitter = _splitter()
        config = SplitConfig(preserve_ids=True)
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_ASSIGNEE, config=config)
        all_ids = set()
        for plan in result:
            for task in plan.get("tasks", []):
                all_ids.add(task["id"])
        assert "t-1" in all_ids

    def test_internal_dependencies_only(self):
        splitter = _splitter()
        result = splitter.split_plan("plan-large", strategy=SplitStrategy.BY_ASSIGNEE)
        for plan in result:
            task_ids = {t["id"] for t in plan.get("tasks", [])}
            for task in plan.get("tasks", []):
                for dep in task.get("depends_on", []):
                    assert dep in task_ids


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_split_nonexistent_plan(self):
        splitter = _splitter()
        result = splitter.split_plan("nonexistent")
        assert result == []

    def test_split_single_task_plan(self):
        splitter = _splitter()
        result = splitter.split_plan("plan-small", strategy=SplitStrategy.BY_TAGS)
        assert len(result) == 1  # Can't meaningfully split single task

    def test_split_empty_plan(self):
        store = SplitDataStore()
        store.plans["empty"] = {
            "id": "empty",
            "title": "Empty",
            "status": "draft",
            "tags": [],
            "user_ids": [],
            "tasks": [],
            "created_at": _now(),
        }
        splitter = PlanSplitter(store=store)
        result = splitter.split_plan("empty", strategy=SplitStrategy.BY_TAGS)
        assert len(result) == 1

    def test_all_strategies_work(self):
        splitter = _splitter()
        for strategy in SplitStrategy:
            result = splitter.split_plan("plan-large", strategy=strategy)
            assert len(result) >= 1
            total = sum(len(p.get("tasks", [])) for p in result)
            assert total == 6  # No tasks lost
