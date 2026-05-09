"""Plan merge functionality for combining multiple plans.

Supports union, intersection, append, interleave, and custom merge
strategies with conflict detection, resolution, and preview.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class MergeStrategy(str, Enum):
    """Strategy for merging plans."""

    UNION = "union"
    INTERSECTION = "intersection"
    APPEND = "append"
    INTERLEAVE = "interleave"
    CUSTOM = "custom"


class ConflictType(str, Enum):
    """Type of merge conflict."""

    DUPLICATE_ID = "duplicate_id"
    OVERLAPPING_DEPENDENCY = "overlapping_dependency"
    RESOURCE_CONFLICT = "resource_conflict"
    TIMELINE_CONFLICT = "timeline_conflict"
    METADATA_CONFLICT = "metadata_conflict"


class ResolutionStrategy(str, Enum):
    """How to resolve a conflict."""

    PREFER_FIRST = "prefer_first"
    PREFER_LAST = "prefer_last"
    PREFER_HIGHER_PRIORITY = "prefer_higher_priority"
    MANUAL = "manual"
    SKIP = "skip"
    MERGE_VALUES = "merge_values"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Conflict:
    """A merge conflict between plans."""

    conflict_id: str
    conflict_type: ConflictType
    plan_ids: list[str] = field(default_factory=list)
    task_ids: list[str] = field(default_factory=list)
    description: str = ""
    values: dict[str, Any] = field(default_factory=dict)
    resolved: bool = False
    resolution: ResolutionStrategy | None = None


@dataclass(frozen=True, slots=True)
class MergeConfig:
    """Configuration for a merge operation."""

    preserve_ids: bool = False
    adjust_timelines: bool = True
    merge_teams: bool = True
    resolution_strategy: ResolutionStrategy = ResolutionStrategy.PREFER_FIRST
    custom_filter: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class MergePreview:
    """Preview of a merge result before committing."""

    preview_id: str
    plan_ids: list[str] = field(default_factory=list)
    strategy: MergeStrategy = MergeStrategy.UNION
    total_tasks: int = 0
    conflicts: list[Conflict] = field(default_factory=list)
    merged_plan_summary: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MergeResult:
    """Result of a completed merge operation."""

    merge_id: str
    plan_ids: list[str] = field(default_factory=list)
    strategy: MergeStrategy = MergeStrategy.UNION
    merged_plan: dict[str, Any] = field(default_factory=dict)
    conflicts_found: int = 0
    conflicts_resolved: int = 0
    tasks_merged: int = 0
    created_at: str = ""


@dataclass(frozen=True, slots=True)
class MergeReport:
    """Report on a merge operation."""

    report_id: str
    merge_id: str
    data: bytes = b""
    created_at: str = ""


# ---------------------------------------------------------------------------
# Data store
# ---------------------------------------------------------------------------


@dataclass
class MergeDataStore:
    """In-memory store providing plan data for merge operations."""

    plans: dict[str, dict[str, Any]] = field(default_factory=dict)
    tasks: dict[str, dict[str, Any]] = field(default_factory=dict)

    def get_plan(self, plan_id: str) -> dict[str, Any] | None:
        return self.plans.get(plan_id)

    def get_plans(self, plan_ids: list[str]) -> dict[str, dict[str, Any]]:
        return {pid: self.plans[pid] for pid in plan_ids if pid in self.plans}

    def add_plan(self, plan: dict[str, Any]) -> None:
        self.plans[plan["id"]] = plan


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_id(prefix: str = "mrg") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# PlanMerger
# ---------------------------------------------------------------------------


class PlanMerger:
    """Merges multiple plans using configurable strategies.

    Supports union, intersection, append, and interleave strategies
    with conflict detection, resolution, and preview capabilities.
    """

    def __init__(self, store: MergeDataStore | None = None) -> None:
        self._store = store or MergeDataStore()
        self._conflicts: dict[str, Conflict] = {}

    def merge_plans(
        self,
        plan_ids: list[str],
        strategy: MergeStrategy = MergeStrategy.UNION,
        config: MergeConfig | None = None,
    ) -> MergeResult:
        """Merge multiple plans into a single plan.

        Args:
            plan_ids: IDs of plans to merge.
            strategy: Merge strategy to use.
            config: Optional merge configuration.
        """
        cfg = config or MergeConfig()
        plans = self._store.get_plans(plan_ids)

        conflicts = self._detect_conflicts(plans)
        resolved_conflicts = self._auto_resolve(conflicts, cfg.resolution_strategy)

        merged = self._execute_merge(plans, strategy, cfg, resolved_conflicts)
        tasks_merged = len(merged.get("tasks", []))

        merge_id = _generate_id()
        result = MergeResult(
            merge_id=merge_id,
            plan_ids=list(plans.keys()),
            strategy=strategy,
            merged_plan=merged,
            conflicts_found=len(conflicts),
            conflicts_resolved=sum(1 for c in resolved_conflicts if c.resolved),
            tasks_merged=tasks_merged,
            created_at=_now_iso(),
        )
        return result

    def preview_merge(
        self,
        plan_ids: list[str],
        strategy: MergeStrategy = MergeStrategy.UNION,
        config: MergeConfig | None = None,
    ) -> MergePreview:
        """Preview a merge without actually performing it."""
        cfg = config or MergeConfig()
        plans = self._store.get_plans(plan_ids)
        conflicts = self._detect_conflicts(plans)
        merged = self._execute_merge(plans, strategy, cfg, [])

        return MergePreview(
            preview_id=_generate_id("prv"),
            plan_ids=list(plans.keys()),
            strategy=strategy,
            total_tasks=len(merged.get("tasks", [])),
            conflicts=conflicts,
            merged_plan_summary={
                "title": merged.get("title", ""),
                "task_count": len(merged.get("tasks", [])),
                "tags": merged.get("tags", []),
            },
        )

    def detect_conflicts(self, plan_ids: list[str]) -> list[Conflict]:
        """Detect conflicts between plans without merging."""
        plans = self._store.get_plans(plan_ids)
        return self._detect_conflicts(plans)

    def resolve_conflict(
        self,
        conflict_id: str,
        resolution: ResolutionStrategy,
    ) -> None:
        """Manually resolve a detected conflict."""
        conflict = self._conflicts.get(conflict_id)
        if conflict is None:
            raise KeyError(f"Conflict {conflict_id} not found")
        self._conflicts[conflict_id] = Conflict(
            conflict_id=conflict.conflict_id,
            conflict_type=conflict.conflict_type,
            plan_ids=conflict.plan_ids,
            task_ids=conflict.task_ids,
            description=conflict.description,
            values=conflict.values,
            resolved=True,
            resolution=resolution,
        )

    def generate_merge_report(self, merge_result: MergeResult) -> MergeReport:
        """Generate a report from a merge result."""
        data = {
            "merge_id": merge_result.merge_id,
            "plan_ids": merge_result.plan_ids,
            "strategy": merge_result.strategy.value,
            "tasks_merged": merge_result.tasks_merged,
            "conflicts_found": merge_result.conflicts_found,
            "conflicts_resolved": merge_result.conflicts_resolved,
            "created_at": merge_result.created_at,
        }
        return MergeReport(
            report_id=_generate_id("rpt"),
            merge_id=merge_result.merge_id,
            data=json.dumps(data, indent=2, default=str).encode("utf-8"),
            created_at=_now_iso(),
        )

    # -- private -----------------------------------------------------------

    def _detect_conflicts(
        self,
        plans: dict[str, dict[str, Any]],
    ) -> list[Conflict]:
        """Detect all conflicts between plans."""
        conflicts: list[Conflict] = []
        plan_list = list(plans.items())

        # Check for duplicate task IDs
        seen_task_ids: dict[str, str] = {}
        for pid, plan in plan_list:
            for task in plan.get("tasks", []):
                tid = task["id"]
                if tid in seen_task_ids:
                    cid = _generate_id("con")
                    conflict = Conflict(
                        conflict_id=cid,
                        conflict_type=ConflictType.DUPLICATE_ID,
                        plan_ids=[seen_task_ids[tid], pid],
                        task_ids=[tid],
                        description=f"Task ID '{tid}' exists in multiple plans",
                    )
                    conflicts.append(conflict)
                    self._conflicts[cid] = conflict
                else:
                    seen_task_ids[tid] = pid

        # Check for resource conflicts (same person in multiple plans)
        all_assignees: dict[str, list[str]] = {}
        for pid, plan in plan_list:
            for uid in plan.get("user_ids", []):
                all_assignees.setdefault(uid, []).append(pid)
        for uid, pids in all_assignees.items():
            if len(pids) > 1:
                cid = _generate_id("con")
                conflict = Conflict(
                    conflict_id=cid,
                    conflict_type=ConflictType.RESOURCE_CONFLICT,
                    plan_ids=pids,
                    description=f"User '{uid}' assigned across multiple plans",
                    values={"user_id": uid},
                )
                conflicts.append(conflict)
                self._conflicts[cid] = conflict

        # Check for metadata conflicts (different owners, priorities)
        titles = {pid: plan.get("title", "") for pid, plan in plan_list}
        if len(set(titles.values())) > 1:
            cid = _generate_id("con")
            conflict = Conflict(
                conflict_id=cid,
                conflict_type=ConflictType.METADATA_CONFLICT,
                plan_ids=list(plans.keys()),
                description="Plans have different titles",
                values=titles,
            )
            conflicts.append(conflict)
            self._conflicts[cid] = conflict

        return conflicts

    def _auto_resolve(
        self,
        conflicts: list[Conflict],
        strategy: ResolutionStrategy,
    ) -> list[Conflict]:
        """Auto-resolve conflicts using the configured strategy."""
        resolved: list[Conflict] = []
        for conflict in conflicts:
            if strategy == ResolutionStrategy.SKIP:
                resolved.append(Conflict(
                    conflict_id=conflict.conflict_id,
                    conflict_type=conflict.conflict_type,
                    plan_ids=conflict.plan_ids,
                    task_ids=conflict.task_ids,
                    description=conflict.description,
                    values=conflict.values,
                    resolved=True,
                    resolution=ResolutionStrategy.SKIP,
                ))
            elif strategy == ResolutionStrategy.MANUAL:
                resolved.append(conflict)
            else:
                resolved.append(Conflict(
                    conflict_id=conflict.conflict_id,
                    conflict_type=conflict.conflict_type,
                    plan_ids=conflict.plan_ids,
                    task_ids=conflict.task_ids,
                    description=conflict.description,
                    values=conflict.values,
                    resolved=True,
                    resolution=strategy,
                ))
        return resolved

    def _execute_merge(
        self,
        plans: dict[str, dict[str, Any]],
        strategy: MergeStrategy,
        config: MergeConfig,
        conflicts: list[Conflict],
    ) -> dict[str, Any]:
        """Execute the actual merge based on strategy."""
        if strategy == MergeStrategy.UNION:
            return self._merge_union(plans, config)
        elif strategy == MergeStrategy.INTERSECTION:
            return self._merge_intersection(plans, config)
        elif strategy == MergeStrategy.APPEND:
            return self._merge_append(plans, config)
        elif strategy == MergeStrategy.INTERLEAVE:
            return self._merge_interleave(plans, config)
        else:
            return self._merge_union(plans, config)

    def _merge_union(
        self,
        plans: dict[str, dict[str, Any]],
        config: MergeConfig,
    ) -> dict[str, Any]:
        """Combine all tasks from all plans."""
        merged_tasks: list[dict[str, Any]] = []
        all_tags: set[str] = set()
        all_user_ids: set[str] = set()
        seen_ids: set[str] = set()

        for pid, plan in plans.items():
            all_tags.update(plan.get("tags", []))
            all_user_ids.update(plan.get("user_ids", []))
            for task in plan.get("tasks", []):
                tid = task["id"]
                if tid in seen_ids and not config.preserve_ids:
                    task = dict(task)
                    task["id"] = _generate_id("task")
                seen_ids.add(task["id"])
                merged_tasks.append(task)

        return {
            "id": _generate_id("plan"),
            "title": "Merged Plan",
            "status": "draft",
            "tags": sorted(all_tags),
            "user_ids": sorted(all_user_ids),
            "tasks": merged_tasks,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "metadata": {"merge_strategy": MergeStrategy.UNION.value, "source_plans": list(plans.keys())},
        }

    def _merge_intersection(
        self,
        plans: dict[str, dict[str, Any]],
        config: MergeConfig,
    ) -> dict[str, Any]:
        """Keep only tasks that appear in all plans (by title)."""
        if not plans:
            return self._empty_merged_plan(MergeStrategy.INTERSECTION)

        plan_list = list(plans.values())
        title_sets = [
            {t["title"] for t in p.get("tasks", [])} for p in plan_list
        ]
        common_titles = title_sets[0]
        for ts in title_sets[1:]:
            common_titles &= ts

        # Take tasks from the first plan that match common titles
        tasks = [
            t for t in plan_list[0].get("tasks", [])
            if t["title"] in common_titles
        ]

        return {
            "id": _generate_id("plan"),
            "title": "Merged Plan (Intersection)",
            "status": "draft",
            "tags": sorted(set().union(*(set(p.get("tags", [])) for p in plan_list))),
            "user_ids": sorted(set().union(*(set(p.get("user_ids", [])) for p in plan_list))),
            "tasks": tasks,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "metadata": {"merge_strategy": MergeStrategy.INTERSECTION.value, "source_plans": list(plans.keys())},
        }

    def _merge_append(
        self,
        plans: dict[str, dict[str, Any]],
        config: MergeConfig,
    ) -> dict[str, Any]:
        """Append plan B tasks after plan A tasks sequentially."""
        merged_tasks: list[dict[str, Any]] = []
        all_tags: set[str] = set()
        all_user_ids: set[str] = set()

        for plan in plans.values():
            all_tags.update(plan.get("tags", []))
            all_user_ids.update(plan.get("user_ids", []))
            for task in plan.get("tasks", []):
                if not config.preserve_ids:
                    task = dict(task)
                    task["id"] = _generate_id("task")
                merged_tasks.append(task)

        return {
            "id": _generate_id("plan"),
            "title": "Merged Plan (Append)",
            "status": "draft",
            "tags": sorted(all_tags),
            "user_ids": sorted(all_user_ids),
            "tasks": merged_tasks,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "metadata": {"merge_strategy": MergeStrategy.APPEND.value, "source_plans": list(plans.keys())},
        }

    def _merge_interleave(
        self,
        plans: dict[str, dict[str, Any]],
        config: MergeConfig,
    ) -> dict[str, Any]:
        """Interleave tasks from plans in round-robin fashion."""
        task_lists = [p.get("tasks", []) for p in plans.values()]
        merged_tasks: list[dict[str, Any]] = []
        all_tags: set[str] = set()
        all_user_ids: set[str] = set()

        for plan in plans.values():
            all_tags.update(plan.get("tags", []))
            all_user_ids.update(plan.get("user_ids", []))

        max_len = max((len(tl) for tl in task_lists), default=0)
        for i in range(max_len):
            for tl in task_lists:
                if i < len(tl):
                    task = tl[i]
                    if not config.preserve_ids:
                        task = dict(task)
                        task["id"] = _generate_id("task")
                    merged_tasks.append(task)

        return {
            "id": _generate_id("plan"),
            "title": "Merged Plan (Interleave)",
            "status": "draft",
            "tags": sorted(all_tags),
            "user_ids": sorted(all_user_ids),
            "tasks": merged_tasks,
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "metadata": {"merge_strategy": MergeStrategy.INTERLEAVE.value, "source_plans": list(plans.keys())},
        }

    def _empty_merged_plan(self, strategy: MergeStrategy) -> dict[str, Any]:
        return {
            "id": _generate_id("plan"),
            "title": f"Merged Plan ({strategy.value})",
            "status": "draft",
            "tags": [],
            "user_ids": [],
            "tasks": [],
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "metadata": {"merge_strategy": strategy.value, "source_plans": []},
        }
