"""Plan split functionality for decomposing large plans.

Supports splitting by phase, team, dependency clusters, timeline,
tags, and assignee with smart splitting that minimizes cross-plan
dependencies.
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


class SplitStrategy(str, Enum):
    """Strategy for splitting plans."""

    BY_PHASE = "by_phase"
    BY_TEAM = "by_team"
    BY_DEPENDENCY_CLUSTER = "by_dependency_cluster"
    BY_TIMELINE = "by_timeline"
    BY_TAGS = "by_tags"
    BY_ASSIGNEE = "by_assignee"
    CUSTOM = "custom"


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SplitConfig:
    """Configuration for a split operation."""

    preserve_ids: bool = True
    create_hierarchy: bool = False
    duplicate_shared_tasks: bool = False
    balance_sizes: bool = True
    phases: list[str] | None = None
    timeline_period: str = "monthly"
    custom_filter: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class SplitSuggestion:
    """A suggested split point in a plan."""

    suggestion_id: str
    description: str
    strategy: SplitStrategy
    split_point: str
    tasks_before: int = 0
    tasks_after: int = 0
    cross_dependencies: int = 0
    score: float = 0.0


@dataclass(frozen=True, slots=True)
class ExternalDependency:
    """A dependency link between split plans."""

    source_plan_id: str
    source_task_id: str
    target_plan_id: str
    target_task_id: str


@dataclass(frozen=True, slots=True)
class SplitPreview:
    """Preview of a split result."""

    preview_id: str
    plan_id: str
    strategy: SplitStrategy
    result_count: int = 0
    plan_summaries: list[dict[str, Any]] = field(default_factory=list)
    cross_dependencies: list[ExternalDependency] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Result of validating a split configuration."""

    valid: bool
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class SplitResult:
    """Result of a completed split operation."""

    split_id: str
    source_plan_id: str
    strategy: SplitStrategy
    plans: list[dict[str, Any]] = field(default_factory=list)
    cross_dependencies: list[ExternalDependency] = field(default_factory=list)
    created_at: str = ""


@dataclass(frozen=True, slots=True)
class SplitReport:
    """Report on a split operation."""

    report_id: str
    split_id: str
    data: bytes = b""
    created_at: str = ""


# ---------------------------------------------------------------------------
# Data store
# ---------------------------------------------------------------------------


@dataclass
class SplitDataStore:
    """In-memory store providing plan data for split operations."""

    plans: dict[str, dict[str, Any]] = field(default_factory=dict)
    tasks: dict[str, dict[str, Any]] = field(default_factory=dict)

    def get_plan(self, plan_id: str) -> dict[str, Any] | None:
        return self.plans.get(plan_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _generate_id(prefix: str = "spl") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _build_plan(
    title: str,
    tasks: list[dict[str, Any]],
    source_plan: dict[str, Any],
    strategy: SplitStrategy,
) -> dict[str, Any]:
    """Create a new plan from a subset of tasks."""
    task_ids = {t["id"] for t in tasks}
    # Filter dependencies to only include tasks within this plan
    adjusted_tasks = []
    for t in tasks:
        t_copy = dict(t)
        t_copy["depends_on"] = [
            d for d in t.get("depends_on", []) if d in task_ids
        ]
        adjusted_tasks.append(t_copy)

    tags = set()
    for t in tasks:
        tags.update(t.get("tags", []))
    tags.update(source_plan.get("tags", []))

    user_ids = set()
    for t in tasks:
        if t.get("assignee"):
            user_ids.add(t["assignee"])

    return {
        "id": _generate_id("plan"),
        "title": title,
        "status": "draft",
        "tags": sorted(tags),
        "user_ids": sorted(user_ids),
        "tasks": adjusted_tasks,
        "created_at": _now_iso(),
        "updated_at": _now_iso(),
        "metadata": {
            "split_strategy": strategy.value,
            "source_plan_id": source_plan.get("id", ""),
        },
    }


def _find_cross_deps(
    plans: list[dict[str, Any]],
) -> list[ExternalDependency]:
    """Find dependencies that cross plan boundaries."""
    task_to_plan: dict[str, str] = {}
    for plan in plans:
        for task in plan.get("tasks", []):
            task_to_plan[task["id"]] = plan["id"]

    cross: list[ExternalDependency] = []
    for plan in plans:
        for task in plan.get("tasks", []):
            # Check original depends_on (before filtering)
            for dep_id in task.get("_original_depends_on", task.get("depends_on", [])):
                if dep_id in task_to_plan and task_to_plan[dep_id] != plan["id"]:
                    cross.append(ExternalDependency(
                        source_plan_id=plan["id"],
                        source_task_id=task["id"],
                        target_plan_id=task_to_plan[dep_id],
                        target_task_id=dep_id,
                    ))
    return cross


# ---------------------------------------------------------------------------
# PlanSplitter
# ---------------------------------------------------------------------------


class PlanSplitter:
    """Splits large plans into smaller focused plans.

    Supports splitting by phase, team, dependency clusters, timeline,
    tags, and assignee with dependency analysis and split suggestions.
    """

    def __init__(self, store: SplitDataStore | None = None) -> None:
        self._store = store or SplitDataStore()

    def split_plan(
        self,
        plan_id: str,
        strategy: SplitStrategy = SplitStrategy.BY_TAGS,
        config: SplitConfig | None = None,
    ) -> list[dict[str, Any]]:
        """Split a plan into multiple plans.

        Args:
            plan_id: ID of the plan to split.
            strategy: Strategy for splitting.
            config: Optional split configuration.
        """
        cfg = config or SplitConfig()
        plan = self._store.get_plan(plan_id)
        if plan is None:
            return []

        if strategy == SplitStrategy.BY_TAGS:
            return self._split_by_tags(plan, cfg)
        elif strategy == SplitStrategy.BY_ASSIGNEE:
            return self._split_by_assignee(plan, cfg)
        elif strategy == SplitStrategy.BY_DEPENDENCY_CLUSTER:
            return self._split_by_dependency_cluster(plan, cfg)
        elif strategy == SplitStrategy.BY_PHASE:
            return self._split_by_phase(plan, cfg)
        elif strategy == SplitStrategy.BY_TEAM:
            return self._split_by_team(plan, cfg)
        elif strategy == SplitStrategy.BY_TIMELINE:
            return self._split_by_timeline(plan, cfg)
        else:
            return [dict(plan)]

    def preview_split(
        self,
        plan_id: str,
        strategy: SplitStrategy = SplitStrategy.BY_TAGS,
        config: SplitConfig | None = None,
    ) -> SplitPreview:
        """Preview a split without modifying data."""
        plans = self.split_plan(plan_id, strategy, config)
        cross_deps = _find_cross_deps(plans)
        summaries = [
            {
                "plan_id": p["id"],
                "title": p["title"],
                "task_count": len(p.get("tasks", [])),
                "tags": p.get("tags", []),
            }
            for p in plans
        ]

        return SplitPreview(
            preview_id=_generate_id("prv"),
            plan_id=plan_id,
            strategy=strategy,
            result_count=len(plans),
            plan_summaries=summaries,
            cross_dependencies=cross_deps,
        )

    def suggest_split_points(self, plan_id: str) -> list[SplitSuggestion]:
        """Suggest natural split points for a plan."""
        plan = self._store.get_plan(plan_id)
        if plan is None:
            return []

        suggestions: list[SplitSuggestion] = []
        tasks = plan.get("tasks", [])

        # Suggest by tags if tasks have different tags
        all_tags: set[str] = set()
        for t in tasks:
            all_tags.update(t.get("tags", []))
        if len(all_tags) > 1:
            suggestions.append(SplitSuggestion(
                suggestion_id=_generate_id("sug"),
                description=f"Split by tags: {', '.join(sorted(all_tags))}",
                strategy=SplitStrategy.BY_TAGS,
                split_point="tags",
                tasks_before=len(tasks),
                tasks_after=len(tasks),
                score=0.8,
            ))

        # Suggest by assignee if multiple assignees
        assignees: set[str] = set()
        for t in tasks:
            if t.get("assignee"):
                assignees.add(t["assignee"])
        if len(assignees) > 1:
            suggestions.append(SplitSuggestion(
                suggestion_id=_generate_id("sug"),
                description=f"Split by assignee: {', '.join(sorted(assignees))}",
                strategy=SplitStrategy.BY_ASSIGNEE,
                split_point="assignee",
                tasks_before=len(tasks),
                tasks_after=len(tasks),
                score=0.7,
            ))

        # Suggest by dependency clusters
        clusters = self._find_clusters(tasks)
        if len(clusters) > 1:
            suggestions.append(SplitSuggestion(
                suggestion_id=_generate_id("sug"),
                description=f"Split into {len(clusters)} dependency clusters",
                strategy=SplitStrategy.BY_DEPENDENCY_CLUSTER,
                split_point="dependency_clusters",
                tasks_before=len(tasks),
                tasks_after=len(tasks),
                cross_dependencies=0,
                score=0.9,
            ))

        return sorted(suggestions, key=lambda s: s.score, reverse=True)

    def validate_split(self, plan_id: str, config: SplitConfig) -> ValidationResult:
        """Validate a split configuration."""
        errors: list[str] = []
        warnings: list[str] = []

        plan = self._store.get_plan(plan_id)
        if plan is None:
            errors.append(f"Plan {plan_id} not found")
            return ValidationResult(valid=False, errors=errors)

        tasks = plan.get("tasks", [])
        if len(tasks) < 2:
            warnings.append("Plan has fewer than 2 tasks; splitting may not be useful")

        return ValidationResult(valid=len(errors) == 0, errors=errors, warnings=warnings)

    def generate_split_report(self, split_result: SplitResult) -> SplitReport:
        """Generate a report from a split result."""
        data = {
            "split_id": split_result.split_id,
            "source_plan_id": split_result.source_plan_id,
            "strategy": split_result.strategy.value,
            "plans_created": len(split_result.plans),
            "cross_dependencies": len(split_result.cross_dependencies),
            "created_at": split_result.created_at,
        }
        return SplitReport(
            report_id=_generate_id("rpt"),
            split_id=split_result.split_id,
            data=json.dumps(data, indent=2).encode("utf-8"),
            created_at=_now_iso(),
        )

    # -- private split strategies ------------------------------------------

    def _split_by_tags(
        self,
        plan: dict[str, Any],
        config: SplitConfig,
    ) -> list[dict[str, Any]]:
        """Split by task tags."""
        tasks = plan.get("tasks", [])
        groups: dict[str, list[dict[str, Any]]] = {}
        untagged: list[dict[str, Any]] = []

        for task in tasks:
            task_tags = task.get("tags", [])
            if task_tags:
                key = task_tags[0]  # Use first tag as grouping key
                groups.setdefault(key, []).append(task)
            else:
                untagged.append(task)

        # If no tags found, return original plan
        if not groups:
            return [dict(plan)]

        # Add untagged tasks to the largest group
        if untagged and groups:
            largest = max(groups.keys(), key=lambda k: len(groups[k]))
            groups[largest].extend(untagged)

        result = []
        for tag, group_tasks in groups.items():
            title = f"{plan.get('title', 'Plan')} - {tag}"
            result.append(_build_plan(title, group_tasks, plan, SplitStrategy.BY_TAGS))
        return result

    def _split_by_assignee(
        self,
        plan: dict[str, Any],
        config: SplitConfig,
    ) -> list[dict[str, Any]]:
        """Split by task assignee."""
        tasks = plan.get("tasks", [])
        groups: dict[str, list[dict[str, Any]]] = {}

        for task in tasks:
            assignee = task.get("assignee", "unassigned")
            groups.setdefault(assignee, []).append(task)

        result = []
        for assignee, group_tasks in groups.items():
            title = f"{plan.get('title', 'Plan')} - {assignee}"
            result.append(_build_plan(title, group_tasks, plan, SplitStrategy.BY_ASSIGNEE))
        return result

    def _split_by_dependency_cluster(
        self,
        plan: dict[str, Any],
        config: SplitConfig,
    ) -> list[dict[str, Any]]:
        """Split into independent dependency clusters."""
        tasks = plan.get("tasks", [])
        clusters = self._find_clusters(tasks)

        if len(clusters) <= 1:
            return [dict(plan)]

        result = []
        for i, cluster_tasks in enumerate(clusters):
            title = f"{plan.get('title', 'Plan')} - Cluster {i + 1}"
            result.append(_build_plan(title, cluster_tasks, plan, SplitStrategy.BY_DEPENDENCY_CLUSTER))
        return result

    def _split_by_phase(
        self,
        plan: dict[str, Any],
        config: SplitConfig,
    ) -> list[dict[str, Any]]:
        """Split by phase/milestone markers."""
        tasks = plan.get("tasks", [])
        phases = config.phases or []

        if not phases:
            # Auto-detect phases: split in half
            mid = len(tasks) // 2
            if mid == 0:
                return [dict(plan)]
            phase_groups = [tasks[:mid], tasks[mid:]]
            phase_names = ["Phase 1", "Phase 2"]
        else:
            phase_groups: list[list[dict[str, Any]]] = [[] for _ in phases]
            remainder: list[dict[str, Any]] = []
            for task in tasks:
                placed = False
                for i, phase in enumerate(phases):
                    if phase.lower() in task.get("title", "").lower() or phase in task.get("tags", []):
                        phase_groups[i].append(task)
                        placed = True
                        break
                if not placed:
                    remainder.append(task)
            # Add remainder to last phase
            if remainder and phase_groups:
                phase_groups[-1].extend(remainder)
            phase_names = phases

        result = []
        for name, group_tasks in zip(phase_names, phase_groups):
            if group_tasks:
                title = f"{plan.get('title', 'Plan')} - {name}"
                result.append(_build_plan(title, group_tasks, plan, SplitStrategy.BY_PHASE))
        return result or [dict(plan)]

    def _split_by_team(
        self,
        plan: dict[str, Any],
        config: SplitConfig,
    ) -> list[dict[str, Any]]:
        """Split by team (same as by_assignee but grouped by user_ids)."""
        return self._split_by_assignee(plan, config)

    def _split_by_timeline(
        self,
        plan: dict[str, Any],
        config: SplitConfig,
    ) -> list[dict[str, Any]]:
        """Split into time periods."""
        tasks = plan.get("tasks", [])
        if len(tasks) < 2:
            return [dict(plan)]

        # Split into equal chunks based on task ordering
        chunk_size = max(len(tasks) // 3, 1)
        chunks = [tasks[i:i + chunk_size] for i in range(0, len(tasks), chunk_size)]

        result = []
        for i, chunk in enumerate(chunks):
            title = f"{plan.get('title', 'Plan')} - Period {i + 1}"
            result.append(_build_plan(title, chunk, plan, SplitStrategy.BY_TIMELINE))
        return result

    def _find_clusters(self, tasks: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
        """Find connected components in the dependency graph."""
        if not tasks:
            return []

        task_map = {t["id"]: t for t in tasks}
        visited: set[str] = set()
        clusters: list[list[dict[str, Any]]] = []

        # Build adjacency (undirected)
        adj: dict[str, set[str]] = {t["id"]: set() for t in tasks}
        for t in tasks:
            for dep in t.get("depends_on", []):
                if dep in adj:
                    adj[t["id"]].add(dep)
                    adj[dep].add(t["id"])

        for t in tasks:
            if t["id"] in visited:
                continue
            # BFS from this task
            cluster: list[dict[str, Any]] = []
            queue = [t["id"]]
            while queue:
                tid = queue.pop(0)
                if tid in visited:
                    continue
                visited.add(tid)
                if tid in task_map:
                    cluster.append(task_map[tid])
                for neighbor in adj.get(tid, set()):
                    if neighbor not in visited:
                        queue.append(neighbor)
            if cluster:
                clusters.append(cluster)

        return clusters
