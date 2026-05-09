"""Optimize execution plan task sequencing for minimal duration.

Uses the Critical Path Method (CPM) to compute optimal scheduling,
supports resource constraints, and generates alternative sequencing
options with trade-off analysis.
"""

from __future__ import annotations

import heapq
from dataclasses import dataclass, field
from typing import Any, Literal


# ---------------------------------------------------------------------------
# Data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TaskNode:
    """A single task in the dependency graph with CPM timing attributes."""

    id: str
    title: str
    duration: float
    dependencies: list[str] = field(default_factory=list)
    skills: list[str] = field(default_factory=list)
    earliest_start: float = 0.0
    earliest_finish: float = 0.0
    latest_start: float = 0.0
    latest_finish: float = 0.0
    slack: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "id": self.id,
            "title": self.title,
            "duration": self.duration,
            "dependencies": self.dependencies,
            "skills": self.skills,
            "earliest_start": self.earliest_start,
            "earliest_finish": self.earliest_finish,
            "latest_start": self.latest_start,
            "latest_finish": self.latest_finish,
            "slack": self.slack,
        }


@dataclass(frozen=True, slots=True)
class ResourceConstraints:
    """Resource limits for scheduling."""

    max_parallel_tasks: int = 4
    available_skills: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "max_parallel_tasks": self.max_parallel_tasks,
            "available_skills": self.available_skills,
        }


@dataclass(frozen=True, slots=True)
class OptimizerConfig:
    """Configuration for the plan optimizer."""

    strategy: Literal["minimize_duration", "balance_load", "minimize_cost"] = (
        "minimize_duration"
    )
    max_alternatives: int = 3

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "strategy": self.strategy,
            "max_alternatives": self.max_alternatives,
        }


@dataclass(frozen=True, slots=True)
class ScheduledTask:
    """A task with its computed schedule placement."""

    task_id: str
    start_time: float
    end_time: float
    is_critical: bool
    slack: float

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "task_id": self.task_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "is_critical": self.is_critical,
            "slack": self.slack,
        }


@dataclass(frozen=True, slots=True)
class OptimizationResult:
    """Complete result of an optimization run."""

    scheduled_tasks: list[ScheduledTask] = field(default_factory=list)
    critical_path: list[str] = field(default_factory=list)
    total_duration: float = 0.0
    resource_utilization: float = 0.0
    alternatives: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "scheduled_tasks": [t.to_dict() for t in self.scheduled_tasks],
            "critical_path": self.critical_path,
            "total_duration": self.total_duration,
            "resource_utilization": self.resource_utilization,
            "alternatives": self.alternatives,
        }


@dataclass(frozen=True, slots=True)
class WhatIfResult:
    """Result of a what-if scenario analysis."""

    original_duration: float
    modified_duration: float
    duration_delta: float
    affected_tasks: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable representation."""
        return {
            "original_duration": self.original_duration,
            "modified_duration": self.modified_duration,
            "duration_delta": self.duration_delta,
            "affected_tasks": self.affected_tasks,
        }


# ---------------------------------------------------------------------------
# PlanOptimizer
# ---------------------------------------------------------------------------


class PlanOptimizer:
    """Optimizes execution plan task sequencing for minimal duration.

    Uses the Critical Path Method (CPM) for unconstrained scheduling and
    list-scheduling with a priority queue for resource-constrained scenarios.
    """

    def __init__(self, config: OptimizerConfig | None = None) -> None:
        self._config = config or OptimizerConfig()

    # -- public API ---------------------------------------------------------

    def optimize(
        self,
        tasks: list[dict[str, Any]],
        constraints: ResourceConstraints | None = None,
    ) -> OptimizationResult:
        """Compute an optimal schedule for *tasks*.

        Parameters
        ----------
        tasks:
            Each dict must contain ``id``, ``title``, ``duration``, and may
            contain ``dependencies`` (list of task-id strings) and ``skills``
            (list of skill strings).
        constraints:
            Optional resource constraints (parallelism cap, skill pool).

        Returns
        -------
        OptimizationResult
            Scheduled tasks, critical path, total duration, utilization, and
            alternative sequencing options.

        Raises
        ------
        ValueError
            If circular dependencies are detected.
        """
        if not tasks:
            return OptimizationResult()

        nodes = _build_nodes(tasks)
        nodes_by_id = {n.id: n for n in nodes}
        dep_map = {n.id: list(n.dependencies) for n in nodes}

        _detect_cycle(dep_map)

        topo_order = _topological_sort(dep_map)

        # Forward pass --------------------------------------------------
        earliest: dict[str, tuple[float, float]] = {}
        for tid in topo_order:
            node = nodes_by_id[tid]
            es = max(
                (earliest[d][1] for d in node.dependencies if d in earliest),
                default=0.0,
            )
            earliest[tid] = (es, es + node.duration)

        project_end = max(ef for _, ef in earliest.values()) if earliest else 0.0

        # Backward pass -------------------------------------------------
        dependents = _dependents_map(dep_map)
        latest: dict[str, tuple[float, float]] = {}
        for tid in reversed(topo_order):
            node = nodes_by_id[tid]
            lf = min(
                (latest[s][0] for s in dependents.get(tid, []) if s in latest),
                default=project_end,
            )
            latest[tid] = (lf - node.duration, lf)

        # Annotate nodes with timing -----------------------------------
        annotated: dict[str, TaskNode] = {}
        for tid in topo_order:
            node = nodes_by_id[tid]
            es, ef = earliest[tid]
            ls, lf = latest[tid]
            slack = ls - es
            annotated[tid] = TaskNode(
                id=node.id,
                title=node.title,
                duration=node.duration,
                dependencies=node.dependencies,
                skills=node.skills,
                earliest_start=es,
                earliest_finish=ef,
                latest_start=ls,
                latest_finish=lf,
                slack=round(slack, 10),
            )

        critical_ids = [tid for tid in topo_order if annotated[tid].slack == 0.0]

        # Resource-constrained scheduling -------------------------------
        effective_constraints = constraints or ResourceConstraints()
        scheduled = _resource_constrained_schedule(
            topo_order, annotated, dep_map, effective_constraints
        )

        total_duration = (
            max(s.end_time for s in scheduled) if scheduled else 0.0
        )
        utilization = _compute_utilization(
            scheduled, total_duration, effective_constraints.max_parallel_tasks
        )

        alternatives = self.generate_alternatives(
            tasks, constraints, limit=self._config.max_alternatives
        )

        return OptimizationResult(
            scheduled_tasks=scheduled,
            critical_path=critical_ids,
            total_duration=total_duration,
            resource_utilization=utilization,
            alternatives=alternatives,
        )

    def generate_alternatives(
        self,
        tasks: list[dict[str, Any]],
        constraints: ResourceConstraints | None = None,
        *,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Generate alternative sequencing options with trade-off analysis.

        Creates variants by adjusting resource constraints to explore the
        parallelism/duration trade-off space.
        """
        if not tasks:
            return []

        max_alts = limit if limit is not None else self._config.max_alternatives
        base_constraints = constraints or ResourceConstraints()
        alternatives: list[dict[str, Any]] = []

        candidate_limits = _alternative_parallelism_levels(
            base_constraints.max_parallel_tasks, max_alts
        )

        for par_limit in candidate_limits:
            alt_constraints = ResourceConstraints(
                max_parallel_tasks=par_limit,
                available_skills=base_constraints.available_skills,
            )
            # Compute a schedule under the alternative constraints without
            # recursing into generate_alternatives again.
            nodes = _build_nodes(tasks)
            nodes_by_id = {n.id: n for n in nodes}
            dep_map = {n.id: list(n.dependencies) for n in nodes}
            topo_order = _topological_sort(dep_map)

            earliest: dict[str, tuple[float, float]] = {}
            for tid in topo_order:
                node = nodes_by_id[tid]
                es = max(
                    (earliest[d][1] for d in node.dependencies if d in earliest),
                    default=0.0,
                )
                earliest[tid] = (es, es + node.duration)

            project_end = max(ef for _, ef in earliest.values()) if earliest else 0.0
            dependents = _dependents_map(dep_map)

            latest: dict[str, tuple[float, float]] = {}
            for tid in reversed(topo_order):
                node = nodes_by_id[tid]
                lf = min(
                    (latest[s][0] for s in dependents.get(tid, []) if s in latest),
                    default=project_end,
                )
                latest[tid] = (lf - node.duration, lf)

            annotated: dict[str, TaskNode] = {}
            for tid in topo_order:
                node = nodes_by_id[tid]
                es, ef = earliest[tid]
                ls, lf = latest[tid]
                annotated[tid] = TaskNode(
                    id=node.id,
                    title=node.title,
                    duration=node.duration,
                    dependencies=node.dependencies,
                    skills=node.skills,
                    earliest_start=es,
                    earliest_finish=ef,
                    latest_start=ls,
                    latest_finish=lf,
                    slack=round(ls - es, 10),
                )

            scheduled = _resource_constrained_schedule(
                topo_order, annotated, dep_map, alt_constraints
            )
            alt_duration = max(s.end_time for s in scheduled) if scheduled else 0.0
            alt_util = _compute_utilization(scheduled, alt_duration, par_limit)

            alternatives.append(
                {
                    "max_parallel_tasks": par_limit,
                    "total_duration": alt_duration,
                    "resource_utilization": alt_util,
                    "trade_off": (
                        "higher parallelism, shorter duration"
                        if par_limit > base_constraints.max_parallel_tasks
                        else "lower parallelism, longer duration"
                    ),
                }
            )

        return alternatives

    def what_if(
        self,
        tasks: list[dict[str, Any]],
        scenario: dict[str, Any],
    ) -> WhatIfResult:
        """Evaluate the impact of a hypothetical change.

        Supported scenario keys:

        * ``add_tasks`` -- list of task dicts to insert.
        * ``remove_tasks`` -- list of task-id strings to remove.
        * ``change_durations`` -- dict mapping task-id to new duration.
        """
        original = self.optimize(tasks)

        modified_tasks = list(tasks)

        # Remove tasks ---------------------------------------------------
        remove_ids = set(_string_list(scenario.get("remove_tasks")))
        if remove_ids:
            modified_tasks = [
                t for t in modified_tasks if str(t.get("id", "")) not in remove_ids
            ]
            # Strip removed ids from remaining dependency lists.
            modified_tasks = [
                (
                    {**t, "dependencies": [d for d in t["dependencies"] if str(d) not in remove_ids]}
                    if isinstance(t.get("dependencies"), list)
                    else t
                )
                for t in modified_tasks
            ]

        # Add tasks ------------------------------------------------------
        add_tasks = scenario.get("add_tasks")
        if isinstance(add_tasks, list):
            modified_tasks.extend(add_tasks)

        # Change durations -----------------------------------------------
        change_durations = scenario.get("change_durations")
        if isinstance(change_durations, dict):
            by_id = {str(t.get("id", "")): i for i, t in enumerate(modified_tasks)}
            for tid, new_dur in change_durations.items():
                idx = by_id.get(str(tid))
                if idx is not None:
                    modified_tasks[idx] = {**modified_tasks[idx], "duration": float(new_dur)}

        modified = self.optimize(modified_tasks)

        # Determine affected tasks (those whose start/end time changed).
        orig_schedule = {s.task_id: s for s in original.scheduled_tasks}
        mod_schedule = {s.task_id: s for s in modified.scheduled_tasks}
        affected: list[str] = []
        all_ids = dict.fromkeys(
            list(orig_schedule.keys()) + list(mod_schedule.keys())
        )
        for tid in all_ids:
            o = orig_schedule.get(tid)
            m = mod_schedule.get(tid)
            if o is None or m is None:
                affected.append(tid)
            elif o.start_time != m.start_time or o.end_time != m.end_time:
                affected.append(tid)

        return WhatIfResult(
            original_duration=original.total_duration,
            modified_duration=modified.total_duration,
            duration_delta=modified.total_duration - original.total_duration,
            affected_tasks=affected,
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _build_nodes(tasks: list[dict[str, Any]]) -> list[TaskNode]:
    """Convert raw task dicts into ``TaskNode`` instances."""
    nodes: list[TaskNode] = []
    for task in tasks:
        tid = str(task.get("id", ""))
        if not tid:
            continue
        nodes.append(
            TaskNode(
                id=tid,
                title=str(task.get("title", "Untitled")),
                duration=float(task.get("duration", 0)),
                dependencies=_string_list(task.get("dependencies")),
                skills=_string_list(task.get("skills")),
            )
        )
    return nodes


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if item]


def _topological_sort(dep_map: dict[str, list[str]]) -> list[str]:
    """Kahn's algorithm -- returns a deterministic topological ordering.

    Tasks at the same depth are sorted by id for determinism.
    """
    in_degree: dict[str, int] = {
        tid: sum(1 for d in deps if d in dep_map)
        for tid, deps in dep_map.items()
    }

    ready: list[str] = sorted(
        tid for tid, deg in in_degree.items() if deg == 0
    )
    order: list[str] = []

    while ready:
        tid = ready.pop(0)
        order.append(tid)
        for other_tid, deps in dep_map.items():
            if tid in deps and other_tid not in order:
                in_degree[other_tid] -= 1
                if in_degree[other_tid] == 0:
                    # Insert in sorted position for determinism.
                    _sorted_insert(ready, other_tid)

    return order


def _sorted_insert(lst: list[str], value: str) -> None:
    """Insert *value* into an already-sorted list maintaining order."""
    lo, hi = 0, len(lst)
    while lo < hi:
        mid = (lo + hi) // 2
        if lst[mid] < value:
            lo = mid + 1
        else:
            hi = mid
    lst.insert(lo, value)


def _dependents_map(dep_map: dict[str, list[str]]) -> dict[str, list[str]]:
    """Build a reverse adjacency: task -> list of dependents."""
    dependents: dict[str, list[str]] = {}
    for tid, deps in dep_map.items():
        for d in deps:
            dependents.setdefault(d, []).append(tid)
    return dependents


def _detect_cycle(dep_map: dict[str, list[str]]) -> None:
    """Raise ``ValueError`` if the dependency graph contains a cycle."""
    state: dict[str, int] = {}  # 0=unvisited, 1=visiting, 2=visited
    path: list[str] = []

    def _visit(tid: str) -> None:
        state[tid] = 1
        path.append(tid)
        for dep in dep_map.get(tid, []):
            if dep not in dep_map:
                continue
            dep_state = state.get(dep, 0)
            if dep_state == 1:
                cycle_start = path.index(dep)
                cycle = path[cycle_start:] + [dep]
                raise ValueError(
                    f"Circular dependency detected: {' -> '.join(cycle)}"
                )
            if dep_state == 0:
                _visit(dep)
        path.pop()
        state[tid] = 2

    for tid in dep_map:
        if state.get(tid, 0) == 0:
            _visit(tid)


def _resource_constrained_schedule(
    topo_order: list[str],
    annotated: dict[str, TaskNode],
    dep_map: dict[str, list[str]],
    constraints: ResourceConstraints,
) -> list[ScheduledTask]:
    """List-scheduling algorithm with a priority queue.

    Priority: critical tasks first (lowest slack), then by earliest start.
    """
    if not topo_order:
        return []

    max_parallel = max(constraints.max_parallel_tasks, 1)
    available_skills = set(constraints.available_skills) if constraints.available_skills else None

    # Priority for each task: (slack, earliest_start, id) -- lower is higher priority.
    remaining = set(topo_order)
    scheduled_map: dict[str, ScheduledTask] = {}

    # Event-driven simulation: maintain a time cursor and a set of running tasks.
    running: list[tuple[float, str]] = []  # heap of (end_time, task_id)
    time = 0.0

    def _ready_tasks() -> list[str]:
        """Return unscheduled tasks whose dependencies are all finished."""
        ready = []
        for tid in topo_order:
            if tid not in remaining:
                continue
            deps = dep_map.get(tid, [])
            if all(d in scheduled_map for d in deps if d in annotated):
                ready.append(tid)
        return ready

    while remaining:
        # Advance time to the earliest finishing task if at capacity.
        if len(running) >= max_parallel and running:
            end_time, _finished_id = heapq.heappop(running)
            time = max(time, end_time)
            # Drain all tasks finishing at the same time.
            while running and running[0][0] <= time:
                heapq.heappop(running)

        ready = _ready_tasks()
        if not ready:
            if running:
                # Advance time to next completion.
                end_time, _ = heapq.heappop(running)
                time = max(time, end_time)
                while running and running[0][0] <= time:
                    heapq.heappop(running)
                continue
            # No ready tasks and nothing running -- remaining tasks are
            # unreachable (should not happen after cycle detection).
            break

        # Sort ready tasks by priority: lowest slack first, then earliest
        # start, then id for determinism.
        ready.sort(key=lambda tid: (annotated[tid].slack, annotated[tid].earliest_start, tid))

        prev_remaining = len(remaining)

        for tid in ready:
            if len(running) >= max_parallel:
                break

            node = annotated[tid]

            # Skill check: if constraints specify available skills and the
            # task requires skills, ensure at least one is available.
            if available_skills and node.skills:
                if not any(s in available_skills for s in node.skills):
                    continue

            # Earliest this task can actually start given dependency finishes.
            dep_finish = max(
                (scheduled_map[d].end_time for d in node.dependencies if d in scheduled_map),
                default=0.0,
            )
            start = max(time, dep_finish)
            end = start + node.duration

            st = ScheduledTask(
                task_id=tid,
                start_time=start,
                end_time=end,
                is_critical=node.slack == 0.0,
                slack=node.slack,
            )
            scheduled_map[tid] = st
            remaining.discard(tid)
            heapq.heappush(running, (end, tid))

        # No progress was made this iteration.
        if len(remaining) == prev_remaining:
            if not running:
                # Nothing running and no tasks were scheduled — remaining
                # tasks are unschedulable (e.g. skill constraints).
                break
            # Advance time past the next finishing task so the outer loop
            # can re-evaluate readiness.
            end_time, _ = heapq.heappop(running)
            time = max(time, end_time)
            while running and running[0][0] <= time:
                heapq.heappop(running)

    return [scheduled_map[tid] for tid in topo_order if tid in scheduled_map]


def _compute_utilization(
    scheduled: list[ScheduledTask],
    total_duration: float,
    max_parallel_tasks: int,
) -> float:
    """Compute resource utilization as sum(durations) / (slots * makespan).

    Returns a value between 0.0 and 1.0.  If there is nothing to schedule,
    returns 0.0.
    """
    if not scheduled or total_duration <= 0:
        return 0.0
    total_work = sum(s.end_time - s.start_time for s in scheduled)
    capacity = total_duration * max(max_parallel_tasks, 1)
    return round(min(total_work / capacity, 1.0), 4)


def _alternative_parallelism_levels(
    base: int, max_alternatives: int
) -> list[int]:
    """Return a set of parallelism levels to explore as alternatives.

    Always produces levels different from *base*.
    """
    candidates: list[int] = []
    # Lower parallelism levels.
    for p in range(max(base - 1, 1), 0, -1):
        if p != base:
            candidates.append(p)
            if len(candidates) >= max_alternatives:
                return candidates[:max_alternatives]

    # Higher parallelism levels.
    for p in range(base + 1, base + max_alternatives + 1):
        if p != base:
            candidates.append(p)
            if len(candidates) >= max_alternatives:
                break

    return candidates[:max_alternatives]


__all__ = [
    "OptimizerConfig",
    "OptimizationResult",
    "PlanOptimizer",
    "ResourceConstraints",
    "ScheduledTask",
    "TaskNode",
    "WhatIfResult",
]
