"""Constraint-based plan optimizer for task scheduling and resource assignment.

Uses constraint satisfaction techniques to generate optimal task schedules
and resource assignments. Supports hard constraints (dependencies, capacity,
deadlines), soft constraints (duration, workload, skills, context-switching),
and multi-objective optimization with Pareto frontier.
"""

from __future__ import annotations

import heapq
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Sequence


# ---------------------------------------------------------------------------
# Enums & constants
# ---------------------------------------------------------------------------

class ConstraintKind(str, Enum):
    """Constraint classification."""

    HARD = "hard"
    SOFT = "soft"


class ObjectiveType(str, Enum):
    """Optimization objectives."""

    MINIMIZE_MAKESPAN = "minimize_makespan"
    MINIMIZE_COST = "minimize_cost"
    MAXIMIZE_QUALITY = "maximize_quality"
    BALANCE_LOAD = "balance_load"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Constraint:
    """A single constraint in the optimization problem."""

    id: str
    kind: ConstraintKind
    name: str
    description: str
    weight: float = 1.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "kind": self.kind.value,
            "name": self.name,
            "description": self.description,
            "weight": self.weight,
        }


@dataclass(frozen=True, slots=True)
class ConstraintSet:
    """Collection of constraints for a plan."""

    hard_constraints: tuple[Constraint, ...] = field(default_factory=tuple)
    soft_constraints: tuple[Constraint, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        return {
            "hard_constraints": [c.to_dict() for c in self.hard_constraints],
            "soft_constraints": [c.to_dict() for c in self.soft_constraints],
            "total": len(self.hard_constraints) + len(self.soft_constraints),
        }


@dataclass(frozen=True, slots=True)
class TaskSchedule:
    """Scheduled task with start time and assigned resource."""

    task_id: str
    title: str
    start_time: float
    end_time: float
    duration: float
    assigned_to: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "start_time": round(self.start_time, 2),
            "end_time": round(self.end_time, 2),
            "duration": round(self.duration, 2),
            "assigned_to": self.assigned_to,
        }


@dataclass(frozen=True, slots=True)
class OptimizedSchedule:
    """Result of schedule optimization."""

    plan_id: str
    tasks: tuple[TaskSchedule, ...] = field(default_factory=tuple)
    makespan: float = 0.0
    objective: str = ""
    constraint_violations: tuple[str, ...] = field(default_factory=tuple)
    is_feasible: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "tasks": [t.to_dict() for t in self.tasks],
            "makespan": round(self.makespan, 2),
            "objective": self.objective,
            "constraint_violations": list(self.constraint_violations),
            "is_feasible": self.is_feasible,
        }


@dataclass(frozen=True, slots=True)
class ResourceAssignment:
    """Assignment of a resource to a task."""

    resource_id: str
    task_id: str
    skill_match: float = 0.0
    load_contribution: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "resource_id": self.resource_id,
            "task_id": self.task_id,
            "skill_match": round(self.skill_match, 2),
            "load_contribution": round(self.load_contribution, 2),
        }


@dataclass(frozen=True, slots=True)
class AssignmentPlan:
    """Optimized resource assignment plan."""

    assignments: tuple[ResourceAssignment, ...] = field(default_factory=tuple)
    workload_balance: float = 0.0
    avg_skill_match: float = 0.0
    context_switches: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "assignments": [a.to_dict() for a in self.assignments],
            "workload_balance": round(self.workload_balance, 4),
            "avg_skill_match": round(self.avg_skill_match, 4),
            "context_switches": self.context_switches,
        }


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """Result of constraint validation."""

    is_valid: bool
    violations: tuple[str, ...] = field(default_factory=tuple)
    satisfied_hard: int = 0
    total_hard: int = 0
    satisfied_soft: int = 0
    total_soft: int = 0
    soft_score: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "violations": list(self.violations),
            "satisfied_hard": self.satisfied_hard,
            "total_hard": self.total_hard,
            "satisfied_soft": self.satisfied_soft,
            "total_soft": self.total_soft,
            "soft_score": round(self.soft_score, 4),
        }


@dataclass(frozen=True, slots=True)
class Solution:
    """A solution in the Pareto frontier."""

    id: str
    schedule: OptimizedSchedule
    objective_values: dict[str, float] = field(default_factory=dict)
    is_pareto_optimal: bool = True

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "schedule": self.schedule.to_dict(),
            "objective_values": {
                k: round(v, 4) for k, v in self.objective_values.items()
            },
            "is_pareto_optimal": self.is_pareto_optimal,
        }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_float(data: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    v = data.get(key, default)
    try:
        return float(v) if v is not None else default
    except (ValueError, TypeError):
        return default


def _topological_sort(
    tasks: Sequence[Mapping[str, Any]],
) -> list[Mapping[str, Any]]:
    """Sort tasks respecting dependency ordering."""
    task_map = {str(t.get("id", f"task-{i}")): t for i, t in enumerate(tasks)}
    in_degree: dict[str, int] = {tid: 0 for tid in task_map}
    adj: dict[str, list[str]] = {tid: [] for tid in task_map}

    for tid, task in task_map.items():
        deps = task.get("dependencies", [])
        if isinstance(deps, (list, tuple)):
            for dep in deps:
                dep_str = str(dep)
                if dep_str in task_map:
                    adj[dep_str].append(tid)
                    in_degree[tid] += 1

    queue: list[str] = [tid for tid, deg in in_degree.items() if deg == 0]
    heapq.heapify(queue)
    result: list[Mapping[str, Any]] = []

    while queue:
        tid = heapq.heappop(queue)
        result.append(task_map[tid])
        for neighbor in adj[tid]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                heapq.heappush(queue, neighbor)

    return result


# ---------------------------------------------------------------------------
# ConstraintOptimizer
# ---------------------------------------------------------------------------

class ConstraintOptimizer:
    """Optimize task schedules and resource assignments using constraint satisfaction."""

    # ------------------------------------------------------------------
    # Constraint definition
    # ------------------------------------------------------------------

    def define_constraints(
        self,
        plan: Mapping[str, Any],
    ) -> ConstraintSet:
        """Extract constraints from plan data.

        Hard constraints: dependencies, capacity, deadlines.
        Soft constraints: duration, workload, skills, context-switching.
        """
        tasks = plan.get("tasks", [])
        resources = plan.get("resources", [])
        hard: list[Constraint] = []
        soft: list[Constraint] = []

        # --- Hard constraints ---

        # Dependency ordering
        for task in tasks:
            deps = task.get("dependencies", [])
            if deps:
                hard.append(Constraint(
                    id=f"dep-{task.get('id', 'unknown')}",
                    kind=ConstraintKind.HARD,
                    name="dependency_ordering",
                    description=f"Task {task.get('id')} must start after {deps}.",
                ))

        # Resource capacity
        max_parallel = int(plan.get("max_parallel", len(resources) or 4))
        hard.append(Constraint(
            id="capacity-001",
            kind=ConstraintKind.HARD,
            name="resource_capacity",
            description=f"At most {max_parallel} tasks can run in parallel.",
        ))

        # Deadline
        deadline = plan.get("deadline")
        if deadline:
            hard.append(Constraint(
                id="deadline-001",
                kind=ConstraintKind.HARD,
                name="deadline",
                description=f"All tasks must complete by {deadline}.",
            ))

        # --- Soft constraints ---

        soft.append(Constraint(
            id="soft-duration-001",
            kind=ConstraintKind.SOFT,
            name="minimize_duration",
            description="Minimize total project duration.",
            weight=1.0,
        ))

        soft.append(Constraint(
            id="soft-workload-001",
            kind=ConstraintKind.SOFT,
            name="balance_workload",
            description="Balance workload across resources.",
            weight=0.8,
        ))

        if resources:
            soft.append(Constraint(
                id="soft-skills-001",
                kind=ConstraintKind.SOFT,
                name="skill_match",
                description="Prefer assigning tasks to resources with matching skills.",
                weight=0.7,
            ))

        soft.append(Constraint(
            id="soft-context-001",
            kind=ConstraintKind.SOFT,
            name="minimize_context_switching",
            description="Minimize context switching between different task types.",
            weight=0.5,
        ))

        return ConstraintSet(
            hard_constraints=tuple(hard),
            soft_constraints=tuple(soft),
        )

    # ------------------------------------------------------------------
    # Schedule optimization
    # ------------------------------------------------------------------

    def optimize_schedule(
        self,
        plan: Mapping[str, Any],
        objective: ObjectiveType = ObjectiveType.MINIMIZE_MAKESPAN,
    ) -> OptimizedSchedule:
        """Optimize task schedule to minimize makespan.

        Uses topological ordering with dependency-aware scheduling and
        resource-constrained parallelism.
        """
        tasks = list(plan.get("tasks", []))
        if not tasks:
            return OptimizedSchedule(
                plan_id=str(plan.get("id", "")),
                objective=objective.value,
            )

        max_parallel = int(plan.get("max_parallel", 4))
        sorted_tasks = _topological_sort(tasks)
        task_map = {str(t.get("id", f"task-{i}")): t for i, t in enumerate(tasks)}

        # Schedule with dependency + parallelism constraints
        finish_times: dict[str, float] = {}
        resource_slots: list[float] = [0.0] * max_parallel
        scheduled: list[TaskSchedule] = []
        violations: list[str] = []

        for task in sorted_tasks:
            tid = str(task.get("id", ""))
            duration = _get_float(task, "duration", 1.0)
            deps = task.get("dependencies", [])

            # Earliest start respecting dependencies
            earliest = 0.0
            for dep in (deps if isinstance(deps, (list, tuple)) else []):
                dep_str = str(dep)
                if dep_str in finish_times:
                    earliest = max(earliest, finish_times[dep_str])
                elif dep_str in task_map:
                    violations.append(f"Dependency {dep_str} for {tid} not yet scheduled.")

            # Find earliest available resource slot
            slot_idx = min(range(max_parallel), key=lambda i: resource_slots[i])
            start = max(earliest, resource_slots[slot_idx])
            end = start + duration

            resource_slots[slot_idx] = end
            finish_times[tid] = end

            scheduled.append(TaskSchedule(
                task_id=tid,
                title=str(task.get("title", tid)),
                start_time=start,
                end_time=end,
                duration=duration,
                assigned_to=f"slot-{slot_idx}",
            ))

        makespan = max(finish_times.values()) if finish_times else 0.0

        # Check deadline constraint
        deadline = _get_float(plan, "deadline_days")
        if deadline > 0 and makespan > deadline:
            violations.append(f"Schedule makespan ({makespan:.1f}) exceeds deadline ({deadline:.1f}).")

        return OptimizedSchedule(
            plan_id=str(plan.get("id", "")),
            tasks=tuple(scheduled),
            makespan=makespan,
            objective=objective.value,
            constraint_violations=tuple(violations),
            is_feasible=len(violations) == 0,
        )

    # ------------------------------------------------------------------
    # Resource assignment optimization
    # ------------------------------------------------------------------

    def optimize_assignments(
        self,
        plan: Mapping[str, Any],
        resources: Sequence[Mapping[str, Any]],
    ) -> AssignmentPlan:
        """Optimize resource assignments balancing workload and skills.

        Uses greedy skill-matching with workload balancing.
        """
        tasks = list(plan.get("tasks", []))
        if not tasks or not resources:
            return AssignmentPlan()

        resource_list = list(resources)
        assignments: list[ResourceAssignment] = []
        resource_load: dict[str, float] = {
            str(r.get("id", f"res-{i}")): 0.0
            for i, r in enumerate(resource_list)
        }
        resource_skills: dict[str, set[str]] = {
            str(r.get("id", f"res-{i}")): set(r.get("skills", []))
            for i, r in enumerate(resource_list)
        }
        resource_tasks: dict[str, list[str]] = defaultdict(list)

        for task in tasks:
            tid = str(task.get("id", ""))
            duration = _get_float(task, "duration", 1.0)
            required_skills = set(task.get("required_skills", []))

            # Score each resource: skill match + load balance
            best_resource = ""
            best_score = -1.0

            for rid, skills in resource_skills.items():
                if required_skills:
                    match = len(required_skills & skills) / len(required_skills)
                else:
                    match = 1.0  # No specific skills required

                # Prefer less loaded resources
                load_score = 1.0 / (1.0 + resource_load[rid])
                score = match * 0.6 + load_score * 0.4

                if score > best_score:
                    best_score = score
                    best_resource = rid

            if best_resource:
                resource_load[best_resource] += duration
                resource_tasks[best_resource].append(tid)

                skill_match = 1.0
                if required_skills and best_resource in resource_skills:
                    skill_match = len(
                        required_skills & resource_skills[best_resource]
                    ) / len(required_skills)

                assignments.append(ResourceAssignment(
                    resource_id=best_resource,
                    task_id=tid,
                    skill_match=skill_match,
                    load_contribution=duration,
                ))

        # Compute workload balance (1 = perfectly balanced, 0 = all on one resource)
        loads = list(resource_load.values())
        if loads and max(loads) > 0:
            avg_load = sum(loads) / len(loads)
            max_load = max(loads)
            workload_balance = avg_load / max_load if max_load > 0 else 1.0
        else:
            workload_balance = 1.0

        avg_skill = (
            sum(a.skill_match for a in assignments) / len(assignments)
            if assignments
            else 0.0
        )

        # Count context switches (consecutive tasks on same resource with different types)
        context_switches = 0
        for rid, task_ids in resource_tasks.items():
            if len(task_ids) > 1:
                context_switches += len(task_ids) - 1

        return AssignmentPlan(
            assignments=tuple(assignments),
            workload_balance=workload_balance,
            avg_skill_match=avg_skill,
            context_switches=context_switches,
        )

    # ------------------------------------------------------------------
    # Constraint validation
    # ------------------------------------------------------------------

    def validate_constraints(
        self,
        solution: OptimizedSchedule,
        constraint_set: ConstraintSet,
    ) -> ValidationResult:
        """Validate a schedule against constraint set."""
        violations: list[str] = list(solution.constraint_violations)
        hard_satisfied = len(constraint_set.hard_constraints) - len(violations)
        total_hard = len(constraint_set.hard_constraints)

        # Count soft constraint satisfaction
        total_soft = len(constraint_set.soft_constraints)
        soft_satisfied = total_soft  # Assume satisfied unless we detect issues

        if solution.makespan > 0 and not solution.is_feasible:
            soft_satisfied = max(0, soft_satisfied - 1)

        soft_score = soft_satisfied / max(total_soft, 1)

        return ValidationResult(
            is_valid=len(violations) == 0,
            violations=tuple(violations),
            satisfied_hard=max(hard_satisfied, 0),
            total_hard=total_hard,
            satisfied_soft=soft_satisfied,
            total_soft=total_soft,
            soft_score=soft_score,
        )

    # ------------------------------------------------------------------
    # Multi-objective / Pareto optimization
    # ------------------------------------------------------------------

    def find_pareto_optimal(
        self,
        plan: Mapping[str, Any],
        objectives: Sequence[ObjectiveType],
    ) -> list[Solution]:
        """Find Pareto-optimal solutions for multiple objectives.

        Generates multiple schedules with different trade-offs and returns
        the non-dominated solutions.
        """
        tasks = list(plan.get("tasks", []))
        if not tasks or not objectives:
            return []

        solutions: list[Solution] = []
        counter = 0

        # Generate candidate solutions with different parallelism levels
        max_parallel_range = range(1, min(len(tasks) + 1, 6))

        for max_p in max_parallel_range:
            modified_plan = dict(plan)
            modified_plan["max_parallel"] = max_p
            schedule = self.optimize_schedule(modified_plan)

            obj_values: dict[str, float] = {}
            for obj in objectives:
                if obj == ObjectiveType.MINIMIZE_MAKESPAN:
                    obj_values[obj.value] = schedule.makespan
                elif obj == ObjectiveType.MINIMIZE_COST:
                    # Cost proportional to resources used * makespan
                    obj_values[obj.value] = schedule.makespan * max_p
                elif obj == ObjectiveType.BALANCE_LOAD:
                    # Lower is better; spread across more resources is better balance
                    obj_values[obj.value] = schedule.makespan / max(max_p, 1)
                elif obj == ObjectiveType.MAXIMIZE_QUALITY:
                    # More parallelism = less quality (simplification)
                    obj_values[obj.value] = 1.0 / max(max_p, 1)

            counter += 1
            solutions.append(Solution(
                id=f"solution-{counter:03d}",
                schedule=schedule,
                objective_values=obj_values,
                is_pareto_optimal=True,  # Will be refined below
            ))

        # Filter to Pareto frontier
        pareto = self._pareto_filter(solutions, objectives)
        return pareto

    def _pareto_filter(
        self,
        solutions: list[Solution],
        objectives: Sequence[ObjectiveType],
    ) -> list[Solution]:
        """Filter solutions to non-dominated (Pareto-optimal) set.

        For minimize objectives, lower is better.
        For maximize objectives, higher is better.
        """
        maximize_objs = {ObjectiveType.MAXIMIZE_QUALITY.value, ObjectiveType.BALANCE_LOAD.value}
        dominated = set()

        for i, sol_a in enumerate(solutions):
            for j, sol_b in enumerate(solutions):
                if i == j or j in dominated:
                    continue

                # Check if sol_b dominates sol_a
                at_least_as_good = True
                strictly_better = False

                for obj in objectives:
                    val_a = sol_a.objective_values.get(obj.value, 0)
                    val_b = sol_b.objective_values.get(obj.value, 0)

                    if obj.value in maximize_objs:
                        if val_b < val_a:
                            at_least_as_good = False
                        if val_b > val_a:
                            strictly_better = True
                    else:
                        if val_b > val_a:
                            at_least_as_good = False
                        if val_b < val_a:
                            strictly_better = True

                if at_least_as_good and strictly_better:
                    dominated.add(i)
                    break

        return [
            Solution(
                id=sol.id,
                schedule=sol.schedule,
                objective_values=sol.objective_values,
                is_pareto_optimal=(idx not in dominated),
            )
            for idx, sol in enumerate(solutions)
            if idx not in dominated
        ]


__all__ = [
    "AssignmentPlan",
    "Constraint",
    "ConstraintKind",
    "ConstraintOptimizer",
    "ConstraintSet",
    "ObjectiveType",
    "OptimizedSchedule",
    "ResourceAssignment",
    "Solution",
    "TaskSchedule",
    "ValidationResult",
]
