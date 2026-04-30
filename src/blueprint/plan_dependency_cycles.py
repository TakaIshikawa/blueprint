"""Dependency cycle explanation helpers for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from blueprint.domain.models import ExecutionPlan


CYCLE_SEVERITY = "blocking"
NO_CYCLE_SEVERITY = "none"


@dataclass(frozen=True)
class MissingDependency:
    """A task dependency reference that does not point at a plan task."""

    task_id: str
    dependency_id: str

    def to_dict(self) -> dict[str, str]:
        """Return a stable JSON-serializable missing dependency payload."""
        return {
            "task_id": self.task_id,
            "dependency_id": self.dependency_id,
        }


@dataclass(frozen=True)
class DependencyRemovalSuggestion:
    """A dependency edge that can be removed to break a cycle."""

    task_id: str
    dependency_id: str

    def to_dict(self) -> dict[str, str]:
        """Return a stable JSON-serializable removal suggestion payload."""
        return {
            "task_id": self.task_id,
            "dependency_id": self.dependency_id,
        }


@dataclass(frozen=True)
class DependencyCycle:
    """A dependency cycle and concrete repair hints for it."""

    path: list[str] = field(default_factory=list)
    affected_task_ids: list[str] = field(default_factory=list)
    severity: str = CYCLE_SEVERITY
    suggested_removals: list[DependencyRemovalSuggestion] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable cycle payload."""
        return {
            "path": self.path,
            "affected_task_ids": self.affected_task_ids,
            "severity": self.severity,
            "suggested_removals": [suggestion.to_dict() for suggestion in self.suggested_removals],
        }


@dataclass(frozen=True)
class PlanDependencyCycleExplanation:
    """Dependency cycle explanation for an execution plan."""

    plan_id: str
    task_count: int
    severity: str
    cycles: list[DependencyCycle] = field(default_factory=list)
    affected_task_ids: list[str] = field(default_factory=list)
    missing_dependencies: list[MissingDependency] = field(default_factory=list)

    @property
    def has_cycles(self) -> bool:
        """Return True when one or more dependency cycles were detected."""
        return bool(self.cycles)

    @property
    def has_missing_dependencies(self) -> bool:
        """Return True when one or more dependencies reference unknown tasks."""
        return bool(self.missing_dependencies)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable cycle explanation payload."""
        return {
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "severity": self.severity,
            "affected_task_ids": self.affected_task_ids,
            "cycles": [cycle.to_dict() for cycle in self.cycles],
            "missing_dependencies": [
                dependency.to_dict() for dependency in self.missing_dependencies
            ],
        }


def explain_plan_dependency_cycles(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> PlanDependencyCycleExplanation:
    """Detect and explain dependency cycles in an execution plan.

    The input is validated through the ExecutionPlan domain model. References to
    missing task IDs are reported separately and ignored while searching for
    cycles so incomplete plans can still produce actionable cycle results.
    """
    plan = ExecutionPlan.model_validate(execution_plan).model_dump(mode="python")
    tasks = plan.get("tasks", [])
    tasks_by_id = {str(task["id"]): task for task in tasks}
    task_ids = set(tasks_by_id)
    dependencies_by_task_id = {
        task_id: [
            dependency_id for dependency_id in _dependency_ids(task) if dependency_id in task_ids
        ]
        for task_id, task in tasks_by_id.items()
    }
    missing_dependencies = _missing_dependencies(tasks_by_id, task_ids)
    cycles = _dependency_cycles(dependencies_by_task_id)
    affected_task_ids = sorted({task_id for cycle in cycles for task_id in cycle.affected_task_ids})

    return PlanDependencyCycleExplanation(
        plan_id=str(plan.get("id") or ""),
        task_count=len(tasks_by_id),
        severity=CYCLE_SEVERITY if cycles else NO_CYCLE_SEVERITY,
        cycles=cycles,
        affected_task_ids=affected_task_ids,
        missing_dependencies=missing_dependencies,
    )


def _dependency_cycles(
    dependencies_by_task_id: dict[str, list[str]],
) -> list[DependencyCycle]:
    seen_cycles: set[tuple[str, ...]] = set()

    def visit(current_task_id: str, path: list[str]) -> None:
        path.append(current_task_id)
        for dependency_id in dependencies_by_task_id.get(current_task_id, []):
            if dependency_id not in dependencies_by_task_id:
                continue
            if dependency_id in path:
                cycle_nodes = path[path.index(dependency_id) :]
                seen_cycles.add(_canonical_cycle(cycle_nodes))
                continue
            visit(dependency_id, path)
        path.pop()

    for task_id in sorted(dependencies_by_task_id):
        visit(task_id, [])

    cycles: list[DependencyCycle] = []
    for cycle_nodes in sorted(seen_cycles):
        path = [*cycle_nodes, cycle_nodes[0]]
        cycles.append(
            DependencyCycle(
                path=path,
                affected_task_ids=sorted(cycle_nodes),
                suggested_removals=_removal_suggestions(path),
            )
        )
    return cycles


def _canonical_cycle(cycle_nodes: list[str]) -> tuple[str, ...]:
    if len(cycle_nodes) == 1:
        return (cycle_nodes[0],)

    rotations = [
        tuple(cycle_nodes[index:] + cycle_nodes[:index]) for index in range(len(cycle_nodes))
    ]
    return min(rotations)


def _removal_suggestions(path: list[str]) -> list[DependencyRemovalSuggestion]:
    return [
        DependencyRemovalSuggestion(task_id=task_id, dependency_id=dependency_id)
        for task_id, dependency_id in zip(path, path[1:])
    ]


def _missing_dependencies(
    tasks_by_id: dict[str, dict[str, Any]],
    task_ids: set[str],
) -> list[MissingDependency]:
    missing_dependencies: list[MissingDependency] = []
    for task_id in sorted(tasks_by_id):
        missing_dependencies.extend(
            MissingDependency(task_id=task_id, dependency_id=dependency_id)
            for dependency_id in _dependency_ids(tasks_by_id[task_id])
            if dependency_id not in task_ids
        )
    return missing_dependencies


def _dependency_ids(task: dict[str, Any]) -> list[str]:
    dependency_ids: list[str] = []
    seen: set[str] = set()
    for dependency_id in task.get("depends_on", []):
        if not isinstance(dependency_id, str) or not dependency_id.strip():
            continue
        normalized_dependency_id = dependency_id.strip()
        if normalized_dependency_id in seen:
            continue
        dependency_ids.append(normalized_dependency_id)
        seen.add(normalized_dependency_id)
    return dependency_ids


__all__ = [
    "CYCLE_SEVERITY",
    "NO_CYCLE_SEVERITY",
    "DependencyCycle",
    "DependencyRemovalSuggestion",
    "MissingDependency",
    "PlanDependencyCycleExplanation",
    "explain_plan_dependency_cycles",
]
