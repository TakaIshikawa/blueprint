"""Critical path analysis for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


COMPLEXITY_WEIGHTS = {
    "low": 1,
    "medium": 2,
    "high": 3,
}
DEFAULT_COMPLEXITY_WEIGHT = 2


class CriticalPathError(ValueError):
    """Raised when critical path analysis cannot be completed."""


class DependencyCycleError(CriticalPathError):
    """Raised when an execution plan contains a dependency cycle."""

    def __init__(self, cycle: list[str]):
        self.cycle = cycle
        super().__init__(f"Dependency cycle detected: {' -> '.join(cycle)}")


class UnknownDependencyError(CriticalPathError):
    """Raised when a task depends on an ID that is not in the plan."""

    def __init__(self, unknown_dependencies: dict[str, list[str]]):
        self.unknown_dependencies = unknown_dependencies
        details = "; ".join(
            f"{task_id}: {', '.join(dependency_ids)}"
            for task_id, dependency_ids in sorted(unknown_dependencies.items())
        )
        super().__init__(f"Unknown dependency IDs found: {details}")


@dataclass(frozen=True)
class CriticalPathTask:
    """One task on the critical dependency chain."""

    id: str
    title: str
    milestone: str | None
    estimated_complexity: str | None
    weight: int
    cumulative_weight: int
    blocking_dependencies: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable task payload."""
        return {
            "id": self.id,
            "title": self.title,
            "milestone": self.milestone,
            "estimated_complexity": self.estimated_complexity,
            "weight": self.weight,
            "cumulative_weight": self.cumulative_weight,
            "blocking_dependencies": self.blocking_dependencies,
        }


@dataclass(frozen=True)
class CriticalPathResult:
    """Critical path analysis result."""

    plan_id: str
    total_weight: int
    tasks: list[CriticalPathTask]

    @property
    def task_ids(self) -> list[str]:
        return [task.id for task in self.tasks]

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable analysis payload."""
        return {
            "plan_id": self.plan_id,
            "total_weight": self.total_weight,
            "task_ids": self.task_ids,
            "tasks": [task.to_dict() for task in self.tasks],
        }


def analyze_critical_path(plan: dict[str, Any]) -> CriticalPathResult:
    """Find the longest weighted dependency chain in an execution plan."""
    tasks = _list_of_dicts(plan.get("tasks"))
    if not tasks:
        return CriticalPathResult(
            plan_id=str(plan.get("id") or ""),
            total_weight=0,
            tasks=[],
        )

    task_ids = [str(task.get("id") or "") for task in tasks]
    duplicate_ids = _duplicates(task_ids)
    if duplicate_ids:
        raise CriticalPathError(
            "Duplicate task IDs found: " + ", ".join(duplicate_ids)
        )

    tasks_by_id = {str(task["id"]): task for task in tasks if task.get("id")}
    unknown_dependencies = _unknown_dependencies(tasks, set(tasks_by_id))
    if unknown_dependencies:
        raise UnknownDependencyError(unknown_dependencies)

    dependency_ids_by_task_id = {
        task_id: _dependency_ids(task)
        for task_id, task in tasks_by_id.items()
    }
    dependents_by_task_id = _dependents_by_task_id(dependency_ids_by_task_id)

    cycle = _find_cycle(dependency_ids_by_task_id)
    if cycle:
        raise DependencyCycleError(cycle)

    topological_order = _topological_order(dependency_ids_by_task_id, dependents_by_task_id)
    best_weight: dict[str, int] = {}
    predecessor: dict[str, str | None] = {}

    for task_id in topological_order:
        current_task = tasks_by_id[task_id]
        current_weight = _task_weight(current_task)
        dependencies = dependency_ids_by_task_id[task_id]
        best_dependency = _best_dependency(dependencies, best_weight)
        if best_dependency is None:
            best_weight[task_id] = current_weight
            predecessor[task_id] = None
        else:
            best_weight[task_id] = best_weight[best_dependency] + current_weight
            predecessor[task_id] = best_dependency

    terminal_task_id = _best_terminal_task_id(topological_order, best_weight)
    path_ids = _path_ids(terminal_task_id, predecessor)
    path_tasks = _critical_path_tasks(path_ids, tasks_by_id, dependency_ids_by_task_id)
    return CriticalPathResult(
        plan_id=str(plan.get("id") or ""),
        total_weight=best_weight[terminal_task_id],
        tasks=path_tasks,
    )


def _critical_path_tasks(
    path_ids: list[str],
    tasks_by_id: dict[str, dict[str, Any]],
    dependency_ids_by_task_id: dict[str, list[str]],
) -> list[CriticalPathTask]:
    cumulative_weight = 0
    path_tasks = []
    for task_id in path_ids:
        task = tasks_by_id[task_id]
        weight = _task_weight(task)
        cumulative_weight += weight
        path_tasks.append(
            CriticalPathTask(
                id=task_id,
                title=str(task.get("title") or "Untitled task"),
                milestone=(
                    str(task["milestone"])
                    if task.get("milestone") is not None
                    else None
                ),
                estimated_complexity=(
                    str(task["estimated_complexity"])
                    if task.get("estimated_complexity") is not None
                    else None
                ),
                weight=weight,
                cumulative_weight=cumulative_weight,
                blocking_dependencies=dependency_ids_by_task_id[task_id],
            )
        )
    return path_tasks


def _best_dependency(
    dependencies: list[str],
    best_weight: dict[str, int],
) -> str | None:
    best_dependency = None
    for dependency_id in dependencies:
        if best_dependency is None:
            best_dependency = dependency_id
        elif best_weight[dependency_id] > best_weight[best_dependency]:
            best_dependency = dependency_id
    return best_dependency


def _best_terminal_task_id(
    topological_order: list[str],
    best_weight: dict[str, int],
) -> str:
    best_task_id = topological_order[0]
    for task_id in topological_order[1:]:
        if best_weight[task_id] > best_weight[best_task_id]:
            best_task_id = task_id
    return best_task_id


def _path_ids(
    terminal_task_id: str,
    predecessor: dict[str, str | None],
) -> list[str]:
    path = []
    current_task_id: str | None = terminal_task_id
    while current_task_id is not None:
        path.append(current_task_id)
        current_task_id = predecessor[current_task_id]
    return list(reversed(path))


def _topological_order(
    dependency_ids_by_task_id: dict[str, list[str]],
    dependents_by_task_id: dict[str, list[str]],
) -> list[str]:
    in_degree = {
        task_id: len(dependency_ids)
        for task_id, dependency_ids in dependency_ids_by_task_id.items()
    }
    ready = [
        task_id
        for task_id, dependency_ids in dependency_ids_by_task_id.items()
        if not dependency_ids
    ]
    order = []

    while ready:
        task_id = ready.pop(0)
        order.append(task_id)
        for dependent_id in dependents_by_task_id.get(task_id, []):
            in_degree[dependent_id] -= 1
            if in_degree[dependent_id] == 0:
                ready.append(dependent_id)

    return order


def _find_cycle(adjacency: dict[str, list[str]]) -> list[str] | None:
    state: dict[str, str] = {}
    stack: list[str] = []

    def visit(task_id: str) -> list[str] | None:
        state[task_id] = "visiting"
        stack.append(task_id)

        for dependency_id in adjacency.get(task_id, []):
            if state.get(dependency_id) == "visiting":
                return stack[stack.index(dependency_id) :] + [dependency_id]
            if state.get(dependency_id) is None:
                cycle = visit(dependency_id)
                if cycle:
                    return cycle

        stack.pop()
        state[task_id] = "visited"
        return None

    for task_id in adjacency:
        if state.get(task_id) is None:
            cycle = visit(task_id)
            if cycle:
                return cycle
    return None


def _dependents_by_task_id(
    dependency_ids_by_task_id: dict[str, list[str]],
) -> dict[str, list[str]]:
    dependents: dict[str, list[str]] = {}
    for task_id, dependency_ids in dependency_ids_by_task_id.items():
        for dependency_id in dependency_ids:
            dependents.setdefault(dependency_id, []).append(task_id)
    return dependents


def _unknown_dependencies(
    tasks: list[dict[str, Any]],
    task_ids: set[str],
) -> dict[str, list[str]]:
    unknown = {}
    for task in tasks:
        task_id = str(task.get("id") or "")
        missing = [
            dependency_id
            for dependency_id in _dependency_ids(task)
            if dependency_id not in task_ids
        ]
        if missing:
            unknown[task_id] = missing
    return unknown


def _dependency_ids(task: dict[str, Any]) -> list[str]:
    return [
        str(dependency_id)
        for dependency_id in task.get("depends_on") or []
        if str(dependency_id)
    ]


def _task_weight(task: dict[str, Any]) -> int:
    complexity = str(task.get("estimated_complexity") or "").strip().lower()
    return COMPLEXITY_WEIGHTS.get(complexity, DEFAULT_COMPLEXITY_WEIGHT)


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _duplicates(values: list[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if not value:
            continue
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)
