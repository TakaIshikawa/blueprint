"""Dependency wave analysis for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class ExecutionWaveError(ValueError):
    """Raised when execution wave analysis cannot be completed."""


class DependencyCycleError(ExecutionWaveError):
    """Raised when an execution plan contains a dependency cycle."""

    def __init__(self, cycle: list[str]):
        self.cycle = cycle
        super().__init__(f"Dependency cycle detected: {' -> '.join(cycle)}")


class UnknownDependencyError(ExecutionWaveError):
    """Raised when a task depends on an ID that is not in the plan."""

    def __init__(self, unknown_dependencies: dict[str, list[str]]):
        self.unknown_dependencies = unknown_dependencies
        details = "; ".join(
            f"{task_id}: {', '.join(dependency_ids)}"
            for task_id, dependency_ids in sorted(unknown_dependencies.items())
        )
        super().__init__(f"Unknown dependency IDs found: {details}")


@dataclass(frozen=True)
class ExecutionWaveTask:
    """One task assigned to a dependency-ready wave."""

    id: str
    title: str
    milestone: str | None
    suggested_engine: str | None
    files_or_modules: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable task payload."""
        return {
            "id": self.id,
            "title": self.title,
            "milestone": self.milestone,
            "suggested_engine": self.suggested_engine,
            "files_or_modules": self.files_or_modules,
        }


@dataclass(frozen=True)
class ExecutionWave:
    """A group of tasks whose dependencies are satisfied by earlier waves."""

    wave_number: int
    tasks: list[ExecutionWaveTask] = field(default_factory=list)

    @property
    def task_ids(self) -> list[str]:
        return [task.id for task in self.tasks]

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable wave payload."""
        return {
            "wave_number": self.wave_number,
            "task_ids": self.task_ids,
            "tasks": [task.to_dict() for task in self.tasks],
        }


@dataclass(frozen=True)
class ExecutionWavesResult:
    """Dependency wave analysis result."""

    plan_id: str
    waves: list[ExecutionWave] = field(default_factory=list)

    @property
    def task_count(self) -> int:
        return sum(len(wave.tasks) for wave in self.waves)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable analysis payload."""
        return {
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "waves": [wave.to_dict() for wave in self.waves],
        }


def analyze_execution_waves(plan: dict[str, Any]) -> ExecutionWavesResult:
    """Group execution tasks into dependency-ready waves.

    Wave 1 contains tasks with no dependencies in the plan. Each later wave
    contains tasks whose dependencies all appeared in earlier waves. Task status
    does not affect scheduling; completed and skipped tasks still appear.
    """
    tasks = _list_of_dicts(plan.get("tasks"))
    if not tasks:
        return ExecutionWavesResult(plan_id=str(plan.get("id") or ""))

    task_ids = [str(task.get("id") or "") for task in tasks]
    duplicate_ids = _duplicates(task_ids)
    if duplicate_ids:
        raise ExecutionWaveError("Duplicate task IDs found: " + ", ".join(duplicate_ids))

    tasks_by_id = {str(task["id"]): task for task in tasks if task.get("id")}
    unknown_dependencies = _unknown_dependencies(tasks, set(tasks_by_id))
    if unknown_dependencies:
        raise UnknownDependencyError(unknown_dependencies)

    dependency_ids_by_task_id = {
        task_id: _dependency_ids(task) for task_id, task in tasks_by_id.items()
    }
    dependents_by_task_id = _dependents_by_task_id(dependency_ids_by_task_id)

    cycle = _find_cycle(dependency_ids_by_task_id)
    if cycle:
        raise DependencyCycleError(cycle)

    remaining_dependency_count = {
        task_id: len(dependency_ids)
        for task_id, dependency_ids in dependency_ids_by_task_id.items()
    }
    current_wave_ids = [
        task_id for task_id in tasks_by_id if remaining_dependency_count[task_id] == 0
    ]
    waves: list[ExecutionWave] = []

    while current_wave_ids:
        waves.append(
            ExecutionWave(
                wave_number=len(waves) + 1,
                tasks=[_wave_task(tasks_by_id[task_id]) for task_id in current_wave_ids],
            )
        )

        next_ready_ids: list[str] = []
        for task_id in current_wave_ids:
            for dependent_id in dependents_by_task_id.get(task_id, []):
                remaining_dependency_count[dependent_id] -= 1
                if remaining_dependency_count[dependent_id] == 0:
                    next_ready_ids.append(dependent_id)
        next_ready_id_set = set(next_ready_ids)
        current_wave_ids = [task_id for task_id in tasks_by_id if task_id in next_ready_id_set]

    if sum(len(wave.tasks) for wave in waves) != len(tasks_by_id):
        cycle = _find_cycle(dependency_ids_by_task_id)
        if cycle:
            raise DependencyCycleError(cycle)
        unscheduled_ids = [
            task_id for task_id in tasks_by_id if remaining_dependency_count[task_id] > 0
        ]
        raise ExecutionWaveError("Could not schedule all tasks: " + ", ".join(unscheduled_ids))

    return ExecutionWavesResult(
        plan_id=str(plan.get("id") or ""),
        waves=waves,
    )


def _wave_task(task: dict[str, Any]) -> ExecutionWaveTask:
    return ExecutionWaveTask(
        id=str(task["id"]),
        title=str(task.get("title") or "Untitled task"),
        milestone=(str(task["milestone"]) if task.get("milestone") is not None else None),
        suggested_engine=(
            str(task["suggested_engine"]) if task.get("suggested_engine") is not None else None
        ),
        files_or_modules=_string_list(task.get("files_or_modules")),
    )


def _dependents_by_task_id(
    dependency_ids_by_task_id: dict[str, list[str]],
) -> dict[str, list[str]]:
    dependents: dict[str, list[str]] = {}
    for task_id, dependency_ids in dependency_ids_by_task_id.items():
        for dependency_id in dependency_ids:
            dependents.setdefault(dependency_id, []).append(task_id)
    return dependents


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


def _unknown_dependencies(
    tasks: list[dict[str, Any]],
    task_ids: set[str],
) -> dict[str, list[str]]:
    unknown_dependencies: dict[str, list[str]] = {}
    for task in tasks:
        task_id = str(task.get("id") or "")
        missing = [
            dependency_id
            for dependency_id in _dependency_ids(task)
            if dependency_id not in task_ids
        ]
        if missing:
            unknown_dependencies[task_id] = sorted(missing)
    return unknown_dependencies


def _dependency_ids(task: dict[str, Any]) -> list[str]:
    return _string_list(task.get("depends_on"))


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


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, str) and item.strip()]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
