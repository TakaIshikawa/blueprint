"""Build compact dependency health summaries for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


_COMPLETED_STATUS = "completed"
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class MissingDependencyReference:
    """One declared dependency that does not resolve to a task in the plan."""

    task_id: str
    dependency_id: str

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "dependency_id": self.dependency_id,
        }


@dataclass(frozen=True, slots=True)
class DependencyHotspot:
    """A task with notable dependency fan-in or fan-out."""

    task_id: str
    title: str
    fan_in: int = 0
    fan_out: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "fan_in": self.fan_in,
            "fan_out": self.fan_out,
        }


@dataclass(frozen=True, slots=True)
class DependencyChain:
    """The longest bounded dependency chain found in a plan."""

    task_ids: tuple[str, ...] = field(default_factory=tuple)
    length: int = 0
    bounded: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_ids": list(self.task_ids),
            "length": self.length,
            "bounded": self.bounded,
        }


@dataclass(frozen=True, slots=True)
class PlanDependencyHealth:
    """High-level dependency health summary for an execution plan."""

    plan_id: str | None = None
    total_dependencies: int = 0
    missing_dependency_references: tuple[MissingDependencyReference, ...] = field(
        default_factory=tuple
    )
    blocked_task_ids: tuple[str, ...] = field(default_factory=tuple)
    ready_task_ids: tuple[str, ...] = field(default_factory=tuple)
    leaf_task_ids: tuple[str, ...] = field(default_factory=tuple)
    root_task_ids: tuple[str, ...] = field(default_factory=tuple)
    longest_dependency_chain: DependencyChain = field(default_factory=DependencyChain)
    fan_in_hotspots: tuple[DependencyHotspot, ...] = field(default_factory=tuple)
    fan_out_hotspots: tuple[DependencyHotspot, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "total_dependencies": self.total_dependencies,
            "missing_dependency_references": [
                item.to_dict() for item in self.missing_dependency_references
            ],
            "blocked_task_ids": list(self.blocked_task_ids),
            "ready_task_ids": list(self.ready_task_ids),
            "leaf_task_ids": list(self.leaf_task_ids),
            "root_task_ids": list(self.root_task_ids),
            "longest_dependency_chain": self.longest_dependency_chain.to_dict(),
            "fan_in_hotspots": [item.to_dict() for item in self.fan_in_hotspots],
            "fan_out_hotspots": [item.to_dict() for item in self.fan_out_hotspots],
            "warnings": list(self.warnings),
            "summary": dict(self.summary),
        }


def build_plan_dependency_health(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> PlanDependencyHealth:
    """Summarize dependency readiness and graph shape for an execution plan."""
    payload = _plan_payload(plan)
    records = _task_records(_task_payloads(payload.get("tasks")))
    task_ids = [record.task_id for record in records]
    task_id_set = set(task_ids)
    status_by_task_id = {
        record.task_id: _text(record.task.get("status")).casefold()
        for record in records
    }
    dependencies_by_task_id = {
        record.task_id: _dependency_ids(record.task) for record in records
    }
    dependents_by_task_id: dict[str, list[str]] = {task_id: [] for task_id in task_ids}
    missing_references: list[MissingDependencyReference] = []

    for record in records:
        for dependency_id in dependencies_by_task_id[record.task_id]:
            if dependency_id in task_id_set:
                dependents_by_task_id[dependency_id].append(record.task_id)
            else:
                missing_references.append(
                    MissingDependencyReference(
                        task_id=record.task_id,
                        dependency_id=dependency_id,
                    )
                )

    total_dependencies = sum(len(ids) for ids in dependencies_by_task_id.values())
    missing_by_task_id = {
        item.task_id for item in missing_references
    }
    blocked_task_ids = tuple(
        record.task_id
        for record in records
        if record.task_id in missing_by_task_id
        or any(
            dependency_id in task_id_set
            and status_by_task_id.get(dependency_id) != _COMPLETED_STATUS
            for dependency_id in dependencies_by_task_id[record.task_id]
        )
    )
    ready_task_ids = tuple(
        record.task_id
        for record in records
        if record.task_id not in blocked_task_ids
        and status_by_task_id.get(record.task_id) != _COMPLETED_STATUS
    )
    root_task_ids = tuple(
        record.task_id
        for record in records
        if not [
            dependency_id
            for dependency_id in dependencies_by_task_id[record.task_id]
            if dependency_id in task_id_set
        ]
    )
    leaf_task_ids = tuple(
        record.task_id for record in records if not dependents_by_task_id[record.task_id]
    )
    chain, chain_warnings = _longest_dependency_chain(task_ids, dependencies_by_task_id)
    fan_in_hotspots = _hotspots(
        records=records,
        dependencies_by_task_id=dependencies_by_task_id,
        dependents_by_task_id=dependents_by_task_id,
        direction="fan_in",
    )
    fan_out_hotspots = _hotspots(
        records=records,
        dependencies_by_task_id=dependencies_by_task_id,
        dependents_by_task_id=dependents_by_task_id,
        direction="fan_out",
    )

    return PlanDependencyHealth(
        plan_id=_optional_text(payload.get("id")),
        total_dependencies=total_dependencies,
        missing_dependency_references=tuple(missing_references),
        blocked_task_ids=blocked_task_ids,
        ready_task_ids=ready_task_ids,
        leaf_task_ids=leaf_task_ids,
        root_task_ids=root_task_ids,
        longest_dependency_chain=chain,
        fan_in_hotspots=fan_in_hotspots,
        fan_out_hotspots=fan_out_hotspots,
        warnings=chain_warnings,
        summary={
            "task_count": len(records),
            "total_dependencies": total_dependencies,
            "missing_dependency_count": len(missing_references),
            "blocked_task_count": len(blocked_task_ids),
            "ready_task_count": len(ready_task_ids),
            "leaf_task_count": len(leaf_task_ids),
            "root_task_count": len(root_task_ids),
            "longest_dependency_chain_length": chain.length,
            "fan_in_hotspot_count": len(fan_in_hotspots),
            "fan_out_hotspot_count": len(fan_out_hotspots),
            "warning_count": len(chain_warnings),
        },
    )


def plan_dependency_health_to_dict(result: PlanDependencyHealth) -> dict[str, Any]:
    """Serialize a dependency health summary to a plain dictionary."""
    return result.to_dict()


plan_dependency_health_to_dict.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str
    title: str
    index: int


def _longest_dependency_chain(
    task_ids: list[str],
    dependencies_by_task_id: dict[str, tuple[str, ...]],
) -> tuple[DependencyChain, tuple[str, ...]]:
    task_id_set = set(task_ids)
    best_path: tuple[str, ...] = ()
    cycle_paths: list[tuple[str, ...]] = []
    memo: dict[str, tuple[str, ...]] = {}

    def visit(task_id: str, stack: tuple[str, ...]) -> tuple[str, ...]:
        nonlocal best_path
        if task_id in stack:
            cycle = stack[stack.index(task_id) :] + (task_id,)
            cycle_paths.append(cycle)
            bounded = cycle[:-1]
            if _is_better_path(bounded, best_path, task_ids):
                best_path = bounded
            return (task_id,)
        if task_id in memo:
            return memo[task_id]

        child_paths = [
            visit(dependency_id, stack + (task_id,))
            for dependency_id in dependencies_by_task_id.get(task_id, ())
            if dependency_id in task_id_set
        ]
        if child_paths:
            path = ()
            for child_path in child_paths:
                candidate = child_path if task_id in child_path else child_path + (task_id,)
                if _is_better_path(candidate, path, task_ids):
                    path = candidate
        else:
            path = (task_id,)
        if _is_better_path(path, best_path, task_ids):
            best_path = path
        memo[task_id] = path
        return path

    for task_id in task_ids:
        visit(task_id, ())

    bounded = bool(cycle_paths)
    warnings = (
        (
            "Dependency cycle detected; longest_dependency_chain is bounded to an "
            "acyclic traversal."
        ),
    ) if bounded else ()
    chain = DependencyChain(
        task_ids=best_path,
        length=len(best_path),
        bounded=bounded,
    )
    return chain, warnings


def _hotspots(
    *,
    records: list[_TaskRecord],
    dependencies_by_task_id: dict[str, tuple[str, ...]],
    dependents_by_task_id: dict[str, list[str]],
    direction: str,
) -> tuple[DependencyHotspot, ...]:
    record_by_id = {record.task_id: record for record in records}
    hotspots: list[DependencyHotspot] = []
    for record in records:
        fan_in = len(dependencies_by_task_id[record.task_id])
        fan_out = len(dependents_by_task_id[record.task_id])
        score = fan_in if direction == "fan_in" else fan_out
        if score <= 1:
            continue
        hotspots.append(
            DependencyHotspot(
                task_id=record.task_id,
                title=record.title,
                fan_in=fan_in,
                fan_out=fan_out,
            )
        )
    hotspots.sort(
        key=lambda item: (
            -(item.fan_in if direction == "fan_in" else item.fan_out),
            record_by_id[item.task_id].index,
        )
    )
    return tuple(hotspots)


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_records(tasks: list[dict[str, Any]]) -> list[_TaskRecord]:
    records: list[_TaskRecord] = []
    seen_ids: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        if task_id in seen_ids:
            continue
        seen_ids.add(task_id)
        records.append(
            _TaskRecord(
                task=task,
                task_id=task_id,
                title=_optional_text(task.get("title")) or task_id,
                index=index,
            )
        )
    return records


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _dependency_ids(task: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(_dedupe(_strings(task.get("depends_on"))))


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _is_better_path(
    candidate: tuple[str, ...],
    current: tuple[str, ...],
    task_ids: list[str],
) -> bool:
    if len(candidate) != len(current):
        return len(candidate) > len(current)
    return _path_order_key(candidate, task_ids) < _path_order_key(current, task_ids)


def _path_order_key(path: tuple[str, ...], task_ids: list[str]) -> tuple[int, ...]:
    order = {task_id: index for index, task_id in enumerate(task_ids)}
    return tuple(order.get(task_id, len(task_ids)) for task_id in path)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "DependencyChain",
    "DependencyHotspot",
    "MissingDependencyReference",
    "PlanDependencyHealth",
    "build_plan_dependency_health",
    "plan_dependency_health_to_dict",
]
