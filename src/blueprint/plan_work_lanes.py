"""Build deterministic parallel work lanes for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import Any, Iterable, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class PlanWorkLaneAssignment:
    """One task assigned to a dependency-safe work lane."""

    task_id: str
    title: str
    dependency_level: int
    files_or_modules: tuple[str, ...] = field(default_factory=tuple)
    conflict_reasons: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "dependency_level": self.dependency_level,
            "files_or_modules": list(self.files_or_modules),
            "conflict_reasons": list(self.conflict_reasons),
        }


@dataclass(frozen=True, slots=True)
class PlanWorkLane:
    """A deterministic group of tasks that can run in parallel."""

    lane_index: int
    dependency_level: int
    assignments: tuple[PlanWorkLaneAssignment, ...] = field(default_factory=tuple)

    @property
    def task_ids(self) -> tuple[str, ...]:
        """Task ids assigned to this lane in execution order."""
        return tuple(assignment.task_id for assignment in self.assignments)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "lane_index": self.lane_index,
            "dependency_level": self.dependency_level,
            "task_ids": list(self.task_ids),
            "assignments": [assignment.to_dict() for assignment in self.assignments],
        }


@dataclass(frozen=True, slots=True)
class BlockedPlanWorkTask:
    """A task that could not be placed because dependencies are unresolved."""

    task_id: str
    title: str
    depends_on: tuple[str, ...] = field(default_factory=tuple)
    unresolved_dependency_ids: tuple[str, ...] = field(default_factory=tuple)
    block_reasons: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "depends_on": list(self.depends_on),
            "unresolved_dependency_ids": list(self.unresolved_dependency_ids),
            "block_reasons": list(self.block_reasons),
        }


@dataclass(frozen=True, slots=True)
class PlanWorkLaneResult:
    """Complete work-lane planning result."""

    plan_id: str
    lanes: tuple[PlanWorkLane, ...] = field(default_factory=tuple)
    blocked_tasks: tuple[BlockedPlanWorkTask, ...] = field(default_factory=tuple)
    unresolved_dependency_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "lanes": [lane.to_dict() for lane in self.lanes],
            "blocked_tasks": [task.to_dict() for task in self.blocked_tasks],
            "unresolved_dependency_ids": list(self.unresolved_dependency_ids),
        }


def build_plan_work_lanes(plan: Mapping[str, Any] | ExecutionPlan) -> PlanWorkLaneResult:
    """Group execution-plan tasks into deterministic dependency-safe work lanes."""
    payload = _plan_payload(plan)
    records = _task_records(_task_payloads(payload.get("tasks")))
    task_ids = [record.task_id for record in records]
    task_id_set = set(task_ids)
    dependencies_by_task_id = {
        record.task_id: _dependency_ids(record.task) for record in records
    }
    unknown_by_task_id = {
        record.task_id: [
            dependency_id
            for dependency_id in dependencies_by_task_id.get(record.task_id, ())
            if dependency_id not in task_id_set
        ]
        for record in records
    }

    scheduled_ids: set[str] = set()
    dependency_level_by_task_id: dict[str, int] = {}
    lanes: list[PlanWorkLane] = []
    blocked_ids = {
        task_id for task_id, unknown_ids in unknown_by_task_id.items() if unknown_ids
    }

    while True:
        ready_records = [
            record
            for record in records
            if record.task_id not in scheduled_ids
            and record.task_id not in blocked_ids
            and all(
                dependency_id in scheduled_ids
                for dependency_id in dependencies_by_task_id.get(record.task_id, ())
            )
        ]
        if not ready_records:
            break

        level = max(
            (
                dependency_level_by_task_id.get(dependency_id, 0) + 1
                for record in ready_records
                for dependency_id in dependencies_by_task_id.get(record.task_id, ())
            ),
            default=1,
        )
        level_lanes = _split_ready_records_into_lanes(
            records=ready_records,
            lane_index_offset=len(lanes),
            dependency_level=level,
        )
        lanes.extend(level_lanes)

        for record in ready_records:
            scheduled_ids.add(record.task_id)
            dependency_level_by_task_id[record.task_id] = level

    unscheduled_ids = [
        record.task_id for record in records if record.task_id not in scheduled_ids
    ]
    blocked_tasks = tuple(
        _blocked_task(
            record=record,
            dependencies=dependencies_by_task_id.get(record.task_id, ()),
            unknown_dependencies=unknown_by_task_id.get(record.task_id, ()),
            scheduled_ids=scheduled_ids,
            task_id_set=task_id_set,
        )
        for record in records
        if record.task_id in unscheduled_ids
    )
    unresolved_dependency_ids = _dedupe(
        dependency_id
        for record in records
        if record.task_id in unscheduled_ids
        for dependency_id in unknown_by_task_id.get(record.task_id, ())
    )

    return PlanWorkLaneResult(
        plan_id=_text(payload.get("id")),
        lanes=tuple(lanes),
        blocked_tasks=blocked_tasks,
        unresolved_dependency_ids=tuple(unresolved_dependency_ids),
    )


def plan_work_lanes_to_dict(result: PlanWorkLaneResult) -> dict[str, Any]:
    """Serialize a work-lane result to a plain dictionary."""
    return result.to_dict()


plan_work_lanes_to_dict.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str
    title: str
    files_or_modules: tuple[str, ...]


def _split_ready_records_into_lanes(
    *,
    records: list[_TaskRecord],
    lane_index_offset: int,
    dependency_level: int,
) -> list[PlanWorkLane]:
    mutable_lanes: list[list[PlanWorkLaneAssignment]] = []

    for record in records:
        assignment: PlanWorkLaneAssignment | None = None
        fallback_conflicts: list[str] = []
        for lane_assignments in mutable_lanes:
            conflicts = _lane_conflicts(record, lane_assignments)
            if conflicts:
                fallback_conflicts.extend(conflicts)
                continue
            assignment = _assignment(
                record,
                dependency_level=dependency_level,
                conflict_reasons=(),
            )
            lane_assignments.append(assignment)
            break

        if assignment is None:
            mutable_lanes.append(
                [
                    _assignment(
                        record,
                        dependency_level=dependency_level,
                        conflict_reasons=tuple(_dedupe(fallback_conflicts)),
                    )
                ]
            )

    return [
        PlanWorkLane(
            lane_index=lane_index_offset + index,
            dependency_level=dependency_level,
            assignments=tuple(assignments),
        )
        for index, assignments in enumerate(mutable_lanes, start=1)
    ]


def _lane_conflicts(
    record: _TaskRecord,
    lane_assignments: list[PlanWorkLaneAssignment],
) -> list[str]:
    conflicts: list[str] = []
    for assignment in lane_assignments:
        overlapping_paths = _overlapping_paths(
            record.files_or_modules,
            assignment.files_or_modules,
        )
        if overlapping_paths:
            conflicts.append(
                "Separated from "
                f"{assignment.task_id} due to overlapping files_or_modules: "
                + ", ".join(overlapping_paths)
            )
    return conflicts


def _assignment(
    record: _TaskRecord,
    *,
    dependency_level: int,
    conflict_reasons: tuple[str, ...],
) -> PlanWorkLaneAssignment:
    return PlanWorkLaneAssignment(
        task_id=record.task_id,
        title=record.title,
        dependency_level=dependency_level,
        files_or_modules=record.files_or_modules,
        conflict_reasons=conflict_reasons,
    )


def _blocked_task(
    *,
    record: _TaskRecord,
    dependencies: tuple[str, ...],
    unknown_dependencies: tuple[str, ...],
    scheduled_ids: set[str],
    task_id_set: set[str],
) -> BlockedPlanWorkTask:
    unscheduled_known_dependencies = [
        dependency_id
        for dependency_id in dependencies
        if dependency_id in task_id_set and dependency_id not in scheduled_ids
    ]
    block_reasons: list[str] = []
    for dependency_id in unknown_dependencies:
        block_reasons.append(f"depends_on references unknown task '{dependency_id}'")
    for dependency_id in unscheduled_known_dependencies:
        block_reasons.append(f"depends_on references unscheduled task '{dependency_id}'")

    return BlockedPlanWorkTask(
        task_id=record.task_id,
        title=record.title,
        depends_on=dependencies,
        unresolved_dependency_ids=tuple(unknown_dependencies),
        block_reasons=tuple(block_reasons),
    )


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
    return {}


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
                files_or_modules=tuple(_dedupe(_strings(task.get("files_or_modules")))),
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


def _overlapping_paths(
    left_values: Iterable[str],
    right_values: Iterable[str],
) -> list[str]:
    overlaps: list[str] = []
    left_paths = [_normalized_path(value) for value in left_values]
    right_paths = [_normalized_path(value) for value in right_values]
    for left_path in left_paths:
        if not left_path:
            continue
        for right_path in right_paths:
            if not right_path:
                continue
            if _paths_overlap(left_path, right_path):
                overlap = left_path if left_path == right_path else f"{left_path} <-> {right_path}"
                overlaps.append(overlap)
    return _dedupe(overlaps)


def _paths_overlap(left_path: str, right_path: str) -> bool:
    if left_path == right_path:
        return True
    left_parts = PurePosixPath(left_path).parts
    right_parts = PurePosixPath(right_path).parts
    return (
        left_parts == right_parts[: len(left_parts)]
        or right_parts == left_parts[: len(right_parts)]
    )


def _normalized_path(value: str) -> str:
    return value.strip().strip("`'\",;:()[]{}").rstrip(".").replace("\\", "/").strip("/")


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
    "BlockedPlanWorkTask",
    "PlanWorkLane",
    "PlanWorkLaneAssignment",
    "PlanWorkLaneResult",
    "build_plan_work_lanes",
    "plan_work_lanes_to_dict",
]
