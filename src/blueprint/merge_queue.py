"""Recommend deterministic merge and review order for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import posixpath
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


MergeQueueAction = Literal["merge", "review", "hold"]

_MERGEABLE_STATUSES = {"completed", "skipped"}
_READY_STATUSES = {"pending"}
_BROAD_TEST_HINTS = ("pytest", "tox", "nox", "npm test", "pnpm test", "yarn test")
_NARROW_TEST_HINTS = (" -k ", "::", " --lf", " --last-failed")


@dataclass(frozen=True, slots=True)
class MergeQueueEntry:
    """One task-level merge queue recommendation."""

    task_id: str
    recommended_action: MergeQueueAction
    merge_after: tuple[str, ...] = field(default_factory=tuple)
    conflicts_with: tuple[str, ...] = field(default_factory=tuple)
    review_notes: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "recommended_action": self.recommended_action,
            "merge_after": list(self.merge_after),
            "conflicts_with": list(self.conflicts_with),
            "review_notes": list(self.review_notes),
        }


def recommend_merge_queue(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> tuple[MergeQueueEntry, ...]:
    """Order completed or ready tasks for branch review and merge.

    Completed and skipped tasks are candidates to merge. Pending tasks are
    review candidates when their dependencies are satisfied, otherwise they are
    held behind the prerequisite task ids. Blocked and in-progress tasks are not
    merge queue candidates.
    """
    plan = _plan_payload(execution_plan)
    tasks = [_task_context(index, task) for index, task in enumerate(_task_payloads(plan), start=1)]
    task_by_id = {task["task_id"]: task for task in tasks}
    conflicts_by_task_id = _conflicts_by_task_id(tasks)

    queueable = [
        task
        for task in tasks
        if task["status"] in _MERGEABLE_STATUSES
        or task["status"] in _READY_STATUSES
        or _unsatisfied_dependency_ids(task, task_by_id)
    ]
    queueable_ids = {task["task_id"] for task in queueable}
    ordered_tasks = sorted(queueable, key=lambda task: _queue_sort_key(task, task_by_id, queueable_ids))

    return tuple(
        MergeQueueEntry(
            task_id=task["task_id"],
            recommended_action=_recommended_action(task, task_by_id),
            merge_after=tuple(_merge_after(task, task_by_id)),
            conflicts_with=tuple(conflicts_by_task_id.get(task["task_id"], ())),
            review_notes=tuple(_review_notes(task)),
        )
        for task in ordered_tasks
    )


def merge_queue_to_dicts(
    entries: tuple[MergeQueueEntry, ...] | list[MergeQueueEntry],
) -> list[dict[str, Any]]:
    """Serialize merge queue entries to dictionaries."""
    return [entry.to_dict() for entry in entries]


merge_queue_to_dicts.__test__ = False


def _recommended_action(
    task: Mapping[str, Any],
    task_by_id: Mapping[str, Mapping[str, Any]],
) -> MergeQueueAction:
    if _unsatisfied_dependency_ids(task, task_by_id):
        return "hold"
    if task["status"] in _MERGEABLE_STATUSES:
        return "merge"
    return "review"


def _merge_after(
    task: Mapping[str, Any],
    task_by_id: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    merge_after: list[str] = []
    for dependency_id in task["depends_on"]:
        if dependency_id in task_by_id and dependency_id not in merge_after:
            merge_after.append(dependency_id)
    for dependency_id in _unsatisfied_dependency_ids(task, task_by_id):
        if dependency_id not in merge_after:
            merge_after.append(dependency_id)
    return merge_after


def _unsatisfied_dependency_ids(
    task: Mapping[str, Any],
    task_by_id: Mapping[str, Mapping[str, Any]],
) -> list[str]:
    unsatisfied: list[str] = []
    for dependency_id in task["depends_on"]:
        dependency = task_by_id.get(dependency_id)
        if dependency is None:
            unsatisfied.append(dependency_id)
            continue
        if dependency["status"] not in _MERGEABLE_STATUSES:
            unsatisfied.append(dependency_id)
    return unsatisfied


def _queue_sort_key(
    task: Mapping[str, Any],
    task_by_id: Mapping[str, Mapping[str, Any]],
    queueable_ids: set[str],
) -> tuple[int, int]:
    return (_dependency_depth(task, task_by_id, queueable_ids, seen=set()), task["index"])


def _dependency_depth(
    task: Mapping[str, Any],
    task_by_id: Mapping[str, Mapping[str, Any]],
    queueable_ids: set[str],
    *,
    seen: set[str],
) -> int:
    task_id = task["task_id"]
    if task_id in seen:
        return 0
    seen = {*seen, task_id}

    dependency_depths: list[int] = []
    for dependency_id in task["depends_on"]:
        dependency = task_by_id.get(dependency_id)
        if dependency is None:
            dependency_depths.append(1)
        elif dependency_id in queueable_ids:
            dependency_depths.append(
                _dependency_depth(dependency, task_by_id, queueable_ids, seen=seen) + 1
            )
    return max(dependency_depths, default=0)


def _conflicts_by_task_id(tasks: list[dict[str, Any]]) -> dict[str, list[str]]:
    task_ids_by_path: dict[str, list[str]] = {}
    for task in tasks:
        for path in task["files"]:
            task_ids_by_path.setdefault(path, []).append(task["task_id"])

    conflicts: dict[str, list[str]] = {task["task_id"]: [] for task in tasks}
    for task_ids in task_ids_by_path.values():
        if len(task_ids) < 2:
            continue
        for task_id in task_ids:
            conflicts[task_id].extend(peer_id for peer_id in task_ids if peer_id != task_id)

    return {task_id: _dedupe(peer_ids) for task_id, peer_ids in conflicts.items()}


def _review_notes(task: Mapping[str, Any]) -> list[str]:
    notes: list[str] = []
    if _risk_rank(task["risk_level"]) >= 3:
        notes.append(f"Manual review required before merge: {task['risk_level']} risk")

    test_command = task["test_command"]
    if not test_command:
        notes.append("No test command recorded; confirm validation before merge")
    elif _test_breadth(test_command) == "narrow":
        notes.append("Test command appears narrow; consider broader regression coverage")
    return notes


def _test_breadth(test_command: str) -> Literal["broad", "narrow", "unknown"]:
    normalized = f" {test_command.lower()} "
    if any(hint in normalized for hint in _NARROW_TEST_HINTS):
        return "narrow"
    if any(hint in normalized for hint in _BROAD_TEST_HINTS):
        return "broad"
    return "unknown"


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


def _task_payloads(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    value = plan.get("tasks")
    if not isinstance(value, list):
        return []

    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_context(index: int, task: Mapping[str, Any]) -> dict[str, Any]:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    return {
        "index": index,
        "task_id": task_id,
        "status": (_optional_text(task.get("status")) or "pending").lower(),
        "depends_on": tuple(_strings(task.get("depends_on"))),
        "files": tuple(
            _dedupe([_normalized_path(path) for path in _strings(task.get("files_or_modules"))])
        ),
        "risk_level": (_optional_text(task.get("risk_level")) or "unspecified").lower(),
        "test_command": _optional_text(task.get("test_command")),
    }


def _risk_rank(value: str | None) -> int:
    return {
        "none": 0,
        "low": 1,
        "medium": 2,
        "moderate": 2,
        "high": 3,
        "critical": 4,
        "blocker": 4,
    }.get((value or "").lower(), 2)


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        return [text for item in items if (text := _optional_text(item))]
    return []


def _normalized_path(path: str) -> str:
    return posixpath.normpath(path.replace("\\", "/")).strip("/")


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _dedupe(values: list[str] | tuple[str, ...]) -> list[str]:
    deduped: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "MergeQueueAction",
    "MergeQueueEntry",
    "merge_queue_to_dicts",
    "recommend_merge_queue",
]
