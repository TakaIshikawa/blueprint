"""Execution plan diffing utilities."""

from __future__ import annotations

from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any


TASK_DIFF_FIELDS = (
    "title",
    "description",
    "milestone",
    "owner_type",
    "suggested_engine",
    "depends_on",
    "files_or_modules",
    "acceptance_criteria",
    "estimated_complexity",
    "status",
    "blocked_reason",
)

MILESTONE_DIFF_FIELDS = (
    "id",
    "name",
    "title",
    "description",
)


@dataclass(frozen=True)
class PlanDiffFieldChange:
    """A single field change inside a plan diff."""

    field: str
    left: Any
    right: Any

    def to_dict(self) -> dict[str, Any]:
        return {
            "field": self.field,
            "left": self.left,
            "right": self.right,
        }


@dataclass(frozen=True)
class PlanDiffTaskChange:
    """Diff details for one task present in both plans."""

    task_key: str
    left_task_id: str
    right_task_id: str
    left: dict[str, Any]
    right: dict[str, Any]
    changes: list[PlanDiffFieldChange] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_key": self.task_key,
            "left_task_id": self.left_task_id,
            "right_task_id": self.right_task_id,
            "left": self.left,
            "right": self.right,
            "changes": [change.to_dict() for change in self.changes],
        }


@dataclass(frozen=True)
class PlanDiffMilestoneChange:
    """Diff details for one milestone present in both plans."""

    milestone_key: str
    left: dict[str, Any]
    right: dict[str, Any]
    changes: list[PlanDiffFieldChange] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "milestone_key": self.milestone_key,
            "left": self.left,
            "right": self.right,
            "changes": [change.to_dict() for change in self.changes],
        }


@dataclass(frozen=True)
class PlanDiffResult:
    """Structured diff between two execution plans."""

    left_plan_id: str
    right_plan_id: str
    added_milestones: list[dict[str, Any]] = field(default_factory=list)
    removed_milestones: list[dict[str, Any]] = field(default_factory=list)
    changed_milestones: list[PlanDiffMilestoneChange] = field(default_factory=list)
    added_tasks: list[dict[str, Any]] = field(default_factory=list)
    removed_tasks: list[dict[str, Any]] = field(default_factory=list)
    changed_tasks: list[PlanDiffTaskChange] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        return any(
            (
                self.added_milestones,
                self.removed_milestones,
                self.changed_milestones,
                self.added_tasks,
                self.removed_tasks,
                self.changed_tasks,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "left_plan_id": self.left_plan_id,
            "right_plan_id": self.right_plan_id,
            "summary": {
                "added_milestones": len(self.added_milestones),
                "removed_milestones": len(self.removed_milestones),
                "changed_milestones": len(self.changed_milestones),
                "added_tasks": len(self.added_tasks),
                "removed_tasks": len(self.removed_tasks),
                "changed_tasks": len(self.changed_tasks),
            },
            "milestones": {
                "added": self.added_milestones,
                "removed": self.removed_milestones,
                "changed": [change.to_dict() for change in self.changed_milestones],
            },
            "tasks": {
                "added": self.added_tasks,
                "removed": self.removed_tasks,
                "changed": [change.to_dict() for change in self.changed_tasks],
            },
        }


def diff_execution_plans(left_plan: dict[str, Any], right_plan: dict[str, Any]) -> PlanDiffResult:
    """Compare two execution plans and return a deterministic diff."""
    left_milestones = _list_of_dicts(left_plan.get("milestones"))
    right_milestones = _list_of_dicts(right_plan.get("milestones"))
    left_tasks = _list_of_dicts(left_plan.get("tasks"))
    right_tasks = _list_of_dicts(right_plan.get("tasks"))

    left_milestone_map = _milestone_map(left_milestones)
    right_milestone_map = _milestone_map(right_milestones)
    left_task_map = _task_map(left_tasks)
    right_task_map = _task_map(right_tasks)

    added_milestone_keys = sorted(right_milestone_map.keys() - left_milestone_map.keys())
    removed_milestone_keys = sorted(left_milestone_map.keys() - right_milestone_map.keys())
    shared_milestone_keys = sorted(left_milestone_map.keys() & right_milestone_map.keys())

    task_matches, unmatched_left_task_ids, unmatched_right_task_ids = _match_tasks(
        left_task_map,
        right_task_map,
    )

    added_milestones = [right_milestone_map[key] for key in added_milestone_keys]
    removed_milestones = [left_milestone_map[key] for key in removed_milestone_keys]
    changed_milestones = [
        _diff_milestone(key, left_milestone_map[key], right_milestone_map[key])
        for key in shared_milestone_keys
        if _milestone_has_changes(left_milestone_map[key], right_milestone_map[key])
    ]

    added_tasks = [
        _task_snapshot(right_task_map[task_id])
        for task_id in sorted(unmatched_right_task_ids, key=lambda task_id: _task_sort_key(right_task_map[task_id]))
    ]
    removed_tasks = [
        _task_snapshot(left_task_map[task_id])
        for task_id in sorted(unmatched_left_task_ids, key=lambda task_id: _task_sort_key(left_task_map[task_id]))
    ]
    changed_tasks = [
        _diff_task(match_key, left_id, right_id, left_task_map[left_id], right_task_map[right_id])
        for match_key, left_id, right_id in sorted(task_matches, key=lambda item: item[0])
        if _task_has_changes(left_task_map[left_id], right_task_map[right_id])
    ]

    return PlanDiffResult(
        left_plan_id=str(left_plan.get("id") or ""),
        right_plan_id=str(right_plan.get("id") or ""),
        added_milestones=added_milestones,
        removed_milestones=removed_milestones,
        changed_milestones=changed_milestones,
        added_tasks=added_tasks,
        removed_tasks=removed_tasks,
        changed_tasks=changed_tasks,
    )


def _diff_task(
    task_key: str,
    left_task_id: str,
    right_task_id: str,
    left_task: dict[str, Any],
    right_task: dict[str, Any],
) -> PlanDiffTaskChange:
    changes = _field_changes(
        left_task,
        right_task,
        TASK_DIFF_FIELDS,
        ignored_fields={"id", "execution_plan_id", "metadata", "created_at", "updated_at"},
    )
    return PlanDiffTaskChange(
        task_key=task_key,
        left_task_id=left_task_id,
        right_task_id=right_task_id,
        left=_task_snapshot(left_task),
        right=_task_snapshot(right_task),
        changes=changes,
    )


def _diff_milestone(
    milestone_key: str,
    left_milestone: dict[str, Any],
    right_milestone: dict[str, Any],
) -> PlanDiffMilestoneChange:
    changes = _field_changes(
        left_milestone,
        right_milestone,
        MILESTONE_DIFF_FIELDS,
        ignored_fields={"tasks", "metadata", "created_at", "updated_at"},
    )
    return PlanDiffMilestoneChange(
        milestone_key=milestone_key,
        left=dict(left_milestone),
        right=dict(right_milestone),
        changes=changes,
    )


def _field_changes(
    left: dict[str, Any],
    right: dict[str, Any],
    preferred_fields: tuple[str, ...],
    ignored_fields: set[str] | None = None,
) -> list[PlanDiffFieldChange]:
    ignored_fields = ignored_fields or set()
    ordered_fields = list(preferred_fields)
    seen = set(ordered_fields)
    for key in sorted(set(left) | set(right)):
        if key not in seen and key not in ignored_fields:
            ordered_fields.append(key)
            seen.add(key)

    changes: list[PlanDiffFieldChange] = []
    for field_name in ordered_fields:
        if field_name in ignored_fields:
            continue
        left_value = left.get(field_name)
        right_value = right.get(field_name)
        if _canonical_value(left_value) != _canonical_value(right_value):
            changes.append(
                PlanDiffFieldChange(
                    field=field_name,
                    left=left_value,
                    right=right_value,
                )
            )
    return changes


def _task_has_changes(left_task: dict[str, Any], right_task: dict[str, Any]) -> bool:
    return bool(
        _field_changes(
            left_task,
            right_task,
            TASK_DIFF_FIELDS,
            ignored_fields={"id", "execution_plan_id", "metadata", "created_at", "updated_at"},
        )
    )


def _milestone_has_changes(left_milestone: dict[str, Any], right_milestone: dict[str, Any]) -> bool:
    return bool(
        _field_changes(
            left_milestone,
            right_milestone,
            MILESTONE_DIFF_FIELDS,
            ignored_fields={"tasks", "metadata", "created_at", "updated_at"},
        )
    )


def _milestone_map(milestones: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    milestone_map: dict[str, dict[str, Any]] = {}
    for index, milestone in enumerate(milestones, 1):
        milestone_map[_milestone_key(milestone, index)] = dict(milestone)
    return milestone_map


def _task_map(tasks: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    task_map: dict[str, dict[str, Any]] = {}
    for task in tasks:
        task_id = str(task.get("id") or "").strip()
        if task_id:
            task_map[task_id] = dict(task)
    return task_map


def _match_tasks(
    left_task_map: dict[str, dict[str, Any]],
    right_task_map: dict[str, dict[str, Any]],
) -> tuple[list[tuple[str, str, str]], set[str], set[str]]:
    """Match tasks deterministically across plans using stable content keys."""
    left_unmatched = set(left_task_map)
    right_unmatched = set(right_task_map)
    matches: list[tuple[str, str, str]] = []

    for match_key, left_id, right_id in _task_matches_by_key(
        left_task_map,
        right_task_map,
        left_unmatched,
        right_unmatched,
        lambda task: _task_key_candidates(task)[0],
    ):
        matches.append((match_key, left_id, right_id))

    for match_key, left_id, right_id in _task_matches_by_key(
        left_task_map,
        right_task_map,
        left_unmatched,
        right_unmatched,
        lambda task: _task_key_candidates(task)[1],
    ):
        matches.append((match_key, left_id, right_id))

    for match_key, left_id, right_id in _task_matches_by_similarity(
        left_task_map,
        right_task_map,
        left_unmatched,
        right_unmatched,
    ):
        matches.append((match_key, left_id, right_id))

    return matches, left_unmatched, right_unmatched


def _task_matches_by_key(
    left_task_map: dict[str, dict[str, Any]],
    right_task_map: dict[str, dict[str, Any]],
    left_unmatched: set[str],
    right_unmatched: set[str],
    key_fn,
) -> list[tuple[str, str, str]]:
    left_keys: dict[str, list[str]] = {}
    right_keys: dict[str, list[str]] = {}
    for task_id in left_unmatched:
        key = key_fn(left_task_map[task_id])
        if key is not None:
            left_keys.setdefault(key, []).append(task_id)
    for task_id in right_unmatched:
        key = key_fn(right_task_map[task_id])
        if key is not None:
            right_keys.setdefault(key, []).append(task_id)

    matches: list[tuple[str, str, str]] = []
    for key in sorted(left_keys.keys() & right_keys.keys()):
        left_ids = sorted(left_keys[key], key=lambda task_id: _task_sort_key(left_task_map[task_id]))
        right_ids = sorted(right_keys[key], key=lambda task_id: _task_sort_key(right_task_map[task_id]))
        pair_count = min(len(left_ids), len(right_ids))
        for index in range(pair_count):
            left_id = left_ids[index]
            right_id = right_ids[index]
            left_unmatched.discard(left_id)
            right_unmatched.discard(right_id)
            matches.append((key, left_id, right_id))
    return matches


def _task_matches_by_similarity(
    left_task_map: dict[str, dict[str, Any]],
    right_task_map: dict[str, dict[str, Any]],
    left_unmatched: set[str],
    right_unmatched: set[str],
) -> list[tuple[str, str, str]]:
    candidates: list[tuple[float, str, str, str]] = []
    for left_id in left_unmatched:
        left_task = left_task_map[left_id]
        for right_id in right_unmatched:
            right_task = right_task_map[right_id]
            score = _task_similarity(left_task, right_task)
            if score >= 0.72:
                candidates.append((score, _task_sort_key(left_task), left_id, right_id))

    matches: list[tuple[str, str, str]] = []
    used_left: set[str] = set()
    used_right: set[str] = set()
    for score, match_key, left_id, right_id in sorted(
        candidates,
        key=lambda item: (-item[0], item[1], item[2], item[3]),
    ):
        if left_id in used_left or right_id in used_right:
            continue
        if left_id not in left_unmatched or right_id not in right_unmatched:
            continue
        used_left.add(left_id)
        used_right.add(right_id)
        left_unmatched.discard(left_id)
        right_unmatched.discard(right_id)
        matches.append((match_key, left_id, right_id))
    return matches


def _task_key_candidates(task: dict[str, Any]) -> tuple[str | None, str | None]:
    title = _normalized_text(task.get("title"))
    milestone = _normalized_text(task.get("milestone"))
    if title and milestone:
        return (f"title+milestone:{title}|{milestone}", f"title:{title}")
    if title:
        return (f"title:{title}", None)
    if milestone:
        return (f"milestone:{milestone}", None)
    return (None, None)


def _task_similarity(left_task: dict[str, Any], right_task: dict[str, Any]) -> float:
    left_text = " ".join(
        part
        for part in (
            _normalized_text(left_task.get("title")),
            _normalized_text(left_task.get("description")),
            _normalized_text(left_task.get("milestone")),
        )
        if part
    )
    right_text = " ".join(
        part
        for part in (
            _normalized_text(right_task.get("title")),
            _normalized_text(right_task.get("description")),
            _normalized_text(right_task.get("milestone")),
        )
        if part
    )
    return SequenceMatcher(a=left_text, b=right_text).ratio()


def _task_sort_key(task: dict[str, Any]) -> str:
    title = _normalized_text(task.get("title"))
    milestone = _normalized_text(task.get("milestone"))
    task_id = _normalized_text(task.get("id"))
    return f"{title}|{milestone}|{task_id}"


def _milestone_key(milestone: dict[str, Any], index: int) -> str:
    for field_name in ("id", "name", "title"):
        value = milestone.get(field_name)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return f"milestone-{index}"


def _normalized_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().lower()


def _task_snapshot(task: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": task.get("id"),
        "title": task.get("title"),
        "description": task.get("description"),
        "milestone": task.get("milestone"),
        "owner_type": task.get("owner_type"),
        "suggested_engine": task.get("suggested_engine"),
        "depends_on": _string_list(task.get("depends_on")),
        "files_or_modules": _list_value(task.get("files_or_modules")),
        "acceptance_criteria": _list_value(task.get("acceptance_criteria")),
        "estimated_complexity": task.get("estimated_complexity"),
        "status": task.get("status"),
        "blocked_reason": task.get("blocked_reason"),
    }


def _canonical_value(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _canonical_value(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [_canonical_value(item) for item in value]
    return value


def _list_value(value: Any) -> list[Any] | None:
    if not isinstance(value, list):
        return None
    return [_canonical_value(item) for item in value]


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, str) and item.strip()]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
