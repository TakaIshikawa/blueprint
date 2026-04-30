"""Detect task branch names that collide or are easy to confuse."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan
from blueprint.task_branch_names import (
    DEFAULT_BRANCH_PREFIX,
    DEFAULT_MAX_BRANCH_NAME_LENGTH,
    generate_task_branch_names,
)


@dataclass(frozen=True, slots=True)
class TaskBranchCollision:
    """A group of tasks whose branch names are duplicated or confusingly similar."""

    generated_branch: str
    task_ids: tuple[str, ...]
    titles: tuple[str, ...]
    suggested_unique_branch: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation."""
        return {
            "generated_branch": self.generated_branch,
            "task_ids": list(self.task_ids),
            "titles": list(self.titles),
            "suggested_unique_branch": dict(self.suggested_unique_branch),
        }


def detect_task_branch_collisions(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
    *,
    prefix: str = DEFAULT_BRANCH_PREFIX,
    max_length: int = DEFAULT_MAX_BRANCH_NAME_LENGTH,
) -> list[TaskBranchCollision]:
    """Return branch collision groups for tasks in an execution plan.

    Metadata-provided branch names are treated as the task's current branch name
    for collision detection. Suggestions are still generated from task IDs and
    titles so they remain deterministic and task-scoped.
    """
    plan = _validated_plan_payload(execution_plan)
    tasks = _list_of_task_payloads(plan.get("tasks"))
    if len(tasks) < 2:
        return []

    records = [
        _task_branch_record(index, task, prefix=prefix, max_length=max_length)
        for index, task in enumerate(tasks, start=1)
    ]
    suggested = _suggested_unique_branches(plan, prefix=prefix, max_length=max_length)
    groups = _collision_index_groups(records)

    collisions: list[TaskBranchCollision] = []
    for indexes in groups:
        group_records = [records[index] for index in indexes]
        task_ids = tuple(record["task_id"] for record in group_records)
        titles = tuple(record["title"] for record in group_records)
        suggestions = {
            record["task_id"]: suggested.get(record["task_id"], record["generated_branch"])
            for record in group_records
        }
        collisions.append(
            TaskBranchCollision(
                generated_branch=_group_generated_branch(group_records),
                task_ids=task_ids,
                titles=titles,
                suggested_unique_branch=suggestions,
            )
        )

    return sorted(collisions, key=lambda item: (item.generated_branch, item.task_ids))


def task_branch_collisions_to_dicts(
    collisions: list[TaskBranchCollision] | tuple[TaskBranchCollision, ...],
) -> list[dict[str, Any]]:
    """Serialize task branch collision groups to dictionaries."""
    return [collision.to_dict() for collision in collisions]


def _validated_plan_payload(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> dict[str, Any]:
    if hasattr(execution_plan, "model_dump"):
        return execution_plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(execution_plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(execution_plan)


def _list_of_task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            tasks.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _task_branch_record(
    index: int,
    task: Mapping[str, Any],
    *,
    prefix: str,
    max_length: int,
) -> dict[str, str]:
    task_id = _text(task.get("id")) or f"task-{index}"
    title = _text(task.get("title"))
    metadata_branch = _metadata_branch_name(task.get("metadata"))
    generated_branch = metadata_branch or _generated_branch_for_task(
        task,
        task_id=task_id,
        prefix=prefix,
        max_length=max_length,
    )
    return {
        "task_id": task_id,
        "title": title,
        "generated_branch": generated_branch,
        "branch_key": _confusing_branch_key(generated_branch),
        "title_key": _confusing_branch_key(title),
    }


def _generated_branch_for_task(
    task: Mapping[str, Any],
    *,
    task_id: str,
    prefix: str,
    max_length: int,
) -> str:
    task_plan = _minimal_plan([dict(task)])
    return generate_task_branch_names(
        task_plan,
        prefix=prefix,
        max_length=max_length,
        collision_strategy="error",
    )[task_id]


def _suggested_unique_branches(
    plan: Mapping[str, Any],
    *,
    prefix: str,
    max_length: int,
) -> dict[str, str]:
    return generate_task_branch_names(
        plan,
        prefix=prefix,
        max_length=max_length,
        collision_strategy="suffix",
    )


def _metadata_branch_name(metadata: Any) -> str | None:
    if not isinstance(metadata, Mapping):
        return None
    for key in (
        "branch_name",
        "git_branch",
        "generated_branch",
        "suggested_branch",
        "branch",
    ):
        value = _text(metadata.get(key))
        if value:
            return value
    return None


def _collision_index_groups(records: list[dict[str, str]]) -> list[list[int]]:
    parents = list(range(len(records)))
    indexes_by_key: dict[tuple[str, str], list[int]] = {}

    for index, record in enumerate(records):
        for key_type in ("generated_branch", "branch_key", "title_key"):
            key = record.get(key_type, "")
            if key:
                indexes_by_key.setdefault((key_type, key), []).append(index)

    for indexes in indexes_by_key.values():
        if len(indexes) < 2:
            continue
        first = indexes[0]
        for index in indexes[1:]:
            _union(parents, first, index)

    groups_by_parent: dict[int, list[int]] = {}
    for index in range(len(records)):
        groups_by_parent.setdefault(_find(parents, index), []).append(index)

    return [
        indexes
        for indexes in groups_by_parent.values()
        if len(indexes) > 1
    ]


def _find(parents: list[int], index: int) -> int:
    while parents[index] != index:
        parents[index] = parents[parents[index]]
        index = parents[index]
    return index


def _union(parents: list[int], left: int, right: int) -> None:
    left_parent = _find(parents, left)
    right_parent = _find(parents, right)
    if left_parent != right_parent:
        parents[right_parent] = left_parent


def _group_generated_branch(group_records: list[dict[str, str]]) -> str:
    counts: dict[str, int] = {}
    for record in group_records:
        branch = record["generated_branch"]
        counts[branch] = counts.get(branch, 0) + 1
    return max(counts, key=lambda branch: (counts[branch], -len(branch), branch))


def _minimal_plan(tasks: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "id": "plan-branch-collision",
        "implementation_brief_id": "brief-branch-collision",
        "target_engine": "codex",
        "target_repo": "example/repo",
        "project_type": "cli_tool",
        "milestones": [{"name": "Implementation", "description": "Implementation"}],
        "test_strategy": "Run focused tests",
        "handoff_prompt": "Implement the task",
        "status": "draft",
        "generation_model": "deterministic",
        "generation_tokens": 0,
        "generation_prompt": "deterministic",
        "tasks": tasks,
    }


def _confusing_branch_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


__all__ = [
    "TaskBranchCollision",
    "detect_task_branch_collisions",
    "task_branch_collisions_to_dicts",
]
