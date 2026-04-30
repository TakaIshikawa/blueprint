"""Recommend git branch groupings for execution plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan
from blueprint.task_branch_names import (
    DEFAULT_BRANCH_PREFIX,
    DEFAULT_MAX_BRANCH_NAME_LENGTH,
    generate_task_branch_names,
)


DEFAULT_BRANCH_BASE = "main"
_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class TaskBranchCandidate:
    """A recommended git branch for one or more execution tasks."""

    branch_name: str
    task_ids: tuple[str, ...]
    reason: str
    conflict_warnings: tuple[str, ...] = field(default_factory=tuple)
    suggested_base: str = DEFAULT_BRANCH_BASE

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "branch_name": self.branch_name,
            "task_ids": list(self.task_ids),
            "reason": self.reason,
            "conflict_warnings": list(self.conflict_warnings),
            "suggested_base": self.suggested_base,
        }


def generate_task_branch_plan(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
    *,
    base_branch: str = DEFAULT_BRANCH_BASE,
    prefix: str = DEFAULT_BRANCH_PREFIX,
    max_length: int = DEFAULT_MAX_BRANCH_NAME_LENGTH,
) -> tuple[TaskBranchCandidate, ...]:
    """Return deterministic branch candidates for dispatching plan tasks.

    Dependency-linked tasks that touch the same files, or are low-risk work in
    the same milestone, are grouped to avoid avoidable branch stacks. Other
    dependency edges are represented through ``suggested_base`` so agents can
    branch from the prerequisite task output.
    """
    plan = _validated_plan_payload(execution_plan)
    tasks = _task_payloads(plan.get("tasks"))
    if not tasks:
        return ()

    contexts = [_task_context(index, task) for index, task in enumerate(tasks, start=1)]
    branch_names = generate_task_branch_names(
        {**plan, "tasks": [context["task"] for context in contexts]},
        prefix=prefix,
        max_length=max_length,
        collision_strategy="suffix",
    )
    groups = _candidate_groups(contexts)
    group_index_by_task_id = {
        context["task_id"]: group_index
        for group_index, group in enumerate(groups)
        for context in group
    }
    candidate_branch_by_group = {
        group_index: branch_names[group[0]["task_id"]] for group_index, group in enumerate(groups)
    }
    warnings_by_task_id = _conflict_warnings_by_task_id(contexts, group_index_by_task_id)

    candidates: list[TaskBranchCandidate] = []
    for group_index, group in enumerate(groups):
        task_ids = tuple(context["task_id"] for context in group)
        candidates.append(
            TaskBranchCandidate(
                branch_name=candidate_branch_by_group[group_index],
                task_ids=task_ids,
                reason=_candidate_reason(group),
                conflict_warnings=tuple(
                    _dedupe(
                        [
                            warning
                            for task_id in task_ids
                            for warning in warnings_by_task_id.get(task_id, ())
                        ]
                    )
                ),
                suggested_base=_suggested_base(
                    group=group,
                    group_index=group_index,
                    group_index_by_task_id=group_index_by_task_id,
                    candidate_branch_by_group=candidate_branch_by_group,
                    base_branch=base_branch,
                ),
            )
        )

    return tuple(candidates)


def task_branch_plan_to_dict(
    candidates: tuple[TaskBranchCandidate, ...] | list[TaskBranchCandidate],
) -> list[dict[str, Any]]:
    """Serialize task branch candidates to dictionaries."""
    return [candidate.to_dict() for candidate in candidates]


task_branch_plan_to_dict.__test__ = False


def _candidate_groups(contexts: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    parents = list(range(len(contexts)))
    index_by_task_id = {context["task_id"]: index for index, context in enumerate(contexts)}

    for index, context in enumerate(contexts):
        for dependency_id in context["depends_on"]:
            dependency_index = index_by_task_id.get(dependency_id)
            if dependency_index is None:
                continue
            dependency = contexts[dependency_index]
            if _should_group_dependency(dependency, context):
                _union(parents, dependency_index, index)

    groups_by_parent: dict[int, list[dict[str, Any]]] = {}
    for index, context in enumerate(contexts):
        groups_by_parent.setdefault(_find(parents, index), []).append(context)

    return sorted(
        groups_by_parent.values(),
        key=lambda group: min(context["index"] for context in group),
    )


def _should_group_dependency(
    dependency: Mapping[str, Any],
    dependent: Mapping[str, Any],
) -> bool:
    if set(dependency["files"]) & set(dependent["files"]):
        return True
    if not dependency["milestone"] or dependency["milestone"] != dependent["milestone"]:
        return False
    if _risk_rank(dependency["risk_level"]) >= 3 or _risk_rank(dependent["risk_level"]) >= 3:
        return False
    return (
        _complexity_rank(dependency["estimated_complexity"])
        + _complexity_rank(dependent["estimated_complexity"])
        <= 4
    )


def _candidate_reason(group: list[dict[str, Any]]) -> str:
    task_ids = [context["task_id"] for context in group]
    parts: list[str] = []
    if len(group) > 1:
        parts.append(f"grouped dependent tasks:{' -> '.join(task_ids)}")
    elif group[0]["depends_on"]:
        parts.append(f"ordered after dependencies:{', '.join(group[0]['depends_on'])}")
    else:
        parts.append("independent task candidate")

    milestones = _dedupe([context["milestone"] for context in group if context["milestone"]])
    risks = _dedupe([context["risk_level"] for context in group if context["risk_level"]])
    complexities = _dedupe(
        [context["estimated_complexity"] for context in group if context["estimated_complexity"]]
    )
    if milestones:
        parts.append(f"milestone:{', '.join(milestones)}")
    if risks:
        parts.append(f"risk:{', '.join(risks)}")
    if complexities:
        parts.append(f"complexity:{', '.join(complexities)}")
    return "; ".join(parts)


def _suggested_base(
    *,
    group: list[dict[str, Any]],
    group_index: int,
    group_index_by_task_id: dict[str, int],
    candidate_branch_by_group: dict[int, str],
    base_branch: str,
) -> str:
    dependency_group_indexes: list[int] = []
    for context in group:
        for dependency_id in context["depends_on"]:
            dependency_group_index = group_index_by_task_id.get(dependency_id)
            if dependency_group_index is None or dependency_group_index == group_index:
                continue
            dependency_group_indexes.append(dependency_group_index)

    if not dependency_group_indexes:
        return base_branch

    latest_dependency_group = max(
        _dedupe(dependency_group_indexes),
        key=lambda index: index,
    )
    return candidate_branch_by_group[latest_dependency_group]


def _conflict_warnings_by_task_id(
    contexts: list[dict[str, Any]],
    group_index_by_task_id: dict[str, int],
) -> dict[str, list[str]]:
    contexts_by_file: dict[str, list[dict[str, Any]]] = {}
    for context in contexts:
        for path in context["files"]:
            contexts_by_file.setdefault(_normalized_path(path), []).append(context)

    warnings: dict[str, list[str]] = {context["task_id"]: [] for context in contexts}
    for path, file_contexts in contexts_by_file.items():
        if len(file_contexts) < 2:
            continue
        for context in file_contexts:
            task_id = context["task_id"]
            peers = [
                peer["task_id"]
                for peer in file_contexts
                if peer["task_id"] != task_id
                and group_index_by_task_id[peer["task_id"]] != group_index_by_task_id[task_id]
            ]
            if peers:
                warnings[task_id].append(f"shares files_or_modules with {', '.join(peers)}: {path}")
    return warnings


def _validated_plan_payload(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
) -> dict[str, Any]:
    if hasattr(execution_plan, "model_dump"):
        return execution_plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(execution_plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(execution_plan)


def _task_payloads(value: Any) -> list[dict[str, Any]]:
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
    task_id = _text(task.get("id")) or f"task-{index}"
    return {
        "index": index,
        "task": dict(task),
        "task_id": task_id,
        "depends_on": _strings(task.get("depends_on")),
        "files": tuple(
            _dedupe([_normalized_path(path) for path in _strings(task.get("files_or_modules"))])
        ),
        "milestone": _optional_text(task.get("milestone")),
        "risk_level": (_optional_text(task.get("risk_level")) or "unspecified").lower(),
        "estimated_complexity": (
            _optional_text(task.get("estimated_complexity")) or "unspecified"
        ).lower(),
    }


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


def _complexity_rank(value: str | None) -> int:
    return {
        "none": 0,
        "trivial": 0,
        "low": 1,
        "small": 1,
        "medium": 2,
        "moderate": 2,
        "high": 3,
        "large": 3,
        "critical": 4,
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


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _normalized_path(value: str) -> str:
    return value.strip().replace("\\", "/").strip("/")


def _dedupe(values: list[_T] | tuple[_T, ...]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "DEFAULT_BRANCH_BASE",
    "TaskBranchCandidate",
    "generate_task_branch_plan",
    "task_branch_plan_to_dict",
]
