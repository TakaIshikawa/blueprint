"""Compact per-task execution context for autonomous handoffs."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
import json
from typing import Any


_BRIEF_FIELDS = (
    "id",
    "source_brief_id",
    "title",
    "status",
    "domain",
    "target_user",
    "workflow_context",
    "problem_statement",
    "mvp_goal",
    "scope",
    "non_goals",
    "assumptions",
    "architecture_notes",
    "data_requirements",
    "integration_points",
    "risks",
    "validation_plan",
    "definition_of_done",
)

_PLAN_FIELDS = (
    "id",
    "implementation_brief_id",
    "status",
    "target_engine",
    "target_repo",
    "project_type",
    "test_strategy",
    "handoff_prompt",
)

_TASK_FIELDS = (
    "id",
    "title",
    "description",
    "status",
    "milestone",
    "owner_type",
    "suggested_engine",
    "estimated_complexity",
    "estimated_hours",
    "depends_on",
    "files_or_modules",
    "acceptance_criteria",
    "test_command",
    "risk_level",
    "risk",
    "metadata",
)


def compact_task_context(
    plan: dict[str, Any],
    brief: dict[str, Any],
    task_id: str,
    *,
    include_dependents: bool = False,
) -> dict[str, Any]:
    """Return a deterministic, JSON-serializable context payload for one task."""
    tasks = [_mapping(task) for task in plan.get("tasks", []) if isinstance(task, Mapping)]
    tasks_by_id = {
        str(task["id"]): task
        for task in tasks
        if task.get("id") is not None and str(task.get("id")) != ""
    }
    selected = tasks_by_id.get(str(task_id))
    if selected is None:
        raise ValueError(f"Unknown task ID: {task_id}")

    dependency_ids = _unique_ids(selected.get("depends_on"))
    dependency_tasks = [
        _compact_task(tasks_by_id[dependency_id])
        for dependency_id in dependency_ids
        if dependency_id in tasks_by_id and dependency_id != str(task_id)
    ]

    payload: dict[str, Any] = {
        "brief": _compact_mapping(brief, _BRIEF_FIELDS),
        "plan": {
            **_compact_mapping(plan, _PLAN_FIELDS),
            "task_count": len(tasks),
        },
        "task": _compact_task(selected),
        "dependency_tasks": dependency_tasks,
        "validation_context": _validation_context(plan, brief, selected, dependency_tasks),
    }

    if include_dependents:
        dependency_set = {task["id"] for task in dependency_tasks}
        payload["dependent_tasks"] = [
            _compact_task(task)
            for task in sorted(tasks, key=_task_sort_key)
            if str(task.get("id") or "") not in {str(task_id), *dependency_set}
            and str(task_id) in _unique_ids(task.get("depends_on"))
        ]

    return _json_ready(payload)


def _compact_task(task: Mapping[str, Any]) -> dict[str, Any]:
    compacted = _compact_mapping(task, _TASK_FIELDS)
    for field_name in sorted(task):
        if field_name.startswith("risk_") and field_name not in compacted:
            compacted[field_name] = task[field_name]
    return compacted


def _validation_context(
    plan: Mapping[str, Any],
    brief: Mapping[str, Any],
    selected: Mapping[str, Any],
    dependency_tasks: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "test_strategy": plan.get("test_strategy"),
        "validation_plan": brief.get("validation_plan"),
        "definition_of_done": brief.get("definition_of_done"),
        "task_acceptance_criteria": selected.get("acceptance_criteria") or [],
        "task_test_command": selected.get("test_command"),
        "dependency_acceptance_criteria": [
            {
                "task_id": task.get("id"),
                "acceptance_criteria": task.get("acceptance_criteria") or [],
            }
            for task in dependency_tasks
        ],
    }


def _compact_mapping(source: Mapping[str, Any], fields: Sequence[str]) -> dict[str, Any]:
    return {field: source[field] for field in fields if field in source}


def _mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return dict(value)


def _unique_ids(value: Any) -> list[str]:
    ids: list[str] = []
    for item in _list(value):
        item_id = str(item)
        if item_id and item_id not in ids:
            ids.append(item_id)
    return ids


def _list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return [value]


def _task_sort_key(task: Mapping[str, Any]) -> tuple[str, str]:
    return (str(task.get("id") or ""), str(task.get("title") or ""))


def _json_ready(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _json_ready(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, list | tuple):
        return [_json_ready(item) for item in value]
    if isinstance(value, set | frozenset):
        return [_json_ready(item) for item in sorted(value, key=str)]
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, str | int | float | bool) or value is None:
        return value

    try:
        json.dumps(value)
    except TypeError:
        return str(value)
    return value
