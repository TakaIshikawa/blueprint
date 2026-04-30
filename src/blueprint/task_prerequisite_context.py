"""Build concise prerequisite context for an execution-plan task."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


@dataclass(frozen=True, slots=True)
class TaskPrerequisiteSummary:
    """Summary of one prerequisite task."""

    task_id: str
    title: str
    status: str
    files_or_modules: tuple[str, ...] = field(default_factory=tuple)
    acceptance_criteria: tuple[str, ...] = field(default_factory=tuple)
    test_command: str | None = None
    evidence: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "status": self.status,
            "files_or_modules": list(self.files_or_modules),
            "acceptance_criteria": list(self.acceptance_criteria),
            "test_command": self.test_command,
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class TaskPrerequisiteContext:
    """Prerequisite context for a selected execution-plan task."""

    plan_id: str | None
    task_id: str
    title: str
    direct_dependencies: tuple[TaskPrerequisiteSummary, ...] = field(default_factory=tuple)
    transitive_dependencies: tuple[TaskPrerequisiteSummary, ...] = field(default_factory=tuple)
    unresolved_dependency_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "task_id": self.task_id,
            "title": self.title,
            "direct_dependencies": [
                dependency.to_dict() for dependency in self.direct_dependencies
            ],
            "transitive_dependencies": [
                dependency.to_dict() for dependency in self.transitive_dependencies
            ],
            "unresolved_dependency_ids": list(self.unresolved_dependency_ids),
        }


def build_task_prerequisite_context(
    execution_plan: Mapping[str, Any] | ExecutionPlan,
    task_id: str,
) -> TaskPrerequisiteContext:
    """Return dependency context for one task in an execution plan."""
    payload = _plan_payload(execution_plan)
    tasks = _task_payloads(payload.get("tasks"))
    task_records = _task_records(tasks)
    tasks_by_id = {record["task_id"]: record for record in task_records}
    selected_task_id = _optional_text(task_id)

    if not selected_task_id or selected_task_id not in tasks_by_id:
        raise ValueError(f"Unknown task_id: {task_id!r}")

    selected_task = tasks_by_id[selected_task_id]
    direct_ids = _dependency_ids(selected_task["task"])
    ancestor_ids, unresolved_dependency_ids = _ancestor_dependency_ids(
        selected_task_id,
        tasks_by_id,
    )
    direct_known_ids = [
        dependency_id for dependency_id in direct_ids if dependency_id in tasks_by_id
    ]
    transitive_ids = [
        dependency_id
        for dependency_id in ancestor_ids
        if dependency_id not in set(direct_known_ids)
    ]

    return TaskPrerequisiteContext(
        plan_id=_optional_text(payload.get("id")),
        task_id=selected_task_id,
        title=selected_task["title"],
        direct_dependencies=tuple(
            _summary(tasks_by_id[dependency_id])
            for dependency_id in _topological_order(direct_known_ids, tasks_by_id)
        ),
        transitive_dependencies=tuple(
            _summary(tasks_by_id[dependency_id])
            for dependency_id in _topological_order(transitive_ids, tasks_by_id)
        ),
        unresolved_dependency_ids=tuple(unresolved_dependency_ids),
    )


def task_prerequisite_context_to_dict(
    context: TaskPrerequisiteContext,
) -> dict[str, Any]:
    """Serialize a task prerequisite context to a dictionary."""
    return context.to_dict()


task_prerequisite_context_to_dict.__test__ = False


def _ancestor_dependency_ids(
    task_id: str,
    tasks_by_id: dict[str, dict[str, Any]],
) -> tuple[list[str], list[str]]:
    ancestor_ids: set[str] = set()
    unresolved_dependency_ids: list[str] = []
    visiting: set[str] = set()

    def visit(current_task_id: str) -> None:
        if current_task_id in visiting:
            return
        visiting.add(current_task_id)
        for dependency_id in _dependency_ids(tasks_by_id[current_task_id]["task"]):
            dependency = tasks_by_id.get(dependency_id)
            if dependency is None:
                if dependency_id not in unresolved_dependency_ids:
                    unresolved_dependency_ids.append(dependency_id)
                continue
            ancestor_ids.add(dependency_id)
            visit(dependency_id)
        visiting.remove(current_task_id)

    visit(task_id)
    ancestor_ids.discard(task_id)
    return (_topological_order(ancestor_ids, tasks_by_id), unresolved_dependency_ids)


def _topological_order(
    task_ids: set[str] | list[str],
    tasks_by_id: dict[str, dict[str, Any]],
) -> list[str]:
    target_ids = set(task_ids)
    ordered: list[str] = []
    temporary: set[str] = set()
    permanent: set[str] = set()

    def visit(task_id: str) -> None:
        if task_id in permanent or task_id in temporary:
            return
        temporary.add(task_id)
        task = tasks_by_id.get(task_id)
        if task is not None:
            for dependency_id in _dependency_ids(task["task"]):
                if dependency_id in target_ids:
                    visit(dependency_id)
        temporary.remove(task_id)
        permanent.add(task_id)
        if task_id in target_ids:
            ordered.append(task_id)

    for task_id in sorted(target_ids, key=lambda item: tasks_by_id[item]["index"]):
        visit(task_id)
    return ordered


def _summary(record: dict[str, Any]) -> TaskPrerequisiteSummary:
    task = record["task"]
    return TaskPrerequisiteSummary(
        task_id=record["task_id"],
        title=record["title"],
        status=_task_status(task),
        files_or_modules=tuple(_string_list(task.get("files_or_modules"))),
        acceptance_criteria=tuple(_string_list(task.get("acceptance_criteria"))),
        test_command=_optional_text(task.get("test_command")),
        evidence=_completion_evidence(task),
    )


def _task_records(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        records.append(
            {
                "index": index,
                "task_id": task_id,
                "title": _optional_text(task.get("title")) or task_id,
                "task": task,
            }
        )
    return records


def _completion_evidence(task: Mapping[str, Any]) -> dict[str, Any]:
    evidence: dict[str, Any] = {}
    _merge_evidence(evidence, "evidence", task.get("evidence"))

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in (
            "evidence",
            "completion_evidence",
            "validation_evidence",
            "artifacts",
        ):
            _merge_evidence(evidence, key, metadata.get(key))
    return evidence


def _merge_evidence(target: dict[str, Any], key: str, value: Any) -> None:
    if isinstance(value, Mapping):
        for nested_key, nested_value in value.items():
            text_key = _optional_text(nested_key)
            if text_key:
                target[text_key] = nested_value
        return
    if isinstance(value, list) and value:
        target[key] = value
        return
    text = _optional_text(value)
    if text:
        target[key] = text


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


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


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _task_status(task: Mapping[str, Any]) -> str:
    return (_optional_text(task.get("status")) or "pending").lower()


def _dependency_ids(task: Mapping[str, Any]) -> list[str]:
    return _string_list(task.get("depends_on"))


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [text for item in value if isinstance(item, str) and (text := " ".join(item.split()))]


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


__all__ = [
    "TaskPrerequisiteContext",
    "TaskPrerequisiteSummary",
    "build_task_prerequisite_context",
    "task_prerequisite_context_to_dict",
]
