"""Estimate file ownership coverage for execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import posixpath
from typing import Any, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


_METADATA_OWNER_KEYS = (
    "owner",
    "assignee",
    "responsible",
    "accountable",
    "owned_by",
)


@dataclass(frozen=True, slots=True)
class TaskPathOwnership:
    """Ownership status for one task file or module path."""

    task_id: str
    title: str
    path: str
    ownership_status: str
    owner_type: str | None = None
    suggested_engine: str | None = None
    metadata_owner: str | None = None
    owner_label: str | None = None
    shared_with_task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "path": self.path,
            "ownership_status": self.ownership_status,
            "owner_type": self.owner_type,
            "suggested_engine": self.suggested_engine,
            "metadata_owner": self.metadata_owner,
            "owner_label": self.owner_label,
            "shared_with_task_ids": list(self.shared_with_task_ids),
        }


@dataclass(frozen=True, slots=True)
class TaskOwnerClarification:
    """A task with file paths but no usable ownership assignment."""

    task_id: str
    title: str
    paths: tuple[str, ...]
    suggested_engine: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "paths": list(self.paths),
            "suggested_engine": self.suggested_engine,
        }


@dataclass(frozen=True, slots=True)
class SharedTaskPath:
    """A file or module path assigned to multiple owners."""

    path: str
    owners: tuple[str, ...]
    task_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "path": self.path,
            "owners": list(self.owners),
            "task_ids": list(self.task_ids),
        }


@dataclass(frozen=True, slots=True)
class TaskFileOwnershipReport:
    """Ownership coverage report for execution-plan file paths."""

    plan_id: str | None
    task_count: int
    path_count: int
    owned_paths: tuple[str, ...] = field(default_factory=tuple)
    unowned_paths: tuple[str, ...] = field(default_factory=tuple)
    shared_paths: tuple[SharedTaskPath, ...] = field(default_factory=tuple)
    task_paths: tuple[TaskPathOwnership, ...] = field(default_factory=tuple)
    tasks_needing_owner_clarification: tuple[TaskOwnerClarification, ...] = (
        field(default_factory=tuple)
    )

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "task_count": self.task_count,
            "path_count": self.path_count,
            "owned_paths": list(self.owned_paths),
            "unowned_paths": list(self.unowned_paths),
            "shared_paths": [path.to_dict() for path in self.shared_paths],
            "task_paths": [task_path.to_dict() for task_path in self.task_paths],
            "tasks_needing_owner_clarification": [
                task.to_dict() for task in self.tasks_needing_owner_clarification
            ],
        }


def estimate_task_file_ownership(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> TaskFileOwnershipReport:
    """Estimate ownership quality for every task files_or_modules entry."""
    payload = _plan_payload(plan)
    tasks = _task_payloads(payload.get("tasks"))
    task_path_records = _task_path_records(tasks)
    shared_paths = _shared_paths(task_path_records)
    shared_path_ids = {shared.path: shared for shared in shared_paths}

    task_paths = tuple(
        _task_path_ownership(record, shared_path_ids.get(record["path"]))
        for record in task_path_records
    )

    return TaskFileOwnershipReport(
        plan_id=_optional_text(payload.get("id")),
        task_count=len(tasks),
        path_count=len(task_paths),
        owned_paths=tuple(
            sorted(
                {
                    task_path.path
                    for task_path in task_paths
                    if task_path.ownership_status in {"owned", "shared"}
                }
            )
        ),
        unowned_paths=tuple(
            sorted(
                {
                    task_path.path
                    for task_path in task_paths
                    if task_path.ownership_status == "unowned"
                }
            )
        ),
        shared_paths=tuple(shared_paths),
        task_paths=task_paths,
        tasks_needing_owner_clarification=tuple(_clarifications(task_path_records)),
    )


def task_file_ownership_report_to_dict(
    report: TaskFileOwnershipReport,
) -> dict[str, Any]:
    """Serialize a task file ownership report to a dictionary."""
    return report.to_dict()


task_file_ownership_report_to_dict.__test__ = False


def _task_path_ownership(
    record: dict[str, Any],
    shared_path: SharedTaskPath | None,
) -> TaskPathOwnership:
    has_owner = bool(record["owner_type"] or record["metadata_owner"])
    ownership_status = "unowned"
    if has_owner:
        ownership_status = "shared" if shared_path else "owned"

    shared_with_task_ids: tuple[str, ...] = ()
    if shared_path:
        shared_with_task_ids = tuple(
            task_id for task_id in shared_path.task_ids if task_id != record["task_id"]
        )

    return TaskPathOwnership(
        task_id=record["task_id"],
        title=record["title"],
        path=record["path"],
        ownership_status=ownership_status,
        owner_type=record["owner_type"],
        suggested_engine=record["suggested_engine"],
        metadata_owner=record["metadata_owner"],
        owner_label=record["owner_label"],
        shared_with_task_ids=shared_with_task_ids,
    )


def _task_path_records(tasks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        title = _optional_text(task.get("title")) or task_id
        owner_type = _optional_text(task.get("owner_type"))
        suggested_engine = _optional_text(task.get("suggested_engine"))
        metadata_owner = _metadata_owner(task.get("metadata"))
        owner_label = _owner_label(owner_type, suggested_engine, metadata_owner)

        for path in _paths(task.get("files_or_modules")):
            records.append(
                {
                    "task_id": task_id,
                    "title": title,
                    "path": path,
                    "owner_type": owner_type,
                    "suggested_engine": suggested_engine,
                    "metadata_owner": metadata_owner,
                    "owner_label": owner_label,
                }
            )
    return records


def _shared_paths(records: list[dict[str, Any]]) -> list[SharedTaskPath]:
    by_path: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        by_path.setdefault(record["path"], []).append(record)

    shared: list[SharedTaskPath] = []
    for path, path_records in by_path.items():
        owners = sorted(
            {
                record["owner_label"]
                for record in path_records
                if record["owner_label"]
            }
        )
        if len(owners) <= 1:
            continue
        shared.append(
            SharedTaskPath(
                path=path,
                owners=tuple(owners),
                task_ids=tuple(sorted({record["task_id"] for record in path_records})),
            )
        )

    return sorted(shared, key=lambda item: item.path)


def _clarifications(records: list[dict[str, Any]]) -> list[TaskOwnerClarification]:
    by_task: dict[str, dict[str, Any]] = {}
    for record in records:
        if record["owner_type"] or record["metadata_owner"]:
            continue
        task = by_task.setdefault(
            record["task_id"],
            {
                "task_id": record["task_id"],
                "title": record["title"],
                "paths": [],
                "suggested_engine": record["suggested_engine"],
            },
        )
        if record["path"] not in task["paths"]:
            task["paths"].append(record["path"])

    return [
        TaskOwnerClarification(
            task_id=task["task_id"],
            title=task["title"],
            paths=tuple(task["paths"]),
            suggested_engine=task["suggested_engine"],
        )
        for task in by_task.values()
    ]


def _owner_label(
    owner_type: str | None,
    suggested_engine: str | None,
    metadata_owner: str | None,
) -> str | None:
    if metadata_owner:
        return metadata_owner
    if owner_type and suggested_engine:
        return f"{owner_type}: {suggested_engine}"
    return owner_type


def _metadata_owner(value: Any) -> str | None:
    if not isinstance(value, Mapping):
        return None
    for key in _METADATA_OWNER_KEYS:
        text = _optional_text(value.get(key))
        if text:
            return text
    return None


def _paths(value: Any) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()
    if not isinstance(value, list):
        return paths

    for item in value:
        text = _optional_text(item)
        if not text:
            continue
        path = _normalized_path(text)
        if path and path not in seen:
            paths.append(path)
            seen.add(path)
    return paths


def _normalized_path(path: str) -> str:
    return posixpath.normpath(path.replace("\\", "/"))


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


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "SharedTaskPath",
    "TaskFileOwnershipReport",
    "TaskOwnerClarification",
    "TaskPathOwnership",
    "estimate_task_file_ownership",
    "task_file_ownership_report_to_dict",
]
