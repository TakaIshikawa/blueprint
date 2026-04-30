"""Repository change budget audit for execution tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import posixpath
from typing import Any


DEFAULT_MAX_FILES_PER_TASK = 5
DEFAULT_MAX_DIRECTORIES_PER_TASK = 2


@dataclass(frozen=True)
class ChangeBudgetFinding:
    """A task that exceeds the configured repository change budget."""

    task_id: str
    title: str
    file_count: int
    directory_count: int
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable finding payload."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "file_count": self.file_count,
            "directory_count": self.directory_count,
            "recommendation": self.recommendation,
        }


@dataclass(frozen=True)
class ChangeBudgetResult:
    """Change budget audit result for an execution plan."""

    plan_id: str
    task_count: int
    max_files_per_task: int = DEFAULT_MAX_FILES_PER_TASK
    max_directories_per_task: int = DEFAULT_MAX_DIRECTORIES_PER_TASK
    findings: list[ChangeBudgetFinding] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.findings

    @property
    def finding_count(self) -> int:
        return len(self.findings)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable audit payload."""
        return {
            "plan_id": self.plan_id,
            "passed": self.passed,
            "summary": {
                "findings": self.finding_count,
                "tasks": self.task_count,
                "max_files_per_task": self.max_files_per_task,
                "max_directories_per_task": self.max_directories_per_task,
            },
            "findings": [finding.to_dict() for finding in self.findings],
        }


def audit_change_budget(
    execution_plan: dict[str, Any],
    *,
    max_files_per_task: int = DEFAULT_MAX_FILES_PER_TASK,
    max_directories_per_task: int = DEFAULT_MAX_DIRECTORIES_PER_TASK,
) -> ChangeBudgetResult:
    """Flag tasks whose files_or_modules list exceeds change-budget thresholds."""
    tasks = _list_of_dicts(execution_plan.get("tasks"))
    findings: list[ChangeBudgetFinding] = []

    for task in tasks:
        files = _unique_normalized_paths(task.get("files_or_modules"))
        if not files:
            continue

        directories = {_source_directory(path) for path in files}
        file_count = len(files)
        directory_count = len(directories)

        if (
            file_count <= max_files_per_task
            and directory_count <= max_directories_per_task
        ):
            continue

        findings.append(
            ChangeBudgetFinding(
                task_id=str(task.get("id") or ""),
                title=str(task.get("title") or ""),
                file_count=file_count,
                directory_count=directory_count,
                recommendation=_recommendation(
                    file_count=file_count,
                    directory_count=directory_count,
                    max_files_per_task=max_files_per_task,
                    max_directories_per_task=max_directories_per_task,
                ),
            )
        )

    return ChangeBudgetResult(
        plan_id=str(execution_plan.get("id") or ""),
        task_count=len(tasks),
        max_files_per_task=max_files_per_task,
        max_directories_per_task=max_directories_per_task,
        findings=findings,
    )


def _recommendation(
    *,
    file_count: int,
    directory_count: int,
    max_files_per_task: int,
    max_directories_per_task: int,
) -> str:
    return (
        "Split or narrow the task scope before execution: "
        f"it touches {_count_label(file_count, 'file', 'files')} across "
        f"{_count_label(directory_count, 'source directory', 'source directories')} "
        f"(budget: {_count_label(max_files_per_task, 'file', 'files')}, "
        f"{_count_label(max_directories_per_task, 'directory', 'directories')})."
    )


def _source_directory(path: str) -> str:
    parts = [part for part in path.split("/") if part and part != "."]
    if not parts:
        return ""
    if parts[0] == "src":
        if len(parts) >= 3:
            return "/".join(parts[:3])
        return "/".join(parts[:-1] or parts)
    if parts[0] == "tests":
        return "tests"
    return "/".join(parts[:-1] or parts[:1])


def _count_label(count: int, singular: str, plural: str) -> str:
    label = singular if count == 1 else plural
    return f"{count} {label}"


def _unique_normalized_paths(value: Any) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()
    for path in _string_list(value):
        normalized = posixpath.normpath(path.strip().replace("\\", "/"))
        if normalized == ".":
            continue
        if normalized not in seen:
            seen.add(normalized)
            paths.append(normalized)
    return paths


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
