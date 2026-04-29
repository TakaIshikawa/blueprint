"""Execution task file path hygiene audit."""

from __future__ import annotations

from dataclasses import dataclass, field
import posixpath
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Literal


FilePathHygieneCode = Literal[
    "absolute_path",
    "parent_directory_traversal",
    "empty_path_entry",
    "duplicate_path",
    "broad_root_glob",
]
Severity = Literal["blocking", "warning"]

_BROAD_ROOT_GLOBS = {"*", "**/*", "."}


@dataclass(frozen=True)
class FilePathHygieneFinding:
    """A single unsafe or ambiguous task path finding."""

    code: FilePathHygieneCode
    severity: Severity
    task_id: str
    task_title: str
    path: str
    normalized_path: str
    message: str
    remediation: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "task_id": self.task_id,
            "task_title": self.task_title,
            "path": self.path,
            "normalized_path": self.normalized_path,
            "message": self.message,
            "remediation": self.remediation,
        }


@dataclass(frozen=True)
class FilePathHygieneResult:
    """File path hygiene audit result for an execution plan."""

    plan_id: str
    task_count: int
    findings: list[FilePathHygieneFinding] = field(default_factory=list)

    @property
    def blocking_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "blocking")

    @property
    def warning_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "warning")

    @property
    def passed(self) -> bool:
        return not self.findings

    def findings_by_severity(self) -> dict[str, list[FilePathHygieneFinding]]:
        return {
            "blocking": [
                finding for finding in self.findings if finding.severity == "blocking"
            ],
            "warning": [
                finding for finding in self.findings if finding.severity == "warning"
            ],
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "passed": self.passed,
            "summary": {
                "blocking": self.blocking_count,
                "warning": self.warning_count,
                "findings": len(self.findings),
                "tasks": self.task_count,
            },
            "findings": [finding.to_dict() for finding in self.findings],
        }


def audit_file_path_hygiene(plan: dict[str, Any]) -> FilePathHygieneResult:
    """Find unsafe or overly broad task files_or_modules path hints."""
    tasks = _list_of_dicts(plan.get("tasks"))
    findings: list[FilePathHygieneFinding] = []

    for task in sorted(tasks, key=lambda item: (_task_id(item), _task_title(item))):
        findings.extend(_task_path_findings(task))

    return FilePathHygieneResult(
        plan_id=str(plan.get("id") or ""),
        task_count=len(tasks),
        findings=sorted(
            findings,
            key=lambda finding: (
                finding.task_id,
                finding.normalized_path,
                finding.path,
                finding.code,
            ),
        ),
    )


def _task_path_findings(task: dict[str, Any]) -> list[FilePathHygieneFinding]:
    findings: list[FilePathHygieneFinding] = []
    task_id = _task_id(task)
    task_title = _task_title(task)
    seen_paths: set[str] = set()
    duplicate_paths: set[str] = set()

    for path in _path_entries(task.get("files_or_modules")):
        normalized_path = _normalized_path(path)
        stripped_path = path.strip()

        if not stripped_path:
            findings.append(
                FilePathHygieneFinding(
                    code="empty_path_entry",
                    severity="blocking",
                    task_id=task_id,
                    task_title=task_title,
                    path=path,
                    normalized_path="",
                    message=f"Task {task_id} has a blank files_or_modules entry.",
                    remediation="Remove blank entries or replace them with repo-relative paths.",
                )
            )
            continue

        if _is_absolute_path(stripped_path):
            findings.append(
                FilePathHygieneFinding(
                    code="absolute_path",
                    severity="blocking",
                    task_id=task_id,
                    task_title=task_title,
                    path=path,
                    normalized_path=normalized_path,
                    message=f"Task {task_id} uses an absolute path: {stripped_path}",
                    remediation=(
                        "Use a repository-relative path instead of a machine-local "
                        "absolute path."
                    ),
                )
            )

        if _has_parent_directory_traversal(stripped_path):
            findings.append(
                FilePathHygieneFinding(
                    code="parent_directory_traversal",
                    severity="blocking",
                    task_id=task_id,
                    task_title=task_title,
                    path=path,
                    normalized_path=normalized_path,
                    message=(
                        f"Task {task_id} uses parent-directory traversal: "
                        f"{stripped_path}"
                    ),
                    remediation=(
                        "Remove '..' segments and scope the task to paths inside "
                        "the repository."
                    ),
                )
            )

        if normalized_path in _BROAD_ROOT_GLOBS:
            findings.append(
                FilePathHygieneFinding(
                    code="broad_root_glob",
                    severity="warning",
                    task_id=task_id,
                    task_title=task_title,
                    path=path,
                    normalized_path=normalized_path,
                    message=(
                        f"Task {task_id} uses a broad root-level path scope: "
                        f"{stripped_path}"
                    ),
                    remediation=(
                        "Replace broad root globs with the narrow files, modules, "
                        "or directories expected to change."
                    ),
                )
            )

        if normalized_path in seen_paths and normalized_path not in duplicate_paths:
            duplicate_paths.add(normalized_path)
            findings.append(
                FilePathHygieneFinding(
                    code="duplicate_path",
                    severity="warning",
                    task_id=task_id,
                    task_title=task_title,
                    path=path,
                    normalized_path=normalized_path,
                    message=(
                        f"Task {task_id} lists the same normalized path more "
                        f"than once: {normalized_path}"
                    ),
                    remediation="Remove duplicate files_or_modules entries from the task.",
                )
            )
        seen_paths.add(normalized_path)

    return findings


def _path_entries(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item if isinstance(item, str) else str(item or "") for item in value]


def _normalized_path(path: str) -> str:
    normalized = posixpath.normpath(path.strip().replace("\\", "/"))
    return "" if normalized == "." and not path.strip() else normalized


def _is_absolute_path(path: str) -> bool:
    return PurePosixPath(path.replace("\\", "/")).is_absolute() or PureWindowsPath(
        path
    ).is_absolute()


def _has_parent_directory_traversal(path: str) -> bool:
    return ".." in PurePosixPath(path.replace("\\", "/")).parts


def _task_id(task: dict[str, Any]) -> str:
    return str(task.get("id") or "")


def _task_title(task: dict[str, Any]) -> str:
    return str(task.get("title") or "")


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
