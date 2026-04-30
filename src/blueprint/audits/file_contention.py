"""File contention audit for parallel execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import posixpath
from typing import Any, Literal


Severity = Literal["low", "medium", "high"]

_IGNORED_STATUSES = {"completed", "skipped"}
_HIGH_RISK_LEVELS = {"high", "critical"}
_MEDIUM_RISK_LEVELS = {"medium", "moderate"}
_PLACEHOLDER_PATHS = {".", "*", "**", "**/*"}


@dataclass(frozen=True)
class FileContentionTask:
    """A task participating in a file contention finding."""

    task_id: str
    title: str
    milestone: str | None
    status: str | None
    risk_level: str | None
    matched_paths: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "title": self.title,
            "milestone": self.milestone,
            "status": self.status,
            "risk_level": self.risk_level,
            "matched_paths": self.matched_paths,
        }


@dataclass(frozen=True)
class FileContentionFinding:
    """A shared file or module path that may collide in parallel execution."""

    path: str
    severity: Severity
    task_count: int
    task_ids: list[str] = field(default_factory=list)
    task_titles: list[str] = field(default_factory=list)
    milestones: list[str] = field(default_factory=list)
    statuses: list[str] = field(default_factory=list)
    tasks: list[FileContentionTask] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "severity": self.severity,
            "task_count": self.task_count,
            "task_ids": self.task_ids,
            "task_titles": self.task_titles,
            "milestones": self.milestones,
            "statuses": self.statuses,
            "tasks": [task.to_dict() for task in self.tasks],
        }


@dataclass(frozen=True)
class FileContentionResult:
    """File contention audit result for an execution plan."""

    plan_id: str
    task_count: int
    analyzed_task_count: int
    findings: list[FileContentionFinding] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.findings

    @property
    def finding_count(self) -> int:
        return len(self.findings)

    @property
    def high_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "high")

    @property
    def medium_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "medium")

    @property
    def low_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "low")

    def findings_by_severity(self) -> dict[str, list[FileContentionFinding]]:
        return {
            "high": [
                finding for finding in self.findings if finding.severity == "high"
            ],
            "medium": [
                finding for finding in self.findings if finding.severity == "medium"
            ],
            "low": [
                finding for finding in self.findings if finding.severity == "low"
            ],
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "passed": self.passed,
            "summary": {
                "findings": self.finding_count,
                "high": self.high_count,
                "medium": self.medium_count,
                "low": self.low_count,
                "tasks": self.task_count,
                "analyzed_tasks": self.analyzed_task_count,
            },
            "findings": [finding.to_dict() for finding in self.findings],
        }


def audit_file_contention(
    execution_plan: dict[str, Any],
    *,
    ignore_completed_skipped_tasks: bool = True,
    exact_path_matches_only: bool = False,
    ignore_empty_or_directory_placeholders: bool = True,
) -> FileContentionResult:
    """Find execution tasks whose files_or_modules paths may collide."""
    tasks = _list_of_dicts(execution_plan.get("tasks"))
    analyzed_tasks = [
        task
        for task in tasks
        if not (
            ignore_completed_skipped_tasks
            and _normalized_status(task) in _IGNORED_STATUSES
        )
    ]
    task_paths = _task_paths(
        analyzed_tasks,
        ignore_empty_or_directory_placeholders=ignore_empty_or_directory_placeholders,
    )
    findings = (
        _exact_findings(task_paths)
        if exact_path_matches_only
        else _overlap_findings(task_paths)
    )

    return FileContentionResult(
        plan_id=str(execution_plan.get("id") or ""),
        task_count=len(tasks),
        analyzed_task_count=len(analyzed_tasks),
        findings=sorted(
            findings,
            key=lambda finding: (
                {"high": 0, "medium": 1, "low": 2}[finding.severity],
                finding.path,
                finding.task_ids,
            ),
        ),
    )


def _exact_findings(task_paths: dict[str, dict[str, Any]]) -> list[FileContentionFinding]:
    paths_to_task_ids: dict[str, set[str]] = {}
    for task_id, payload in task_paths.items():
        for path in payload["paths"]:
            paths_to_task_ids.setdefault(path, set()).add(task_id)

    return [
        _finding(path, task_ids, task_paths, matched_path=path)
        for path, task_ids in paths_to_task_ids.items()
        if len(task_ids) > 1
    ]


def _overlap_findings(task_paths: dict[str, dict[str, Any]]) -> list[FileContentionFinding]:
    all_paths = sorted(
        {path for payload in task_paths.values() for path in payload["paths"]},
        key=lambda path: (path.count("/"), path),
    )
    findings: list[FileContentionFinding] = []
    seen_groups: list[tuple[str, tuple[str, ...]]] = []

    for path in all_paths:
        matched_task_paths = _matched_task_paths(path, task_paths)
        if len(matched_task_paths) <= 1:
            continue

        task_ids = frozenset(matched_task_paths)
        group_key = tuple(sorted(task_ids))
        if any(
            previous_group_key == group_key and _paths_overlap(previous_path, path)
            for previous_path, previous_group_key in seen_groups
        ):
            continue

        seen_groups.append((path, group_key))
        findings.append(_finding(path, task_ids, task_paths, matched_task_paths))

    return findings


def _matched_task_paths(
    path: str,
    task_paths: dict[str, dict[str, Any]],
) -> dict[str, list[str]]:
    matched: dict[str, list[str]] = {}
    for task_id, payload in task_paths.items():
        overlapping_paths = [
            candidate
            for candidate in payload["paths"]
            if _paths_overlap(path, candidate)
        ]
        if overlapping_paths:
            matched[task_id] = sorted(overlapping_paths)
    return matched


def _finding(
    path: str,
    task_ids: set[str] | frozenset[str],
    task_paths: dict[str, dict[str, Any]],
    matched_paths_by_task_id: dict[str, list[str]] | None = None,
    *,
    matched_path: str | None = None,
) -> FileContentionFinding:
    tasks: list[FileContentionTask] = []
    for task_id in sorted(task_ids):
        task = task_paths[task_id]["task"]
        matched_paths = (
            matched_paths_by_task_id.get(task_id, [])
            if matched_paths_by_task_id is not None
            else [matched_path or path]
        )
        tasks.append(
            FileContentionTask(
                task_id=task_id,
                title=_task_title(task),
                milestone=_optional_text(task.get("milestone")),
                status=_optional_text(task.get("status")),
                risk_level=_risk_level(task),
                matched_paths=matched_paths,
            )
        )

    return FileContentionFinding(
        path=path,
        severity=_severity(tasks),
        task_count=len(tasks),
        task_ids=[task.task_id for task in tasks],
        task_titles=[task.title for task in tasks],
        milestones=_unique_present(task.milestone for task in tasks),
        statuses=_unique_present(task.status for task in tasks),
        tasks=tasks,
    )


def _severity(tasks: list[FileContentionTask]) -> Severity:
    risk_levels = {_normalized_text(task.risk_level) for task in tasks}
    task_count = len(tasks)
    if task_count >= 4 or risk_levels & _HIGH_RISK_LEVELS:
        return "high"
    if task_count >= 3 and risk_levels & _MEDIUM_RISK_LEVELS:
        return "high"
    if task_count >= 3 or risk_levels & _MEDIUM_RISK_LEVELS:
        return "medium"
    return "low"


def _task_paths(
    tasks: list[dict[str, Any]],
    *,
    ignore_empty_or_directory_placeholders: bool,
) -> dict[str, dict[str, Any]]:
    payload: dict[str, dict[str, Any]] = {}
    for task in tasks:
        task_id = _task_id(task)
        if not task_id:
            continue

        paths = _paths(
            task.get("files_or_modules"),
            ignore_empty_or_directory_placeholders=(
                ignore_empty_or_directory_placeholders
            ),
        )
        if paths:
            payload[task_id] = {"task": task, "paths": paths}
    return payload


def _paths(
    value: Any,
    *,
    ignore_empty_or_directory_placeholders: bool,
) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()
    for item in _string_list(value):
        if ignore_empty_or_directory_placeholders and _is_ignored_placeholder(item):
            continue

        normalized = _normalized_path(item)
        if ignore_empty_or_directory_placeholders and _is_ignored_normalized_path(
            normalized
        ):
            continue
        if normalized and normalized not in seen:
            seen.add(normalized)
            paths.append(normalized)
    return paths


def _paths_overlap(left: str, right: str) -> bool:
    return (
        left == right
        or right.startswith(f"{left}/")
        or left.startswith(f"{right}/")
    )


def _is_ignored_placeholder(path: str) -> bool:
    stripped = path.strip()
    return not stripped or stripped.endswith("/")


def _is_ignored_normalized_path(path: str) -> bool:
    return not path or path in _PLACEHOLDER_PATHS


def _normalized_path(path: str) -> str:
    normalized = posixpath.normpath(path.strip().replace("\\", "/"))
    return "" if normalized == "." and not path.strip() else normalized


def _risk_level(task: dict[str, Any]) -> str | None:
    value = task.get("risk_level") or task.get("risk")
    return _optional_text(value)


def _task_id(task: dict[str, Any]) -> str:
    return str(task.get("id") or "")


def _task_title(task: dict[str, Any]) -> str:
    return str(task.get("title") or "")


def _normalized_status(task: dict[str, Any]) -> str:
    return _normalized_text(task.get("status"))


def _normalized_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _unique_present(values: Any) -> list[str]:
    return sorted({value for value in values if value})


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item if isinstance(item, str) else str(item or "") for item in value]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
