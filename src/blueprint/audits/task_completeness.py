"""Task completeness audit for autonomous-agent execution readiness."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal


Severity = Literal["blocking", "warning"]

_BLOCKING_PENALTY = 25
_WARNING_PENALTY = 10
_TOKEN_RE = re.compile(r"[a-z0-9]+")


@dataclass(frozen=True)
class TaskCompletenessFinding:
    """A single task completeness finding."""

    severity: Severity
    code: str
    task_id: str
    task_title: str
    field: str
    message: str
    remediation: str
    value: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "severity": self.severity,
            "code": self.code,
            "task_id": self.task_id,
            "task_title": self.task_title,
            "field": self.field,
            "message": self.message,
            "remediation": self.remediation,
        }
        if self.value is not None:
            payload["value"] = self.value
        return payload


@dataclass(frozen=True)
class TaskCompletenessItem:
    """Completeness score and findings for one execution task."""

    task_id: str
    title: str
    score: int
    findings: list[TaskCompletenessFinding] = field(default_factory=list)

    @property
    def blocking_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "blocking")

    @property
    def warning_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "warning")

    @property
    def ready(self) -> bool:
        return self.blocking_count == 0

    def findings_by_severity(self) -> dict[str, list[TaskCompletenessFinding]]:
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
            "task_id": self.task_id,
            "title": self.title,
            "score": self.score,
            "ready": self.ready,
            "findings": [finding.to_dict() for finding in self.findings],
            "summary": {
                "blocking": self.blocking_count,
                "warning": self.warning_count,
            },
        }


@dataclass(frozen=True)
class TaskCompletenessResult:
    """Task completeness audit result for an execution plan."""

    plan_id: str
    score: int
    tasks: list[TaskCompletenessItem] = field(default_factory=list)

    @property
    def blocking_count(self) -> int:
        return sum(task.blocking_count for task in self.tasks)

    @property
    def warning_count(self) -> int:
        return sum(task.warning_count for task in self.tasks)

    @property
    def passed(self) -> bool:
        return self.blocking_count == 0

    @property
    def findings(self) -> list[TaskCompletenessFinding]:
        return [finding for task in self.tasks for finding in task.findings]

    def findings_by_severity(self) -> dict[str, list[TaskCompletenessFinding]]:
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
            "score": self.score,
            "passed": self.passed,
            "summary": {
                "blocking": self.blocking_count,
                "warning": self.warning_count,
                "tasks": len(self.tasks),
            },
            "tasks": [task.to_dict() for task in self.tasks],
            "findings": [finding.to_dict() for finding in self.findings],
        }


def audit_task_completeness(plan_dict: dict[str, Any]) -> TaskCompletenessResult:
    """Score execution tasks for autonomous-agent readiness."""
    plan_id = str(plan_dict.get("id") or "")
    tasks = _list_of_dicts(plan_dict.get("tasks"))
    present_task_ids = {str(task.get("id") or "") for task in tasks if task.get("id")}
    duplicate_titles = _duplicate_normalized_titles(tasks)

    task_items: list[TaskCompletenessItem] = []
    for task in tasks:
        findings = _task_findings(task, present_task_ids, duplicate_titles)
        score = _task_score(findings)
        task_items.append(
            TaskCompletenessItem(
                task_id=str(task.get("id") or ""),
                title=str(task.get("title") or ""),
                score=score,
                findings=findings,
            )
        )

    overall_score = _overall_score(task_items)
    return TaskCompletenessResult(plan_id=plan_id, score=overall_score, tasks=task_items)


def _task_findings(
    task: dict[str, Any],
    present_task_ids: set[str],
    duplicate_titles: set[str],
) -> list[TaskCompletenessFinding]:
    task_id = str(task.get("id") or "")
    task_title = str(task.get("title") or "")
    findings: list[TaskCompletenessFinding] = []

    if not _has_text(task.get("description")):
        findings.append(
            TaskCompletenessFinding(
                severity="blocking",
                code="missing_description",
                task_id=task_id,
                task_title=task_title,
                field="description",
                message=f"Task {task_id} has no actionable description.",
                remediation="Add enough implementation detail for an agent to start work.",
            )
        )

    if not _has_string_list_item(task.get("acceptance_criteria")):
        findings.append(
            TaskCompletenessFinding(
                severity="blocking",
                code="empty_acceptance_criteria",
                task_id=task_id,
                task_title=task_title,
                field="acceptance_criteria",
                message=f"Task {task_id} has no acceptance criteria.",
                remediation="Add observable completion criteria for this task.",
            )
        )

    if not _has_string_list_item(task.get("files_or_modules")):
        findings.append(
            TaskCompletenessFinding(
                severity="blocking",
                code="missing_files_or_modules",
                task_id=task_id,
                task_title=task_title,
                field="files_or_modules",
                message=f"Task {task_id} does not identify files or modules to touch.",
                remediation="List the expected files, modules, packages, or surfaces.",
            )
        )

    for dependency_id in _string_list(task.get("depends_on")):
        if dependency_id not in present_task_ids:
            findings.append(
                TaskCompletenessFinding(
                    severity="blocking",
                    code="unresolved_dependency",
                    task_id=task_id,
                    task_title=task_title,
                    field="depends_on",
                    value=dependency_id,
                    message=f"Task {task_id} depends on missing task {dependency_id}.",
                    remediation="Remove the dependency or add the referenced task.",
                )
            )

    if _normalized_title(task_title) in duplicate_titles:
        findings.append(
            TaskCompletenessFinding(
                severity="warning",
                code="duplicate_task_title",
                task_id=task_id,
                task_title=task_title,
                field="title",
                value=task_title,
                message=f"Task title is duplicated: {task_title}",
                remediation="Rename or merge tasks so each title is distinct.",
            )
        )

    if not _has_text(task.get("suggested_engine")):
        findings.append(
            TaskCompletenessFinding(
                severity="warning",
                code="missing_suggested_engine",
                task_id=task_id,
                task_title=task_title,
                field="suggested_engine",
                message=f"Task {task_id} has no suggested execution engine.",
                remediation="Set the engine best suited for the task.",
            )
        )

    if task.get("status") == "blocked" and not _has_text(_blocked_reason(task)):
        findings.append(
            TaskCompletenessFinding(
                severity="warning",
                code="blocked_without_reason",
                task_id=task_id,
                task_title=task_title,
                field="blocked_reason",
                message=f"Blocked task {task_id} has no blocked_reason.",
                remediation="Explain what is blocking the task and what would unblock it.",
            )
        )

    return findings


def _task_score(findings: list[TaskCompletenessFinding]) -> int:
    penalty = 0
    for finding in findings:
        if finding.severity == "blocking":
            penalty += _BLOCKING_PENALTY
        else:
            penalty += _WARNING_PENALTY
    return max(0, 100 - penalty)


def _overall_score(tasks: list[TaskCompletenessItem]) -> int:
    if not tasks:
        return 100
    return round(sum(task.score for task in tasks) / len(tasks))


def _duplicate_normalized_titles(tasks: list[dict[str, Any]]) -> set[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for task in tasks:
        title = _normalized_title(str(task.get("title") or ""))
        if not title:
            continue
        if title in seen:
            duplicates.add(title)
        seen.add(title)
    return duplicates


def _normalized_title(value: str) -> str:
    return " ".join(_TOKEN_RE.findall(value.lower()))


def _blocked_reason(task: dict[str, Any]) -> Any:
    if _has_text(task.get("blocked_reason")):
        return task.get("blocked_reason")
    metadata = task.get("metadata")
    if isinstance(metadata, dict):
        return metadata.get("blocked_reason")
    return None


def _has_string_list_item(value: Any) -> bool:
    return bool(_string_list(value))


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if _has_text(item)]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())
