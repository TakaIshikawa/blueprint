"""Structural audit checks for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


Severity = Literal["error", "warning"]


@dataclass(frozen=True)
class PlanAuditIssue:
    """A single structural audit finding."""

    severity: Severity
    code: str
    message: str
    task_id: str | None = None
    dependency_id: str | None = None
    milestone: str | None = None
    cycle: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-serializable issue payload."""
        payload: dict[str, Any] = {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
        }
        if self.task_id is not None:
            payload["task_id"] = self.task_id
        if self.dependency_id is not None:
            payload["dependency_id"] = self.dependency_id
        if self.milestone is not None:
            payload["milestone"] = self.milestone
        if self.cycle is not None:
            payload["cycle"] = self.cycle
        return payload


@dataclass(frozen=True)
class PlanAuditResult:
    """Structural audit result for an execution plan."""

    plan_id: str
    issues: list[PlanAuditIssue] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for issue in self.issues if issue.severity == "warning")

    @property
    def ok(self) -> bool:
        return self.error_count == 0

    def issues_by_severity(self) -> dict[str, list[PlanAuditIssue]]:
        return {
            "error": [issue for issue in self.issues if issue.severity == "error"],
            "warning": [issue for issue in self.issues if issue.severity == "warning"],
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "ok": self.ok,
            "summary": {
                "errors": self.error_count,
                "warnings": self.warning_count,
            },
            "issues": [issue.to_dict() for issue in self.issues],
        }


def audit_execution_plan(plan: dict[str, Any]) -> PlanAuditResult:
    """Validate whether a stored execution plan is structurally executable."""
    plan_id = str(plan.get("id") or "")
    tasks = _list_of_dicts(plan.get("tasks"))
    task_ids = [str(task.get("id") or "") for task in tasks]
    present_task_ids = {task_id for task_id in task_ids if task_id}
    duplicate_task_ids = _duplicates(task_ids)
    milestone_aliases = _milestone_aliases(_list_of_dicts(plan.get("milestones")))

    issues: list[PlanAuditIssue] = []
    issues.extend(_duplicate_task_id_issues(duplicate_task_ids))
    issues.extend(_task_detail_issues(tasks, present_task_ids, milestone_aliases))
    issues.extend(_dependency_cycle_issues(tasks, present_task_ids))

    return PlanAuditResult(plan_id=plan_id, issues=issues)


def _duplicate_task_id_issues(duplicate_task_ids: list[str]) -> list[PlanAuditIssue]:
    return [
        PlanAuditIssue(
            severity="error",
            code="duplicate_task_id",
            task_id=task_id,
            message=f"Task ID appears more than once in the plan payload: {task_id}",
        )
        for task_id in duplicate_task_ids
    ]


def _task_detail_issues(
    tasks: list[dict[str, Any]],
    present_task_ids: set[str],
    milestone_aliases: set[str],
) -> list[PlanAuditIssue]:
    issues: list[PlanAuditIssue] = []

    for task in tasks:
        task_id = str(task.get("id") or "")
        for dependency_id in _string_list(task.get("depends_on")):
            if dependency_id == task_id:
                issues.append(
                    PlanAuditIssue(
                        severity="error",
                        code="self_dependency",
                        task_id=task_id,
                        dependency_id=dependency_id,
                        message=f"Task {task_id} depends on itself",
                    )
                )
            elif dependency_id not in present_task_ids:
                issues.append(
                    PlanAuditIssue(
                        severity="error",
                        code="unknown_dependency",
                        task_id=task_id,
                        dependency_id=dependency_id,
                        message=f"Task {task_id} depends on missing task {dependency_id}",
                    )
                )

        milestone = task.get("milestone")
        if _has_text(milestone) and str(milestone) not in milestone_aliases:
            issues.append(
                PlanAuditIssue(
                    severity="error",
                    code="unknown_milestone",
                    task_id=task_id,
                    milestone=str(milestone),
                    message=f"Task {task_id} is assigned to missing milestone {milestone}",
                )
            )

        if not _has_acceptance_criteria(task.get("acceptance_criteria")):
            issues.append(
                PlanAuditIssue(
                    severity="error",
                    code="empty_acceptance_criteria",
                    task_id=task_id,
                    message=f"Task {task_id} has no acceptance criteria",
                )
            )

        if task.get("status") == "blocked" and not _has_text(task.get("blocked_reason")):
            issues.append(
                PlanAuditIssue(
                    severity="warning",
                    code="blocked_without_reason",
                    task_id=task_id,
                    message=f"Blocked task {task_id} has no blocked_reason",
                )
            )

    return issues


def _dependency_cycle_issues(
    tasks: list[dict[str, Any]],
    present_task_ids: set[str],
) -> list[PlanAuditIssue]:
    adjacency: dict[str, list[str]] = {}
    for task in tasks:
        task_id = str(task.get("id") or "")
        if not task_id:
            continue
        adjacency.setdefault(task_id, [])
        for dependency_id in _string_list(task.get("depends_on")):
            if dependency_id in present_task_ids and dependency_id != task_id:
                adjacency[task_id].append(dependency_id)

    cycles = _find_cycles(adjacency)
    return [
        PlanAuditIssue(
            severity="error",
            code="dependency_cycle",
            message=f"Dependency cycle detected: {' -> '.join(cycle)}",
            cycle=cycle,
        )
        for cycle in cycles
    ]


def _find_cycles(adjacency: dict[str, list[str]]) -> list[list[str]]:
    state: dict[str, str] = {}
    stack: list[str] = []
    cycles: list[list[str]] = []
    seen_cycles: set[tuple[str, ...]] = set()

    def visit(task_id: str) -> None:
        state[task_id] = "visiting"
        stack.append(task_id)

        for dependency_id in adjacency.get(task_id, []):
            if state.get(dependency_id) == "visiting":
                cycle = stack[stack.index(dependency_id) :] + [dependency_id]
                key = _cycle_key(cycle)
                if key not in seen_cycles:
                    seen_cycles.add(key)
                    cycles.append(cycle)
            elif state.get(dependency_id) is None:
                visit(dependency_id)

        stack.pop()
        state[task_id] = "visited"

    for task_id in sorted(adjacency):
        if state.get(task_id) is None:
            visit(task_id)

    return cycles


def _cycle_key(cycle: list[str]) -> tuple[str, ...]:
    nodes = cycle[:-1]
    rotations = [tuple(nodes[index:] + nodes[:index]) for index in range(len(nodes))]
    return min(rotations)


def _milestone_aliases(milestones: list[dict[str, Any]]) -> set[str]:
    aliases: set[str] = set()
    for index, milestone in enumerate(milestones, 1):
        display_name = milestone.get("name") or milestone.get("title") or f"Milestone {index}"
        for alias in (
            milestone.get("id"),
            milestone.get("name"),
            milestone.get("title"),
            display_name,
        ):
            if _has_text(alias):
                aliases.add(str(alias))
    return aliases


def _duplicates(values: list[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if not value:
            continue
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return sorted(duplicates)


def _has_acceptance_criteria(value: Any) -> bool:
    if not isinstance(value, list):
        return False
    return any(_has_text(item) for item in value)


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
