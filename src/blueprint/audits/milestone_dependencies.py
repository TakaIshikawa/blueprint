"""Milestone dependency coherence audit for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


MilestoneDependencyCode = Literal[
    "reversed_milestone_dependency",
    "missing_milestone",
    "empty_milestone",
    "cross_milestone_chain",
]

MilestoneDependencySeverity = Literal["error", "warning"]


@dataclass(frozen=True)
class MilestoneDependencyFinding:
    """A milestone ordering or coherence issue found in an execution plan."""

    code: MilestoneDependencyCode
    severity: MilestoneDependencySeverity
    message: str
    milestone: str
    task_id: str | None = None
    dependency_task_id: str | None = None
    dependency_milestone: str | None = None
    chain_task_ids: list[str] = field(default_factory=list)
    chain_milestones: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "milestone": self.milestone,
        }
        if self.task_id is not None:
            payload["task_id"] = self.task_id
        if self.dependency_task_id is not None:
            payload["dependency_task_id"] = self.dependency_task_id
        if self.dependency_milestone is not None:
            payload["dependency_milestone"] = self.dependency_milestone
        if self.chain_task_ids:
            payload["chain_task_ids"] = self.chain_task_ids
        if self.chain_milestones:
            payload["chain_milestones"] = self.chain_milestones
        return payload


@dataclass(frozen=True)
class MilestoneDependencyResult:
    """Milestone dependency audit result for an execution plan."""

    plan_id: str
    declared_milestones: list[str] = field(default_factory=list)
    findings: list[MilestoneDependencyFinding] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.findings

    @property
    def error_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for finding in self.findings if finding.severity == "warning")

    def findings_by_milestone(self) -> dict[str, list[MilestoneDependencyFinding]]:
        grouped: dict[str, list[MilestoneDependencyFinding]] = {}
        for finding in self.findings:
            grouped.setdefault(finding.milestone, []).append(finding)
        return dict(sorted(grouped.items()))

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "ok": self.ok,
            "summary": {
                "errors": self.error_count,
                "warnings": self.warning_count,
                "findings": len(self.findings),
            },
            "declared_milestones": self.declared_milestones,
            "findings": [finding.to_dict() for finding in self.findings],
        }


def audit_milestone_dependencies(plan: dict[str, Any]) -> MilestoneDependencyResult:
    """Detect milestone ordering and dependency coherence issues."""
    tasks = _list_of_dicts(plan.get("tasks"))
    tasks_by_id = {
        task_id: task
        for task in tasks
        if (task_id := _text(task.get("id")))
    }
    declared_milestones = _declared_milestones(plan.get("milestones"))
    milestone_indexes = {
        milestone: index
        for index, milestone in enumerate(declared_milestones)
    }

    findings: list[MilestoneDependencyFinding] = []
    findings.extend(_missing_milestone_findings(tasks, milestone_indexes))
    findings.extend(_empty_milestone_findings(tasks, declared_milestones))
    findings.extend(
        _reversed_dependency_findings(tasks, tasks_by_id, milestone_indexes)
    )
    findings.extend(_cross_milestone_chain_findings(tasks, tasks_by_id))

    return MilestoneDependencyResult(
        plan_id=str(plan.get("id") or ""),
        declared_milestones=declared_milestones,
        findings=_dedupe_findings(findings),
    )


def _missing_milestone_findings(
    tasks: list[dict[str, Any]],
    milestone_indexes: dict[str, int],
) -> list[MilestoneDependencyFinding]:
    findings: list[MilestoneDependencyFinding] = []
    for task in tasks:
        task_id = _text(task.get("id"))
        milestone = _text(task.get("milestone"))
        if not task_id or not milestone or milestone in milestone_indexes:
            continue
        findings.append(
            MilestoneDependencyFinding(
                code="missing_milestone",
                severity="error",
                task_id=task_id,
                milestone=milestone,
                message=(
                    f"Task {task_id} is assigned to undeclared milestone "
                    f"{milestone}."
                ),
            )
        )
    return findings


def _empty_milestone_findings(
    tasks: list[dict[str, Any]],
    declared_milestones: list[str],
) -> list[MilestoneDependencyFinding]:
    used_milestones = {
        milestone
        for task in tasks
        if (milestone := _text(task.get("milestone")))
    }
    return [
        MilestoneDependencyFinding(
            code="empty_milestone",
            severity="warning",
            milestone=milestone,
            message=f"Milestone {milestone} is declared but has no tasks.",
        )
        for milestone in declared_milestones
        if milestone not in used_milestones
    ]


def _reversed_dependency_findings(
    tasks: list[dict[str, Any]],
    tasks_by_id: dict[str, dict[str, Any]],
    milestone_indexes: dict[str, int],
) -> list[MilestoneDependencyFinding]:
    findings: list[MilestoneDependencyFinding] = []
    for task in tasks:
        task_id = _text(task.get("id"))
        task_milestone = _text(task.get("milestone"))
        task_index = milestone_indexes.get(task_milestone)
        if not task_id or task_index is None:
            continue

        for dependency_id in _string_list(task.get("depends_on")):
            dependency = tasks_by_id.get(dependency_id)
            if dependency is None:
                continue
            dependency_milestone = _text(dependency.get("milestone"))
            dependency_index = milestone_indexes.get(dependency_milestone)
            if dependency_index is None or task_index >= dependency_index:
                continue
            findings.append(
                MilestoneDependencyFinding(
                    code="reversed_milestone_dependency",
                    severity="error",
                    task_id=task_id,
                    dependency_task_id=dependency_id,
                    milestone=task_milestone,
                    dependency_milestone=dependency_milestone,
                    message=(
                        f"Task {task_id} in milestone {task_milestone} depends on "
                        f"{dependency_id} in later milestone {dependency_milestone}."
                    ),
                )
            )
    return findings


def _cross_milestone_chain_findings(
    tasks: list[dict[str, Any]],
    tasks_by_id: dict[str, dict[str, Any]],
) -> list[MilestoneDependencyFinding]:
    findings: list[MilestoneDependencyFinding] = []
    for task in tasks:
        task_id = _text(task.get("id"))
        if not task_id:
            continue
        for chain in _dependency_chains(task_id, tasks_by_id):
            milestones = _chain_milestones(chain, tasks_by_id)
            if len(set(milestones)) < 3:
                continue
            findings.append(
                MilestoneDependencyFinding(
                    code="cross_milestone_chain",
                    severity="warning",
                    task_id=task_id,
                    milestone=milestones[0],
                    chain_task_ids=chain,
                    chain_milestones=milestones,
                    message=(
                        "Task dependency chain crosses multiple milestones: "
                        + " -> ".join(
                            f"{chain[index]} ({milestones[index]})"
                            for index in range(len(chain))
                        )
                        + "."
                    ),
                )
            )
    return findings


def _dependency_chains(
    task_id: str,
    tasks_by_id: dict[str, dict[str, Any]],
) -> list[list[str]]:
    task = tasks_by_id.get(task_id)
    if task is None:
        return []

    dependencies = _string_list(task.get("depends_on"))
    if not dependencies:
        return [[task_id]]

    chains: list[list[str]] = []
    for dependency_id in dependencies:
        if dependency_id not in tasks_by_id:
            continue
        for dependency_chain in _dependency_chains_without_cycles(
            dependency_id,
            tasks_by_id,
            visited={task_id},
        ):
            chains.append([task_id, *dependency_chain])
    return chains or [[task_id]]


def _dependency_chains_without_cycles(
    task_id: str,
    tasks_by_id: dict[str, dict[str, Any]],
    *,
    visited: set[str],
) -> list[list[str]]:
    if task_id in visited:
        return [[task_id]]

    task = tasks_by_id.get(task_id)
    if task is None:
        return []

    next_visited = {*visited, task_id}
    dependencies = [
        dependency_id
        for dependency_id in _string_list(task.get("depends_on"))
        if dependency_id in tasks_by_id
    ]
    if not dependencies:
        return [[task_id]]

    chains: list[list[str]] = []
    for dependency_id in dependencies:
        for dependency_chain in _dependency_chains_without_cycles(
            dependency_id,
            tasks_by_id,
            visited=next_visited,
        ):
            chains.append([task_id, *dependency_chain])
    return chains or [[task_id]]


def _chain_milestones(
    chain: list[str],
    tasks_by_id: dict[str, dict[str, Any]],
) -> list[str]:
    return [
        _text(tasks_by_id[task_id].get("milestone")) or "unspecified"
        for task_id in chain
        if task_id in tasks_by_id
    ]


def _dedupe_findings(
    findings: list[MilestoneDependencyFinding],
) -> list[MilestoneDependencyFinding]:
    by_key: dict[
        tuple[str, str, str | None, str | None, tuple[str, ...]],
        MilestoneDependencyFinding,
    ] = {}
    for finding in findings:
        key = (
            finding.code,
            finding.milestone,
            finding.task_id,
            finding.dependency_task_id,
            tuple(finding.chain_task_ids),
        )
        if key not in by_key:
            by_key[key] = finding
    return sorted(
        by_key.values(),
        key=lambda finding: (
            0 if finding.severity == "error" else 1,
            finding.milestone,
            finding.code,
            finding.task_id or "",
            finding.dependency_task_id or "",
            finding.chain_task_ids,
        ),
    )


def _declared_milestones(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    milestones: list[str] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        milestone = _text(item.get("name"))
        if milestone:
            milestones.append(milestone)
    return milestones


def _text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    return ""


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, dict)]
