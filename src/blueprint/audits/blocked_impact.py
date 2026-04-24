"""Blocked task downstream impact audit for execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


BlockedImpactSeverity = Literal["low", "medium", "high", "critical"]


@dataclass(frozen=True)
class BlockedTaskImpact:
    """Downstream execution impact for one blocked task."""

    blocked_task_id: str
    blocked_task_title: str
    blocked_reason: str | None
    milestone: str | None
    direct_dependents: list[str] = field(default_factory=list)
    transitive_dependents: list[str] = field(default_factory=list)
    impacted_milestones: list[str] = field(default_factory=list)
    impacted_count: int = 0
    critical_dependency_position: bool = False
    severity: BlockedImpactSeverity = "low"

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic JSON-serializable impact payload."""
        return {
            "blocked_task_id": self.blocked_task_id,
            "blocked_task_title": self.blocked_task_title,
            "blocked_reason": self.blocked_reason,
            "milestone": self.milestone,
            "direct_dependents": self.direct_dependents,
            "transitive_dependents": self.transitive_dependents,
            "impacted_milestones": self.impacted_milestones,
            "impacted_count": self.impacted_count,
            "critical_dependency_position": self.critical_dependency_position,
            "severity": self.severity,
        }


@dataclass(frozen=True)
class BlockedImpactResult:
    """Blocked task impact audit result."""

    plan_id: str
    blocked_tasks: list[BlockedTaskImpact] = field(default_factory=list)

    @property
    def blocked_count(self) -> int:
        return len(self.blocked_tasks)

    @property
    def has_impact(self) -> bool:
        return any(blocked_task.impacted_count > 0 for blocked_task in self.blocked_tasks)

    def to_dict(self) -> dict[str, Any]:
        """Return a deterministic JSON-serializable audit payload."""
        return {
            "plan_id": self.plan_id,
            "blocked_count": self.blocked_count,
            "has_impact": self.has_impact,
            "blocked_tasks": [
                blocked_task.to_dict()
                for blocked_task in self.blocked_tasks
            ],
        }


def audit_blocked_impact(plan: dict[str, Any]) -> BlockedImpactResult:
    """Report how blocked tasks affect direct and downstream execution."""
    tasks = _list_of_dicts(plan.get("tasks"))
    tasks_by_id = {
        _task_id(task): task
        for task in tasks
        if _task_id(task)
    }
    dependents_by_task_id = _dependents_by_task_id(tasks, set(tasks_by_id))
    milestone_order = _milestone_order(plan, tasks)

    blocked_tasks = []
    for task in tasks:
        if task.get("status") != "blocked":
            continue

        blocked_task_id = _task_id(task)
        direct_dependents = dependents_by_task_id.get(blocked_task_id, [])
        downstream_dependents = _downstream_dependents(
            blocked_task_id,
            dependents_by_task_id,
        )
        direct_dependent_ids = set(direct_dependents)
        transitive_dependents = [
            dependent_id
            for dependent_id in downstream_dependents
            if dependent_id not in direct_dependent_ids
        ]
        impacted_task_ids = _unique_in_order(direct_dependents + transitive_dependents)
        impacted_milestones = _impacted_milestones(
            impacted_task_ids,
            tasks_by_id,
            milestone_order,
        )
        critical_dependency_position = _is_critical_dependency_position(
            direct_dependents,
            transitive_dependents,
            impacted_milestones,
        )

        blocked_tasks.append(
            BlockedTaskImpact(
                blocked_task_id=blocked_task_id,
                blocked_task_title=str(task.get("title") or "Untitled task"),
                blocked_reason=_blocked_reason(task),
                milestone=_optional_string(task.get("milestone")),
                direct_dependents=direct_dependents,
                transitive_dependents=transitive_dependents,
                impacted_milestones=impacted_milestones,
                impacted_count=len(impacted_task_ids),
                critical_dependency_position=critical_dependency_position,
                severity=_severity(
                    impacted_count=len(impacted_task_ids),
                    critical_dependency_position=critical_dependency_position,
                ),
            )
        )

    return BlockedImpactResult(
        plan_id=str(plan.get("id") or ""),
        blocked_tasks=blocked_tasks,
    )


def _dependents_by_task_id(
    tasks: list[dict[str, Any]],
    known_task_ids: set[str],
) -> dict[str, list[str]]:
    dependents: dict[str, list[str]] = {}
    for task in tasks:
        task_id = _task_id(task)
        for dependency_id in _string_list(task.get("depends_on")):
            if dependency_id in known_task_ids:
                dependents.setdefault(dependency_id, []).append(task_id)
    return dependents


def _downstream_dependents(
    task_id: str,
    dependents_by_task_id: dict[str, list[str]],
) -> list[str]:
    downstream = []
    seen = {task_id}
    pending = dependents_by_task_id.get(task_id, [])[:]

    while pending:
        dependent_id = pending.pop(0)
        if dependent_id in seen:
            continue
        seen.add(dependent_id)
        downstream.append(dependent_id)
        pending.extend(dependents_by_task_id.get(dependent_id, []))

    return downstream


def _impacted_milestones(
    task_ids: list[str],
    tasks_by_id: dict[str, dict[str, Any]],
    milestone_order: dict[str, int],
) -> list[str]:
    milestone_names = {
        _milestone_key(tasks_by_id[task_id].get("milestone"))
        for task_id in task_ids
        if task_id in tasks_by_id
    }
    return sorted(
        milestone_names,
        key=lambda milestone: (milestone_order.get(milestone, len(milestone_order)), milestone),
    )


def _milestone_order(
    plan: dict[str, Any],
    tasks: list[dict[str, Any]],
) -> dict[str, int]:
    names = []
    for milestone in _list_of_dicts(plan.get("milestones")):
        name = _optional_string(milestone.get("name"))
        if name:
            names.append(name)
    for task in tasks:
        name = _milestone_key(task.get("milestone"))
        if name not in names:
            names.append(name)
    return {name: index for index, name in enumerate(names)}


def _is_critical_dependency_position(
    direct_dependents: list[str],
    transitive_dependents: list[str],
    impacted_milestones: list[str],
) -> bool:
    return (
        bool(transitive_dependents)
        or len(impacted_milestones) > 1
        or len(direct_dependents) >= 3
    )


def _severity(
    *,
    impacted_count: int,
    critical_dependency_position: bool,
) -> BlockedImpactSeverity:
    if impacted_count == 0:
        return "low"
    if critical_dependency_position:
        return "critical"
    if impacted_count >= 2:
        return "high"
    return "medium"


def _blocked_reason(task: dict[str, Any]) -> str | None:
    reason = _optional_string(task.get("blocked_reason"))
    if reason:
        return reason
    metadata = task.get("metadata")
    if isinstance(metadata, dict):
        return _optional_string(metadata.get("blocked_reason"))
    return None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item)]


def _list_of_dicts(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _task_id(task: dict[str, Any]) -> str:
    return str(task.get("id") or "")


def _milestone_key(value: Any) -> str:
    return _optional_string(value) or "unspecified"


def _optional_string(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None


def _unique_in_order(values: list[str]) -> list[str]:
    seen = set()
    unique_values = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique_values.append(value)
    return unique_values
