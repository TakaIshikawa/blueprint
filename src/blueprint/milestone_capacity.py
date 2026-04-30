"""Milestone capacity analysis helpers for execution plans.

Effort normalization is intentionally conservative and field-based:

- ``estimated_hours`` is treated as already normalized effort.
- ``estimate`` and ``story_points`` numeric values are treated as effort units.
- ``size`` numeric values are treated as effort units.
- ``size`` t-shirt values normalize to XS=1, S=2, M=3, L=5, XL=8, XXL=13.

Task-level metadata may provide the same fields when the top-level task field is
missing. Unknown, blank, negative, or non-numeric values contribute zero effort.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any


UNASSIGNED_MILESTONE = "Unassigned"
EFFORT_FIELDS = ("estimated_hours", "estimate", "story_points", "size")
SIZE_EFFORTS = {
    "xs": 1.0,
    "extra small": 1.0,
    "s": 2.0,
    "small": 2.0,
    "m": 3.0,
    "medium": 3.0,
    "l": 5.0,
    "large": 5.0,
    "xl": 8.0,
    "extra large": 8.0,
    "xxl": 13.0,
    "2xl": 13.0,
}
HIGH_RISK_VALUES = {"high", "critical"}


@dataclass(frozen=True)
class MilestoneCapacitySummary:
    """Capacity metrics for one milestone."""

    milestone: str
    task_count: int
    effort: float
    high_risk_task_count: int
    dependency_pressure: int
    incoming_dependency_count: int
    outgoing_dependency_count: int
    task_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "milestone": self.milestone,
            "task_count": self.task_count,
            "effort": self.effort,
            "high_risk_task_count": self.high_risk_task_count,
            "dependency_pressure": self.dependency_pressure,
            "incoming_dependency_count": self.incoming_dependency_count,
            "outgoing_dependency_count": self.outgoing_dependency_count,
            "task_ids": self.task_ids,
        }


@dataclass(frozen=True)
class MilestoneCapacityFinding:
    """An overload finding for one capacity threshold."""

    code: str
    milestone: str
    metric: str
    value: float
    threshold: float
    task_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "milestone": self.milestone,
            "metric": self.metric,
            "value": self.value,
            "threshold": self.threshold,
            "task_ids": self.task_ids,
        }


@dataclass(frozen=True)
class MilestoneTaskMove:
    """A possible move from an overloaded milestone to a lighter milestone."""

    task_id: str
    from_milestone: str
    to_milestone: str
    effort: float
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "from_milestone": self.from_milestone,
            "to_milestone": self.to_milestone,
            "effort": self.effort,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class MilestoneCapacityResult:
    """Capacity analysis result for an execution plan."""

    plan_id: str
    max_tasks_per_milestone: int
    max_effort_per_milestone: float | None
    summaries: list[MilestoneCapacitySummary] = field(default_factory=list)
    overload_findings: list[MilestoneCapacityFinding] = field(default_factory=list)
    suggested_moves: list[MilestoneTaskMove] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.overload_findings

    def to_dict(self) -> dict[str, Any]:
        return {
            "plan_id": self.plan_id,
            "ok": self.ok,
            "max_tasks_per_milestone": self.max_tasks_per_milestone,
            "max_effort_per_milestone": self.max_effort_per_milestone,
            "summaries": [summary.to_dict() for summary in self.summaries],
            "overload_findings": [finding.to_dict() for finding in self.overload_findings],
            "suggested_moves": [move.to_dict() for move in self.suggested_moves],
        }


def analyze_milestone_capacity(
    plan: dict[str, Any],
    max_tasks_per_milestone: int = 8,
    max_effort_per_milestone: float | None = None,
) -> MilestoneCapacityResult:
    """Estimate milestone overload from task count, effort, risk, and dependencies."""
    tasks = _list_of_dicts(plan.get("tasks"))
    tasks_by_id = {task_id: task for task in tasks if (task_id := _text(task.get("id")))}
    grouped_tasks = _group_tasks(tasks)
    milestone_order = _milestone_order(plan, grouped_tasks)
    summaries = _summaries(milestone_order, grouped_tasks, tasks_by_id)
    overload_findings = _overload_findings(
        summaries,
        max_tasks_per_milestone,
        max_effort_per_milestone,
    )

    return MilestoneCapacityResult(
        plan_id=str(plan.get("id") or ""),
        max_tasks_per_milestone=max_tasks_per_milestone,
        max_effort_per_milestone=max_effort_per_milestone,
        summaries=summaries,
        overload_findings=overload_findings,
        suggested_moves=_suggested_moves(
            milestone_order,
            grouped_tasks,
            tasks_by_id,
            summaries,
            overload_findings,
            max_tasks_per_milestone,
            max_effort_per_milestone,
        ),
    )


def _summaries(
    milestone_order: list[str],
    grouped_tasks: dict[str, list[dict[str, Any]]],
    tasks_by_id: dict[str, dict[str, Any]],
) -> list[MilestoneCapacitySummary]:
    cross_dependencies = _cross_milestone_dependencies(tasks_by_id)
    summaries: list[MilestoneCapacitySummary] = []
    for milestone in milestone_order:
        tasks = grouped_tasks.get(milestone, [])
        incoming = sum(1 for _, to_milestone in cross_dependencies if to_milestone == milestone)
        outgoing = sum(1 for from_milestone, _ in cross_dependencies if from_milestone == milestone)
        summaries.append(
            MilestoneCapacitySummary(
                milestone=milestone,
                task_count=len(tasks),
                effort=_rounded_effort(sum(_effort(task) for task in tasks)),
                high_risk_task_count=sum(1 for task in tasks if _is_high_risk(task)),
                dependency_pressure=incoming + outgoing,
                incoming_dependency_count=incoming,
                outgoing_dependency_count=outgoing,
                task_ids=[_text(task.get("id")) for task in tasks if _text(task.get("id"))],
            )
        )
    return summaries


def _overload_findings(
    summaries: list[MilestoneCapacitySummary],
    max_tasks_per_milestone: int,
    max_effort_per_milestone: float | None,
) -> list[MilestoneCapacityFinding]:
    findings: list[MilestoneCapacityFinding] = []
    for summary in summaries:
        if summary.task_count > max_tasks_per_milestone:
            findings.append(
                MilestoneCapacityFinding(
                    code="task_count_overload",
                    milestone=summary.milestone,
                    metric="task_count",
                    value=float(summary.task_count),
                    threshold=float(max_tasks_per_milestone),
                    task_ids=summary.task_ids,
                )
            )
        if max_effort_per_milestone is not None and summary.effort > max_effort_per_milestone:
            findings.append(
                MilestoneCapacityFinding(
                    code="effort_overload",
                    milestone=summary.milestone,
                    metric="effort",
                    value=summary.effort,
                    threshold=float(max_effort_per_milestone),
                    task_ids=summary.task_ids,
                )
            )
    return sorted(
        findings,
        key=lambda finding: (finding.milestone, finding.metric),
    )


def _suggested_moves(
    milestone_order: list[str],
    grouped_tasks: dict[str, list[dict[str, Any]]],
    tasks_by_id: dict[str, dict[str, Any]],
    summaries: list[MilestoneCapacitySummary],
    overload_findings: list[MilestoneCapacityFinding],
    max_tasks_per_milestone: int,
    max_effort_per_milestone: float | None,
) -> list[MilestoneTaskMove]:
    if not overload_findings:
        return []

    summary_by_milestone = {summary.milestone: summary for summary in summaries}
    overloaded_milestones = {finding.milestone for finding in overload_findings}
    moves: list[MilestoneTaskMove] = []
    for source_milestone in milestone_order:
        if source_milestone not in overloaded_milestones:
            continue
        target_milestone = _target_milestone(
            source_milestone,
            milestone_order,
            summary_by_milestone,
            max_tasks_per_milestone,
            max_effort_per_milestone,
        )
        if target_milestone is None:
            continue
        for task in _movable_tasks(
            grouped_tasks.get(source_milestone, []),
            tasks_by_id,
        ):
            moves.append(
                MilestoneTaskMove(
                    task_id=_text(task.get("id")),
                    from_milestone=source_milestone,
                    to_milestone=target_milestone,
                    effort=_effort(task),
                    reason="Task has no cross-milestone dependency constraints.",
                )
            )
    return sorted(
        moves,
        key=lambda move: (
            move.from_milestone,
            move.to_milestone,
            -move.effort,
            move.task_id,
        ),
    )


def _target_milestone(
    source_milestone: str,
    milestone_order: list[str],
    summary_by_milestone: dict[str, MilestoneCapacitySummary],
    max_tasks_per_milestone: int,
    max_effort_per_milestone: float | None,
) -> str | None:
    candidates: list[MilestoneCapacitySummary] = []
    for milestone in milestone_order:
        if milestone == source_milestone or milestone == UNASSIGNED_MILESTONE:
            continue
        summary = summary_by_milestone[milestone]
        if summary.task_count >= max_tasks_per_milestone:
            continue
        if max_effort_per_milestone is not None and summary.effort >= max_effort_per_milestone:
            continue
        candidates.append(summary)

    if not candidates:
        return None
    order_index = {milestone: index for index, milestone in enumerate(milestone_order)}
    return min(
        candidates,
        key=lambda summary: (
            summary.task_count,
            summary.effort,
            order_index[summary.milestone],
        ),
    ).milestone


def _movable_tasks(
    tasks: list[dict[str, Any]],
    tasks_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    movable: list[dict[str, Any]] = []
    for task in tasks:
        task_id = _text(task.get("id"))
        if task_id and not _has_cross_milestone_dependency_constraint(task, tasks_by_id):
            movable.append(task)
    return sorted(
        movable,
        key=lambda task: (_effort(task), _text(task.get("id"))),
        reverse=True,
    )


def _has_cross_milestone_dependency_constraint(
    task: dict[str, Any],
    tasks_by_id: dict[str, dict[str, Any]],
) -> bool:
    task_id = _text(task.get("id"))
    milestone = _milestone_key(task.get("milestone"))

    for dependency_id in _string_list(task.get("depends_on")):
        dependency = tasks_by_id.get(dependency_id)
        if dependency is not None and _milestone_key(dependency.get("milestone")) != milestone:
            return True

    for dependent in tasks_by_id.values():
        if task_id not in _string_list(dependent.get("depends_on")):
            continue
        if _milestone_key(dependent.get("milestone")) != milestone:
            return True

    return False


def _cross_milestone_dependencies(
    tasks_by_id: dict[str, dict[str, Any]],
) -> list[tuple[str, str]]:
    dependencies: list[tuple[str, str]] = []
    for task in tasks_by_id.values():
        to_milestone = _milestone_key(task.get("milestone"))
        for dependency_id in _string_list(task.get("depends_on")):
            dependency = tasks_by_id.get(dependency_id)
            if dependency is None:
                continue
            from_milestone = _milestone_key(dependency.get("milestone"))
            if from_milestone != to_milestone:
                dependencies.append((from_milestone, to_milestone))
    return dependencies


def _group_tasks(tasks: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped_tasks: dict[str, list[dict[str, Any]]] = {}
    for task in tasks:
        grouped_tasks.setdefault(_milestone_key(task.get("milestone")), []).append(task)
    return grouped_tasks


def _milestone_order(
    plan: dict[str, Any],
    grouped_tasks: dict[str, list[dict[str, Any]]],
) -> list[str]:
    ordered: OrderedDict[str, None] = OrderedDict()
    for index, milestone in enumerate(_list_of_dicts(plan.get("milestones")), 1):
        ordered[_milestone_name(milestone, index)] = None

    task_milestones = set(grouped_tasks)
    for milestone in sorted(task_milestones - set(ordered) - {UNASSIGNED_MILESTONE}):
        ordered[milestone] = None

    if UNASSIGNED_MILESTONE in grouped_tasks:
        ordered[UNASSIGNED_MILESTONE] = None

    return list(ordered)


def _milestone_name(milestone: dict[str, Any], index: int) -> str:
    return _text(milestone.get("name")) or _text(milestone.get("title")) or f"Milestone {index}"


def _effort(task: dict[str, Any]) -> float:
    metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
    for field_name in EFFORT_FIELDS:
        value = task.get(field_name)
        if value is None and isinstance(metadata, dict):
            value = metadata.get(field_name)
        effort = _normalize_effort_value(value)
        if effort is not None:
            return effort
    return 0.0


def _normalize_effort_value(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int | float):
        return float(value) if value >= 0 else None
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    if not normalized:
        return None
    if normalized in SIZE_EFFORTS:
        return SIZE_EFFORTS[normalized]
    try:
        parsed = float(normalized)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _is_high_risk(task: dict[str, Any]) -> bool:
    metadata = task.get("metadata") if isinstance(task.get("metadata"), dict) else {}
    risk_values = [
        task.get("risk_level"),
        task.get("risk"),
        task.get("risk_profile"),
    ]
    if isinstance(metadata, dict):
        risk_values.extend(
            [metadata.get("risk_level"), metadata.get("risk"), metadata.get("risk_profile")]
        )
    return any(_text(value).lower() in HIGH_RISK_VALUES for value in risk_values)


def _rounded_effort(value: float) -> float:
    return round(value, 2)


def _milestone_key(value: Any) -> str:
    return _text(value) or UNASSIGNED_MILESTONE


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


__all__ = [
    "MilestoneCapacityFinding",
    "MilestoneCapacityResult",
    "MilestoneCapacitySummary",
    "MilestoneTaskMove",
    "analyze_milestone_capacity",
]
