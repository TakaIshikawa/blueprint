"""Summarize remaining execution-plan risk by milestone.

Risk points are intentionally conservative and deterministic:

* ``risk_level`` contributes low=1, medium=3, high=5, critical=8.
* ``estimated_complexity`` contributes low=0, medium=1, high=2, critical=3.
* Missing or unknown risk and complexity values default to medium weights.
* Metadata risk fields can provide ``risk_level``/``riskLevel``/``risk`` or an
  explicit numeric ``risk_points``/``riskPoints``/``risk_score``/``riskScore``.
  When multiple risk signals exist, the highest risk-point value is used.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class MilestoneRiskBurndownRecord:
    """Risk burndown summary for one milestone."""

    milestone: str
    total_tasks: int
    completed_tasks: int
    remaining_tasks: int
    total_risk_points: int
    remaining_risk_points: int
    high_risk_task_ids: tuple[str, ...] = field(default_factory=tuple)
    blocked_task_ids: tuple[str, ...] = field(default_factory=tuple)
    completion_percent: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "milestone": self.milestone,
            "total_tasks": self.total_tasks,
            "completed_tasks": self.completed_tasks,
            "remaining_tasks": self.remaining_tasks,
            "total_risk_points": self.total_risk_points,
            "remaining_risk_points": self.remaining_risk_points,
            "high_risk_task_ids": list(self.high_risk_task_ids),
            "blocked_task_ids": list(self.blocked_task_ids),
            "completion_percent": self.completion_percent,
        }


def build_milestone_risk_burndown(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> tuple[MilestoneRiskBurndownRecord, ...]:
    """Build deterministic milestone risk summaries for an execution plan."""
    payload = _plan_payload(plan)
    tasks = _task_payloads(payload.get("tasks"))
    grouped_tasks = _group_tasks(tasks)

    return tuple(
        _record(milestone, grouped_tasks.get(milestone, []))
        for milestone in _milestone_order(payload, grouped_tasks)
    )


def milestone_risk_burndown_to_dict(
    records: tuple[MilestoneRiskBurndownRecord, ...] | list[MilestoneRiskBurndownRecord],
) -> list[dict[str, Any]]:
    """Serialize milestone risk burndown records to plain dictionaries."""
    return [record.to_dict() for record in records]


def _record(
    milestone: str,
    tasks: list[tuple[int, dict[str, Any]]],
) -> MilestoneRiskBurndownRecord:
    total_tasks = len(tasks)
    completed_tasks = 0
    remaining_tasks = 0
    total_risk_points = 0
    remaining_risk_points = 0
    high_risk_task_ids: list[str] = []
    blocked_task_ids: list[str] = []

    for index, task in tasks:
        task_id = _task_id(task, index)
        status = _status(task)
        risk_points = _task_risk_points(task)

        total_risk_points += risk_points
        if status == "completed":
            completed_tasks += 1
        if status not in _DONE_STATUSES:
            remaining_tasks += 1
            remaining_risk_points += risk_points
        if _is_high_risk(task, risk_points):
            high_risk_task_ids.append(task_id)
        if status == "blocked" or _optional_text(task.get("blocked_reason")):
            blocked_task_ids.append(task_id)

    return MilestoneRiskBurndownRecord(
        milestone=milestone,
        total_tasks=total_tasks,
        completed_tasks=completed_tasks,
        remaining_tasks=remaining_tasks,
        total_risk_points=total_risk_points,
        remaining_risk_points=remaining_risk_points,
        high_risk_task_ids=tuple(_dedupe(high_risk_task_ids)),
        blocked_task_ids=tuple(_dedupe(blocked_task_ids)),
        completion_percent=_completion_percent(completed_tasks, total_tasks),
    )


def _group_tasks(tasks: list[dict[str, Any]]) -> dict[str, list[tuple[int, dict[str, Any]]]]:
    grouped_tasks: dict[str, list[tuple[int, dict[str, Any]]]] = {}
    for index, task in enumerate(tasks, start=1):
        milestone = _optional_text(task.get("milestone")) or _UNASSIGNED_MILESTONE
        grouped_tasks.setdefault(milestone, []).append((index, task))
    return grouped_tasks


def _milestone_order(
    payload: dict[str, Any],
    grouped_tasks: dict[str, list[tuple[int, dict[str, Any]]]],
) -> list[str]:
    ordered: OrderedDict[str, None] = OrderedDict()
    for index, milestone in enumerate(_milestone_payloads(payload.get("milestones")), start=1):
        ordered[_milestone_name(milestone, index)] = None

    task_milestones = set(grouped_tasks)
    for milestone in sorted(
        task_milestones - set(ordered) - {_UNASSIGNED_MILESTONE}
    ):
        ordered[milestone] = None

    if _UNASSIGNED_MILESTONE in grouped_tasks:
        ordered[_UNASSIGNED_MILESTONE] = None

    return list(ordered)


def _task_risk_points(task: Mapping[str, Any]) -> int:
    risk_points = _risk_point_candidates(task)
    complexity_points = _complexity_point_candidates(task)
    return max(risk_points or [_DEFAULT_RISK_POINTS]) + max(
        complexity_points or [_DEFAULT_COMPLEXITY_POINTS]
    )


def _risk_point_candidates(task: Mapping[str, Any]) -> list[int]:
    values: list[int] = []
    task_risk = _risk_level_points(task.get("risk_level"))
    if task_risk is not None:
        values.append(task_risk)

    metadata = task.get("metadata")
    if not isinstance(metadata, Mapping):
        return values

    for key in ("risk_level", "riskLevel", "risk"):
        metadata_risk = _risk_level_points(metadata.get(key))
        if metadata_risk is not None:
            values.append(metadata_risk)

    for key in ("risk_points", "riskPoints", "risk_score", "riskScore"):
        metadata_points = _nonnegative_int(metadata.get(key))
        if metadata_points is not None:
            values.append(metadata_points)

    return values


def _complexity_point_candidates(task: Mapping[str, Any]) -> list[int]:
    values: list[int] = []
    task_complexity = _complexity_points(task.get("estimated_complexity"))
    if task_complexity is not None:
        values.append(task_complexity)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        for key in ("estimated_complexity", "estimatedComplexity", "complexity"):
            metadata_complexity = _complexity_points(metadata.get(key))
            if metadata_complexity is not None:
                values.append(metadata_complexity)

    return values


def _is_high_risk(task: Mapping[str, Any], risk_points: int) -> bool:
    risk_levels = [_text(task.get("risk_level")).lower()]
    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        risk_levels.extend(
            _text(metadata.get(key)).lower()
            for key in ("risk_level", "riskLevel", "risk")
        )
    return bool({"high", "critical"} & set(risk_levels)) or risk_points >= _HIGH_RISK_POINTS


def _completion_percent(completed_tasks: int, total_tasks: int) -> float:
    if total_tasks == 0:
        return 0.0
    return round((completed_tasks / total_tasks) * 100, 2)


def _risk_level_points(value: Any) -> int | None:
    text = _text(value).lower()
    if not text:
        return None
    return _RISK_LEVEL_POINTS.get(text, _DEFAULT_RISK_POINTS)


def _complexity_points(value: Any) -> int | None:
    text = _text(value).lower()
    if not text:
        return None
    return _COMPLEXITY_POINTS.get(text, _DEFAULT_COMPLEXITY_POINTS)


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return max(0, value)
    if isinstance(value, float) and value.is_integer():
        return max(0, int(value))
    if isinstance(value, str):
        text = value.strip()
        if text.isdecimal():
            return int(text)
    return None


def _milestone_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    milestones: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            milestones.append(item.model_dump(mode="python"))
        elif isinstance(item, Mapping):
            milestones.append(dict(item))
    return milestones


def _milestone_name(milestone: Mapping[str, Any], index: int) -> str:
    return (
        _optional_text(milestone.get("name"))
        or _optional_text(milestone.get("title"))
        or f"Milestone {index}"
    )


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
    return {}


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


def _status(task: Mapping[str, Any]) -> str:
    return _optional_text(task.get("status")) or "pending"


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if isinstance(value, str):
        return " ".join(value.split())
    return ""


def _dedupe(values: list[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


_UNASSIGNED_MILESTONE = "Unassigned"
_DONE_STATUSES = {"completed", "skipped"}
_RISK_LEVEL_POINTS = {"low": 1, "medium": 3, "high": 5, "critical": 8}
_COMPLEXITY_POINTS = {"low": 0, "medium": 1, "high": 2, "critical": 3}
_DEFAULT_RISK_POINTS = _RISK_LEVEL_POINTS["medium"]
_DEFAULT_COMPLEXITY_POINTS = _COMPLEXITY_POINTS["medium"]
_HIGH_RISK_POINTS = _RISK_LEVEL_POINTS["high"] + _COMPLEXITY_POINTS["high"]


__all__ = [
    "MilestoneRiskBurndownRecord",
    "build_milestone_risk_burndown",
    "milestone_risk_burndown_to_dict",
]
