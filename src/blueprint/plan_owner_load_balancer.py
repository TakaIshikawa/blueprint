"""Analyze execution-plan owner load and recommend rebalancing candidates."""

from __future__ import annotations

from dataclasses import dataclass, field
from numbers import Real
import re
from typing import Any, Iterable, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_HIGH_RISK_VALUES = {"high", "critical", "blocker", "severe"}
_COMPLEXITY_EFFORT = {
    "xs": 1.0,
    "extra small": 1.0,
    "small": 2.0,
    "low": 2.0,
    "s": 2.0,
    "medium": 4.0,
    "moderate": 4.0,
    "m": 4.0,
    "large": 8.0,
    "high": 8.0,
    "l": 8.0,
    "xl": 13.0,
    "extra large": 13.0,
}
_OVERLOAD_RATIO = 1.5
_RISK_CONCENTRATION_RATIO = 0.6


@dataclass(frozen=True, slots=True)
class PlanOwnerTask:
    """Task-level record used in owner load recommendations."""

    task_id: str
    title: str
    estimated_effort: float | None = None
    risk_level: str = "unspecified"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "estimated_effort": self.estimated_effort,
            "risk_level": self.risk_level,
        }


@dataclass(frozen=True, slots=True)
class PlanOwnerLoad:
    """Aggregated task, effort, and high-risk load for one owner."""

    owner: str
    task_count: int
    estimated_effort: float
    missing_estimate_count: int = 0
    high_risk_task_count: int = 0
    high_risk_task_ids: tuple[str, ...] = field(default_factory=tuple)
    task_ids: tuple[str, ...] = field(default_factory=tuple)
    overload_reasons: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "owner": self.owner,
            "task_count": self.task_count,
            "estimated_effort": self.estimated_effort,
            "missing_estimate_count": self.missing_estimate_count,
            "high_risk_task_count": self.high_risk_task_count,
            "high_risk_task_ids": list(self.high_risk_task_ids),
            "task_ids": list(self.task_ids),
            "overload_reasons": list(self.overload_reasons),
        }


@dataclass(frozen=True, slots=True)
class PlanOwnerLoadRecommendation:
    """Suggested non-mutating owner reassignment candidate."""

    task_id: str
    title: str
    from_owner: str
    to_owner: str | None
    reason: str
    estimated_effort: float | None = None
    risk_level: str = "unspecified"

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "title": self.title,
            "from_owner": self.from_owner,
            "to_owner": self.to_owner,
            "reason": self.reason,
            "estimated_effort": self.estimated_effort,
            "risk_level": self.risk_level,
        }


@dataclass(frozen=True, slots=True)
class PlanOwnerLoadBalance:
    """Owner load summary and rebalance guidance for a plan or task collection."""

    plan_id: str | None = None
    owner_loads: tuple[PlanOwnerLoad, ...] = field(default_factory=tuple)
    overloaded_owners: tuple[str, ...] = field(default_factory=tuple)
    unowned_tasks: tuple[PlanOwnerTask, ...] = field(default_factory=tuple)
    recommendations: tuple[PlanOwnerLoadRecommendation, ...] = field(default_factory=tuple)
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "owner_loads": [load.to_dict() for load in self.owner_loads],
            "overloaded_owners": list(self.overloaded_owners),
            "unowned_tasks": [task.to_dict() for task in self.unowned_tasks],
            "recommendations": [
                recommendation.to_dict() for recommendation in self.recommendations
            ],
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return owner load records as plain dictionaries."""
        return [load.to_dict() for load in self.owner_loads]

    def to_markdown(self) -> str:
        """Render owner load balance as deterministic Markdown."""
        title = "# Plan Owner Load Balance"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.owner_loads and not self.unowned_tasks:
            lines.extend(["", "No tasks were available for owner load analysis."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Owner | Tasks | Estimated Effort | High-risk Tasks | Overload Reasons |",
                "| --- | --- | --- | --- | --- |",
            ]
        )
        for load in self.owner_loads:
            lines.append(
                "| "
                f"{_markdown_cell(load.owner)} | "
                f"{load.task_count} | "
                f"{load.estimated_effort:g} | "
                f"{load.high_risk_task_count} | "
                f"{_markdown_cell('; '.join(load.overload_reasons) or 'none')} |"
            )
        if self.unowned_tasks:
            lines.extend(
                ["", "Unowned tasks: " + ", ".join(task.task_id for task in self.unowned_tasks)]
            )
        return "\n".join(lines)


def build_plan_owner_load_balance(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanOwnerLoadBalance:
    """Analyze task ownership load and suggest non-mutating rebalance candidates."""
    plan_id, tasks = _source_payload(source)
    records = [_task_record(task, index) for index, task in enumerate(tasks, start=1)]
    owned_records = [record for record in records if record["owner"]]
    unowned_tasks = tuple(
        PlanOwnerTask(
            task_id=record["task_id"],
            title=record["title"],
            estimated_effort=record["estimated_effort"],
            risk_level=record["risk_level"],
        )
        for record in records
        if not record["owner"]
    )

    raw_loads = _owner_load_records(owned_records)
    average_task_count = len(owned_records) / len(raw_loads) if raw_loads else 0.0
    average_effort = (
        sum(load["estimated_effort"] for load in raw_loads) / len(raw_loads) if raw_loads else 0.0
    )
    total_high_risk = sum(1 for record in owned_records if record["is_high_risk"])
    loads = tuple(
        PlanOwnerLoad(
            owner=load["owner"],
            task_count=load["task_count"],
            estimated_effort=_stable_number(load["estimated_effort"]),
            missing_estimate_count=load["missing_estimate_count"],
            high_risk_task_count=load["high_risk_task_count"],
            high_risk_task_ids=tuple(load["high_risk_task_ids"]),
            task_ids=tuple(load["task_ids"]),
            overload_reasons=tuple(
                _overload_reasons(load, average_task_count, average_effort, total_high_risk)
            ),
        )
        for load in raw_loads
    )
    overloaded_owners = tuple(load.owner for load in loads if load.overload_reasons)
    recommendations = tuple(_recommendations(owned_records, loads, overloaded_owners))

    return PlanOwnerLoadBalance(
        plan_id=plan_id,
        owner_loads=loads,
        overloaded_owners=overloaded_owners,
        unowned_tasks=unowned_tasks,
        recommendations=recommendations,
        summary={
            "task_count": len(records),
            "owned_task_count": len(owned_records),
            "unowned_task_count": len(unowned_tasks),
            "owner_count": len(loads),
            "overloaded_owner_count": len(overloaded_owners),
            "recommendation_count": len(recommendations),
            "estimated_effort": _stable_number(
                sum(record["effort_for_total"] for record in records)
            ),
            "high_risk_task_count": sum(1 for record in records if record["is_high_risk"]),
        },
    )


def plan_owner_load_balance_to_dict(result: PlanOwnerLoadBalance) -> dict[str, Any]:
    """Serialize owner load balance analysis to a plain dictionary."""
    return result.to_dict()


plan_owner_load_balance_to_dict.__test__ = False


def plan_owner_load_balance_to_markdown(result: PlanOwnerLoadBalance) -> str:
    """Render owner load balance analysis as Markdown."""
    return result.to_markdown()


plan_owner_load_balance_to_markdown.__test__ = False


def recommend_plan_owner_load_balance(
    source: (
        Mapping[str, Any]
        | ExecutionPlan
        | ExecutionTask
        | Iterable[Mapping[str, Any] | ExecutionTask]
    ),
) -> PlanOwnerLoadBalance:
    """Compatibility alias for building owner load balance recommendations."""
    return build_plan_owner_load_balance(source)


def _task_record(task: Mapping[str, Any], index: int) -> dict[str, Any]:
    task_id = _optional_text(task.get("id")) or f"task-{index}"
    title = _optional_text(task.get("title")) or task_id
    effort, missing = _estimated_effort(task)
    risk_level = _risk_level(task)
    return {
        "task_id": task_id,
        "title": title,
        "owner": _owner(task),
        "estimated_effort": effort,
        "effort_for_total": effort or 0.0,
        "missing_estimate": missing,
        "risk_level": risk_level,
        "is_high_risk": risk_level in _HIGH_RISK_VALUES,
    }


def _owner_load_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    loads: dict[str, dict[str, Any]] = {}
    for record in records:
        owner = record["owner"]
        if owner not in loads:
            loads[owner] = {
                "owner": owner,
                "task_count": 0,
                "estimated_effort": 0.0,
                "missing_estimate_count": 0,
                "high_risk_task_count": 0,
                "high_risk_task_ids": [],
                "task_ids": [],
            }
        load = loads[owner]
        load["task_count"] += 1
        load["estimated_effort"] += record["effort_for_total"]
        if record["missing_estimate"]:
            load["missing_estimate_count"] += 1
        if record["is_high_risk"]:
            load["high_risk_task_count"] += 1
            load["high_risk_task_ids"].append(record["task_id"])
        load["task_ids"].append(record["task_id"])

    return sorted(
        loads.values(),
        key=lambda load: (-load["estimated_effort"], -load["task_count"], load["owner"].casefold()),
    )


def _overload_reasons(
    load: Mapping[str, Any],
    average_task_count: float,
    average_effort: float,
    total_high_risk: int,
) -> list[str]:
    reasons: list[str] = []
    task_limit = max(3, average_task_count * _OVERLOAD_RATIO)
    effort_limit = max(8.0, average_effort * _OVERLOAD_RATIO)
    high_risk_count = int(load["high_risk_task_count"])

    if load["task_count"] > task_limit:
        reasons.append("task_count_above_plan_average")
    if load["estimated_effort"] > effort_limit:
        reasons.append("estimated_effort_above_plan_average")
    if (
        total_high_risk >= 2
        and high_risk_count >= 2
        and high_risk_count / total_high_risk >= _RISK_CONCENTRATION_RATIO
    ):
        reasons.append("high_risk_concentration")
    return reasons


def _recommendations(
    records: list[dict[str, Any]],
    loads: tuple[PlanOwnerLoad, ...],
    overloaded_owners: tuple[str, ...],
) -> list[PlanOwnerLoadRecommendation]:
    if not overloaded_owners:
        return []

    target_owners = [load for load in loads if load.owner not in overloaded_owners]
    recommendations: list[PlanOwnerLoadRecommendation] = []
    for owner in overloaded_owners:
        source_load = next(load for load in loads if load.owner == owner)
        reason = source_load.overload_reasons[0]
        target = _target_owner(target_owners, source_load)
        owner_records = [record for record in records if record["owner"] == owner]
        if "high_risk_concentration" in source_load.overload_reasons:
            owner_records = [
                record for record in owner_records if record["is_high_risk"]
            ] or owner_records
        for record in sorted(
            owner_records,
            key=lambda item: (
                not item["is_high_risk"],
                -(item["estimated_effort"] or 0.0),
                item["task_id"],
            ),
        )[:2]:
            recommendations.append(
                PlanOwnerLoadRecommendation(
                    task_id=record["task_id"],
                    title=record["title"],
                    from_owner=owner,
                    to_owner=target,
                    reason=reason,
                    estimated_effort=record["estimated_effort"],
                    risk_level=record["risk_level"],
                )
            )
    return recommendations


def _target_owner(
    target_owners: list[PlanOwnerLoad],
    source_load: PlanOwnerLoad,
) -> str | None:
    candidates = [
        load
        for load in target_owners
        if load.estimated_effort < source_load.estimated_effort
        or load.task_count < source_load.task_count
    ]
    if not candidates:
        return None
    return sorted(
        candidates, key=lambda load: (load.estimated_effort, load.task_count, load.owner.casefold())
    )[0].owner


def _owner(task: Mapping[str, Any]) -> str | None:
    metadata = task.get("metadata")
    value = (
        task.get("owner")
        or task.get("owner_id")
        or task.get("assignee")
        or _metadata_value(metadata, "owner")
        or _metadata_value(metadata, "owner_id")
        or _metadata_value(metadata, "assignee")
        or _first_string(task.get("assignees"))
        or _first_string(_metadata_value(metadata, "assignees"))
        or task.get("owner_type")
        or _metadata_value(metadata, "owner_type")
    )
    return _optional_text(value)


def _estimated_effort(task: Mapping[str, Any]) -> tuple[float | None, bool]:
    metadata = task.get("metadata")
    hours = _non_negative_number(
        task.get("estimated_hours")
        or task.get("estimate")
        or task.get("effort")
        or _metadata_value(metadata, "estimated_hours")
        or _metadata_value(metadata, "estimate")
        or _metadata_value(metadata, "effort")
    )
    if hours is not None:
        return _stable_number(hours), False

    complexity = _optional_text(
        task.get("estimated_complexity")
        or task.get("complexity")
        or _metadata_value(metadata, "estimated_complexity")
        or _metadata_value(metadata, "complexity")
    )
    if complexity:
        effort = _COMPLEXITY_EFFORT.get(complexity.casefold())
        if effort is not None:
            return effort, False
    return None, True


def _risk_level(task: Mapping[str, Any]) -> str:
    metadata = task.get("metadata")
    value = (
        task.get("risk_level")
        or task.get("risk")
        or _metadata_value(metadata, "risk_level")
        or _metadata_value(metadata, "risk")
    )
    return (_optional_text(value) or "unspecified").casefold()


def _source_payload(
    source: Mapping[str, Any]
    | ExecutionPlan
    | ExecutionTask
    | Iterable[Mapping[str, Any] | ExecutionTask],
) -> tuple[str | None, list[dict[str, Any]]]:
    if isinstance(source, ExecutionTask):
        return None, [source.model_dump(mode="python")]
    if isinstance(source, ExecutionPlan):
        return _optional_text(source.id), [task.model_dump(mode="python") for task in source.tasks]
    if isinstance(source, Mapping):
        if "tasks" in source:
            payload = _plan_payload(source)
            return _optional_text(payload.get("id")), _task_payloads(payload.get("tasks"))
        return None, [dict(source)]

    try:
        iterator = iter(source)
    except TypeError:
        return None, []

    tasks: list[dict[str, Any]] = []
    for item in iterator:
        if isinstance(item, ExecutionTask):
            tasks.append(item.model_dump(mode="python"))
        elif hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return None, tasks


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        return dict(plan) if isinstance(plan, Mapping) else {}


def _task_payloads(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    tasks: list[dict[str, Any]] = []
    for item in value:
        if hasattr(item, "model_dump"):
            task = item.model_dump(mode="python")
            if isinstance(task, Mapping):
                tasks.append(dict(task))
        elif isinstance(item, Mapping):
            tasks.append(dict(item))
    return tasks


def _metadata_value(value: Any, key: str) -> Any:
    return value.get(key) if isinstance(value, Mapping) else None


def _first_string(value: Any) -> str | None:
    values = _strings(value)
    return values[0] if values else None


def _strings(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = _optional_text(value)
        return [text] if text else []
    if isinstance(value, Mapping):
        strings: list[str] = []
        for key in sorted(value, key=lambda item: str(item)):
            strings.extend(_strings(value[key]))
        return strings
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        strings: list[str] = []
        for item in items:
            strings.extend(_strings(item))
        return strings
    text = _optional_text(value)
    return [text] if text else []


def _non_negative_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, Real):
        number = float(value)
    elif isinstance(value, str):
        try:
            number = float(value.strip())
        except ValueError:
            return None
    else:
        return None

    if number < 0:
        return None
    return number


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _stable_number(value: float) -> float:
    rounded = round(value, 6)
    if rounded == 0:
        return 0.0
    return rounded


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped
