"""Recommend execution plan status transitions from task readiness signals."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan
from blueprint.validation_commands import flatten_validation_commands


PlanStatusRecommendationCode = Literal[
    "all_tasks_completed",
    "blocked_tasks_present",
    "invalid_dependencies",
    "missing_tasks",
    "missing_validation_coverage",
    "tasks_in_progress",
    "ready_tasks_available",
    "queued_tasks_available",
    "waiting_on_dependencies",
    "draft_blocked",
]

PlanTransitionStatus = Literal[
    "draft",
    "ready",
    "queued",
    "in_progress",
    "completed",
    "failed",
]

_SATISFIED_DEPENDENCY_STATUSES = {"completed", "skipped"}
_ACTIVE_TASK_STATUSES = {"pending", "in_progress", "blocked"}
_STARTED_PLAN_STATUSES = {"queued", "in_progress"}


@dataclass(frozen=True, slots=True)
class PlanStatusRecommendation:
    """Deterministic status recommendation for an execution plan."""

    plan_id: str
    current_status: str
    recommended_status: PlanTransitionStatus
    explanation_codes: tuple[PlanStatusRecommendationCode, ...]
    relevant_task_ids: tuple[str, ...] = field(default_factory=tuple)
    blocking_task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a stable JSON-compatible payload."""
        return {
            "plan_id": self.plan_id,
            "current_status": self.current_status,
            "recommended_status": self.recommended_status,
            "explanation_codes": list(self.explanation_codes),
            "relevant_task_ids": list(self.relevant_task_ids),
            "blocking_task_ids": list(self.blocking_task_ids),
        }


def recommend_plan_status(
    plan: Mapping[str, Any] | ExecutionPlan,
) -> PlanStatusRecommendation:
    """Recommend the next plan status from tasks, dependencies, and validation coverage."""
    payload = _plan_payload(plan)
    tasks = _task_payloads(payload.get("tasks"))
    current_status = _status_text(payload.get("status")) or "draft"
    task_ids = [_task_id(task, index) for index, task in enumerate(tasks, start=1)]
    task_statuses = [_task_status(task) for task in tasks]

    if not tasks:
        return _recommendation(
            payload,
            current_status,
            "draft",
            ("missing_tasks",),
        )

    completed_task_ids = [
        task_id
        for task_id, status in zip(task_ids, task_statuses, strict=True)
        if status == "completed"
    ]
    skipped_task_ids = [
        task_id
        for task_id, status in zip(task_ids, task_statuses, strict=True)
        if status == "skipped"
    ]
    if len(completed_task_ids) + len(skipped_task_ids) == len(tasks):
        return _recommendation(
            payload,
            current_status,
            "completed",
            ("all_tasks_completed",),
            completed_task_ids or skipped_task_ids,
        )

    blocked_task_ids = [
        task_id
        for task_id, task, status in zip(task_ids, tasks, task_statuses, strict=True)
        if status == "blocked" or _has_blocked_reason(task)
    ]
    invalid_dependency_task_ids = _invalid_dependency_task_ids(tasks)
    if blocked_task_ids or invalid_dependency_task_ids:
        codes: list[PlanStatusRecommendationCode] = []
        if blocked_task_ids:
            codes.append("blocked_tasks_present")
        if invalid_dependency_task_ids:
            codes.append("invalid_dependencies")
        recommended_status: PlanTransitionStatus = (
            "failed" if current_status in _STARTED_PLAN_STATUSES else "draft"
        )
        if recommended_status == "draft":
            codes.append("draft_blocked")
        return _recommendation(
            payload,
            current_status,
            recommended_status,
            tuple(codes),
            [*blocked_task_ids, *invalid_dependency_task_ids],
            [*blocked_task_ids, *invalid_dependency_task_ids],
        )

    in_progress_task_ids = [
        task_id
        for task_id, status in zip(task_ids, task_statuses, strict=True)
        if status == "in_progress"
    ]
    if in_progress_task_ids:
        return _recommendation(
            payload,
            current_status,
            "in_progress",
            ("tasks_in_progress",),
            in_progress_task_ids,
        )

    missing_validation_task_ids = _missing_validation_task_ids(tasks, payload)
    if missing_validation_task_ids:
        return _recommendation(
            payload,
            current_status,
            "draft",
            ("missing_validation_coverage", "draft_blocked"),
            missing_validation_task_ids,
            missing_validation_task_ids,
        )

    ready_task_ids, waiting_task_ids = _pending_readiness(tasks)
    if ready_task_ids:
        if current_status in {"ready", "queued"}:
            return _recommendation(
                payload,
                current_status,
                "queued",
                ("queued_tasks_available",),
                ready_task_ids,
            )
        return _recommendation(
            payload,
            current_status,
            "ready",
            ("ready_tasks_available",),
            ready_task_ids,
        )

    if waiting_task_ids:
        recommended_status = "queued" if current_status in {"ready", "queued"} else "draft"
        codes: tuple[PlanStatusRecommendationCode, ...] = ("waiting_on_dependencies",)
        if recommended_status == "draft":
            codes = (*codes, "draft_blocked")
        return _recommendation(
            payload,
            current_status,
            recommended_status,
            codes,
            waiting_task_ids,
            waiting_task_ids if recommended_status == "draft" else (),
        )

    return _recommendation(
        payload,
        current_status,
        "draft",
        ("missing_tasks",),
    )


def plan_status_recommendation_to_dict(
    recommendation: PlanStatusRecommendation,
) -> dict[str, Any]:
    """Serialize a plan status recommendation to a dictionary."""
    return recommendation.to_dict()


plan_status_recommendation_to_dict.__test__ = False


def _recommendation(
    payload: Mapping[str, Any],
    current_status: str,
    recommended_status: PlanTransitionStatus,
    explanation_codes: tuple[PlanStatusRecommendationCode, ...],
    relevant_task_ids: list[str] | tuple[str, ...] = (),
    blocking_task_ids: list[str] | tuple[str, ...] = (),
) -> PlanStatusRecommendation:
    return PlanStatusRecommendation(
        plan_id=_optional_text(payload.get("id")) or "",
        current_status=current_status,
        recommended_status=recommended_status,
        explanation_codes=explanation_codes,
        relevant_task_ids=tuple(_dedupe(relevant_task_ids)),
        blocking_task_ids=tuple(_dedupe(blocking_task_ids)),
    )


def _pending_readiness(tasks: list[dict[str, Any]]) -> tuple[list[str], list[str]]:
    tasks_by_id = {_task_id(task, index): task for index, task in enumerate(tasks, start=1)}
    ready_task_ids: list[str] = []
    waiting_task_ids: list[str] = []

    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        if _task_status(task) != "pending":
            continue

        waiting = False
        for dependency_id in _string_list(task.get("depends_on")):
            dependency = tasks_by_id.get(dependency_id)
            if dependency is None:
                waiting = True
                continue
            if _task_status(dependency) not in _SATISFIED_DEPENDENCY_STATUSES:
                waiting = True

        if waiting:
            waiting_task_ids.append(task_id)
        else:
            ready_task_ids.append(task_id)

    return ready_task_ids, waiting_task_ids


def _invalid_dependency_task_ids(tasks: list[dict[str, Any]]) -> list[str]:
    known_task_ids = {
        _task_id(task, index) for index, task in enumerate(tasks, start=1)
    }
    invalid_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        dependency_ids = _string_list(task.get("depends_on"))
        if any(dependency_id not in known_task_ids for dependency_id in dependency_ids):
            invalid_task_ids.append(_task_id(task, index))
    return invalid_task_ids


def _missing_validation_task_ids(
    tasks: list[dict[str, Any]],
    plan: Mapping[str, Any],
) -> list[str]:
    if _plan_validation_commands(plan):
        return []

    missing_task_ids: list[str] = []
    for index, task in enumerate(tasks, start=1):
        if _task_status(task) not in _ACTIVE_TASK_STATUSES:
            continue
        if not _task_validation_commands(task):
            missing_task_ids.append(_task_id(task, index))
    return missing_task_ids


def _task_validation_commands(task: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    for key in ("test_command", "suggested_test_command", "validation_command"):
        command = _optional_text(task.get(key))
        if command:
            commands.append(command)

    metadata = task.get("metadata")
    if isinstance(metadata, Mapping):
        commands.extend(_commands_from_value(metadata.get("validation_commands")))
        commands.extend(_commands_from_value(metadata.get("test_commands")))
    return _dedupe(commands)


def _plan_validation_commands(plan: Mapping[str, Any]) -> list[str]:
    commands: list[str] = []
    test_strategy = _optional_text(plan.get("test_strategy"))
    if test_strategy:
        commands.append(test_strategy)

    metadata = plan.get("metadata")
    if isinstance(metadata, Mapping):
        commands.extend(_commands_from_value(metadata.get("validation_commands")))
        commands.extend(_commands_from_value(metadata.get("test_commands")))
    return _dedupe(commands)


def _commands_from_value(value: Any) -> list[str]:
    if isinstance(value, Mapping):
        return flatten_validation_commands(value)
    if isinstance(value, list):
        return [
            item.strip()
            for item in value
            if isinstance(item, str) and item.strip()
        ]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        return plan.model_dump(mode="python")
    try:
        return ExecutionPlan.model_validate(plan).model_dump(mode="python")
    except (TypeError, ValueError, ValidationError):
        return dict(plan)


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


def _task_status(task: Mapping[str, Any]) -> str:
    return _status_text(task.get("status")) or "pending"


def _status_text(value: Any) -> str | None:
    text = _optional_text(value)
    return text.lower() if text else None


def _has_blocked_reason(task: Mapping[str, Any]) -> bool:
    return bool(_optional_text(task.get("blocked_reason")))


def _optional_text(value: Any) -> str | None:
    if isinstance(value, str):
        text = " ".join(value.split())
        return text or None
    return None


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [
        item.strip()
        for item in value
        if isinstance(item, str) and item.strip()
    ]


def _dedupe(values: list[str] | tuple[str, ...]) -> list[str]:
    return list(dict.fromkeys(values))


__all__ = [
    "PlanStatusRecommendation",
    "recommend_plan_status",
    "plan_status_recommendation_to_dict",
]
