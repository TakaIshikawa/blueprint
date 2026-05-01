"""Summarize unresolved blockers across execution-plan tasks."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan, ExecutionTask


PlanBlockerType = Literal[
    "blocked_task",
    "missing_dependency_output",
    "validation_blocker",
    "missing_owner",
    "unresolved_assumption",
    "unanswered_question",
]
PlanBlockerSeverity = Literal["high", "medium", "low"]
_T = TypeVar("_T")

_SPACE_RE = re.compile(r"\s+")
_UNRESOLVED_RE = re.compile(
    r"\b(?:block|blocked|blocker|cannot|can't|missing|need|needs|pending|"
    r"question|tbd|todo|unanswered|unknown|unresolved|waiting)\b",
    re.IGNORECASE,
)
_RESOLVED_STATUSES = {
    "answered",
    "complete",
    "completed",
    "done",
    "not_applicable",
    "resolved",
    "validated",
}
_OPEN_STATUSES = {
    "blocked",
    "missing",
    "open",
    "pending",
    "todo",
    "unanswered",
    "unknown",
    "unresolved",
    "waiting",
}
_BLOCKER_ORDER: dict[PlanBlockerType, int] = {
    "blocked_task": 0,
    "missing_dependency_output": 1,
    "validation_blocker": 2,
    "missing_owner": 3,
    "unresolved_assumption": 4,
    "unanswered_question": 5,
}
_SEVERITY_ORDER: dict[PlanBlockerSeverity, int] = {
    "high": 0,
    "medium": 1,
    "low": 2,
}
_BLOCKER_SEVERITY: dict[PlanBlockerType, PlanBlockerSeverity] = {
    "blocked_task": "high",
    "missing_dependency_output": "high",
    "validation_blocker": "high",
    "missing_owner": "medium",
    "unresolved_assumption": "medium",
    "unanswered_question": "low",
}


@dataclass(frozen=True, slots=True)
class PlanUnresolvedBlockerGroup:
    """Unresolved blocker group with affected task IDs."""

    blocker_type: PlanBlockerType
    severity: PlanBlockerSeverity
    affected_task_ids: tuple[str, ...] = field(default_factory=tuple)
    count: int = 0
    details: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "blocker_type": self.blocker_type,
            "severity": self.severity,
            "affected_task_ids": list(self.affected_task_ids),
            "count": self.count,
            "details": list(self.details),
        }


@dataclass(frozen=True, slots=True)
class PlanUnresolvedBlockerSummary:
    """Plan-level unresolved blocker summary."""

    plan_id: str | None = None
    blocker_groups: tuple[PlanUnresolvedBlockerGroup, ...] = field(default_factory=tuple)
    blocked_task_ids: tuple[str, ...] = field(default_factory=tuple)
    should_pause_execution: bool = False
    recommended_next_action: str = "continue_execution"
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "plan_id": self.plan_id,
            "blocker_groups": [group.to_dict() for group in self.blocker_groups],
            "blocked_task_ids": list(self.blocked_task_ids),
            "should_pause_execution": self.should_pause_execution,
            "recommended_next_action": self.recommended_next_action,
            "summary": dict(self.summary),
        }

    def to_dicts(self) -> list[dict[str, Any]]:
        """Return blocker group records as plain dictionaries."""
        return [group.to_dict() for group in self.blocker_groups]

    def to_markdown(self) -> str:
        """Render unresolved blockers as deterministic Markdown."""
        title = "# Plan Unresolved Blockers"
        if self.plan_id:
            title = f"{title}: {self.plan_id}"
        lines = [title]
        if not self.blocker_groups:
            lines.extend(["", "No unresolved blockers were found."])
            return "\n".join(lines)

        lines.extend(
            [
                "",
                "| Blocker Type | Severity | Count | Affected Tasks |",
                "| --- | --- | --- | --- |",
            ]
        )
        for group in self.blocker_groups:
            lines.append(
                "| "
                f"{group.blocker_type} | "
                f"{group.severity} | "
                f"{group.count} | "
                f"{_markdown_cell(', '.join(group.affected_task_ids) or 'None')} |"
            )
        return "\n".join(lines)


def build_plan_unresolved_blocker_summary(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanUnresolvedBlockerSummary:
    """Summarize unresolved blockers for a plan, task, or task collection."""
    plan_id, tasks = _source_payload(source)
    records: dict[PlanBlockerType, list[tuple[str, str]]] = {
        blocker_type: [] for blocker_type in _BLOCKER_ORDER
    }
    task_ids = [_task_id(task, index) for index, task in enumerate(tasks, start=1)]
    task_by_id = {
        task_id: task for task_id, task in zip(task_ids, tasks, strict=False)
    }

    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        for blocker_type, detail in _task_blockers(task, task_id, task_by_id):
            records[blocker_type].append((task_id, detail))

    groups = tuple(
        PlanUnresolvedBlockerGroup(
            blocker_type=blocker_type,
            severity=_BLOCKER_SEVERITY[blocker_type],
            affected_task_ids=tuple(_dedupe(task_id for task_id, _ in values)),
            count=len(values),
            details=tuple(_dedupe(detail for _, detail in values)),
        )
        for blocker_type, values in records.items()
        if values
    )
    sorted_groups = tuple(
        sorted(
            groups,
            key=lambda group: (
                _SEVERITY_ORDER[group.severity],
                _BLOCKER_ORDER[group.blocker_type],
            ),
        )
    )
    blocked_task_ids = tuple(_dedupe(task_id for group in sorted_groups for task_id in group.affected_task_ids))
    severity_counts = {
        severity: sum(group.count for group in sorted_groups if group.severity == severity)
        for severity in _SEVERITY_ORDER
    }
    blocker_type_counts = {
        blocker_type: sum(group.count for group in sorted_groups if group.blocker_type == blocker_type)
        for blocker_type in _BLOCKER_ORDER
    }
    should_pause = severity_counts["high"] > 0

    return PlanUnresolvedBlockerSummary(
        plan_id=plan_id,
        blocker_groups=sorted_groups,
        blocked_task_ids=blocked_task_ids,
        should_pause_execution=should_pause,
        recommended_next_action=(
            "pause_execution_resolve_high_severity_blockers"
            if should_pause
            else "continue_execution"
        ),
        summary={
            "task_count": len(tasks),
            "blocker_group_count": len(sorted_groups),
            "blocker_count": sum(group.count for group in sorted_groups),
            "affected_task_count": len(blocked_task_ids),
            "high_severity_count": severity_counts["high"],
            "medium_severity_count": severity_counts["medium"],
            "low_severity_count": severity_counts["low"],
            "blocker_type_counts": blocker_type_counts,
        },
    )


def plan_unresolved_blocker_summary_to_dict(
    result: PlanUnresolvedBlockerSummary,
) -> dict[str, Any]:
    """Serialize an unresolved blocker summary to a plain dictionary."""
    return result.to_dict()


plan_unresolved_blocker_summary_to_dict.__test__ = False


def plan_unresolved_blocker_summary_to_markdown(
    result: PlanUnresolvedBlockerSummary,
) -> str:
    """Render an unresolved blocker summary as Markdown."""
    return result.to_markdown()


plan_unresolved_blocker_summary_to_markdown.__test__ = False


def summarize_plan_unresolved_blockers(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
) -> PlanUnresolvedBlockerSummary:
    """Compatibility alias for building unresolved blocker summaries."""
    return build_plan_unresolved_blocker_summary(source)


def _task_blockers(
    task: Mapping[str, Any],
    task_id: str,
    task_by_id: Mapping[str, Mapping[str, Any]],
) -> list[tuple[PlanBlockerType, str]]:
    blockers: list[tuple[PlanBlockerType, str]] = []
    if reason := _blocked_reason(task):
        blockers.append(("blocked_task", reason))
    for detail in _dependency_blockers(task, task_by_id):
        blockers.append(("missing_dependency_output", detail))
    for detail in _validation_blockers(task):
        blockers.append(("validation_blocker", detail))
    if _missing_owner(task):
        blockers.append(("missing_owner", f"{task_id}: missing owner"))
    for detail in _unresolved_items(task, "assumptions"):
        blockers.append(("unresolved_assumption", detail))
    for detail in _unresolved_items(task, "questions"):
        blockers.append(("unanswered_question", detail))
    return blockers


def _blocked_reason(task: Mapping[str, Any]) -> str:
    status = _optional_text(task.get("status"))
    explicit = _optional_text(
        task.get("blocked_reason")
        or task.get("blocker")
        or task.get("blockers")
        or _metadata_value(task.get("metadata"), "blocked_reason")
        or _metadata_value(task.get("metadata"), "blocker")
        or _metadata_value(task.get("metadata"), "blockers")
    )
    if status and status.casefold() == "blocked":
        return explicit or "status: blocked"
    if explicit and _UNRESOLVED_RE.search(explicit):
        return explicit
    if _truthy(task.get("blocked")) or _truthy(_metadata_value(task.get("metadata"), "blocked")):
        return explicit or "blocked: true"
    return ""


def _dependency_blockers(
    task: Mapping[str, Any],
    task_by_id: Mapping[str, Mapping[str, Any]],
) -> tuple[str, ...]:
    blockers: list[str] = []
    for dependency_id in _strings(task.get("depends_on") or task.get("dependencies")):
        dependency = task_by_id.get(dependency_id)
        if dependency is None:
            blockers.append(f"missing dependency task: {dependency_id}")
            continue
        if _has_outputs(dependency):
            continue
        if _status_resolved(dependency.get("status")):
            blockers.append(f"dependency {dependency_id} is complete but has no outputs")
        else:
            blockers.append(f"dependency {dependency_id} has no available outputs")
    return tuple(_dedupe(blockers))


def _validation_blockers(task: Mapping[str, Any]) -> tuple[str, ...]:
    blockers = _unresolved_items(task, "validation_blockers")
    blockers.extend(_unresolved_items(task, "validation"))
    blockers.extend(_unresolved_items(task, "validation_questions"))
    if not _strings(task.get("acceptance_criteria")):
        blockers.append("missing acceptance criteria")
    return tuple(_dedupe(blockers))


def _missing_owner(task: Mapping[str, Any]) -> bool:
    owner = (
        task.get("owner")
        or task.get("owner_id")
        or task.get("owner_type")
        or task.get("assignee")
        or _metadata_value(task.get("metadata"), "owner")
        or _metadata_value(task.get("metadata"), "owner_id")
        or _metadata_value(task.get("metadata"), "owner_type")
        or _metadata_value(task.get("metadata"), "assignee")
    )
    return _optional_text(owner) is None


def _unresolved_items(task: Mapping[str, Any], key: str) -> list[str]:
    values = (
        task.get(key)
        or task.get(f"unresolved_{key}")
        or _metadata_value(task.get("metadata"), key)
        or _metadata_value(task.get("metadata"), f"unresolved_{key}")
    )
    return [
        text
        for text, raw_value in _item_details(values)
        if text and _is_unresolved_item(text, raw_value)
    ]


def _item_details(value: Any) -> list[tuple[str, Any]]:
    if value is None:
        return []
    if isinstance(value, Mapping):
        if any(key in value for key in ("text", "question", "assumption", "description", "status", "resolved", "answer")):
            return [(_item_text(value), value)]
        items: list[tuple[str, Any]] = []
        for key in sorted(value, key=lambda item: str(item)):
            child = value[key]
            if isinstance(child, (Mapping, list, tuple, set)):
                items.extend(_item_details(child))
            elif text := _optional_text(child):
                items.append((f"{key}: {text}", child))
        return items
    if isinstance(value, str):
        text = _optional_text(value)
        return [(text, value)] if text else []
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        details: list[tuple[str, Any]] = []
        for item in items:
            details.extend(_item_details(item))
        return details
    text = _optional_text(value)
    return [(text, value)] if text else []


def _item_text(value: Mapping[str, Any]) -> str:
    for key in ("text", "question", "assumption", "description", "title", "id"):
        if text := _optional_text(value.get(key)):
            return text
    return _text(value)


def _is_unresolved_item(text: str, value: Any) -> bool:
    if isinstance(value, Mapping):
        if "resolved" in value:
            return not _truthy(value.get("resolved"))
        if value.get("answer") or value.get("answered_by"):
            return False
        status = _optional_text(value.get("status"))
        if status:
            folded = status.casefold()
            if folded in _RESOLVED_STATUSES:
                return False
            if folded in _OPEN_STATUSES:
                return True
        severity = _optional_text(value.get("severity"))
        if severity and severity.casefold() == "none":
            return False
    return bool(_UNRESOLVED_RE.search(text))


def _has_outputs(task: Mapping[str, Any]) -> bool:
    for key in ("outputs", "output", "dependency_outputs", "deliverables", "artifacts"):
        value = task.get(key) or _metadata_value(task.get("metadata"), key)
        if _strings(value):
            return True
    return False


def _source_payload(
    source: Mapping[str, Any] | ExecutionPlan | ExecutionTask | Iterable[Mapping[str, Any] | ExecutionTask],
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


def _task_id(task: Mapping[str, Any], index: int) -> str:
    return _optional_text(task.get("id")) or f"task-{index}"


def _status_resolved(value: Any) -> bool:
    text = _optional_text(value)
    return text is not None and text.casefold() in _RESOLVED_STATUSES


def _metadata_value(value: Any, key: str) -> Any:
    return value.get(key) if isinstance(value, Mapping) else None


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = _optional_text(value)
    return text is not None and text.casefold() in {"1", "true", "yes", "y"}


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


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return _SPACE_RE.sub(" ", str(value)).strip()


def _markdown_cell(value: str) -> str:
    return _text(value).replace("|", "\\|").replace("\n", " ")


def _dedupe(values: Iterable[_T]) -> list[_T]:
    deduped: list[_T] = []
    seen: set[_T] = set()
    for value in values:
        if not value or value in seen:
            continue
        deduped.append(value)
        seen.add(value)
    return deduped


__all__ = [
    "PlanBlockerSeverity",
    "PlanBlockerType",
    "PlanUnresolvedBlockerGroup",
    "PlanUnresolvedBlockerSummary",
    "build_plan_unresolved_blocker_summary",
    "plan_unresolved_blocker_summary_to_dict",
    "plan_unresolved_blocker_summary_to_markdown",
    "summarize_plan_unresolved_blockers",
]
