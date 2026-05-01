"""Detect unsafe task sequencing constraints in execution plans."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Iterable, Literal, Mapping, TypeVar

from pydantic import ValidationError

from blueprint.domain.models import ExecutionPlan


TaskSequencingSeverity = Literal["error", "warning"]
_T = TypeVar("_T")

_OPEN_DECISION_STATUSES = {
    "blocked",
    "open",
    "pending",
    "todo",
    "tbd",
    "unanswered",
    "unknown",
    "unresolved",
    "waiting",
}
_RESOLVED_DECISION_STATUSES = {
    "accepted",
    "answered",
    "approved",
    "closed",
    "complete",
    "completed",
    "decided",
    "done",
    "resolved",
}
_UNRESOLVED_DECISION_RE = re.compile(
    r"\b(?:awaiting|blocked|decision|pending|tbd|unknown|unresolved|waiting)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class TaskSequencingFinding:
    """One sequencing problem that can make autonomous execution unsafe."""

    task_id: str
    severity: TaskSequencingSeverity
    reason: str
    suggested_remediation: str
    code: str
    related_task_ids: tuple[str, ...] = field(default_factory=tuple)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-compatible representation in stable key order."""
        return {
            "task_id": self.task_id,
            "severity": self.severity,
            "reason": self.reason,
            "suggested_remediation": self.suggested_remediation,
            "code": self.code,
            "related_task_ids": list(self.related_task_ids),
        }


def analyze_task_sequencing_constraints(
    plan: Mapping[str, Any] | ExecutionPlan,
    *,
    max_chain_depth: int | None = None,
) -> tuple[TaskSequencingFinding, ...]:
    """Return sequencing findings for dependency safety in an execution plan.

    Dependency depth is measured as the number of prerequisite edges on the
    longest known chain ending at a task. Missing dependency references are
    reported separately and ignored for depth calculation.
    """
    payload = _plan_payload(plan)
    records = _task_records(_task_payloads(payload.get("tasks")))
    tasks_by_id = {record.task_id: record.task for record in records}
    task_ids = set(tasks_by_id)
    dependencies_by_task_id = {
        record.task_id: _dependency_ids(record.task) for record in records
    }
    known_dependencies_by_task_id = {
        task_id: tuple(
            dependency_id for dependency_id in dependency_ids if dependency_id in task_ids
        )
        for task_id, dependency_ids in dependencies_by_task_id.items()
    }

    findings: list[TaskSequencingFinding] = []
    findings.extend(
        _missing_dependency_findings(
            records=records,
            dependencies_by_task_id=dependencies_by_task_id,
            task_ids=task_ids,
        )
    )
    findings.extend(
        _parallelization_findings(
            records=records,
            dependencies_by_task_id=known_dependencies_by_task_id,
        )
    )
    findings.extend(_unresolved_decision_findings(records))
    if max_chain_depth is not None:
        findings.extend(
            _chain_depth_findings(
                records=records,
                dependencies_by_task_id=known_dependencies_by_task_id,
                max_chain_depth=max(0, max_chain_depth),
            )
        )

    return tuple(findings)


def task_sequencing_findings_to_dicts(
    findings: Iterable[TaskSequencingFinding],
) -> list[dict[str, Any]]:
    """Serialize sequencing findings to plain dictionaries."""
    return [finding.to_dict() for finding in findings]


task_sequencing_findings_to_dicts.__test__ = False


@dataclass(frozen=True, slots=True)
class _TaskRecord:
    task: dict[str, Any]
    task_id: str
    title: str


def _missing_dependency_findings(
    *,
    records: tuple[_TaskRecord, ...],
    dependencies_by_task_id: dict[str, tuple[str, ...]],
    task_ids: set[str],
) -> list[TaskSequencingFinding]:
    findings: list[TaskSequencingFinding] = []
    for record in records:
        missing_ids = [
            dependency_id
            for dependency_id in dependencies_by_task_id.get(record.task_id, ())
            if dependency_id not in task_ids
        ]
        if not missing_ids:
            continue
        findings.append(
            TaskSequencingFinding(
                task_id=record.task_id,
                severity="error",
                code="missing_dependency",
                reason=(
                    "Task depends on missing prerequisite task(s): "
                    + ", ".join(missing_ids)
                    + "."
                ),
                suggested_remediation=(
                    "Add the missing prerequisite task(s), correct the depends_on IDs, "
                    "or remove stale dependency references before dispatch."
                ),
                related_task_ids=tuple(missing_ids),
            )
        )
    return findings


def _parallelization_findings(
    *,
    records: tuple[_TaskRecord, ...],
    dependencies_by_task_id: dict[str, tuple[str, ...]],
) -> list[TaskSequencingFinding]:
    findings: list[TaskSequencingFinding] = []
    parallel_safe_task_ids = {
        record.task_id for record in records if _claims_independent_execution(record.task)
    }
    for record in records:
        conflicting_dependencies = [
            dependency_id
            for dependency_id in dependencies_by_task_id.get(record.task_id, ())
            if record.task_id in parallel_safe_task_ids
            and dependency_id in parallel_safe_task_ids
        ]
        if not conflicting_dependencies:
            continue
        findings.append(
            TaskSequencingFinding(
                task_id=record.task_id,
                severity="error",
                code="parallel_dependency_conflict",
                reason=(
                    "Task is marked independently runnable but depends on task(s) "
                    "with the same parallel-safe metadata: "
                    + ", ".join(conflicting_dependencies)
                    + "."
                ),
                suggested_remediation=(
                    "Remove independent/parallel-safe metadata from the dependent task "
                    "or split the dependency so the prerequisite completes first."
                ),
                related_task_ids=tuple(conflicting_dependencies),
            )
        )
    return findings


def _unresolved_decision_findings(
    records: tuple[_TaskRecord, ...],
) -> list[TaskSequencingFinding]:
    findings: list[TaskSequencingFinding] = []
    for record in records:
        blockers = _unresolved_decision_blockers(record.task)
        if not blockers:
            continue
        blocker_text = "; ".join(blocker.rstrip(".") for blocker in blockers)
        findings.append(
            TaskSequencingFinding(
                task_id=record.task_id,
                severity="error",
                code="unresolved_decision_blocker",
                reason="Task is blocked by unresolved decision(s): " + blocker_text + ".",
                suggested_remediation=(
                    "Resolve and record the decision outcome, then clear the decision blocker "
                    "before autonomous execution."
                ),
            )
        )
    return findings


def _chain_depth_findings(
    *,
    records: tuple[_TaskRecord, ...],
    dependencies_by_task_id: dict[str, tuple[str, ...]],
    max_chain_depth: int,
) -> list[TaskSequencingFinding]:
    depths_by_task_id = _depths_by_task_id(dependencies_by_task_id)
    findings: list[TaskSequencingFinding] = []
    for record in records:
        depth = depths_by_task_id.get(record.task_id, 0)
        if depth <= max_chain_depth:
            continue
        findings.append(
            TaskSequencingFinding(
                task_id=record.task_id,
                severity="warning",
                code="dependency_chain_depth_exceeded",
                reason=(
                    f"Task has dependency chain depth {depth}, which exceeds "
                    f"the configured limit of {max_chain_depth}."
                ),
                suggested_remediation=(
                    "Review whether the chain can be shortened, split into staged waves, "
                    "or assigned a higher max_chain_depth for this plan."
                ),
                related_task_ids=dependencies_by_task_id.get(record.task_id, ()),
            )
        )
    return findings


def _depths_by_task_id(
    dependencies_by_task_id: dict[str, tuple[str, ...]],
) -> dict[str, int]:
    depths: dict[str, int] = {}
    visiting: set[str] = set()

    def depth_for(task_id: str) -> int:
        if task_id in depths:
            return depths[task_id]
        if task_id in visiting:
            depths[task_id] = 0
            return 0

        visiting.add(task_id)
        dependencies = dependencies_by_task_id.get(task_id, ())
        depth = max((depth_for(dependency_id) for dependency_id in dependencies), default=-1) + 1
        visiting.remove(task_id)
        depths[task_id] = depth
        return depth

    for task_id in dependencies_by_task_id:
        depth_for(task_id)
    return depths


def _unresolved_decision_blockers(task: Mapping[str, Any]) -> tuple[str, ...]:
    blockers: list[str] = []
    metadata = task.get("metadata")
    for value in (
        task.get("decision_blockers"),
        task.get("unresolved_decisions"),
        _metadata_value(metadata, "decision_blockers"),
        _metadata_value(metadata, "unresolved_decisions"),
        _metadata_value(metadata, "pending_decisions"),
    ):
        blockers.extend(_open_decision_texts(value))

    for value in (task.get("decisions"), _metadata_value(metadata, "decisions")):
        blockers.extend(_open_decision_records(value))

    blocked_reason = _optional_text(task.get("blocked_reason")) or _optional_text(
        _metadata_value(metadata, "blocked_reason")
    )
    if blocked_reason and _UNRESOLVED_DECISION_RE.search(blocked_reason):
        blockers.append(blocked_reason)

    return tuple(_dedupe(blockers))


def _open_decision_records(value: Any) -> list[str]:
    if not isinstance(value, (list, tuple)):
        return []
    blockers: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            continue
        status = _optional_text(item.get("status"))
        if status and status.lower() in _RESOLVED_DECISION_STATUSES:
            continue
        if status and status.lower() not in _OPEN_DECISION_STATUSES:
            continue
        text = (
            _optional_text(item.get("decision"))
            or _optional_text(item.get("question"))
            or _optional_text(item.get("title"))
            or _optional_text(item.get("description"))
        )
        if text:
            blockers.append(text)
    return blockers


def _open_decision_texts(value: Any) -> list[str]:
    if value in (None, False):
        return []
    if value is True:
        return ["unresolved decision"]
    if isinstance(value, Mapping):
        status = _optional_text(value.get("status"))
        if status and status.lower() in _RESOLVED_DECISION_STATUSES:
            return []
        text = (
            _optional_text(value.get("decision"))
            or _optional_text(value.get("question"))
            or _optional_text(value.get("title"))
            or _optional_text(value.get("description"))
            or _optional_text(value.get("reason"))
        )
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        items = sorted(value, key=lambda item: str(item)) if isinstance(value, set) else value
        blockers: list[str] = []
        for item in items:
            blockers.extend(_open_decision_texts(item))
        return blockers
    text = _optional_text(value)
    return [text] if text else []


def _claims_independent_execution(task: Mapping[str, Any]) -> bool:
    metadata = task.get("metadata")
    for key in (
        "independently_runnable",
        "independent",
        "parallel_safe",
        "parallelizable",
        "can_run_in_parallel",
    ):
        if _truthy(task.get(key)) or _truthy(_metadata_value(metadata, key)):
            return True
    return False


def _plan_payload(plan: Mapping[str, Any] | ExecutionPlan) -> dict[str, Any]:
    if hasattr(plan, "model_dump"):
        value = plan.model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    try:
        value = ExecutionPlan.model_validate(plan).model_dump(mode="python")
        return dict(value) if isinstance(value, Mapping) else {}
    except (TypeError, ValueError, ValidationError):
        if isinstance(plan, Mapping):
            return dict(plan)
    return {}


def _task_records(tasks: list[dict[str, Any]]) -> tuple[_TaskRecord, ...]:
    records: list[_TaskRecord] = []
    seen_ids: set[str] = set()
    for index, task in enumerate(tasks, start=1):
        task_id = _task_id(task, index)
        if task_id in seen_ids:
            continue
        seen_ids.add(task_id)
        records.append(
            _TaskRecord(
                task=task,
                task_id=task_id,
                title=_optional_text(task.get("title")) or task_id,
            )
        )
    return tuple(records)


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


def _dependency_ids(task: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(_dedupe(_strings(task.get("depends_on"))))


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


def _metadata_value(metadata: Any, key: str) -> Any:
    if not isinstance(metadata, Mapping):
        return None
    return metadata.get(key)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split())


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
    "TaskSequencingFinding",
    "TaskSequencingSeverity",
    "analyze_task_sequencing_constraints",
    "task_sequencing_findings_to_dicts",
]
